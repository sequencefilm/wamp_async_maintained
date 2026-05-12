use std::collections::{HashMap, HashSet};

use log::*;
use tokio::sync::oneshot::Sender;

use crate::{common::*, core::*, message::*};

/// Constructs the connection-lost sentinel used to mark transport failures
/// without losing the original error's `Display` content. Send paths must
/// surface a [`WampError`] both to the in-flight oneshot AND to
/// [`Status::ConnectionLost`]; `WampError` is not `Clone`, so we stringify the
/// cause into a fresh `WampError` for the supervisor while moving the original
/// to the caller.
fn connection_lost_from(cause: &WampError) -> WampError {
    From::from(format!("transport failed: {}", cause))
}

pub type JoinRealmResult = Result<(WampId, HashMap<WampString, Arg>), WampError>;
pub enum Request<'a> {
    Shutdown,
    Join {
        uri: WampString,
        roles: HashSet<ClientRole>,
        agent_str: Option<WampString>,
        authentication_methods: Vec<AuthenticationMethod>,
        authentication_id: Option<WampString>,
        authextra: Option<HashMap<String, String>>,
        on_challenge_handler: Option<AuthenticationChallengeHandler<'a>>,
        res: Sender<JoinRealmResult>,
    },
    Leave {
        res: Sender<Result<(), WampError>>,
    },
    Subscribe {
        uri: WampString,
        options: WampDict,
        res: PendingSubResult,
    },
    Unsubscribe {
        sub_id: WampId,
        res: Sender<Result<Option<WampId>, WampError>>,
    },
    Publish {
        uri: WampString,
        options: WampDict,
        arguments: Option<WampArgs>,
        arguments_kw: Option<WampKwArgs>,
        res: Sender<Result<Option<WampId>, WampError>>,
    },
    Register {
        uri: WampString,
        res: PendingRegisterResult,
        func_ptr: RpcFunc<'a>,
        options: WampDict,
    },
    Unregister {
        rpc_id: WampId,
        res: Sender<Result<Option<WampId>, WampError>>,
    },
    InvocationResult {
        request: WampId,
        res: Result<(Option<WampArgs>, Option<WampKwArgs>), WampError>,
    },
    Call {
        uri: WampString,
        options: WampDict,
        arguments: Option<WampArgs>,
        arguments_kw: Option<WampKwArgs>,
        res: PendingCallResult,
    },
}

/// Handler for any join realm request. This will send a HELLO and wait for the WELCOME response
#[allow(clippy::too_many_arguments)] // This should be turned back on
pub async fn join_realm<'a>(
    core: &mut Core<'a>,
    uri: WampString,
    roles: HashSet<ClientRole>,
    agent_str: Option<WampString>,
    authentication_methods: Vec<AuthenticationMethod>,
    authextra: Option<HashMap<String, String>>,
    authid: Option<WampString>,
    on_challenge_handler: Option<AuthenticationChallengeHandler<'a>>,
    res: JoinResult,
) -> Status {
    // Snapshot inputs up front so the same join can be replayed verbatim
    // after a reconnect when auto-replay is enabled. The handler `Arc` is
    // shared (not cloned deeply) so a single user closure backs both the
    // initial join and every subsequent replay.
    let replay_snapshot = if core.replay_state.is_some() {
        Some(RealmJoinArgs {
            realm: uri.clone(),
            roles: roles.clone(),
            agent_str: agent_str.clone(),
            authentication_methods: authentication_methods.clone(),
            authentication_id: authid.clone(),
            authextra: authextra.clone(),
            on_challenge_handler: on_challenge_handler.clone(),
        })
    } else {
        None
    };
    let mut details: WampDict = WampDict::new();
    let mut client_roles: WampDict = WampDict::new();
    // Add all of our roles
    for role in &roles {
        let mut roledict = WampDict::new();
        // Support for pattern_based_subscription MUST be announced by Subscribers.
        // Crossbar doesn't enforce this, but other brokers might.
        if role.has_features() {
            roledict.insert("features".to_owned(), Arg::Dict(role.get_features()));
        }

        client_roles.insert(String::from(role.to_str()), Arg::Dict(roledict.clone()));
    }
    details.insert("roles".to_owned(), Arg::Dict(client_roles));

    if let Some(agent) = agent_str {
        details.insert("agent".to_owned(), Arg::String(agent));
    }

    if !authentication_methods.is_empty() {
        details.insert(
            "authmethods".to_owned(),
            Arg::List(
                authentication_methods
                    .iter()
                    .map(|authentication_method| {
                        Arg::String(authentication_method.as_ref().to_owned())
                    })
                    .collect::<Vec<_>>(),
            ),
        );
        if let Some(extra) = authextra {
            let a: WampDict = WampDict::from([(
                "pubkey".to_owned(),
                Arg::String(extra.get("pubkey").unwrap().to_owned()),
            )]);
            details.insert("authextra".to_owned(), Arg::Dict(a));
        }
    }

    if let Some(authid) = authid {
        details.insert("authid".to_owned(), Arg::String(authid));
    }

    // Send hello with our info
    if let Err(e) = core
        .send(&Msg::Hello {
            realm: uri,
            details,
        })
        .await
    {
        let lost = connection_lost_from(&e);
        let _ = res.send(Err(e));
        return Status::ConnectionLost(lost);
    }

    // Make sure the server responded with the proper message
    let (session_id, server_roles) = loop {
        // Receive the response to the HELLO message (either WELCOME or CHALLENGE are expected)
        let resp = match core.recv().await {
            Ok(r) => r,
            Err(e) => {
                let lost = connection_lost_from(&e);
                let _ = res.send(Err(e));
                return Status::ConnectionLost(lost);
            }
        };

        match resp {
            Msg::Welcome { session, details } => break (session, details),
            Msg::Challenge {
                authentication_method,
                extra,
            } => {
                if let Some(ref on_challenge_handler) = on_challenge_handler {
                    match on_challenge_handler(authentication_method, extra).await {
                        Ok(AuthenticationChallengeResponse { signature, extra }) => {
                            if let Err(e) = core.send(&Msg::Authenticate { signature, extra }).await
                            {
                                let lost = connection_lost_from(&e);
                                let _ = res.send(Err(e));
                                return Status::ConnectionLost(lost);
                            }
                        }
                        Err(e) => {
                            let _ = res.send(Err(e));
                            return Status::Shutdown;
                        }
                    }
                } else {
                    let _ = res.send(Err(From::from(
                        "Server requested a CHALLENGE to authenticate, but there was no challenge handler provided".to_string()
                    )));
                    return Status::Shutdown;
                }
            }
            m => {
                let _ = res.send(Err(From::from(format!(
                    "Server did not respond with WELCOME : {:?}",
                    m
                ))));
                return Status::Shutdown;
            }
        }
    };

    // Return the pertinent info to the caller
    core.valid_session = true;
    if let Some(rs) = core.replay_state.as_mut() {
        rs.realm = replay_snapshot;
    }
    let _ = res.send(Ok((session_id, server_roles)));

    Status::Ok
}

/// Re-runs the HELLO/CHALLENGE/WELCOME flow against `core`'s freshly-built
/// transport using the realm-join args cached in `replay_state` from the last
/// successful user-initiated `join_realm`. Returns `Status::Ok` on success
/// (the new session is live on the new connection) or `Status::ConnectionLost`
/// if any send/recv fails — the supervisor's outer loop will count that as a
/// failed reconnect attempt and back off.
///
/// Differs from [`join_realm`] in two ways: there is no caller-side oneshot to
/// reply on (the supervisor is driving), and the closure for CHALLENGE is the
/// cached `Arc` from the original join, so the application doesn't need to
/// re-provide credentials. `core.valid_session` is set on success.
pub async fn replay_join_realm<'a>(
    core: &mut Core<'a>,
    args: &RealmJoinArgs<'a>,
) -> Result<(), WampError> {
    let mut details: WampDict = WampDict::new();
    let mut client_roles: WampDict = WampDict::new();
    for role in &args.roles {
        let mut roledict = WampDict::new();
        if role.has_features() {
            roledict.insert("features".to_owned(), Arg::Dict(role.get_features()));
        }
        client_roles.insert(String::from(role.to_str()), Arg::Dict(roledict));
    }
    details.insert("roles".to_owned(), Arg::Dict(client_roles));

    if let Some(agent) = &args.agent_str {
        details.insert("agent".to_owned(), Arg::String(agent.clone()));
    }
    if !args.authentication_methods.is_empty() {
        details.insert(
            "authmethods".to_owned(),
            Arg::List(
                args.authentication_methods
                    .iter()
                    .map(|m| Arg::String(m.as_ref().to_owned()))
                    .collect(),
            ),
        );
        if let Some(extra) = &args.authextra {
            let a: WampDict = WampDict::from([(
                "pubkey".to_owned(),
                Arg::String(extra.get("pubkey").unwrap().to_owned()),
            )]);
            details.insert("authextra".to_owned(), Arg::Dict(a));
        }
    }
    if let Some(authid) = &args.authentication_id {
        details.insert("authid".to_owned(), Arg::String(authid.clone()));
    }

    core.send(&Msg::Hello {
        realm: args.realm.clone(),
        details,
    })
    .await?;

    loop {
        let resp = core.recv().await?;
        match resp {
            Msg::Welcome { session, details: _ } => {
                core.valid_session = true;
                debug!("Replay: realm rejoined with session_id {}", session);
                return Ok(());
            }
            Msg::Challenge {
                authentication_method,
                extra,
            } => match &args.on_challenge_handler {
                Some(handler) => match handler(authentication_method, extra).await {
                    Ok(AuthenticationChallengeResponse { signature, extra }) => {
                        core.send(&Msg::Authenticate { signature, extra }).await?;
                    }
                    Err(e) => return Err(e),
                },
                None => {
                    return Err(From::from(
                        "Replay: server requested CHALLENGE but cached realm has no handler"
                            .to_string(),
                    ));
                }
            },
            m => {
                return Err(From::from(format!(
                    "Replay: server did not respond with WELCOME : {:?}",
                    m
                )));
            }
        }
    }
}

/// Handler for any leave realm request. This function will send a GOODBYE and wait for a GOODBYE response
pub async fn leave_realm(core: &mut Core<'_>, res: Sender<Result<(), WampError>>) -> Status {
    core.valid_session = false;
    // The caller is explicitly leaving the realm; if we reconnect after this
    // the supervisor must not silently re-join. Drop the cached realm and
    // all session-scoped bindings so replay is a no-op.
    if let Some(rs) = core.replay_state.as_mut() {
        rs.realm = None;
        rs.subscriptions.clear();
        rs.registrations.clear();
    }

    if let Err(e) = core
        .send(&Msg::Goodbye {
            reason: "wamp.close.close_realm".to_string(),
            details: WampDict::new(),
        })
        .await
    {
        let lost = connection_lost_from(&e);
        let _ = res.send(Err(e));
        return Status::ConnectionLost(lost);
    }

    let _ = res.send(Ok(()));

    Status::Ok
}

pub async fn subscribe(
    core: &mut Core<'_>,
    topic: WampString,
    options: WampDict,
    res: PendingSubResult,
) -> Status {
    let request = core.create_request();
    if let Err(e) = core
        .send(&Msg::Subscribe {
            request,
            topic: topic.clone(),
            options: options.clone(),
        })
        .await
    {
        core.pending_requests.remove(&request);
        let lost = connection_lost_from(&e);
        let _ = res.send(Err(e));
        return Status::ConnectionLost(lost);
    }
    core.pending_sub.insert(
        request,
        PendingSubEntry::Initial {
            topic,
            options,
            res,
        },
    );

    Status::Ok
}

/// Re-issues a SUBSCRIBE for a replay entry that already lives in
/// [`SessionReplayState`]. On SUBSCRIBED the recv handler will refresh
/// `subscription_aliases[client_sub_id]` and the per-connection
/// `subscriptions` map. Caller (the supervisor) awaits the response by
/// running [`Core::event_loop`] or by driving a single recv outside it.
pub async fn replay_subscribe<'a>(
    core: &mut Core<'a>,
    client_sub_id: WampId,
    entry: &SubReplayEntry,
) -> Result<(), WampError> {
    let request = core.create_request();
    core.send(&Msg::Subscribe {
        request,
        topic: entry.topic.clone(),
        options: entry.options.clone(),
    })
    .await?;
    core.pending_sub
        .insert(request, PendingSubEntry::Replay { client_sub_id });
    Ok(())
}

pub async fn unsubscribe(
    core: &mut Core<'_>,
    sub_id: WampId,
    res: Sender<Result<Option<WampId>, WampError>>,
) -> Status {
    // `sub_id` is the user-facing (client) ID. Translate to whatever the
    // current server thinks the ID is, then unwind both alias map and
    // replay state so a future reconnect doesn't re-subscribe to a topic
    // the user has explicitly unsubscribed from.
    let server_sub_id = match core.subscription_aliases.remove(&sub_id) {
        Some(s) => s,
        None => {
            warn!("Tried to unsubscribe using invalid sub_id : {}", sub_id);
            let _ = res.send(Err(From::from(
                "Tried to unsubscribe from unknown sub_id".to_string(),
            )));
            return Status::Ok;
        }
    };
    core.subscriptions.remove(&server_sub_id);
    if let Some(rs) = core.replay_state.as_mut() {
        rs.subscriptions.remove(&sub_id);
    }

    let request = core.create_request();

    if let Err(e) = core
        .send(&Msg::Unsubscribe {
            request,
            subscription: server_sub_id,
        })
        .await
    {
        core.pending_requests.remove(&request);
        let lost = connection_lost_from(&e);
        let _ = res.send(Err(e));
        return Status::ConnectionLost(lost);
    }

    core.pending_transactions.insert(request, res);

    Status::Ok
}

pub async fn publish(
    core: &mut Core<'_>,
    uri: WampString,
    options: WampDict,
    arguments: Option<WampArgs>,
    arguments_kw: Option<WampKwArgs>,
    res: Sender<Result<Option<WampId>, WampError>>,
) -> Status {
    let request = core.create_request();

    if let Err(e) = core
        .send(&Msg::Publish {
            request,
            topic: uri,
            options,
            arguments,
            arguments_kw,
        })
        .await
    {
        core.pending_requests.remove(&request);
        let lost = connection_lost_from(&e);
        let _ = res.send(Err(e));
        return Status::ConnectionLost(lost);
    }

    core.pending_transactions.insert(request, res);

    Status::Ok
}

pub async fn register<'a>(
    core: &mut Core<'a>,
    uri: WampString,
    res: PendingRegisterResult,
    func_ptr: RpcFunc<'a>,
    options: Option<WampDict>,
) -> Status {
    let request = core.create_request();
    let op = options.unwrap_or_default();

    if let Err(e) = core
        .send(&Msg::Register {
            request,
            procedure: uri.clone(),
            options: op.clone(),
        })
        .await
    {
        core.pending_requests.remove(&request);
        let lost = connection_lost_from(&e);
        let _ = res.send(Err(e));
        return Status::ConnectionLost(lost);
    }

    // Convert the user-supplied Box<dyn Fn> into the internal Arc<dyn Fn>
    // so the same closure can later be cloned cheaply into replay state.
    let shared_func: SharedRpcFunc = std::sync::Arc::from(func_ptr);
    core.pending_register.insert(
        request,
        PendingRegisterEntry::Initial {
            uri,
            options: op,
            func_ptr: shared_func,
            res,
        },
    );
    Status::Ok
}

/// Re-issues a REGISTER for a replay entry already in
/// [`SessionReplayState`]. The recv handler updates
/// `registration_aliases[client_reg_id]` and re-installs the cached
/// `RpcFunc` under the new server-assigned ID.
pub async fn replay_register<'a>(
    core: &mut Core<'a>,
    client_reg_id: WampId,
    entry: &RegReplayEntry<'a>,
) -> Result<(), WampError> {
    let request = core.create_request();
    core.send(&Msg::Register {
        request,
        procedure: entry.uri.clone(),
        options: entry.options.clone(),
    })
    .await?;
    core.pending_register
        .insert(request, PendingRegisterEntry::Replay { client_reg_id });
    Ok(())
}

pub async fn unregister(
    core: &mut Core<'_>,
    rpc_id: WampId,
    res: Sender<Result<Option<WampId>, WampError>>,
) -> Status {
    // Same translation/cleanup as unsubscribe: `rpc_id` is user-facing, look
    // up the current server registration ID, drop the per-connection
    // endpoint, alias, and replay entry.
    let server_reg_id = match core.registration_aliases.remove(&rpc_id) {
        Some(s) => s,
        None => {
            warn!("Tried to unregister RPC using invalid ID : {}", rpc_id);
            let _ = res.send(Err(From::from(
                "Tried to unregister RPC using invalid ID".to_string(),
            )));
            return Status::Ok;
        }
    };
    core.rpc_endpoints.remove(&server_reg_id);
    if let Some(rs) = core.replay_state.as_mut() {
        rs.registrations.remove(&rpc_id);
    }

    let request = core.create_request();

    if let Err(e) = core
        .send(&Msg::Unregister {
            request,
            registration: server_reg_id,
        })
        .await
    {
        core.pending_requests.remove(&request);
        let lost = connection_lost_from(&e);
        let _ = res.send(Err(e));
        return Status::ConnectionLost(lost);
    }

    core.pending_transactions.insert(request, res);

    Status::Ok
}

pub async fn invoke_yield(
    core: &mut Core<'_>,
    request: WampId,
    res: Result<(Option<WampArgs>, Option<WampKwArgs>), WampError>,
) -> Status {
    let msg: Msg = match res {
        Ok((arguments, arguments_kw)) => Msg::Yield {
            request,
            options: WampDict::new(),
            arguments,
            arguments_kw,
        },
        Err(e) => {
            let uri = match &e {
                WampError::ApplicationError(c, _d) => c.to_owned(),
                _ => "wamp.async.rs.rpc.failed".to_string(),
            };

            let service_details = match &e {
                WampError::ApplicationError(_c, d) => Some(d.to_owned()),
                _ => None,
            };

            let (arguments, kwargs): (Vec<WampPayloadValue>, WampKwArgs) = match service_details {
                Some(d) => (d.get_args().unwrap(), d.get_kwargs().unwrap()),
                None => (vec![format!("{:?}", e).into()], WampKwArgs::new()),
            };

            Msg::Error {
                typ: INVOCATION_ID as WampInteger,
                request,
                details: WampDict::new(),
                error: uri,
                arguments: Some(arguments),
                arguments_kw: Some(kwargs),
            }
        }
    };
    if let Err(e) = core.send(&msg).await {
        let lost = connection_lost_from(&e);
        return Status::ConnectionLost(lost);
    }

    Status::Ok
}

pub async fn call(
    core: &mut Core<'_>,
    uri: WampString,
    options: WampDict,
    arguments: Option<WampArgs>,
    arguments_kw: Option<WampKwArgs>,
    res: PendingCallResult,
) -> Status {
    let request = core.create_request();

    if let Err(e) = core
        .send(&Msg::Call {
            request,
            procedure: uri,
            options,
            arguments,
            arguments_kw,
        })
        .await
    {
        core.pending_requests.remove(&request);
        let lost = connection_lost_from(&e);
        let _ = res.send(Err(e));
        return Status::ConnectionLost(lost);
    }

    core.pending_call.insert(request, res);

    Status::Ok
}
