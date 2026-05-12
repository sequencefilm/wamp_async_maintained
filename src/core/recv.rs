use crate::core::*;

pub async fn subscribed(core: &mut Core<'_>, request: WampId, sub_id: WampId) -> Status {
    let entry = match core.pending_sub.remove(&request) {
        Some(v) => v,
        None => {
            warn!(
                "Server sent subscribed event for ID we never asked for : {}",
                request
            );
            return Status::Ok;
        }
    };

    match entry {
        PendingSubEntry::Initial { topic, options, res } => {
            let (evt_queue_w, evt_queue_r) = mpsc::unbounded_channel();

            // Add the new sender to the current-connection subscriptions map.
            core.subscriptions
                .entry(sub_id)
                .or_default()
                .insert(0, evt_queue_w.clone());

            // The very first server-assigned ID becomes the stable
            // client-facing ID. Subsequent reconnects update the alias to
            // point at whatever the server gives us, but the user keeps
            // calling unsubscribe(sub_id) with this original value.
            let client_sub_id = sub_id;
            core.subscription_aliases.insert(client_sub_id, sub_id);

            // Record into replay state so we can re-subscribe after a
            // reconnect. The sender Vec lives here (the per-connection
            // subscriptions map holds a clone).
            if let Some(rs) = core.replay_state.as_mut() {
                rs.subscriptions
                    .entry(client_sub_id)
                    .or_insert_with(|| SubReplayEntry {
                        topic,
                        options,
                        senders: Vec::new(),
                    })
                    .senders
                    .push(evt_queue_w);
            }

            let _ = res.send(Ok((client_sub_id, evt_queue_r)));
        }
        PendingSubEntry::Replay { client_sub_id } => {
            // The senders are already alive in replay_state — clone them
            // into the per-connection subscriptions map under the new
            // server-assigned ID, and update the alias so user-facing
            // unsubscribe calls translate to the live server ID.
            let senders = match core.replay_state.as_ref() {
                Some(rs) => match rs.subscriptions.get(&client_sub_id) {
                    Some(entry) => entry.senders.clone(),
                    None => {
                        warn!(
                            "Replay SUBSCRIBED for missing client_sub_id {} ({})",
                            client_sub_id, sub_id
                        );
                        return Status::Ok;
                    }
                },
                None => {
                    warn!("Replay SUBSCRIBED with no replay state — programmer error");
                    return Status::Ok;
                }
            };
            core.subscriptions
                .entry(sub_id)
                .or_default()
                .extend(senders);
            core.subscription_aliases.insert(client_sub_id, sub_id);
            debug!(
                "Replay: re-bound sub {} → server ID {}",
                client_sub_id, sub_id
            );
        }
    }

    Status::Ok
}
pub async fn unsubscribed(core: &mut Core<'_>, request: WampId) -> Status {
    let res = match core.pending_transactions.remove(&request) {
        Some(v) => v,
        None => {
            warn!(
                "Server sent unsubscribed event for ID we never asked for : {}",
                request
            );
            return Status::Ok;
        }
    };

    // Send the event queue back to the requestor
    let _ = res.send(Ok(None));

    Status::Ok
}
pub async fn published(core: &mut Core<'_>, request: WampId, pub_id: WampId) -> Status {
    let res = match core.pending_transactions.remove(&request) {
        Some(v) => v,
        None => {
            warn!(
                "Server sent published event for ID we never asked for : {}",
                request
            );
            return Status::Ok;
        }
    };
    let _ = res.send(Ok(Some(pub_id)));

    Status::Ok
}
pub async fn event(
    core: &mut Core<'_>,
    subscription: WampId,
    publication: WampId,
    details: WampDict,
    arguments: Option<WampArgs>,
    arguments_kw: Option<WampKwArgs>,
) -> Status {
    let evt_queues = match core.subscriptions.get(&subscription) {
        Some(e) => e,
        None => {
            warn!(
                "Server sent event for sub ID we are not subscribed to : {}",
                subscription
            );
            return Status::Ok;
        }
    };

    // Forward the event to the client
    evt_queues.iter().for_each(|evt_queue| {
        if evt_queue
            .send((
                publication,
                details.clone(),
                arguments.clone(),
                arguments_kw.clone(),
            ))
            .is_err()
        {
            warn!(
                "Client not listenning to subscription {} but did not unsubscribe...",
                subscription
            );
            // TODO : Should we be nice and send an UNSUBSCRIBE to the server ?
        }
    });

    Status::Ok
}
pub async fn registered(core: &mut Core<'_>, request: WampId, rpc_id: WampId) -> Status {
    let entry = match core.pending_register.remove(&request) {
        Some(v) => v,
        None => {
            warn!(
                "Server sent registered event for ID we never asked for : {}",
                request
            );
            return Status::Ok;
        }
    };

    match entry {
        PendingRegisterEntry::Initial {
            uri,
            options,
            func_ptr,
            res,
        } => {
            if core.rpc_endpoints.contains_key(&rpc_id) {
                warn!("Server sent registered ID we already had registered");
                return Status::Ok;
            }

            let client_reg_id = rpc_id;

            // The per-connection map gets a clone of the Arc so invocations
            // dispatch via a single HashMap lookup; the canonical copy lives
            // in replay_state so it survives reconnects.
            core.rpc_endpoints.insert(rpc_id, func_ptr.clone());
            core.registration_aliases.insert(client_reg_id, rpc_id);

            if let Some(rs) = core.replay_state.as_mut() {
                rs.registrations.insert(
                    client_reg_id,
                    RegReplayEntry {
                        uri,
                        options,
                        func_ptr,
                    },
                );
            }

            let _ = res.send(Ok(client_reg_id));
        }
        PendingRegisterEntry::Replay { client_reg_id } => {
            if core.rpc_endpoints.contains_key(&rpc_id) {
                warn!(
                    "Replay REGISTERED collided with existing rpc endpoint {}",
                    rpc_id
                );
                return Status::Ok;
            }
            let func_ptr = match core.replay_state.as_ref() {
                Some(rs) => match rs.registrations.get(&client_reg_id) {
                    Some(entry) => entry.func_ptr.clone(),
                    None => {
                        warn!(
                            "Replay REGISTERED for missing client_reg_id {} ({})",
                            client_reg_id, rpc_id
                        );
                        return Status::Ok;
                    }
                },
                None => {
                    warn!("Replay REGISTERED with no replay state — programmer error");
                    return Status::Ok;
                }
            };
            core.rpc_endpoints.insert(rpc_id, func_ptr);
            core.registration_aliases.insert(client_reg_id, rpc_id);
            debug!(
                "Replay: re-bound rpc {} → server ID {}",
                client_reg_id, rpc_id
            );
        }
    }

    Status::Ok
}
pub async fn unregisterd(core: &mut Core<'_>, request: WampId) -> Status {
    let res = match core.pending_transactions.remove(&request) {
        Some(v) => v,
        None => {
            warn!("Server sent unsolicited unregistered ID : {}", request);
            return Status::Ok;
        }
    };

    // Send the event queue back to the requestor
    let _ = res.send(Ok(None));

    Status::Ok
}

/// Runs the RPC function and forwards the result
async fn rpc_func_runner(
    ctl_channel: UnboundedSender<Request<'_>>,
    request: WampId,
    rpc_func: RpcFuture<'_>,
) -> Result<(), WampError> {
    // Run the RPC func
    let res = rpc_func.await;

    // Send the result
    match ctl_channel.send(Request::InvocationResult { request, res }) {
        Ok(_) => Ok(()),
        Err(_) => Err(From::from("Event loop has died !".to_string())),
    }
}

pub async fn invocation(
    core: &mut Core<'_>,
    request: WampId,
    registration: WampId,
    _details: WampDict,
    arguments: Option<WampArgs>,
    arguments_kw: Option<WampKwArgs>,
) -> Status {
    let rpc_func = match core.rpc_endpoints.get(&registration) {
        Some(e) => e,
        None => {
            warn!(
                "Server sent invocation for rpc ID but we do not have this endpoint : {}",
                registration
            );
            return Status::Ok;
        }
    };

    let ctl_channel = core.ctl_sender.clone();
    let func_future = rpc_func(arguments, arguments_kw);

    // Forward the event to the client
    if core
        .rpc_event_queue_w
        .send(Box::pin(rpc_func_runner(ctl_channel, request, func_future)))
        .is_err()
    {
        warn!(
            "Client not listenning to rpc events but got invocation for rpc ID {}",
            registration
        );
        // TODO : Should we be nice and send an UNSUBSCRIBE to the server ?
    }

    Status::Ok
}
pub async fn call_result(
    core: &mut Core<'_>,
    request: WampId,
    _details: WampDict,
    arguments: Option<WampArgs>,
    arguments_kw: Option<WampKwArgs>,
) -> Status {
    let res = match core.pending_call.remove(&request) {
        Some(r) => r,
        None => {
            warn!(
                "Server sent result for CALL we never sent : request id {}",
                request
            );
            return Status::Ok;
        }
    };

    // Forward the event to the client
    if res.send(Ok((arguments, arguments_kw))).is_err() {
        warn!("Client not waiting for call result id {}", request);
        // TODO : Should we be nice and send an UNSUBSCRIBE to the server ?
    }

    Status::Ok
}

pub async fn goodbye(core: &mut Core<'_>, details: WampDict, reason: WampString) -> Status {
    debug!("Server sent goodbye : {:?} {:?}", details, reason);

    if !core.valid_session && reason == "wamp.close.goodbye_and_out" {
        Status::Ok
    } else {
        debug!("Peer is closing on us !");
        let _ = core
            .send(&Msg::Goodbye {
                details: WampDict::new(),
                reason: "wamp.close.goodbye_and_out".to_string(),
            })
            .await;
        Status::Shutdown
    }
}

pub async fn abort(_core: &mut Core<'_>, details: WampDict, reason: WampString) -> Status {
    error!("Server sent abort : {:?} {:?}", details, reason);
    Status::Shutdown
}
// Handles an error sent by the peer
pub async fn error(
    core: &mut Core<'_>,
    typ: WampInteger,
    request: WampId,
    details: WampDict,
    error: WampUri,
    _arguments: Option<WampArgs>,
    _arguments_kw: Option<WampKwArgs>,
) -> Status {
    let error = WampError::ServerError(error, details);
    match typ {
        SUBSCRIBE_ID => {
            match core.pending_sub.remove(&request) {
                Some(PendingSubEntry::Initial { res, .. }) => {
                    let _ = res.send(Err(error));
                }
                Some(PendingSubEntry::Replay { client_sub_id }) => {
                    // The server refused a replay SUBSCRIBE (permissions
                    // changed, topic gone, etc.). Drop the entry from
                    // replay state so it doesn't keep failing forever; the
                    // application will need to re-subscribe explicitly.
                    warn!(
                        "Replay SUBSCRIBE rejected for client_sub_id {} : {:?}",
                        client_sub_id, error
                    );
                    if let Some(rs) = core.replay_state.as_mut() {
                        rs.subscriptions.remove(&client_sub_id);
                    }
                    core.subscription_aliases.remove(&client_sub_id);
                }
                None => {
                    warn!("Received error for subscribe message we never sent");
                    return Status::Ok;
                }
            }
        }
        REGISTER_ID => {
            match core.pending_register.remove(&request) {
                Some(PendingRegisterEntry::Initial { res, .. }) => {
                    let _ = res.send(Err(error));
                }
                Some(PendingRegisterEntry::Replay { client_reg_id }) => {
                    warn!(
                        "Replay REGISTER rejected for client_reg_id {} : {:?}",
                        client_reg_id, error
                    );
                    if let Some(rs) = core.replay_state.as_mut() {
                        rs.registrations.remove(&client_reg_id);
                    }
                    core.registration_aliases.remove(&client_reg_id);
                }
                None => {
                    warn!("Received error for RPC register message we never sent");
                    return Status::Ok;
                }
            }
        }
        CALL_ID => {
            let res = match core.pending_call.remove(&request) {
                Some(r) => r,
                None => {
                    warn!("Received error for CALL message we never sent");
                    return Status::Ok;
                }
            };
            let _ = res.send(Err(error));
        }
        PUBLISH_ID | UNSUBSCRIBE_ID | UNREGISTER_ID => {
            let res = match core.pending_transactions.remove(&request) {
                Some(r) => r,
                None => {
                    warn!("Received error for message we never sent");
                    return Status::Ok;
                }
            };
            let _ = res.send(Err(error));
        }
        _ => {}
    };
    Status::Ok
}
