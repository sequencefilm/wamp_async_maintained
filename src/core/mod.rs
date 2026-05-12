use std::collections::{HashMap, HashSet};

use log::*;
use tokio::{
    select,
    sync::{
        mpsc,
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
};

use crate::{common::*, error::*, serializer::*, transport::*};

mod recv;
mod replay;
mod send;
mod supervisor;

pub use replay::{RealmJoinArgs, RegReplayEntry, SessionReplayState, SubReplayEntry};
pub use send::Request;
pub use supervisor::Supervisor;

use crate::{Arg, client, message::*};

pub enum Status {
    /// Returned when the event loop should shutdown gracefully (peer GOODBYE,
    /// peer ABORT, explicit `Request::Shutdown`).
    Shutdown,
    /// Returned when a request handler detected a transport-level failure
    /// (typically `send()` returning Err). The event loop must surface this as
    /// [`EventLoopExit::ConnectionLost`] so the supervisor will reconnect
    /// instead of treating the drop as a clean teardown.
    ConnectionLost(WampError),
    Ok,
}

/// Reason the event loop returned. The supervisor inspects this to decide
/// whether to attempt a reconnect or tear down.
pub enum EventLoopExit {
    /// Graceful shutdown (Request::Shutdown, peer GOODBYE, or leaving the
    /// realm). No reconnect should be attempted.
    Shutdown,
    /// The Client handle went away (ctl channel sender was dropped) before
    /// sending Shutdown. Treated as terminal.
    ClientDied,
    /// Transport-level failure. Eligible for reconnect if a policy is set.
    ConnectionLost(WampError),
}

/// Result of running [`Core::event_loop`] to completion. Bundles the exit
/// reason with the [`SessionReplayState`] so the supervisor can hand the
/// cached realm/subscriptions/registrations to the next `Core` for replay
/// when auto-replay is enabled.
pub struct CoreExit<'a> {
    pub exit: EventLoopExit,
    pub replay_state: Option<SessionReplayState<'a>>,
}

pub type JoinResult = Sender<
    Result<
        (
            WampId,                   // Session ID
            HashMap<WampString, Arg>, // Server roles
        ),
        WampError,
    >,
>;
pub type SubscriptionQueue = UnboundedReceiver<(
    WampId,           // Publish event ID
    WampDict,         // Publish event Details
    Option<WampArgs>, // Publish args
    Option<WampKwArgs>,
)>; // publish kwargs
pub type PendingSubResult = Sender<
    Result<
        (
            WampId,            //Subcription ID
            SubscriptionQueue, // Queue for incoming events
        ),
        WampError,
    >,
>;
pub type PendingRegisterResult = Sender<
    Result<
        WampId, // Registration ID
        WampError,
    >,
>;
pub type PendingCallResult = Sender<
    Result<
        (
            Option<WampArgs>,   // Return args
            Option<WampKwArgs>, // Return kwargs
        ),
        WampError,
    >,
>;

type SubscriptionChannel =
    UnboundedSender<(WampId, WampDict, Option<WampArgs>, Option<WampKwArgs>)>;

/// Wraps a pending SUBSCRIBE request so the reply handler knows whether the
/// caller is a user-facing `Client::subscribe` call (Initial) or a replay
/// re-binding driven by the supervisor (Replay).
pub(crate) enum PendingSubEntry {
    Initial {
        topic: WampString,
        options: WampDict,
        res: PendingSubResult,
    },
    /// Re-issued SUBSCRIBE during session replay. The senders are already
    /// alive in [`SessionReplayState`]; the reply handler just needs to know
    /// which `client_sub_id` this new server ID is replacing.
    Replay { client_sub_id: WampId },
}

/// Same Initial/Replay split as [`PendingSubEntry`] for REGISTER. The
/// `func_ptr` is the internal Arc-wrapped form so it can be cloned cheaply
/// into both `rpc_endpoints` and `replay_state` from the recv handler.
pub(crate) enum PendingRegisterEntry<'a> {
    Initial {
        uri: WampString,
        options: WampDict,
        func_ptr: SharedRpcFunc<'a>,
        res: PendingRegisterResult,
    },
    Replay { client_reg_id: WampId },
}

pub struct Core<'a> {
    /// Generic transport
    sock: Box<dyn Transport + Send>,
    valid_session: bool,
    /// Generic serializer
    serializer: Box<dyn SerializerImpl + Send>,
    /// Sender onto the control channel; cloned into RPC runner futures so
    /// they can push InvocationResult replies back to the event loop.
    ctl_sender: UnboundedSender<Request<'a>>,

    /// Holds set of pending requests
    pending_requests: HashSet<WampId>,
    /// Holds generic transactions that can succeed/fail
    pending_transactions: HashMap<WampId, Sender<Result<Option<WampId>, WampError>>>,

    /// Pending subscription requests sent to the server
    pending_sub: HashMap<WampId, PendingSubEntry>,
    /// Current subscriptions, keyed by the *server-assigned* sub ID for this
    /// connection. Cleared and rebuilt on every reconnect; user-facing IDs
    /// remain stable via [`Self::subscription_aliases`].
    subscriptions: HashMap<WampId, Vec<SubscriptionChannel>>,
    /// `client_sub_id` (the server ID returned to the user on the *first*
    /// SUBSCRIBE) → current connection's server sub ID. Populated on every
    /// SUBSCRIBED. Used to translate user-facing IDs (e.g. on UNSUBSCRIBE)
    /// to whatever the live server thinks the ID is now.
    pub(crate) subscription_aliases: HashMap<WampId, WampId>,

    /// Pending RPC registration requests sent to the server
    pending_register: HashMap<WampId, PendingRegisterEntry<'a>>,
    /// Currently registered RPC endpoints, keyed by the connection's
    /// server-assigned registration ID. Held as `SharedRpcFunc` so the same
    /// closure can also live in [`SessionReplayState`] for cross-reconnect
    /// replay.
    rpc_endpoints: HashMap<WampId, SharedRpcFunc<'a>>,
    /// `client_reg_id` → current server registration ID. Same role as
    /// [`Self::subscription_aliases`] but for RPC registrations.
    pub(crate) registration_aliases: HashMap<WampId, WampId>,
    /// Supervisor-owned writer for RPC invocation futures. Kept across
    /// reconnects so the Client's receiver stays valid.
    rpc_event_queue_w: UnboundedSender<GenericFuture<'a>>,

    pending_call: HashMap<WampId, PendingCallResult>,

    /// When `Some`, every successful realm join / SUBSCRIBE / REGISTER is
    /// recorded here so the supervisor can replay it after a transparent
    /// reconnect. `None` when [`ReconnectPolicy::auto_replay_session`] is
    /// disabled. Owned by `Core` while the event loop runs and handed back
    /// via [`CoreExit`] on exit.
    pub(crate) replay_state: Option<SessionReplayState<'a>>,
}

impl<'a> Core<'a> {
    /// Establishes a connection with a WAMP server.
    ///
    /// The `ctl_sender` is cloned into RPC invocation futures so they can push
    /// `InvocationResult` onto the event loop's control channel.
    /// `rpc_event_queue_w` is owned by the supervisor and persists across
    /// reconnects, keeping the Client's receiver valid.
    ///
    /// `replay_state` is `Some` when the supervisor is configured to
    /// transparently replay realm join / subscriptions / registrations after
    /// a reconnect. It is owned by the new `Core` and returned in
    /// [`CoreExit`] when the event loop exits.
    pub async fn connect(
        uri: &url::Url,
        cfg: &client::ClientConfig,
        ctl_sender: UnboundedSender<Request<'a>>,
        rpc_event_queue_w: UnboundedSender<GenericFuture<'a>>,
        replay_state: Option<SessionReplayState<'a>>,
    ) -> Result<Core<'a>, WampError> {
        // Connect to the router using the requested transport
        let (sock, serializer_type) = match uri.scheme() {
            "ws" | "wss" => ws::connect(uri, cfg).await?,
            "tcp" | "tcps" => {
                let host_port = match uri.port() {
                    Some(p) => p,
                    None => {
                        return Err(From::from("No port specified for tcp host".to_string()));
                    }
                };

                // Perform the TCP connection
                tcp::connect(
                    uri.host_str().unwrap(),
                    host_port,
                    uri.scheme() != "tcp",
                    cfg,
                )
                .await?
            }
            s => return Err(From::from(format!("Unknown uri scheme : {}", s))),
        };

        debug!("Connected with serializer : {:?}", serializer_type);

        let serializer: Box<dyn SerializerImpl + Send> = match serializer_type {
            SerializerType::Cbor => Box::new(cbor::CborSerializer {}),
            SerializerType::Json => Box::new(json::JsonSerializer {}),
            SerializerType::MsgPack => Box::new(msgpack::MsgPackSerializer {}),
        };

        Ok(Core {
            sock,
            valid_session: false,
            serializer,
            ctl_sender,
            pending_requests: HashSet::new(),
            pending_transactions: HashMap::new(),

            pending_sub: HashMap::new(),
            subscriptions: HashMap::new(),
            subscription_aliases: HashMap::new(),

            pending_register: HashMap::new(),
            rpc_endpoints: HashMap::new(),
            registration_aliases: HashMap::new(),
            rpc_event_queue_w,
            pending_call: HashMap::new(),
            replay_state,
        })
    }

    /// Drives the event loop against the supervisor-owned control channel.
    ///
    /// Returns a [`CoreExit`] carrying both the reason for the loop ending
    /// and the [`SessionReplayState`] that the supervisor needs to replay
    /// the realm/subscriptions/registrations on the next connection. The
    /// transport is closed before returning.
    pub async fn event_loop(
        mut self,
        ctl_channel: &mut UnboundedReceiver<Request<'a>>,
    ) -> CoreExit<'a> {
        let exit: EventLoopExit = loop {
            let status = select! {
                // Peer sent us a message
                msg = self.recv() => {
                    match msg {
                        Err(e) => {
                            /* The WAMP spec leaves it up to the server implementation
                            to decide whether to close a connection or not after a
                            GOODBYE message (leaving the realm). If we have left the realm,
                            treat a recv() error as expected */
                            if self.valid_session {
                                warn!("Failed to recv : {:?}", e);
                                break EventLoopExit::ConnectionLost(e);
                            }
                            break EventLoopExit::Shutdown;
                        },
                        Ok(m) => self.handle_peer_msg(m).await,
                    }
                },
                // client wants to send a message
                req = ctl_channel.recv() => {
                    let req = match req {
                        Some(r) => r,
                        None => {
                            break EventLoopExit::ClientDied;
                        }
                    };
                    self.handle_local_request(req).await
                }
            };
            match status {
                Status::Shutdown => break EventLoopExit::Shutdown,
                Status::ConnectionLost(e) => {
                    warn!("Event loop: handler reported connection lost : {}", e);
                    break EventLoopExit::ConnectionLost(e);
                }
                Status::Ok => {}
            }
        };

        debug!("Event loop returning : shutting down transport");
        let replay_state = self.replay_state.take();
        self.shutdown().await;
        CoreExit { exit, replay_state }
    }

    /// Removes and returns the cached session-replay state, leaving `None` in
    /// its place. Used by the supervisor between reconnect attempts so the
    /// cache can be re-installed into the next `Core::connect`.
    pub fn take_replay_state(&mut self) -> Option<SessionReplayState<'a>> {
        self.replay_state.take()
    }

    /// Handles unsolicited messages from the peer (events, rpc calls, etc...)
    pub(crate) async fn handle_peer_msg<'b>(&'b mut self, msg: Msg) -> Status
    where
        'a: 'b,
    {
        // Make sure we were expecting this message if it has a request ID
        if let Some(ref request) = msg.request_id()
            && !self.pending_requests.remove(request)
        {
            warn!("Peer sent a response to an unknown request : {}", request);
            return Status::Ok;
        }
        match msg {
            Msg::Subscribed {
                request,
                subscription,
            } => recv::subscribed(self, request, subscription).await,
            Msg::Unsubscribed { request } => recv::unsubscribed(self, request).await,
            Msg::Published {
                request,
                publication,
            } => recv::published(self, request, publication).await,
            Msg::Event {
                subscription,
                publication,
                details,
                arguments,
                arguments_kw,
            } => {
                recv::event(
                    self,
                    subscription,
                    publication,
                    details,
                    arguments,
                    arguments_kw,
                )
                .await
            }
            Msg::Registered {
                request,
                registration,
            } => recv::registered(self, request, registration).await,
            Msg::Unregistered { request } => recv::unregisterd(self, request).await,
            Msg::Invocation {
                request,
                registration,
                details,
                arguments,
                arguments_kw,
            } => {
                recv::invocation(
                    self,
                    request,
                    registration,
                    details,
                    arguments,
                    arguments_kw,
                )
                .await
            }
            Msg::Result {
                request,
                details,
                arguments,
                arguments_kw,
            } => recv::call_result(self, request, details, arguments, arguments_kw).await,
            Msg::Goodbye { details, reason } => recv::goodbye(self, details, reason).await,
            Msg::Abort { details, reason } => recv::abort(self, details, reason).await,
            Msg::Error {
                typ,
                request,
                details,
                error,
                arguments,
                arguments_kw,
            } => recv::error(self, typ, request, details, error, arguments, arguments_kw).await,
            _ => {
                warn!("Recevied unhandled message {:?}", msg);
                Status::Ok
            }
        }
    }

    /// Handles the basic ways one can interact with the peer
    async fn handle_local_request(&mut self, req: Request<'a>) -> Status {
        // Forward the request the the implementor
        match req {
            Request::Shutdown => Status::Shutdown,
            Request::Join {
                uri,
                roles,
                agent_str,
                authentication_methods,
                authentication_id,
                authextra,
                on_challenge_handler,
                res,
            } => {
                send::join_realm(
                    self,
                    uri,
                    roles,
                    agent_str,
                    authentication_methods,
                    authextra,
                    authentication_id,
                    on_challenge_handler,
                    res,
                )
                .await
            }
            Request::Leave { res } => send::leave_realm(self, res).await,
            Request::Subscribe { uri, options, res } => {
                send::subscribe(self, uri, options, res).await
            }
            Request::Unsubscribe { sub_id, res } => send::unsubscribe(self, sub_id, res).await,
            Request::Publish {
                uri,
                options,
                arguments,
                arguments_kw,
                res,
            } => send::publish(self, uri, options, arguments, arguments_kw, res).await,
            Request::Register {
                uri,
                res,
                func_ptr,
                options,
            } => send::register(self, uri, res, func_ptr, Some(options)).await,
            Request::Unregister { rpc_id, res } => send::unregister(self, rpc_id, res).await,
            Request::InvocationResult { request, res } => {
                send::invoke_yield(self, request, res).await
            }
            Request::Call {
                uri,
                options,
                arguments,
                arguments_kw,
                res,
            } => send::call(self, uri, options, arguments, arguments_kw, res).await,
        }
    }

    /// Serializes a message and sends it on the transport
    pub async fn send(&mut self, msg: &Msg) -> Result<(), WampError> {
        // Serialize the data
        let payload = self.serializer.pack(msg)?;

        match std::str::from_utf8(&payload) {
            Ok(v) => debug!("Send : {}", v),
            Err(_) => debug!("Send : {:?}", msg),
        };

        // Send to host
        self.sock.send(&payload).await?;

        Ok(())
    }

    /// Receives a message and deserializes it
    pub async fn recv<'b>(&'b mut self) -> Result<Msg, WampError>
    where
        'a: 'b,
    {
        // Receive a full message from the host
        let payload = self.sock.recv().await?;

        // Deserialize into a Msg
        let msg = self.serializer.unpack(&payload);

        match std::str::from_utf8(&payload) {
            Ok(v) => debug!("Recv : {}", v),
            Err(_) => debug!("Recv : {:?}", msg),
        };

        Ok(msg?)
    }

    /// Closes the transport
    pub async fn shutdown(mut self) {
        // Close the transport
        self.sock.close().await;
    }

    /// Generates a new request_id and inserts it into the pending_requests
    fn create_request(&mut self) -> WampId {
        let mut request = WampId::generate();
        // Pick a unique request_id
        while !self.pending_requests.insert(request) {
            request = WampId::generate();
        }
        request
    }
}
