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
mod send;
mod supervisor;

pub use send::Request;
pub use supervisor::Supervisor;

use crate::{Arg, client, message::*};

pub enum Status {
    /// Returned when the event loop should shutdown
    Shutdown,
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
    pending_sub: HashMap<WampId, PendingSubResult>,
    /// Current subscriptions
    subscriptions: HashMap<WampId, Vec<SubscriptionChannel>>,

    /// Pending RPC registration requests sent to the server
    pending_register: HashMap<WampId, (RpcFunc<'a>, PendingRegisterResult)>,
    /// Currently registered RPC endpoints
    rpc_endpoints: HashMap<WampId, RpcFunc<'a>>,
    /// Supervisor-owned writer for RPC invocation futures. Kept across
    /// reconnects so the Client's receiver stays valid.
    rpc_event_queue_w: UnboundedSender<GenericFuture<'a>>,

    pending_call: HashMap<WampId, PendingCallResult>,
}

impl<'a> Core<'a> {
    /// Establishes a connection with a WAMP server.
    ///
    /// The `ctl_sender` is cloned into RPC invocation futures so they can push
    /// `InvocationResult` onto the event loop's control channel.
    /// `rpc_event_queue_w` is owned by the supervisor and persists across
    /// reconnects, keeping the Client's receiver valid.
    pub async fn connect(
        uri: &url::Url,
        cfg: &client::ClientConfig,
        ctl_sender: UnboundedSender<Request<'a>>,
        rpc_event_queue_w: UnboundedSender<GenericFuture<'a>>,
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

            pending_register: HashMap::new(),
            rpc_endpoints: HashMap::new(),
            rpc_event_queue_w,
            pending_call: HashMap::new(),
        })
    }

    /// Drives the event loop against the supervisor-owned control channel.
    ///
    /// Returns an [`EventLoopExit`] describing why the loop stopped so the
    /// supervisor can decide between reconnect, terminate, or propagate an
    /// error. The transport is closed before returning.
    pub async fn event_loop(
        mut self,
        ctl_channel: &mut UnboundedReceiver<Request<'a>>,
    ) -> EventLoopExit {
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
                Status::Ok => {}
            }
        };

        debug!("Event loop returning : shutting down transport");
        self.shutdown().await;
        exit
    }

    /// Handles unsolicited messages from the peer (events, rpc calls, etc...)
    async fn handle_peer_msg<'b>(&'b mut self, msg: Msg) -> Status
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
