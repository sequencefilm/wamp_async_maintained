//! Session-replay state for transparent reconnects.
//!
//! When the [`Supervisor`](super::supervisor::Supervisor) is configured with
//! [`ReconnectPolicy::auto_replay_session`](crate::client::ReconnectPolicy),
//! the [`Core`](super::Core) records every successful realm join,
//! subscription, and RPC registration into this struct as it happens. After
//! a transparent transport reconnect the supervisor hands the cached state
//! to the freshly-built `Core`, which re-runs HELLO/SUBSCRIBE/REGISTER so
//! the application sees the new connection as if it were the old one — same
//! user-facing IDs, same subscription channels, same RPC closures.
//!
//! The state is owned (not shared) by `Core` while the event loop runs, and
//! handed back to the supervisor through
//! [`super::CoreExit`](super::CoreExit) on disconnect, so there is no
//! interior-mutability cost on the hot path.

use std::collections::{HashMap, HashSet};

use crate::{
    common::*,
    core::SubscriptionChannel,
};

/// Arguments captured from the last successful realm join, used to replay
/// HELLO/CHALLENGE/WELCOME on a fresh transport.
pub struct RealmJoinArgs<'a> {
    pub realm: WampString,
    pub roles: HashSet<ClientRole>,
    pub agent_str: Option<WampString>,
    pub authentication_methods: Vec<AuthenticationMethod>,
    pub authentication_id: Option<WampString>,
    pub authextra: Option<HashMap<String, String>>,
    pub on_challenge_handler: Option<AuthenticationChallengeHandler<'a>>,
}

/// Captured state for one live subscription. `senders` is the canonical owner
/// of the channel write-ends for this subscription; the per-connection
/// `Core::subscriptions` map holds clones for fast event dispatch.
pub struct SubReplayEntry {
    pub topic: WampString,
    pub options: WampDict,
    pub senders: Vec<SubscriptionChannel>,
}

/// Captured state for one live RPC registration. `func_ptr` is the shared
/// Arc-wrapped form so the per-connection `Core::rpc_endpoints` map can hold
/// a cheap clone without giving up the canonical copy here.
pub struct RegReplayEntry<'a> {
    pub uri: WampString,
    pub options: WampDict,
    pub func_ptr: SharedRpcFunc<'a>,
}

/// Snapshot of what the application *thinks* its session looks like. Survives
/// across transparent reconnects when auto-replay is enabled.
pub struct SessionReplayState<'a> {
    pub realm: Option<RealmJoinArgs<'a>>,
    /// Keyed by the *client-facing* subscription ID (the server ID returned
    /// by the very first SUBSCRIBE). The current-connection server ID lives
    /// in [`super::Core::subscription_aliases`].
    pub subscriptions: HashMap<WampId, SubReplayEntry>,
    /// Keyed by the client-facing registration ID. See
    /// [`super::Core::registration_aliases`] for the current-connection
    /// server ID.
    pub registrations: HashMap<WampId, RegReplayEntry<'a>>,
}

impl<'a> SessionReplayState<'a> {
    pub fn new() -> Self {
        Self {
            realm: None,
            subscriptions: HashMap::new(),
            registrations: HashMap::new(),
        }
    }

    /// `true` if there's nothing for the supervisor to replay after a
    /// reconnect — used to skip the replay phase entirely.
    pub fn is_empty(&self) -> bool {
        self.realm.is_none() && self.subscriptions.is_empty() && self.registrations.is_empty()
    }
}

impl<'a> Default for SessionReplayState<'a> {
    fn default() -> Self {
        Self::new()
    }
}
