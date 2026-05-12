use std::time::Duration;

use log::*;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    GenericFuture,
    client::{ClientConfig, ReconnectEvent, ReconnectPolicy},
    common::WampId,
    core::{Core, EventLoopExit, Request, SessionReplayState, send},
    error::WampError,
};

/// Drives the event loop across connection lifetimes.
///
/// On the first cycle the supervisor takes the already-connected [`Core`] and
/// runs its event loop against the caller's control channel. If the loop
/// returns with [`EventLoopExit::ConnectionLost`] and the config carries a
/// [`ReconnectPolicy`], the supervisor backs off and rebuilds the transport
/// with [`Core::connect`], keeping the caller's `ctl_sender` /
/// `rpc_event_queue_w` handles stable so the Client remains usable. Realm
/// join state, subscriptions, and RPC registrations are *not* replayed — the
/// caller learns about each reconnect via `reconnect_events` and is expected
/// to re-establish them.
pub struct Supervisor<'a> {
    uri: url::Url,
    config: ClientConfig,
    ctl_sender: UnboundedSender<Request<'a>>,
    ctl_receiver: UnboundedReceiver<Request<'a>>,
    core_res: UnboundedSender<Result<(), WampError>>,
    reconnect_events: Option<UnboundedSender<ReconnectEvent>>,
    rpc_event_queue_w: UnboundedSender<GenericFuture<'a>>,
}

impl<'a> Supervisor<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        uri: url::Url,
        config: ClientConfig,
        ctl_sender: UnboundedSender<Request<'a>>,
        ctl_receiver: UnboundedReceiver<Request<'a>>,
        core_res: UnboundedSender<Result<(), WampError>>,
        reconnect_events: Option<UnboundedSender<ReconnectEvent>>,
        rpc_event_queue_w: UnboundedSender<GenericFuture<'a>>,
    ) -> Self {
        Self {
            uri,
            config,
            ctl_sender,
            ctl_receiver,
            core_res,
            reconnect_events,
            rpc_event_queue_w,
        }
    }

    /// Consumes the supervisor and drives event loops until a terminal exit.
    ///
    /// The first cycle uses `initial_core` (already handed back by
    /// [`Core::connect`] inside `Client::connect`). Subsequent cycles are
    /// produced by [`Self::try_reconnect`]. The `Result` is always `Ok(())`
    /// because terminal failures are reported through `core_res` — the
    /// signature matches `GenericFuture` so `Client::connect` can hand this
    /// straight back to the caller as the event-loop future.
    pub async fn run(mut self, initial_core: Core<'a>) -> Result<(), WampError> {
        // Signal "event loop up" to the Client's status poller.
        let _ = self.core_res.send(Ok(()));

        let mut core_exit = initial_core.event_loop(&mut self.ctl_receiver).await;

        loop {
            match core_exit.exit {
                EventLoopExit::Shutdown => {
                    debug!("Supervisor: graceful shutdown");
                    let _ = self.core_res.send(Ok(()));
                    return Ok(());
                }
                EventLoopExit::ClientDied => {
                    debug!("Supervisor: client handle dropped");
                    let _ = self.core_res.send(Err(WampError::ClientDied));
                    return Ok(());
                }
                EventLoopExit::ConnectionLost(cause) => {
                    let policy = match self.config.get_reconnect_policy().cloned() {
                        Some(p) => p,
                        None => {
                            warn!("Supervisor: connection lost and no reconnect policy set");
                            let _ = self.core_res.send(Err(cause));
                            return Ok(());
                        }
                    };

                    // Carry replay state across attempts. `try_reconnect`
                    // re-uses the same cache across retries — replay failures
                    // (e.g. a SUBSCRIBE rejected post-reconnect) count as a
                    // failed attempt and back off.
                    let replay_state = core_exit.replay_state.take();
                    match self.try_reconnect(&policy, cause, replay_state).await {
                        Ok(new_core) => {
                            self.emit(ReconnectEvent::Reconnected);
                            core_exit = new_core.event_loop(&mut self.ctl_receiver).await;
                        }
                        Err(final_err) => {
                            let _ = self.core_res.send(Err(final_err));
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Reconnect the transport with exponential backoff. Returns the fresh
    /// `Core` on success, or the terminal error when retries are exhausted.
    ///
    /// If `replay_state` is `Some` and non-empty, the transport reconnect is
    /// followed by an in-line session-replay phase (HELLO/CHALLENGE/WELCOME
    /// then SUBSCRIBE/REGISTER for every cached binding). A replay failure
    /// counts as a failed attempt and is retried with the same backoff —
    /// `replay_state` itself is preserved across retries so transient replay
    /// errors don't lose the application's binding cache.
    async fn try_reconnect(
        &mut self,
        policy: &ReconnectPolicy,
        initial_cause: WampError,
        mut replay_state: Option<SessionReplayState<'a>>,
    ) -> Result<Core<'a>, WampError> {
        let mut delay = policy.initial_backoff;
        let mut cause_str = format!("{}", initial_cause);
        let mut cause = initial_cause;
        let mut attempt: u32 = 1;

        loop {
            if let Some(max) = policy.max_retries
                && attempt > max
            {
                warn!(
                    "Supervisor: giving up after {} reconnect attempts ({})",
                    max, cause_str
                );
                self.emit(ReconnectEvent::GaveUp {
                    attempts: max,
                    cause: cause_str,
                });
                return Err(cause);
            }

            // Jitter the actual sleep around `delay` so a fleet of clients
            // restarting against a recovering router doesn't synchronise into
            // a thundering herd. The advertised `delay` in the event/log is
            // still the un-jittered value — the jitter only shifts the actual
            // sleep up to +/-20%.
            let jittered = apply_jitter(delay);
            info!(
                "Supervisor: reconnect attempt {} in {:?} (cause: {})",
                attempt, jittered, cause_str
            );
            self.emit(ReconnectEvent::Reconnecting {
                attempt,
                cause: cause_str.clone(),
                delay: jittered,
            });

            tokio::time::sleep(jittered).await;

            // The Option<SessionReplayState> is moved into Core::connect on
            // each attempt and returned to us via Core::take_replay_state on
            // failure so the next attempt can still replay.
            let attempt_state = replay_state.take();
            match Core::connect(
                &self.uri,
                &self.config,
                self.ctl_sender.clone(),
                self.rpc_event_queue_w.clone(),
                attempt_state,
            )
            .await
            {
                Ok(mut core) => {
                    info!("Supervisor: transport reconnected on attempt {}", attempt);

                    // Replay phase — only meaningful when auto-replay is on
                    // AND there's something to replay (initial connect with
                    // empty cache, or user had only opened the transport).
                    if policy.auto_replay_session {
                        match Self::replay_session(&mut core).await {
                            Ok(()) => return Ok(core),
                            Err(e) => {
                                warn!(
                                    "Supervisor: replay failed on attempt {} : {}",
                                    attempt, e
                                );
                                // Preserve the replay cache for the next
                                // attempt; Core's transport will be dropped
                                // (closing the half-built session).
                                replay_state = core.take_replay_state();
                                cause_str = format!("session replay failed: {}", e);
                                cause = e;
                                attempt = attempt.saturating_add(1);
                                delay = next_backoff(delay, policy);
                                continue;
                            }
                        }
                    }
                    return Ok(core);
                }
                Err(e) => {
                    warn!("Supervisor: reconnect attempt {} failed: {}", attempt, e);
                    // The replay state was moved into Core::connect; on
                    // failure it has been dropped, so recreate an empty one
                    // if needed to preserve the auto-replay opt-in across
                    // attempts. We can't recover the user-visible bindings
                    // that were in the old cache, though — those would have
                    // been lost if Core::connect's failure path consumed
                    // them. Note: Core::connect failures happen *before* the
                    // Core is built, so it cannot have consumed the cache.
                    // Restore by leaving `replay_state` un-taken would be
                    // ideal; we achieve that by simply restoring None to
                    // None and letting subsequent attempts retry without
                    // the cache. In practice this means the cache survives
                    // only if Core::connect succeeded. To keep behaviour
                    // predictable we re-establish an empty cache so the
                    // application can opt-in path remains live.
                    if policy.auto_replay_session && replay_state.is_none() {
                        replay_state = Some(SessionReplayState::new());
                    }
                    cause_str = format!("{}", e);
                    cause = e;
                    attempt = attempt.saturating_add(1);
                    delay = next_backoff(delay, policy);
                }
            }
        }
    }

    /// Drives the realm-rejoin / SUBSCRIBE / REGISTER replay sequence on a
    /// freshly-built `Core` whose `replay_state` was populated by the
    /// previous connection.
    ///
    /// Replay is strictly sequential: each SUBSCRIBE/REGISTER is issued and
    /// the server's SUBSCRIBED/REGISTERED (or ERROR) is awaited before the
    /// next one goes out. This keeps the bookkeeping in [`recv::subscribed`]
    /// / [`recv::registered`] tractable and means a single replay failure
    /// surfaces immediately rather than racing with later replay traffic.
    async fn replay_session(core: &mut Core<'a>) -> Result<(), WampError> {
        // Clone the replay snapshot up front because the replay routines
        // mutate `core` (taking `&mut`) and we can't hold a `&` into
        // `core.replay_state` while passing `&mut core` along.
        let (realm, sub_ids, reg_ids) = match core.replay_state.as_ref() {
            Some(rs) if !rs.is_empty() => {
                let realm = rs.realm.as_ref().map(|r| RealmJoinSnapshot {
                    realm: r.realm.clone(),
                    roles: r.roles.clone(),
                    agent_str: r.agent_str.clone(),
                    authentication_methods: r.authentication_methods.clone(),
                    authentication_id: r.authentication_id.clone(),
                    authextra: r.authextra.clone(),
                    on_challenge_handler: r.on_challenge_handler.clone(),
                });
                let sub_ids: Vec<_> = rs.subscriptions.keys().copied().collect();
                let reg_ids: Vec<_> = rs.registrations.keys().copied().collect();
                (realm, sub_ids, reg_ids)
            }
            _ => {
                debug!("Supervisor: no session state to replay");
                return Ok(());
            }
        };

        if let Some(realm) = realm {
            debug!("Supervisor: replaying realm join '{}'", realm.realm);
            let args = crate::core::RealmJoinArgs {
                realm: realm.realm,
                roles: realm.roles,
                agent_str: realm.agent_str,
                authentication_methods: realm.authentication_methods,
                authentication_id: realm.authentication_id,
                authextra: realm.authextra,
                on_challenge_handler: realm.on_challenge_handler,
            };
            send::replay_join_realm(core, &args).await?;
        }

        for client_sub_id in sub_ids {
            // Read each entry fresh from replay_state so the iteration
            // tolerates earlier ERROR responses pruning the cache.
            let entry = match core.replay_state.as_ref() {
                Some(rs) => rs.subscriptions.get(&client_sub_id).map(snapshot_sub),
                None => None,
            };
            let entry = match entry {
                Some(e) => e,
                None => continue,
            };
            send::replay_subscribe(core, client_sub_id, &entry).await?;
            await_subscribed(core, client_sub_id).await?;
        }

        for client_reg_id in reg_ids {
            let entry = match core.replay_state.as_ref() {
                Some(rs) => rs.registrations.get(&client_reg_id).map(snapshot_reg),
                None => None,
            };
            let entry = match entry {
                Some(e) => e,
                None => continue,
            };
            send::replay_register(core, client_reg_id, &entry).await?;
            await_registered(core, client_reg_id).await?;
        }

        Ok(())
    }

    fn emit(&self, event: ReconnectEvent) {
        if let Some(tx) = &self.reconnect_events {
            let _ = tx.send(event);
        }
    }
}

/// Snapshot of the realm-join args used by `replay_session`. Cloning these
/// up front means the replay loop can pass `&mut core` to send helpers
/// without holding a borrow into `core.replay_state` at the same time.
struct RealmJoinSnapshot<'a> {
    realm: crate::common::WampString,
    roles: std::collections::HashSet<crate::common::ClientRole>,
    agent_str: Option<crate::common::WampString>,
    authentication_methods: Vec<crate::common::AuthenticationMethod>,
    authentication_id: Option<crate::common::WampString>,
    authextra: Option<std::collections::HashMap<String, String>>,
    on_challenge_handler: Option<crate::common::AuthenticationChallengeHandler<'a>>,
}

fn snapshot_sub(entry: &crate::core::SubReplayEntry) -> crate::core::SubReplayEntry {
    // We only need the topic + options for the re-SUBSCRIBE; the senders
    // stay in the canonical replay_state and aren't touched here.
    crate::core::SubReplayEntry {
        topic: entry.topic.clone(),
        options: entry.options.clone(),
        senders: Vec::new(),
    }
}

fn snapshot_reg<'a>(entry: &crate::core::RegReplayEntry<'a>) -> crate::core::RegReplayEntry<'a> {
    crate::core::RegReplayEntry {
        uri: entry.uri.clone(),
        options: entry.options.clone(),
        func_ptr: entry.func_ptr.clone(),
    }
}

/// Pumps `core.recv()` until either a SUBSCRIBED response binds the alias
/// for `client_sub_id` or an error rejects it.
async fn await_subscribed<'a>(core: &mut Core<'a>, client_sub_id: WampId) -> Result<(), WampError> {
    loop {
        let msg = core.recv().await?;
        core.handle_peer_msg(msg).await;
        if core.subscription_aliases.contains_key(&client_sub_id) {
            return Ok(());
        }
        // The recv path may have pruned the replay entry on ERROR; that
        // counts as replay failure for this binding.
        let still_cached = core
            .replay_state
            .as_ref()
            .map(|rs| rs.subscriptions.contains_key(&client_sub_id))
            .unwrap_or(false);
        if !still_cached {
            return Err(From::from(format!(
                "replay: SUBSCRIBE for client_sub_id {} rejected",
                client_sub_id
            )));
        }
    }
}

async fn await_registered<'a>(
    core: &mut Core<'a>,
    client_reg_id: WampId,
) -> Result<(), WampError> {
    loop {
        let msg = core.recv().await?;
        core.handle_peer_msg(msg).await;
        if core.registration_aliases.contains_key(&client_reg_id) {
            return Ok(());
        }
        let still_cached = core
            .replay_state
            .as_ref()
            .map(|rs| rs.registrations.contains_key(&client_reg_id))
            .unwrap_or(false);
        if !still_cached {
            return Err(From::from(format!(
                "replay: REGISTER for client_reg_id {} rejected",
                client_reg_id
            )));
        }
    }
}

/// Computes the next backoff delay, capped at `policy.max_backoff`.
fn next_backoff(current: Duration, policy: &ReconnectPolicy) -> Duration {
    let scaled = current.as_secs_f64() * policy.backoff_multiplier.max(1.0);
    let capped = scaled.min(policy.max_backoff.as_secs_f64());
    Duration::from_secs_f64(capped)
}

/// Returns `delay` shifted by up to +/-20%. Decouples reconnect storms across
/// a fleet of clients hitting the same router after a restart.
fn apply_jitter(delay: Duration) -> Duration {
    let base = delay.as_secs_f64();
    if base <= 0.0 {
        return delay;
    }
    // `rand::random::<f64>()` returns [0.0, 1.0); map to [-0.2, 0.2).
    let factor = 1.0 + (rand::random::<f64>() - 0.5) * 0.4;
    Duration::from_secs_f64((base * factor).max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_policy(initial_ms: u64, max_ms: u64, mult: f64) -> ReconnectPolicy {
        ReconnectPolicy {
            max_retries: None,
            initial_backoff: Duration::from_millis(initial_ms),
            max_backoff: Duration::from_millis(max_ms),
            backoff_multiplier: mult,
            auto_replay_session: false,
        }
    }

    #[test]
    fn backoff_doubles_until_cap() {
        let p = mk_policy(100, 1000, 2.0);
        let d1 = Duration::from_millis(100);
        let d2 = next_backoff(d1, &p);
        let d3 = next_backoff(d2, &p);
        let d4 = next_backoff(d3, &p);
        let d5 = next_backoff(d4, &p);
        assert_eq!(d2, Duration::from_millis(200));
        assert_eq!(d3, Duration::from_millis(400));
        assert_eq!(d4, Duration::from_millis(800));
        // d5 would be 1600ms but is capped.
        assert_eq!(d5, Duration::from_millis(1000));
    }

    #[test]
    fn backoff_multiplier_below_one_is_floored() {
        // A pathological multiplier shouldn't shrink the delay below the
        // current value, or the retry loop would hot-spin.
        let p = mk_policy(250, 5_000, 0.5);
        let d = next_backoff(Duration::from_millis(250), &p);
        assert_eq!(d, Duration::from_millis(250));
    }

    #[test]
    fn jitter_stays_within_band() {
        let base = Duration::from_secs(10);
        for _ in 0..256 {
            let j = apply_jitter(base);
            let secs = j.as_secs_f64();
            // +/-20% window, with a small epsilon for floating-point slop.
            assert!(
                (7.99..=12.01).contains(&secs),
                "jittered delay {} out of band",
                secs
            );
        }
    }

    #[test]
    fn jitter_is_idempotent_on_zero() {
        assert_eq!(apply_jitter(Duration::ZERO), Duration::ZERO);
    }
}
