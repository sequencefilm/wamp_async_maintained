use std::time::Duration;

use log::*;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    GenericFuture,
    client::{ClientConfig, ReconnectEvent, ReconnectPolicy},
    core::{Core, EventLoopExit, Request},
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

        let mut exit = initial_core.event_loop(&mut self.ctl_receiver).await;

        loop {
            match exit {
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

                    match self.try_reconnect(&policy, cause).await {
                        Ok(new_core) => {
                            self.emit(ReconnectEvent::Reconnected);
                            exit = new_core.event_loop(&mut self.ctl_receiver).await;
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
    async fn try_reconnect(
        &mut self,
        policy: &ReconnectPolicy,
        initial_cause: WampError,
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

            info!(
                "Supervisor: reconnect attempt {} in {:?} (cause: {})",
                attempt, delay, cause_str
            );
            self.emit(ReconnectEvent::Reconnecting {
                attempt,
                cause: cause_str.clone(),
                delay,
            });

            tokio::time::sleep(delay).await;

            match Core::connect(
                &self.uri,
                &self.config,
                self.ctl_sender.clone(),
                self.rpc_event_queue_w.clone(),
            )
            .await
            {
                Ok(core) => {
                    info!("Supervisor: transport reconnected on attempt {}", attempt);
                    return Ok(core);
                }
                Err(e) => {
                    warn!("Supervisor: reconnect attempt {} failed: {}", attempt, e);
                    cause_str = format!("{}", e);
                    cause = e;
                    attempt = attempt.saturating_add(1);
                    delay = next_backoff(delay, policy);
                }
            }
        }
    }

    fn emit(&self, event: ReconnectEvent) {
        if let Some(tx) = &self.reconnect_events {
            let _ = tx.send(event);
        }
    }
}

/// Computes the next backoff delay, capped at `policy.max_backoff`.
fn next_backoff(current: Duration, policy: &ReconnectPolicy) -> Duration {
    let scaled = current.as_secs_f64() * policy.backoff_multiplier.max(1.0);
    let capped = scaled.min(policy.max_backoff.as_secs_f64());
    Duration::from_secs_f64(capped)
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
}
