# Spec: Migrate `wamp_helper` to `wamp_async` transparent auto-reconnect

## Goal

Replace the helper's hand-rolled reconnect / heartbeat / re-publish logic with the supervisor-driven auto-reconnect + session replay built into `wamp_async`. End state: a transport drop is invisible to callers — subscriptions keep delivering, registered RPCs keep being invocable, and existing `WampId` handles for subs/regs remain valid.

## Prerequisites

* Crate dependency on `wamp_async` must include:
  * `ReconnectPolicy::auto_replay_session: bool` (new field)
  * `ClientConfig::set_keepalive_max_missed_pongs(u32)` (new method)
  * `Client::take_reconnect_events()` (existing in this branch)
* If `Cargo.toml` pins a published version that predates these, bump to the local/git branch that has them.

## Behavioural contract after the change

* First `Client::connect()` call may fail — propagate up (env misconfig, router down at boot).
* Every subsequent transport drop is recovered transparently. The supervisor re-runs HELLO → SUBSCRIBE × N → REGISTER × N and only emits `Reconnected` once the session is whole.
* `evt_loop.await` returning is a **terminal** event (supervisor gave up). The application should treat this as fatal — log loudly and either exit or rebuild `Conn` from scratch.
* `Subscription { id, queue }` returned from `Conn::subscribe()` is stable across reconnects. So is the `WampId` returned from `Conn::register()`.
* Calling `Conn::reconnect()` during steady-state is **forbidden** — it drops the supervisor's replay cache. Reserve it for explicit "I want to rebuild everything" recovery (e.g. after a `GaveUp` event).

## Changes

### Change 1 — Build a `ClientConfig` with the new policy

**File:** `wamp_helper/src/lib.rs` (or wherever `Conn::connect` lives)

Replace the existing `client_config` builder (both the dead outer `_client_config` and the inner one in the retry loop) with a single helper:

```rust
fn client_config() -> ClientConfig {
    ClientConfig::default()
        .set_keepalive_interval(Some(Duration::from_secs(20)))
        .set_keepalive_max_missed_pongs(2)
        .set_ssl_verify(false)
        .set_serializers(vec![wamp_async::SerializerType::MsgPack])
        .set_reconnect_policy(Some(wamp_async::ReconnectPolicy {
            max_retries: None,
            initial_backoff: Duration::from_millis(500),
            max_backoff:     Duration::from_secs(30),
            backoff_multiplier: 2.0,
            auto_replay_session: true,
        }))
}
```

Justification: keepalive prevents idle-timeout resets; `max_missed_pongs: 2` surfaces half-open connections within ~40 s; `auto_replay_session: true` is the whole point of this migration.

### Change 2 — Collapse the connect retry loop

**Before:**

```rust
let mut attempts = 0;
let max_attempts = 5;
let mut backoff = tokio::time::Duration::from_secs(1);
let (mut client, (evt_loop, rpc_evt_queue)) = loop {
    let client_config = ClientConfig::default()…;
    match Client::connect(&wamp_router_address, Some(client_config)).await {
        Ok(connection) => break connection,
        Err(_e) if attempts < max_attempts => { … sleep, retry … }
        Err(e) => return Err(e.into()),
    }
};
```

**After:**

```rust
let (mut client, (evt_loop, rpc_evt_queue)) =
    Client::connect(&wamp_router_address, Some(client_config())).await?;
```

Justification: the supervisor handles transient reconnect inside the `Client`. Wrapping that with our own connect-retry layer just rebuilds the `Client` from scratch on first failure and would discard any cached session state if it ran post-boot. If "router down at boot" needs to be tolerated, retry **only the very first connect** with a bounded outer loop; once the call succeeds, never call `Client::connect` again for this `Conn`.

### Change 3 — Tear down on event-loop exit but not on side-task exit

**File:** same.

The `event_loop_task` is correct as written (a return from `evt_loop.await` is terminal):

```rust
let event_loop_task = tokio::task::spawn({
    let self_for_evt = self.clone();
    async move {
        // Returning from evt_loop now means the supervisor gave up
        // (or the caller explicitly shut down).
        let _ = evt_loop.await;
        warn!("wamp event loop returned — session is terminal");
        self_for_evt.disconnect().await;
    }
});
```

The `rpc_event_queue_task` should **not** call `self.disconnect()` on its own anymore — the queue stays alive across reconnects:

```rust
let rpc_event_queue_task = tokio::task::spawn(async move {
    let mut rpc_event_queue = rpc_evt_queue.unwrap();
    while let Some(event) = rpc_event_queue.recv().await {
        tokio::task::spawn(async move {
            if let Err(error) = event.await {
                warn!("wamp rpc event error: {:?}", error);
            }
        });
    }
    warn!("wamp RPC event queue closed");
    // Do NOT call self.disconnect() here. If this channel closes it's
    // because the event_loop_task has already exited and will handle it.
});
```

### Change 4 — Delete the heartbeat task

Justification: redundant with the websocket keepalive Ping (configured at 20 s with missed-pong detection). The heartbeat publish was acting as both liveness probe *and* tear-down trigger; both jobs move to the supervisor.

**Before:**

```rust
let heartbeat_task = tokio::task::spawn(async move {
    let client = client_clone;
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        if let Err(error) = client.publish(&WampChannel::from("ping.heartbeat"), None, None, false).await {
            warn!("Wamp ping error: {:?}", error);
            break;
        }
    }
    self_clone2.disconnect().await
});
```

**After:** remove the task entirely. Remove `heartbeat_task.abort_handle()` from the `abort_handles` array.

If `ping.heartbeat` is consumed by some downstream observer (i.e. it has subscribers that rely on seeing it), keep the loop but:
* Remove the `break` — log the error and continue.
* Remove the `self_clone2.disconnect().await`.

### Change 5 — Remove `self.reconnect()` fallbacks from publish paths

**File:** same. Functions affected: `publish`, `publish_with_ack`.

**Before:**

```rust
if let Err(error) = result.wrap_err_with(|| format!("channel: {channel}")) {
    warn!("Failed to send message to: {channel}, error: {error:?}, will attempt to reconnect and publish again");
    self.reconnect().await?;
    self.client().await.publish(channel, args.clone(), kw_args.clone(), false).await?;
}
```

**After:**

```rust
result.wrap_err_with(|| format!("channel: {channel}"))?;
```

(Same shape for `publish_with_ack`, returning the `WampId`.)

Justification: `self.reconnect()` calls `disconnect()` + `connect()`, building a fresh `Client` and discarding the supervisor's auto-replay cache (realm join args, all subs, all regs). The `handle_retry` inner loop already covers the "racing a reconnect" window — requests queue in `ctl_channel` and the next attempt is processed by the post-replay event loop.

Keep `handle_retry` as-is.

### Change 6 — Guard `Conn::reconnect()` so it's caller-explicit only

**File:** same.

Either remove `Conn::reconnect()` entirely (preferred — the supervisor obsoletes it) **or** add a doc-comment marking it for explicit recovery use only:

```rust
/// Tear down and rebuild the WAMP client from scratch. Drops the
/// supervisor's auto-replay cache — call only after a terminal
/// `ReconnectEvent::GaveUp` or for an explicit operator-driven reset.
/// Do NOT call from publish/call retry paths; the supervisor handles
/// transient drops on its own.
pub async fn reconnect(&self) -> Result<&Self> { … }
```

### Change 7 — Surface reconnect events for observability

**File:** same, inside `connect()` after the `join_realm` succeeds and before returning.

```rust
if let Some(mut events) = client.take_reconnect_events().await {
    tokio::spawn(async move {
        use wamp_async::ReconnectEvent::*;
        while let Some(ev) = events.recv().await {
            match ev {
                Reconnecting { attempt, cause, delay } =>
                    warn!(target: "wamp.reconnect",
                          "attempt {attempt} in {delay:?} (cause: {cause})"),
                Reconnected =>
                    info!(target: "wamp.reconnect",
                          "transport + session re-established"),
                GaveUp { attempts, cause } =>
                    error!(target: "wamp.reconnect",
                           "supervisor gave up after {attempts} attempts: {cause}"),
            }
        }
    });
}
```

`take_reconnect_events` returns `None` if no policy is set or if it's already been taken — keep the `if let Some` guard so the helper still works if someone disables the policy.

## What you can delete

After the above:

* The 5-attempt outer connect loop and its `attempts`/`backoff` locals.
* The unused `_client_config` (the one prefixed with underscore).
* The `heartbeat_task` and its `abort_handle`.
* The `self.reconnect().await?` lines inside `publish` and `publish_with_ack` (the ones inside the `if let Err(error) = result.wrap_err_with…` block — keep `handle_retry`).
* Probably `Conn::reconnect()` itself.

## Verification

Code-level:
1. `cargo build` clean.
2. `cargo clippy` shows no new warnings about unused imports / unreachable code from the deletions.
3. The `connect()` method no longer references `attempts`, `max_attempts`, `backoff`, or `heartbeat_task`.

Behavioural — minimum smoke test against a router you control:

1. **Happy path** — connect, subscribe to a topic, publish to it, observe the event. Register an RPC, call it, observe the response.
2. **Transport drop with no work in flight** — `tcpkill` or restart the router. Observe in logs:
   * `Reconnecting attempt N in <duration>` from the new observer.
   * `Replay: re-bound sub <id> → server ID <new_id>` (debug-level).
   * `Replay: re-bound rpc <id> → server ID <new_id>` (debug-level).
   * `Reconnected — transport + session re-established`.
3. **Publish after a drop, with same `WampChannel`** — should succeed without the caller doing anything.
4. **Pre-existing subscription receiver** — events should resume flowing on the same `Subscription` handle after the router comes back. No need to re-subscribe.
5. **Pre-existing RPC registration** — call it from a second client after the router restart; the original closure should still service it.
6. **GaveUp path** — set `max_retries: Some(2)` temporarily, kill the router, observe `GaveUp` is logged and `event_loop_task` exits.

Edge cases worth eyeballing once:

* **In-flight `publish_with_ack` during a drop** — should resolve to `Err("Core never returned a response")` and the existing `handle_retry` loop should successfully retry. Make sure the retry actually fires (no silent drops).
* **Auth replay** — if `use_auth: true`, kill the router and confirm the cached CHALLENGE handler is re-invoked. The closure is `Arc`-shared internally so it must be safe to call many times (the current implementation reads `WAMP_CHALLENGE` env on each call, which is fine; nothing stateful inside it).

## Out of scope for this spec

* Reconnect against a router whose realm config changed between sessions (e.g. dropped topic permissions). Server-rejected replays prune themselves from the cache and the reconnect attempt is retried with backoff. If the rejection persists, you'll see the supervisor loop forever (with `max_retries: None`). Surface this via the `Reconnecting` log spam and decide operationally.
* Multi-realm support — this helper only ever joins `"realm1"`. If that changes, the cached realm args reflect whatever was used in the first successful `join_realm`.
* Session-id stability — the application never reads `client.session_id`. If something downstream depends on it (e.g. for correlation IDs), note that the supervisor's replay yields a *new* server-assigned session id and the helper currently doesn't expose it.
