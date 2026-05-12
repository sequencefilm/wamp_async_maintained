# Auto-reconnect handoff spec

How `wamp_async` reconnects after a transport drop, and what the **consumer of
this crate** still has to do to fully recover a working WAMP session.

## Two modes

The crate offers two recovery modes, chosen on `ReconnectPolicy`:

* **Transport-only (`auto_replay_session: false`, default)** — supervisor
  re-establishes the websocket; application is responsible for re-running
  HELLO / SUBSCRIBE / REGISTER on every `Reconnected` event. Use when you
  want explicit visibility into every reconnect (e.g. to reconcile caches,
  re-fetch state, decide what to re-subscribe to).
* **Full session replay (`auto_replay_session: true`)** — supervisor
  additionally re-runs the realm join (with cached credentials) and
  re-issues SUBSCRIBE for every cached topic + REGISTER for every cached
  RPC endpoint, transparently. User-facing subscription/registration IDs
  remain stable across the reconnect via internal aliases, so existing
  `SubscriptionQueue` receivers and `unsubscribe` / `unregister` calls keep
  working with no application bookkeeping.

## What the crate handles for you

Configured via `ClientConfig`:

```rust
let cfg = ClientConfig::default()
    // Enable websocket-level Ping keepalive. The interval should be well
    // under any idle-timeout on the path (load balancer, NAT, router).
    .set_keepalive_interval(Some(Duration::from_secs(20)))
    // Declare the connection dead after this many consecutive missed pongs.
    // Default 2. Set to 0 to disable.
    .set_keepalive_max_missed_pongs(2)
    // Enable transparent transport reconnect.
    .set_reconnect_policy(Some(ReconnectPolicy {
        max_retries: None,                              // forever
        initial_backoff: Duration::from_millis(500),
        max_backoff:     Duration::from_secs(30),
        backoff_multiplier: 2.0,
        // Opt-in to full session replay (realm + subs + regs). Defaults to
        // `false` for backwards compatibility.
        auto_replay_session: true,
    }));
```

With this configured, the supervisor will:

1. **Detect a dead connection** in either direction:
   * `recv()` errors (peer RST, TLS error, close frame, websocket error).
   * `send()` errors (e.g. a `publish` while the peer is already gone).
   * Keepalive Ping with no peer activity for `max_missed_pongs` intervals
     (catches half-open NAT / black-hole peers).

2. **Re-establish the underlying transport** with exponential backoff
   (`initial_backoff` → `max_backoff`), with ±20% jitter to avoid thundering
   herd against a recovering router.

3. **Keep the `Client` handle valid** across the reconnect. The
   `ctl_channel`, `rpc_event_queue`, and `reconnect_events` receivers stay
   stable, so existing references to the `Client` keep working.

4. **Emit lifecycle events** on the `reconnect_events` channel:
   * `Reconnecting { attempt, cause, delay }` before each attempt.
   * `Reconnected` once a fresh transport is up.
   * `GaveUp { attempts, cause }` if `max_retries` is exceeded.

## Mode A — Full session replay (`auto_replay_session: true`)

With this flag set, the supervisor itself runs HELLO / SUBSCRIBE / REGISTER
against the new transport before emitting `Reconnected`. The consumer's job
shrinks to:

* Spawn the event-loop future as usual.
* (Optional) Take `take_reconnect_events()` if you want to log
  reconnect attempts or surface `GaveUp` as a fatal error. Even if you do
  nothing with the events, recovery still happens.
* Make sure your registered RPC closures are safe to call across multiple
  connections (they're `Fn`, not `FnOnce`, so this is already the contract,
  but if you cache per-connection state inside the closure you'll want to
  invalidate it when a `Reconnected` event arrives).

What the supervisor replays automatically:

1. Realm join: the same realm name, roles, agent, auth method, auth id,
   `authextra`, and challenge handler from the **first** successful
   `join_realm[_with_authentication|_with_cryptosign]` call. The cached
   `AuthenticationChallengeHandler` is `Arc`-wrapped and re-invoked on
   every CHALLENGE, so the handler closure must be safe to call many
   times.
2. Every `client.subscribe(topic, …)` that succeeded, in the order it was
   first registered, with the *same client-facing `sub_id`*. Outstanding
   `SubscriptionQueue` receivers keep delivering events from the new
   connection without the application touching them.
3. Every `client.register(uri, …)` that succeeded, with the *same
   client-facing `rpc_id`*. Subsequent RPC calls dispatch to the cached
   closure on the new connection.

What is **not** replayed automatically (even in this mode):

* In-flight `call`/`publish(ack)`/`subscribe`/`register` requests that
  were outstanding at the moment of the drop. They resolve with `"Core
  never returned a response"`. Wrap user-visible RPCs in a retry layer if
  you need at-least-once semantics.
* `leave_realm` semantics: calling `leave_realm()` clears the cached
  session, so the next disconnect-and-reconnect cycle will rebuild only
  the transport, not the realm. (Equivalent to opting out of replay.)
* Subscriptions / registrations the server *rejects* during replay (e.g.
  permissions changed, topic policy changed). These are dropped from the
  cache and the application will need to re-issue them explicitly — and
  the replay attempt counts as a failed reconnect attempt and is retried
  with backoff.

## Mode B — Transport-only (`auto_replay_session: false`)

The WAMP **realm session** is per-connection. The supervisor does **not**
replay it. On `ReconnectEvent::Reconnected`, the consumer is responsible for:

1. **Reset the stale session state** on the `Client`:
   ```rust
   client.reset_session().await;
   ```
   Without this, `join_realm` will reject the call with `"Client already
   joined to a realm"` because the old `session_id` is still cached.

2. **Re-join the realm** (with the same credentials used initially):
   ```rust
   client.join_realm("realm1").await?;
   // or join_realm_with_authentication / join_realm_with_cryptosign
   ```

3. **Re-subscribe to every topic** the application cares about and rebuild
   the `SubscriptionQueue` receivers — the previous queues will stop
   producing because the subscription IDs are tied to the dead session.

4. **Re-register every RPC endpoint** under the same URIs.

5. **Replay or fail any in-flight requests** that were outstanding at the
   moment of the drop. Pending `call`, `subscribe`, `publish(ack)`,
   `register`, etc. will resolve with `"Core never returned a response"`
   when the oneshot is dropped on the dead `Core`. The consumer should
   treat that as "retry once we're reconnected" rather than "fatal."

## Recommended consumer skeleton (Mode A — auto-replay)

```rust
use wamp_async::{Client, ClientConfig, ReconnectPolicy};

let cfg = ClientConfig::default()
    .set_keepalive_interval(Some(std::time::Duration::from_secs(20)))
    .set_reconnect_policy(Some(ReconnectPolicy {
        auto_replay_session: true,
        ..Default::default()
    }));

let (mut client, (event_loop, rpc_queue)) =
    Client::connect("wss://router.example/ws", Some(cfg)).await?;

tokio::spawn(event_loop);
if let Some(mut rpc_queue) = rpc_queue {
    tokio::spawn(async move {
        while let Some(fut) = rpc_queue.recv().await { tokio::spawn(fut); }
    });
}

// First-time setup. After this point, transport drops are recovered
// automatically — no recovery code needed.
client.join_realm("realm1").await?;
let (_sub_id, mut events) = client.subscribe("topic.example").await?;
client.register("rpc.example", |args, kwargs| async move { Ok((args, kwargs)) }).await?;

// `events` keeps producing across reconnects with the same _sub_id.
```

## Recommended consumer skeleton (Mode B — transport-only)

```rust
use tokio::sync::mpsc::UnboundedReceiver;
use wamp_async::{Client, ClientConfig, ReconnectEvent, ReconnectPolicy};

async fn run_with_auto_reconnect() -> anyhow::Result<()> {
    let cfg = ClientConfig::default()
        .set_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .set_reconnect_policy(Some(ReconnectPolicy::default()));

    let (mut client, (event_loop, rpc_queue)) =
        Client::connect("wss://router.example/ws", Some(cfg)).await?;

    // Spawn the supervisor event loop.
    tokio::spawn(event_loop);

    // Spawn the RPC runner if you're a Callee.
    if let Some(mut rpc_queue) = rpc_queue {
        tokio::spawn(async move {
            while let Some(fut) = rpc_queue.recv().await {
                tokio::spawn(fut);
            }
        });
    }

    // Take the reconnect-event receiver BEFORE the first event fires.
    let reconnect_events = client.take_reconnect_events().await
        .expect("policy is set so events channel must exist");

    // First-time setup.
    initial_session_setup(&mut client).await?;

    // Drive recovery on every Reconnected.
    drive_recovery_loop(client, reconnect_events).await
}

async fn initial_session_setup(client: &mut Client<'_>) -> anyhow::Result<()> {
    client.join_realm("realm1").await?;
    // subscribe / register …
    Ok(())
}

async fn drive_recovery_loop(
    mut client: Client<'_>,
    mut events: UnboundedReceiver<ReconnectEvent>,
) -> anyhow::Result<()> {
    while let Some(ev) = events.recv().await {
        match ev {
            ReconnectEvent::Reconnecting { attempt, cause, delay } => {
                log::warn!(
                    "WAMP transport down (cause: {}); reconnect attempt {} in {:?}",
                    cause, attempt, delay
                );
            }
            ReconnectEvent::Reconnected => {
                log::info!("WAMP transport back up; re-establishing session");
                client.reset_session().await;
                // Re-run the same setup the first connection used.
                if let Err(e) = initial_session_setup(&mut client).await {
                    log::error!("Failed to re-establish session: {}", e);
                    // Choice: keep trying on next reconnect, or give up.
                }
            }
            ReconnectEvent::GaveUp { attempts, cause } => {
                anyhow::bail!(
                    "WAMP supervisor gave up after {} attempts: {}",
                    attempts, cause
                );
            }
        }
    }
    Ok(())
}
```

## Operational guidance

* **Idempotent setup.** `initial_session_setup` is called once at boot and
  again after every reconnect. Make it idempotent — assume nothing about
  prior subscription IDs or registrations.

* **Reconcile application state.** Anything you cached based on WAMP events
  (e.g. live device state from a topic) may be stale at the moment of
  reconnect. Decide whether to invalidate caches and re-fetch via RPC, or
  trust them and let the next event correct them.

* **Stop the world on `GaveUp`.** If the supervisor exhausts retries the
  event loop future resolves and the `Client` becomes dead. The consumer
  should surface this as a hard failure rather than silently swallowing it.

* **Caller serialisation around reconnect.** Calls to `client.call()`,
  `client.publish()`, etc., that race a reconnect will resolve with
  `"Core never returned a response"`. If your consumer wants
  retry-on-disconnect semantics on user-visible RPCs, gate calls behind a
  "session is healthy" flag that you flip in the recovery loop above.

* **Logs to watch.** The crate logs at these levels:
  * `warn!` on every transport failure (recv, send, keepalive timeout).
  * `info!` on every reconnect attempt and success.
  * `warn!` on `GaveUp`.

  If you're not seeing the original "reset by peer" anymore but want
  visibility, ensure your `env_logger` filter accepts `wamp_async=info` (or
  `=debug` for full message tracing).

## Known limitations

* Authenticated sessions (`join_realm_with_authentication`,
  `join_realm_with_cryptosign`) must re-run the full challenge handshake on
  every reconnect. The crate does not cache credentials. Pass the same
  closure shape to your `initial_session_setup` so the recovery path uses
  the same handler.

* If the supervisor reconnects while a `leave_realm()` is in flight, the
  GOODBYE may be sent on the newly-established connection before the new
  HELLO/WELCOME, which the peer will refuse. In practice you wouldn't call
  `leave_realm` mid-reconnect — but if you race shutdown and reconnect,
  prefer `client.disconnect()` (which sends `Request::Shutdown` to the
  supervisor and exits) over `leave_realm`.

* The TCP raw-socket transport (`tcp://`, `tcps://`) has an unrelated
  read-path bug (`Vec::with_capacity` + `read_exact` reads 0 bytes). Use
  the WebSocket transport (`ws://`, `wss://`) until that is fixed.
