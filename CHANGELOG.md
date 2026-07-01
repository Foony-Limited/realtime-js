# Changelog

All notable changes to `@foony/realtime`. Format loosely follows
[Keep a Changelog](https://keepachangelog.com); versions are semver.

## 0.12.0

### Added

- **Frame coalescing.** The client now tells the server (via a `coalesce` flag on the auth
  handshake) that it can decode a WebSocket message carrying several frames joined by `'\n'`,
  and it splits incoming messages accordingly. Under load the server packs many acks and
  deliveries into one message and one socket write, a large edge-CPU saving at high message
  rates. Purely additive and backward compatible: an older client that does not send the flag
  keeps receiving one frame per message.
- **Binary message delivery.** The client now advertises `binaryDelivery` on the auth
  handshake, and the server delivers single messages in a compact length-prefixed binary form
  on the WebSocket binary opcode instead of JSON. The SDK decodes them back into identical
  message events. Encoding a delivered message this way is several times cheaper for the edge
  than `JSON.stringify`, which raises fan-out throughput at high message rates. Purely additive
  and backward compatible: an older client that does not send the flag keeps receiving JSON
  message frames. Server-coalesced bundles are delivered in binary too (a bundle record is just
  the member records concatenated) and unwrapped as before; client batch publishes still arrive
  as JSON for now.

### Fixed

- **`ErrorCode` and `FrameType` now match the server.** The SDK's wire tables had drifted
  from the canonical Go definitions. `ErrorCode` was missing `Capability` (40301),
  `ChannelDenied` (40302), `RateLimited` (42900), and `Bootstrap` (50001) — most importantly
  `RateLimited`, so an app could not cleanly tell throttling apart from other errors on an
  `err` frame. The `FrameType` discriminator union was missing `presSub`, `presUnsub`,
  `fetch`, and `fetchRes` (the frame shapes themselves were already present). Purely additive,
  so existing code keeps working.

## 0.11.0

### Changed

- **Presence is now opt-in.** Subscribing to a channel's messages no longer also starts
  presence. To receive presence events, register a presence listener
  (`channel.presence.subscribe(...)` or `channel.presence.on(...)`); only then does the SDK
  ask the server to deliver presence on that channel, and it stops again once the last
  presence listener is removed. A channel used only for messages opens no presence machinery,
  which makes presence close to free for apps that don't use it. This matches Ably, where
  receiving presence events is a separate, explicit step. **Breaking** if you relied on a
  message `subscribe` implicitly delivering presence events. Requires an edge that understands
  the new `presSub` / `presUnsub` frames.

### Added

- **Automatic presence re-entry on reconnect.** If this connection has entered presence
  (`presence.enter` / `presence.update`), the SDK re-enters it automatically after a
  reconnect, and re-opens any presence subscriptions, so a dropped connection heals without
  the app re-entering by hand. This matches Ably's behavior. An explicit `presence.leave()` or
  `channel.detach()` stops the automatic re-entry.
- **Stable connection id across reconnects.** On reconnect the SDK now asks the server to
  reuse its previous connection id (`resumeConnectionId` on the auth frame). Combined with a
  server-side grace window, a brief drop and quick reconnect no longer makes observers see a
  presence leave followed by a re-enter — the member simply stays present. Requires an edge
  that honors the reclaim; older edges just assign a fresh id as before.

## 0.10.0

### Added

- **Per-channel serial cursor + gap detection.** Durable messages now carry a contiguous
  per-channel `seq` (serial). This is used for ensuring delivery of all messages for a
  channel, and is preferred over the (now-deprecated) message-id cursor. This enables us
  to safely provide migrations for apps on our backend from one geographic location to
  another. It also allows us to have cheaper, more efficient channel subscribers.
- **Automatic gap backfill (surgical).** If a message arrives with a serial beyond the next
  expected one (a message was dropped to a briefly-slow consumer), the channel issues a small
  `fetch` for just the messages after its contiguous cursor and applies them. The existing
  `(clientId, messageId)` dedup removes any overlap with the live tail. If the cursor has
  aged out of retention, the server reports a discontinuity (`resumed: false`) and the channel
  re-baselines.

### Changed

- A publish `ack` may now carry the assigned `seq` so a publisher can track its own cursor.
- Resume on reconnect now sends the serial cursor when available, falling back to the message-id
  cursor for channels that have only seen unsequenced (ephemeral/retained) messages. Fully
  backward compatible: against a server that predates serials, no `seq` is ever seen, so the
  message-id resume path is used unchanged.

## 0.9.0

### Added

- **Connection resume.** On reconnect (and on re-attach) a channel now sends the id
  of the last message it delivered as a resume cursor, and the edge replays everything
  published after it before resuming the live tail — so messages published while a
  client was briefly disconnected are no longer lost. The cursor advances monotonically
  and the existing `(clientId, messageId)` dedup removes any overlap with the live tail.
  When the cursor has aged out of the server's retention window, the (re)attach surfaces
  a **discontinuity** (`resumed: false` in the channel's `attached` state change) instead
  of silently resuming with a gap; within the window it reports `resumed: true`. On a
  reconnect a suspended channel now passes briefly through `attaching` until the resume
  ack arrives, rather than optimistically claiming `resumed: true`.
- **Per-message ephemeral publishes.** `channel.publish(name, data, { ephemeral: true })`
  (also valid on a batch publish) marks a message fire-and-forget: it is delivered live to
  current subscribers — flagged `ephemeral` on the delivered message — but is excluded from
  history and connection-resume, and never advances the resume cursor. Lets transient events
  (typing indicators, cursors, reactions) ride a channel that otherwise persists, without
  polluting its history.

### Fixed

- **Intermittent `1006` on connect (handshake race).** The connect path created the
  WebSocket and then `await`ed `authCallback` (a token fetch) *before* attaching the
  `open` listener, so a fast upgrade racing a slow token fetch lost the `open` event — the
  auth frame was never sent and the connection hung until it was dropped. The auth frame
  is now built before the socket is opened (with a `readyState` guard for a synchronously
  opening WebSocket), so it is always sent. Also fixes a socket leak when the token fetch threw.
- **Idle connections dropped with `1006` (no keep-alive).** The SDK never sent pings, so a
  connection with no subscriptions or traffic was culled by intermediary WebSocket idle
  timeouts (e.g. Cloudflare). It now sends a ping every server-advertised `keepAliveMs`.
- **A channel whose attach failed was orphaned.** A subscription was remembered only on
  attach *success*, so a channel whose attach failed because the connection dropped was
  never re-subscribed on reconnect (it stayed dead even on a healthy new connection). The
  intent is now remembered up front and recovered on reconnect; only a terminal capability
  denial (403xx) stops retrying and surfaces `failed`.

## 0.8.1

### Changed

- **`publish()` no longer attaches the channel.** Previously every publish
  implicitly subscribed the publisher to the channel so it would also receive
  live messages. A publish-only client (e.g. a server bridge) thus accumulated
  one server-side subscription per channel it published to, and could exhaust a
  connection's active-channel quota. Offline publishes are still buffered and
  resent by `queueMessages`, and are unaffected by this change.

### Fixed

- **Reconnect no longer crashes the process on a handshake error.** When the
  server rejected the handshake (e.g. an expired token surfaced as an `err`
  frame on reconnect), the SDK closed the socket with reserved WebSocket code
  `1002`. Node's `undici` WebSocket follows the spec strictly and threw
  `InvalidAccessError: invalid code`. Thrown from inside a `message` listener,
  it escaped to `process.nextTick` and killed the host process. Handshake
  teardown now uses an app-specific close code (`4001`) and swallows any
  `close()` error, so a rejected handshake degrades to a normal
  failed-connect/reconnect instead of a crash.

### Changed

- **Disconnect reason is now surfaced to connection listeners.** The
  `disconnected` state event now carries an `Error` describing the close (code
  and server reason), so a credential problem the reconnect loop can't fix on
  its own is visible instead of looping silently. A failed `close()` during
  teardown is also logged via `console.error` rather than swallowed.
- **Unrecoverable auth errors now go terminal instead of retrying forever.** A
  handshake rejected with an auth error (`BadAuth` / `AuthExpired`) only keeps
  retrying when an `authCallback` can mint a fresh credential on the next
  attempt. With a static `token` or `key`, the same credential would be re-sent
  and rejected identically, so the connection now enters the terminal `failed`
  state (carrying the auth error) rather than looping. A later explicit
  `connect()` can still retry. Non-auth handshake errors keep retrying as
  before.

## 0.8.0

### Added

- **`auth.createJwt` — local JWT minting.** A trusted backend holding a
  Realtime API key can now mint a short-lived, capability-scoped token for a
  less-trusted client without a network round-trip: `realtime.auth.createJwt({
  capability, clientId, ttlMs })` (or the standalone `createJwt(params, { key })`)
  signs an HS256 token locally with the key secret. The token's `kid` header is
  the public key name so the edge can look up the secret to verify it; the payload
  carries only `sub`/`cap`/`iat`/`exp` — no secret material. The browser returns
  the token from its `authCallback` and the edge verifies it on the handshake.
  Exports: `Auth`, `createJwt`, and the `Capability` / `CreateJwtParams` /
  `CreateJwtOptions` types.

## 0.7.0

### Added

- **Transparent server-coalesced bundles + delivered-message dedup.** The edge
  may now pack independent publishes on a channel (across clients) into one stream
  record — an "envelope of envelopes" — to raise server-side throughput. The SDK
  unwraps these bundles automatically, so subscribers still receive individual
  messages, and it now **deduplicates delivered messages by `(clientId,
  messageId)`**: a publisher retry never surfaces a message to a subscriber twice.
  This preserves exactly-once *delivery* as a system-wide property even though the
  publish path is at-least-once. Dedup is keyed on the server-stamped `clientId`,
  so one client cannot suppress another's message by reusing an id. No API change.

## 0.6.0

### Changed

- **Auto-batching is always on, throttled by `intervalMs`.** Single `publish`
  calls are batched automatically on every channel, which greatly raises the max
  throughput per channel. A publish is sent right away unless a batch went out in
  the last `intervalMs` (default `10`). When one did, the publish waits until the
  window is up, so only fast bursts get grouped into one batch. Publishes spaced
  further apart than `intervalMs` are never batched together. `BatchOptions`
  (`intervalMs` and `maxMessages`) lets you adjust this. Array publishes and
  `batchPublish` are never buffered.

## 0.5.0

### Changed

- **Default endpoint moved to `realtime.foony.io`.** The Realtime SaaS now lives on
  `foony.io`, so `DEFAULT_REALTIME_ENDPOINT` (used when `endpoint` is
  omitted) has been changed to `realtime.foony.io`.

## 0.4.0

### Added

- **Message batching.** `channel.publish(messages[])` and 
  `realtime.batchPublish({ channels, messages })` bundle multiple messages into one
  frame that the edge stores and dedups as a single message, reducing costs and
  increasing maximum throughput. Subscribers still process the members individually
  (same reduced cost, and no DevEx overhead).
- **Opt-in auto-batching.** `{ batch: { enabled, intervalMs, maxMessages } }` on
  the `Realtime` client sets a default that can be overridden via
  `channels.get(name, { batch })`). Single `publish` calls are buffered and
  flushed as one batch (`intervalMs: 0` coalesces same-tick bursts). Disabled by
  default. `channel.flush()` forces a flush.
- New exports: `BatchSpec`, `BatchMessage`, `BatchPublishResult`, `BatchMember`,
  `BatchOptions`.

### Changed

- `PublishFrame`/`MessageFrame` gained an optional `messages` array (batch
  members), and presence/encoding stay per-member for encrypted batches.

## 0.3.0

### Added

- **End-to-end channel encryption.** `channels.get(name, { cipher: { key } })`
  encrypts the `data` of both messages and presence client-side with AES-GCM
  (256-bit default), so the realtime edge only ever sees ciphertext. The key is
  shared between clients out of band and never sent to the server. Encrypted
  payloads are tagged with a transport `encoding` (e.g.
  `cipher+aes-256-gcm/base64`, HTTP `Content-Encoding`-style) that the edge
  forwards opaquely; the IV is prepended to the ciphertext. New exports:
  `generateRandomKey`, `Cipher`, and the `CipherParams`, `CipherAlgorithm`,
  `EncryptResult`, and `ChannelOptions` types. Only `data` is encrypted; the
  event `name` and `clientId` stay in clear.

### Changed

- `PublishFrame`/`MessageFrame` and the presence frames gained an optional
  `encoding` field (backward-compatible; absent means plain JSON).

## 0.2.0

### Added

- **Per-message publish TTL.** `channel.publish(name, data, { ttlMs })` requests
  how long a message is retained for history; the edge clamps it to the app's
  plan ceiling. `PublishFrame.ttlMs` added.

## 0.1.0

### Added

- **Message history.** `channel.history({ limit, start })` returns recent
  messages, oldest-first, with backward pagination. `HistoryFrame` exported.
