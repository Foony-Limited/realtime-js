# Changelog

All notable changes to `@foony/realtime`. Format loosely follows
[Keep a Changelog](https://keepachangelog.com). Versions are semver.

## 0.13.0

### Added

- **REST client.** New `Rest` class for request/response access without holding a
  WebSocket open, aimed at backends, cron jobs, and serverless functions:
  `new Rest({ key })`, then `rest.channels.get(name).publish(...)`. Publishes are
  durable and identical to WebSocket publishes for subscribers, history, and
  billing.
- `channel.history(params)` on REST channels returns a `PaginatedResult` (newest
  first by default) with `next()`, `hasNext()`, and `isLast()` for paging through
  older messages. `channel.presence.get(params)` returns the current members,
  filterable by `clientId` and `connectionId`.
- `rest.auth.requestToken({ clientId, ttl, capability })` asks the service to mint
  a client JWT from your API key and returns `TokenDetails` with the expiry, so
  you can cache tokens instead of minting per connection.
- `rest.time()` returns the server clock in milliseconds for clock-skew checks.
- End-to-end encryption works on REST channels: pass the same `cipher` key to
  `rest.channels.get(name, { cipher })` and publishes are encrypted before they
  leave the process, history and presence data decrypted on read.
- Failed requests reject with `RestError`, carrying the same numeric `code`
  values as `ErrorCode` plus the HTTP `statusCode`.

## 0.12.1

### Changed

- Message delivery is now fully binary. Replaced the few remaining cases (e.g. message batching)
  that still used JSON.

### Removed

- Dropped the old message-id resume cursor. Reconnect resume uses the `seq` cursor only, which
  every durable message carries. A channel that has seen only unsequenced messages resubscribes
  fresh. No change for `ephemeral: true` messages, which were never resumable.

## 0.12.0

### Added

- **Frame coalescing.** The SDK now supports unpacking several frames in one WebSocket message,
  increasing throughput under load by ~3x or more in some cases.
- **Binary wire protocol.** Messages now travel in a compact binary format instead of JSON, which
  is up to 3x faster at high message rates. The edge still accepts older JSON clients for now, but
  that fallback will be removed in 0.13.0.
- **Binary batches.** Batched publishes (`channel.publish([...])` and auto-batching) now use the
  new binary format.

### Fixed

- **`ErrorCode` and `FrameType` now match the server.** Added the missing error codes `Capability`,
  `ChannelDenied`, `RateLimited`, and `Bootstrap` (so you can now detect throttling), and the
  `presSub`, `presUnsub`, `fetch`, and `fetchRes` frame types.

## 0.11.0

### Changed

- **Presence is now opt-in.** Subscribing to a channel's messages no longer starts presence.
  Register a presence listener (`channel.presence.subscribe(...)` or `channel.presence.on(...)`) to
  receive presence events, and the SDK stops them once the last listener is removed. **Breaking** if
  you relied on message `subscribe` also delivering presence events.

### Added

- **Automatic presence re-entry on reconnect.** After a reconnect the SDK re-enters presence and
  re-opens presence subscriptions for you. An explicit `presence.leave()` or `channel.detach()`
  stops that.
- **Stable connection id across reconnects.** A brief drop and quick reconnect no longer shows
  observers a presence leave followed by a re-enter.

## 0.10.0

### Added

- **Per-channel message serial (`seq`).** Delivered messages now carry a contiguous per-channel
  `seq`, used to detect gaps and to resume reliably.
- **Automatic gap backfill.** If a message is missed (a gap in `seq`), the channel fetches just the
  missing messages and fills them in. If the gap is too old to recover, it reports a discontinuity
  (`resumed: false`).

### Changed

- A publish `ack` may now include the assigned `seq`, so a publisher can track its own cursor.
- Reconnect resume now uses the `seq` cursor when available, falling back to the message-id cursor.
  Backward compatible with servers that predate `seq`.

## 0.9.0

### Added

- **Connection resume.** On reconnect, the channel replays messages published while you were briefly
  disconnected, so they are no longer lost. If the disconnect was too long to recover, the
  `attached` state reports a discontinuity (`resumed: false`) instead of silently skipping messages.
- **Ephemeral publishes.** `channel.publish(name, data, { ephemeral: true })` (also on batch
  publishes) delivers a message live to current subscribers but keeps it out of history and resume.
  Good for transient events like typing indicators, cursors, and reactions. Ephemeral publishes have
  significantly higher throughput and reduced latency, but are an at-most-once delivery guarantee.

### Fixed

- **Intermittent `1006` on connect.** A race between opening the socket and fetching the auth token
  could drop the connection at startup. Fixed, along with a socket leak when the token fetch throws.
- **Idle connections dropped with `1006`.** The SDK now sends a keep-alive ping every `keepAliveMs`,
  so idle connections are no longer culled by proxy idle timeouts (e.g. Cloudflare).
- **A channel whose attach failed is now retried.** A channel that failed to attach because the
  connection dropped stayed dead even after reconnecting. It now re-subscribes on reconnect, and
  stops only on a capability denial (403xx).

## 0.8.1

### Changed

- **`publish()` no longer attaches the channel.** A publish-only client (e.g. a server bridge) no
  longer accumulates a subscription per channel it publishes to, which could exhaust the
  connection's channel quota. Offline publishes are still buffered by `queueMessages`.
- **Disconnect reason is surfaced to connection listeners.** The `disconnected` event now carries an
  `Error` with the close code and server reason, so a credential problem is visible instead of
  failing silently.
- **Unrecoverable auth errors now go terminal instead of retrying forever.** A handshake rejected
  with `BadAuth` or `AuthExpired` keeps retrying only when an `authCallback` can mint a fresh
  credential. With a static `token` or `key` the connection goes to `failed`, carrying the auth
  error. A later `connect()` can still retry.

### Fixed

- **Reconnect no longer crashes the process on a handshake error.** A rejected handshake (e.g. an
  expired token) could throw from a WebSocket close and kill the host process under Node's `undici`.
  It now degrades to a normal failed-connect and reconnect.

## 0.8.0

### Added

- **Local JWT minting (`auth.createJwt`).** A backend holding a Realtime API key can mint a
  short-lived, capability-scoped token for a client without a network round-trip:
  `realtime.auth.createJwt({ capability, clientId, ttlMs })`. The browser returns it from
  `authCallback`. Exports `Auth`, `createJwt`, and the `Capability`, `CreateJwtParams`, and
  `CreateJwtOptions` types.

## 0.7.0

### Added

- **Exactly-once delivery.** Subscribers no longer see a duplicate when a publish is retried. The
  SDK deduplicates delivered messages by `(clientId, messageId)`.

## 0.6.0

### Changed

- **Auto-batching is always on, throttled by `intervalMs`.** Single `publish` calls are batched
  automatically per channel, raising max throughput. A publish sends right away unless a batch went
  out within the last `intervalMs` (default `10`), so only fast bursts are grouped. `BatchOptions`
  (`intervalMs`, `maxMessages`) can be used to configure this behavior. Array publishes and
  `batchPublish` are never buffered.

## 0.5.0

### Changed

- **Default endpoint is now `realtime.foony.io`.** `DEFAULT_REALTIME_ENDPOINT` (used when `endpoint`
  is omitted) changed to `realtime.foony.io`.

## 0.4.0

### Added

- **Message batching.** `channel.publish(messages[])` and `realtime.batchPublish({ channels,
  messages })` send several messages in one frame that the edge stores and dedups as a single
  message, lowering cost and raising throughput. Subscribers still receive the members individually.
- **Opt-in auto-batching.** `{ batch: { enabled, intervalMs, maxMessages } }` on the client sets a
  default, overridable via `channels.get(name, { batch })`. Single `publish` calls are buffered and
  flushed as one batch. Off by default. `channel.flush()` forces a flush.
- New exports: `BatchSpec`, `BatchMessage`, `BatchPublishResult`, `BatchMember`, `BatchOptions`.

### Changed

- Added an optional `messages` array (batch members) to `PublishFrame` and `MessageFrame`. Presence
  and encoding stay per-member for encrypted batches.

## 0.3.0

### Added

- **End-to-end channel encryption.** `channels.get(name, { cipher: { key } })` encrypts message and
  presence `data` client-side with AES-GCM (256-bit default), so the edge only ever sees ciphertext.
  The key is shared between clients out of band and never sent to the server. Only `data` is
  encrypted, not the event `name` or `clientId`. Exports `generateRandomKey`, `Cipher`, and the
  `CipherParams`, `CipherAlgorithm`, `EncryptResult`, and `ChannelOptions` types.

### Changed

- Added an optional `encoding` field to `PublishFrame`, `MessageFrame`, and the presence frames.
  Backward compatible, absent means plain JSON.

## 0.2.0

### Added

- **Per-message publish TTL.** `channel.publish(name, data, { ttlMs })` sets how long a message is
  kept for history, clamped to the app's plan ceiling. Adds `PublishFrame.ttlMs`.

## 0.1.0

### Added

- **Message history.** `channel.history({ limit, start })` returns recent messages, oldest-first,
  with backward pagination. Exports `HistoryFrame`.
