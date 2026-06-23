# Changelog

All notable changes to `@foony/realtime`. Format loosely follows
[Keep a Changelog](https://keepachangelog.com); versions are semver.

## 0.8.1

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
