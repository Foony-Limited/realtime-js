# Changelog

All notable changes to `@foony/realtime`. Format loosely follows
[Keep a Changelog](https://keepachangelog.com); versions are semver.

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
  event `name` and `clientId` stay in clear (matching Ably).

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
