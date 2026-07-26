# Changelog

All notable changes to `@foony/realtime`. Format loosely follows
[Keep a Changelog](https://keepachangelog.com). Versions are semver.

## 0.15.2

### Fixed

- **An `authCallback` that never settles no longer hangs the connection.** The
  SDK now gives your callback 15 seconds to return a token, then fails the
  attempt and retries on the normal reconnect backoff. Before, a token fetch
  on an HTTP client with no timeout of its own (a dropped request that never
  rejects) left the client in `connecting` for the life of the page, with
  nothing scheduled to retry.

## 0.15.1

### Fixed

- **A connect attempt can no longer hang forever.** Every attempt now has a
  hard deadline covering the socket open AND the auth reply: 5 seconds while
  the long-polling fallback is available, 15 seconds on a forced transport.
  Before, a middlebox that admitted the WebSocket upgrade and then went
  silent could park the client in `connecting` forever, and a forced
  `'websocket'` client had no bound at all. On expiry the attempt fails
  normally, so auto mode falls back to long-polling and a forced transport
  lands in the reconnect backoff.
- **A network outage no longer demotes the client to long-polling for good.**
  In 0.15.0 any failed WebSocket attempt switched the client to long-polling
  permanently, so a wifi blip or a server deploy during a reconnect left it
  there for the life of the page. Now the switch only sticks while the
  WebSocket is actually blocked: if the long-polling attempt fails the same
  way, the next attempt is WebSocket again, and a client parked on
  long-polling re-probes the WebSocket about once a minute.

## 0.15.0

### Added

- **Automatic long-polling fallback.** When the WebSocket cannot be
  established, for example behind a corporate proxy that blocks upgrades, the
  client now falls back to HTTP long-polling and everything keeps working:
  publish, subscribe, presence, history, resume, and exactly-once delivery.
  The fallback kicks in within about 5 seconds and the client then stays on
  long-polling for its lifetime. A failure the server answered (like a bad
  token) never triggers the fallback.
- **New `transport` option** (`'auto'` default, `'websocket'`,
  `'long-polling'`) to force one transport, and a **`fetch` option** to
  override the HTTP client the long-polling transport uses, mostly for tests.

## 0.14.0

### Added

- **Dead connections are detected within seconds.** After each keep-alive ping
  the SDK now waits for proof of life (any inbound frame) and tears the link
  down when nothing arrives, letting the normal reconnect take over. Before, a
  half-dead connection sat in `connected` with publishes pending until TCP gave
  up minutes later.

### Removed

- **The `ttlMs` publish option is gone** (`channel.publish(name, data, { ttlMs })`,
  `PublishFrame.ttlMs`): the service no longer honors per-message TTLs, retention
  comes from the channel's namespace rule. `ephemeral: true` still works, and
  publishes that passed `ttlMs` now join auto-batching instead of being sent alone.
  The token-lifetime `ttlMs` on `createJwt` is unchanged.

### Changed

- **Breaking: history pages by serial instead of message id.**
  `channel.history({ before })` and REST `history({ before })` take the oldest
  message's serial (`seq` on realtime frames, `serial` on REST messages) and
  return only messages strictly below it. The `start` message-id cursor is
  gone. Serials are server-assigned and unique, so paging cannot loop or skip
  when a publisher reuses a message id, and deep scrollback into archived
  history is much faster.
- **`channels.get` validates channel names client-side** against the server's
  grammar (`A-Z a-z 0-9 : - _`, at most 255 characters, no leading `:`) and
  throws immediately. Before, a bad name attach-looped against `BadFrame`
  rejections forever.
- **`batchPublish` enforces the 1000-message limit on the merged per-channel
  batch**, not per spec, since specs naming the same channel merge into one
  batch.
- **`Cipher` throws when `algorithm` contradicts the key length.** A 16-byte
  key with `aes-256-gcm` used to silently run AES-128.

### Fixed

- **`close()` racing an in-flight `connect()` no longer crashes a Node process.**
  Tearing down a socket that was still connecting emitted an error event with no
  listener attached, which Node treats as an uncaught exception. Browsers were
  unaffected.
- **The `update` channel event fires now.** A discontinuity found while the channel
  stayed attached previously emitted nothing, so listeners never learned messages
  were lost. Check `resumed` on the payload and reload state or read history when
  it is `false`.
- **Requests no longer hang when the connection drops.** An attach, detach,
  history read, presence change, or gap-fill fetch in flight when the socket
  died stayed pending forever, and a hung gap-fill silently disabled gap
  healing for the channel's lifetime. They now reject, and the reconnect
  restores the channel.
- **`close()` during an in-flight `connect()` no longer resurrects the
  connection.** The handshake used to complete anyway, leaving a live socket
  delivering messages while the client reported `closed`.
- **No duplicate subscribe on the first connect.** Subscribing before
  `connect()` sent the channel's `sub` twice, and the second ack surfaced a
  spurious `update` with `resumed: false` (a false discontinuity signal).
- **A failed `authCallback` before a socket exists retries now.** It used to
  wedge the state at `connecting` forever even with `autoReconnect` on.
- **A stale socket's close event can no longer tear down a newer connection.**
- **Frames coalesced into the same WebSocket message as `connected` are
  delivered** instead of silently dropped.
- **Released channels are fully released.** `channels.release` left the old
  instance listening to connection state forever, retaining it and its dedup
  cache and keeping its state machine running.
- **Encrypted channels deliver in arrival order.** A payload-less event (a
  presence `leave`, a plaintext message) could overtake an encrypted one still
  being decrypted, so an enter-then-leave pair could be observed as
  leave-then-enter.
- **The presence watcher closes when the last listener leaves via `once()` or
  `off()`.** Before, only the unsubscribe function returned by `on()` released
  it, so a lone `presence.once(...)` held the watcher open forever.
- **A detach racing a fresh attach no longer erases the subscription.** The
  channel looked attached but was missing from the reconnect restore set, so it
  silently received nothing after the next drop.
- **`channel.history({ start })` accepts batch-member message ids.** The
  `<batchId>:<index>` member suffix is stripped to the stored record's id.
  Before, the server did not know the id and silently restarted paging from the
  newest page.
- **REST history and presence no longer fail the whole page when one message is
  undecryptable.** The unreadable item comes back undecoded with its `encoding`
  intact, as the field docs always promised.
- **`Rest.auth.requestToken` rejects a malformed API key** with a clear error
  instead of building a mangled URL and surfacing a confusing 401.
- **`off()` removes `once()` listeners.** One-shot listeners used to be
  registered under a hidden wrapper, so removing them by identity was
  impossible. This also means removing a pending `presence.once(...)` releases
  the watcher it was holding open.

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

### Removed

- Dropped the JSON WebSocket protocol, as announced in 0.12.0. The SDK and the edge
  now speak only the binary format that 0.12.0 introduced. SDK versions before
  0.12.0 can no longer connect.

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
