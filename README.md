# @foony/realtime

TypeScript SDK for the Foony Realtime service. A small client for the
wire protocol implemented by `services/realtime-saas` — connect, sub /
unsub, publish, and presence.

## Install

```bash
npm install @foony/realtime
```

The package ships compiled ESM output and TypeScript declarations.

## Quick start

### Browser / Foony client

```ts
import { Realtime } from '@foony/realtime';

const realtime = new Realtime({
  authCallback: async () => {
    const response = await fetch('/api/realtime/token');
    return await response.text();
  },
});

const channel = realtime.channels.get('chat:room-1');

channel.on((message) => {
  console.log('chat message:', message.data);
});

await channel.publish('chat', { text: 'hello world' });

channel.presence.on((event) => {
  console.log(event.action, event.clientId, event.data);
});
await channel.presence.enter({ name: 'Alice' });
```

### Node / server (browser auth)

Browser clients should fetch a short-lived JWT from your backend via the SDK's `authCallback`
option. Your backend obtains that JWT by exchanging its Realtime API key at the service's
`POST /auth/token` endpoint — the signing key never leaves Foony's infrastructure.

## Local development against the realtime backend

Start the backend following `services/realtime-saas/README.md`. Then
mint a dev token:

```bash
cd services/realtime-saas
JWT_SIGNING_KEY=local-dev-key go run ./cmd/devtoken -app foony -client alice
```

Use the printed token in the SDK:

```ts
const realtime = new Realtime({
  endpoint: 'ws://localhost:3000',
  token: process.env.FOONY_REALTIME_DEV_TOKEN!,
});
```

Omit `endpoint` in production to use `wss://realtime.foony.com`.

## Channel names

Channel names must match `[A-Za-z0-9._-]{1,255}` and cannot start or
end with a `.`. Use dots to express hierarchy (`chat.rooms.42`). The
server rejects invalid names with error code `40001` (`BadFrame`).

## API surface

- `Realtime` — top-level client. Owns the WebSocket; channels attach lazily.
- `client.channels.get(name)` — returns a stable `Channel` for that name.
- `channel.on(fn)` — message listener; returns an unsubscribe fn.
- `channel.on(name, fn)` — message listener for one message name.
- `channel.publish(name, data)` — publish one message; resolves on ack.
- `channel.presence.on(fn)` — presence listener.
- `channel.presence.enter|update|leave(data?)` — mutate this connection's membership.
- `client.connection.on(fn)` — observe all connection events.
- `client.connection.on('connected', fn)` — observe one connection event.
- `client.connection.off()` — remove all connection listeners.
- `client.connection.once('connected')` — await the next matching connection event.

## Reconnect

When the connection drops unexpectedly the client retries with
exponential backoff (1s, 2s, 4s, ..., capped at 30s). All
subscriptions that were established before the disconnect are
re-issued automatically; presence membership is NOT automatically
restored — call `enter()` again on the `disconnected -> connected`
transition if you need it.

Pass `autoReconnect: false` to disable retries entirely (useful in tests).

Publishes made while the connection is establishing or temporarily down are
queued locally and flushed on the next successful (re)connect — so a publish
during a brief blip resolves rather than rejects. A publish that was already in
flight when the connection dropped is resent on reconnect too. Every publish
carries a stable client-assigned id, so the server collapses any duplicate that
a resend would otherwise create (exactly-once). Pass `queueMessages: false` to
disable buffering/resend and reject such publishes immediately.

## Tests

```bash
npm test
```

Runs wire unit tests plus an in-process end-to-end test that drives the
SDK against a fake edge built on `ws`. No external services required.

## License

[Apache-2.0](./LICENSE) © Foony Limited
