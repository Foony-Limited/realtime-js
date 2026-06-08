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
  url: 'wss://realtime.foony.com',
  authCallback: async () => {
    const response = await fetch('/api/realtime/token');
    return await response.text();
  },
});

const channel = realtime.channels.get('chat:room-1');

channel.subscribe((message) => {
  console.log('chat message:', message.data);
});

await channel.publish('chat', { text: 'hello world' });

channel.presence.subscribe((event) => {
  console.log(event.action, event.clientId, event.data);
});
await channel.presence.enter({ name: 'Alice' });
```

### Node / server (token minting)

```ts
import { mintRealtimeToken } from '@foony/realtime/server';

app.get('/api/realtime/token', (req, res) => {
  const token = mintRealtimeToken({
    signingKey: process.env.REALTIME_JWT_SIGNING_KEY!,
    appId: 'foony',
    clientId: req.user.id,
    capability: '{"chat:*":["subscribe","publish","presence"]}',
    ttlMs: 15 * 60 * 1000,
  });
  res.type('text/plain').send(token);
});
```

The signing key must exactly match the `JWT_SIGNING_KEY` env var the
realtime edge binary boots with.

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
  url: 'ws://localhost:3000',
  token: process.env.FOONY_REALTIME_DEV_TOKEN!,
});
```

## Channel names

Channel names must match `[A-Za-z0-9._-]{1,255}` and cannot start or
end with a `.`. Use dots to express hierarchy (`chat.rooms.42`). The
server rejects invalid names with error code `40001` (`BadFrame`).

## API surface

- `Realtime` — top-level client. Owns the WebSocket; channels attach lazily.
- `client.channels.get(name)` — returns a stable `Channel` for that name.
- `channel.subscribe(fn)` — message listener; returns an unsubscribe fn.
- `channel.publish(name, data)` — publish one message; resolves on ack.
- `channel.presence.subscribe(fn)` — presence listener.
- `channel.presence.enter|update|leave(data?)` — mutate this connection's membership.
- `client.onStateChange(fn)` — observe `connecting | connected | disconnected | closed | failed`.

## Reconnect

When the connection drops unexpectedly the client retries with
exponential backoff (1s, 2s, 4s, ..., capped at 30s). All
subscriptions that were established before the disconnect are
re-issued automatically; presence membership is NOT automatically
restored — call `enter()` again on the `disconnected -> connected`
transition if you need it.

Pass `autoReconnect: false` to disable retries entirely (useful in tests).

## Tests

```bash
npm test
```

Runs unit tests (wire + token mint) plus an in-process end-to-end test
that drives the SDK against a fake edge built on `ws`. No external
services required.
