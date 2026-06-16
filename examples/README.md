# @foony/realtime examples

Runnable **client** and **server** examples for the SDK. For development convenience, these are wired to `../src`. For actual production code,
you should use the `@foony/realtime` SDK on npm.

Both examples authenticate with a **Realtime API key** (`foony.kid_...:sk_...`) and connect to the
prod edge (`wss://realtime.foony.com`).

```
examples/
  client/        React + Vite browser playground (connect, subscribe, publish, presence)
  server/
    bot.ts       Node SDK client: subscribes + announces presence + publishes a heartbeat
  smoke.mjs      pre-existing end-to-end smoke test (drives the compiled lib/)
```

## Setup

```bash
cd sdks/realtime-js/examples
npm install
```

## Browser playground

```bash
npm run client     # Vite dev server on http://localhost:5180
```

In the UI:

- **Connection** — set a client id, paste your API key (`foony.kid_...:sk_...`), and *Connect*.
- **Channel** — pick a channel name and *Use channel* (eagerly attaches; watch the state badge).
- **Messages** — *Subscribe* to all messages or a comma-separated list of event names, then publish
  JSON payloads under any event name.
- **Presence** — *Enter* / *Update* / *Leave* with a JSON payload; the member list and presence log
  update from the presence stream.

Open two browser tabs (or run the bot) to watch messages and presence flow between members.

> Auth note: these examples use the API key directly, which is fine for trusted contexts. In a real
> browser app you'd keep the key server-side and hand the client a short-lived JWT (the SDK's
> `authCallback` option) — the `key` is shown here only to keep the example self-contained.

## Node bot

A second participant you can run from the terminal:

```bash
REALTIME_KEY="foony.kid_...:sk_..." CHANNEL=chat:site:- npm run bot
```

See the env var docs at the top of `server/bot.ts`.
