/**
 * Server-side SDK example: a "bot" that connects, subscribes, announces presence, and publishes a
 * heartbeat message on an interval. Use it as the second participant while you drive the browser
 * client — you'll see its messages arrive in the client, and the client's messages logged here.
 *
 * Demonstrates the SDK used from Node (server-side) with API key auth — the standard way a trusted
 * server connects. Authenticates with a raw API key (`key` option); connects to the prod edge
 * (wss://realtime.foony.com).
 *
 * Run: `REALTIME_KEY="foony.kid_...:sk_..." npm run bot` (from the examples/ directory).
 * Env:
 *   REALTIME_KEY      API key in `appSlug.publicKeyId:privateKey` form (required)
 *   CLIENT_ID         This bot's client id (default "bot")
 *   CHANNEL           Channel to join (default "demo:lobby")
 *   MESSAGE_NAME      Event name to publish under (default "chat.message")
 *   INTERVAL_MS       Heartbeat publish interval (default 3000)
 */
import {Realtime} from '../../src/index.js';

const apiKey = process.env.REALTIME_KEY;
if (!apiKey) {
  throw new Error('bot: set REALTIME_KEY to a "foony.kid_...:sk_..." API key');
}
const clientId = process.env.CLIENT_ID ?? 'bot';
const channelName = process.env.CHANNEL ?? 'demo:lobby';
const messageName = process.env.MESSAGE_NAME ?? 'chat.message';
const intervalMs = Number(process.env.INTERVAL_MS ?? 3000);

console.log(`[bot] connecting with API key, channel=${channelName}`);
const realtime = new Realtime({clientId, key: apiKey, batch: {intervalMs: 25}});
realtime.connection.on((state, reason) => {
  console.log(`[bot] connection -> ${state}${reason ? ` (${reason.message})` : ''}`);
});

await realtime.connect();
console.log(`[bot] connected: connectionId=${realtime.getConnectionId()} clientId=${realtime.getClientId()}`);

const channel = realtime.channels.get(channelName);
channel.subscribe((message) => {
  // When the payload carries a numeric sentAt, show end-to-end latency.
  const sentAt = (message.data as {sentAt?: unknown} | null)?.sentAt;
  const delta = typeof sentAt === 'number' ? ` (+${Math.abs(Date.now() - sentAt)}ms)` : '';
  console.log(`[bot] message ${message.name} from ${message.clientId ?? '?'}${delta}:`, message.data);
});
channel.presence.subscribe((event) => {
  console.log(`[bot] presence ${event.action} ${event.clientId}:`, event.data ?? '');
});
await channel.presence.enter({name: clientId, role: 'bot'});

let count = 1;
const timer = setInterval(() => {
  const body = `heartbeat ${count++} from ${clientId}`;
  channel.publish(messageName, {body, sentAt: Date.now()}).catch((error) => {
    console.error('[bot] publish failed:', error);
  });
  console.log(`[bot] published: ${body}`);
}, intervalMs);

// Leave presence and close cleanly so other members see us depart.
async function shutdown() {
  clearInterval(timer);
  console.log('[bot] shutting down...');
  try {
    await channel.presence.leave();
  } finally {
    await realtime.close();
    process.exit(0);
  }
}
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());
