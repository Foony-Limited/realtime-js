#!/usr/bin/env node
/**
 * Smoke test: drives the SDK against a running edge binary.
 *
 * Prerequisites:
 *   - The realtime backend running locally (see services/realtime-saas/README.md)
 *   - Edge binary running with `JWT_SIGNING_KEY=local-dev-key go run ./cmd/edge`
 *
 * Run:
 *   node examples/smoke.mjs
 *
 * Two clients exchange a chat message and a presence transition.
 */
import { WebSocket } from 'ws';
import { Realtime } from '../lib/index.js';
import { mintRealtimeToken } from '../lib/server.js';

const url = process.env.FOONY_REALTIME_URL ?? 'ws://localhost:3000';
const signingKey = process.env.JWT_SIGNING_KEY ?? 'local-dev-key';
const appId = process.env.FOONY_REALTIME_APP_ID ?? 'foony-dev';

const aliceToken = mintRealtimeToken({ signingKey, appId, clientId: 'alice' });
const bobToken = mintRealtimeToken({ signingKey, appId, clientId: 'bob' });

const alice = new Realtime({ url, token: aliceToken, webSocket: WebSocket, autoReconnect: false });
const bob = new Realtime({ url, token: bobToken, webSocket: WebSocket, autoReconnect: false });

await Promise.all([alice.connect(), bob.connect()]);
console.log('connected', { alice: alice.getConnectionId(), bob: bob.getConnectionId() });

const channelName = 'demo.1';
const aliceChannel = alice.channels.get(channelName);
const bobChannel = bob.channels.get(channelName);

const messagesReceivedByAlice = [];
const presenceSeenByBob = [];
aliceChannel.subscribe((message) => messagesReceivedByAlice.push(message));
bobChannel.presence.subscribe((event) => presenceSeenByBob.push(event));

await Promise.all([aliceChannel.attach(), bobChannel.attach()]);

await aliceChannel.presence.enter({ status: 'typing' });
await bobChannel.publish('chat', { text: 'hi alice' });

await waitFor(() => messagesReceivedByAlice.length >= 1 && presenceSeenByBob.length >= 1, 'cross-client traffic');

console.log('alice received', messagesReceivedByAlice[0]);
console.log('bob saw presence', presenceSeenByBob[0]);

await Promise.all([alice.close(), bob.close()]);
console.log('OK');
process.exit(0);

async function waitFor(predicate, label) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`timed out waiting for ${label}`);
}
