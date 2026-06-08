/**
 * Connection-level test. Stands up an in-process WebSocketServer (from
 * `ws`) — installed transitively via vitest — and drives the SDK end
 * to end against a hand-rolled fake edge. We do NOT test against the
 * Go edge here; that's covered by the integration tests in
 * `services/realtime-saas`.
 */

import { AddressInfo } from 'node:net';
import { WebSocket as NodeWebSocket, WebSocketServer } from 'ws';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Realtime } from './index.js';
import type { AuthFrame, ClientFrame, ServerFrame } from './wire.js';

type Harness = {
  readonly server: WebSocketServer;
  readonly url: string;
  /** All client connections opened against the fake edge. */
  readonly sockets: NodeWebSocket[];
  readonly authFrames: AuthFrame[];
};

async function startFakeEdge(): Promise<Harness> {
  const server = new WebSocketServer({ port: 0 });
  await new Promise<void>((resolve) => server.once('listening', resolve));
  const address = server.address() as AddressInfo;
  const sockets: NodeWebSocket[] = [];
  const authFrames: AuthFrame[] = [];
  server.on('connection', (socket) => {
    sockets.push(socket);
    let nextConnIndex = sockets.length;
    socket.on('message', (raw) => {
      const frame = JSON.parse(raw.toString()) as ClientFrame;
      if (frame.t === 'auth') {
        const auth = frame as AuthFrame;
        authFrames.push(auth);
        if (auth.token === 'BAD') {
          const err: ServerFrame = { t: 'err', code: 40101, message: 'bad token' };
          socket.send(JSON.stringify(err));
          socket.close(1002, 'bad auth');
          return;
        }
        const connected: ServerFrame = {
          t: 'connected',
          connectionId: `conn-${nextConnIndex}`,
          keepAliveMs: 30_000,
          clientId: 'alice',
        };
        socket.send(JSON.stringify(connected));
        return;
      }
      if (frame.t === 'sub' || frame.t === 'unsub' || frame.t === 'pub' || frame.t === 'pres') {
        const ack: ServerFrame = { t: 'ack', id: frame.id };
        socket.send(JSON.stringify(ack));
        if (frame.t === 'pub') {
          const msg: ServerFrame = {
            t: 'msg',
            channel: frame.channel,
            name: frame.name,
            data: frame.data,
            messageId: 'msg-1',
            timestamp: Date.now(),
            clientId: 'alice',
          };
          socket.send(JSON.stringify(msg));
        }
        if (frame.t === 'pres') {
          const evt: ServerFrame = {
            t: 'presEvt',
            channel: frame.channel,
            action: frame.action,
            clientId: 'alice',
            connectionId: `conn-${nextConnIndex}`,
            timestamp: Date.now(),
            ...((frame as { data?: unknown }).data === undefined ? {} : { data: (frame as { data?: unknown }).data }),
          };
          socket.send(JSON.stringify(evt));
        }
      }
    });
  });
  return { authFrames, server, url: `ws://127.0.0.1:${address.port}`, sockets };
}

describe('Connection end-to-end (fake edge)', () => {
  let harness: Harness;

  beforeEach(async () => {
    harness = await startFakeEdge();
  });

  afterEach(async () => {
    for (const socket of harness.sockets) {
      try {
        socket.terminate();
      } catch {
        // Sockets may already be closed from the test path; swallow.
      }
    }
    await new Promise<void>((resolve) => harness.server.close(() => resolve()));
  });

  it('connects, publishes, and receives the echo', async () => {
    const realtime = new Realtime({
      url: harness.url,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();
    expect(realtime.getConnectionId()).toBe('conn-1');
    expect(realtime.getClientId()).toBe('alice');

    const channel = realtime.channels.get('chat:1');
    const received: unknown[] = [];
    channel.subscribe((message) => received.push(message.data));
    await channel.publish('hello', { text: 'world' });

    await waitFor(() => received.length === 1, 'message echo');
    expect(received[0]).toEqual({ text: 'world' });
    await realtime.close();
  });

  it('drives presence enter/update/leave via the SDK surface', async () => {
    const realtime = new Realtime({
      url: harness.url,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    const channel = realtime.channels.get('chat:1');
    const events: string[] = [];
    channel.presence.subscribe((event) => events.push(event.action));
    await channel.presence.enter({ name: 'Alice' });
    await channel.presence.update({ name: 'Alicia' });
    await channel.presence.leave();
    await waitFor(() => events.length === 3, 'presence events');
    expect(events).toEqual(['enter', 'update', 'leave']);
    await realtime.close();
  });

  it('rejects connect when the server sends an auth err frame', async () => {
    const realtime = new Realtime({
      url: harness.url,
      token: 'BAD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await expect(realtime.connect()).rejects.toThrow(/auth failed/);
  });

  it('sends key auth credentials when configured with a Realtime key', async () => {
    const realtime = new Realtime({
      url: harness.url,
      key: 'my-app.kid_test:sk_test',
      clientId: 'direct-client',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();
    expect(harness.authFrames[0]).toMatchObject({
      t: 'auth',
      key: 'my-app.kid_test:sk_test',
      clientId: 'direct-client',
    });
    await realtime.close();
  });
});

/** Poll until `predicate` is true or 2s elapses. */
async function waitFor(predicate: () => boolean, label: string): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${label}`);
}
