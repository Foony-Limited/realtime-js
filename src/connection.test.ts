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

import { Realtime, generateRandomKey } from './index.js';
import type { AuthFrame, ClientFrame, ServerFrame } from './wire.js';

type Harness = {
  readonly server: WebSocketServer;
  readonly endpoint: string;
  /** All client connections opened against the fake edge. */
  readonly sockets: NodeWebSocket[];
  readonly authFrames: AuthFrame[];
  /** Every publish frame the edge received (across reconnects), in order. */
  readonly publishFrames: { messageId?: string; name: string; ttlMs?: number; data?: unknown; encoding?: string; channel?: string; messages?: readonly { name: string; data: unknown; encoding?: string }[] }[];
  /** Mutable test controls. Set `dropNextPublish` to drop the next publish before acking. */
  readonly control: { dropNextPublish: boolean };
};

async function startFakeEdge(): Promise<Harness> {
  const server = new WebSocketServer({ port: 0 });
  await new Promise<void>((resolve) => server.once('listening', resolve));
  const address = server.address() as AddressInfo;
  const sockets: NodeWebSocket[] = [];
  const authFrames: AuthFrame[] = [];
  const publishFrames: Harness['publishFrames'] = [];
  const control = { dropNextPublish: false };
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
      if (frame.t === 'pub') {
        publishFrames.push({ messageId: frame.messageId, name: frame.name, ttlMs: frame.ttlMs, data: frame.data, encoding: frame.encoding, channel: frame.channel, messages: frame.messages });
        if (control.dropNextPublish) {
          control.dropNextPublish = false;
          // Simulate the socket dying in the gap between receiving the publish and
          // acking it: the client must resend on reconnect (deduped by messageId).
          socket.close(1001, 'drop before ack');
          return;
        }
      }
      if (frame.t === 'hist') {
        const histRes: ServerFrame = {
          t: 'histRes',
          id: frame.id,
          channel: frame.channel,
          messages: [
            { t: 'msg', channel: frame.channel, name: 'msg', data: { n: 0 }, messageId: 'h-0', timestamp: 1, clientId: 'alice' },
            { t: 'msg', channel: frame.channel, name: 'msg', data: { n: 1 }, messageId: 'h-1', timestamp: 2, clientId: 'alice' },
          ],
          more: true,
        };
        socket.send(JSON.stringify(histRes));
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
            messageId: `msg-${publishFrames.length}`,
            timestamp: Date.now(),
            clientId: 'alice',
            // The real edge forwards `encoding` and batch `messages` opaquely.
            ...(frame.encoding === undefined ? {} : { encoding: frame.encoding }),
            ...(frame.messages === undefined ? {} : { messages: frame.messages }),
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
            ...((frame as { encoding?: string }).encoding === undefined ? {} : { encoding: (frame as { encoding?: string }).encoding }),
          };
          socket.send(JSON.stringify(evt));
        }
      }
    });
  });
  return { authFrames, server, endpoint: `ws://127.0.0.1:${address.port}`, sockets, publishFrames, control };
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
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();
    expect(realtime.getConnectionId()).toBe('conn-1');
    expect(realtime.getClientId()).toBe('alice');

    const channel = realtime.channels.get('chat:1');
    const received: unknown[] = [];
    const namedReceived: unknown[] = [];
    const states: string[] = [];
    channel.on((stateChange) => states.push(stateChange.current));
    channel.subscribe((message) => received.push(message.data));
    channel.subscribe('hello', (message) => namedReceived.push(message.data));
    const nextHello = new Promise<unknown>((resolve) => {
      const off = channel.subscribe('hello', (message) => {
        off();
        resolve(message);
      });
    });
    await channel.publish('hello', { text: 'world' });

    await waitFor(() => received.length === 1, 'message echo');
    await expect(nextHello).resolves.toMatchObject({ data: { text: 'world' } });
    expect(received[0]).toEqual({ text: 'world' });
    expect(namedReceived[0]).toEqual({ text: 'world' });
    expect(states).toEqual(['attaching', 'attached']);
    await realtime.close();
  });

  it('requests history and resolves with the histRes frame', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const channel = realtime.channels.get('chat:1');
    const page = await channel.history({ limit: 50 });
    expect(page.more).toBe(true);
    expect(page.messages.map((message) => message.messageId)).toEqual(['h-0', 'h-1']);
    expect(page.messages[0]?.data).toEqual({ n: 0 });
    await realtime.close();
  });

  it('encrypts payloads on the wire and decrypts them for subscribers', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const key = await generateRandomKey();
    const channel = realtime.channels.get('secret:1', { cipher: { key } });
    const received: unknown[] = [];
    channel.subscribe((message) => received.push(message.data));

    await channel.publish('msg', { secret: 'hello' });

    // The fake edge echoes the published frame; the channel decrypts it for the listener.
    await waitFor(() => received.length === 1, 'decrypted echo');
    expect(received[0]).toEqual({ secret: 'hello' });

    // What actually traversed the wire was ciphertext (base64 string) under a cipher encoding.
    const sent = harness.publishFrames[0];
    expect(sent?.encoding).toBe('cipher+aes-256-gcm/base64');
    expect(typeof sent?.data).toBe('string');
    expect(sent?.data).not.toContain('hello');
    await realtime.close();
  });

  it('encrypts presence data and decrypts presence events', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const key = await generateRandomKey();
    const channel = realtime.channels.get('secret:2', { cipher: { key } });
    const seen: unknown[] = [];
    channel.presence.subscribe((event) => seen.push(event.data));

    await channel.presence.enter({ name: 'alice' });

    await waitFor(() => seen.length >= 1, 'decrypted presence event');
    expect(seen[0]).toEqual({ name: 'alice' });
    await realtime.close();
  });

  it('sends ttlMs on publish when requested, and omits it otherwise', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const channel = realtime.channels.get('chat:1');
    await channel.publish('chat.message', { text: 'persist me' }, { ttlMs: 31_536_000_000 });
    await channel.publish('chat.typing', { state: 'started' });

    await waitFor(() => harness.publishFrames.length === 2, 'two publishes received');
    const [persisted, ephemeral] = harness.publishFrames;
    expect(persisted?.ttlMs).toBe(31_536_000_000);
    expect(ephemeral?.ttlMs).toBeUndefined();
    await realtime.close();
  });

  it('subscribes one listener to a list of message names', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const channel = realtime.channels.get('chat:1');
    const received: string[] = [];
    const unsubscribe = channel.subscribe(['chat.message', 'chat.system'], (message) => received.push(message.name));
    await channel.publish('chat.message', { text: 'hi' });
    await channel.publish('chat.system', { text: 'joined' });
    await channel.publish('chat.ignored', { text: 'nope' });

    await waitFor(() => received.length === 2, 'list subscription');
    expect(received).toEqual(['chat.message', 'chat.system']);

    unsubscribe();
    await channel.publish('chat.message', { text: 'after' });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(received).toEqual(['chat.message', 'chat.system']);
    await realtime.close();
  });

  it('drives presence enter/update/leave via the SDK surface', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    const channel = realtime.channels.get('chat:1');
    const events: string[] = [];
    const enterEvents: string[] = [];
    channel.presence.on((event) => events.push(event.action));
    channel.presence.on('enter', (event) => enterEvents.push(event.action));
    const nextLeave = channel.presence.once('leave');
    await channel.presence.enter({ name: 'Alice' });
    await channel.presence.update({ name: 'Alicia' });
    await channel.presence.leave();
    await waitFor(() => events.length === 3, 'presence events');
    await expect(nextLeave).resolves.toMatchObject({ action: 'leave' });
    expect(events).toEqual(['enter', 'update', 'leave']);
    expect(enterEvents).toEqual(['enter']);
    await realtime.close();
  });

  it('rejects connect when the server sends an auth err frame', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'BAD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await expect(realtime.connect()).rejects.toThrow(/auth failed/);
  });

  it('sends key auth credentials when configured with a Realtime key', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
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

  it('falls back to the ws package when no global WebSocket exists (Node < 22)', async () => {
    // Simulate Node 20 by hiding the global; the SDK should lazily load `ws`.
    const globalWithSocket = globalThis as typeof globalThis & { WebSocket?: typeof WebSocket };
    const originalWebSocket = globalWithSocket.WebSocket;
    delete globalWithSocket.WebSocket;
    try {
      const realtime = new Realtime({
        endpoint: harness.endpoint,
        token: 'GOOD',
        autoReconnect: false,
      });
      await realtime.connect();
      expect(realtime.getConnectionId()).toBe('conn-1');
      await realtime.close();
    } finally {
      globalWithSocket.WebSocket = originalWebSocket;
    }
  });

  it('uses the default realtime endpoint when endpoint is omitted', async () => {
    const urls: string[] = [];
    const realtime = new Realtime({
      token: 'GOOD',
      autoReconnect: false,
      webSocket: createCapturingWebSocket(urls),
    });

    await realtime.connect();

    expect(urls).toEqual(['wss://realtime.foony.io']);
    await realtime.close();
  });

  it('prefixes schemeless endpoints with wss', async () => {
    const urls: string[] = [];
    const realtime = new Realtime({
      endpoint: 'realtime.example.com',
      token: 'GOOD',
      autoReconnect: false,
      webSocket: createCapturingWebSocket(urls),
    });

    await realtime.connect();

    expect(urls).toEqual(['wss://realtime.example.com']);
    await realtime.close();
  });

  it('emits connection events through connection.on/off/once', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    const allStates: string[] = [];
    const allOnceStates: string[] = [];
    const connectedStates: string[] = [];
    const connectedOnceStates: string[] = [];
    const removedByOffAllStates: string[] = [];
    const removedByListenerStates: string[] = [];

    realtime.connection.on((state) => removedByOffAllStates.push(state));
    realtime.connection.off();
    const removedListener = (state: string): void => {
      removedByListenerStates.push(state);
    };
    realtime.connection.on(removedListener);
    realtime.connection.off(removedListener);
    const offAllStates = realtime.connection.on((state) => allStates.push(state));
    realtime.connection.once((state) => allOnceStates.push(state));
    const offConnectedStates = realtime.connection.on('connected', (state) => connectedStates.push(state));
    realtime.connection.once('connected', (state) => connectedOnceStates.push(state));
    const connected = realtime.connection.once('connected');

    await realtime.connect();

    await expect(connected).resolves.toEqual({ state: 'connected' });
    expect(allStates).toEqual(['connecting', 'connected']);
    expect(allOnceStates).toEqual(['connecting']);
    expect(connectedStates).toEqual(['connected']);
    expect(connectedOnceStates).toEqual(['connected']);
    expect(removedByOffAllStates).toEqual([]);
    expect(removedByListenerStates).toEqual([]);
    offAllStates();
    offConnectedStates();
    await realtime.close();
    expect(allStates).toEqual(['connecting', 'connected']);
  });

  it('queues a publish made while disconnected and flushes it on reconnect', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      initialReconnectDelayMs: 10,
      maxReconnectDelayMs: 10,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    const received: unknown[] = [];
    const channel = realtime.channels.get('chat:q');
    channel.subscribe((message) => received.push(message.data));
    await realtime.connect();

    // Drop the connection from the server side and wait until the client notices.
    const disconnected = new Promise<void>((resolve) => {
      const off = realtime.connection.on((state) => {
        if (state === 'disconnected') {
          off();
          resolve();
        }
      });
    });
    harness.sockets[0]?.terminate();
    await disconnected;

    // Published while down: must not reject, and resolves only once flushed on reconnect.
    await channel.publish('hello', { text: 'buffered' });
    await waitFor(() => received.length >= 1, 'queued publish echo');
    expect(received).toContainEqual({ text: 'buffered' });

    await realtime.close();
  });

  it('publishes an array as one batch frame and unpacks it into individual messages', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const channel = realtime.channels.get('chat:batch');
    const received: { name: string; data: unknown; id: string }[] = [];
    channel.subscribe((message) => received.push({ name: message.name, data: message.data, id: message.messageId }));

    await channel.publish([
      { name: 'a', data: { n: 1 } },
      { name: 'b', data: { n: 2 } },
    ]);

    // One wire frame carrying both members under a single message id.
    expect(harness.publishFrames).toHaveLength(1);
    expect(harness.publishFrames[0]?.messages).toHaveLength(2);
    expect(harness.publishFrames[0]?.messageId).toBeTruthy();

    // Delivered as two individual messages with `<batchId>:<index>` ids.
    await waitFor(() => received.length === 2, 'batch unpack');
    expect(received.map((message) => message.name)).toEqual(['a', 'b']);
    expect(received[0]?.data).toEqual({ n: 1 });
    expect(received[0]?.id.endsWith(':0')).toBe(true);
    expect(received[1]?.id.endsWith(':1')).toBe(true);
    await realtime.close();
  });

  it('batchPublish fans out to multiple channels and reports per-channel results', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const result = await realtime.batchPublish({ channels: ['c1', 'c2'], messages: [{ name: 'x', data: 1 }] });

    expect(result.successCount).toBe(2);
    expect(result.failureCount).toBe(0);
    expect(result.results.map((entry) => entry.channel).sort()).toEqual(['c1', 'c2']);
    // One batch frame per channel.
    expect(harness.publishFrames.map((frame) => frame.channel).sort()).toEqual(['c1', 'c2']);
    await realtime.close();
  });

  it('auto-batches buffered single publishes into one frame', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      batch: { intervalMs: 0 },
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const channel = realtime.channels.get('chat:auto');
    // Two publishes in the same tick coalesce into one batch frame; both resolve.
    await Promise.all([channel.publish('a', 1), channel.publish('b', 2)]);

    expect(harness.publishFrames).toHaveLength(1);
    expect(harness.publishFrames[0]?.messages?.map((member) => member.name)).toEqual(['a', 'b']);
    await realtime.close();
  });

  it('encrypts each batch member and decrypts them on receipt', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      autoReconnect: false,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    await realtime.connect();

    const key = await generateRandomKey();
    const channel = realtime.channels.get('secret:batch', { cipher: { key } });
    const received: unknown[] = [];
    channel.subscribe((message) => received.push(message.data));

    await channel.publish([
      { name: 'a', data: { secret: 'one' } },
      { name: 'b', data: { secret: 'two' } },
    ]);

    await waitFor(() => received.length === 2, 'encrypted batch unpack');
    expect(received).toEqual([{ secret: 'one' }, { secret: 'two' }]);
    // Each member traveled as ciphertext under a per-member cipher encoding.
    const members = harness.publishFrames[0]?.messages ?? [];
    expect(members.every((member) => member.encoding === 'cipher+aes-256-gcm/base64')).toBe(true);
    expect(JSON.stringify(members)).not.toContain('one');
    await realtime.close();
  });

  it('resends an in-flight publish on reconnect with the same message id', async () => {
    const realtime = new Realtime({
      endpoint: harness.endpoint,
      token: 'GOOD',
      initialReconnectDelayMs: 10,
      maxReconnectDelayMs: 10,
      webSocket: NodeWebSocket as unknown as typeof WebSocket,
    });
    const channel = realtime.channels.get('chat:r');
    await realtime.connect();

    // The edge drops the next publish's socket before acking it. The publish must not
    // hang or reject — it is resent on reconnect and resolves on the resend's ack.
    harness.control.dropNextPublish = true;
    await channel.publish('hello', { text: 'inflight' });

    // The original and the resend carried the SAME client message id, so the stream's
    // dedup window collapses them to one message (exactly-once).
    const ids = harness.publishFrames.map((frame) => frame.messageId);
    expect(ids.length).toBe(2);
    expect(ids[0]).toBeTruthy();
    expect(ids[0]).toBe(ids[1]);

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

function createCapturingWebSocket(urls: string[]): typeof WebSocket {
  type FakeListener = (event: Event) => void;

  class CapturingWebSocket {
    readyState = 1;
    private readonly listenersByType = new Map<string, Set<FakeListener>>();

    constructor(url: string | URL) {
      urls.push(String(url));
      setTimeout(() => this.dispatch('open', {} as Event), 0);
    }

    addEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
      if (typeof listener !== 'function') {
        return;
      }
      let listeners = this.listenersByType.get(type);
      if (!listeners) {
        listeners = new Set();
        this.listenersByType.set(type, listeners);
      }
      listeners.add(listener as FakeListener);
    }

    removeEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
      if (typeof listener !== 'function') {
        return;
      }
      this.listenersByType.get(type)?.delete(listener as FakeListener);
    }

    send(raw: string): void {
      const frame = JSON.parse(raw) as ClientFrame;
      if (frame.t !== 'auth') {
        return;
      }
      const connected: ServerFrame = {
        t: 'connected',
        connectionId: 'conn-default',
        keepAliveMs: 30_000,
        clientId: 'alice',
      };
      setTimeout(() => {
        this.dispatch('message', { data: JSON.stringify(connected) } as MessageEvent);
      }, 0);
    }

    close(): void {
      this.readyState = 3;
    }

    private dispatch(type: string, event: Event): void {
      const listeners = this.listenersByType.get(type);
      if (!listeners) {
        return;
      }
      for (const listener of [...listeners]) {
        listener(event);
      }
    }
  }

  return CapturingWebSocket as unknown as typeof WebSocket;
}
