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
  readonly endpoint: string;
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
  return { authFrames, server, endpoint: `ws://127.0.0.1:${address.port}`, sockets };
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

    expect(urls).toEqual(['wss://realtime.foony.com']);
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
