/**
 * Long-polling transport tests. Stands up an in-process HTTP fake edge
 * speaking the /lp/* surface with the SDK's own reverse-direction codec, and
 * drives the real client over it: forced long-polling, the auto WebSocket →
 * long-poll fallback, and the no-fallback rule for server-answered failures.
 * The real Go edge is covered by services/realtime-saas's integration tests.
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { Realtime } from './index.js';
import type { ClientFrame, MessageFrame, ServerFrame } from './wire.js';
import { decodeClientFrame, encodeServerFrame, frameBinaryRecord, splitBinaryRecords } from './binary.js';

const SESSION_HEADER = 'foony-lp-session';

type LpSession = {
  readonly id: string;
  pending: Buffer[];
  waiter: (() => void) | null;
  closed: boolean;
  /** Channels this session has subscribed to, so publishes fan out like the real edge. */
  readonly subscriptions: Set<string>;
};

type LpEdge = {
  readonly url: string;
  readonly server: Server;
  readonly sessions: Map<string, LpSession>;
  connectCount: number;
  disconnectCount: number;
  readonly publishFrames: ClientFrame[];
  /** Every client frame the edge received, in arrival order across all requests. */
  readonly clientFrames: ClientFrame[];
  /** Deliver a server frame to every live session (as a poll body). */
  push(frame: ServerFrame): void;
  close(): Promise<void>;
};

function readBody(request: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
    request.on('end', () => resolve(Buffer.concat(chunks)));
    request.on('error', reject);
  });
}

function decodeClientRecords(body: Buffer): ClientFrame[] {
  const buffer = body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength) as ArrayBuffer;
  return splitBinaryRecords(buffer).map((record) => decodeClientFrame(record));
}

function record(frame: ServerFrame): Buffer {
  return Buffer.from(frameBinaryRecord(encodeServerFrame(frame)));
}

/** Queue frames on a session and release its held poll, if any. */
function deliver(session: LpSession, ...frames: readonly ServerFrame[]): void {
  session.pending.push(...frames.map(record));
  session.waiter?.();
}

async function startFakeLpEdge(): Promise<LpEdge> {
  const sessions = new Map<string, LpSession>();
  let nextSession = 1;

  const edge: LpEdge = {
    url: '',
    server: createServer(),
    sessions,
    connectCount: 0,
    disconnectCount: 0,
    publishFrames: [],
    clientFrames: [],
    push(frame) {
      for (const session of sessions.values()) {
        deliver(session, frame);
      }
    },
    close() {
      for (const session of sessions.values()) {
        session.closed = true;
        session.waiter?.();
      }
      return new Promise((resolve) => edge.server.close(() => resolve()));
    },
  };

  edge.server.on('request', (request: IncomingMessage, response: ServerResponse) => {
    void handle(request, response);
  });

  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const body = await readBody(request);
    if (request.url === '/lp/connect') {
      edge.connectCount += 1;
      const [auth] = decodeClientRecords(body);
      if (!auth || auth.t !== 'auth' || auth.token === 'BAD') {
        response.writeHead(200).end(record({ t: 'err', code: 40101, message: 'bad token' }));
        return;
      }
      const session: LpSession = { id: `sess-${nextSession++}`, pending: [], waiter: null, closed: false, subscriptions: new Set() };
      sessions.set(session.id, session);
      // Honor the resume id like the real edge, so presence-stable reconnects
      // are observable in tests.
      const connectionId = auth.resumeConnectionId ?? `conn-${session.id}`;
      response
        .writeHead(200, { [SESSION_HEADER]: session.id })
        .end(record({ t: 'connected', connectionId, clientId: 'client-1', keepAliveMs: 30_000 }));
      return;
    }

    const session = sessions.get(String(request.headers[SESSION_HEADER] ?? ''));
    if (!session || (session.closed && session.pending.length === 0)) {
      response.writeHead(410).end();
      return;
    }

    if (request.url === '/lp/send') {
      for (const frame of decodeClientRecords(body)) {
        edge.clientFrames.push(frame);
        if (frame.t === 'ping') {
          deliver(session, { t: 'pong' });
        } else if ('id' in frame) {
          if (frame.t === 'sub') {
            session.subscriptions.add(frame.channel);
          }
          if (frame.t === 'pub') {
            edge.publishFrames.push(frame);
          }
          deliver(session, { t: 'ack', id: frame.id });
          // Fan a publish out to every subscriber of the channel, the publisher
          // included: the edge echoes your own message back to you.
          if (frame.t === 'pub') {
            for (const target of sessions.values()) {
              if (target.subscriptions.has(frame.channel)) {
                deliver(target, {
                  t: 'msg',
                  channel: frame.channel,
                  name: frame.name,
                  data: frame.data,
                  messageId: frame.messageId ?? 'edge-1',
                  timestamp: Date.now(),
                  // The real edge forwards batch `messages` opaquely.
                  ...(frame.messages === undefined ? {} : { messages: frame.messages }),
                });
              }
            }
          }
        }
      }
      response.writeHead(200).end();
      return;
    }
    if (request.url === '/lp/poll') {
      if (session.pending.length === 0 && !session.closed) {
        // Hold briefly, released by deliver()/close. Short so tests stay fast.
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, 1_000);
          session.waiter = () => {
            clearTimeout(timer);
            resolve();
          };
        });
        session.waiter = null;
      }
      if (session.pending.length === 0 && session.closed) {
        response.writeHead(410).end();
        return;
      }
      const bodyOut = Buffer.concat(session.pending);
      session.pending = [];
      response.writeHead(200, { 'content-type': 'application/octet-stream' }).end(bodyOut);
      return;
    }
    if (request.url === '/lp/disconnect') {
      edge.disconnectCount += 1;
      session.closed = true;
      session.waiter?.();
      response.writeHead(200).end();
      return;
    }
    response.writeHead(404).end();
  }

  await new Promise<void>((resolve) => edge.server.listen(0, '127.0.0.1', resolve));
  const address = edge.server.address() as AddressInfo;
  (edge as { url: string }).url = `http://127.0.0.1:${address.port}`;
  return edge;
}

/** Tiny event-target base for the fake WebSockets the fallback tests inject. */
class FakeSocketBase {
  readyState = 0;
  binaryType = 'arraybuffer';
  private readonly listeners = new Map<string, Array<{ listener: (event: unknown) => void; once: boolean }>>();

  addEventListener(type: string, listener: (event: unknown) => void, options?: { once?: boolean }): void {
    const entries = this.listeners.get(type) ?? [];
    entries.push({ listener, once: options?.once ?? false });
    this.listeners.set(type, entries);
  }

  removeEventListener(type: string, listener: (event: unknown) => void): void {
    this.listeners.set(type, (this.listeners.get(type) ?? []).filter((entry) => entry.listener !== listener));
  }

  protected emit(type: string, event: unknown): void {
    const entries = this.listeners.get(type) ?? [];
    this.listeners.set(type, entries.filter((entry) => !entry.once));
    for (const entry of entries) {
      entry.listener(event);
    }
  }

  send(_data: unknown): void {}

  close(code?: number, reason?: string): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    queueMicrotask(() => this.emit('close', { code: code ?? 1000, reason: reason ?? '' }));
  }
}

/** A WebSocket whose upgrade is blocked: it dies without ever opening. */
class BlockedWebSocket extends FakeSocketBase {
  constructor(_url: string) {
    super();
    setTimeout(() => {
      this.readyState = 3;
      this.emit('close', { code: 1006, reason: '' });
    }, 10);
  }
}

/**
 * A WebSocket behind a middlebox that admits the upgrade and then blackholes
 * every frame: it opens, swallows sends, and never delivers or closes.
 */
class SilentWebSocket extends FakeSocketBase {
  constructor(_url: string) {
    super();
    setTimeout(() => {
      this.readyState = 1;
      this.emit('open', {});
    }, 5);
  }
}

/**
 * An in-memory /lp/* edge as a fetch stub, for tests that run under fake
 * timers (a real HTTP fake edge would need real IO waits). Answers connect
 * with a connected frame, acks sends, and holds polls until frames pend.
 */
function makeFetchLpEdge(): {
  fetchStub: typeof fetch;
  state: { connectCount: number; killSession: () => void };
} {
  let sessionDead = false;
  let pending: Buffer[] = [];
  let wake: (() => void) | null = null;
  const state = {
    connectCount: 0,
    // Turns every later poll/send into a 410, waking a held poll, the way the
    // real edge sheds a session. A later connect mints a fresh session.
    killSession(): void {
      sessionDead = true;
      wake?.();
      wake = null;
    },
  };
  function deliver(frame: ServerFrame): void {
    pending.push(record(frame));
    wake?.();
    wake = null;
  }
  const fetchStub = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const path = new URL(String(input)).pathname;
    const raw = init?.body ? Buffer.from(init.body as ArrayBuffer) : Buffer.alloc(0);
    if (path === '/lp/connect') {
      state.connectCount += 1;
      sessionDead = false;
      return new Response(
        record({ t: 'connected', connectionId: 'conn-lp-1', clientId: 'client-1', keepAliveMs: 60_000 }),
        { status: 200, headers: { [SESSION_HEADER]: 'sess-1' } },
      );
    }
    if (path === '/lp/send') {
      if (sessionDead) {
        return new Response(null, { status: 410 });
      }
      for (const frame of decodeClientRecords(raw)) {
        if (frame.t === 'ping') {
          deliver({ t: 'pong' });
        } else if ('id' in frame) {
          deliver({ t: 'ack', id: frame.id });
        }
      }
      return new Response(null, { status: 200 });
    }
    if (path === '/lp/poll') {
      if (pending.length === 0 && !sessionDead) {
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
      if (sessionDead) {
        return new Response(null, { status: 410 });
      }
      const body = Buffer.concat(pending);
      pending = [];
      return new Response(body, { status: 200 });
    }
    return new Response(null, { status: path === '/lp/disconnect' ? 200 : 404 });
  }) as typeof fetch;
  return { fetchStub, state };
}

/**
 * A WebSocket constructor whose server a test reconfigures at runtime:
 * 'silent' opens and blackholes every frame, 'working' opens and answers the
 * auth frame with a connected frame. Counts constructions so tests can see
 * when the client gave the WebSocket another try.
 */
function makeControlledWebSocket(): {
  impl: typeof WebSocket;
  state: { mode: 'silent' | 'working'; attempts: number };
} {
  const state: { mode: 'silent' | 'working'; attempts: number } = { mode: 'silent', attempts: 0 };
  class ControlledWebSocket extends FakeSocketBase {
    private answered = false;

    constructor(_url: string) {
      super();
      state.attempts += 1;
      setTimeout(() => {
        this.readyState = 1;
        this.emit('open', {});
      }, 5);
    }

    override send(_data: unknown): void {
      if (state.mode !== 'working' || this.answered) return;
      this.answered = true;
      const reply = record({ t: 'connected', connectionId: 'conn-ws-1', clientId: 'client-1', keepAliveMs: 60_000 });
      queueMicrotask(() =>
        this.emit('message', { data: reply.buffer.slice(reply.byteOffset, reply.byteOffset + reply.byteLength) }),
      );
    }
  }
  return { impl: ControlledWebSocket as unknown as typeof WebSocket, state };
}

/** A WebSocket whose server answers the auth frame with a 40101 rejection. */
class AuthRejectingWebSocket extends FakeSocketBase {
  constructor(_url: string) {
    super();
    setTimeout(() => {
      this.readyState = 1;
      this.emit('open', {});
    }, 5);
  }

  override send(_data: unknown): void {
    const err = record({ t: 'err', code: 40101, message: 'bad token' });
    queueMicrotask(() =>
      this.emit('message', { data: err.buffer.slice(err.byteOffset, err.byteOffset + err.byteLength) }),
    );
  }
}

describe('long-polling transport', () => {
  const cleanups: Array<() => Promise<void> | void> = [];
  afterEach(async () => {
    while (cleanups.length > 0) {
      await cleanups.pop()!();
    }
  });

  it('connects, publishes, receives, and disconnects over forced long-polling', async () => {
    const edge = await startFakeLpEdge();
    cleanups.push(() => edge.close());
    const client = new Realtime({ token: 'T', endpoint: edge.url, transport: 'long-polling' });
    cleanups.push(() => client.close());

    const received: MessageFrame[] = [];
    const channel = client.channels.get('room:1');
    channel.subscribe((message) => received.push(message));
    await channel.publish('hello', { a: 1 });

    expect(client.getState()).toBe('connected');
    expect(client.getConnectionId()).toMatch(/^conn-sess-/u);
    expect(edge.publishFrames.some((frame) => frame.t === 'pub' && frame.channel === 'room:1')).toBe(true);

    edge.push({ t: 'msg', channel: 'room:1', name: 'from-edge', data: { x: 2 }, timestamp: Date.now(), messageId: 'm1' });
    await vi.waitFor(() => {
      expect(received.some((message) => message.name === 'from-edge')).toBe(true);
    });

    await client.close();
    await vi.waitFor(() => {
      expect(edge.disconnectCount).toBe(1);
    });
  });

  it('delivers a publisher its own echo on a subscription opened in the same tick', async () => {
    // subscribe() and publish() in one tick. The send lane serializes both
    // frames in call order, so the edge subscribes before it publishes and the
    // publisher gets its own message back, exactly as on the WebSocket.
    const edge = await startFakeLpEdge();
    cleanups.push(() => edge.close());
    const client = new Realtime({ token: 'T', endpoint: edge.url, transport: 'long-polling' });
    cleanups.push(() => client.close());
    await client.connect();

    const received: MessageFrame[] = [];
    const channel = client.channels.get('room:echo');
    channel.subscribe((message) => received.push(message));
    await channel.publish('hello', { a: 1 });

    await vi.waitFor(() => {
      expect(received.map((message) => message.name)).toContain('hello');
    });
    const order = edge.clientFrames.filter((frame) => frame.t === 'sub' || frame.t === 'pub').map((frame) => frame.t);
    expect(order).toEqual(['sub', 'pub']);
  });

  it('falls back to long-polling when the WebSocket never opens', async () => {
    const edge = await startFakeLpEdge();
    cleanups.push(() => edge.close());
    const client = new Realtime({
      token: 'T',
      endpoint: edge.url,
      webSocket: BlockedWebSocket as unknown as typeof WebSocket,
    });
    cleanups.push(() => client.close());

    await client.connect();
    expect(client.getState()).toBe('connected');
    expect(edge.connectCount).toBe(1);

    // The fallback transport is fully usable, not just connected.
    await client.channels.get('room:2').publish('hi', {});
    expect(edge.publishFrames.some((frame) => frame.t === 'pub' && frame.channel === 'room:2')).toBe(true);
  });

  it('does not fall back when the server answered the handshake (auth rejection)', async () => {
    const edge = await startFakeLpEdge();
    cleanups.push(() => edge.close());
    const client = new Realtime({
      token: 'whatever',
      endpoint: edge.url,
      webSocket: AuthRejectingWebSocket as unknown as typeof WebSocket,
    });
    cleanups.push(() => client.close());

    await expect(client.connect()).rejects.toThrow(/auth failed: 40101/u);
    expect(edge.connectCount).toBe(0);
  });

  it('remembers a blocked WebSocket so the next connection skips the dead attempt', async () => {
    // The whole point of the persisted memory: an app that builds several connections must not
    // pay the WebSocket connect deadline on each one, and must not pay it again next page load.
    const store = new Map<string, string>();
    vi.stubGlobal('localStorage', {
      getItem: (key: string) => store.get(key) ?? null,
      setItem: (key: string, value: string) => store.set(key, value),
      removeItem: (key: string) => store.delete(key),
      clear: () => store.clear(),
      key: () => null,
      length: 0,
    });
    cleanups.push(() => vi.unstubAllGlobals());

    const edge = await startFakeLpEdge();
    cleanups.push(() => edge.close());

    let socketsBuilt = 0;
    class CountingBlockedWebSocket extends BlockedWebSocket {
      constructor(url: string) {
        super(url);
        socketsBuilt++;
      }
    }

    const first = new Realtime({
      token: 'T',
      endpoint: edge.url,
      webSocket: CountingBlockedWebSocket as unknown as typeof WebSocket,
    });
    cleanups.push(() => first.close());
    await first.connect();
    expect(first.getState()).toBe('connected');
    expect(socketsBuilt).toBe(1);

    // A second connection to the same endpoint goes straight to long-polling.
    const second = new Realtime({
      token: 'T',
      endpoint: edge.url,
      webSocket: CountingBlockedWebSocket as unknown as typeof WebSocket,
    });
    cleanups.push(() => second.close());
    await second.connect();
    expect(second.getState()).toBe('connected');
    expect(socketsBuilt, 'second connection should not have attempted a WebSocket').toBe(1);

    // The memory is not a life sentence: once it ages past the re-probe interval a new
    // connection tries a WebSocket again, so a network that recovers is noticed.
    store.set(`foony-realtime:ws-failed-at:${edge.url}`, String(Date.now() - 10 * 60_000));
    const third = new Realtime({
      token: 'T',
      endpoint: edge.url,
      webSocket: CountingBlockedWebSocket as unknown as typeof WebSocket,
    });
    cleanups.push(() => third.close());
    await third.connect();
    expect(third.getState()).toBe('connected');
    expect(socketsBuilt, 'a stale memory should not stop the client re-probing').toBe(2);
  });

  it('falls back when the WebSocket opens but the handshake reply never arrives', async () => {
    // A middlebox that admits the upgrade and then blackholes frames. The
    // connect attempt must hit a deadline, fail over to long-polling, and
    // settle — not park in `connecting` forever.
    vi.useFakeTimers();
    cleanups.push(() => vi.useRealTimers());
    const { fetchStub, state } = makeFetchLpEdge();
    const client = new Realtime({
      token: 'T',
      endpoint: 'https://blackhole.example',
      webSocket: SilentWebSocket as unknown as typeof WebSocket,
      fetch: fetchStub,
    });
    cleanups.push(() => client.close());

    let settled = false;
    const connected = client.connect().then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    // A full virtual minute is far past any sane deadline. If the attempt is
    // still pending after it, the client is stalled.
    await vi.advanceTimersByTimeAsync(60_000);
    expect(settled).toBe(true);
    await connected;
    expect(client.getState()).toBe('connected');
    expect(state.connectCount).toBe(1);
  });

  it('bounds a forced-websocket handshake instead of hanging in connecting', async () => {
    vi.useFakeTimers();
    cleanups.push(() => vi.useRealTimers());
    const client = new Realtime({
      token: 'T',
      endpoint: 'wss://blackhole.example',
      transport: 'websocket',
      autoReconnect: false,
      webSocket: SilentWebSocket as unknown as typeof WebSocket,
    });
    cleanups.push(() => client.close());

    let outcome = 'pending';
    const connected = client.connect().then(
      () => {
        outcome = 'resolved';
      },
      () => {
        outcome = 'rejected';
      },
    );
    await vi.advanceTimersByTimeAsync(60_000);
    expect(outcome).toBe('rejected');
    await connected;
    // With autoReconnect off the failed attempt lands in `disconnected`,
    // where an explicit connect() can retry. Never a silent forever-hang.
    expect(client.getState()).toBe('disconnected');
  });

  it('returns to WebSocket when the long-poll fallback also fails (outage, not blocking)', async () => {
    // A full network outage kills the WebSocket attempt AND the long-poll
    // attempt. That must not demote the client: when the network returns,
    // the next attempt goes over WebSocket again.
    vi.useFakeTimers();
    cleanups.push(() => vi.useRealTimers());
    const ws = makeControlledWebSocket();
    const failingFetch = (async () => {
      throw new TypeError('fetch failed');
    }) as typeof fetch;
    const client = new Realtime({
      token: 'T',
      endpoint: 'https://offline.example',
      autoReconnect: false,
      webSocket: ws.impl,
      fetch: failingFetch,
    });
    cleanups.push(() => client.close());

    const outageAttempt = client.connect().catch(() => 'rejected');
    await vi.advanceTimersByTimeAsync(60_000);
    expect(await outageAttempt).toBe('rejected');
    expect(ws.state.attempts).toBe(1);

    // Network is back. The retry must not wait out the WebSocket re-probe
    // interval: the failed fallback already proved long-polling was not the
    // answer. The fetch stub still fails, so connecting proves WebSocket.
    ws.state.mode = 'working';
    const reconnected = client.connect();
    await vi.advanceTimersByTimeAsync(1_000);
    await reconnected;
    expect(client.getState()).toBe('connected');
    expect(ws.state.attempts).toBe(2);
  });

  it('re-probes the WebSocket after a minute parked on long-polling', async () => {
    vi.useFakeTimers();
    cleanups.push(() => vi.useRealTimers());
    const ws = makeControlledWebSocket();
    const { fetchStub, state } = makeFetchLpEdge();
    const client = new Realtime({
      token: 'T',
      endpoint: 'https://blocked.example',
      initialReconnectDelayMs: 20,
      webSocket: ws.impl,
      fetch: fetchStub,
    });
    cleanups.push(() => client.close());

    // Blocked WebSocket parks the client on long-polling.
    const connected = client.connect();
    await vi.advanceTimersByTimeAsync(10_000);
    await connected;
    expect(client.getState()).toBe('connected');
    expect(ws.state.attempts).toBe(1);
    expect(state.connectCount).toBe(1);

    // The block clears (say the edge deploy finished). When the long-poll
    // session dies past the re-probe interval, the reconnect tries the
    // WebSocket first and stays there.
    ws.state.mode = 'working';
    await vi.advanceTimersByTimeAsync(61_000);
    state.killSession();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(client.getState()).toBe('connected');
    expect(ws.state.attempts).toBe(2);
    expect(state.connectCount).toBe(1);
  });

  it('reconnects with a fresh long-poll session when the session dies', async () => {
    const edge = await startFakeLpEdge();
    cleanups.push(() => edge.close());
    const client = new Realtime({
      token: 'T',
      endpoint: edge.url,
      transport: 'long-polling',
      initialReconnectDelayMs: 20,
    });
    cleanups.push(() => client.close());

    await client.connect();
    const firstConnection = client.getConnectionId();

    // Kill the session server-side: the next poll 410s, the client reconnects.
    for (const session of edge.sessions.values()) {
      session.closed = true;
      session.waiter?.();
    }
    await vi.waitFor(
      () => {
        expect(edge.connectCount).toBe(2);
        expect(client.getState()).toBe('connected');
      },
      { timeout: 5_000 },
    );
    // The reconnect asked to resume the same connection id (presence survives).
    expect(client.getConnectionId()).toBe(firstConnection);
  });
});
