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
};

type LpEdge = {
  readonly url: string;
  readonly server: Server;
  readonly sessions: Map<string, LpSession>;
  connectCount: number;
  disconnectCount: number;
  readonly publishFrames: ClientFrame[];
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
      const session: LpSession = { id: `sess-${nextSession++}`, pending: [], waiter: null, closed: false };
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
        if (frame.t === 'ping') {
          deliver(session, { t: 'pong' });
        } else if ('id' in frame) {
          if (frame.t === 'pub') {
            edge.publishFrames.push(frame);
          }
          deliver(session, { t: 'ack', id: frame.id });
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
function makeFetchLpEdge(): { fetchStub: typeof fetch; state: { connectCount: number } } {
  const state = { connectCount: 0 };
  let pending: Buffer[] = [];
  let wake: (() => void) | null = null;
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
      return new Response(
        record({ t: 'connected', connectionId: 'conn-lp-1', clientId: 'client-1', keepAliveMs: 60_000 }),
        { status: 200, headers: { [SESSION_HEADER]: 'sess-1' } },
      );
    }
    if (path === '/lp/send') {
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
      if (pending.length === 0) {
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
      const body = Buffer.concat(pending);
      pending = [];
      return new Response(body, { status: 200 });
    }
    return new Response(null, { status: path === '/lp/disconnect' ? 200 : 404 });
  }) as typeof fetch;
  return { fetchStub, state };
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
