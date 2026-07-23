/**
 * HTTP long-polling fallback transport. Some networks (corporate proxies, some
 * antivirus) block WebSocket upgrades, so the SDK falls back to plain
 * request/response HTTP, the one delivery shape that survives them.
 *
 * LongPollSocket speaks the edge's long-poll surface (`/lp/connect`, `/lp/send`,
 * `/lp/poll`, `/lp/disconnect`) while presenting the small WebSocket surface
 * Connection already drives (readyState, send, close, events). The same binary
 * frame records travel in HTTP bodies instead of WebSocket messages, so the
 * whole connection state machine — auth handshake, acks, resume, keep-alive
 * pings — runs unchanged over it:
 *
 * - The socket reports open immediately; Connection then sends the auth frame,
 *   which becomes the body of `POST /lp/connect`. The response carries the
 *   handshake reply frames plus a session token header.
 * - Later sends POST to `/lp/send`, serialized one request at a time (queued
 *   frames coalesce into the next body) so frames cannot reorder in flight.
 * - A poll loop holds `POST /lp/poll` open (the server answers within ~25s);
 *   response bodies dispatch as message events.
 * - close() aborts both loops and fires `POST /lp/disconnect` so the server
 *   drops presence immediately instead of waiting out the session timeout.
 */

/** Header carrying the long-poll session token (response of connect, request of the rest). */
const SESSION_HEADER = 'foony-lp-session';

/** Guard on every long-poll HTTP request. Comfortably above the server's ~25s poll hold. */
const REQUEST_TIMEOUT_MS = 40_000;

type ListenerEntry = {
  readonly listener: (event: unknown) => void;
  readonly once: boolean;
};

/**
 * The WebSocket-shaped long-poll transport. Connection treats it exactly like
 * a WebSocket, which keeps the fallback out of the connection state machine.
 */
export class LongPollSocket {
  /** Mirrors WebSocket.readyState: 1 (open) from creation until close, then 3. */
  readyState = 1;
  /** Accepted for WebSocket parity; bodies are always delivered as ArrayBuffer. */
  binaryType = 'arraybuffer';

  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly listeners = new Map<string, ListenerEntry[]>();
  private readonly abort = new AbortController();
  private session: string | null = null;
  /** Bodies waiting for the send lane; flushed as one concatenated body per request. */
  private sendQueue: Uint8Array[] = [];
  private sendInFlight = false;
  private connectStarted = false;
  private closed = false;

  constructor(httpBaseUrl: string, fetchImpl?: typeof fetch) {
    this.baseUrl = httpBaseUrl.replace(/\/$/u, '');
    // Bind: an unbound fetch loses its `globalThis` receiver and throws in browsers.
    this.fetchImpl = fetchImpl ?? fetch.bind(globalThis);
  }

  /**
   * Queue bytes for the edge. The first send carries the auth frame and
   * becomes the connect request; everything later flows through the send lane.
   */
  send(bytes: Uint8Array): void {
    if (this.closed) {
      throw new Error('LongPollSocket.send: socket is closed');
    }
    if (!this.connectStarted) {
      this.connectStarted = true;
      void this.runConnect(bytes);
      return;
    }
    this.sendQueue.push(bytes);
    void this.flushSends();
  }

  /**
   * Close the transport: abort the poll and send lanes, tell the server this
   * is a deliberate disconnect (so presence leaves immediately), and fire the
   * close event Connection's teardown expects.
   */
  close(code?: number, reason?: string): void {
    if (this.closed) return;
    this.closed = true;
    this.readyState = 3;
    this.abort.abort();
    if (this.session) {
      // Fire-and-forget with its own controller: the shared one is already aborted.
      const disconnectUrl = `${this.baseUrl}/lp/disconnect`;
      void this.fetchImpl(disconnectUrl, {
        method: 'POST',
        headers: { [SESSION_HEADER]: this.session },
      }).catch(() => {});
    }
    // WebSocket delivers close asynchronously; matching that keeps Connection's
    // "attach listeners after construction" pattern safe.
    queueMicrotask(() => this.dispatch('close', { code: code ?? 1000, reason: reason ?? '' }));
  }

  addEventListener(type: string, listener: (event: unknown) => void, options?: { once?: boolean }): void {
    const entries = this.listeners.get(type) ?? [];
    entries.push({ listener, once: options?.once ?? false });
    this.listeners.set(type, entries);
  }

  removeEventListener(type: string, listener: (event: unknown) => void): void {
    const entries = this.listeners.get(type);
    if (!entries) return;
    this.listeners.set(
      type,
      entries.filter((entry) => entry.listener !== listener),
    );
  }

  // ---- internals ----

  private dispatch(type: string, event: unknown): void {
    const entries = this.listeners.get(type);
    if (!entries || entries.length === 0) return;
    this.listeners.set(
      type,
      entries.filter((entry) => !entry.once),
    );
    for (const entry of entries) {
      entry.listener(event);
    }
  }

  /** Fail the connection: everything funnels into one close event, which drives Connection's reconnect. */
  private fail(reason: string): void {
    if (this.closed) return;
    this.closed = true;
    this.readyState = 3;
    this.abort.abort();
    this.dispatch('close', { code: 1006, reason });
  }

  /** One HTTP request with the shared abort signal plus a per-request timeout. */
  private async request(path: string, body: Uint8Array | null): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    const onOuterAbort = (): void => controller.abort();
    this.abort.signal.addEventListener('abort', onOuterAbort);
    try {
      return await this.fetchImpl(`${this.baseUrl}${path}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/octet-stream',
          ...(this.session ? { [SESSION_HEADER]: this.session } : {}),
        },
        // Copy into a fresh ArrayBuffer-backed body: fetch rejects SharedArrayBuffer views.
        body: body ? new Uint8Array(body).buffer : null,
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timeout);
      this.abort.signal.removeEventListener('abort', onOuterAbort);
    }
  }

  private async runConnect(authFrame: Uint8Array): Promise<void> {
    let response: Response;
    let frames: ArrayBuffer;
    try {
      response = await this.request('/lp/connect', authFrame);
      frames = await response.arrayBuffer();
    } catch (error) {
      this.fail(`long-poll connect failed: ${String(error)}`);
      return;
    }
    if (!response.ok) {
      this.fail(`long-poll connect failed: HTTP ${response.status}`);
      return;
    }
    const session = response.headers.get(SESSION_HEADER);
    if (!session) {
      // A handshake rejection (bad auth) has no usable session, but its error
      // frames still matter: deliver them so Connection surfaces the real
      // cause, then end the transport.
      if (frames.byteLength > 0) {
        this.dispatch('message', { data: frames });
      }
      this.fail('long-poll connect returned no session');
      return;
    }
    if (this.closed) return;
    this.session = session;
    if (frames.byteLength > 0) {
      this.dispatch('message', { data: frames });
    }
    void this.runPollLoop();
    void this.flushSends();
  }

  /** The send lane: one request at a time, queued frames coalesced per body. */
  private async flushSends(): Promise<void> {
    if (this.sendInFlight || !this.session || this.closed || this.sendQueue.length === 0) {
      return;
    }
    this.sendInFlight = true;
    try {
      while (this.sendQueue.length > 0 && !this.closed) {
        const chunks = this.sendQueue;
        this.sendQueue = [];
        const body = concat(chunks);
        let response: Response;
        try {
          response = await this.request('/lp/send', body);
        } catch (error) {
          this.fail(`long-poll send failed: ${String(error)}`);
          return;
        }
        if (!response.ok) {
          this.fail(`long-poll send failed: HTTP ${response.status}`);
          return;
        }
        // Drain the body so keep-alive connections are reusable.
        await response.arrayBuffer().catch(() => {});
      }
    } finally {
      this.sendInFlight = false;
    }
  }

  /** The receive lane: poll forever, dispatching each non-empty body as one message. */
  private async runPollLoop(): Promise<void> {
    while (!this.closed) {
      let response: Response;
      let body: ArrayBuffer;
      try {
        response = await this.request('/lp/poll', null);
        body = await response.arrayBuffer();
      } catch (error) {
        if (!this.closed) {
          this.fail(`long-poll poll failed: ${String(error)}`);
        }
        return;
      }
      if (!response.ok) {
        // 410 means the server ended the session (idle timeout, slow consumer,
        // shutdown); anything else is equally terminal for this transport.
        this.fail(`long-poll session ended: HTTP ${response.status}`);
        return;
      }
      if (body.byteLength > 0) {
        this.dispatch('message', { data: body });
      }
    }
  }
}

/** Join queued send bodies into one records stream (records concatenate freely). */
function concat(chunks: readonly Uint8Array[]): Uint8Array {
  let total = 0;
  for (const chunk of chunks) {
    total += chunk.byteLength;
  }
  const joined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return joined;
}

/**
 * Resolve a Realtime endpoint (bare host, ws(s) or http(s) URL) to the HTTP
 * base URL the long-poll routes live under.
 */
export function endpointToHttpUrl(endpoint: string): string {
  if (/^https?:\/\//u.test(endpoint)) {
    return endpoint;
  }
  if (endpoint.startsWith('wss://')) {
    return `https://${endpoint.slice('wss://'.length)}`;
  }
  if (endpoint.startsWith('ws://')) {
    return `http://${endpoint.slice('ws://'.length)}`;
  }
  return `https://${endpoint}`;
}
