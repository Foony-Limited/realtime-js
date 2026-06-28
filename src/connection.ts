/**
 * Low-level WebSocket connection manager. Handles framing, request /
 * response correlation, and dispatch to per-channel listeners.
 *
 * The class is intentionally protocol-aware but channel-agnostic — the
 * Channel and Realtime classes layer the public API on top.
 */

import type {
  AckFrame,
  AuthFrame,
  ClientFrame,
  ConnectedFrame,
  ErrorFrame,
  FetchFrame,
  FetchResponseFrame,
  HistoryFrame,
  HistoryResponseFrame,
  MessageFrame,
  PresenceEventFrame,
  PresenceFrame,
  PublishFrame,
  ServerFrame,
  SubscribeFrame,
  UnsubscribeFrame,
} from './wire.js';
import { ErrorCode } from './wire.js';

/** Function returned from listener registration APIs to remove a listener. */
export type EventUnsubscribeFn = () => void;

/** Public shape for typed event emitters exposed by the SDK. */
export type EventEmitter<EventType extends PropertyKey, CallbackType extends (...args: any[]) => void, ResultType> = {
  /** Listen to every event emitted by this emitter. Returns an unsubscribe function. */
  on(listener: CallbackType): EventUnsubscribeFn;
  /** Listen only to `event`. Returns an unsubscribe function. */
  on(event: EventType, listener: CallbackType): EventUnsubscribeFn;
  /** Remove every listener from this emitter. */
  off(): void;
  /** Remove `listener` wherever it was registered on this emitter. */
  off(listener: CallbackType): void;
  /** Remove `listener` only from `event`. */
  off(event: EventType, listener: CallbackType): void;
  /** Resolve with the next `event` emitted by this emitter. */
  once(event: EventType): Promise<ResultType>;
  /** Invoke `listener` one time for the next event emitted by this emitter. */
  once(listener: CallbackType): void;
  /** Invoke `listener` one time for the next matching `event`. */
  once(event: EventType, listener: CallbackType): void;
};

/**
 * Small typed EventEmitter used by SDK surfaces that need both catch-all and
 * event-specific listeners.
 */
export class TypedEventEmitter<EventType extends PropertyKey, CallbackType extends (...args: any[]) => void, ResultType>
  implements EventEmitter<EventType, CallbackType, ResultType>
{
  private readonly listeners = new Set<CallbackType>();
  private readonly listenersByEvent = new Map<EventType, Set<CallbackType>>();
  private readonly toResult: (event: EventType, args: Parameters<CallbackType>) => ResultType;

  constructor(toResult: (event: EventType, args: Parameters<CallbackType>) => ResultType) {
    this.toResult = toResult;
  }

  on(listener: CallbackType): EventUnsubscribeFn;
  on(event: EventType, listener: CallbackType): EventUnsubscribeFn;
  on(first: EventType | CallbackType, second?: CallbackType): EventUnsubscribeFn {
    if (typeof first === 'function' && second === undefined) {
      const listener = first as CallbackType;
      this.listeners.add(listener);
      return () => this.off(listener);
    }
    if (second !== undefined) {
      const event = first as EventType;
      let listenersForEvent = this.listenersByEvent.get(event);
      if (!listenersForEvent) {
        listenersForEvent = new Set();
        this.listenersByEvent.set(event, listenersForEvent);
      }
      listenersForEvent.add(second);
      return () => this.off(event, second);
    }
    throw new Error('EventEmitter.on: pass a listener or an event and listener');
  }

  off(): void;
  off(listener: CallbackType): void;
  off(event: EventType, listener: CallbackType): void;
  off(first?: EventType | CallbackType, second?: CallbackType): void {
    if (first === undefined) {
      this.listeners.clear();
      this.listenersByEvent.clear();
      return;
    }
    if (typeof first === 'function' && second === undefined) {
      const listener = first as CallbackType;
      this.listeners.delete(listener);
      for (const listenersForEvent of this.listenersByEvent.values()) {
        listenersForEvent.delete(listener);
      }
      return;
    }
    if (second !== undefined) {
      const listenersForEvent = this.listenersByEvent.get(first as EventType);
      listenersForEvent?.delete(second);
      return;
    }
    throw new Error('EventEmitter.off: pass no args, a listener, or an event and listener');
  }

  once(event: EventType): Promise<ResultType>;
  once(listener: CallbackType): void;
  once(event: EventType, listener: CallbackType): void;
  once(first: EventType | CallbackType, second?: CallbackType): Promise<ResultType> | void {
    if (typeof first === 'function' && second === undefined) {
      const listener = first as CallbackType;
      const wrapped = ((...args: Parameters<CallbackType>) => {
        this.off(wrapped);
        listener(...args);
      }) as CallbackType;
      this.on(wrapped);
      return;
    }
    const event = first as EventType;
    if (second === undefined) {
      return new Promise<ResultType>((resolve) => {
        const wrapped = ((...args: Parameters<CallbackType>) => {
          this.off(event, wrapped);
          resolve(this.toResult(event, args));
        }) as CallbackType;
        this.on(event, wrapped);
      });
    }
    const listener = second;
    const wrapped = ((...args: Parameters<CallbackType>) => {
      this.off(event, wrapped);
      listener(...args);
    }) as CallbackType;
    this.on(event, wrapped);
  }

  protected emit(event: EventType, ...args: Parameters<CallbackType>): void {
    for (const listener of [...this.listeners]) {
      listener(...args);
    }
    const listenersForEvent = this.listenersByEvent.get(event);
    if (!listenersForEvent) {
      return;
    }
    for (const listener of [...listenersForEvent]) {
      listener(...args);
    }
  }
}

/**
 * Frames the SDK can issue with `request()`. Each carries an `id` the
 * server echoes on the matching ack/err frame; Connection assigns the
 * id so callers can omit it.
 */
export type AckableFrame =
  | Omit<SubscribeFrame, 'id'>
  | Omit<UnsubscribeFrame, 'id'>
  | Omit<PublishFrame, 'id'>
  | Omit<PresenceFrame, 'id'>;

/** Options that control how Connection reaches the edge. */
export type ConnectionOptions = {
  /**
   * Realtime edge host or absolute ws(s) URL. Defaults to
   * `realtime.foony.io`, which resolves to `wss://realtime.foony.io`.
   */
  readonly endpoint?: string;
  /**
   * A Realtime API key in `appSlug.publicKeyId:privateKey` form. Convenient for trusted
   * quick starts and server-side scripts; browser apps should prefer JWTs
   * returned from `authCallback`.
   */
  readonly key?: string;
  /** Optional client id to attach to a direct key-auth connection. */
  readonly clientId?: string;
  /**
   * A static JWT to send in the auth handshake. Mutually exclusive with
   * `authCallback`. Useful for local dev and short scripts.
   */
  readonly token?: string;
  /**
   * Async callback that returns a fresh JWT. Called once on connect and
   * again on every reconnect. Use this when the token is short-lived
   * (the production path).
   */
  readonly authCallback?: () => Promise<string> | string;
  /**
   * Override the WebSocket constructor. Mostly useful in tests; defaults
   * to `globalThis.WebSocket` (browsers and Node 22+), falling back to
   * the `ws` package on older Node runtimes.
   */
  readonly webSocket?: typeof WebSocket;
  /**
   * If true, attempt to reconnect after unexpected disconnects with
   * exponential backoff. Defaults to true. An auth error that cannot be
   * recovered (a bad/expired static `token` or `key` with no `authCallback` to
   * re-mint) still ends in the terminal `failed` state rather than retrying.
   */
  readonly autoReconnect?: boolean;
  /**
   * Initial backoff for reconnects (default 1000ms). Doubles each
   * attempt up to maxReconnectDelayMs.
   */
  readonly initialReconnectDelayMs?: number;
  /** Cap on the reconnect backoff (default 30000ms). */
  readonly maxReconnectDelayMs?: number;
  /**
   * If true (the default), publishes made while the connection is establishing or
   * temporarily down are queued locally and flushed on (re)connect. If false,
   * publishing while not connected rejects immediately.
   */
  readonly queueMessages?: boolean;
};

/** Default Foony Realtime endpoint used when callers do not pass one. */
export const DEFAULT_REALTIME_ENDPOINT = 'realtime.foony.io';

/** Connection lifecycle states. */
export type ConnectionState =
  | 'initialized'
  | 'connecting'
  | 'connected'
  | 'disconnected'
  | 'closing'
  | 'closed'
  | 'failed';

/** Connection event names are the same lifecycle states exposed by the SDK. */
export type ConnectionEventType = ConnectionState;

/** Listener for connection lifecycle events. */
export type ConnectionEventListener = (state: ConnectionState, reason?: Error) => void;

/** Result returned by promise-based `connection.once(event)`. */
export type ConnectionEventResult = {
  readonly state: ConnectionState;
  readonly reason?: Error;
};

/** Event emitter exposed as methods on `Connection`. */
export type ConnectionEventEmitter = EventEmitter<ConnectionEventType, ConnectionEventListener, ConnectionEventResult>;

/** Backwards-compatible type alias for callers that named state listeners. */
export type ConnectionStateListener = ConnectionEventListener;

/** Internal record kept for every in-flight ack/err request. */
type PendingRequest = {
  readonly resolve: (frame: AckFrame) => void;
  readonly reject: (error: Error) => void;
};

/** Internal record kept for every in-flight history request (resolved by `histRes`). */
type PendingHistoryRequest = {
  readonly resolve: (frame: HistoryResponseFrame) => void;
  readonly reject: (error: Error) => void;
};

/** Internal record kept for every in-flight fetch (gap-fill) request (resolved by `fetchRes`). */
type PendingFetchRequest = {
  readonly resolve: (frame: FetchResponseFrame) => void;
  readonly reject: (error: Error) => void;
};

/** A publish tracked until the server acks it. */
type OutstandingPublish = {
  /** The publish frame to send. Includes a stable id for exactly-once deduplication. */
  readonly frame: Omit<PublishFrame, 'id'>;
  /** Callback to resolve the publish promise when the server acks it. */
  readonly resolve: () => void;
  /** Callback to reject the publish promise if the server sends an error. */
  readonly reject: (error: Error) => void;
  /** The id of the current send attempt, or null when the publish is buffered (not yet sent, or awaiting resend after a disconnect). */
  requestId: number | null;
};

/** Listener invoked for every message frame on a channel. */
export type MessageListener = (message: MessageFrame) => void;

/** Listener invoked for every presence event frame on a channel. */
export type PresenceEventListener = (event: PresenceEventFrame) => void;

/** Per-channel dispatch callbacks owned by Channel instances. */
type ChannelDispatchers = {
  readonly message: (message: MessageFrame) => void;
  readonly presence: (event: PresenceEventFrame) => void;
  /** The channel's legacy resume cursor (last delivered message id), or undefined. */
  readonly lastMessageId: () => string | undefined;
  /** The channel's preferred resume cursor (contiguous serial), or undefined. Migration-safe. */
  readonly lastSerial: () => number | undefined;
  /** Report the resume outcome once a reconnect re-subscribe has acked. */
  readonly resumed: (resumed: boolean) => void;
};

const DEFAULT_INITIAL_RECONNECT_DELAY_MS = 1_000;
const DEFAULT_MAX_RECONNECT_DELAY_MS = 30_000;
/** WebSocket.OPEN — duplicated here so we do not depend on a global. */
const READY_STATE_OPEN = 1;
/**
 * Close code used when we abort a handshake server-side errors. The WebSocket
 * API only permits 1000 or 3000-4999 from application code; reserved codes such
 * as 1002 make `close()` throw InvalidAccessError (strict in Node/undici), so we
 * use an app-specific 4xxx code to signal a failed handshake.
 */
const CLOSE_CODE_HANDSHAKE_FAILED = 4001;

/**
 * A client-assigned message id for a publish — `<unixMillis>-<random>`, so it is
 * roughly time-sortable like the server's ids. Generated once per publish and reused
 * across resends, so the server's dedup window can collapse a retried publish.
 */
function newClientMessageId(): string {
  const random = Math.floor(Math.random() * 0x1_0000_0000).toString(16).padStart(8, '0');
  return `${Date.now()}-${random}`;
}

/**
 * Close a socket without ever throwing. `WebSocket.close()` throws synchronously
 * on a reserved/invalid code, and in a message-event listener that throw escapes
 * to `process.nextTick` and kills the process. We never want a teardown to crash
 * the caller, so swallow any error here.
 */
/**
 * Build an Error for a server `err` frame, tagging it with the numeric code so callers
 * (e.g. Channel.attach) can tell a terminal capability denial apart from a transient
 * failure that should recover on reconnect.
 */
function serverError(code: number, message: string): Error & { code: number } {
  return Object.assign(new Error(`server error ${code}: ${message}`), { code });
}

function safeClose(ws: WebSocket, code: number, reason: string): void {
  try {
    ws.close(code, reason);
  } catch (err) {
    // Already closing, closed, or an environment that rejects the code. We must
    // not rethrow (it would crash the host), but log so a real bug isn't masked.
    console.error(`[realtime] socket close(${code}) failed:`, err);
  }
}

/**
 * Connection is the transport layer. One Realtime client owns one
 * Connection; channels share it.
 *
 * Several methods here are `private` yet called from the sibling `Channel` and
 * `Realtime` classes via index access (e.g. `connection['rememberSubscription']`).
 * That is intentional: they form the SDK-internal contract between those classes,
 * and `private` keeps them off the public `@foony/realtime` type surface. A search
 * for `this.method(` will not find these call sites — look for `['method']` too.
 */
export class Connection extends TypedEventEmitter<ConnectionEventType, ConnectionEventListener, ConnectionEventResult> {
  readonly options: ConnectionOptions;
  private socket: WebSocket | null = null;
  private state: ConnectionState = 'initialized';
  private connectionId: string | null = null;
  private serverClientId: string | null = null;
  private nextRequestId = 1;
  private readonly pending = new Map<number, PendingRequest>();
  private readonly pendingHistory = new Map<number, PendingHistoryRequest>();
  private readonly pendingFetch = new Map<number, PendingFetchRequest>();
  private readonly channelDispatchers = new Map<string, ChannelDispatchers>();
  private connectPromise: Promise<void> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  /**
   * Keep-alive ping timer. Sends a ping every server-advertised `keepAliveMs` so an idle
   * connection (no subscriptions or traffic) is not culled by an intermediary such as
   * Cloudflare's WebSocket idle timeout, which surfaces to the app as a 1006 drop.
   */
  private keepAliveTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectAttempt = 0;
  /**
   * Set when a handshake fails with an auth error we cannot recover from (a bad
   * or expired credential with no `authCallback` to re-mint). The pending socket
   * close reads it to move to a terminal `failed` state instead of retrying a
   * credential that will be rejected identically forever.
   */
  private fatalError: Error | null = null;
  /** Channels the SDK has asked to be subscribed to; re-sent on reconnect. */
  private readonly desiredSubscriptions = new Set<string>();
  /** Publishes awaiting ack, keyed by client messageId; (re)sent on (re)connect. */
  private readonly outstandingPublishes = new Map<string, OutstandingPublish>();
  /** Maps a send attempt's request id back to its publish messageId, to route ack/err. */
  private readonly publishRequestIds = new Map<number, string>();

  constructor(options: ConnectionOptions) {
    super((state, args) => {
      const reason = args[1];
      return reason === undefined ? { state } : { state, reason };
    });
    const authMethods = Number(Boolean(options.token)) + Number(Boolean(options.authCallback)) + Number(Boolean(options.key));
    if (authMethods !== 1) {
      throw new Error('Connection: pass exactly one of options.token, options.authCallback, or options.key');
    }
    this.options = options;
  }

  /** Current connection state. */
  getState(): ConnectionState {
    return this.state;
  }

  /** The server-issued connection id, populated after a successful auth handshake. */
  getConnectionId(): string | null {
    return this.connectionId;
  }

  /** The client id encoded in the token, populated after auth. */
  getClientId(): string | null {
    return this.serverClientId;
  }

  /**
   * Open the WebSocket and complete the auth handshake. Idempotent —
   * concurrent calls await the same in-flight connect.
   */
  async connect(): Promise<void> {
    if (this.state === 'connected') return;
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = this.doConnect().finally(() => {
      this.connectPromise = null;
    });
    return this.connectPromise;
  }

  /** Close the WebSocket and release resources. */
  async close(): Promise<void> {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopKeepAlive();
    this.setState('closing');
    if (this.socket && this.socket.readyState === READY_STATE_OPEN) {
      this.socket.close(1000, 'client close');
    }
    this.setState('closed');
    for (const pending of this.pending.values()) {
      pending.reject(new Error('connection closed'));
    }
    this.pending.clear();
    for (const pending of this.pendingHistory.values()) {
      pending.reject(new Error('connection closed'));
    }
    this.pendingHistory.clear();
    for (const pending of this.pendingFetch.values()) {
      pending.reject(new Error('connection closed'));
    }
    this.pendingFetch.clear();
    this.failOutstandingPublishes(new Error('connection closed'));
  }

  /**
   * Send a frame that expects an ack. Returns the matching AckFrame, or
   * rejects with the server's ErrorFrame (wrapped in an Error).
   */
  private async request(frame: AckableFrame): Promise<AckFrame> {
    await this.connect();
    const id = this.nextRequestId++;
    const out = { ...frame, id } as ClientFrame;
    return new Promise<AckFrame>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      try {
        this.sendRaw(out);
      } catch (err) {
        this.pending.delete(id);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  /**
   * Send a `hist` frame and resolve with the matching `histRes`, or reject
   * with the server's error. Unlike `request`, history is correlated to a
   * dedicated response frame rather than a bare ack.
   */
  private async requestHistory(frame: Omit<HistoryFrame, 'id'>): Promise<HistoryResponseFrame> {
    await this.connect();
    const id = this.nextRequestId++;
    const out = { ...frame, id } as ClientFrame;
    return new Promise<HistoryResponseFrame>((resolve, reject) => {
      this.pendingHistory.set(id, { resolve, reject });
      try {
        this.sendRaw(out);
      } catch (err) {
        this.pendingHistory.delete(id);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  /**
   * Surgical gap-fill: ask the server for the messages after `fromSerial` on a channel without
   * disturbing its live subscription. Used by Channel to heal a detected serial gap. Resolves with
   * the `fetchRes` frame (or rejects on `err`).
   */
  private async requestFetch(frame: Omit<FetchFrame, 'id'>): Promise<FetchResponseFrame> {
    await this.connect();
    const id = this.nextRequestId++;
    const out = { ...frame, id } as ClientFrame;
    return new Promise<FetchResponseFrame>((resolve, reject) => {
      this.pendingFetch.set(id, { resolve, reject });
      try {
        this.sendRaw(out);
      } catch (err) {
        this.pendingFetch.delete(id);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  /**
   * Publish a message, resolving on the server ack. When connected, sends
   * immediately. When the connection is establishing or temporarily down and
   * `queueMessages` is enabled (the default), the publish is buffered and sent on
   * the next successful (re)connect. A publish that was already in flight when the
   * connection dropped is resent on reconnect (its stable `messageId` dedupes it
   * server-side). With `queueMessages` disabled, or in a terminal connection state,
   * it rejects fast.
   */
  private async publish(input: Omit<PublishFrame, 'id' | 'messageId'>): Promise<void> {
    const frame = { ...input, messageId: newClientMessageId() };
    const queueMessages = this.options.queueMessages ?? true;
    const queueable =
      this.state === 'initialized' || this.state === 'connecting' || this.state === 'disconnected';
    if (this.state !== 'connected' && !(queueMessages && queueable)) {
      throw new Error(`Connection.publish: cannot publish while ${this.state}${queueMessages ? '' : ' (queueMessages disabled)'}`);
    }
    return new Promise<void>((resolve, reject) => {
      const outstanding: OutstandingPublish = { frame, resolve, reject, requestId: null };
      this.outstandingPublishes.set(frame.messageId, outstanding);
      if (this.state === 'connected') {
        this.sendPublish(outstanding);
      } else {
        // Buffered: kick a connect so it drains even if no reconnect is pending yet
        // (e.g. the very first publish); reconnect backoff drives subsequent retries.
        void this.connect().catch(() => {});
      }
    });
  }

  /** Send an outstanding publish on the current socket under a fresh request id. */
  private sendPublish(outstanding: OutstandingPublish): void {
    const id = this.nextRequestId++;
    outstanding.requestId = id;
    this.publishRequestIds.set(id, outstanding.frame.messageId);
    try {
      this.sendRaw({ ...outstanding.frame, id });
    } catch {
      // Socket not actually open; leave it outstanding to (re)send on the next connect.
      this.publishRequestIds.delete(id);
      outstanding.requestId = null;
    }
  }

  /** (Re)send every outstanding publish not currently in flight. Called on (re)connect. */
  private flushOutstandingPublishes(): void {
    for (const outstanding of this.outstandingPublishes.values()) {
      if (outstanding.requestId === null) {
        this.sendPublish(outstanding);
      }
    }
  }

  /** Settle the outstanding publish for `requestId`. Returns false if it wasn't a publish. */
  private settlePublish(requestId: number, error: Error | null): boolean {
    const messageId = this.publishRequestIds.get(requestId);
    if (messageId === undefined) return false;
    this.publishRequestIds.delete(requestId);
    const outstanding = this.outstandingPublishes.get(messageId);
    if (outstanding) {
      this.outstandingPublishes.delete(messageId);
      if (error) {
        outstanding.reject(error);
      } else {
        outstanding.resolve();
      }
    }
    return true;
  }

  /** Reject every outstanding publish — used when no resend path remains. */
  private failOutstandingPublishes(error: Error): void {
    this.publishRequestIds.clear();
    const outstanding = [...this.outstandingPublishes.values()];
    this.outstandingPublishes.clear();
    for (const item of outstanding) {
      item.reject(error);
    }
  }

  /** Register the Channel-owned dispatch callbacks used for inbound frames. */
  private registerChannel(channel: string, dispatchers: ChannelDispatchers): void {
    this.channelDispatchers.set(channel, dispatchers);
  }

  /** Forget a channel's frame dispatch callbacks when the channel is released. */
  private unregisterChannel(channel: string): void {
    this.channelDispatchers.delete(channel);
  }

  /** Add `channel` to the set of subscriptions to restore on reconnect. */
  private rememberSubscription(channel: string): void {
    this.desiredSubscriptions.add(channel);
  }

  /** Stop restoring this subscription on future reconnects. */
  private forgetSubscription(channel: string): void {
    this.desiredSubscriptions.delete(channel);
  }

  // ---- internals ----

  private async doConnect(): Promise<void> {
    this.setState('connecting');
    // Build the auth frame BEFORE opening the socket. createAuthFrame may await an async
    // authCallback (a token fetch); if that await straddled socket creation, the WebSocket
    // could fire 'open' before the listener below was attached — the event would be lost,
    // the auth frame never sent, and the connection would hang until it was dropped
    // (surfacing as a 1006 during the handshake). Fetching first removes that window.
    const authFrame = await this.createAuthFrame();
    const ws = await this.makeSocket();
    this.socket = ws;

    return new Promise<void>((resolve, reject) => {
      const onOpen = (): void => {
        try {
          ws.send(JSON.stringify(authFrame));
        } catch (err) {
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      };

      const onAuthMessage = (event: MessageEvent): void => {
        let parsed: ServerFrame;
        try {
          parsed = JSON.parse(typeof event.data === 'string' ? event.data : event.data.toString()) as ServerFrame;
        } catch (err) {
          reject(new Error(`failed to parse auth response: ${(err as Error).message}`));
          safeClose(ws, CLOSE_CODE_HANDSHAKE_FAILED, 'bad auth response');
          return;
        }
        if (parsed.t === 'connected') {
          const connected = parsed as ConnectedFrame;
          this.connectionId = connected.connectionId;
          this.serverClientId = connected.clientId;
          this.reconnectAttempt = 0;
          // Hand future frames over to the steady-state handler.
          ws.removeEventListener('message', onAuthMessage as EventListener);
          ws.addEventListener('message', this.handleMessage);
          this.setState('connected');
          this.startKeepAlive(connected.keepAliveMs);
          resolve();
          this.restoreSubscriptionsOnReconnect();
          this.flushOutstandingPublishes();
        } else if (parsed.t === 'err') {
          const errFrame = parsed as ErrorFrame;
          const authError = errFrame.code === ErrorCode.BadAuth || errFrame.code === ErrorCode.AuthExpired;
          // An auth rejection only retries if `authCallback` can produce a fresh
          // credential next attempt; a static `token`/`key` would be re-sent and
          // rejected identically, so treat that as terminal.
          if (authError && !this.options.authCallback) {
            this.fatalError = new Error(`auth failed: ${errFrame.code} ${errFrame.message}`);
          }
          safeClose(ws, CLOSE_CODE_HANDSHAKE_FAILED, `auth error ${errFrame.code}`);
          reject(new Error(`auth failed: ${errFrame.code} ${errFrame.message}`));
        } else {
          reject(new Error(`unexpected first frame: ${parsed.t}`));
          safeClose(ws, CLOSE_CODE_HANDSHAKE_FAILED, 'unexpected frame');
        }
      };

      const onError = (event: Event): void => {
        const errMessage = (event as ErrorEvent).message ?? 'websocket error';
        reject(new Error(`websocket error: ${errMessage}`));
      };

      const onClose = (event: CloseEvent): void => {
        this.handleClose(event);
        // If close fires before we finished the handshake, the
        // surrounding promise hasn't been settled yet — surface it as
        // a connect failure.
        reject(new Error(`websocket closed during handshake: ${event.code} ${event.reason}`));
      };

      ws.addEventListener('open', onOpen);
      ws.addEventListener('message', onAuthMessage as EventListener);
      ws.addEventListener('error', onError);
      ws.addEventListener('close', onClose, { once: true });
      // No await now stands between socket creation and here, so 'open' cannot have fired
      // yet — but guard anyway in case a WebSocket implementation opens synchronously.
      if (ws.readyState === READY_STATE_OPEN) {
        onOpen();
      }
    });
  }

  private async makeSocket(): Promise<WebSocket> {
    const ctor =
      this.options.webSocket ??
      (globalThis as typeof globalThis & { WebSocket?: typeof WebSocket }).WebSocket ??
      (await loadNodeWebSocket());
    if (!ctor) {
      throw new Error('Connection: no WebSocket implementation available. Pass options.webSocket or install the "ws" package.');
    }
    return new ctor(endpointToUrl(this.options.endpoint));
  }

  private async createAuthFrame(): Promise<AuthFrame> {
    if (this.options.key) {
      return {
        t: 'auth',
        key: this.options.key,
        ...(this.options.clientId ? { clientId: this.options.clientId } : {}),
      };
    }
    if (this.options.token) return { t: 'auth', token: this.options.token };
    if (!this.options.authCallback) {
      throw new Error('Connection: missing auth method');
    }
    return { t: 'auth', token: await this.options.authCallback() };
  }

  /** Steady-state message handler; installed after a successful auth. */
  private readonly handleMessage = (event: MessageEvent): void => {
    let frame: ServerFrame;
    try {
      frame = JSON.parse(typeof event.data === 'string' ? event.data : event.data.toString()) as ServerFrame;
    } catch {
      return;
    }
    switch (frame.t) {
      case 'ack': {
        const pending = this.pending.get(frame.id);
        if (pending) {
          this.pending.delete(frame.id);
          pending.resolve(frame);
          return;
        }
        this.settlePublish(frame.id, null);
        return;
      }
      case 'err': {
        if (frame.id != null) {
          const pending = this.pending.get(frame.id);
          if (pending) {
            this.pending.delete(frame.id);
            pending.reject(serverError(frame.code, frame.message));
            return;
          }
          if (this.settlePublish(frame.id, new Error(`server error ${frame.code}: ${frame.message}`))) {
            return;
          }
          const pendingHistory = this.pendingHistory.get(frame.id);
          if (pendingHistory) {
            this.pendingHistory.delete(frame.id);
            pendingHistory.reject(new Error(`server error ${frame.code}: ${frame.message}`));
            return;
          }
          const pendingFetch = this.pendingFetch.get(frame.id);
          if (pendingFetch) {
            this.pendingFetch.delete(frame.id);
            pendingFetch.reject(new Error(`server error ${frame.code}: ${frame.message}`));
            return;
          }
        }
        // Unscoped errors (id 0 or missing) are surfaced through the
        // current connection event so consumers can observe transport errors.
        this.emitState(this.state, new Error(`server error ${frame.code}: ${frame.message}`));
        return;
      }
      case 'msg': {
        this.channelDispatchers.get(frame.channel)?.message(frame);
        return;
      }
      case 'presEvt': {
        this.channelDispatchers.get(frame.channel)?.presence(frame);
        return;
      }
      case 'histRes': {
        const pending = this.pendingHistory.get(frame.id);
        if (pending) {
          this.pendingHistory.delete(frame.id);
          pending.resolve(frame);
        }
        return;
      }
      case 'fetchRes': {
        const pending = this.pendingFetch.get(frame.id);
        if (pending) {
          this.pendingFetch.delete(frame.id);
          pending.resolve(frame);
        }
        return;
      }
      case 'pong':
      case 'connected':
        // Connected can only fire once (we removed the auth listener
        // above); pong is unused in the MVP — silent forwarding keeps
        // the switch exhaustive.
        return;
    }
  };

  private handleClose(event: CloseEvent): void {
    this.socket = null;
    this.stopKeepAlive();
    // The dead socket's request ids will never be acked; drop the mappings.
    this.publishRequestIds.clear();
    if (this.state === 'closing' || this.state === 'closed') {
      this.setState('closed');
      this.failOutstandingPublishes(new Error('connection closed'));
      return;
    }
    if (this.fatalError) {
      // Unrecoverable auth failure: stop here rather than retry a credential the
      // server will keep rejecting. A later explicit connect() can still retry.
      const fatal = this.fatalError;
      this.fatalError = null;
      this.setState('failed', fatal);
      this.failOutstandingPublishes(fatal);
      return;
    }
    // Surface why we dropped (e.g. our own 4001 auth-error close) so listeners
    // can tell a transient network blip from a credential problem the reconnect
    // loop will never fix on its own.
    const reason = event.reason
      ? new Error(`websocket closed: ${event.code} ${event.reason}`)
      : new Error(`websocket closed: ${event.code}`);
    this.setState('disconnected', reason);
    const willRetry = this.options.autoReconnect !== false && (this.options.queueMessages ?? true);
    if (willRetry) {
      // Keep outstanding publishes (in-flight + buffered) to resend on reconnect.
      for (const outstanding of this.outstandingPublishes.values()) {
        outstanding.requestId = null;
      }
    } else {
      // No resend path remains → don't leave publishes hanging.
      this.failOutstandingPublishes(new Error('connection closed'));
    }
    if (this.options.autoReconnect === false) return;
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    const initial = this.options.initialReconnectDelayMs ?? DEFAULT_INITIAL_RECONNECT_DELAY_MS;
    const max = this.options.maxReconnectDelayMs ?? DEFAULT_MAX_RECONNECT_DELAY_MS;
    const delay = Math.min(initial * 2 ** this.reconnectAttempt, max);
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect().catch(() => {
        // doConnect itself drove the state machine; schedule another attempt
        // unless we've been explicitly closed or hit a terminal auth failure.
        if (this.state !== 'closed' && this.state !== 'closing' && this.state !== 'failed') {
          this.scheduleReconnect();
        }
      });
    }, delay);
  }

  private restoreSubscriptionsOnReconnect(): void {
    // Re-issue a `sub` for every remembered channel, carrying its resume cursor so the
    // server replays whatever was published during the disconnect, then report the
    // resume outcome (replayed vs discontinuity) back to the channel.
    for (const channel of this.desiredSubscriptions) {
      const dispatchers = this.channelDispatchers.get(channel);
      // Prefer the serial cursor (exact + migration-safe); fall back to the message-id cursor for
      // a channel that has only seen unsequenced messages.
      const lastSerial = dispatchers?.lastSerial();
      const lastMessageId = dispatchers?.lastMessageId();
      let frame: Omit<SubscribeFrame, 'id'>;
      if (lastSerial !== undefined) {
        frame = { t: 'sub', channel, lastSerial };
      } else if (lastMessageId !== undefined) {
        frame = { t: 'sub', channel, lastMessageId };
      } else {
        frame = { t: 'sub', channel };
      }
      this.request(frame)
        .then((ack) => dispatchers?.resumed(ack.resumed ?? false))
        .catch(() => {
          // A failed restore surfaces via channel state on the next reconnect; the
          // channel stays 'attaching' until then.
        });
    }
  }

  /** Start sending a keep-alive ping every `keepAliveMs` (no-op when non-positive). */
  private startKeepAlive(keepAliveMs: number): void {
    this.stopKeepAlive();
    if (!keepAliveMs || keepAliveMs <= 0) {
      return;
    }
    this.keepAliveTimer = setInterval(() => {
      if (this.state !== 'connected' || !this.socket || this.socket.readyState !== READY_STATE_OPEN) {
        return;
      }
      try {
        this.sendRaw({ t: 'ping' });
      } catch {
        // Socket is mid-teardown; the close handler will drive the reconnect.
      }
    }, keepAliveMs);
  }

  /** Stop the keep-alive ping timer. */
  private stopKeepAlive(): void {
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }
  }

  private sendRaw(frame: ClientFrame): void {
    if (!this.socket || this.socket.readyState !== READY_STATE_OPEN) {
      throw new Error(`Connection.sendRaw: socket not open (state=${this.state})`);
    }
    this.socket.send(JSON.stringify(frame));
  }

  private setState(state: ConnectionState, reason?: Error): void {
    if (this.state === state) return;
    this.state = state;
    this.emitState(state, reason);
  }

  private emitState(state: ConnectionState, reason?: Error): void {
    if (reason === undefined) {
      this.emit(state, state);
      return;
    }
    this.emit(state, state, reason);
  }
}

/**
 * Lazily load the `ws` package for Node < 22, where there is no global
 * WebSocket. The specifier is built at runtime so browser bundlers (which
 * always have a global WebSocket) do not try to resolve or bundle `ws`.
 */
async function loadNodeWebSocket(): Promise<typeof WebSocket | undefined> {
  try {
    const specifier = 'ws';
    const mod = (await import(/* @vite-ignore */ specifier)) as { WebSocket?: typeof WebSocket; default?: typeof WebSocket };
    return mod.WebSocket ?? mod.default;
  } catch {
    return undefined;
  }
}

function endpointToUrl(endpoint = DEFAULT_REALTIME_ENDPOINT): string {
  if (/^wss?:\/\//u.test(endpoint)) {
    return endpoint;
  }
  return `wss://${endpoint}`;
}
