/**
 * Low-level WebSocket connection manager. Handles framing, request /
 * response correlation, and dispatch to per-channel listeners.
 *
 * The class is intentionally protocol-aware but channel-agnostic. The
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
  PresenceSubscribeFrame,
  PresenceUnsubscribeFrame,
  PublishFrame,
  ServerFrame,
  SubscribeFrame,
  UnsubscribeFrame,
} from './wire.js';
import { ErrorCode } from './wire.js';
import { decodeServerFrames, encodeClientFrame, frameBinaryRecord } from './binary.js';
import { LongPollSocket, endpointToHttpUrl } from './longpoll.js';

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
  /**
   * Listeners mapped to true when one-shot. One-shot listeners are stored
   * under the caller's own function (not a hidden wrapper), so `off()` can
   * remove a `once()` registration by identity.
   */
  private readonly listeners = new Map<CallbackType, boolean>();
  private readonly listenersByEvent = new Map<EventType, Map<CallbackType, boolean>>();
  private readonly toResult: (event: EventType, args: Parameters<CallbackType>) => ResultType;

  constructor(toResult: (event: EventType, args: Parameters<CallbackType>) => ResultType) {
    this.toResult = toResult;
  }

  on(listener: CallbackType): EventUnsubscribeFn;
  on(event: EventType, listener: CallbackType): EventUnsubscribeFn;
  on(first: EventType | CallbackType, second?: CallbackType): EventUnsubscribeFn {
    if (typeof first === 'function' && second === undefined) {
      const listener = first as CallbackType;
      this.listeners.set(listener, false);
      return () => this.off(listener);
    }
    if (second !== undefined) {
      const event = first as EventType;
      this.eventListeners(event).set(second, false);
      return () => this.off(event, second);
    }
    throw new Error('EventEmitter.on: pass a listener or an event and listener');
  }

  off(): void;
  off(listener: CallbackType): void;
  off(event: EventType, listener: CallbackType): void;
  off(first?: EventType | CallbackType, second?: CallbackType): void {
    let removed = false;
    if (first === undefined) {
      removed = this.hasAnyListeners();
      this.listeners.clear();
      this.listenersByEvent.clear();
    } else if (typeof first === 'function' && second === undefined) {
      const listener = first as CallbackType;
      removed = this.listeners.delete(listener);
      for (const listenersForEvent of this.listenersByEvent.values()) {
        removed = listenersForEvent.delete(listener) || removed;
      }
    } else if (second !== undefined) {
      removed = this.listenersByEvent.get(first as EventType)?.delete(second) ?? false;
    } else {
      throw new Error('EventEmitter.off: pass no args, a listener, or an event and listener');
    }
    if (removed) {
      this.onListenerRemoved();
    }
  }

  once(event: EventType): Promise<ResultType>;
  once(listener: CallbackType): void;
  once(event: EventType, listener: CallbackType): void;
  once(first: EventType | CallbackType, second?: CallbackType): Promise<ResultType> | void {
    if (typeof first === 'function' && second === undefined) {
      this.listeners.set(first as CallbackType, true);
      return;
    }
    const event = first as EventType;
    if (second === undefined) {
      return new Promise<ResultType>((resolve) => {
        const listener = ((...args: Parameters<CallbackType>) => {
          resolve(this.toResult(event, args));
        }) as CallbackType;
        this.eventListeners(event).set(listener, true);
      });
    }
    this.eventListeners(event).set(second, true);
  }

  protected emit(event: EventType, ...args: Parameters<CallbackType>): void {
    // Snapshot, then drop one-shot listeners BEFORE invoking so a re-entrant
    // emit from inside a listener cannot fire them twice.
    const catchAll = [...this.listeners.entries()];
    let removed = false;
    for (const [listener, once] of catchAll) {
      if (once) {
        this.listeners.delete(listener);
        removed = true;
      }
    }
    const listenersForEvent = this.listenersByEvent.get(event);
    const forEvent = listenersForEvent ? [...listenersForEvent.entries()] : [];
    for (const [listener, once] of forEvent) {
      if (once) {
        listenersForEvent!.delete(listener);
        removed = true;
      }
    }
    for (const [listener] of catchAll) {
      listener(...args);
    }
    for (const [listener] of forEvent) {
      listener(...args);
    }
    if (removed) {
      this.onListenerRemoved();
    }
  }

  /** True while any listener (catch-all or per-event) is still registered. */
  protected hasAnyListeners(): boolean {
    if (this.listeners.size > 0) {
      return true;
    }
    for (const listenersForEvent of this.listenersByEvent.values()) {
      if (listenersForEvent.size > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Called after listeners are removed, by `off()`, by an unsubscribe
   * function, or by a one-shot firing. Subclasses override it to react to the
   * listener count dropping, e.g. Presence closes its server watcher when the
   * last presence listener leaves. It fires after the listeners ran, so a
   * listener that re-registers keeps `hasAnyListeners()` true.
   */
  protected onListenerRemoved(): void {}

  private eventListeners(event: EventType): Map<CallbackType, boolean> {
    let listenersForEvent = this.listenersByEvent.get(event);
    if (!listenersForEvent) {
      listenersForEvent = new Map();
      this.listenersByEvent.set(event, listenersForEvent);
    }
    return listenersForEvent;
  }
}

/**
 * Frames the SDK can issue with `request()`. Each carries an `id` the
 * server echoes on the matching ack/err frame. Connection assigns the
 * id so callers can omit it.
 */
export type AckableFrame =
  | Omit<SubscribeFrame, 'id'>
  | Omit<UnsubscribeFrame, 'id'>
  | Omit<PublishFrame, 'id'>
  | Omit<PresenceFrame, 'id'>
  | Omit<PresenceSubscribeFrame, 'id'>
  | Omit<PresenceUnsubscribeFrame, 'id'>;

/**
 * Options that control how Connection reaches the edge. Exactly one of `key`,
 * `token`, or `authCallback` must be set. The constructor throws otherwise.
 */
export type ConnectionOptions = {
  /**
   * Realtime edge host or absolute ws(s) URL. Defaults to
   * `realtime.foony.io`, which resolves to `wss://realtime.foony.io`.
   *
   * @defaultValue `'realtime.foony.io'`
   */
  readonly endpoint?: string;
  /**
   * A Realtime API key in `appSlug.publicKeyId:privateKey` form. The key is a
   * long-lived secret, so use it only in server-side code and trusted quick
   * starts. Never ship it in browser code: browser apps should use
   * short-lived JWTs from `authCallback`.
   */
  readonly key?: string;
  /**
   * Client id to attach when authenticating with `key`. With token auth the
   * client id comes from the JWT's subject instead, and this option is not
   * sent.
   */
  readonly clientId?: string;
  /**
   * A static JWT to send in the auth handshake. Mutually exclusive with
   * `authCallback`. Useful for local dev and short scripts. A static token is
   * never renewed: once it expires, the connection ends in the terminal
   * `failed` state, so use `authCallback` for anything long-running.
   */
  readonly token?: string;
  /**
   * Async callback that returns a fresh JWT. Called once on connect and
   * again on every reconnect. This is the recommended auth method for
   * browsers and anything long-running, because the SDK can renew the token
   * whenever it needs one. See the [auth docs](https://foony.io/docs/auth).
   *
   * The SDK gives it 15 seconds to settle, then fails the attempt and retries
   * on the reconnect backoff, so a token fetch that hangs cannot park the
   * connection in `connecting`.
   */
  readonly authCallback?: () => Promise<string> | string;
  /**
   * Override the WebSocket constructor. Mostly useful in tests. Defaults
   * to `globalThis.WebSocket` (browsers and Node 22+), falling back to
   * the `ws` package on older Node runtimes.
   */
  readonly webSocket?: typeof WebSocket;
  /**
   * Which transport to use. `'auto'` (the default) connects over WebSocket
   * and falls back to HTTP long-polling when the WebSocket cannot be
   * established (for example a proxy that blocks upgrades). The client stays
   * on long-polling only while the WebSocket stays blocked: if the fallback
   * attempt fails the same way (the network was down, not the WebSocket
   * blocked) the next attempt is WebSocket again, and a client parked on
   * long-polling re-probes the WebSocket about once a minute. `'websocket'`
   * and `'long-polling'` force one transport and never switch. Long-polling
   * trades higher latency and per-request overhead for working on networks
   * that break WebSockets, so prefer `'auto'` outside of tests.
   *
   * @defaultValue `'auto'`
   */
  readonly transport?: 'auto' | 'websocket' | 'long-polling';
  /**
   * Override the fetch implementation used by the long-polling transport.
   * Mostly useful in tests. Defaults to the global `fetch`.
   */
  readonly fetch?: typeof fetch;
  /**
   * If true (the default), the SDK reconnects after unexpected disconnects
   * with exponential backoff. If false, a dropped connection stays down until
   * you call `connect()` again. An auth error that cannot be recovered (a bad
   * or expired static `token`, or a bad `key`, with no `authCallback` to
   * re-mint a credential) still ends in the terminal `failed` state rather
   * than retrying.
   *
   * @defaultValue `true`
   */
  readonly autoReconnect?: boolean;
  /**
   * Initial backoff for reconnects, in ms. The delay doubles each attempt up
   * to `maxReconnectDelayMs`. The default is 1000.
   *
   * @defaultValue 1000
   */
  readonly initialReconnectDelayMs?: number;
  /**
   * Cap on the reconnect backoff, in ms. The default is 30000.
   *
   * @defaultValue 30000
   */
  readonly maxReconnectDelayMs?: number;
  /**
   * If true (the default), publishes made while the connection is establishing or
   * temporarily down are queued locally and flushed on (re)connect. If false,
   * publishing while not connected rejects immediately.
   *
   * @defaultValue `true`
   */
  readonly queueMessages?: boolean;
};

/** Default Foony Realtime endpoint used when callers do not pass one. */
export const DEFAULT_REALTIME_ENDPOINT = 'realtime.foony.io';

/** Connection lifecycle states. */
export type ConnectionState =
  /** Created locally. No connect has been attempted yet. */
  | 'initialized'
  /**
   * The WebSocket is opening and the auth handshake is in flight. Publishes
   * made now are queued when `queueMessages` is on (the default).
   */
  | 'connecting'
  /**
   * Connected and authenticated. Messages flow, and `getConnectionId()` and
   * `getClientId()` are populated.
   */
  | 'connected'
  /**
   * The connection dropped unexpectedly. The state change's `reason` says
   * why. With `autoReconnect` on (the default), the SDK retries with
   * exponential backoff, starting at `initialReconnectDelayMs` (1 second)
   * and doubling up to `maxReconnectDelayMs` (30 seconds). You can keep
   * publishing: with `queueMessages` on, publishes queue locally and are
   * sent on reconnect, and channels re-attach and replay the messages they
   * missed (within retention).
   */
  | 'disconnected'
  /** `close()` was called and the socket is shutting down. */
  | 'closing'
  /** Closed by `close()`. Publishes that were awaiting an ack have been rejected. */
  | 'closed'
  /**
   * A failure the SDK will not retry on its own, for example a bad or
   * expired credential with no `authCallback` to re-mint one. The state
   * change's `reason` carries the error. An explicit `connect()` starts a
   * fresh attempt.
   */
  | 'failed';

/** Connection event names are the same lifecycle states exposed by the SDK. */
export type ConnectionEventType = ConnectionState;

/** Listener for connection lifecycle events. */
export type ConnectionEventListener = (state: ConnectionState, reason?: Error) => void;

/** Result returned by promise-based `connection.once(event)`. */
export type ConnectionEventResult = {
  /** State the connection is now in. */
  readonly state: ConnectionState;
  /** Error that caused the transition, when the event was error-driven. */
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
  /** The channel's resume cursor (contiguous serial), or undefined. Migration-safe. */
  readonly lastSerial: () => number | undefined;
  /** Report the resume outcome once a reconnect re-subscribe has acked. */
  readonly resumed: (resumed: boolean) => void;
  /** Re-announce this channel's presence membership after a reconnect (re-enter what was entered). */
  readonly reenterPresence: () => void;
};

/** Default initial reconnect backoff, in ms. */
const DEFAULT_INITIAL_RECONNECT_DELAY_MS = 1_000;

/** Default cap on the reconnect backoff, in ms. */
const DEFAULT_MAX_RECONNECT_DELAY_MS = 30_000;
/** WebSocket.CONNECTING and WebSocket.OPEN, duplicated here so we do not depend on a global. */
const READY_STATE_CONNECTING = 0;
const READY_STATE_OPEN = 1;
/**
 * Close code used when we abort a handshake server-side errors. The WebSocket
 * API only permits 1000 or 3000-4999 from application code; reserved codes such
 * as 1002 make `close()` throw InvalidAccessError (strict in Node/undici), so we
 * use an app-specific 4xxx code to signal a failed handshake.
 */
const CLOSE_CODE_HANDSHAKE_FAILED = 4001;

/** Close code used when the keep-alive deadline declares a silent link dead. */
const CLOSE_CODE_KEEPALIVE_TIMEOUT = 4002;

/** Close code used when a connect attempt fails to reach `connected` in time. */
const CLOSE_CODE_CONNECT_TIMEOUT = 4003;

/**
 * Deadlines on one connect attempt, covering everything from socket creation
 * through the server's `connected` frame. While a long-polling fallback is
 * still available, the WebSocket attempt gets the short deadline so the
 * fallback stays snappy. Everywhere else (a forced transport, or long-polling
 * itself) expiry means backoff-and-retry rather than a transport switch, so
 * the laxer deadline avoids churning slow-but-working links.
 */
const CONNECT_TIMEOUT_WITH_FALLBACK_MS = 5_000;
const CONNECT_TIMEOUT_MS = 15_000;

/**
 * Deadline on the consumer's `authCallback`. The token fetch runs before any
 * socket exists, so nothing else in the connection can notice it stalling: a
 * fetch on an HTTP client with no timeout of its own never rejects when the
 * request is dropped, and the attempt would sit in `connecting` forever with
 * no timer running. Matches the forced-transport connect deadline, since the
 * fetch is plain HTTP and no transport fallback can rescue it.
 */
const AUTH_CALLBACK_TIMEOUT_MS = 15_000;

/**
 * How long a client parked on long-polling waits before probing the WebSocket
 * again. Demotions are often transient (an edge deploy, a network blip during
 * one connect attempt), so long-polling must never be a life sentence; a
 * still-blocked network re-pays one short connect deadline per probe.
 */
const WEBSOCKET_REPROBE_INTERVAL_MS = 60_000;

/**
 * Bounds on how long we wait after a ping for proof of life (any inbound
 * frame) before declaring the link dead. The deadline follows the server's
 * advertised ping cadence, clamped to these.
 */
const MIN_PONG_DEADLINE_MS = 250;
const MAX_PONG_DEADLINE_MS = 10_000;

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
 * Build an Error for a server `err` frame, tagging it with the numeric code so callers
 * (e.g. Channel.attach) can tell a terminal capability denial apart from a transient
 * failure that should recover on reconnect.
 */
function serverError(code: number, message: string): Error & { code: number } {
  return Object.assign(new Error(`server error ${code}: ${message}`), { code });
}

/**
 * Close a socket without ever throwing. `WebSocket.close()` throws synchronously
 * on a reserved/invalid code, and in a message-event listener that throw escapes
 * to `process.nextTick` and kills the process. We never want a teardown to crash
 * the caller, so swallow any error here.
 */
function safeClose(ws: WebSocket, code: number, reason: string): void {
  // Closing a socket that is still connecting makes Node's `ws` abort the handshake and emit an
  // asynchronous error event ("WebSocket was closed before the connection was established").
  // Node crashes on an error event with no listener, and the try/catch below cannot see an async
  // event, so make sure a listener exists — the close-during-connect path in particular tears the
  // socket down before any handlers were attached. Browsers never throw on error events, and an
  // extra no-op listener is harmless where handlers are already attached.
  ws.addEventListener('error', () => {});
  try {
    ws.close(code, reason);
  } catch (err) {
    // Already closing, closed, or an environment that rejects the code. We must
    // not rethrow (it would crash the host), but log so a real bug isn't masked.
    console.error(`[realtime] socket close(${code}) failed:`, err);
  }
}

/**
 * Connection is the transport layer. One Realtime client owns one Connection
 * and all of its channels share it. Listen on lifecycle changes with
 * `connection.on(...)`, which delivers every {@link ConnectionState}
 * transition.
 *
 * Several methods here are `private` yet called from the sibling `Channel` and
 * `Realtime` classes via index access (e.g. `connection['rememberSubscription']`).
 * That is intentional: they form the SDK-internal contract between those classes,
 * and `private` keeps them off the public `@foony/realtime` type surface. A search
 * for `this.method(` will not find these call sites, so search for `['method']` too.
 */
export class Connection extends TypedEventEmitter<ConnectionEventType, ConnectionEventListener, ConnectionEventResult> {
  /** The options this connection was created with. */
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
  /** Armed after each ping, disarmed by any inbound frame. Fires when the link is dead. */
  private pongDeadlineTimer: ReturnType<typeof setTimeout> | null = null;
  /** How long the dead-link detector waits, derived from the server's ping cadence. */
  private pongDeadlineMs = MAX_PONG_DEADLINE_MS;
  /** Sockets whose close was already handled (e.g. a synthesized keep-alive timeout), so the real event is ignored. */
  private readonly closedSockets = new WeakSet<WebSocket>();
  private reconnectAttempt = 0;
  /** True once the first handshake has completed, so we can tell a reconnect from the first connect. */
  private hasConnectedBefore = false;
  /**
   * Set when a handshake fails with an auth error we cannot recover from (a bad
   * or expired credential with no `authCallback` to re-mint). The pending socket
   * close reads it to move to a terminal `failed` state instead of retrying a
   * credential that will be rejected identically forever.
   */
  private fatalError: Error | null = null;
  /** Channels the SDK has asked to be subscribed to. Re-sent on reconnect. */
  private readonly desiredSubscriptions = new Set<string>();
  /** Per-channel counter bumped on every rememberSubscription, so a stale detach cannot forget a newer attach. */
  private readonly subscriptionEpochs = new Map<string, number>();
  /** Channels the SDK has asked for presence events on. Re-sent on reconnect. */
  private readonly desiredPresence = new Set<string>();
  /**
   * The transport connect attempts use. Starts on WebSocket (unless the
   * `transport` option forces long-polling) and flips to long-polling when an
   * auto-mode WebSocket attempt fails at the transport level. Never flips
   * back: a network that broke WebSockets once will keep breaking them, and
   * flapping between transports would churn presence and resume state.
   */
  private activeTransport: 'websocket' | 'long-polling';
  /**
   * When the last WebSocket attempt died at the transport level, driving the
   * once-a-minute WebSocket re-probe while parked on long-polling.
   */
  private lastWebSocketFailureAt = 0;
  /** True once the current connect attempt created its socket (a transport was actually tried). */
  private attemptReachedTransport = false;
  /** True once the current connect attempt received any server frame (the transport works). */
  private attemptSawFrame = false;
  /** Publishes awaiting ack, keyed by client messageId. (Re)sent on (re)connect. */
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
    this.activeTransport = options.transport === 'long-polling' ? 'long-polling' : 'websocket';
  }

  /** Current {@link ConnectionState}. Listen on changes with `on(...)`. */
  getState(): ConnectionState {
    return this.state;
  }

  /** The server-issued connection id, populated after a successful auth handshake. */
  getConnectionId(): string | null {
    return this.connectionId;
  }

  /**
   * Client id this connection is authenticated as, or `null` before the auth
   * handshake completes. Never `null` once connected: the server resolves it
   * from the JWT's subject (token and `authCallback` auth), from the
   * `clientId` option (key auth), or assigns one when neither names a client.
   */
  getClientId(): string | null {
    return this.serverClientId;
  }

  /**
   * Open the WebSocket and complete the auth handshake. This method is
   * idempotent, and concurrent calls await the same in-flight connect.
   * Resolves once the connection is `connected`. Rejects with the handshake
   * error when auth fails, for example a bad key or an expired static
   * `token` with no `authCallback` to re-mint one.
   */
  async connect(): Promise<void> {
    if (this.state === 'connected') return;
    if (this.connectPromise) return this.connectPromise;
    // Parked on long-polling with the last WebSocket failure a while back?
    // Probe the WebSocket again: the block may have been one bad moment (an
    // edge deploy, a blip during connect), and this attempt falls back to
    // long-polling below if it is still real.
    if (
      (this.options.transport ?? 'auto') === 'auto' &&
      this.activeTransport === 'long-polling' &&
      Date.now() - this.lastWebSocketFailureAt >= WEBSOCKET_REPROBE_INTERVAL_MS
    ) {
      this.activeTransport = 'websocket';
    }
    this.connectPromise = this.doConnect()
      .catch((error) => {
        // Auto fallback: a WebSocket attempt that died without a single
        // server frame means the transport itself may be blocked (proxy,
        // antivirus), so retry immediately over long-polling. An attempt the
        // server answered (an auth rejection, a protocol error) would fail
        // identically on any transport and is rethrown instead.
        const transportFailed = this.attemptReachedTransport && !this.attemptSawFrame;
        const fallbackAvailable = (this.options.transport ?? 'auto') === 'auto' && this.activeTransport === 'websocket';
        const stillWanted = this.state !== 'closing' && this.state !== 'closed' && this.state !== 'failed';
        if (transportFailed && fallbackAvailable) {
          this.lastWebSocketFailureAt = Date.now();
        }
        if (transportFailed && fallbackAvailable && stillWanted) {
          this.activeTransport = 'long-polling';
          return this.doConnect().catch((fallbackError) => {
            // The fallback died the same transport-level death, so the
            // network was down rather than the WebSocket blocked. Return to
            // WebSocket for the next attempt: an outage must never demote
            // the client for good.
            if (this.attemptReachedTransport && !this.attemptSawFrame) {
              this.activeTransport = 'websocket';
            }
            throw fallbackError;
          });
        }
        throw error;
      })
      .finally(() => {
        this.connectPromise = null;
      });
    return this.connectPromise;
  }

  /**
   * Close the WebSocket and release resources. Resolves once the connection
   * reaches `closed`. Requests and publishes still awaiting an ack reject
   * with a "connection closed" error.
   */
  async close(): Promise<void> {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopKeepAlive();
    this.setState('closing');
    if (this.socket && (this.socket.readyState === READY_STATE_CONNECTING || this.socket.readyState === READY_STATE_OPEN)) {
      // Also abort a socket that is still connecting, or a close() racing an
      // in-flight connect() would leave the handshake to complete and resurrect
      // the connection.
      safeClose(this.socket, 1000, 'client close');
    }
    this.setState('closed');
    this.failPendingRequests(new Error('connection closed'));
    this.failOutstandingPublishes(new Error('connection closed'));
  }

  /** Reject every in-flight ack, history, and fetch request. They can never be answered. */
  private failPendingRequests(error: Error): void {
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
    for (const pending of this.pendingHistory.values()) {
      pending.reject(error);
    }
    this.pendingHistory.clear();
    for (const pending of this.pendingFetch.values()) {
      pending.reject(error);
    }
    this.pendingFetch.clear();
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
   * server-side). It rejects fast when the state is `closing`, `closed`, or
   * `failed`, and with `queueMessages` disabled, in any state but `connected`.
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

  /**
   * Add `channel` to the set of subscriptions to restore on reconnect, and bump
   * its epoch so an older in-flight detach cannot erase this newer intent.
   */
  private rememberSubscription(channel: string): void {
    this.desiredSubscriptions.add(channel);
    this.subscriptionEpochs.set(channel, this.subscriptionEpoch(channel) + 1);
  }

  /** The current subscription epoch for `channel`. Bumped by every rememberSubscription. */
  private subscriptionEpoch(channel: string): number {
    return this.subscriptionEpochs.get(channel) ?? 0;
  }

  /**
   * Stop restoring this subscription on future reconnects. When `epoch` is
   * given, the forget only applies if no newer attach has re-remembered the
   * channel since that epoch was read (a detach ack racing a fresh attach must
   * not erase the new subscription intent).
   */
  private forgetSubscription(channel: string, epoch?: number): void {
    if (epoch !== undefined && epoch !== this.subscriptionEpoch(channel)) {
      return;
    }
    this.desiredSubscriptions.delete(channel);
  }

  /** Add `channel` to the set of presence subscriptions to restore on reconnect. */
  private rememberPresence(channel: string): void {
    this.desiredPresence.add(channel);
  }

  /** Stop restoring this presence subscription on future reconnects. */
  private forgetPresence(channel: string): void {
    this.desiredPresence.delete(channel);
  }

  // ---- internals ----

  private async doConnect(): Promise<void> {
    this.setState('connecting');
    // Per-attempt transport telemetry, read by connect()'s fallback decision:
    // an attempt that created a socket but never saw a server frame failed at
    // the transport level, not at the protocol level.
    this.attemptReachedTransport = false;
    this.attemptSawFrame = false;
    // Build the auth frame BEFORE opening the socket. createAuthFrame may await an async
    // authCallback (a token fetch); if that await straddled socket creation, the WebSocket
    // could fire 'open' before the listener below was attached — the event would be lost,
    // the auth frame never sent, and the connection would hang until it was dropped
    // (surfacing as a 1006 during the handshake). Fetching first removes that window.
    let authFrame: AuthFrame;
    let ws: WebSocket;
    try {
      authFrame = await this.createAuthFrame();
      ws = await this.makeSocket();
      this.attemptReachedTransport = true;
    } catch (error) {
      // No socket exists yet, so no close event will drive the state machine.
      // Mirror handleClose here: mark disconnected and schedule the retry, or a
      // transient token-endpoint failure would wedge the state at `connecting`
      // with nothing in flight.
      const reason = error instanceof Error ? error : new Error(String(error));
      if (this.state === 'connecting') {
        this.setState('disconnected', reason);
        if (this.options.autoReconnect !== false) {
          this.scheduleReconnect();
        }
      }
      throw error;
    }
    if (this.state === 'closing' || this.state === 'closed') {
      // close() ran while the auth frame was being built. Don't resurrect.
      safeClose(ws, 1000, 'client close');
      throw new Error('connection closed during connect');
    }
    this.socket = ws;

    return new Promise<void>((resolve, reject) => {
      // One deadline covers the whole attempt, socket creation through the
      // server's `connected` frame. It MUST NOT be cleared at `open`: a
      // middlebox that admits the upgrade and then blackholes frames would
      // otherwise park the attempt in `connecting` forever, because keep-alive
      // only starts after `connected` and the fallback check only runs once
      // the attempt settles. Expiry closes the socket, and the close event
      // drives everything else through the normal paths: the handshake
      // rejects, and connect() falls back to long-polling (auto mode) or
      // handleClose schedules the reconnect backoff (forced transport).
      const fallbackAvailable = (this.options.transport ?? 'auto') === 'auto' && this.activeTransport === 'websocket';
      const deadlineMs = fallbackAvailable ? CONNECT_TIMEOUT_WITH_FALLBACK_MS : CONNECT_TIMEOUT_MS;
      let connectDeadline: ReturnType<typeof setTimeout> | null = setTimeout(
        () => safeClose(ws, CLOSE_CODE_CONNECT_TIMEOUT, 'connect timeout'),
        deadlineMs,
      );
      const clearConnectDeadline = (): void => {
        if (connectDeadline !== null) {
          clearTimeout(connectDeadline);
          connectDeadline = null;
        }
      };

      const onOpen = (): void => {
        try {
          // A binary auth frame makes the whole connection binary (the edge decides by the
          // WebSocket opcode of this frame).
          ws.send(frameBinaryRecord(encodeClientFrame(authFrame)));
        } catch (err) {
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      };

      const onAuthMessage = (event: MessageEvent): void => {
        // Every branch below settles the handshake (resolve or reject), so
        // the attempt deadline has done its job whatever happens next.
        clearConnectDeadline();
        this.attemptSawFrame = true;
        const binary = toArrayBuffer(event.data);
        const frames = binary ? decodeServerFrames(binary) : [];
        const parsed = frames[0];
        if (!parsed) {
          reject(new Error('failed to parse auth response'));
          safeClose(ws, CLOSE_CODE_HANDSHAKE_FAILED, 'bad auth response');
          return;
        }
        if (parsed.t === 'connected') {
          if (this.state === 'closing' || this.state === 'closed') {
            // close() ran while the handshake was in flight. Don't resurrect the
            // connection into a zombie the app believes is closed.
            safeClose(ws, 1000, 'client close');
            reject(new Error('connection closed during handshake'));
            return;
          }
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
          // The edge may coalesce more frames into the same WebSocket message as
          // `connected`. Dispatch them now that the steady handler owns the socket.
          for (const frame of frames.slice(1)) {
            this.handleFrame(frame);
          }
          const isReconnect = this.hasConnectedBefore;
          this.hasConnectedBefore = true;
          this.restoreSubscriptionsOnReconnect(isReconnect);
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
        clearConnectDeadline();
        const errMessage = (event as ErrorEvent).message ?? 'websocket error';
        reject(new Error(`websocket error: ${errMessage}`));
      };

      const onClose = (event: CloseEvent): void => {
        clearConnectDeadline();
        this.handleClose(ws, event);
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
    if (this.activeTransport === 'long-polling') {
      // The long-poll transport presents the WebSocket surface Connection
      // drives (readyState / send / close / events), so the whole state
      // machine runs unchanged over it.
      const shim = new LongPollSocket(endpointToHttpUrl(this.options.endpoint ?? DEFAULT_REALTIME_ENDPOINT), this.options.fetch);
      return shim as unknown as WebSocket;
    }
    const ctor =
      this.options.webSocket ??
      (globalThis as typeof globalThis & { WebSocket?: typeof WebSocket }).WebSocket ??
      (await loadNodeWebSocket());
    if (!ctor) {
      throw new Error('Connection: no WebSocket implementation available. Pass options.webSocket or install the "ws" package.');
    }
    const socket = new ctor(endpointToUrl(this.options.endpoint));
    // Deliver binary message frames as ArrayBuffer (not Blob) so handleMessage can decode
    // them synchronously; text frames still arrive as strings.
    socket.binaryType = 'arraybuffer';
    return socket;
  }

  private async createAuthFrame(): Promise<AuthFrame> {
    // On a reconnect, ask the server to reuse our previous connection id so presence
    // membership survives the gap with no leave/enter churn. Null on the first connect.
    const resume = this.connectionId ? { resumeConnectionId: this.connectionId } : {};
    // The auth frame goes out on the WebSocket binary opcode, which makes the whole connection
    // binary: the edge then coalesces frames and delivers binary, both implied by speaking binary,
    // so there is nothing to negotiate here (handleMessage splits coalesced messages and decodes
    // binary regardless).
    if (this.options.key) {
      return {
        t: 'auth',
        key: this.options.key,
        ...(this.options.clientId ? { clientId: this.options.clientId } : {}),
        ...resume,
      };
    }
    if (this.options.token) return { t: 'auth', token: this.options.token, ...resume };
    if (!this.options.authCallback) {
      throw new Error('Connection: missing auth method');
    }
    return { t: 'auth', token: await this.awaitAuthCallback(this.options.authCallback), ...resume };
  }

  /**
   * Run the consumer's `authCallback` under a deadline. On expiry this throws
   * from `doConnect`'s auth phase, which is already the "no socket exists yet"
   * error path: the state machine moves to `disconnected` and the attempt
   * falls into the normal reconnect backoff, so the next attempt gets a fresh
   * token fetch.
   */
  private async awaitAuthCallback(callback: () => Promise<string> | string): Promise<string> {
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        // Promise.race keeps a rejection handler on the callback's promise, so
        // a late rejection from the abandoned fetch is never unhandled.
        Promise.resolve(callback()),
        new Promise<never>((_, reject) => {
          deadline = setTimeout(
            () => reject(new Error(`authCallback timed out after ${AUTH_CALLBACK_TIMEOUT_MS}ms`)),
            AUTH_CALLBACK_TIMEOUT_MS,
          );
        }),
      ]);
    } finally {
      clearTimeout(deadline);
    }
  }

  /** Steady-state message handler, installed after a successful auth. Every server frame
   * arrives binary: one WebSocket message carries one or more opcode records. */
  private readonly handleMessage = (event: MessageEvent): void => {
    // Anything inbound proves the link is alive.
    this.clearPongDeadline();
    const binary = toArrayBuffer(event.data);
    if (!binary) return;
    for (const frame of decodeServerFrames(binary)) {
      this.handleFrame(frame);
    }
  };

  /** Dispatches one decoded server frame to its waiting caller or channel. */
  private handleFrame(frame: ServerFrame): void {
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
  }

  private handleClose(ws: WebSocket, event: CloseEvent): void {
    if (this.closedSockets.has(ws)) {
      // Already handled, e.g. synthesized by the keep-alive deadline. The real
      // close event of a dead socket can arrive minutes later.
      return;
    }
    if (this.socket !== null && this.socket !== ws) {
      // A stale socket's close event (an earlier, already-replaced attempt)
      // must not tear down the live connection.
      return;
    }
    this.closedSockets.add(ws);
    this.socket = null;
    this.stopKeepAlive();
    // The dead socket's requests can never be answered: drop the publish id
    // mappings and reject in-flight acks, history, and fetches so attach,
    // detach, history, presence, and gap-fill callers do not hang forever.
    this.publishRequestIds.clear();
    if (this.state === 'closing' || this.state === 'closed') {
      this.setState('closed');
      this.failPendingRequests(new Error('connection closed'));
      this.failOutstandingPublishes(new Error('connection closed'));
      return;
    }
    if (this.fatalError) {
      // Unrecoverable auth failure: stop here rather than retry a credential the
      // server will keep rejecting. A later explicit connect() can still retry.
      const fatal = this.fatalError;
      this.fatalError = null;
      this.setState('failed', fatal);
      this.failPendingRequests(fatal);
      this.failOutstandingPublishes(fatal);
      return;
    }
    // Surface why we dropped (e.g. our own 4001 auth-error close) so listeners
    // can tell a transient network blip from a credential problem the reconnect
    // loop will never fix on its own.
    const reason = event.reason
      ? new Error(`websocket closed: ${event.code} ${event.reason}`)
      : new Error(`websocket closed: ${event.code}`);
    this.failPendingRequests(reason);
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

  private restoreSubscriptionsOnReconnect(isReconnect: boolean): void {
    // Only on an actual reconnect. On the first connect the app's own attach()
    // and presence calls have their frames in flight already (their requests
    // await connect()), so restoring here would send duplicate subs, and the
    // duplicate's ack would surface as a spurious `update` on the channel.
    if (!isReconnect) {
      return;
    }
    // Re-issue a `sub` for every remembered channel, carrying its resume cursor so the
    // server replays whatever was published during the disconnect, then report the
    // resume outcome (replayed vs discontinuity) back to the channel.
    for (const channel of this.desiredSubscriptions) {
      const dispatchers = this.channelDispatchers.get(channel);
      // Resume from the serial cursor (exact + migration-safe). A channel that has only seen
      // unsequenced messages has none and resubscribes fresh.
      const lastSerial = dispatchers?.lastSerial();
      const frame: Omit<SubscribeFrame, 'id'> =
        lastSerial !== undefined ? { t: 'sub', channel, lastSerial } : { t: 'sub', channel };
      this.request(frame)
        .then((ack) => dispatchers?.resumed(ack.resumed ?? false))
        .catch(() => {
          // A failed restore surfaces via channel state on the next reconnect; the
          // channel stays 'attaching' until then.
        });
    }
    // Re-open presence watchers for channels the app is watching presence on.
    for (const channel of this.desiredPresence) {
      this.request({ t: 'presSub', channel }).catch(() => {});
    }
    // Re-announce presence membership: each channel re-enters whatever it had entered.
    for (const dispatchers of this.channelDispatchers.values()) {
      dispatchers.reenterPresence();
    }
  }

  /** Start sending a keep-alive ping every `keepAliveMs` (no-op when non-positive). */
  private startKeepAlive(keepAliveMs: number): void {
    this.stopKeepAlive();
    if (!keepAliveMs || keepAliveMs <= 0) {
      return;
    }
    this.pongDeadlineMs = Math.min(Math.max(keepAliveMs, MIN_PONG_DEADLINE_MS), MAX_PONG_DEADLINE_MS);
    this.keepAliveTimer = setInterval(() => {
      if (this.state !== 'connected' || !this.socket || this.socket.readyState !== READY_STATE_OPEN) {
        return;
      }
      try {
        this.sendRaw({ t: 'ping' });
        this.armPongDeadline();
      } catch {
        // Socket is mid-teardown; the close handler will drive the reconnect.
      }
    }, keepAliveMs);
  }

  /** Stop the keep-alive ping timer and the dead-link detector. */
  private stopKeepAlive(): void {
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }
    this.clearPongDeadline();
  }

  /**
   * Arm the dead-link detector after sending a ping. Any inbound frame counts
   * as proof of life and disarms it (a busy connection may deliver messages
   * ahead of the pong). When nothing arrives before the deadline, the link is
   * dead: without this, a half-dead TCP connection sits in `connected` with
   * publishes pending until the kernel gives up minutes later, because only
   * then does its close event fire.
   */
  private armPongDeadline(): void {
    if (this.pongDeadlineTimer !== null) {
      return;
    }
    this.pongDeadlineTimer = setTimeout(() => {
      this.pongDeadlineTimer = null;
      const ws = this.socket;
      if (!ws || this.state !== 'connected') {
        return;
      }
      // Drive the teardown ourselves instead of waiting for the dead socket's
      // close event. handleClose ignores that event later (closedSockets).
      safeClose(ws, CLOSE_CODE_KEEPALIVE_TIMEOUT, 'keep-alive timeout');
      this.handleClose(ws, { code: CLOSE_CODE_KEEPALIVE_TIMEOUT, reason: 'keep-alive timeout' } as CloseEvent);
    }, this.pongDeadlineMs);
  }

  /** Disarm the dead-link detector. Any inbound frame proves the link is alive. */
  private clearPongDeadline(): void {
    if (this.pongDeadlineTimer !== null) {
      clearTimeout(this.pongDeadlineTimer);
      this.pongDeadlineTimer = null;
    }
  }

  /** Send a client frame in the binary opcode protocol (one length-prefixed record). */
  private sendRaw(frame: ClientFrame): void {
    this.sendBinary(frameBinaryRecord(encodeClientFrame(frame)));
  }

  /** Send an already-encoded binary payload on the socket. */
  private sendBinary(bytes: Uint8Array<ArrayBuffer>): void {
    if (!this.socket || this.socket.readyState !== READY_STATE_OPEN) {
      throw new Error(`Connection.sendBinary: socket not open (state=${this.state})`);
    }
    this.socket.send(bytes);
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

/**
 * toArrayBuffer returns the ArrayBuffer for a binary WebSocket message, or null for a text
 * message. Handles both an ArrayBuffer (browser, with binaryType='arraybuffer') and a typed-
 * array/Buffer view (some Node ws builds), copying the exact bytes of a view.
 */
function toArrayBuffer(data: unknown): ArrayBuffer | null {
  if (data instanceof ArrayBuffer) return data;
  if (ArrayBuffer.isView(data)) {
    const view = data as ArrayBufferView;
    return view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength) as ArrayBuffer;
  }
  return null;
}

function endpointToUrl(endpoint = DEFAULT_REALTIME_ENDPOINT): string {
  if (/^wss?:\/\//u.test(endpoint)) {
    return endpoint;
  }
  return `wss://${endpoint}`;
}
