/**
 * Low-level WebSocket connection manager. Handles framing, request /
 * response correlation, and dispatch to per-channel listeners.
 *
 * The class is intentionally protocol-aware but channel-agnostic — the
 * Channel and Realtime classes layer the public API on top.
 */
import type { MessageFrame, PresenceEventFrame, PresenceFrame, PublishFrame, SubscribeFrame, UnsubscribeFrame } from './wire.js';
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
export declare class TypedEventEmitter<EventType extends PropertyKey, CallbackType extends (...args: any[]) => void, ResultType> implements EventEmitter<EventType, CallbackType, ResultType> {
    private readonly listeners;
    private readonly listenersByEvent;
    private readonly toResult;
    constructor(toResult: (event: EventType, args: Parameters<CallbackType>) => ResultType);
    on(listener: CallbackType): EventUnsubscribeFn;
    on(event: EventType, listener: CallbackType): EventUnsubscribeFn;
    off(): void;
    off(listener: CallbackType): void;
    off(event: EventType, listener: CallbackType): void;
    once(event: EventType): Promise<ResultType>;
    once(listener: CallbackType): void;
    once(event: EventType, listener: CallbackType): void;
    protected emit(event: EventType, ...args: Parameters<CallbackType>): void;
}
/**
 * Frames the SDK can issue with `request()`. Each carries an `id` the
 * server echoes on the matching ack/err frame; Connection assigns the
 * id so callers can omit it.
 */
export type AckableFrame = Omit<SubscribeFrame, 'id'> | Omit<UnsubscribeFrame, 'id'> | Omit<PublishFrame, 'id'> | Omit<PresenceFrame, 'id'>;
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
     * exponential backoff. Defaults to true.
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
export declare const DEFAULT_REALTIME_ENDPOINT = "realtime.foony.io";
/** Connection lifecycle states. */
export type ConnectionState = 'initialized' | 'connecting' | 'connected' | 'disconnected' | 'closing' | 'closed' | 'failed';
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
/** Listener invoked for every message frame on a channel. */
export type MessageListener = (message: MessageFrame) => void;
/** Listener invoked for every presence event frame on a channel. */
export type PresenceEventListener = (event: PresenceEventFrame) => void;
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
export declare class Connection extends TypedEventEmitter<ConnectionEventType, ConnectionEventListener, ConnectionEventResult> {
    readonly options: ConnectionOptions;
    private socket;
    private state;
    private connectionId;
    private serverClientId;
    private nextRequestId;
    private readonly pending;
    private readonly pendingHistory;
    private readonly channelDispatchers;
    private connectPromise;
    private reconnectTimer;
    private reconnectAttempt;
    /** Channels the SDK has asked to be subscribed to; re-sent on reconnect. */
    private readonly desiredSubscriptions;
    /** Publishes awaiting ack, keyed by client messageId; (re)sent on (re)connect. */
    private readonly outstandingPublishes;
    /** Maps a send attempt's request id back to its publish messageId, to route ack/err. */
    private readonly publishRequestIds;
    constructor(options: ConnectionOptions);
    /** Current connection state. */
    getState(): ConnectionState;
    /** The server-issued connection id, populated after a successful auth handshake. */
    getConnectionId(): string | null;
    /** The client id encoded in the token, populated after auth. */
    getClientId(): string | null;
    /**
     * Open the WebSocket and complete the auth handshake. Idempotent —
     * concurrent calls await the same in-flight connect.
     */
    connect(): Promise<void>;
    /** Close the WebSocket and release resources. */
    close(): Promise<void>;
    /**
     * Send a frame that expects an ack. Returns the matching AckFrame, or
     * rejects with the server's ErrorFrame (wrapped in an Error).
     */
    private request;
    /**
     * Send a `hist` frame and resolve with the matching `histRes`, or reject
     * with the server's error. Unlike `request`, history is correlated to a
     * dedicated response frame rather than a bare ack.
     */
    private requestHistory;
    /**
     * Publish a message, resolving on the server ack. When connected, sends
     * immediately. When the connection is establishing or temporarily down and
     * `queueMessages` is enabled (the default), the publish is buffered and sent on
     * the next successful (re)connect. A publish that was already in flight when the
     * connection dropped is resent on reconnect (its stable `messageId` dedupes it
     * server-side). With `queueMessages` disabled, or in a terminal connection state,
     * it rejects fast.
     */
    private publish;
    /** Send an outstanding publish on the current socket under a fresh request id. */
    private sendPublish;
    /** (Re)send every outstanding publish not currently in flight. Called on (re)connect. */
    private flushOutstandingPublishes;
    /** Settle the outstanding publish for `requestId`. Returns false if it wasn't a publish. */
    private settlePublish;
    /** Reject every outstanding publish — used when no resend path remains. */
    private failOutstandingPublishes;
    /** Register the Channel-owned dispatch callbacks used for inbound frames. */
    private registerChannel;
    /** Forget a channel's frame dispatch callbacks when the channel is released. */
    private unregisterChannel;
    /** Add `channel` to the set of subscriptions to restore on reconnect. */
    private rememberSubscription;
    /** Stop restoring this subscription on future reconnects. */
    private forgetSubscription;
    private doConnect;
    private makeSocket;
    private createAuthFrame;
    /** Steady-state message handler; installed after a successful auth. */
    private readonly handleMessage;
    private handleClose;
    private scheduleReconnect;
    private restoreSubscriptionsOnReconnect;
    private sendRaw;
    private setState;
    private emitState;
}
//# sourceMappingURL=connection.d.ts.map