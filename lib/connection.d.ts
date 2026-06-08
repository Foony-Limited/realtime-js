/**
 * Low-level WebSocket connection manager. Handles framing, request /
 * response correlation, and dispatch to per-channel listeners.
 *
 * The class is intentionally protocol-aware but channel-agnostic — the
 * Channel and Realtime classes layer the public API on top.
 */
import type { AckFrame, ClientFrame, MessageFrame, PresenceEventFrame, PresenceFrame, PublishFrame, SubscribeFrame, UnsubscribeFrame } from './wire.js';
/**
 * Frames the SDK can issue with `request()`. Each carries an `id` the
 * server echoes on the matching ack/err frame; Connection assigns the
 * id so callers can omit it.
 */
export type AckableFrame = Omit<SubscribeFrame, 'id'> | Omit<UnsubscribeFrame, 'id'> | Omit<PublishFrame, 'id'> | Omit<PresenceFrame, 'id'>;
/** Options that control how Connection reaches the edge. */
export type ConnectionOptions = {
    /** ws:// or wss:// URL pointing at the realtime edge binary. */
    readonly url: string;
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
     * Override the global WebSocket constructor. Mostly useful in tests;
     * defaults to `globalThis.WebSocket` which is present in browsers and
     * Node 22+.
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
};
/** Connection lifecycle states. */
export type ConnectionState = 'initialized' | 'connecting' | 'connected' | 'disconnected' | 'closing' | 'closed' | 'failed';
/** Listener for state transitions. */
export type ConnectionStateListener = (state: ConnectionState, reason?: Error) => void;
/** Listener invoked for every message frame on a channel. */
export type MessageListener = (message: MessageFrame) => void;
/** Listener invoked for every presence event frame on a channel. */
export type PresenceEventListener = (event: PresenceEventFrame) => void;
/**
 * Internal listener registry, keyed by channel name. Connection owns
 * the maps so reconnect can transparently re-subscribe.
 */
type ChannelListeners = {
    readonly messages: Set<MessageListener>;
    readonly presence: Set<PresenceEventListener>;
};
/**
 * Connection is the transport layer. One Realtime client owns one
 * Connection; channels share it.
 */
export declare class Connection {
    readonly options: ConnectionOptions;
    private socket;
    private state;
    private connectionId;
    private serverClientId;
    private nextRequestId;
    private readonly pending;
    private readonly channelListeners;
    private readonly stateListeners;
    private connectPromise;
    private reconnectTimer;
    private reconnectAttempt;
    /** Channels the SDK has asked to be subscribed to; re-sent on reconnect. */
    private readonly desiredSubscriptions;
    constructor(options: ConnectionOptions);
    /** Current connection state. */
    getState(): ConnectionState;
    /** The server-issued connection id, populated after a successful auth handshake. */
    getConnectionId(): string | null;
    /** The client id encoded in the token, populated after auth. */
    getClientId(): string | null;
    /** Register a state-change listener. Returns an unsubscribe function. */
    onStateChange(listener: ConnectionStateListener): () => void;
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
    request(frame: AckableFrame): Promise<AckFrame>;
    /** Send a fire-and-forget frame (no ack expected). */
    send(frame: ClientFrame): Promise<void>;
    /**
     * Register listeners for a channel. Connection remembers the
     * registration so it can re-attach across reconnects, but actually
     * issuing the `sub` frame is the caller's job (Channel does that).
     */
    addChannelListeners(channel: string): ChannelListeners;
    /** Forget all listeners for a channel. Called from Channel.detach. */
    removeChannelListeners(channel: string): void;
    /** Add `channel` to the set of subscriptions to restore on reconnect. */
    rememberSubscription(channel: string): void;
    /** Stop restoring this subscription on future reconnects. */
    forgetSubscription(channel: string): void;
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
}
export {};
//# sourceMappingURL=connection.d.ts.map