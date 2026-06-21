/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 *
 * The channel deliberately exposes two separate listener surfaces so callers
 * never confuse lifecycle with data: `on` / `once` / `off` observe the
 * channel's lifecycle *state* (a closed set of events), while `subscribe` /
 * `unsubscribe` carry application *messages* (open-ended event names).
 */
import { TypedEventEmitter, type Connection, type EventUnsubscribeFn, type MessageListener, type PresenceEventListener } from './connection.js';
import { Cipher, type CipherParams } from './crypto.js';
import type { MessageFrame, PresenceAction, PresenceEventFrame } from './wire.js';
/** Listener handle returned by `subscribe` — call to remove the listener. */
export type UnsubscribeFn = EventUnsubscribeFn;
/**
 * Automatic publish batching. When enabled, single `publish(name, data)` calls
 * are buffered and flushed as one batch frame (one stored, dedupable message),
 * trading a little latency for fewer messages and less transport overhead.
 * Disabled by default. Array publishes and `batchPublish` are never buffered.
 */
export type BatchOptions = {
    /** Turn batching on for the channel. Default false. */
    readonly enabled?: boolean;
    /**
     * How long to buffer before flushing, in ms. 0 (default) coalesces publishes
     * made in the same tick with no added latency; a larger value batches more at
     * the cost of up to `intervalMs` extra latency.
     */
    readonly intervalMs?: number;
    /** Flush early once this many messages are buffered. Default 200. */
    readonly maxMessages?: number;
};
/**
 * Channel lifecycle states. A channel walks this set as it attaches to
 * and detaches from the server, exposed so callers can react to (re)attach
 * and failure transitions.
 */
export type ChannelState = 
/** Created locally; no attach has been attempted yet. */
'initialized'
/** An attach has been requested and is awaiting server confirmation. */
 | 'attaching'
/** Attached — messages and presence for this channel are flowing. */
 | 'attached'
/** A detach has been requested and is awaiting server confirmation. */
 | 'detaching'
/** Detached — no messages or presence are delivered until re-attached. */
 | 'detached'
/** Temporarily lost (e.g. the connection dropped); the SDK will re-attach on reconnect. */
 | 'suspended'
/** Attach failed and will not be retried automatically (e.g. permission denied). */
 | 'failed';
/**
 * Events emitted to channel state listeners: every {@link ChannelState}
 * plus `update` (a no-transition re-confirmation, e.g. a resume).
 */
export type ChannelEventType = ChannelState
/**
 * A change that is not a state transition — the channel stayed in the
 * same state but something was re-confirmed (e.g. the subscription was
 * resumed after a reconnect). Carries `resumed` on the state change.
 */
 | 'update';
/** Payload delivered to channel state listeners on every {@link ChannelEventType}. */
export type ChannelStateChange = {
    /** State the channel is now in. */
    readonly current: ChannelState;
    /** State the channel was in immediately before this event. */
    readonly previous: ChannelState;
    /** Error that caused the transition, when the event was error-driven. */
    readonly reason?: Error;
    /** True when the channel resumed without missing messages (e.g. after a reconnect). */
    readonly resumed: boolean;
};
/** Listener for channel lifecycle state changes. */
export type ChannelStateListener = (stateChange: ChannelStateChange) => void;
/** Listener for channel lifecycle state changes (alias of {@link ChannelStateListener}). */
export type ChannelEventListener = ChannelStateListener;
/** Result returned by promise-based `channel.once(event)`. */
export type ChannelEventResult = ChannelStateChange;
/** Presence event names emitted by a channel presence facade. */
export type PresenceEventType = PresenceAction;
/** Result returned by promise-based `presence.once(event)`. */
export type PresenceEventResult = PresenceEventFrame;
/**
 * One subscription handle per (channel, listener) pair. Channels are
 * value-equal by name on a given Realtime client — calling
 * `client.channels.get('chat:1')` twice returns the same instance.
 *
 * `on` / `once` / `off` observe the channel's {@link ChannelState};
 * `subscribe` / `unsubscribe` receive {@link MessageFrame} messages.
 */
export declare class Channel extends TypedEventEmitter<ChannelEventType, ChannelStateListener, ChannelStateChange> {
    readonly name: string;
    readonly presence: Presence;
    private readonly connection;
    private readonly messages;
    private readonly cipher;
    /** Serializes async decryption so encrypted messages keep their arrival order. */
    private decryptChain;
    /** Resolved auto-batch config (defaults applied). */
    private readonly batch;
    /** Buffered single publishes awaiting flush, when batching is enabled. */
    private batchBuffer;
    private batchTimer;
    private attachPromise;
    private channelState;
    constructor(connection: Connection, name: string, cipher?: CipherParams, batch?: BatchOptions);
    /** Current channel lifecycle state. */
    get state(): ChannelState;
    /**
     * Ensure the server is subscribed to this channel. Called implicitly
     * by `subscribe()` and `presence.subscribe()`; expose it so callers
     * can pre-attach if they want to surface attach errors before the
     * first message arrives.
     */
    attach(): Promise<void>;
    /**
     * Detach from the server (stop receiving messages and presence
     * events). Local listeners are preserved — call `off()` or
     * `unsubscribe()` to clear them.
     */
    detach(): Promise<void>;
    /**
     * Register a listener for every message frame on this channel.
     * Implicitly attaches if needed. Returns an unsubscribe function.
     */
    subscribe(listener: MessageListener): UnsubscribeFn;
    /** Register a listener for messages with a matching `name`. */
    subscribe(event: string, listener: MessageListener): UnsubscribeFn;
    /** Register one listener for messages matching any name in `events`. */
    subscribe(events: readonly string[], listener: MessageListener): UnsubscribeFn;
    /** Remove every message listener on this channel. */
    unsubscribe(): void;
    /** Remove `listener` wherever it was registered for messages. */
    unsubscribe(listener: MessageListener): void;
    /** Remove `listener` only from messages with a matching `name`. */
    unsubscribe(event: string, listener: MessageListener): void;
    /** Remove `listener` from messages matching any name in `events`. */
    unsubscribe(events: readonly string[], listener: MessageListener): void;
    /**
     * Publish one application-level message to the channel.
     *
     * @param name - The event name.
     * @param data - The data to publish.
     * @param options - Optional publish controls. `ttlMs` requests how long the
     *   message is retained for history (server-clamped to your plan ceiling);
     *   omit it for the short ephemeral default.
     */
    publish(name: string, data: unknown, options?: {
        readonly ttlMs?: number;
    }): Promise<void>;
    /**
     * Publish a batch of messages in a single frame under one message id. The
     * batch is the atomic unit: the server stores and dedups it as one durable
     * message (so a resend is collapsed), while subscribers receive the members
     * individually. Reduces stored-message count and transport overhead.
     *
     * @param messages - The messages to publish, each with its own `name`/`data`.
     * @param options - Optional publish controls (e.g. `ttlMs`).
     */
    publish(messages: ReadonlyArray<{
        readonly name: string;
        readonly data: unknown;
    }>, options?: {
        readonly ttlMs?: number;
    }): Promise<void>;
    /** Build a wire batch member, encrypting `data` per-member when a cipher is set. */
    private toMember;
    /**
     * Flush any buffered (auto-batched) publishes now, as a single batch frame.
     * Runs automatically on the configured interval, when the buffer is full, and
     * on detach; call it to force an immediate send. No-op when nothing is buffered.
     */
    flush(): void;
    /** Buffer a member for the next flush, scheduling or forcing a flush as needed. */
    private enqueue;
    /**
     * Fetch recent messages for this channel, oldest-first. Does not interleave
     * with the live subscription. Pass `start` (a message id) to page backward.
     */
    history(params?: {
        readonly limit?: number;
        readonly start?: string;
    }): Promise<{
        readonly messages: readonly MessageFrame[];
        readonly more: boolean;
    }>;
    /**
     * Deliver an inbound frame to subscribers. A batch frame is expanded into its
     * member frames (in order) first; each member is then dispatched like a single
     * message.
     */
    private deliverMessage;
    /**
     * Dispatch one message frame, decrypting first when a cipher is set.
     * Decryption is serialized through a per-channel promise chain so messages
     * are emitted in arrival order even though decrypt is async. A frame whose
     * `encoding` isn't a cipher encoding passes through unchanged.
     */
    private deliverSingle;
    /** Drive the state machine from connection lifecycle changes. */
    private onConnectionState;
    /** True while the channel is in an attach-related state worth transitioning out of. */
    private isLive;
    private transition;
}
/**
 * Per-channel presence facade. Wraps the `pres` frame and `presEvt`
 * listener dispatch.
 */
export declare class Presence extends TypedEventEmitter<PresenceEventType, PresenceEventListener, PresenceEventResult> {
    private readonly connection;
    private readonly channelName;
    private readonly channel;
    private readonly cipher;
    /** Serializes async decryption so presence events keep their arrival order. */
    private decryptChain;
    constructor(connection: Connection, channelName: string, channel: Channel, cipher: Cipher | null);
    /**
     * Register a listener for presence events. Implicitly attaches the
     * underlying channel — presence events arrive on the same WebSocket
     * subscription as message frames.
     */
    on(listener: PresenceEventListener): UnsubscribeFn;
    /** Register a listener for presence events with a matching action. */
    on(event: PresenceEventType, listener: PresenceEventListener): UnsubscribeFn;
    /** Resolve the next presence event with the matching action. */
    once(event: PresenceEventType): Promise<PresenceEventResult>;
    /** Invoke `listener` one time for the next presence event. */
    once(listener: PresenceEventListener): void;
    /** Invoke `listener` one time for the next presence event with a matching action. */
    once(event: PresenceEventType, listener: PresenceEventListener): void;
    subscribe(listener: PresenceEventListener): UnsubscribeFn;
    /** Announce this connection as present in the channel. */
    enter(data?: unknown): Promise<void>;
    /** Update the data attached to this connection's presence entry. */
    update(data?: unknown): Promise<void>;
    /** Remove this connection's presence entry. */
    leave(): Promise<void>;
    /**
     * @internal Dispatch a presence frame from the Connection transport,
     * decrypting its data first when a cipher is set. Decryption is serialized
     * through a promise chain so events keep their arrival order.
     */
    private emitPresence;
    private send;
}
//# sourceMappingURL=channel.d.ts.map