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
import type { MessageFrame, PresenceAction, PresenceEventFrame } from './wire.js';
/** Listener handle returned by `subscribe` — call to remove the listener. */
export type UnsubscribeFn = EventUnsubscribeFn;
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
    private attachPromise;
    private channelState;
    constructor(connection: Connection, name: string);
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
     */
    publish(name: string, data: unknown): Promise<void>;
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
    constructor(connection: Connection, channelName: string, channel: Channel);
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
    /** @internal Dispatch a presence frame from the Connection transport. */
    private emitPresence;
    private send;
}
//# sourceMappingURL=channel.d.ts.map