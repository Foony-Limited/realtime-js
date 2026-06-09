/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 */
import { TypedEventEmitter, type Connection, type EventUnsubscribeFn, type MessageListener, type PresenceEventListener } from './connection.js';
import type { MessageFrame, PresenceAction, PresenceEventFrame } from './wire.js';
/** Listener handle returned by `subscribe` — call to remove the listener. */
export type UnsubscribeFn = EventUnsubscribeFn;
/** Message names emitted by a channel. */
export type ChannelEventType = string;
/** Listener for channel messages. */
export type ChannelEventListener = MessageListener;
/** Result returned by promise-based `channel.once(event)`. */
export type ChannelEventResult = MessageFrame;
/** Presence event names emitted by a channel presence facade. */
export type PresenceEventType = PresenceAction;
/** Result returned by promise-based `presence.once(event)`. */
export type PresenceEventResult = PresenceEventFrame;
/**
 * One subscription handle per (channel, listener) pair. Channels are
 * value-equal by name on a given Realtime client — calling
 * `client.channels.get('chat:1')` twice returns the same instance.
 */
export declare class Channel extends TypedEventEmitter<ChannelEventType, ChannelEventListener, ChannelEventResult> {
    readonly name: string;
    readonly presence: Presence;
    private readonly connection;
    private attachPromise;
    private attached;
    constructor(connection: Connection, name: string);
    /**
     * Ensure the server is subscribed to this channel. Called implicitly
     * by `on()` / `subscribe()` and `presence.on()` / `presence.subscribe()`; expose it so callers
     * can pre-attach if they want to surface attach errors before the
     * first message arrives.
     */
    attach(): Promise<void>;
    /**
     * Detach from the server (stop receiving messages and presence
     * events). Local listeners are preserved — call `off()` or `unsubscribe()` to
     * clear them.
     */
    detach(): Promise<void>;
    /**
     * Register a listener for message frames on this channel. Implicitly
     * attaches if needed. Returns an unsubscribe function.
     */
    on(listener: ChannelEventListener): UnsubscribeFn;
    /** Register a listener for messages with a matching `name`. */
    on(event: ChannelEventType, listener: ChannelEventListener): UnsubscribeFn;
    /** Resolve the next message with the matching `name`. */
    once(event: ChannelEventType): Promise<ChannelEventResult>;
    /** Invoke `listener` one time for the next message on this channel. */
    once(listener: ChannelEventListener): void;
    /** Invoke `listener` one time for the next message with a matching `name`. */
    once(event: ChannelEventType, listener: ChannelEventListener): void;
    subscribe(listener: MessageListener): UnsubscribeFn;
    /** Publish one application-level message to the channel. */
    publish(name: string, data: unknown): Promise<void>;
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