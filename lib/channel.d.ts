/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 */
import type { Connection, MessageListener, PresenceEventListener } from './connection.js';
/** Listener handle returned by `subscribe` — call to remove the listener. */
export type UnsubscribeFn = () => void;
/**
 * One subscription handle per (channel, listener) pair. Channels are
 * value-equal by name on a given Realtime client — calling
 * `client.channels.get('chat:1')` twice returns the same instance.
 */
export declare class Channel {
    readonly name: string;
    readonly presence: Presence;
    private readonly connection;
    private attachPromise;
    private attached;
    constructor(connection: Connection, name: string);
    /**
     * Ensure the server is subscribed to this channel. Called implicitly
     * by `subscribe()` and `presence.subscribe()`; expose it so callers
     * can pre-attach if they want to surface attach errors before the
     * first message arrives.
     */
    attach(): Promise<void>;
    /**
     * Detach from the server (stop receiving messages and presence
     * events). Local listeners are preserved — call `unsubscribe()` to
     * clear them.
     */
    detach(): Promise<void>;
    /**
     * Register a listener for message frames on this channel. Implicitly
     * attaches if needed. Returns an unsubscribe function.
     */
    subscribe(listener: MessageListener): UnsubscribeFn;
    /** Publish one application-level message to the channel. */
    publish(name: string, data: unknown): Promise<void>;
}
/**
 * Per-channel presence facade. Wraps the `pres` frame and `presEvt`
 * listener dispatch.
 */
export declare class Presence {
    private readonly connection;
    private readonly channelName;
    private readonly channel;
    constructor(connection: Connection, channelName: string, channel: Channel);
    /**
     * Register a listener for presence events. Implicitly attaches the
     * underlying channel — presence events arrive on the same WebSocket
     * subscription as message frames.
     */
    subscribe(listener: PresenceEventListener): UnsubscribeFn;
    /** Announce this connection as present in the channel. */
    enter(data?: unknown): Promise<void>;
    /** Update the data attached to this connection's presence entry. */
    update(data?: unknown): Promise<void>;
    /** Remove this connection's presence entry. */
    leave(): Promise<void>;
    private send;
}
//# sourceMappingURL=channel.d.ts.map