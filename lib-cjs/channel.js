"use strict";
/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 *
 * Mirrors Ably's split: `on` / `once` / `off` observe the channel's
 * lifecycle *state* (a closed set of events), while `subscribe` /
 * `unsubscribe` carry application *messages* (open-ended event names).
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Presence = exports.Channel = void 0;
const connection_js_1 = require("./connection.js");
/**
 * One subscription handle per (channel, listener) pair. Channels are
 * value-equal by name on a given Realtime client — calling
 * `client.channels.get('chat:1')` twice returns the same instance.
 *
 * `on` / `once` / `off` observe the channel's {@link ChannelState};
 * `subscribe` / `unsubscribe` receive {@link MessageFrame} messages.
 */
class Channel extends connection_js_1.TypedEventEmitter {
    name;
    presence;
    connection;
    messages = new ChannelMessageEmitter((_event, args) => args[0]);
    attachPromise = null;
    channelState = 'initialized';
    constructor(connection, name) {
        super((_event, args) => args[0]);
        this.connection = connection;
        this.name = name;
        this.presence = new Presence(connection, name, this);
        this.connection['registerChannel'](this.name, {
            message: (message) => this.messages.dispatch(message),
            presence: (event) => this.presence['emitPresence'](event),
        });
        this.connection.on((state, reason) => this.onConnectionState(state, reason));
    }
    /** Current channel lifecycle state. */
    get state() {
        return this.channelState;
    }
    /**
     * Ensure the server is subscribed to this channel. Called implicitly
     * by `subscribe()` and `presence.subscribe()`; expose it so callers
     * can pre-attach if they want to surface attach errors before the
     * first message arrives.
     */
    async attach() {
        if (this.channelState === 'attached')
            return;
        if (this.attachPromise)
            return this.attachPromise;
        this.transition('attaching');
        this.attachPromise = this.connection['request']({ t: 'sub', channel: this.name })
            .then(() => {
            this.connection['rememberSubscription'](this.name);
            this.transition('attached', { resumed: false });
        })
            .catch((error) => {
            this.transition('failed', { reason: asError(error) });
            throw error;
        })
            .finally(() => {
            this.attachPromise = null;
        });
        return this.attachPromise;
    }
    /**
     * Detach from the server (stop receiving messages and presence
     * events). Local listeners are preserved — call `off()` or
     * `unsubscribe()` to clear them.
     */
    async detach() {
        if (this.channelState === 'initialized' || this.channelState === 'detached' || this.channelState === 'detaching')
            return;
        this.transition('detaching');
        try {
            await this.connection['request']({ t: 'unsub', channel: this.name });
        }
        finally {
            this.connection['forgetSubscription'](this.name);
            this.transition('detached');
        }
    }
    subscribe(first, second) {
        let unsubscribe;
        if (typeof first === 'function') {
            unsubscribe = this.messages.on(first);
        }
        else if (typeof first === 'string') {
            unsubscribe = this.messages.on(first, second);
        }
        else {
            const listener = second;
            const offs = first.map((event) => this.messages.on(event, listener));
            unsubscribe = () => {
                for (const off of offs) {
                    off();
                }
            };
        }
        // Fire-and-forget attach; the listener stays registered even if
        // attach fails so a retry-on-reconnect surfaces the right state.
        this.attach().catch(() => { });
        return unsubscribe;
    }
    unsubscribe(first, second) {
        if (first === undefined) {
            this.messages.off();
            return;
        }
        if (typeof first === 'function') {
            this.messages.off(first);
            return;
        }
        if (typeof first === 'string') {
            this.messages.off(first, second);
            return;
        }
        for (const event of first) {
            this.messages.off(event, second);
        }
    }
    /**
     * Publish one application-level message to the channel.
     *
     * @param name - The event name.
     * @param data - The data to publish.
     */
    async publish(name, data) {
        await this.attach();
        await this.connection['request']({ t: 'pub', channel: this.name, name, data });
    }
    /** Drive the state machine from connection lifecycle changes. */
    onConnectionState(state, reason) {
        if (state === 'disconnected' && this.channelState === 'attached') {
            this.transition('suspended', { reason: reason ?? new Error('connection disconnected') });
            return;
        }
        if (state === 'connected') {
            // The connection restores remembered subscriptions on reconnect, so
            // reflect the resume back to channel state listeners.
            if (this.channelState === 'suspended') {
                this.transition('attached', { resumed: true });
            }
            else if (this.channelState === 'attached') {
                this.emit('update', { current: 'attached', previous: 'attached', resumed: true });
            }
            return;
        }
        if (state === 'failed' && this.isLive()) {
            this.transition('failed', reason !== undefined ? { reason } : undefined);
            return;
        }
        if (state === 'closed' && this.isLive()) {
            this.transition('detached');
        }
    }
    /** True while the channel is in an attach-related state worth transitioning out of. */
    isLive() {
        return this.channelState !== 'initialized' && this.channelState !== 'detached' && this.channelState !== 'failed';
    }
    transition(next, options) {
        if (this.channelState === next)
            return;
        const previous = this.channelState;
        this.channelState = next;
        this.emit(next, {
            current: next,
            previous,
            resumed: options?.resumed ?? false,
            ...(options?.reason !== undefined ? { reason: options.reason } : {}),
        });
    }
}
exports.Channel = Channel;
/**
 * Per-channel presence facade. Wraps the `pres` frame and `presEvt`
 * listener dispatch.
 */
class Presence extends connection_js_1.TypedEventEmitter {
    connection;
    channelName;
    channel;
    constructor(connection, channelName, channel) {
        super((_event, args) => args[0]);
        this.connection = connection;
        this.channelName = channelName;
        this.channel = channel;
    }
    on(first, second) {
        const unsubscribe = second === undefined ? super.on(first) : super.on(first, second);
        this.channel.attach().catch(() => { });
        return unsubscribe;
    }
    once(first, second) {
        if (second === undefined && typeof first !== 'function') {
            const result = super.once(first);
            this.channel.attach().catch(() => { });
            return result;
        }
        if (second === undefined) {
            super.once(first);
            this.channel.attach().catch(() => { });
            return;
        }
        super.once(first, second);
        this.channel.attach().catch(() => { });
    }
    subscribe(listener) {
        return this.on(listener);
    }
    /** Announce this connection as present in the channel. */
    async enter(data) {
        await this.send('enter', data);
    }
    /** Update the data attached to this connection's presence entry. */
    async update(data) {
        await this.send('update', data);
    }
    /** Remove this connection's presence entry. */
    async leave() {
        await this.send('leave', undefined);
    }
    /** @internal Dispatch a presence frame from the Connection transport. */
    emitPresence(event) {
        this.emit(event.action, event);
    }
    async send(action, data) {
        await this.channel.attach();
        await this.connection['request']({
            t: 'pres',
            channel: this.channelName,
            action,
            ...(data === undefined ? {} : { data }),
        });
    }
}
exports.Presence = Presence;
/**
 * Message-name event emitter for a channel. Separate from the channel's
 * own state emitter so `subscribe` (messages) and `on` (state) don't
 * collide; exposes `dispatch` so the Connection transport can deliver
 * frames into it.
 */
class ChannelMessageEmitter extends connection_js_1.TypedEventEmitter {
    /** Deliver a message frame to listeners keyed by its `name`. */
    dispatch(message) {
        this.emit(message.name, message);
    }
}
/** Coerce an unknown thrown value into an Error for state-change reasons. */
function asError(error) {
    return error instanceof Error ? error : new Error(String(error));
}
//# sourceMappingURL=channel.js.map