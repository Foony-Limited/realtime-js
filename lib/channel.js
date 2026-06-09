/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 */
import { TypedEventEmitter } from './connection.js';
/**
 * One subscription handle per (channel, listener) pair. Channels are
 * value-equal by name on a given Realtime client — calling
 * `client.channels.get('chat:1')` twice returns the same instance.
 */
export class Channel extends TypedEventEmitter {
    name;
    presence;
    connection;
    attachPromise = null;
    attached = false;
    constructor(connection, name) {
        super((_event, args) => args[0]);
        this.connection = connection;
        this.name = name;
        this.presence = new Presence(connection, name, this);
        this.connection['registerChannel'](this.name, {
            message: (message) => this.emit(message.name, message),
            presence: (event) => this.presence['emitPresence'](event),
        });
    }
    /**
     * Ensure the server is subscribed to this channel. Called implicitly
     * by `on()` / `subscribe()` and `presence.on()` / `presence.subscribe()`; expose it so callers
     * can pre-attach if they want to surface attach errors before the
     * first message arrives.
     */
    async attach() {
        if (this.attached)
            return;
        if (this.attachPromise)
            return this.attachPromise;
        this.attachPromise = this.connection['request']({ t: 'sub', channel: this.name })
            .then(() => {
            this.attached = true;
            this.connection['rememberSubscription'](this.name);
        })
            .finally(() => {
            this.attachPromise = null;
        });
        return this.attachPromise;
    }
    /**
     * Detach from the server (stop receiving messages and presence
     * events). Local listeners are preserved — call `off()` or `unsubscribe()` to
     * clear them.
     */
    async detach() {
        if (!this.attached)
            return;
        await this.connection['request']({ t: 'unsub', channel: this.name });
        this.attached = false;
        this.connection['forgetSubscription'](this.name);
    }
    on(first, second) {
        const unsubscribe = second === undefined ? super.on(first) : super.on(first, second);
        // Fire-and-forget attach; the listener stays registered even if
        // attach fails so a retry-on-reconnect surfaces the right state.
        this.attach().catch(() => { });
        return unsubscribe;
    }
    once(first, second) {
        if (second === undefined && typeof first !== 'function') {
            const result = super.once(first);
            this.attach().catch(() => { });
            return result;
        }
        if (second === undefined) {
            super.once(first);
            this.attach().catch(() => { });
            return;
        }
        super.once(first, second);
        this.attach().catch(() => { });
    }
    subscribe(listener) {
        return this.on(listener);
    }
    /** Publish one application-level message to the channel. */
    async publish(name, data) {
        await this.attach();
        await this.connection['request']({ t: 'pub', channel: this.name, name, data });
    }
}
/**
 * Per-channel presence facade. Wraps the `pres` frame and `presEvt`
 * listener dispatch.
 */
export class Presence extends TypedEventEmitter {
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
//# sourceMappingURL=channel.js.map