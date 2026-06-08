/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 */
/**
 * One subscription handle per (channel, listener) pair. Channels are
 * value-equal by name on a given Realtime client — calling
 * `client.channels.get('chat:1')` twice returns the same instance.
 */
export class Channel {
    name;
    presence;
    connection;
    attachPromise = null;
    attached = false;
    constructor(connection, name) {
        this.connection = connection;
        this.name = name;
        this.presence = new Presence(connection, name, this);
    }
    /**
     * Ensure the server is subscribed to this channel. Called implicitly
     * by `subscribe()` and `presence.subscribe()`; expose it so callers
     * can pre-attach if they want to surface attach errors before the
     * first message arrives.
     */
    async attach() {
        if (this.attached)
            return;
        if (this.attachPromise)
            return this.attachPromise;
        this.attachPromise = this.connection
            .request({ t: 'sub', channel: this.name })
            .then(() => {
            this.attached = true;
            this.connection.rememberSubscription(this.name);
        })
            .finally(() => {
            this.attachPromise = null;
        });
        return this.attachPromise;
    }
    /**
     * Detach from the server (stop receiving messages and presence
     * events). Local listeners are preserved — call `unsubscribe()` to
     * clear them.
     */
    async detach() {
        if (!this.attached)
            return;
        await this.connection.request({ t: 'unsub', channel: this.name });
        this.attached = false;
        this.connection.forgetSubscription(this.name);
    }
    /**
     * Register a listener for message frames on this channel. Implicitly
     * attaches if needed. Returns an unsubscribe function.
     */
    subscribe(listener) {
        const listeners = this.connection.addChannelListeners(this.name);
        listeners.messages.add(listener);
        // Fire-and-forget attach; the listener stays registered even if
        // attach fails so a retry-on-reconnect surfaces the right state.
        this.attach().catch(() => { });
        return () => {
            listeners.messages.delete(listener);
        };
    }
    /** Publish one application-level message to the channel. */
    async publish(name, data) {
        await this.attach();
        await this.connection.request({ t: 'pub', channel: this.name, name, data });
    }
}
/**
 * Per-channel presence facade. Wraps the `pres` frame and `presEvt`
 * listener dispatch.
 */
export class Presence {
    connection;
    channelName;
    channel;
    constructor(connection, channelName, channel) {
        this.connection = connection;
        this.channelName = channelName;
        this.channel = channel;
    }
    /**
     * Register a listener for presence events. Implicitly attaches the
     * underlying channel — presence events arrive on the same WebSocket
     * subscription as message frames.
     */
    subscribe(listener) {
        const listeners = this.connection.addChannelListeners(this.channelName);
        listeners.presence.add(listener);
        this.channel.attach().catch(() => { });
        return () => {
            listeners.presence.delete(listener);
        };
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
    async send(action, data) {
        await this.channel.attach();
        await this.connection.request({
            t: 'pres',
            channel: this.channelName,
            action,
            ...(data === undefined ? {} : { data }),
        });
    }
}
//# sourceMappingURL=channel.js.map