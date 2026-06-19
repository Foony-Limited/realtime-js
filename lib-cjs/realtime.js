"use strict";
/**
 * Realtime is the top-level client class. It owns a Connection and a
 * `channels.get(name)` registry — the public entry point for app code.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Realtime = void 0;
const channel_js_1 = require("./channel.js");
const connection_js_1 = require("./connection.js");
/**
 * Realtime client — call `new Realtime({ token })` and use
 * `client.channels.get('chat:1')` to start sending and receiving.
 */
class Realtime {
    connection;
    channelsByName = new Map();
    /** Map-like accessor for channels. Stable instance per name. */
    channels = {
        /**
         * Gets a channel by `name` (or creates the channel if it doesn't yet exist). `name` may only
         * consist of the characters: `/[a-zA-Z0-9._-:]+/`, and may not start with a ':'.
         *
         * `options` (e.g. `cipher`) apply when the channel is first created; passing different
         * options to a later `get` of the same name returns the existing instance unchanged.
         */
        get: (name, options) => {
            let existing = this.channelsByName.get(name);
            if (!existing) {
                existing = new channel_js_1.Channel(this.connection, name, options?.cipher);
                this.channelsByName.set(name, existing);
            }
            return existing;
        },
        /**
         * Releases a channel by `name`. The channel will be detached and removed from the client.
         * If the channel is not found, this is a no-op.
         */
        release: (name) => {
            const channel = this.channelsByName.get(name);
            if (!channel)
                return;
            this.channelsByName.delete(name);
            this.connection['unregisterChannel'](name);
            channel.detach().catch(() => { });
        },
    };
    constructor(options) {
        this.connection = new connection_js_1.Connection(options);
    }
    /** Eagerly open the WebSocket. Optional — channels attach lazily. */
    async connect() {
        await this.connection.connect();
    }
    /** Close the WebSocket and release every channel. */
    async close() {
        for (const name of [...this.channelsByName.keys()]) {
            this.channels.release(name);
        }
        await this.connection.close();
    }
    /** Current connection state. */
    getState() {
        return this.connection.getState();
    }
    /** Server-issued connection id, populated after auth. */
    getConnectionId() {
        return this.connection.getConnectionId();
    }
    /** Server-confirmed client id (from the JWT), populated after auth. */
    getClientId() {
        return this.connection.getClientId();
    }
}
exports.Realtime = Realtime;
//# sourceMappingURL=realtime.js.map