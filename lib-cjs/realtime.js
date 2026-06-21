"use strict";
/**
 * Realtime is the top-level client class. It owns a Connection and a
 * `channels.get(name)` registry — the public entry point for app code.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Realtime = void 0;
const channel_js_1 = require("./channel.js");
const connection_js_1 = require("./connection.js");
/** Ably's batch limits, enforced client-side. */
const MAX_BATCH_CHANNELS = 100;
const MAX_BATCH_MESSAGES = 1000;
/**
 * Realtime client — call `new Realtime({ token })` and use
 * `client.channels.get('chat:1')` to start sending and receiving.
 */
class Realtime {
    connection;
    channelsByName = new Map();
    batchDefault;
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
                existing = new channel_js_1.Channel(this.connection, name, options?.cipher, options?.batch ?? this.batchDefault);
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
        this.batchDefault = options.batch;
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
    /**
     * Publish messages to many channels in one call (Ably-compatible). Each spec
     * sends its `messages` to each of its `channels`; messages to a single channel
     * go as one idempotent batch frame. This is publish-only — it does not attach
     * or subscribe the channels (so it scales to many channels), and it sends
     * payloads as-is (use `channel.publish` for an end-to-end-encrypted channel).
     *
     * @returns Per-channel success/failure; one channel failing does not fail the others.
     */
    async batchPublish(specs) {
        const list = Array.isArray(specs) ? specs : [specs];
        const channelMessages = new Map();
        for (const spec of list) {
            const channels = typeof spec.channels === 'string' ? [spec.channels] : spec.channels;
            const messages = isMessageArray(spec.messages) ? spec.messages : [spec.messages];
            if (messages.length > MAX_BATCH_MESSAGES) {
                throw new Error(`batchPublish: a spec may carry at most ${MAX_BATCH_MESSAGES} messages`);
            }
            for (const channel of channels) {
                const existing = channelMessages.get(channel);
                if (existing) {
                    existing.push(...messages);
                }
                else {
                    channelMessages.set(channel, [...messages]);
                }
            }
        }
        if (channelMessages.size > MAX_BATCH_CHANNELS) {
            throw new Error(`batchPublish: at most ${MAX_BATCH_CHANNELS} channels per request`);
        }
        const results = await Promise.all([...channelMessages].map(([channel, messages]) => this.connection['publish']({ t: 'pub', channel, name: '', data: null, messages })
            .then(() => ({ channel }))
            .catch((error) => ({ channel, error: error instanceof Error ? error : new Error(String(error)) }))));
        const failureCount = results.filter((result) => result.error !== undefined).length;
        return { successCount: results.length - failureCount, failureCount, results };
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
/** Narrow a single-or-array of messages to the array case. */
function isMessageArray(value) {
    return Array.isArray(value);
}
//# sourceMappingURL=realtime.js.map