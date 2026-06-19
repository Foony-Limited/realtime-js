/**
 * Realtime is the top-level client class. It owns a Connection and a
 * `channels.get(name)` registry — the public entry point for app code.
 */
import { Channel } from './channel.js';
import { Connection, type ConnectionOptions, type ConnectionState } from './connection.js';
import type { CipherParams } from './crypto.js';
/** Options for the Realtime client; mirrors ConnectionOptions. */
export type RealtimeOptions = ConnectionOptions;
/** Per-channel options passed to `channels.get(name, options)`. */
export type ChannelOptions = {
    /** Enable end-to-end payload encryption on this channel. */
    readonly cipher?: CipherParams;
};
/**
 * Realtime client — call `new Realtime({ token })` and use
 * `client.channels.get('chat:1')` to start sending and receiving.
 */
export declare class Realtime {
    readonly connection: Connection;
    private readonly channelsByName;
    /** Map-like accessor for channels. Stable instance per name. */
    readonly channels: {
        /**
         * Gets a channel by `name` (or creates the channel if it doesn't yet exist). `name` may only
         * consist of the characters: `/[a-zA-Z0-9._-:]+/`, and may not start with a ':'.
         *
         * `options` (e.g. `cipher`) apply when the channel is first created; passing different
         * options to a later `get` of the same name returns the existing instance unchanged.
         */
        get: (name: string, options?: ChannelOptions) => Channel;
        /**
         * Releases a channel by `name`. The channel will be detached and removed from the client.
         * If the channel is not found, this is a no-op.
         */
        release: (name: string) => void;
    };
    constructor(options: RealtimeOptions);
    /** Eagerly open the WebSocket. Optional — channels attach lazily. */
    connect(): Promise<void>;
    /** Close the WebSocket and release every channel. */
    close(): Promise<void>;
    /** Current connection state. */
    getState(): ConnectionState;
    /** Server-issued connection id, populated after auth. */
    getConnectionId(): string | null;
    /** Server-confirmed client id (from the JWT), populated after auth. */
    getClientId(): string | null;
}
//# sourceMappingURL=realtime.d.ts.map