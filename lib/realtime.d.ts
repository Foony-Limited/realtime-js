/**
 * Realtime is the top-level client class. It owns a Connection and a
 * `channels.get(name)` registry — the public entry point for app code.
 */
import { Channel } from './channel.js';
import { Connection, type ConnectionOptions, type ConnectionState } from './connection.js';
/** Options for the Realtime client; mirrors ConnectionOptions. */
export type RealtimeOptions = ConnectionOptions;
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
         */
        get: (name: string) => Channel;
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