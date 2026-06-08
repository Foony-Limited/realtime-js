/**
 * Realtime is the top-level client class. It owns a Connection and a
 * `channels.get(name)` registry — the public entry point for app code.
 */
import { Channel } from './channel.js';
import { type ConnectionOptions, type ConnectionState, type ConnectionStateListener } from './connection.js';
/** Options for the Realtime client; mirrors ConnectionOptions. */
export type RealtimeOptions = ConnectionOptions;
/**
 * Realtime client — call `new Realtime({ url, token })` and use
 * `client.channels.get('chat:1')` to start sending and receiving.
 */
export declare class Realtime {
    private readonly connection;
    private readonly channelsByName;
    /** Map-like accessor for channels. Stable instance per name. */
    readonly channels: {
        get: (name: string) => Channel;
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
    /** Register a connection-state listener. Returns an unsubscribe fn. */
    onStateChange(listener: ConnectionStateListener): () => void;
}
//# sourceMappingURL=realtime.d.ts.map