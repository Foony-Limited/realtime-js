/**
 * Realtime is the top-level client class. It owns a Connection and a
 * `channels.get(name)` registry — the public entry point for app code.
 */

import { Channel } from './channel.js';
import {
  Connection,
  type ConnectionOptions,
  type ConnectionState,
  type ConnectionStateListener,
} from './connection.js';

/** Options for the Realtime client; mirrors ConnectionOptions. */
export type RealtimeOptions = ConnectionOptions;

/**
 * Realtime client — call `new Realtime({ url, token })` and use
 * `client.channels.get('chat:1')` to start sending and receiving.
 */
export class Realtime {
  private readonly connection: Connection;
  private readonly channelsByName = new Map<string, Channel>();

  /** Map-like accessor for channels. Stable instance per name. */
  readonly channels = {
    get: (name: string): Channel => {
      let existing = this.channelsByName.get(name);
      if (!existing) {
        existing = new Channel(this.connection, name);
        this.channelsByName.set(name, existing);
      }
      return existing;
    },
    release: (name: string): void => {
      const channel = this.channelsByName.get(name);
      if (!channel) return;
      this.channelsByName.delete(name);
      this.connection.removeChannelListeners(name);
      channel.detach().catch(() => {});
    },
  };

  constructor(options: RealtimeOptions) {
    this.connection = new Connection(options);
  }

  /** Eagerly open the WebSocket. Optional — channels attach lazily. */
  async connect(): Promise<void> {
    await this.connection.connect();
  }

  /** Close the WebSocket and release every channel. */
  async close(): Promise<void> {
    for (const name of [...this.channelsByName.keys()]) {
      this.channels.release(name);
    }
    await this.connection.close();
  }

  /** Current connection state. */
  getState(): ConnectionState {
    return this.connection.getState();
  }

  /** Server-issued connection id, populated after auth. */
  getConnectionId(): string | null {
    return this.connection.getConnectionId();
  }

  /** Server-confirmed client id (from the JWT), populated after auth. */
  getClientId(): string | null {
    return this.connection.getClientId();
  }

  /** Register a connection-state listener. Returns an unsubscribe fn. */
  onStateChange(listener: ConnectionStateListener): () => void {
    return this.connection.onStateChange(listener);
  }
}
