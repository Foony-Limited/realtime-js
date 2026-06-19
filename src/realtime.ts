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
export class Realtime {
  readonly connection: Connection;
  private readonly channelsByName = new Map<string, Channel>();

  /** Map-like accessor for channels. Stable instance per name. */
  readonly channels = {
    /**
     * Gets a channel by `name` (or creates the channel if it doesn't yet exist). `name` may only
     * consist of the characters: `/[a-zA-Z0-9._-:]+/`, and may not start with a ':'.
     *
     * `options` (e.g. `cipher`) apply when the channel is first created; passing different
     * options to a later `get` of the same name returns the existing instance unchanged.
     */
    get: (name: string, options?: ChannelOptions): Channel => {
      let existing = this.channelsByName.get(name);
      if (!existing) {
        existing = new Channel(this.connection, name, options?.cipher);
        this.channelsByName.set(name, existing);
      }
      return existing;
    },
    /**
     * Releases a channel by `name`. The channel will be detached and removed from the client.
     * If the channel is not found, this is a no-op.
     */
    release: (name: string): void => {
      const channel = this.channelsByName.get(name);
      if (!channel) return;
      this.channelsByName.delete(name);
      this.connection['unregisterChannel'](name);
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
}
