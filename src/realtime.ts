/**
 * Realtime is the top-level client class. It owns a Connection and a
 * `channels.get(name)` registry — the public entry point for app code.
 */

import { Auth } from './auth.js';
import { Channel, type BatchOptions } from './channel.js';
import { Connection, type ConnectionOptions, type ConnectionState } from './connection.js';
import type { CipherParams } from './crypto.js';

/** Options for the Realtime client; the connection options plus a default batch config. */
export type RealtimeOptions = ConnectionOptions & {
  /**
   * Tuning for the always-on auto-batching applied to every channel
   * (overridable per channel). Batching is on by default; this only adjusts it.
   */
  readonly batch?: BatchOptions;
};

/** Per-channel options passed to `channels.get(name, options)`. */
export type ChannelOptions = {
  /** Enable end-to-end payload encryption on this channel. */
  readonly cipher?: CipherParams;
  /** Auto-batch tuning for this channel; overrides the client-level default. */
  readonly batch?: BatchOptions;
};

/** One message in a {@link BatchSpec}. */
export type BatchMessage = {
  /** Application-level event name. */
  readonly name: string;
  /** Arbitrary JSON-serializable payload. */
  readonly data: unknown;
};

/** A batch-publish spec: send `messages` to each of `channels`. */
export type BatchSpec = {
  /** One channel name or a list of them. */
  readonly channels: string | readonly string[];
  /** One message or a list of them, published to every channel in `channels`. */
  readonly messages: BatchMessage | readonly BatchMessage[];
};

/** Per-channel outcome from {@link Realtime.batchPublish}. */
export type BatchPublishResult = {
  /** Number of channels published successfully. */
  readonly successCount: number;
  /** Number of channels that failed. */
  readonly failureCount: number;
  /** One entry per (spec × channel); `error` is set when that channel failed. */
  readonly results: ReadonlyArray<{ readonly channel: string; readonly error?: Error }>;
};

/** Batch limits, enforced client-side. */
const MAX_BATCH_CHANNELS = 100;
const MAX_BATCH_MESSAGES = 1000;

/**
 * Realtime client — call `new Realtime({ token })` and use
 * `client.channels.get('chat:1')` to start sending and receiving.
 */
export class Realtime {
  readonly connection: Connection;
  /** Token-minting namespace. Signs with the client's key. */
  readonly auth: Auth;
  private readonly channelsByName = new Map<string, Channel>();
  private readonly batchDefault: BatchOptions | undefined;

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
        existing = new Channel(this.connection, name, options?.cipher, options?.batch ?? this.batchDefault);
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
    this.auth = new Auth(() => this.connection.options.key);
    this.batchDefault = options.batch;
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

  /**
   * Publish messages to many channels in one call. Each spec
   * sends its `messages` to each of its `channels`; messages to a single channel
   * go as one idempotent batch frame. This is publish-only — it does not attach
   * or subscribe the channels (so it scales to many channels), and it sends
   * payloads as-is (use `channel.publish` for an end-to-end-encrypted channel).
   *
   * @returns Per-channel success/failure; one channel failing does not fail the others.
   */
  async batchPublish(specs: BatchSpec | readonly BatchSpec[]): Promise<BatchPublishResult> {
    const list = Array.isArray(specs) ? (specs as readonly BatchSpec[]) : [specs as BatchSpec];
    const channelMessages = new Map<string, BatchMessage[]>();
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
        } else {
          channelMessages.set(channel, [...messages]);
        }
      }
    }
    if (channelMessages.size > MAX_BATCH_CHANNELS) {
      throw new Error(`batchPublish: at most ${MAX_BATCH_CHANNELS} channels per request`);
    }

    const results = await Promise.all(
      [...channelMessages].map(([channel, messages]) =>
        this.connection['publish']({ t: 'pub', channel, name: '', data: null, messages })
          .then((): { channel: string; error?: Error } => ({ channel }))
          .catch((error: unknown): { channel: string; error?: Error } => ({ channel, error: error instanceof Error ? error : new Error(String(error)) })),
      ),
    );
    const failureCount = results.filter((result) => result.error !== undefined).length;
    return { successCount: results.length - failureCount, failureCount, results };
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

/** Narrow a single-or-array of messages to the array case. */
function isMessageArray(value: BatchMessage | readonly BatchMessage[]): value is readonly BatchMessage[] {
  return Array.isArray(value);
}
