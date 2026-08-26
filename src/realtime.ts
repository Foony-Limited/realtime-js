/**
 * Realtime is the top-level client class. It owns a Connection and a
 * `channels.get(name)` registry, and is the public entry point for app code.
 */

import { Auth } from './auth.js';
import { Channel, type BatchOptions } from './channel.js';
import { Connection, type ConnectionOptions, type ConnectionState } from './connection.js';
import type { CipherParams } from './crypto.js';

/** Options for the {@link Realtime} client: the {@link ConnectionOptions} plus a default batch config. */
export type RealtimeOptions = ConnectionOptions & {
  /**
   * Configuration for the always-on auto-batching applied to every channel,
   * overridable per channel via {@link ChannelOptions.batch}. Batching is on
   * by default. Defaults are documented on {@link BatchOptions}.
   */
  readonly batch?: BatchOptions;
};

/**
 * Per-channel options passed to `channels.get(name, options)`. They apply only
 * on the first `get` of a given name (see {@link Realtime.channels}).
 */
export type ChannelOptions = {
  /**
   * Enable end-to-end payload encryption on this channel with the given {@link CipherParams}.
   * This prevents the Foony backend from seeing the plaintext `data` of messages published to this channel.
   * The `cipher` key should be kept private and never shared with the public or our backend.
   */
  readonly cipher?: CipherParams;
  /** Auto-batch tuning for this channel. Overrides the {@link RealtimeOptions.batch} default. */
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
  /** One channel name or a list of them to which `messages` will be published. */
  readonly channels: string | readonly string[];
  /** One message or a list of them, published to every channel in `channels`. */
  readonly messages: BatchMessage | readonly BatchMessage[];
};

/** Per-channel results from {@link Realtime.batchPublish}. */
export type BatchPublishResult = {
  /** Number of channels published successfully. */
  readonly successCount: number;
  /** Number of channels that failed to publish. */
  readonly failureCount: number;
  /** One entry per channel. `error` is set when that channel's publish failed. */
  readonly results: ReadonlyArray<{ readonly channel: string; readonly error?: Error }>;
};

/** Maximum number of channels per batch. */
const MAX_BATCH_CHANNELS = 100;

/** Maximum number of messages per channel per batch. */
const MAX_BATCH_MESSAGES = 1000;

/** Maximum channel name length, matching the server's `MaxChannelNameLength`. */
const MAX_CHANNEL_NAME_LENGTH = 255;

/**
 * The realtime client and the entry point for app code. It owns one WebSocket
 * {@link Connection} (opened lazily on first use) and a map of
 * {@link Channel} instances retrieved via {@link Realtime.channels | `channels.get(name)`}.
 * See the [getting started guide](https://foony.io/docs/getting-started) for
 * the auth options and a full walkthrough.
 *
 * @example
 * ```ts
 * // Prefer `authCallback` over `key` for production apps.
 * const client = new Realtime({ authCallback: () => fetchTokenFromYourServer() });
 * const channel = client.channels.get('chat:room-1');
 * channel.subscribe((message) => {
 *   console.log(message.name, message.data);
 * });
 * await channel.publish('greeting', { text: 'hi' });
 * ```
 */
export class Realtime {
  /**
   * The underlying {@link Connection}. Listen on lifecycle state with
   * `connection.on(...)` and read it with {@link Realtime.getState | `getState()`}.
   */
  readonly connection: Connection;
  /**
   * Token-minting namespace. Signs with the client's key. See the
   * [auth docs](https://foony.io/docs/auth) for when to mint tokens yourself.
   */
  readonly auth: Auth;
  private readonly channelsByName = new Map<string, Channel>();
  private readonly batchDefault: BatchOptions | undefined;

  /** Map-like accessor for channels. Stable instance per name. */
  readonly channels = {
    /**
     * Get the {@link Channel} named `name`, creating it on first use. The same
     * name always returns the same instance. `name` is 1 to 255 characters
     * from `A-Z a-z 0-9 : - _` and may not start with a ':'. Colons express
     * hierarchy (e.g. `chat:rooms:42`), dots are not allowed, and the server
     * rejects an invalid name with `BadFrame` (40001).
     *
     * `options` (e.g. `cipher`) apply when the channel is first created.
     * Passing different {@link ChannelOptions} to a later `get` of the same
     * name returns the existing instance unchanged.
     */
    get: (name: string, options?: ChannelOptions): Channel => {
      // Enforce the server's grammar client-side so a bad name fails loudly
      // here instead of attach-looping against `BadFrame` rejections.
      if (!isValidChannelName(name)) {
        throw new Error(`channels.get: invalid channel name "${name}" (allowed: A-Z a-z 0-9 : - _, at most 255 characters, not starting with ':')`);
      }
      let existing = this.channelsByName.get(name);
      if (!existing) {
        existing = new Channel(this.connection, name, options?.cipher, options?.batch ?? this.batchDefault);
        this.channelsByName.set(name, existing);
      }
      return existing;
    },

    /**
     * Release the channel named `name`. The channel is detached and removed
     * from the client, so a later `get` of the same name returns a fresh
     * instance. A no-op when no channel with that name exists.
     */
    release: (name: string): void => {
      const channel = this.channelsByName.get(name);
      if (!channel) return;
      this.channelsByName.delete(name);
      this.connection['unregisterChannel'](name);
      // Remove the channel's connection state listener, or every released
      // instance would be retained (and keep running its state machine) for
      // the life of the client.
      channel['dispose']();
      channel.detach().catch(() => {});
    },
  };

  constructor(options: RealtimeOptions) {
    this.connection = new Connection(options);
    this.auth = new Auth(() => this.connection.options.key);
    this.batchDefault = options.batch;
  }

  /**
   * Eagerly open the WebSocket and complete the auth handshake. Calling this
   * is optional: channels connect and attach lazily on first use. This
   * method is idempotent, and concurrent calls await the same in-flight
   * connect. Resolves once the connection is `connected`. Rejects with the
   * handshake error when auth fails (for example a bad key, or an expired
   * static `token` with no `authCallback` to re-mint one).
   */
  async connect(): Promise<void> {
    await this.connection.connect();
  }

  /**
   * Close the connection but keep every channel: listeners, subscriptions,
   * and resume cursors all survive. The next {@link connect | `connect()`}
   * re-attaches everything and replays missed messages (within retention).
   * While suspended, channel calls wait for `connect()` instead of
   * reconnecting on their own. Use this instead of
   * {@link close | `close()`} when the client will be used again, e.g. while
   * a browser tab is hidden.
   *
   * @example
   * document.addEventListener('visibilitychange', () => {
   *   if (document.visibilityState === 'hidden') {
   *     void realtime.suspend();
   *   } else {
   *     void realtime.connect();
   *   }
   * });
   */
  async suspend(): Promise<void> {
    await this.connection.suspend();
  }

  /**
   * Close the WebSocket and release every channel. Resolves once the
   * connection reaches `closed`. Publishes still awaiting an ack reject with
   * a "connection closed" error.
   */
  async close(): Promise<void> {
    for (const name of [...this.channelsByName.keys()]) {
      this.channels.release(name);
    }
    await this.connection.close();
  }

  /**
   * Publish messages to many channels in one call. Each {@link BatchSpec}
   * sends its `messages` to each of its `channels`, and all messages to a
   * single channel go as one idempotent batch frame. `batchPublish` is
   * publish-only and does not handle end-to-end encryption. If you need that,
   * use {@link Channel.publish | `channel.publish()`} (which is the preferred
   * method of publishing all messages).
   *
   * A `batchPublish` is limited to at most 100 distinct channels per call and
   * at most 1000 messages per spec. Throws an `Error` before sending anything
   * when a limit is exceeded.
   *
   * @param specs - One {@link BatchSpec} or a list of them.
   * @returns Resolves with a {@link BatchPublishResult}. A channel that fails
   *   shows up there as an `error` entry rather than rejecting the call, so
   *   one channel failing does not fail the others.
   */
  async batchPublish(specs: BatchSpec | readonly BatchSpec[]): Promise<BatchPublishResult> {
    const list = Array.isArray(specs) ? (specs as readonly BatchSpec[]) : [specs as BatchSpec];
    const channelMessages = new Map<string, BatchMessage[]>();
    for (const spec of list) {
      const channels = typeof spec.channels === 'string' ? [spec.channels] : spec.channels;
      const messages = isMessageArray(spec.messages) ? spec.messages : [spec.messages];
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
    // Check the limit on the merged per-channel batches, not per spec: several
    // specs naming the same channel merge into one batch, and that merged batch
    // is what must fit.
    for (const [channel, messages] of channelMessages) {
      if (messages.length > MAX_BATCH_MESSAGES) {
        throw new Error(`batchPublish: at most ${MAX_BATCH_MESSAGES} messages per channel per request (channel "${channel}" has ${messages.length})`);
      }
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

  /** Current {@link ConnectionState}. Observe changes with `connection.on(...)`. */
  getState(): ConnectionState {
    return this.connection.getState();
  }

  /** Server-issued connection id, or `null` before the auth handshake completes. */
  getConnectionId(): string | null {
    return this.connection.getConnectionId();
  }

  /**
   * Client id this connection is authenticated as, or `null` before the auth
   * handshake completes. Never `null` once connected: the server resolves it
   * from the JWT's subject (token / `authCallback` auth), from the
   * `clientId` option (key auth), or assigns one when neither names a client.
   */
  getClientId(): string | null {
    return this.connection.getClientId();
  }
}

/** Narrow a single-or-array of messages to the array case. */
function isMessageArray(value: BatchMessage | readonly BatchMessage[]): value is readonly BatchMessage[] {
  return Array.isArray(value);
}

/**
 * True when `name` satisfies the server's channel grammar: 1 to 255 characters
 * from `A-Z a-z 0-9 : - _`, not starting with ':'. Mirrors
 * `wire.ValidateChannelName` on the Go side (the canonical source).
 */
function isValidChannelName(name: string): boolean {
  if (name.length === 0 || name.length > MAX_CHANNEL_NAME_LENGTH || name.startsWith(':')) {
    return false;
  }
  return /^[A-Za-z0-9:_-]+$/u.test(name);
}
