/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 *
 * The channel deliberately exposes two separate listener surfaces so callers
 * never confuse lifecycle with data: `on` / `once` / `off` observe the
 * channel's lifecycle *state* (a closed set of events), while `subscribe` /
 * `unsubscribe` carry application *messages* (open-ended event names).
 */

import { TypedEventEmitter, type Connection, type ConnectionState, type EventUnsubscribeFn, type MessageListener, type PresenceEventListener } from './connection.js';
import { Cipher, isCipherEncoding, type CipherParams } from './crypto.js';
import type { MessageFrame, PresenceAction, PresenceEventFrame } from './wire.js';

/** Listener handle returned by `subscribe` — call to remove the listener. */
export type UnsubscribeFn = EventUnsubscribeFn;

/**
 * Channel lifecycle states. A channel walks this set as it attaches to
 * and detaches from the server, exposed so callers can react to (re)attach
 * and failure transitions.
 */
export type ChannelState =
  /** Created locally; no attach has been attempted yet. */
  | 'initialized'
  /** An attach has been requested and is awaiting server confirmation. */
  | 'attaching'
  /** Attached — messages and presence for this channel are flowing. */
  | 'attached'
  /** A detach has been requested and is awaiting server confirmation. */
  | 'detaching'
  /** Detached — no messages or presence are delivered until re-attached. */
  | 'detached'
  /** Temporarily lost (e.g. the connection dropped); the SDK will re-attach on reconnect. */
  | 'suspended'
  /** Attach failed and will not be retried automatically (e.g. permission denied). */
  | 'failed';

/**
 * Events emitted to channel state listeners: every {@link ChannelState}
 * plus `update` (a no-transition re-confirmation, e.g. a resume).
 */
export type ChannelEventType =
  | ChannelState
  /**
   * A change that is not a state transition — the channel stayed in the
   * same state but something was re-confirmed (e.g. the subscription was
   * resumed after a reconnect). Carries `resumed` on the state change.
   */
  | 'update';

/** Payload delivered to channel state listeners on every {@link ChannelEventType}. */
export type ChannelStateChange = {
  /** State the channel is now in. */
  readonly current: ChannelState;
  /** State the channel was in immediately before this event. */
  readonly previous: ChannelState;
  /** Error that caused the transition, when the event was error-driven. */
  readonly reason?: Error;
  /** True when the channel resumed without missing messages (e.g. after a reconnect). */
  readonly resumed: boolean;
};

/** Listener for channel lifecycle state changes. */
export type ChannelStateListener = (stateChange: ChannelStateChange) => void;

/** Listener for channel lifecycle state changes (alias of {@link ChannelStateListener}). */
export type ChannelEventListener = ChannelStateListener;

/** Result returned by promise-based `channel.once(event)`. */
export type ChannelEventResult = ChannelStateChange;

/** Presence event names emitted by a channel presence facade. */
export type PresenceEventType = PresenceAction;

/** Result returned by promise-based `presence.once(event)`. */
export type PresenceEventResult = PresenceEventFrame;

/**
 * One subscription handle per (channel, listener) pair. Channels are
 * value-equal by name on a given Realtime client — calling
 * `client.channels.get('chat:1')` twice returns the same instance.
 *
 * `on` / `once` / `off` observe the channel's {@link ChannelState};
 * `subscribe` / `unsubscribe` receive {@link MessageFrame} messages.
 */
export class Channel extends TypedEventEmitter<ChannelEventType, ChannelStateListener, ChannelStateChange> {
  readonly name: string;
  readonly presence: Presence;
  private readonly connection: Connection;
  private readonly messages = new ChannelMessageEmitter((_event, args) => args[0]);
  private readonly cipher: Cipher | null;
  /** Serializes async decryption so encrypted messages keep their arrival order. */
  private decryptChain: Promise<void> = Promise.resolve();
  private attachPromise: Promise<void> | null = null;
  private channelState: ChannelState = 'initialized';

  constructor(connection: Connection, name: string, cipher?: CipherParams) {
    super((_event, args) => args[0]);
    this.connection = connection;
    this.name = name;
    this.cipher = cipher ? new Cipher(cipher) : null;
    this.presence = new Presence(connection, name, this, this.cipher);
    this.connection['registerChannel'](this.name, {
      message: (message) => this.deliverMessage(message),
      presence: (event) => this.presence['emitPresence'](event),
    });
    this.connection.on((state, reason) => this.onConnectionState(state, reason));
  }

  /** Current channel lifecycle state. */
  get state(): ChannelState {
    return this.channelState;
  }

  /**
   * Ensure the server is subscribed to this channel. Called implicitly
   * by `subscribe()` and `presence.subscribe()`; expose it so callers
   * can pre-attach if they want to surface attach errors before the
   * first message arrives.
   */
  async attach(): Promise<void> {
    if (this.channelState === 'attached') return;
    if (this.attachPromise) return this.attachPromise;
    this.transition('attaching');
    this.attachPromise = this.connection['request']({ t: 'sub', channel: this.name })
      .then(() => {
        this.connection['rememberSubscription'](this.name);
        this.transition('attached', { resumed: false });
      })
      .catch((error: unknown) => {
        this.transition('failed', { reason: asError(error) });
        throw error;
      })
      .finally(() => {
        this.attachPromise = null;
      });
    return this.attachPromise;
  }

  /**
   * Detach from the server (stop receiving messages and presence
   * events). Local listeners are preserved — call `off()` or
   * `unsubscribe()` to clear them.
   */
  async detach(): Promise<void> {
    if (this.channelState === 'initialized' || this.channelState === 'detached' || this.channelState === 'detaching') return;
    this.transition('detaching');
    try {
      await this.connection['request']({ t: 'unsub', channel: this.name });
    } finally {
      this.connection['forgetSubscription'](this.name);
      this.transition('detached');
    }
  }

  /**
   * Register a listener for every message frame on this channel.
   * Implicitly attaches if needed. Returns an unsubscribe function.
   */
  subscribe(listener: MessageListener): UnsubscribeFn;
  /** Register a listener for messages with a matching `name`. */
  subscribe(event: string, listener: MessageListener): UnsubscribeFn;
  /** Register one listener for messages matching any name in `events`. */
  subscribe(events: readonly string[], listener: MessageListener): UnsubscribeFn;
  subscribe(first: string | readonly string[] | MessageListener, second?: MessageListener): UnsubscribeFn {
    let unsubscribe: UnsubscribeFn;
    if (typeof first === 'function') {
      unsubscribe = this.messages.on(first);
    } else if (typeof first === 'string') {
      unsubscribe = this.messages.on(first, second as MessageListener);
    } else {
      const listener = second as MessageListener;
      const offs = first.map((event) => this.messages.on(event, listener));
      unsubscribe = () => {
        for (const off of offs) {
          off();
        }
      };
    }
    // Fire-and-forget attach; the listener stays registered even if
    // attach fails so a retry-on-reconnect surfaces the right state.
    this.attach().catch(() => {});
    return unsubscribe;
  }

  /** Remove every message listener on this channel. */
  unsubscribe(): void;
  /** Remove `listener` wherever it was registered for messages. */
  unsubscribe(listener: MessageListener): void;
  /** Remove `listener` only from messages with a matching `name`. */
  unsubscribe(event: string, listener: MessageListener): void;
  /** Remove `listener` from messages matching any name in `events`. */
  unsubscribe(events: readonly string[], listener: MessageListener): void;
  unsubscribe(first?: string | readonly string[] | MessageListener, second?: MessageListener): void {
    if (first === undefined) {
      this.messages.off();
      return;
    }
    if (typeof first === 'function') {
      this.messages.off(first);
      return;
    }
    if (typeof first === 'string') {
      this.messages.off(first, second as MessageListener);
      return;
    }
    for (const event of first) {
      this.messages.off(event, second as MessageListener);
    }
  }

  /**
   * Publish one application-level message to the channel.
   *
   * @param name - The event name.
   * @param data - The data to publish.
   * @param options - Optional publish controls. `ttlMs` requests how long the
   *   message is retained for history (server-clamped to your plan ceiling);
   *   omit it for the short ephemeral default.
   */
  async publish(name: string, data: unknown, options?: { readonly ttlMs?: number }): Promise<void> {
    // Attach so the publisher also receives this channel's live messages, but don't
    // block the publish on it: when the connection is down, queueMessages buffers the
    // publish and the subscription is restored on reconnect.
    void this.attach().catch(() => {});
    // With a cipher set, the edge only ever sees ciphertext; `encoding` tells the
    // receiving SDK how to read it.
    const encrypted = this.cipher ? await this.cipher.encrypt(data) : null;
    await this.connection['publish']({
      t: 'pub',
      channel: this.name,
      name,
      data: encrypted ? encrypted.data : data,
      ...(encrypted ? { encoding: encrypted.encoding } : {}),
      ...(options?.ttlMs === undefined ? {} : { ttlMs: options.ttlMs }),
    });
  }

  /**
   * Fetch recent messages for this channel, oldest-first. Does not interleave
   * with the live subscription. Pass `start` (a message id) to page backward.
   */
  async history(params?: { readonly limit?: number; readonly start?: string }): Promise<{ readonly messages: readonly MessageFrame[]; readonly more: boolean }> {
    const response = await this.connection['requestHistory']({
      t: 'hist',
      channel: this.name,
      ...(params?.limit === undefined ? {} : { limit: params.limit }),
      ...(params?.start === undefined ? {} : { start: params.start }),
    });
    if (!this.cipher) {
      return { messages: response.messages, more: response.more ?? false };
    }
    const cipher = this.cipher;
    const messages = await Promise.all(response.messages.map((frame) => decryptFrame(cipher, frame).catch(() => frame)));
    return { messages, more: response.more ?? false };
  }

  /**
   * Deliver an inbound message frame to subscribers, decrypting first when a
   * cipher is set. Decryption is serialized through a per-channel promise chain
   * so messages are emitted in arrival order even though decrypt is async.
   * A frame whose `encoding` isn't a cipher encoding passes through unchanged.
   */
  private deliverMessage(frame: MessageFrame): void {
    if (!this.cipher || !isCipherEncoding(frame.encoding)) {
      this.messages.dispatch(frame);
      return;
    }
    const cipher = this.cipher;
    this.decryptChain = this.decryptChain.then(async () => {
      try {
        this.messages.dispatch(await decryptFrame(cipher, frame));
      } catch (error) {
        // A failed decrypt (wrong key / tampered) is dropped rather than delivered as garbage.
        console.warn(`[realtime] failed to decrypt message on channel ${this.name}:`, error);
      }
    });
  }

  /** Drive the state machine from connection lifecycle changes. */
  private onConnectionState(state: ConnectionState, reason?: Error): void {
    if (state === 'disconnected' && this.channelState === 'attached') {
      this.transition('suspended', { reason: reason ?? new Error('connection disconnected') });
      return;
    }
    if (state === 'connected') {
      // The connection restores remembered subscriptions on reconnect, so
      // reflect the resume back to channel state listeners.
      if (this.channelState === 'suspended') {
        this.transition('attached', { resumed: true });
      } else if (this.channelState === 'attached') {
        this.emit('update', { current: 'attached', previous: 'attached', resumed: true });
      }
      return;
    }
    if (state === 'failed' && this.isLive()) {
      this.transition('failed', reason !== undefined ? { reason } : undefined);
      return;
    }
    if (state === 'closed' && this.isLive()) {
      this.transition('detached');
    }
  }

  /** True while the channel is in an attach-related state worth transitioning out of. */
  private isLive(): boolean {
    return this.channelState !== 'initialized' && this.channelState !== 'detached' && this.channelState !== 'failed';
  }

  private transition(next: ChannelState, options?: { readonly reason?: Error; readonly resumed?: boolean }): void {
    if (this.channelState === next) return;
    const previous = this.channelState;
    this.channelState = next;
    this.emit(next, {
      current: next,
      previous,
      resumed: options?.resumed ?? false,
      ...(options?.reason !== undefined ? { reason: options.reason } : {}),
    });
  }
}

/**
 * Per-channel presence facade. Wraps the `pres` frame and `presEvt`
 * listener dispatch.
 */
export class Presence extends TypedEventEmitter<PresenceEventType, PresenceEventListener, PresenceEventResult> {
  private readonly connection: Connection;
  private readonly channelName: string;
  private readonly channel: Channel;
  private readonly cipher: Cipher | null;
  /** Serializes async decryption so presence events keep their arrival order. */
  private decryptChain: Promise<void> = Promise.resolve();

  constructor(connection: Connection, channelName: string, channel: Channel, cipher: Cipher | null) {
    super((_event, args) => args[0]);
    this.connection = connection;
    this.channelName = channelName;
    this.channel = channel;
    this.cipher = cipher;
  }

  /**
   * Register a listener for presence events. Implicitly attaches the
   * underlying channel — presence events arrive on the same WebSocket
   * subscription as message frames.
   */
  override on(listener: PresenceEventListener): UnsubscribeFn;
  /** Register a listener for presence events with a matching action. */
  override on(event: PresenceEventType, listener: PresenceEventListener): UnsubscribeFn;
  override on(first: PresenceEventType | PresenceEventListener, second?: PresenceEventListener): UnsubscribeFn {
    const unsubscribe = second === undefined ? super.on(first as PresenceEventListener) : super.on(first as PresenceEventType, second);
    this.channel.attach().catch(() => {});
    return unsubscribe;
  }

  /** Resolve the next presence event with the matching action. */
  override once(event: PresenceEventType): Promise<PresenceEventResult>;
  /** Invoke `listener` one time for the next presence event. */
  override once(listener: PresenceEventListener): void;
  /** Invoke `listener` one time for the next presence event with a matching action. */
  override once(event: PresenceEventType, listener: PresenceEventListener): void;
  override once(first: PresenceEventType | PresenceEventListener, second?: PresenceEventListener): Promise<PresenceEventResult> | void {
    if (second === undefined && typeof first !== 'function') {
      const result = super.once(first);
      this.channel.attach().catch(() => {});
      return result;
    }
    if (second === undefined) {
      super.once(first as PresenceEventListener);
      this.channel.attach().catch(() => {});
      return;
    }
    super.once(first as PresenceEventType, second);
    this.channel.attach().catch(() => {});
  }

  subscribe(listener: PresenceEventListener): UnsubscribeFn {
    return this.on(listener);
  }

  /** Announce this connection as present in the channel. */
  async enter(data?: unknown): Promise<void> {
    await this.send('enter', data);
  }

  /** Update the data attached to this connection's presence entry. */
  async update(data?: unknown): Promise<void> {
    await this.send('update', data);
  }

  /** Remove this connection's presence entry. */
  async leave(): Promise<void> {
    await this.send('leave', undefined);
  }

  /**
   * @internal Dispatch a presence frame from the Connection transport,
   * decrypting its data first when a cipher is set. Decryption is serialized
   * through a promise chain so events keep their arrival order.
   */
  private emitPresence(event: PresenceEventFrame): void {
    if (!this.cipher || !isCipherEncoding(event.encoding)) {
      this.emit(event.action, event);
      return;
    }
    const cipher = this.cipher;
    const encoding = event.encoding;
    this.decryptChain = this.decryptChain.then(async () => {
      try {
        const data = await cipher.decrypt(encoding, event.data);
        const { encoding: _encoding, ...rest } = event;
        this.emit(event.action, { ...rest, data });
      } catch (error) {
        console.warn(`[realtime] failed to decrypt presence on channel ${this.channelName}:`, error);
      }
    });
  }

  private async send(action: PresenceAction, data: unknown): Promise<void> {
    await this.channel.attach();
    // Encrypt the presence payload so the edge only sees ciphertext (matching messages).
    const encrypted = this.cipher !== null && data !== undefined ? await this.cipher.encrypt(data) : null;
    const payload = encrypted ? encrypted.data : data;
    await this.connection['request']({
      t: 'pres',
      channel: this.channelName,
      action,
      ...(payload === undefined ? {} : { data: payload }),
      ...(encrypted ? { encoding: encrypted.encoding } : {}),
    });
  }
}

/**
 * Message-name event emitter for a channel. Separate from the channel's
 * own state emitter so `subscribe` (messages) and `on` (state) don't
 * collide; exposes `dispatch` so the Connection transport can deliver
 * frames into it.
 */
class ChannelMessageEmitter extends TypedEventEmitter<string, MessageListener, MessageFrame> {
  /** Deliver a message frame to listeners keyed by its `name`. */
  dispatch(message: MessageFrame): void {
    this.emit(message.name, message);
  }
}

/** Coerce an unknown thrown value into an Error for state-change reasons. */
function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Return a copy of `frame` with its encrypted payload decrypted and its cipher
 * `encoding` stripped (the delivered data is now plaintext). Frames without a
 * cipher encoding are returned unchanged. Rejects if decryption fails.
 */
async function decryptFrame(cipher: Cipher, frame: MessageFrame): Promise<MessageFrame> {
  if (!isCipherEncoding(frame.encoding)) {
    return frame;
  }
  const data = await cipher.decrypt(frame.encoding, frame.data);
  const { encoding: _encoding, ...rest } = frame;
  return { ...rest, data };
}
