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
import type { BatchMember, BundledMessage, MessageFrame, PresenceAction, PresenceEventFrame } from './wire.js';

/** Listener handle returned by `subscribe` — call to remove the listener. */
export type UnsubscribeFn = EventUnsubscribeFn;

/**
 * Tuning for automatic publish batching. Single `publish(name, data)` calls are
 * always auto-batched: buffered and flushed as one batch frame (one stored,
 * dedupable message), which massively raises per-channel throughput for little
 * to no latency cost. This type only tunes that behavior; it is never needed to
 * turn batching on. Array publishes and `batchPublish` are never buffered.
 */
export type BatchOptions = {
  /**
   * Minimum gap between batch sends, in ms, applied as a throttle. A publish is
   * sent right away unless a batch went out within the last `intervalMs`, in
   * which case it waits until the window is up. Publishes spaced further apart
   * than `intervalMs` are never batched together and add no latency. Only fast
   * bursts get grouped into one batch. Default 10.
   */
  readonly intervalMs?: number;
  /** Flush early once this many messages are buffered. Default 200. */
  readonly maxMessages?: number;
};

/** One buffered publish awaiting the next flush. */
type BufferedPublish = {
  readonly member: BatchMember;
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
};

const DEFAULT_BATCH_INTERVAL_MS = 10;
const DEFAULT_BATCH_MAX_MESSAGES = 200;

/**
 * Cap on the per-channel delivered-message dedup cache. Bounds memory; the window
 * it covers (this many recent messages) comfortably exceeds any realistic
 * publisher-retry or live/coalescing reorder gap.
 */
const DEDUP_CACHE_MAX = 8192;

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
  /** Resolved auto-batch config (defaults applied). */
  private readonly batch: { readonly intervalMs: number; readonly maxMessages: number };
  /** Buffered single publishes awaiting the next auto-batch flush. */
  private batchBuffer: BufferedPublish[] = [];
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  /** When the last batch was sent, to throttle sends to one per `intervalMs`. */
  private lastFlushMs = 0;
  private attachPromise: Promise<void> | null = null;
  private channelState: ChannelState = 'initialized';
  /**
   * Bounded, insertion-ordered set of recently delivered (clientId, messageId)
   * keys, for exactly-once delivery. The server coalesces publishes across
   * clients into one record and does not dedup the individual messages within
   * it, so a publisher retry can deliver a message twice — we drop the repeat
   * here. Keyed on the server-stamped clientId, so one client cannot suppress
   * another's message by reusing its id.
   */
  private readonly seenMessages = new Map<string, true>();

  constructor(connection: Connection, name: string, cipher?: CipherParams, batch?: BatchOptions) {
    super((_event, args) => args[0]);
    this.connection = connection;
    this.name = name;
    this.cipher = cipher ? new Cipher(cipher) : null;
    this.batch = {
      intervalMs: batch?.intervalMs ?? DEFAULT_BATCH_INTERVAL_MS,
      maxMessages: batch?.maxMessages ?? DEFAULT_BATCH_MAX_MESSAGES,
    };
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
    // Don't strand buffered auto-batched publishes on detach.
    this.flush();
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
  publish(name: string, data: unknown, options?: { readonly ttlMs?: number }): Promise<void>;
  /**
   * Publish a batch of messages in a single frame under one message id. The
   * batch is the atomic unit: the server stores and dedups it as one durable
   * message (so a resend is collapsed), while subscribers receive the members
   * individually. Reduces stored-message count and transport overhead.
   *
   * @param messages - The messages to publish, each with its own `name`/`data`.
   * @param options - Optional publish controls (e.g. `ttlMs`).
   */
  publish(messages: ReadonlyArray<{ readonly name: string; readonly data: unknown }>, options?: { readonly ttlMs?: number }): Promise<void>;
  async publish(
    nameOrMessages: string | ReadonlyArray<{ readonly name: string; readonly data: unknown }>,
    dataOrOptions?: unknown,
    options?: { readonly ttlMs?: number },
  ): Promise<void> {
    // Attach so the publisher also receives this channel's live messages, but don't
    // block the publish on it: when the connection is down, queueMessages buffers the
    // publish and the subscription is restored on reconnect.
    void this.attach().catch(() => {});
    if (typeof nameOrMessages === 'string') {
      const member = await this.toMember(nameOrMessages, dataOrOptions);
      // Auto-batch single publishes — but not when a per-message ttl override is
      // set (a batch shares one ttl), so send those immediately.
      if (options?.ttlMs === undefined) {
        return this.enqueue(member);
      }
      await this.connection['publish']({
        t: 'pub',
        channel: this.name,
        name: member.name,
        data: member.data,
        ...(member.encoding === undefined ? {} : { encoding: member.encoding }),
        ...(options?.ttlMs === undefined ? {} : { ttlMs: options.ttlMs }),
      });
      return;
    }
    const opts = dataOrOptions as { readonly ttlMs?: number } | undefined;
    const members = await Promise.all(nameOrMessages.map((message) => this.toMember(message.name, message.data)));
    await this.connection['publish']({
      t: 'pub',
      channel: this.name,
      name: '',
      data: null,
      messages: members,
      ...(opts?.ttlMs === undefined ? {} : { ttlMs: opts.ttlMs }),
    });
  }

  /** Build a wire batch member, encrypting `data` per-member when a cipher is set. */
  private async toMember(name: string, data: unknown): Promise<BatchMember> {
    if (!this.cipher) {
      return { name, data };
    }
    const { encoding, data: encrypted } = await this.cipher.encrypt(data);
    return { name, data: encrypted, encoding };
  }

  /**
   * Flush any buffered (auto-batched) publishes now, as a single batch frame.
   * Runs automatically once the throttle window elapses, when the buffer is
   * full, and on detach; call it to force an immediate send. No-op when nothing
   * is buffered.
   */
  flush(): void {
    if (this.batchTimer !== null) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    if (this.batchBuffer.length === 0) {
      return;
    }
    const pending = this.batchBuffer;
    this.batchBuffer = [];
    this.lastFlushMs = Date.now();
    void this.connection['publish']({
      t: 'pub',
      channel: this.name,
      name: '',
      data: null,
      messages: pending.map((entry) => entry.member),
    }).then(
      () => {
        for (const entry of pending) {
          entry.resolve();
        }
      },
      (error: unknown) => {
        const wrapped = asError(error);
        for (const entry of pending) {
          entry.reject(wrapped);
        }
      },
    );
  }

  /** Buffer a member for the next flush, scheduling or forcing a flush as needed. */
  private enqueue(member: BatchMember): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.batchBuffer.push({ member, resolve, reject });
      if (this.batchBuffer.length >= this.batch.maxMessages) {
        this.flush();
      } else if (this.batchTimer === null) {
        // Throttle, don't fixed-delay: send right away unless a batch went out
        // within `intervalMs`, in which case wait out the rest of the window.
        // Publishes spaced further apart than `intervalMs` thus never batch.
        const sinceLast = Date.now() - this.lastFlushMs;
        const wait = sinceLast >= this.batch.intervalMs ? 0 : this.batch.intervalMs - sinceLast;
        this.batchTimer = setTimeout(() => this.flush(), wait);
      }
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
    // Expand any batch frames into their member frames before decrypting.
    const expanded = response.messages.flatMap(expandBatch);
    if (!this.cipher) {
      return { messages: expanded, more: response.more ?? false };
    }
    const cipher = this.cipher;
    const messages = await Promise.all(expanded.map((frame) => decryptFrame(cipher, frame).catch(() => frame)));
    return { messages, more: response.more ?? false };
  }

  /**
   * Deliver an inbound frame to subscribers. A batch frame is expanded into its
   * member frames (in order) first; each member is then dispatched like a single
   * message.
   */
  private deliverMessage(frame: MessageFrame): void {
    // Server bundle ("envelope of envelopes"): unwrap each member back into a
    // frame and re-deliver it — a member may itself be a client batch, so this
    // recurses one level before reaching deliverSingle.
    if (frame.bundle !== undefined && frame.bundle.length > 0) {
      for (const member of frame.bundle) {
        this.deliverMessage(bundledToFrame(frame.channel, member));
      }
      return;
    }
    if (frame.messages !== undefined && frame.messages.length > 0) {
      for (let index = 0; index < frame.messages.length; index++) {
        this.deliverSingle(memberFrame(frame, frame.messages[index]!, index));
      }
      return;
    }
    this.deliverSingle(frame);
  }

  /**
   * True if this (clientId, messageId) was already delivered — drops duplicates
   * a publisher retry can introduce once the server coalesces. Records unseen
   * keys, evicting the oldest past the cap.
   */
  private isDuplicate(frame: MessageFrame): boolean {
    const key = `${frame.clientId ?? ''} ${frame.messageId}`;
    if (this.seenMessages.has(key)) {
      return true;
    }
    this.seenMessages.set(key, true);
    if (this.seenMessages.size > DEDUP_CACHE_MAX) {
      const oldest = this.seenMessages.keys().next().value;
      if (oldest !== undefined) {
        this.seenMessages.delete(oldest);
      }
    }
    return false;
  }

  /**
   * Dispatch one message frame, decrypting first when a cipher is set.
   * Decryption is serialized through a per-channel promise chain so messages
   * are emitted in arrival order even though decrypt is async. A frame whose
   * `encoding` isn't a cipher encoding passes through unchanged.
   */
  private deliverSingle(frame: MessageFrame): void {
    if (this.isDuplicate(frame)) {
      return;
    }
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

/** Build a per-member message frame from a batch frame; member id is `<batchId>:<index>`. */
function memberFrame(base: MessageFrame, member: BatchMember, index: number): MessageFrame {
  return {
    t: 'msg',
    channel: base.channel,
    name: member.name,
    data: member.data,
    timestamp: base.timestamp,
    messageId: `${base.messageId}:${index}`,
    ...(base.clientId === undefined ? {} : { clientId: base.clientId }),
    ...(member.encoding === undefined ? {} : { encoding: member.encoding }),
  };
}

/** Build a full message frame from one server-bundle member, taking the channel from the carrying frame. */
function bundledToFrame(channel: string, member: BundledMessage): MessageFrame {
  return {
    t: 'msg',
    channel,
    name: member.name,
    data: member.data,
    timestamp: member.timestamp,
    messageId: member.messageId,
    ...(member.clientId === undefined ? {} : { clientId: member.clientId }),
    ...(member.encoding === undefined ? {} : { encoding: member.encoding }),
    ...(member.messages === undefined ? {} : { messages: member.messages }),
  };
}

/** Expand a batch frame into its member frames; a non-batch frame is returned as a single-item array. */
function expandBatch(frame: MessageFrame): MessageFrame[] {
  if (frame.messages !== undefined && frame.messages.length > 0) {
    return frame.messages.map((member, index) => memberFrame(frame, member, index));
  }
  return [frame];
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
