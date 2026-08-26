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

/** Function returned by `subscribe`. Call it to remove the listener. */
export type UnsubscribeFn = EventUnsubscribeFn;

/**
 * Configuration for automatic publish batching. Single `publish(name, data)`
 * calls are always auto-batched, buffered and flushed as one batch frame (one
 * stored, dedupable message), which massively raises per-channel throughput for
 * little to no latency cost. Batching is always on. Array publishes and
 * `batchPublish` are never batched (they assume the caller is managing batching).
 */
export type BatchOptions = {
  /**
   * Minimum gap between batch sends, in ms, applied as a throttle. A publish is
   * sent right away unless a batch went out within the last `intervalMs`, in
   * which case it waits until the window is up. Publishes spaced further apart
   * than `intervalMs` are never batched together and add no latency. Only fast
   * bursts get grouped into one batch.
   *
   * @defaultValue 10
   */
  readonly intervalMs?: number;
  /**
   * Flush early once this many messages are buffered.
   *
   * @defaultValue 200
   */
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
 * Cap on the per-channel delivered-message dedup cache (number of message ids).
 * This limit determines how many recent messages are stored for "exactly-once
 * delivery" guarantees. A larger limit means more memory, but more reliable delivery.
 */
const DEDUP_CACHE_MAX = 8192;

/**
 * Channel lifecycle states. A healthy channel follows the lifecycle states in order:
 * `initialized` -> `attaching` -> `attached` -> `detaching` -> `detached` -> `attaching`, etc.
 * A channel in a `failed` state is not retried and is not re-attached.
 */
export type ChannelState =
  /** Created locally. No attach has been attempted yet. */
  | 'initialized'
  /** An attach has been requested and is awaiting server confirmation. */
  | 'attaching'
  /** Attached. Messages and presence for this channel are flowing. */
  | 'attached'
  /** A detach has been requested and is awaiting server confirmation. */
  | 'detaching'
  /** Detached. No messages or presence are delivered until re-attached. */
  | 'detached'
  /**
   * Temporarily lost, for example because the connection dropped. The SDK
   * re-attaches on reconnect. You can keep publishing: with `queueMessages` on
   * (the default), publishes are queued locally and sent once reconnected.
   */
  | 'suspended'
  /**
   * The attach failed with an error that a retry will not fix, for example a
   * missing capability, and the SDK will not retry it. Call `attach()` to try
   * again manually, for example after obtaining a token with more capabilities.
   */
  | 'failed';

/**
 * Events emitted to channel state listeners: every {@link ChannelState}
 * plus `update` (a no-transition re-confirmation, e.g. a resume).
 */
export type ChannelEventType =
  | ChannelState
  /**
   * A change that is not a state transition. The channel stayed in the same
   * state but something was updated, for example the server reported a
   * resume outcome while the channel was already attached. Check `resumed` on
   * the payload: `false` means messages may have been missed beyond
   * retention, so reload state or read history.
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
 * A named channel. Subscribe to receive its messages, publish to send them,
 * and use {@link Channel.presence | `presence`} to see who is there. Get
 * instances via `client.channels.get(name)`. The same name always returns the
 * same instance on a given client.
 *
 * The channel has two separate listener surfaces: `on` / `once` / `off` listen
 * on the channel's lifecycle {@link ChannelState}, while `subscribe` /
 * `unsubscribe` receive application messages.
 *
 * @example
 * ```ts
 * const channel = client.channels.get('chat:room-1');
 * channel.subscribe('greeting', (message) => {
 *   console.log(message.data);
 * });
 * await channel.publish('greeting', { text: 'hi' });
 * ```
 */
export class Channel extends TypedEventEmitter<ChannelEventType, ChannelStateListener, ChannelStateChange> {
  /** The channel name this instance is bound to (e.g. "chat:room-1"). */
  readonly name: string;
  /** Presence for this channel: announce membership and listen on who comes and goes. */
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
  /** Unix timestamp when the last batch was sent. Used to throttle sends to one per `intervalMs`. */
  private lastFlushMs = 0;
  private attachPromise: Promise<void> | null = null;
  private channelState: ChannelState = 'initialized';
  /** Removes this channel's connection state listener. Called on release. */
  private readonly connectionOff: EventUnsubscribeFn;
  /**
   * Bounded, insertion-ordered set of recently delivered (clientId, messageId)
   * keys, for exactly-once delivery. The server coalesces publishes across
   * clients into one record and does not dedup the individual messages within
   * it, so a publisher retry can deliver a message twice. We drop the repeat
   * here. Keyed on the server-stamped clientId, so one client cannot suppress
   * another's message by reusing its id.
   */
  private readonly seenMessages = new Map<string, true>();

  /**
   * The highest serial up to which this channel has received every message with no gap. This
   * serial is sent on (re)subscribe so the server replays anything missed during a disconnect.
   * Contiguous per channel and identical across cells (e.g. "us-west"), so resume is exact and
   * migration-safe. 0 means no baseline yet: the next sequenced message is adopted as the baseline
   * (a fresh subscriber starts from "now", not from serial 1). A channel that has only seen unsequenced
   * messages (such as `{ephemeral: true}` messages) keeps 0 and resubscribes fresh.
   */
  private contiguousSerial = 0;

  /**
   * True while a gap-fill fetch is in flight. Gapped messages arriving during
   * that window do not start more fetches: one fetch from the cursor replays
   * everything after it, so it heals the whole burst.
   */
  private backfilling = false;

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
      lastSerial: () => (this.contiguousSerial > 0 ? this.contiguousSerial : undefined),
      resumed: (resumed) => this.onResumed(resumed),
      reenterPresence: () => this.presence['reenterOnReconnect'](),
    });
    this.connectionOff = this.connection.on((state, reason) => this.onConnectionState(state, reason));
  }

  /**
   * Called by `channels.release` (via index access) when this instance is
   * removed from the client. Detaches this channel's state machine from the
   * connection so released instances are not retained forever by its
   * listener set.
   */
  private dispose(): void {
    this.connectionOff();
  }

  /** Current {@link ChannelState}. Listen on changes with `on(...)`. */
  get state(): ChannelState {
    return this.channelState;
  }

  /**
   * Ensure the server is subscribed to this channel. `subscribe()` and the
   * presence methods call this implicitly, so calling it yourself is optional.
   * It is useful for surfacing attach errors before the first message arrives.
   * Resolves once the server confirms the channel subscription. Rejects with the
   * server's error when the token lacks the subscribe capability (the channel
   * moves to `failed` and is not retried) or when the request fails in transit
   * (the channel moves to `suspended` and re-attaches on reconnect).
   */
  async attach(): Promise<void> {
    if (this.channelState === 'attached') return;
    if (this.attachPromise) return this.attachPromise;
    this.transition('attaching');
    // Remember the intent before the request resolves: if the connection drops mid-attach,
    // this channel must still be re-subscribed once the connection is restored, not left
    // orphaned. A terminal capability denial forgets it again below so it isn't retried.
    this.connection['rememberSubscription'](this.name);
    this.attachPromise = this.connection['request'](this.subscribeFrame())
      .then((ack) => {
        // The reconnect restore path may have re-subscribed and reported the
        // authoritative resume outcome while this request was in flight. Don't
        // clobber it with a same-state re-confirmation (a spurious 'update').
        if (this.channelState !== 'attached') {
          this.transition('attached', { resumed: ack.resumed ?? false });
        }
      })
      .catch((error: unknown) => {
        if (isCapabilityError(error)) {
          // Permission won't change on retry — stop trying and surface it.
          this.connection['forgetSubscription'](this.name);
          this.transition('failed', { reason: asError(error) });
        } else {
          // Transient (e.g. the connection dropped mid-attach): stay remembered and
          // suspended so the reconnect re-subscribe recovers the channel.
          this.transition('suspended', { reason: asError(error) });
        }
        throw error;
      })
      .finally(() => {
        this.attachPromise = null;
      });
    return this.attachPromise;
  }

  /**
   * Build the `sub` frame, carrying the serial resume cursor when this channel has one. A channel
   * that has only seen unsequenced messages has no cursor and resubscribes fresh.
   */
  private subscribeFrame(): {
    readonly t: 'sub';
    readonly channel: string;
    readonly lastSerial?: number;
  } {
    return this.contiguousSerial > 0
      ? { t: 'sub', channel: this.name, lastSerial: this.contiguousSerial }
      : { t: 'sub', channel: this.name };
  }

  /**
   * Detach from the server: stop receiving messages and presence events.
   * Buffered auto-batched publishes are flushed first. Local listeners are
   * preserved, call `off()` or `unsubscribe()` to clear them. Resolves once
   * the server confirms the detach. Rejects when the request fails, though
   * the channel is marked detached either way.
   */
  async detach(): Promise<void> {
    // Don't strand buffered auto-batched publishes on detach.
    this.flush();
    if (this.channelState === 'initialized' || this.channelState === 'detached' || this.channelState === 'detaching') return;
    this.transition('detaching');
    // Detaching the channel ends presence too: the server's unsub closes the presence
    // watcher, so stop re-opening it and re-entering on future reconnects.
    this.presence['onDetached']();
    // A fresh attach (on this instance or on a replacement after `release`) can
    // race this detach. Capture the subscription epoch now: if the attach
    // re-remembered the channel while the unsub was in flight, the epoch moved
    // on and this detach must not erase the newer intent or clobber the state.
    const epoch = this.connection['subscriptionEpoch'](this.name);
    try {
      await this.connection['request']({ t: 'unsub', channel: this.name });
    } finally {
      this.connection['forgetSubscription'](this.name, epoch);
      // Narrowing note: read via the getter, TS narrowed the field above.
      if (this.state === 'detaching') {
        this.transition('detached');
      }
    }
  }

  /**
   * Register a listener for every message on this channel. Implicitly attaches
   * if needed and returns an unsubscribe function. `subscribe` itself is
   * synchronous, so attach failures do not surface here: call
   * {@link Channel.attach | `attach()`} first if you want to observe them.
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
   * Publish one message to the channel. On a channel with a `cipher`, `data`
   * is end-to-end encrypted before it is sent. Resolves once the server acks
   * the publish. With `queueMessages` on (the default), publishes made while
   * the connection is down are queued locally and sent on reconnect. Rejects
   * with the server's error when the service refuses the publish (for example
   * a token without the publish capability), and rejects immediately when the
   * connection is `closing`, `closed`, or `failed`. With `queueMessages` off,
   * any connection state but `connected` rejects immediately.
   *
   * @param name - The event name.
   * @param data - The data to publish.
   * @param options - Optional publish controls. `ephemeral: true` marks a
   *   fire-and-forget message: delivered live to current subscribers but never
   *   stored, so it is excluded from history and reconnect replay.
   */
  publish(name: string, data: unknown, options?: { readonly ephemeral?: boolean }): Promise<void>;
  /**
   * Publish a batch of messages in a single frame under one message id. This is
   * an atomic batch. The server stores and dedups it as one durable message, while
   * subscribers receive the members individually. This counts as 1 message for the
   * purposes of usage limits / quotas (message size limits still apply).
   *
   * @param messages - The messages to publish, each with its own `name`/`data`.
   * @param options - Optional publish controls (e.g. `ephemeral`).
   */
  publish(messages: ReadonlyArray<{ readonly name: string; readonly data: unknown }>, options?: { readonly ephemeral?: boolean }): Promise<void>;
  async publish(
    nameOrMessages: string | ReadonlyArray<{ readonly name: string; readonly data: unknown }>,
    dataOrOptions?: unknown,
    options?: { readonly ephemeral?: boolean },
  ): Promise<void> {
    // Publishing does not attach: a publisher that never subscribed should not
    // hold a server-side subscription. Offline publishes are still buffered and
    // resent by the connection's queueMessages, independent of attach state.
    if (typeof nameOrMessages === 'string') {
      const member = await this.toMember(nameOrMessages, dataOrOptions);
      // Auto-batch single publishes, but not ephemeral ones (a batch shares one
      // ephemeral disposition), so send those immediately.
      if (options?.ephemeral !== true) {
        return this.enqueue(member);
      }
      await this.connection['publish']({
        t: 'pub',
        channel: this.name,
        name: member.name,
        data: member.data,
        ...(member.encoding === undefined ? {} : { encoding: member.encoding }),
        ephemeral: true,
      });
      return;
    }
    const opts = dataOrOptions as { readonly ephemeral?: boolean } | undefined;
    const members = await Promise.all(nameOrMessages.map((message) => this.toMember(message.name, message.data)));
    await this.connection['publish']({
      t: 'pub',
      channel: this.name,
      name: '',
      data: null,
      messages: members,
      ...(opts?.ephemeral === true ? { ephemeral: true } : {}),
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
   * This runs automatically once the throttle window elapses, when the buffer
   * is full, and on detach. Call it to force an immediate send. A no-op when
   * nothing is buffered.
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
   * Fetch recent messages for this channel, oldest first. History is a
   * one-shot read and does not interleave with the live subscription. How far
   * back it reaches depends on each message's retention, see the
   * [history docs](https://foony.io/docs/history). On a channel with a
   * `cipher`, messages are decrypted before they are returned.
   *
   * @param params - `limit` caps how many messages are returned. `before` (a
   *   message serial, see {@link MessageFrame.seq}) pages backward: only
   *   messages with a serial strictly below it are returned.
   * @returns Resolves with the messages and `more`, which is true when older
   *   messages remain (pass the oldest message's `seq` as `before` to fetch
   *   them). Rejects with the server's error when history cannot be read, for
   *   example a missing history capability.
   */
  async history(params?: { readonly limit?: number; readonly before?: number }): Promise<{ readonly messages: readonly MessageFrame[]; readonly more: boolean }> {
    const response = await this.connection['requestHistory']({
      t: 'hist',
      channel: this.name,
      ...(params?.limit === undefined ? {} : { limit: params.limit }),
      ...(params?.before === undefined ? {} : { before: params.before }),
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
   * member frames (in order) first. Each member is then dispatched like a single
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
      // The whole batch is one server record with one serial. Ephemeral messages are never
      // resumable, so they must not advance the cursor — the server would not find them.
      if (frame.ephemeral !== true) {
        this.recordSerial(frame.seq);
      }
      return;
    }
    this.deliverSingle(frame);
    if (frame.ephemeral !== true) {
      this.recordSerial(frame.seq);
    }
  }

  /**
   * Track the contiguous per-channel serial and detect gaps. The fence on the server means stored
   * order equals serial order and the live tail delivers in that order, so the only way a serial
   * arrives out of sequence is real loss (a dropped message to a briefly-slow consumer). When that
   * happens we trigger a gap-fill fetch, whose ordered replay closes the hole.
   *
   *  - no baseline yet (cursor 0): adopt this serial as the baseline (fresh subscriber starts now).
   *  - next in sequence: advance the cursor.
   *  - already covered (<= cursor): a replay or duplicate, ignored (dedup handles the payload).
   *  - ahead of sequence (> cursor + 1): a gap. Backfill from the cursor and leave it un-advanced
   *    so the replay can close the hole before the cursor moves past it.
   */
  private recordSerial(seq: number | undefined): void {
    if (seq === undefined || seq === 0) {
      return;
    }
    if (this.contiguousSerial === 0 || seq === this.contiguousSerial + 1) {
      this.contiguousSerial = seq;
      return;
    }
    if (seq <= this.contiguousSerial) {
      return;
    }
    this.triggerBackfill();
  }

  /**
   * Heal a detected gap with a surgical fetch from the contiguous-serial cursor, NOT a
   * re-subscribe. The server returns just the messages after the cursor, leaving the live
   * subscription, presence watcher, and retained replay untouched (a re-subscribe would tear all
   * three down for one dropped message). The returned messages are applied in order, so the cursor
   * walks forward past the gap, and dedup drops any overlap with messages already delivered live.
   * Debounced to one in-flight fetch. If the cursor has aged out of retention the server reports a
   * discontinuity (resumed=false), which we surface and re-baseline from rather than re-applying.
   */
  private triggerBackfill(): void {
    if (this.backfilling || this.channelState !== 'attached' || this.contiguousSerial <= 0) {
      return;
    }
    this.backfilling = true;
    const fromSerial = this.contiguousSerial;
    this.connection['requestFetch']({ t: 'fetch', channel: this.name, fromSerial })
      .then((response) => {
        if (!response.resumed) {
          // The gap aged out of retention: the returned messages start above the hole, so applying
          // them would leave the cursor stuck. Re-baseline and surface the discontinuity instead.
          this.onResumed(false);
          return;
        }
        for (const message of response.messages) {
          this.deliverMessage(message);
        }
      })
      .catch(() => {
        // A failed fetch leaves the gap; the next gapped message retries it.
      })
      .finally(() => {
        this.backfilling = false;
      });
  }

  /**
   * True if this (clientId, messageId) was already delivered. Drops duplicates
   * a publisher retry can introduce once the server coalesces. Records unseen
   * keys, evicting the oldest past the cap.
   */
  private isDuplicate(frame: MessageFrame): boolean {
    const key = `${frame.clientId ?? ''}\u0000${frame.messageId}`;
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
    if (!this.cipher) {
      this.messages.dispatch(frame);
      return;
    }
    const cipher = this.cipher;
    // On an encrypted channel EVERY frame rides the decrypt chain, including
    // plaintext ones, so a plaintext frame can never overtake an encrypted one
    // that is still decrypting (decrypt is async, dispatch order must match
    // arrival order).
    this.decryptChain = this.decryptChain.then(async () => {
      if (!isCipherEncoding(frame.encoding)) {
        this.messages.dispatch(frame);
        return;
      }
      try {
        this.messages.dispatch(await decryptFrame(cipher, frame));
      } catch (error) {
        // A failed decrypt (wrong key / tampered) is dropped rather than delivered as garbage.
        console.warn(`[realtime] failed to decrypt message on channel ${this.name}:`, error);
      }
    });
  }

  /**
   * Finalize a reconnect re-subscribe: the connection re-issued the `sub` with our resume
   * cursor and the server reported whether the gap was replayed. resumed=false is a
   * discontinuity (messages may have been missed beyond retention), which listeners can
   * act on by reloading state or reading history.
   */
  private onResumed(resumed: boolean): void {
    // A discontinuity means the cursor aged out of retention, so the gap can't be filled. Drop the
    // baseline and adopt the next serial we see, otherwise every later message would look gapped
    // and we'd backfill-loop. The 'attached' {resumed:false} update tells listeners to recover
    // (reload state / read history) themselves.
    if (!resumed) {
      this.contiguousSerial = 0;
    }
    if (this.channelState === 'attaching' || this.channelState === 'suspended' || this.channelState === 'attached') {
      this.transition('attached', { resumed });
    }
  }

  /** Drive the state machine from connection lifecycle changes. */
  private onConnectionState(state: ConnectionState, reason?: Error): void {
    // An app-suspended connection parks the channel the same way a drop does:
    // 'suspended' channels re-attach (with resume) on the next connect.
    if ((state === 'disconnected' || state === 'suspended') && this.channelState === 'attached') {
      this.transition('suspended', { reason: reason ?? new Error(`connection ${state}`) });
      return;
    }
    if (state === 'connected') {
      // The connection re-subscribes remembered channels on reconnect and reports the
      // true resume outcome via onResumed; move to 'attaching' until that ack arrives
      // rather than optimistically claiming a resume that may be a discontinuity.
      if (this.channelState === 'suspended') {
        this.transition('attaching');
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
    if (this.channelState === next) {
      // No transition, but a re-confirmation that carries information (e.g. a
      // resume outcome while already attached) still matters to listeners:
      // emit 'update' instead of dropping it.
      if (options !== undefined) {
        this.emit('update', {
          current: next,
          previous: next,
          resumed: options.resumed ?? false,
          ...(options.reason !== undefined ? { reason: options.reason } : {}),
        });
      }
      return;
    }
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
 * Presence for one channel. Announce this connection with `enter` / `update` /
 * `leave`, and listen on who comes and goes with `on`. Events include this
 * connection's own transitions, so a member list built from events contains
 * this client too. Reached via
 * {@link Channel.presence | `channel.presence`}. See the
 * [presence docs](https://foony.io/docs/presence) for the full model.
 *
 * @example
 * ```ts
 * channel.presence.on('enter', (member) => {
 *   console.log(`${member.clientId} joined`);
 * });
 * await channel.presence.enter({ status: 'online' });
 * ```
 */
export class Presence extends TypedEventEmitter<PresenceEventType, PresenceEventListener, PresenceEventResult> {
  private readonly connection: Connection;
  private readonly channelName: string;
  private readonly channel: Channel;
  private readonly cipher: Cipher | null;
  /** Serializes async decryption so presence events keep their arrival order. */
  private decryptChain: Promise<void> = Promise.resolve();
  /**
   * True once we have asked the server for presence events on this channel. Set when the
   * first presence listener is added and cleared when the last leaves, so a channel used
   * only for messages never opens a presence watcher.
   */
  private watching = false;
  /**
   * What this connection has entered into the presence set, or null if not present. Kept so
   * the SDK can re-enter automatically after a reconnect. `{ data }` rather than the raw
   * value so "entered with no data" is distinct from "not entered".
   */
  private enteredState: { readonly data: unknown } | null = null;

  constructor(connection: Connection, channelName: string, channel: Channel, cipher: Cipher | null) {
    super((_event, args) => args[0]);
    this.connection = connection;
    this.channelName = channelName;
    this.channel = channel;
    this.cipher = cipher;
  }

  /**
   * Register a listener for presence events. Adding the first listener asks
   * the server for presence on this channel: an initial member snapshot, then
   * live transitions. Both include this connection's own membership, so your
   * own `enter`, `update`, and `leave` come back like any other member's.
   * This is independent of a message `subscribe`, so a
   * channel used only for messages never opens a presence watcher, and the
   * watcher is dropped again when the last presence listener is removed.
   */
  override on(listener: PresenceEventListener): UnsubscribeFn;
  /** Register a listener for presence events with a matching action. */
  override on(event: PresenceEventType, listener: PresenceEventListener): UnsubscribeFn;
  override on(first: PresenceEventType | PresenceEventListener, second?: PresenceEventListener): UnsubscribeFn {
    const unsubscribe = second === undefined ? super.on(first as PresenceEventListener) : super.on(first as PresenceEventType, second);
    this.ensureWatching();
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
      this.ensureWatching();
      return result;
    }
    if (second === undefined) {
      super.once(first as PresenceEventListener);
      this.ensureWatching();
      return;
    }
    super.once(first as PresenceEventType, second);
    this.ensureWatching();
  }

  /**
   * The watcher follows the listener count: every removal path (an unsubscribe
   * function, `off()`, a one-shot firing) funnels through this hook, so the
   * presence subscription is dropped as soon as the last listener leaves.
   */
  protected override onListenerRemoved(): void {
    this.maybeStopWatching();
  }

  /** Alias of {@link Presence.on | `on(listener)`}: register a listener for every presence event. */
  subscribe(listener: PresenceEventListener): UnsubscribeFn {
    return this.on(listener);
  }

  /**
   * Announce this connection as present on the channel, with optional `data`
   * (a display name, a status) shown to other members. Every watcher,
   * including this connection, receives the `enter` event. Implicitly attaches
   * the channel. The membership is remembered, and the SDK re-enters it
   * automatically after a reconnect. Resolves once the server acks the entry.
   * Rejects with the server's error when the token lacks the presence
   * capability.
   */
  async enter(data?: unknown): Promise<void> {
    this.enteredState = { data };
    await this.send('enter', data);
  }

  /**
   * Replace the `data` on this connection's presence entry. Every watcher,
   * including this connection, receives an `update` event. Resolves once the
   * server acks the update.
   * Rejects with the server's error when the token lacks the presence
   * capability.
   */
  async update(data?: unknown): Promise<void> {
    this.enteredState = { data };
    await this.send('update', data);
  }

  /**
   * Remove this connection's presence entry and stop the automatic re-entry
   * on reconnect. Every watcher, including this connection, receives the
   * `leave` event. Resolves once the server acks the leave. Rejects with the
   * server's error when the request fails.
   */
  async leave(): Promise<void> {
    this.enteredState = null;
    await this.send('leave', undefined);
  }

  /**
   * Ask the server to start sending presence events on this channel, once. This is
   * idempotent and remembered so it is re-sent on reconnect. A capability denial
   * gives up (the permission will not change on retry).
   */
  private ensureWatching(): void {
    if (this.watching) {
      return;
    }
    this.watching = true;
    this.connection['rememberPresence'](this.channelName);
    this.connection['request']({ t: 'presSub', channel: this.channelName }).catch((error: unknown) => {
      if (isCapabilityError(error) && !this.hasAnyListeners()) {
        this.watching = false;
        this.connection['forgetPresence'](this.channelName);
      }
    });
  }

  /** Drop the presence watcher once no presence listeners remain, to keep idle channels free. */
  private maybeStopWatching(): void {
    if (!this.watching || this.hasAnyListeners()) {
      return;
    }
    this.watching = false;
    this.connection['forgetPresence'](this.channelName);
    this.connection['request']({ t: 'presUnsub', channel: this.channelName }).catch(() => {});
  }

  /**
   * Re-announce this connection's presence after a reconnect: re-enter whatever was entered.
   * Presence watching is restored separately by the connection re-sending `presSub`.
   */
  private reenterOnReconnect(): void {
    if (this.enteredState !== null) {
      this.send('enter', this.enteredState.data).catch(() => {});
    }
  }

  /** Forget presence state when the channel detaches (the server's unsub closed the watcher). */
  private onDetached(): void {
    this.watching = false;
    this.enteredState = null;
    this.connection['forgetPresence'](this.channelName);
  }

  /**
   * @internal Dispatch a presence frame from the Connection transport,
   * decrypting its data first when a cipher is set. Decryption is serialized
   * through a promise chain so events keep their arrival order.
   */
  private emitPresence(event: PresenceEventFrame): void {
    if (!this.cipher) {
      this.emit(event.action, event);
      return;
    }
    const cipher = this.cipher;
    // On an encrypted channel EVERY event rides the decrypt chain, including
    // payload-less ones like `leave`, so a leave can never overtake the enter
    // it follows while the enter's data is still decrypting.
    this.decryptChain = this.decryptChain.then(async () => {
      const encoding = event.encoding;
      if (!isCipherEncoding(encoding)) {
        this.emit(event.action, event);
        return;
      }
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
 * collide. Exposes `dispatch` so the Connection transport can deliver
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
 * True for a server error that won't change on retry: the forbidden / capability /
 * channel-denied family (403xx). A failed attach with such an error is terminal.
 * Any other failure (e.g. a dropped connection) is transient and recovers on
 * reconnect.
 */
function isCapabilityError(error: unknown): boolean {
  const code = (error as { code?: number } | null)?.code;
  return typeof code === 'number' && code >= 40300 && code < 40400;
}

/** Build a per-member message frame from a batch frame. The member id is `<batchId>:<index>`. */
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
    ...(base.ephemeral === true ? { ephemeral: true } : {}),
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
    ...(member.seq === undefined ? {} : { seq: member.seq }),
    ...(member.ephemeral === true ? { ephemeral: true } : {}),
    ...(member.messages === undefined ? {} : { messages: member.messages }),
  };
}

/** Expand a batch frame into its member frames. A non-batch frame is returned as a single-item array. */
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
