/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 *
 * Mirrors Ably's split: `on` / `once` / `off` observe the channel's
 * lifecycle *state* (a closed set of events), while `subscribe` /
 * `unsubscribe` carry application *messages* (open-ended event names).
 */

import { TypedEventEmitter, type Connection, type ConnectionState, type EventUnsubscribeFn, type MessageListener, type PresenceEventListener } from './connection.js';
import type { MessageFrame, PresenceAction, PresenceEventFrame } from './wire.js';

/** Listener handle returned by `subscribe` — call to remove the listener. */
export type UnsubscribeFn = EventUnsubscribeFn;

/**
 * Channel lifecycle states. A channel walks this set as it attaches to
 * and detaches from the server; mirrors Ably's `ChannelState`.
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
 * plus `update`. Mirrors Ably's `ChannelEvent`.
 */
export type ChannelEventType =
  | ChannelState
  /**
   * A change that is not a state transition — the channel stayed in the
   * same state but something was re-confirmed (e.g. the subscription was
   * resumed after a reconnect). Carries `resumed` on the state change.
   */
  | 'update';

/**
 * Payload delivered to channel state listeners on every {@link ChannelEventType}.
 * Mirrors Ably's `ChannelStateChange`.
 */
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
  private attachPromise: Promise<void> | null = null;
  private channelState: ChannelState = 'initialized';

  constructor(connection: Connection, name: string) {
    super((_event, args) => args[0]);
    this.connection = connection;
    this.name = name;
    this.presence = new Presence(connection, name, this);
    this.connection['registerChannel'](this.name, {
      message: (message) => this.messages.dispatch(message),
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
   */
  async publish(name: string, data: unknown): Promise<void> {
    await this.attach();
    await this.connection['request']({ t: 'pub', channel: this.name, name, data });
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

  constructor(connection: Connection, channelName: string, channel: Channel) {
    super((_event, args) => args[0]);
    this.connection = connection;
    this.channelName = channelName;
    this.channel = channel;
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

  /** @internal Dispatch a presence frame from the Connection transport. */
  private emitPresence(event: PresenceEventFrame): void {
    this.emit(event.action, event);
  }

  private async send(action: PresenceAction, data: unknown): Promise<void> {
    await this.channel.attach();
    await this.connection['request']({
      t: 'pres',
      channel: this.channelName,
      action,
      ...(data === undefined ? {} : { data }),
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
