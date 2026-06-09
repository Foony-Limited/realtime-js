/**
 * Channel + Presence public API. Wraps the Connection layer with
 * per-channel state.
 */

import { TypedEventEmitter, type Connection, type EventUnsubscribeFn, type MessageListener, type PresenceEventListener } from './connection.js';
import type { MessageFrame, PresenceAction, PresenceEventFrame } from './wire.js';

/** Listener handle returned by `subscribe` — call to remove the listener. */
export type UnsubscribeFn = EventUnsubscribeFn;

/** Message names emitted by a channel. */
export type ChannelEventType = string;

/** Listener for channel messages. */
export type ChannelEventListener = MessageListener;

/** Result returned by promise-based `channel.once(event)`. */
export type ChannelEventResult = MessageFrame;

/** Presence event names emitted by a channel presence facade. */
export type PresenceEventType = PresenceAction;

/** Result returned by promise-based `presence.once(event)`. */
export type PresenceEventResult = PresenceEventFrame;

/**
 * One subscription handle per (channel, listener) pair. Channels are
 * value-equal by name on a given Realtime client — calling
 * `client.channels.get('chat:1')` twice returns the same instance.
 */
export class Channel extends TypedEventEmitter<ChannelEventType, ChannelEventListener, ChannelEventResult> {
  readonly name: string;
  readonly presence: Presence;
  private readonly connection: Connection;
  private attachPromise: Promise<void> | null = null;
  private attached = false;

  constructor(connection: Connection, name: string) {
    super((_event, args) => args[0]);
    this.connection = connection;
    this.name = name;
    this.presence = new Presence(connection, name, this);
    this.connection['registerChannel'](this.name, {
      message: (message) => this.emit(message.name, message),
      presence: (event) => this.presence['emitPresence'](event),
    });
  }

  /**
   * Ensure the server is subscribed to this channel. Called implicitly
   * by `on()` / `subscribe()` and `presence.on()` / `presence.subscribe()`; expose it so callers
   * can pre-attach if they want to surface attach errors before the
   * first message arrives.
   */
  async attach(): Promise<void> {
    if (this.attached) return;
    if (this.attachPromise) return this.attachPromise;
    this.attachPromise = this.connection
      ['request']({ t: 'sub', channel: this.name })
      .then(() => {
        this.attached = true;
        this.connection['rememberSubscription'](this.name);
      })
      .finally(() => {
        this.attachPromise = null;
      });
    return this.attachPromise;
  }

  /**
   * Detach from the server (stop receiving messages and presence
   * events). Local listeners are preserved — call `off()` or `unsubscribe()` to
   * clear them.
   */
  async detach(): Promise<void> {
    if (!this.attached) return;
    await this.connection['request']({ t: 'unsub', channel: this.name });
    this.attached = false;
    this.connection['forgetSubscription'](this.name);
  }

  /**
   * Register a listener for message frames on this channel. Implicitly
   * attaches if needed. Returns an unsubscribe function.
   */
  override on(listener: ChannelEventListener): UnsubscribeFn;
  /** Register a listener for messages with a matching `name`. */
  override on(event: ChannelEventType, listener: ChannelEventListener): UnsubscribeFn;
  override on(first: ChannelEventType | ChannelEventListener, second?: ChannelEventListener): UnsubscribeFn {
    const unsubscribe = second === undefined ? super.on(first as ChannelEventListener) : super.on(first as ChannelEventType, second);
    // Fire-and-forget attach; the listener stays registered even if
    // attach fails so a retry-on-reconnect surfaces the right state.
    this.attach().catch(() => {});
    return unsubscribe;
  }

  /** Resolve the next message with the matching `name`. */
  override once(event: ChannelEventType): Promise<ChannelEventResult>;
  /** Invoke `listener` one time for the next message on this channel. */
  override once(listener: ChannelEventListener): void;
  /** Invoke `listener` one time for the next message with a matching `name`. */
  override once(event: ChannelEventType, listener: ChannelEventListener): void;
  override once(first: ChannelEventType | ChannelEventListener, second?: ChannelEventListener): Promise<ChannelEventResult> | void {
    if (second === undefined && typeof first !== 'function') {
      const result = super.once(first);
      this.attach().catch(() => {});
      return result;
    }
    if (second === undefined) {
      super.once(first as ChannelEventListener);
      this.attach().catch(() => {});
      return;
    }
    super.once(first as ChannelEventType, second);
    this.attach().catch(() => {});
  }

  subscribe(listener: MessageListener): UnsubscribeFn {
    return this.on(listener);
  }

  /** Publish one application-level message to the channel. */
  async publish(name: string, data: unknown): Promise<void> {
    await this.attach();
    await this.connection['request']({ t: 'pub', channel: this.name, name, data });
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
