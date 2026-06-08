/**
 * Low-level WebSocket connection manager. Handles framing, request /
 * response correlation, and dispatch to per-channel listeners.
 *
 * The class is intentionally protocol-aware but channel-agnostic — the
 * Channel and Realtime classes layer the public API on top.
 */

import type {
  AckFrame,
  AuthFrame,
  ClientFrame,
  ConnectedFrame,
  ErrorFrame,
  MessageFrame,
  PresenceEventFrame,
  PresenceFrame,
  PublishFrame,
  ServerFrame,
  SubscribeFrame,
  UnsubscribeFrame,
} from './wire.js';

/**
 * Frames the SDK can issue with `request()`. Each carries an `id` the
 * server echoes on the matching ack/err frame; Connection assigns the
 * id so callers can omit it.
 */
export type AckableFrame =
  | Omit<SubscribeFrame, 'id'>
  | Omit<UnsubscribeFrame, 'id'>
  | Omit<PublishFrame, 'id'>
  | Omit<PresenceFrame, 'id'>;

/** Options that control how Connection reaches the edge. */
export type ConnectionOptions = {
  /** ws:// or wss:// URL pointing at the realtime edge binary. */
  readonly url: string;
  /**
   * A Realtime API key in `appSlug.publicKeyId:privateKey` form. Convenient for trusted
   * quick starts and server-side scripts; browser apps should prefer JWTs
   * returned from `authCallback`.
   */
  readonly key?: string;
  /** Optional client id to attach to a direct key-auth connection. */
  readonly clientId?: string;
  /**
   * A static JWT to send in the auth handshake. Mutually exclusive with
   * `authCallback`. Useful for local dev and short scripts.
   */
  readonly token?: string;
  /**
   * Async callback that returns a fresh JWT. Called once on connect and
   * again on every reconnect. Use this when the token is short-lived
   * (the production path).
   */
  readonly authCallback?: () => Promise<string> | string;
  /**
   * Override the global WebSocket constructor. Mostly useful in tests;
   * defaults to `globalThis.WebSocket` which is present in browsers and
   * Node 22+.
   */
  readonly webSocket?: typeof WebSocket;
  /**
   * If true, attempt to reconnect after unexpected disconnects with
   * exponential backoff. Defaults to true.
   */
  readonly autoReconnect?: boolean;
  /**
   * Initial backoff for reconnects (default 1000ms). Doubles each
   * attempt up to maxReconnectDelayMs.
   */
  readonly initialReconnectDelayMs?: number;
  /** Cap on the reconnect backoff (default 30000ms). */
  readonly maxReconnectDelayMs?: number;
};

/** Connection lifecycle states. */
export type ConnectionState =
  | 'initialized'
  | 'connecting'
  | 'connected'
  | 'disconnected'
  | 'closing'
  | 'closed'
  | 'failed';

/** Listener for state transitions. */
export type ConnectionStateListener = (state: ConnectionState, reason?: Error) => void;

/** Internal record kept for every in-flight ack/err request. */
type PendingRequest = {
  readonly resolve: (frame: AckFrame) => void;
  readonly reject: (error: Error) => void;
};

/** Listener invoked for every message frame on a channel. */
export type MessageListener = (message: MessageFrame) => void;

/** Listener invoked for every presence event frame on a channel. */
export type PresenceEventListener = (event: PresenceEventFrame) => void;

/**
 * Internal listener registry, keyed by channel name. Connection owns
 * the maps so reconnect can transparently re-subscribe.
 */
type ChannelListeners = {
  readonly messages: Set<MessageListener>;
  readonly presence: Set<PresenceEventListener>;
};

const DEFAULT_INITIAL_RECONNECT_DELAY_MS = 1_000;
const DEFAULT_MAX_RECONNECT_DELAY_MS = 30_000;
/** WebSocket.OPEN — duplicated here so we do not depend on a global. */
const READY_STATE_OPEN = 1;

/**
 * Connection is the transport layer. One Realtime client owns one
 * Connection; channels share it.
 */
export class Connection {
  readonly options: ConnectionOptions;
  private socket: WebSocket | null = null;
  private state: ConnectionState = 'initialized';
  private connectionId: string | null = null;
  private serverClientId: string | null = null;
  private nextRequestId = 1;
  private readonly pending = new Map<number, PendingRequest>();
  private readonly channelListeners = new Map<string, ChannelListeners>();
  private readonly stateListeners = new Set<ConnectionStateListener>();
  private connectPromise: Promise<void> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;
  /** Channels the SDK has asked to be subscribed to; re-sent on reconnect. */
  private readonly desiredSubscriptions = new Set<string>();

  constructor(options: ConnectionOptions) {
    const authMethods = Number(Boolean(options.token)) + Number(Boolean(options.authCallback)) + Number(Boolean(options.key));
    if (authMethods !== 1) {
      throw new Error('Connection: pass exactly one of options.token, options.authCallback, or options.key');
    }
    this.options = options;
  }

  /** Current connection state. */
  getState(): ConnectionState {
    return this.state;
  }

  /** The server-issued connection id, populated after a successful auth handshake. */
  getConnectionId(): string | null {
    return this.connectionId;
  }

  /** The client id encoded in the token, populated after auth. */
  getClientId(): string | null {
    return this.serverClientId;
  }

  /** Register a state-change listener. Returns an unsubscribe function. */
  onStateChange(listener: ConnectionStateListener): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  /**
   * Open the WebSocket and complete the auth handshake. Idempotent —
   * concurrent calls await the same in-flight connect.
   */
  async connect(): Promise<void> {
    if (this.state === 'connected') return;
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = this.doConnect().finally(() => {
      this.connectPromise = null;
    });
    return this.connectPromise;
  }

  /** Close the WebSocket and release resources. */
  async close(): Promise<void> {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.setState('closing');
    if (this.socket && this.socket.readyState === READY_STATE_OPEN) {
      this.socket.close(1000, 'client close');
    }
    this.setState('closed');
    for (const pending of this.pending.values()) {
      pending.reject(new Error('connection closed'));
    }
    this.pending.clear();
  }

  /**
   * Send a frame that expects an ack. Returns the matching AckFrame, or
   * rejects with the server's ErrorFrame (wrapped in an Error).
   */
  async request(frame: AckableFrame): Promise<AckFrame> {
    await this.connect();
    const id = this.nextRequestId++;
    const out = { ...frame, id } as ClientFrame;
    return new Promise<AckFrame>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      try {
        this.sendRaw(out);
      } catch (err) {
        this.pending.delete(id);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  /** Send a fire-and-forget frame (no ack expected). */
  async send(frame: ClientFrame): Promise<void> {
    await this.connect();
    this.sendRaw(frame);
  }

  /**
   * Register listeners for a channel. Connection remembers the
   * registration so it can re-attach across reconnects, but actually
   * issuing the `sub` frame is the caller's job (Channel does that).
   */
  addChannelListeners(channel: string): ChannelListeners {
    let entry = this.channelListeners.get(channel);
    if (!entry) {
      entry = { messages: new Set(), presence: new Set() };
      this.channelListeners.set(channel, entry);
    }
    return entry;
  }

  /** Forget all listeners for a channel. Called from Channel.detach. */
  removeChannelListeners(channel: string): void {
    this.channelListeners.delete(channel);
  }

  /** Add `channel` to the set of subscriptions to restore on reconnect. */
  rememberSubscription(channel: string): void {
    this.desiredSubscriptions.add(channel);
  }

  /** Stop restoring this subscription on future reconnects. */
  forgetSubscription(channel: string): void {
    this.desiredSubscriptions.delete(channel);
  }

  // ---- internals ----

  private async doConnect(): Promise<void> {
    this.setState('connecting');
    const ws = this.makeSocket();
    this.socket = ws;
    const authFrame = await this.createAuthFrame();

    return new Promise<void>((resolve, reject) => {
      const onOpen = (): void => {
        try {
          ws.send(JSON.stringify(authFrame));
        } catch (err) {
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      };

      const onAuthMessage = (event: MessageEvent): void => {
        let parsed: ServerFrame;
        try {
          parsed = JSON.parse(typeof event.data === 'string' ? event.data : event.data.toString()) as ServerFrame;
        } catch (err) {
          reject(new Error(`failed to parse auth response: ${(err as Error).message}`));
          ws.close(1002, 'bad auth response');
          return;
        }
        if (parsed.t === 'connected') {
          const connected = parsed as ConnectedFrame;
          this.connectionId = connected.connectionId;
          this.serverClientId = connected.clientId;
          this.reconnectAttempt = 0;
          // Hand future frames over to the steady-state handler.
          ws.removeEventListener('message', onAuthMessage as EventListener);
          ws.addEventListener('message', this.handleMessage);
          this.setState('connected');
          resolve();
          this.restoreSubscriptionsOnReconnect();
        } else if (parsed.t === 'err') {
          const errFrame = parsed as ErrorFrame;
          ws.close(1002, `auth error ${errFrame.code}`);
          reject(new Error(`auth failed: ${errFrame.code} ${errFrame.message}`));
        } else {
          reject(new Error(`unexpected first frame: ${parsed.t}`));
          ws.close(1002, 'unexpected frame');
        }
      };

      const onError = (event: Event): void => {
        const errMessage = (event as ErrorEvent).message ?? 'websocket error';
        reject(new Error(`websocket error: ${errMessage}`));
      };

      const onClose = (event: CloseEvent): void => {
        this.handleClose(event);
        // If close fires before we finished the handshake, the
        // surrounding promise hasn't been settled yet — surface it as
        // a connect failure.
        reject(new Error(`websocket closed during handshake: ${event.code} ${event.reason}`));
      };

      ws.addEventListener('open', onOpen);
      ws.addEventListener('message', onAuthMessage as EventListener);
      ws.addEventListener('error', onError);
      ws.addEventListener('close', onClose, { once: true });
    });
  }

  private makeSocket(): WebSocket {
    const ctor = this.options.webSocket ?? (globalThis as typeof globalThis & { WebSocket?: typeof WebSocket }).WebSocket;
    if (!ctor) {
      throw new Error('Connection: no WebSocket implementation available. Pass options.webSocket or upgrade to Node 22+.');
    }
    return new ctor(this.options.url);
  }

  private async createAuthFrame(): Promise<AuthFrame> {
    if (this.options.key) {
      return {
        t: 'auth',
        key: this.options.key,
        ...(this.options.clientId ? { clientId: this.options.clientId } : {}),
      };
    }
    if (this.options.token) return { t: 'auth', token: this.options.token };
    if (!this.options.authCallback) {
      throw new Error('Connection: missing auth method');
    }
    return { t: 'auth', token: await this.options.authCallback() };
  }

  /** Steady-state message handler; installed after a successful auth. */
  private readonly handleMessage = (event: MessageEvent): void => {
    let frame: ServerFrame;
    try {
      frame = JSON.parse(typeof event.data === 'string' ? event.data : event.data.toString()) as ServerFrame;
    } catch {
      return;
    }
    switch (frame.t) {
      case 'ack': {
        const pending = this.pending.get(frame.id);
        if (pending) {
          this.pending.delete(frame.id);
          pending.resolve(frame);
        }
        return;
      }
      case 'err': {
        if (frame.id != null) {
          const pending = this.pending.get(frame.id);
          if (pending) {
            this.pending.delete(frame.id);
            pending.reject(new Error(`server error ${frame.code}: ${frame.message}`));
            return;
          }
        }
        // Unscoped errors (id 0 or missing) are surfaced via state-change
        // listeners; SDK consumers can subscribe via onStateChange.
        for (const listener of this.stateListeners) {
          listener(this.state, new Error(`server error ${frame.code}: ${frame.message}`));
        }
        return;
      }
      case 'msg': {
        const listeners = this.channelListeners.get(frame.channel);
        if (listeners) {
          for (const listener of listeners.messages) listener(frame);
        }
        return;
      }
      case 'presEvt': {
        const listeners = this.channelListeners.get(frame.channel);
        if (listeners) {
          for (const listener of listeners.presence) listener(frame);
        }
        return;
      }
      case 'pong':
      case 'connected':
      case 'histRes':
        // Connected can only fire once (we removed the auth listener
        // above); pong and histRes are unused in the MVP — silent
        // forwarding keeps the switch exhaustive.
        return;
    }
  };

  private handleClose(_event: CloseEvent): void {
    this.socket = null;
    if (this.state === 'closing' || this.state === 'closed') {
      this.setState('closed');
      return;
    }
    this.setState('disconnected');
    if (this.options.autoReconnect === false) return;
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    const initial = this.options.initialReconnectDelayMs ?? DEFAULT_INITIAL_RECONNECT_DELAY_MS;
    const max = this.options.maxReconnectDelayMs ?? DEFAULT_MAX_RECONNECT_DELAY_MS;
    const delay = Math.min(initial * 2 ** this.reconnectAttempt, max);
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect().catch(() => {
        // doConnect itself drove the state machine; schedule another
        // attempt unless we've been explicitly closed in the meantime.
        if (this.state !== 'closed' && this.state !== 'closing') {
          this.scheduleReconnect();
        }
      });
    }, delay);
  }

  private restoreSubscriptionsOnReconnect(): void {
    // The Channel layer is responsible for re-issuing `sub` frames; we
    // expose desiredSubscriptions so it can iterate without leaking the
    // set.
    for (const channel of this.desiredSubscriptions) {
      this.request({ t: 'sub', channel }).catch(() => {
        // Failure to restore a subscription bubbles up via state
        // listeners on the next request; nothing else to do here.
      });
    }
  }

  private sendRaw(frame: ClientFrame): void {
    if (!this.socket || this.socket.readyState !== READY_STATE_OPEN) {
      throw new Error(`Connection.sendRaw: socket not open (state=${this.state})`);
    }
    this.socket.send(JSON.stringify(frame));
  }

  private setState(state: ConnectionState): void {
    if (this.state === state) return;
    this.state = state;
    for (const listener of this.stateListeners) listener(state);
  }
}
