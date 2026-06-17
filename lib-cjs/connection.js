"use strict";
/**
 * Low-level WebSocket connection manager. Handles framing, request /
 * response correlation, and dispatch to per-channel listeners.
 *
 * The class is intentionally protocol-aware but channel-agnostic — the
 * Channel and Realtime classes layer the public API on top.
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.Connection = exports.DEFAULT_REALTIME_ENDPOINT = exports.TypedEventEmitter = void 0;
/**
 * Small typed EventEmitter used by SDK surfaces that need both catch-all and
 * event-specific listeners.
 */
class TypedEventEmitter {
    listeners = new Set();
    listenersByEvent = new Map();
    toResult;
    constructor(toResult) {
        this.toResult = toResult;
    }
    on(first, second) {
        if (typeof first === 'function' && second === undefined) {
            const listener = first;
            this.listeners.add(listener);
            return () => this.off(listener);
        }
        if (second !== undefined) {
            const event = first;
            let listenersForEvent = this.listenersByEvent.get(event);
            if (!listenersForEvent) {
                listenersForEvent = new Set();
                this.listenersByEvent.set(event, listenersForEvent);
            }
            listenersForEvent.add(second);
            return () => this.off(event, second);
        }
        throw new Error('EventEmitter.on: pass a listener or an event and listener');
    }
    off(first, second) {
        if (first === undefined) {
            this.listeners.clear();
            this.listenersByEvent.clear();
            return;
        }
        if (typeof first === 'function' && second === undefined) {
            const listener = first;
            this.listeners.delete(listener);
            for (const listenersForEvent of this.listenersByEvent.values()) {
                listenersForEvent.delete(listener);
            }
            return;
        }
        if (second !== undefined) {
            const listenersForEvent = this.listenersByEvent.get(first);
            listenersForEvent?.delete(second);
            return;
        }
        throw new Error('EventEmitter.off: pass no args, a listener, or an event and listener');
    }
    once(first, second) {
        if (typeof first === 'function' && second === undefined) {
            const listener = first;
            const wrapped = ((...args) => {
                this.off(wrapped);
                listener(...args);
            });
            this.on(wrapped);
            return;
        }
        const event = first;
        if (second === undefined) {
            return new Promise((resolve) => {
                const wrapped = ((...args) => {
                    this.off(event, wrapped);
                    resolve(this.toResult(event, args));
                });
                this.on(event, wrapped);
            });
        }
        const listener = second;
        const wrapped = ((...args) => {
            this.off(event, wrapped);
            listener(...args);
        });
        this.on(event, wrapped);
    }
    emit(event, ...args) {
        for (const listener of [...this.listeners]) {
            listener(...args);
        }
        const listenersForEvent = this.listenersByEvent.get(event);
        if (!listenersForEvent) {
            return;
        }
        for (const listener of [...listenersForEvent]) {
            listener(...args);
        }
    }
}
exports.TypedEventEmitter = TypedEventEmitter;
/** Default Foony Realtime endpoint used when callers do not pass one. */
exports.DEFAULT_REALTIME_ENDPOINT = 'realtime.foony.com';
const DEFAULT_INITIAL_RECONNECT_DELAY_MS = 1_000;
const DEFAULT_MAX_RECONNECT_DELAY_MS = 30_000;
/** WebSocket.OPEN — duplicated here so we do not depend on a global. */
const READY_STATE_OPEN = 1;
/**
 * A client-assigned message id for a publish — `<unixMillis>-<random>`, so it is
 * roughly time-sortable like the server's ids. Generated once per publish and reused
 * across resends, so the server's dedup window can collapse a retried publish.
 */
function newClientMessageId() {
    const random = Math.floor(Math.random() * 0x1_0000_0000).toString(16).padStart(8, '0');
    return `${Date.now()}-${random}`;
}
/**
 * Connection is the transport layer. One Realtime client owns one
 * Connection; channels share it.
 *
 * Several methods here are `private` yet called from the sibling `Channel` and
 * `Realtime` classes via index access (e.g. `connection['rememberSubscription']`).
 * That is intentional: they form the SDK-internal contract between those classes,
 * and `private` keeps them off the public `@foony/realtime` type surface. A search
 * for `this.method(` will not find these call sites — look for `['method']` too.
 */
class Connection extends TypedEventEmitter {
    options;
    socket = null;
    state = 'initialized';
    connectionId = null;
    serverClientId = null;
    nextRequestId = 1;
    pending = new Map();
    pendingHistory = new Map();
    channelDispatchers = new Map();
    connectPromise = null;
    reconnectTimer = null;
    reconnectAttempt = 0;
    /** Channels the SDK has asked to be subscribed to; re-sent on reconnect. */
    desiredSubscriptions = new Set();
    /** Publishes awaiting ack, keyed by client messageId; (re)sent on (re)connect. */
    outstandingPublishes = new Map();
    /** Maps a send attempt's request id back to its publish messageId, to route ack/err. */
    publishRequestIds = new Map();
    constructor(options) {
        super((state, args) => {
            const reason = args[1];
            return reason === undefined ? { state } : { state, reason };
        });
        const authMethods = Number(Boolean(options.token)) + Number(Boolean(options.authCallback)) + Number(Boolean(options.key));
        if (authMethods !== 1) {
            throw new Error('Connection: pass exactly one of options.token, options.authCallback, or options.key');
        }
        this.options = options;
    }
    /** Current connection state. */
    getState() {
        return this.state;
    }
    /** The server-issued connection id, populated after a successful auth handshake. */
    getConnectionId() {
        return this.connectionId;
    }
    /** The client id encoded in the token, populated after auth. */
    getClientId() {
        return this.serverClientId;
    }
    /**
     * Open the WebSocket and complete the auth handshake. Idempotent —
     * concurrent calls await the same in-flight connect.
     */
    async connect() {
        if (this.state === 'connected')
            return;
        if (this.connectPromise)
            return this.connectPromise;
        this.connectPromise = this.doConnect().finally(() => {
            this.connectPromise = null;
        });
        return this.connectPromise;
    }
    /** Close the WebSocket and release resources. */
    async close() {
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
        for (const pending of this.pendingHistory.values()) {
            pending.reject(new Error('connection closed'));
        }
        this.pendingHistory.clear();
        this.failOutstandingPublishes(new Error('connection closed'));
    }
    /**
     * Send a frame that expects an ack. Returns the matching AckFrame, or
     * rejects with the server's ErrorFrame (wrapped in an Error).
     */
    async request(frame) {
        await this.connect();
        const id = this.nextRequestId++;
        const out = { ...frame, id };
        return new Promise((resolve, reject) => {
            this.pending.set(id, { resolve, reject });
            try {
                this.sendRaw(out);
            }
            catch (err) {
                this.pending.delete(id);
                reject(err instanceof Error ? err : new Error(String(err)));
            }
        });
    }
    /**
     * Send a `hist` frame and resolve with the matching `histRes`, or reject
     * with the server's error. Unlike `request`, history is correlated to a
     * dedicated response frame rather than a bare ack.
     */
    async requestHistory(frame) {
        await this.connect();
        const id = this.nextRequestId++;
        const out = { ...frame, id };
        return new Promise((resolve, reject) => {
            this.pendingHistory.set(id, { resolve, reject });
            try {
                this.sendRaw(out);
            }
            catch (err) {
                this.pendingHistory.delete(id);
                reject(err instanceof Error ? err : new Error(String(err)));
            }
        });
    }
    /**
     * Publish a message, resolving on the server ack. When connected, sends
     * immediately. When the connection is establishing or temporarily down and
     * `queueMessages` is enabled (the default), the publish is buffered and sent on
     * the next successful (re)connect. A publish that was already in flight when the
     * connection dropped is resent on reconnect (its stable `messageId` dedupes it
     * server-side). With `queueMessages` disabled, or in a terminal connection state,
     * it rejects fast.
     */
    async publish(input) {
        const frame = { ...input, messageId: newClientMessageId() };
        const queueMessages = this.options.queueMessages ?? true;
        const queueable = this.state === 'initialized' || this.state === 'connecting' || this.state === 'disconnected';
        if (this.state !== 'connected' && !(queueMessages && queueable)) {
            throw new Error(`Connection.publish: cannot publish while ${this.state}${queueMessages ? '' : ' (queueMessages disabled)'}`);
        }
        return new Promise((resolve, reject) => {
            const outstanding = { frame, resolve, reject, requestId: null };
            this.outstandingPublishes.set(frame.messageId, outstanding);
            if (this.state === 'connected') {
                this.sendPublish(outstanding);
            }
            else {
                // Buffered: kick a connect so it drains even if no reconnect is pending yet
                // (e.g. the very first publish); reconnect backoff drives subsequent retries.
                void this.connect().catch(() => { });
            }
        });
    }
    /** Send an outstanding publish on the current socket under a fresh request id. */
    sendPublish(outstanding) {
        const id = this.nextRequestId++;
        outstanding.requestId = id;
        this.publishRequestIds.set(id, outstanding.frame.messageId);
        try {
            this.sendRaw({ ...outstanding.frame, id });
        }
        catch {
            // Socket not actually open; leave it outstanding to (re)send on the next connect.
            this.publishRequestIds.delete(id);
            outstanding.requestId = null;
        }
    }
    /** (Re)send every outstanding publish not currently in flight. Called on (re)connect. */
    flushOutstandingPublishes() {
        for (const outstanding of this.outstandingPublishes.values()) {
            if (outstanding.requestId === null) {
                this.sendPublish(outstanding);
            }
        }
    }
    /** Settle the outstanding publish for `requestId`. Returns false if it wasn't a publish. */
    settlePublish(requestId, error) {
        const messageId = this.publishRequestIds.get(requestId);
        if (messageId === undefined)
            return false;
        this.publishRequestIds.delete(requestId);
        const outstanding = this.outstandingPublishes.get(messageId);
        if (outstanding) {
            this.outstandingPublishes.delete(messageId);
            if (error) {
                outstanding.reject(error);
            }
            else {
                outstanding.resolve();
            }
        }
        return true;
    }
    /** Reject every outstanding publish — used when no resend path remains. */
    failOutstandingPublishes(error) {
        this.publishRequestIds.clear();
        const outstanding = [...this.outstandingPublishes.values()];
        this.outstandingPublishes.clear();
        for (const item of outstanding) {
            item.reject(error);
        }
    }
    /** Register the Channel-owned dispatch callbacks used for inbound frames. */
    registerChannel(channel, dispatchers) {
        this.channelDispatchers.set(channel, dispatchers);
    }
    /** Forget a channel's frame dispatch callbacks when the channel is released. */
    unregisterChannel(channel) {
        this.channelDispatchers.delete(channel);
    }
    /** Add `channel` to the set of subscriptions to restore on reconnect. */
    rememberSubscription(channel) {
        this.desiredSubscriptions.add(channel);
    }
    /** Stop restoring this subscription on future reconnects. */
    forgetSubscription(channel) {
        this.desiredSubscriptions.delete(channel);
    }
    // ---- internals ----
    async doConnect() {
        this.setState('connecting');
        const ws = await this.makeSocket();
        this.socket = ws;
        const authFrame = await this.createAuthFrame();
        return new Promise((resolve, reject) => {
            const onOpen = () => {
                try {
                    ws.send(JSON.stringify(authFrame));
                }
                catch (err) {
                    reject(err instanceof Error ? err : new Error(String(err)));
                }
            };
            const onAuthMessage = (event) => {
                let parsed;
                try {
                    parsed = JSON.parse(typeof event.data === 'string' ? event.data : event.data.toString());
                }
                catch (err) {
                    reject(new Error(`failed to parse auth response: ${err.message}`));
                    ws.close(1002, 'bad auth response');
                    return;
                }
                if (parsed.t === 'connected') {
                    const connected = parsed;
                    this.connectionId = connected.connectionId;
                    this.serverClientId = connected.clientId;
                    this.reconnectAttempt = 0;
                    // Hand future frames over to the steady-state handler.
                    ws.removeEventListener('message', onAuthMessage);
                    ws.addEventListener('message', this.handleMessage);
                    this.setState('connected');
                    resolve();
                    this.restoreSubscriptionsOnReconnect();
                    this.flushOutstandingPublishes();
                }
                else if (parsed.t === 'err') {
                    const errFrame = parsed;
                    ws.close(1002, `auth error ${errFrame.code}`);
                    reject(new Error(`auth failed: ${errFrame.code} ${errFrame.message}`));
                }
                else {
                    reject(new Error(`unexpected first frame: ${parsed.t}`));
                    ws.close(1002, 'unexpected frame');
                }
            };
            const onError = (event) => {
                const errMessage = event.message ?? 'websocket error';
                reject(new Error(`websocket error: ${errMessage}`));
            };
            const onClose = (event) => {
                this.handleClose(event);
                // If close fires before we finished the handshake, the
                // surrounding promise hasn't been settled yet — surface it as
                // a connect failure.
                reject(new Error(`websocket closed during handshake: ${event.code} ${event.reason}`));
            };
            ws.addEventListener('open', onOpen);
            ws.addEventListener('message', onAuthMessage);
            ws.addEventListener('error', onError);
            ws.addEventListener('close', onClose, { once: true });
        });
    }
    async makeSocket() {
        const ctor = this.options.webSocket ??
            globalThis.WebSocket ??
            (await loadNodeWebSocket());
        if (!ctor) {
            throw new Error('Connection: no WebSocket implementation available. Pass options.webSocket or install the "ws" package.');
        }
        return new ctor(endpointToUrl(this.options.endpoint));
    }
    async createAuthFrame() {
        if (this.options.key) {
            return {
                t: 'auth',
                key: this.options.key,
                ...(this.options.clientId ? { clientId: this.options.clientId } : {}),
            };
        }
        if (this.options.token)
            return { t: 'auth', token: this.options.token };
        if (!this.options.authCallback) {
            throw new Error('Connection: missing auth method');
        }
        return { t: 'auth', token: await this.options.authCallback() };
    }
    /** Steady-state message handler; installed after a successful auth. */
    handleMessage = (event) => {
        let frame;
        try {
            frame = JSON.parse(typeof event.data === 'string' ? event.data : event.data.toString());
        }
        catch {
            return;
        }
        switch (frame.t) {
            case 'ack': {
                const pending = this.pending.get(frame.id);
                if (pending) {
                    this.pending.delete(frame.id);
                    pending.resolve(frame);
                    return;
                }
                this.settlePublish(frame.id, null);
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
                    if (this.settlePublish(frame.id, new Error(`server error ${frame.code}: ${frame.message}`))) {
                        return;
                    }
                    const pendingHistory = this.pendingHistory.get(frame.id);
                    if (pendingHistory) {
                        this.pendingHistory.delete(frame.id);
                        pendingHistory.reject(new Error(`server error ${frame.code}: ${frame.message}`));
                        return;
                    }
                }
                // Unscoped errors (id 0 or missing) are surfaced through the
                // current connection event so consumers can observe transport errors.
                this.emitState(this.state, new Error(`server error ${frame.code}: ${frame.message}`));
                return;
            }
            case 'msg': {
                this.channelDispatchers.get(frame.channel)?.message(frame);
                return;
            }
            case 'presEvt': {
                this.channelDispatchers.get(frame.channel)?.presence(frame);
                return;
            }
            case 'histRes': {
                const pending = this.pendingHistory.get(frame.id);
                if (pending) {
                    this.pendingHistory.delete(frame.id);
                    pending.resolve(frame);
                }
                return;
            }
            case 'pong':
            case 'connected':
                // Connected can only fire once (we removed the auth listener
                // above); pong is unused in the MVP — silent forwarding keeps
                // the switch exhaustive.
                return;
        }
    };
    handleClose(_event) {
        this.socket = null;
        // The dead socket's request ids will never be acked; drop the mappings.
        this.publishRequestIds.clear();
        if (this.state === 'closing' || this.state === 'closed') {
            this.setState('closed');
            this.failOutstandingPublishes(new Error('connection closed'));
            return;
        }
        this.setState('disconnected');
        const willRetry = this.options.autoReconnect !== false && (this.options.queueMessages ?? true);
        if (willRetry) {
            // Keep outstanding publishes (in-flight + buffered) to resend on reconnect.
            for (const outstanding of this.outstandingPublishes.values()) {
                outstanding.requestId = null;
            }
        }
        else {
            // No resend path remains → don't leave publishes hanging.
            this.failOutstandingPublishes(new Error('connection closed'));
        }
        if (this.options.autoReconnect === false)
            return;
        this.scheduleReconnect();
    }
    scheduleReconnect() {
        if (this.reconnectTimer)
            return;
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
    restoreSubscriptionsOnReconnect() {
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
    sendRaw(frame) {
        if (!this.socket || this.socket.readyState !== READY_STATE_OPEN) {
            throw new Error(`Connection.sendRaw: socket not open (state=${this.state})`);
        }
        this.socket.send(JSON.stringify(frame));
    }
    setState(state) {
        if (this.state === state)
            return;
        this.state = state;
        this.emitState(state);
    }
    emitState(state, reason) {
        if (reason === undefined) {
            this.emit(state, state);
            return;
        }
        this.emit(state, state, reason);
    }
}
exports.Connection = Connection;
/**
 * Lazily load the `ws` package for Node < 22, where there is no global
 * WebSocket. The specifier is built at runtime so browser bundlers (which
 * always have a global WebSocket) do not try to resolve or bundle `ws`.
 */
async function loadNodeWebSocket() {
    try {
        const specifier = 'ws';
        const mod = (await Promise.resolve(`${specifier}`).then(s => __importStar(require(s))));
        return mod.WebSocket ?? mod.default;
    }
    catch {
        return undefined;
    }
}
function endpointToUrl(endpoint = exports.DEFAULT_REALTIME_ENDPOINT) {
    if (/^wss?:\/\//u.test(endpoint)) {
        return endpoint;
    }
    return `wss://${endpoint}`;
}
//# sourceMappingURL=connection.js.map