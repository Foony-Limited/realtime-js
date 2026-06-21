/**
 * Wire protocol types for the Foony Realtime WebSocket service.
 *
 * Mirrors `services/realtime-saas/internal/wire/wire.go` exactly — any
 * change here MUST be mirrored on the Go side and vice versa.
 *
 * Conventions:
 *  - Every frame has a single-character `t` discriminator.
 *  - Client-originated frames carry a numeric `id`; the server echoes it
 *    back on the matching `ack` / `err` frame so SDKs can correlate
 *    requests to responses.
 *  - Field names favor readability over brevity (`channel`, not `ch`),
 *    except for `t`, which we keep short because every frame carries it.
 */
/** Discriminator values for the `t` field. */
export type FrameType = 'auth' | 'sub' | 'unsub' | 'pub' | 'pres' | 'hist' | 'ping' | 'connected' | 'ack' | 'msg' | 'presEvt' | 'err' | 'pong' | 'histRes';
/** Recognized presence transition values. */
export type PresenceAction = 'enter' | 'leave' | 'update';
/**
 * One member of a batch publish. A batch bundles many messages into a single
 * `pub`/`msg` frame under one `messageId` (the batch is the atomic unit), so the
 * server stores and dedups it as one durable message while subscribers see the
 * members individually. `encoding` is per-member (each is encrypted on its own).
 */
export type BatchMember = {
    /** Application-level event name subscribers filter on. */
    readonly name: string;
    /** Arbitrary JSON-serializable payload. */
    readonly data: unknown;
    /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`); absent for plain JSON. */
    readonly encoding?: string;
};
/** First frame after the WebSocket handshake. Carries either a JWT token or API key. */
export type AuthFrame = {
    /** Frame discriminator: authentication handshake. */
    readonly t: 'auth';
    /** JWT minted by your server for token auth; mutually exclusive with `key`. */
    readonly token?: string;
    /** API key for direct key auth from trusted servers; mutually exclusive with `token`. */
    readonly key?: string;
    /** Stable identifier for this client, surfaced to others in presence. */
    readonly clientId?: string;
};
/** Start delivering messages + presence for `channel`. */
export type SubscribeFrame = {
    /** Frame discriminator: subscribe request. */
    readonly t: 'sub';
    /** Channel to start receiving messages and presence for. */
    readonly channel: string;
    /** Client request id echoed back on the matching `ack` / `err` frame. */
    readonly id: number;
    /** Optional resume cursor; when set, replay messages with id > this. */
    readonly lastMessageId?: string;
};
/** Stop delivering messages + presence for `channel`. */
export type UnsubscribeFrame = {
    /** Frame discriminator: unsubscribe request. */
    readonly t: 'unsub';
    /** Channel to stop receiving messages and presence for. */
    readonly channel: string;
    /** Client request id echoed back on the matching `ack` / `err` frame. */
    readonly id: number;
};
/**
 * Publish a message to `channel`. Single by default (`name` + `data`); set
 * `messages` to publish a batch — many messages under one `messageId`, stored
 * and deduped by the server as one durable message. When `messages` is present,
 * `name`/`data` are ignored.
 */
export type PublishFrame = {
    /** Frame discriminator: publish request. */
    readonly t: 'pub';
    /** Channel to publish the message to. */
    readonly channel: string;
    /** Application-level event name subscribers filter on. */
    readonly name: string;
    /** Arbitrary JSON-serializable payload delivered to subscribers. */
    readonly data: unknown;
    /** Batch members; when set, this is a batch publish and `name`/`data` are ignored. */
    readonly messages?: readonly BatchMember[];
    /**
     * Client-assigned message id, stable across resends. The server uses it as the
     * JetStream dedup key (`Nats-Msg-Id`), so a publish resent after a reconnect is
     * collapsed to one message (exactly-once).
     */
    readonly messageId: string;
    /**
     * Requested retention for this message, in milliseconds. The server clamps it
     * to your plan's ceiling; omit it for the short default (ephemeral). Set a
     * larger value to opt into durable history.
     */
    readonly ttlMs?: number;
    /**
     * How `data` is encoded, e.g. `cipher+aes-256-gcm/base64` for an encrypted
     * payload. Opaque to the server, which stores and forwards it verbatim; only
     * SDKs interpret it. Absent means `data` is the plain JSON value.
     */
    readonly encoding?: string;
    /** Client request id echoed back on the matching `ack` / `err` frame. */
    readonly id: number;
};
/** Mutate the publisher's presence membership in `channel`. */
export type PresenceFrame = {
    /** Frame discriminator: presence mutation request. */
    readonly t: 'pres';
    /** Channel whose presence set is being mutated. */
    readonly channel: string;
    /** Presence transition to apply (enter, leave, or update). */
    readonly action: PresenceAction;
    /** Optional presence payload attached to an enter or update. */
    readonly data?: unknown;
    /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`); absent for plain JSON. */
    readonly encoding?: string;
    /** Client request id echoed back on the matching `ack` / `err` frame. */
    readonly id: number;
};
/** Request recent messages for `channel`. Server replies with `histRes`. */
export type HistoryFrame = {
    /** Frame discriminator: history query. */
    readonly t: 'hist';
    /** Channel to read history for. */
    readonly channel: string;
    /** Maximum number of messages to return (server caps this). */
    readonly limit?: number;
    /** Cursor: when set, page backward from this message id (exclusive). */
    readonly start?: string;
    /** Client request id echoed back on the matching `histRes` / `err` frame. */
    readonly id: number;
};
/** Application-level liveness probe. Server replies with `pong`. */
export type PingFrame = {
    /** Frame discriminator: liveness probe; server replies with `pong`. */
    readonly t: 'ping';
};
/** Sent once after a successful auth handshake. */
export type ConnectedFrame = {
    /** Frame discriminator: post-auth confirmation. */
    readonly t: 'connected';
    /** Server-assigned identifier for this connection. */
    readonly connectionId: string;
    /** Interval, in ms, the client should ping within to stay connected. */
    readonly keepAliveMs: number;
    /** Resolved client id for this connection (from auth or server-assigned). */
    readonly clientId: string;
};
/** Acks a client request that does not need a structured reply. */
export type AckFrame = {
    /** Frame discriminator: request acknowledgement. */
    readonly t: 'ack';
    /** Echoes the id of the client request being acknowledged. */
    readonly id: number;
};
/** Server-originated channel message. */
export type MessageFrame = {
    /** Frame discriminator: channel message. */
    readonly t: 'msg';
    /** Channel the message was published to. */
    readonly channel: string;
    /** Application-level event name the message was published under. */
    readonly name: string;
    /** Message payload as published. */
    readonly data: unknown;
    /** Server publish time in milliseconds since the Unix epoch. */
    readonly timestamp: number;
    /** Unique, ordered message id usable as a history resume cursor. */
    readonly messageId: string;
    /** Client id of the publisher, when known. */
    readonly clientId?: string;
    /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`); absent for plain JSON. */
    readonly encoding?: string;
    /** Batch members; when set, this frame carries a batch and `name`/`data` are ignored. */
    readonly messages?: readonly BatchMember[];
};
/** Server-originated presence transition. */
export type PresenceEventFrame = {
    /** Frame discriminator: presence transition. */
    readonly t: 'presEvt';
    /** Channel the presence transition occurred on. */
    readonly channel: string;
    /** Which presence transition occurred (enter, leave, or update). */
    readonly action: PresenceAction;
    /** Client id of the member whose presence changed. */
    readonly clientId: string;
    /** Connection id of the member whose presence changed. */
    readonly connectionId: string;
    /** Presence payload supplied on enter/update, if any. */
    readonly data?: unknown;
    /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`); absent for plain JSON. */
    readonly encoding?: string;
    /** Transition time in milliseconds since the Unix epoch. */
    readonly timestamp: number;
};
/** Protocol or authorization error related to a specific client request. */
export type ErrorFrame = {
    /** Frame discriminator: error response. */
    readonly t: 'err';
    /** Id of the client request that failed, when the error is request-scoped. */
    readonly id?: number;
    /** Machine-readable error code; see {@link ErrorCode}. */
    readonly code: number;
    /** Human-readable error description for logging and debugging. */
    readonly message: string;
};
/** Response to `ping`. */
export type PongFrame = {
    /** Frame discriminator: reply to `ping`. */
    readonly t: 'pong';
};
/** Response to `hist`. Messages oldest-first. */
export type HistoryResponseFrame = {
    /** Frame discriminator: history query result. */
    readonly t: 'histRes';
    /** Echoes the id of the `hist` request this responds to. */
    readonly id: number;
    /** Channel the history was queried for. */
    readonly channel: string;
    /** Matching messages, ordered oldest-first. */
    readonly messages: readonly MessageFrame[];
    /** True when older messages remain beyond this page. */
    readonly more?: boolean;
};
/** Any frame the client may send. */
export type ClientFrame = AuthFrame | SubscribeFrame | UnsubscribeFrame | PublishFrame | PresenceFrame | HistoryFrame | PingFrame;
/** Any frame the server may send. */
export type ServerFrame = ConnectedFrame | AckFrame | MessageFrame | PresenceEventFrame | ErrorFrame | PongFrame | HistoryResponseFrame;
/**
 * Error codes the server uses on `err` frames. Mirrors the Go
 * `internal/wire` constants — keep in sync.
 */
export declare const ErrorCode: {
    /** Malformed or unparseable frame. */
    readonly BadFrame: 40001;
    /** Authentication failed (bad token or key). */
    readonly BadAuth: 40101;
    /** Previously valid auth has expired; re-authenticate. */
    readonly AuthExpired: 40102;
    /** Authenticated but not permitted for this channel or action. */
    readonly Forbidden: 40300;
    /** Referenced resource (e.g. channel) does not exist. */
    readonly NotFound: 40400;
    /** Unexpected server-side error. */
    readonly Server: 50000;
};
/** Union of the {@link ErrorCode} member names (e.g. `'Forbidden'`), for typing error handlers. */
export type ErrorCodeName = keyof typeof ErrorCode;
//# sourceMappingURL=wire.d.ts.map