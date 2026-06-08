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
/** First frame after the WebSocket handshake. Carries either a JWT token or API key. */
export type AuthFrame = {
    readonly t: 'auth';
    readonly token?: string;
    readonly key?: string;
    readonly clientId?: string;
};
/** Start delivering messages + presence for `channel`. */
export type SubscribeFrame = {
    readonly t: 'sub';
    readonly channel: string;
    readonly id: number;
    /** Optional resume cursor; when set, replay messages with id > this. */
    readonly lastMessageId?: string;
};
/** Stop delivering messages + presence for `channel`. */
export type UnsubscribeFrame = {
    readonly t: 'unsub';
    readonly channel: string;
    readonly id: number;
};
/** Publish a single application-level message to `channel`. */
export type PublishFrame = {
    readonly t: 'pub';
    readonly channel: string;
    readonly name: string;
    readonly data: unknown;
    readonly id: number;
};
/** Mutate the publisher's presence membership in `channel`. */
export type PresenceFrame = {
    readonly t: 'pres';
    readonly channel: string;
    readonly action: PresenceAction;
    readonly data?: unknown;
    readonly id: number;
};
/** Application-level liveness probe. Server replies with `pong`. */
export type PingFrame = {
    readonly t: 'ping';
};
/** Sent once after a successful auth handshake. */
export type ConnectedFrame = {
    readonly t: 'connected';
    readonly connectionId: string;
    readonly keepAliveMs: number;
    readonly clientId: string;
};
/** Acks a client request that does not need a structured reply. */
export type AckFrame = {
    readonly t: 'ack';
    readonly id: number;
};
/** Server-originated channel message. */
export type MessageFrame = {
    readonly t: 'msg';
    readonly channel: string;
    readonly name: string;
    readonly data: unknown;
    readonly timestamp: number;
    readonly messageId: string;
    readonly clientId?: string;
};
/** Server-originated presence transition. */
export type PresenceEventFrame = {
    readonly t: 'presEvt';
    readonly channel: string;
    readonly action: PresenceAction;
    readonly clientId: string;
    readonly connectionId: string;
    readonly data?: unknown;
    readonly timestamp: number;
};
/** Protocol or authorization error related to a specific client request. */
export type ErrorFrame = {
    readonly t: 'err';
    readonly id?: number;
    readonly code: number;
    readonly message: string;
};
/** Response to `ping`. */
export type PongFrame = {
    readonly t: 'pong';
};
/** Response to `hist`. Messages oldest-first. */
export type HistoryResponseFrame = {
    readonly t: 'histRes';
    readonly id: number;
    readonly channel: string;
    readonly messages: readonly MessageFrame[];
    readonly more?: boolean;
};
/** Any frame the client may send. */
export type ClientFrame = AuthFrame | SubscribeFrame | UnsubscribeFrame | PublishFrame | PresenceFrame | PingFrame;
/** Any frame the server may send. */
export type ServerFrame = ConnectedFrame | AckFrame | MessageFrame | PresenceEventFrame | ErrorFrame | PongFrame | HistoryResponseFrame;
/**
 * Error codes the server uses on `err` frames. Mirrors the Go
 * `internal/wire` constants — keep in sync.
 */
export declare const ErrorCode: {
    readonly BadFrame: 40001;
    readonly BadAuth: 40101;
    readonly AuthExpired: 40102;
    readonly Forbidden: 40300;
    readonly NotFound: 40400;
    readonly Server: 50000;
};
export type ErrorCodeName = keyof typeof ErrorCode;
//# sourceMappingURL=wire.d.ts.map