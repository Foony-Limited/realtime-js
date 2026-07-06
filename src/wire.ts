/**
 * Wire protocol types for the Foony Realtime WebSocket service. These are the in-memory
 * frame shapes; on the wire every frame travels in the binary opcode format (binary.ts),
 * with `t` as the local discriminator the codec maps to and from opcodes.
 *
 * Mirrors `services/realtime-saas/internal/wire/wire.go` exactly. Any
 * change here MUST be mirrored on the Go side and vice versa. The Go file is
 * the canonical source. Two tables have drifted before, so watch them: the
 * `FrameType` discriminator list (Go `Frame*` consts) and the `ErrorCode`
 * table (Go `Code*` consts). Both must stay one-for-one.
 *
 * Conventions:
 *  - Client-originated frames carry a numeric `id`; the server echoes it
 *    back on the matching `ack` / `err` frame so SDKs can correlate
 *    requests to responses.
 */

/**
 * Discriminator values for the `t` field. Must list every `Frame*` const in
 * the Go `FrameType` tables (`internal/wire/wire.go`). See the sync note in
 * this file's header.
 */
export type FrameType =
  // Client -> Server
  | 'auth'
  | 'sub'
  | 'unsub'
  | 'pub'
  | 'pres'
  | 'presSub'
  | 'presUnsub'
  | 'hist'
  | 'fetch'
  | 'ping'
  // Server -> Client
  | 'connected'
  | 'ack'
  | 'msg'
  | 'presEvt'
  | 'err'
  | 'pong'
  | 'histRes'
  | 'fetchRes';

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
  /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`). Absent for plain JSON. */
  readonly encoding?: string;
};

// ---- Client -> Server frames ----

/** First frame after the WebSocket handshake. Carries either a JWT token or API key. */
export type AuthFrame = {
  /** Frame discriminator: authentication handshake. */
  readonly t: 'auth';
  /** JWT minted by your server for token auth. Mutually exclusive with `key`. */
  readonly token?: string;
  /** API key for direct key auth from trusted servers. Mutually exclusive with `token`. */
  readonly key?: string;
  /** Stable identifier for this client, surfaced to others in presence. */
  readonly clientId?: string;
  /**
   * The connection id from a previous `connected` frame, sent on a reconnect so the server
   * reuses it. This keeps this connection's presence membership stable across a brief drop
   * (the member key does not change), so a quick reconnect causes no leave/enter churn.
   */
  readonly resumeConnectionId?: string;
};

/** Start delivering messages + presence for `channel`. */
export type SubscribeFrame = {
  /** Frame discriminator: subscribe request. */
  readonly t: 'sub';
  /** Channel to start receiving messages and presence for. */
  readonly channel: string;
  /** Client request id echoed back on the matching `ack` / `err` frame. */
  readonly id: number;
  /**
   * Resume cursor: when > 0, replay messages with serial > this before going live. The serial is
   * contiguous per channel and identical across cells, so this resume is exact and migration-safe.
   * A channel that has only seen unsequenced messages carries no cursor and resubscribes fresh.
   */
  readonly lastSerial?: number;
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
 * Publish a message to `channel`. Single by default (`name` + `data`). Set
 * `messages` to publish a batch: many messages under one `messageId`, stored
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
   * Fire-and-forget: deliver live to current subscribers but exclude from history and
   * connection-resume (and retain minimally). For transient events on a channel that
   * otherwise persists.
   */
  readonly ephemeral?: boolean;
  /**
   * Client-assigned message id, stable across resends. The server uses it as the
   * JetStream dedup key (`Nats-Msg-Id`), so a publish resent after a reconnect is
   * collapsed to one message (exactly-once).
   */
  readonly messageId: string;
  /**
   * How `data` is encoded, e.g. `cipher+aes-256-gcm/base64` for an encrypted
   * payload. Opaque to the server, which stores and forwards it verbatim, and
   * only SDKs interpret it. Absent means `data` is the plain JSON value.
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
  /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`). Absent for plain JSON. */
  readonly encoding?: string;
  /** Client request id echoed back on the matching `ack` / `err` frame. */
  readonly id: number;
};

/**
 * Start receiving presence events for `channel` (an initial snapshot of current
 * members, then live enter/update/leave). Independent of a message `sub`: a
 * channel used only for messages never sends this, so the server opens no
 * presence watcher for it.
 */
export type PresenceSubscribeFrame = {
  /** Frame discriminator: presence-subscribe request. */
  readonly t: 'presSub';
  /** Channel to start receiving presence events for. */
  readonly channel: string;
  /** Client request id echoed back on the matching `ack` / `err` frame. */
  readonly id: number;
};

/** Stop receiving presence events for `channel`. Does not remove this connection's own membership. */
export type PresenceUnsubscribeFrame = {
  /** Frame discriminator: presence-unsubscribe request. */
  readonly t: 'presUnsub';
  /** Channel to stop receiving presence events for. */
  readonly channel: string;
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
  /** Cursor: when set, return the messages with serial strictly below it (backward paging). */
  readonly before?: number;
  /** Client request id echoed back on the matching `histRes` / `err` frame. */
  readonly id: number;
};

/**
 * Surgical forward gap-fill: ask the server for the messages with serial > `fromSerial`
 * (oldest-first), without touching the live subscription. The SDK sends this when it detects a
 * serial gap mid-stream, so recovering a dropped message is a small read, not a re-subscribe.
 * Server replies with `fetchRes`.
 */
export type FetchFrame = {
  /** Frame discriminator: surgical gap-fill request. */
  readonly t: 'fetch';
  /** Channel to backfill. */
  readonly channel: string;
  /** Return messages with serial strictly greater than this. */
  readonly fromSerial: number;
  /** Client request id echoed back on the matching `fetchRes` / `err` frame. */
  readonly id: number;
};

/** Application-level liveness probe. Server replies with `pong`. */
export type PingFrame = {
  /** Frame discriminator: liveness probe. The server replies with `pong`. */
  readonly t: 'ping';
};

// ---- Server -> Client frames ----

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
  /**
   * Resume outcome for a `sub` that carried `lastSerial`: true when the missed messages
   * were replayed before this ack, false when the cursor had aged out of retention (a
   * discontinuity, messages may have been missed beyond the window). Absent for non-resume
   * requests.
   */
  readonly resumed?: boolean;
  /**
   * For a publish ack: the contiguous per-channel serial the server assigned (0/absent for
   * ephemeral/retained/unsequenced publishes), so the publisher can advance its own cursor.
   */
  readonly seq?: number;
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
  /** Unique message id, for dedup and idempotent publishing. */
  readonly messageId: string;
  /** Client id of the publisher, when known. */
  readonly clientId?: string;
  /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`). Absent for plain JSON. */
  readonly encoding?: string;
  /**
   * Contiguous per-channel serial (0/absent for ephemeral/retained/unsequenced messages). The
   * SDK uses it to detect gaps (serial != last+1), as the migration-safe resume cursor, and as
   * history's `before` cursor to page backward from this message. For a bundle the outer serial
   * is absent and each member carries its own.
   */
  readonly seq?: number;
  /** Batch members; when set, this frame carries a batch and `name`/`data` are ignored. */
  readonly messages?: readonly BatchMember[];
  /** Fire-and-forget message: not stored in history and not replayed on resume. */
  readonly ephemeral?: boolean;
  /**
   * Server-coalesced bundle: several independent publishes the edge packed into
   * one stream record. Each member is a full message with its own server-stamped
   * `clientId` + `messageId` (and may itself be a client batch). When set,
   * `name`/`data`/`messages` are ignored. The SDK unwraps the members and dedups
   * by (clientId, messageId).
   */
  readonly bundle?: readonly BundledMessage[];
};

/**
 * One member of a server-coalesced {@link MessageFrame.bundle}. It is a delivered
 * message minus the redundant `t`/`channel` (taken from the carrying frame): its
 * own id/clientId/timestamp/encoding, plus `messages` when the member was itself a
 * client batch.
 */
export type BundledMessage = {
  /** Application-level event name the message was published under. */
  readonly name: string;
  /** Message payload as published. */
  readonly data: unknown;
  /** Server publish time in milliseconds since the Unix epoch. */
  readonly timestamp: number;
  /** This member's own message id, for dedup and idempotent publishing. */
  readonly messageId: string;
  /** Client id of the publisher, when known. */
  readonly clientId?: string;
  /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`). Absent for plain JSON. */
  readonly encoding?: string;
  /** This member's contiguous per-channel serial (see {@link MessageFrame.seq}). */
  readonly seq?: number;
  /** Fire-and-forget message: not stored in history and not replayed on resume. */
  readonly ephemeral?: boolean;
  /** Batch members, set when this member was itself a client batch. */
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
  /** How `data` is encoded (e.g. `cipher+aes-256-gcm/base64`). Absent for plain JSON. */
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

/** Response to `fetch`. The missed messages oldest-first plus whether the gap was fillable. */
export type FetchResponseFrame = {
  /** Frame discriminator: surgical gap-fill result. */
  readonly t: 'fetchRes';
  /** Echoes the id of the `fetch` request this responds to. */
  readonly id: number;
  /** Channel the gap-fill was for. */
  readonly channel: string;
  /** Missed messages with serial > the requested cursor, oldest-first. */
  readonly messages: readonly MessageFrame[];
  /**
   * True when the cursor was still within the retained window (the gap is filled by `messages`),
   * false when it had aged out — a discontinuity the SDK surfaces and re-baselines from instead of
   * re-applying.
   */
  readonly resumed: boolean;
};

/** Any frame the client may send. */
export type ClientFrame =
  | AuthFrame
  | SubscribeFrame
  | UnsubscribeFrame
  | PublishFrame
  | PresenceFrame
  | PresenceSubscribeFrame
  | PresenceUnsubscribeFrame
  | HistoryFrame
  | FetchFrame
  | PingFrame;

/** Any frame the server may send. */
export type ServerFrame =
  | ConnectedFrame
  | AckFrame
  | MessageFrame
  | PresenceEventFrame
  | ErrorFrame
  | PongFrame
  | HistoryResponseFrame
  | FetchResponseFrame;

/**
 * Error codes the server uses on `err` frames. Mirrors the `Code*` constant
 * table in the Go `internal/wire/wire.go` (the canonical source) one-for-one.
 * See the sync note in this file's header. Keep the order and values identical.
 */
export const ErrorCode = {
  /** Malformed or unparseable frame. */
  BadFrame: 40001,
  /** Authentication failed (bad token or key). */
  BadAuth: 40101,
  /** Previously valid auth has expired. Re-authenticate. */
  AuthExpired: 40102,
  /** Authenticated but not permitted for this channel or action. */
  Forbidden: 40300,
  /** The token's capability does not grant the requested action. */
  Capability: 40301,
  /** The token's capability does not grant access to this specific channel. */
  ChannelDenied: 40302,
  /** Referenced resource (e.g. channel) does not exist. */
  NotFound: 40400,
  /** Too many requests. The publish or connection rate limit was exceeded. */
  RateLimited: 42900,
  /** Unexpected server-side error. */
  Server: 50000,
  /** The edge could not bootstrap its NATS streams/buckets. Retry later. */
  Bootstrap: 50001,
} as const;

/** Union of the {@link ErrorCode} member names (e.g. `'Forbidden'`), for typing error handlers. */
export type ErrorCodeName = keyof typeof ErrorCode;
