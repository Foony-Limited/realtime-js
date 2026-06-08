/**
 * Public entry point for @foony/realtime.
 *
 * Public surface: a `Realtime` class, a `channels.get(name)` registry,
 * and per-channel `subscribe` / `publish` / `presence` methods.
 *
 * For server-side token minting see `@foony/realtime/server`.
 */
export { Realtime, type RealtimeOptions } from './realtime.js';
export { Channel, Presence, type UnsubscribeFn } from './channel.js';
export { Connection, type ConnectionOptions, type ConnectionState, type ConnectionStateListener, type MessageListener, type PresenceEventListener, } from './connection.js';
export { type AckFrame, type AuthFrame, type ClientFrame, type ConnectedFrame, type ErrorFrame, type FrameType, type HistoryResponseFrame, type MessageFrame, type PingFrame, type PongFrame, type PresenceAction, type PresenceEventFrame, type PresenceFrame, type PublishFrame, type ServerFrame, type SubscribeFrame, type UnsubscribeFrame, ErrorCode, type ErrorCodeName, } from './wire.js';
//# sourceMappingURL=index.d.ts.map