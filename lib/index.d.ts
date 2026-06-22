/**
 * Public entry point for @foony/realtime.
 *
 * Public surface: a `Realtime` class, a `channels.get(name)` registry,
 * and per-channel `subscribe` / `publish` / `presence` methods.
 */
export { Realtime, type RealtimeOptions, type ChannelOptions, type BatchSpec, type BatchMessage, type BatchPublishResult, } from './realtime.js';
export { Auth, createJwt, type Capability, type CreateJwtParams, type CreateJwtOptions, } from './auth.js';
export { Cipher, generateRandomKey, type CipherParams, type CipherAlgorithm, type EncryptResult, } from './crypto.js';
export { Channel, Presence, type BatchOptions, type ChannelEventListener, type ChannelEventResult, type ChannelEventType, type ChannelState, type ChannelStateChange, type ChannelStateListener, type PresenceEventResult, type PresenceEventType, type UnsubscribeFn, } from './channel.js';
export { Connection, DEFAULT_REALTIME_ENDPOINT, TypedEventEmitter, type ConnectionEventEmitter, type ConnectionEventListener, type ConnectionEventResult, type ConnectionEventType, type ConnectionOptions, type ConnectionState, type ConnectionStateListener, type EventEmitter, type EventUnsubscribeFn, type MessageListener, type PresenceEventListener, } from './connection.js';
export { type AckFrame, type AuthFrame, type BatchMember, type ClientFrame, type ConnectedFrame, type ErrorFrame, type FrameType, type HistoryFrame, type HistoryResponseFrame, type MessageFrame, type PingFrame, type PongFrame, type PresenceAction, type PresenceEventFrame, type PresenceFrame, type PublishFrame, type ServerFrame, type SubscribeFrame, type UnsubscribeFrame, ErrorCode, type ErrorCodeName, } from './wire.js';
//# sourceMappingURL=index.d.ts.map