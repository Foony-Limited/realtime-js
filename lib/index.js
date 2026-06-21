/**
 * Public entry point for @foony/realtime.
 *
 * Public surface: a `Realtime` class, a `channels.get(name)` registry,
 * and per-channel `subscribe` / `publish` / `presence` methods.
 */
export { Realtime, } from './realtime.js';
export { Cipher, generateRandomKey, } from './crypto.js';
export { Channel, Presence, } from './channel.js';
export { Connection, DEFAULT_REALTIME_ENDPOINT, TypedEventEmitter, } from './connection.js';
export { ErrorCode, } from './wire.js';
//# sourceMappingURL=index.js.map