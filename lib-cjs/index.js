"use strict";
/**
 * Public entry point for @foony/realtime.
 *
 * Public surface: a `Realtime` class, a `channels.get(name)` registry,
 * and per-channel `subscribe` / `publish` / `presence` methods.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.ErrorCode = exports.TypedEventEmitter = exports.DEFAULT_REALTIME_ENDPOINT = exports.Connection = exports.Presence = exports.Channel = exports.generateRandomKey = exports.Cipher = exports.Realtime = void 0;
var realtime_js_1 = require("./realtime.js");
Object.defineProperty(exports, "Realtime", { enumerable: true, get: function () { return realtime_js_1.Realtime; } });
var crypto_js_1 = require("./crypto.js");
Object.defineProperty(exports, "Cipher", { enumerable: true, get: function () { return crypto_js_1.Cipher; } });
Object.defineProperty(exports, "generateRandomKey", { enumerable: true, get: function () { return crypto_js_1.generateRandomKey; } });
var channel_js_1 = require("./channel.js");
Object.defineProperty(exports, "Channel", { enumerable: true, get: function () { return channel_js_1.Channel; } });
Object.defineProperty(exports, "Presence", { enumerable: true, get: function () { return channel_js_1.Presence; } });
var connection_js_1 = require("./connection.js");
Object.defineProperty(exports, "Connection", { enumerable: true, get: function () { return connection_js_1.Connection; } });
Object.defineProperty(exports, "DEFAULT_REALTIME_ENDPOINT", { enumerable: true, get: function () { return connection_js_1.DEFAULT_REALTIME_ENDPOINT; } });
Object.defineProperty(exports, "TypedEventEmitter", { enumerable: true, get: function () { return connection_js_1.TypedEventEmitter; } });
var wire_js_1 = require("./wire.js");
Object.defineProperty(exports, "ErrorCode", { enumerable: true, get: function () { return wire_js_1.ErrorCode; } });
//# sourceMappingURL=index.js.map