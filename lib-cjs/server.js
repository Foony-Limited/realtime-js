"use strict";
/**
 * Server-side helpers for the Foony Realtime SDK.
 *
 * `mintRealtimeToken` produces the same HS256 JWT layout the Go edge
 * binary verifies (see `services/realtime-saas/internal/auth/jwt.go`).
 * Foony's application server uses this to hand short-lived tokens to
 * its own clients — the client passes the result into the SDK's
 * `authCallback`.
 *
 * Node-only: relies on `node:crypto` for HMAC-SHA256. Browsers should
 * never see this module.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.mintRealtimeToken = mintRealtimeToken;
const node_crypto_1 = require("node:crypto");
const DEFAULT_TTL_MS = 60 * 60 * 1_000;
const DEFAULT_CAPABILITY = '{"*":["*"]}';
/**
 * Returns a compact-encoded HS256 JWT carrying the supplied claims.
 *
 * The token shape matches what `services/realtime-saas/internal/auth`
 * mints with `MintForDev` plus a configurable capability and TTL.
 */
function mintRealtimeToken(options) {
    const ttlMs = options.ttlMs ?? DEFAULT_TTL_MS;
    if (ttlMs <= 0) {
        throw new Error('mintRealtimeToken: ttlMs must be > 0');
    }
    const issuedAtSec = Math.floor(Date.now() / 1_000);
    const expiresAtSec = issuedAtSec + Math.floor(ttlMs / 1_000);
    const payload = {
        sub: options.clientId,
        app: options.appId,
        cap: options.capability ?? DEFAULT_CAPABILITY,
        iat: issuedAtSec,
        exp: expiresAtSec,
    };
    const header = { alg: 'HS256', typ: 'JWT' };
    const signingInput = `${encodeSegment(header)}.${encodeSegment(payload)}`;
    const key = typeof options.signingKey === 'string' ? Buffer.from(options.signingKey, 'utf8') : Buffer.from(options.signingKey);
    const signature = (0, node_crypto_1.createHmac)('sha256', key).update(signingInput).digest();
    return `${signingInput}.${base64UrlEncode(signature)}`;
}
/**
 * Encode a JSON value as a base64url segment. JWT spec requires no
 * padding and the URL-safe alphabet — `Buffer.toString('base64url')`
 * is exactly that, available in Node 16+.
 */
function encodeSegment(value) {
    return base64UrlEncode(Buffer.from(JSON.stringify(value), 'utf8'));
}
function base64UrlEncode(buffer) {
    return buffer.toString('base64url');
}
//# sourceMappingURL=server.js.map