"use strict";
/**
 * Token minting for trusted callers that hold a Realtime API key.
 *
 * `createJwt` signs a compact HS256 token locally with the key's secret — no
 * network round-trip. A trusted backend mints a
 * short-lived, capability-scoped token for a less-trusted browser client, which
 * returns it from its `authCallback`; the edge verifies the signature against
 * the same key secret on the WebSocket handshake. The key secret never leaves
 * the backend, and the token carries no secret material.
 *
 * Signing uses `globalThis.crypto.subtle` (browsers and Node 20+), so this
 * module is isomorphic and pulls in no Node-only crypto.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Auth = void 0;
exports.createJwt = createJwt;
/** Default token lifetime: one hour. Short enough to bound a leaked token. */
const DEFAULT_JWT_TTL_MS = 60 * 60 * 1_000;
/**
 * Sign a JWT locally with `options.key`. The token's `kid` header is the
 * public key name (`appSlug.publicKeyId`) so the edge can look up the secret to
 * verify it; the payload carries only the subject, capability, and expiry — no
 * secret. Returns the compact `header.payload.signature` string.
 */
async function createJwt(params, options) {
    if (!options.key) {
        throw new Error('createJwt: an API key is required (options.key)');
    }
    const { keyName, secret } = splitApiKey(options.key);
    const capability = typeof params.capability === 'string' ? params.capability : JSON.stringify(params.capability);
    const nowSeconds = Math.floor(Date.now() / 1_000);
    const ttlMs = params.ttlMs ?? DEFAULT_JWT_TTL_MS;
    if (ttlMs <= 0) {
        throw new Error('createJwt: ttlMs must be positive');
    }
    const header = { alg: 'HS256', typ: 'JWT', kid: keyName };
    const payload = {
        sub: params.clientId,
        cap: capability,
        iat: nowSeconds,
        exp: nowSeconds + Math.ceil(ttlMs / 1_000),
    };
    const signingInput = `${base64UrlJson(header)}.${base64UrlJson(payload)}`;
    const signature = await hmacSha256(secret, signingInput);
    return `${signingInput}.${base64UrlBytes(signature)}`;
}
/**
 * The `auth` namespace on a {@link Realtime} client. Holds the client's API
 * key so `createJwt` can default to it.
 */
class Auth {
    resolveKey;
    constructor(resolveKey) {
        this.resolveKey = resolveKey;
    }
    /**
     * Mint a short-lived JWT scoped to `params.capability`, signed with the
     * client's API key (override with `options.key`). Local — no network call.
     */
    async createJwt(params, options) {
        const key = options?.key ?? this.resolveKey();
        if (!key) {
            throw new Error('auth.createJwt: no API key available — construct Realtime with { key } or pass options.key');
        }
        return createJwt(params, { key });
    }
}
exports.Auth = Auth;
/** Split `appSlug.publicKeyId:privateKey` into the public key name and secret. */
function splitApiKey(key) {
    const colon = key.indexOf(':');
    if (colon <= 0 || colon === key.length - 1) {
        throw new Error('createJwt: malformed API key (expected "appSlug.publicKeyId:privateKey")');
    }
    const keyName = key.slice(0, colon);
    const secret = key.slice(colon + 1);
    if (!keyName.includes('.')) {
        throw new Error('createJwt: malformed API key name (expected "appSlug.publicKeyId")');
    }
    return { keyName, secret };
}
/** HMAC-SHA256 of `message` under `secret`, via the isomorphic WebCrypto subtle API. */
async function hmacSha256(secret, message) {
    const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    const signature = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(message));
    return new Uint8Array(signature);
}
/** Serialize `value` to JSON and base64url-encode it (JWT segment encoding). */
function base64UrlJson(value) {
    return base64UrlBytes(new TextEncoder().encode(JSON.stringify(value)));
}
/** Base64url-encode bytes (RFC 7515): base64 with `+/` → `-_` and no padding. */
function base64UrlBytes(bytes) {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
//# sourceMappingURL=auth.js.map