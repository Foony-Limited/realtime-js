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
/** Capability map: channel pattern → allowed operations. */
export type Capability = Record<string, readonly string[]>;
/** Inputs describing the token to mint. */
export type CreateJwtParams = {
    /**
     * The capability the token grants, as a capability map or a pre-serialized
     * JSON string (e.g. `{ 'chat:site:*': ['subscribe'] }`). Must be a subset of
     * the signing key's own capability or the edge rejects it on connect.
     */
    readonly capability: Capability | string;
    /** Identifier for the end user the token represents; echoed back as `clientId`. */
    readonly clientId: string;
    /** Token lifetime in milliseconds. Defaults to one hour. */
    readonly ttlMs?: number;
};
/** Overrides for {@link Auth.createJwt} / {@link createJwt}. */
export type CreateJwtOptions = {
    /**
     * The API key (`appSlug.publicKeyId:privateKey`) to sign with. Optional on
     * {@link Auth.createJwt} (defaults to the client's configured key); required
     * for the standalone {@link createJwt}.
     */
    readonly key?: string;
};
/**
 * Sign a JWT locally with `options.key`. The token's `kid` header is the
 * public key name (`appSlug.publicKeyId`) so the edge can look up the secret to
 * verify it; the payload carries only the subject, capability, and expiry — no
 * secret. Returns the compact `header.payload.signature` string.
 */
export declare function createJwt(params: CreateJwtParams, options: CreateJwtOptions): Promise<string>;
/**
 * The `auth` namespace on a {@link Realtime} client. Holds the client's API
 * key so `createJwt` can default to it.
 */
export declare class Auth {
    private readonly resolveKey;
    constructor(resolveKey: () => string | undefined);
    /**
     * Mint a short-lived JWT scoped to `params.capability`, signed with the
     * client's API key (override with `options.key`). Local — no network call.
     */
    createJwt(params: CreateJwtParams, options?: CreateJwtOptions): Promise<string>;
}
//# sourceMappingURL=auth.d.ts.map