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
/** Options to mint a Foony Realtime token. */
export type MintRealtimeTokenOptions = {
    /**
     * The HS256 signing key — must exactly match the edge binary's
     * `JWT_SIGNING_KEY` env var. 32 bytes recommended for production.
     */
    readonly signingKey: string | Uint8Array;
    /** App id the token is scoped to. Encoded in the JWT `app` claim. */
    readonly appId: string;
    /** Customer-controlled identifier for the end user (the JWT `sub`). */
    readonly clientId: string;
    /**
     * Capability JSON string scoping the token (e.g. `{"chat:*":["subscribe","publish"]}`).
     * Defaults to `{"*":["*"]}` (full wildcard). The MVP edge does not
     * enforce capabilities yet, but tokens minted today will be checked
     * once enforcement lands.
     */
    readonly capability?: string;
    /**
     * Token TTL. Defaults to 1 hour. Must be > 0; long-lived tokens are
     * an antipattern — the SDK calls `authCallback` on every reconnect
     * so 5-15 minutes is usually plenty.
     */
    readonly ttlMs?: number;
};
/**
 * Returns a compact-encoded HS256 JWT carrying the supplied claims.
 *
 * The token shape matches what `services/realtime-saas/internal/auth`
 * mints with `MintForDev` plus a configurable capability and TTL.
 */
export declare function mintRealtimeToken(options: MintRealtimeTokenOptions): string;
//# sourceMappingURL=server.d.ts.map