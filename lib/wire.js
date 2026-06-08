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
/**
 * Error codes the server uses on `err` frames. Mirrors the Go
 * `internal/wire` constants — keep in sync.
 */
export const ErrorCode = {
    BadFrame: 40001,
    BadAuth: 40101,
    AuthExpired: 40102,
    Forbidden: 40300,
    NotFound: 40400,
    Server: 50000,
};
//# sourceMappingURL=wire.js.map