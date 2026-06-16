"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.ErrorCode = void 0;
/**
 * Error codes the server uses on `err` frames. Mirrors the Go
 * `internal/wire` constants — keep in sync.
 */
exports.ErrorCode = {
    /** Malformed or unparseable frame. */
    BadFrame: 40001,
    /** Authentication failed (bad token or key). */
    BadAuth: 40101,
    /** Previously valid auth has expired; re-authenticate. */
    AuthExpired: 40102,
    /** Authenticated but not permitted for this channel or action. */
    Forbidden: 40300,
    /** Referenced resource (e.g. channel) does not exist. */
    NotFound: 40400,
    /** Unexpected server-side error. */
    Server: 50000,
};
//# sourceMappingURL=wire.js.map