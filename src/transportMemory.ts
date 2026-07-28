/**
 * Remembers, per endpoint, when a WebSocket attempt last failed at the transport level.
 *
 * Without this the SDK re-learns a blocked network on every page load: each new connection
 * opens a WebSocket, waits out the connect deadline, and only then falls back, so an app that
 * builds several connections leaves the user staring at empty UI for seconds every load. The
 * timestamp is the same signal the in-process WebSocket re-probe already runs on, so persisting
 * it costs no new policy: a connection built while the memory is fresh starts on long-polling,
 * and once the memory ages past the re-probe interval the next attempt tries a WebSocket again.
 * A network that recovers therefore heals on its own, and a client is never parked on
 * long-polling for good.
 *
 * Storage is best-effort. `localStorage` is missing on Node and throws when a browser has
 * storage disabled or is out of quota, and none of that is worth failing a connection over, so
 * every path here degrades to "nothing remembered".
 */

/** Key prefix for the persisted failure stamp. Scoped per endpoint so two apps do not share. */
const STORAGE_KEY_PREFIX = 'foony-realtime:ws-failed-at:';

/**
 * The browser storage the memory lives in, or null when unavailable (Node, SSR, storage
 * disabled). Read through a function rather than cached, because tests swap the global.
 */
function storage(): Storage | null {
  try {
    return globalThis.localStorage ?? null;
  } catch {
    // Accessing localStorage itself throws when cookies/storage are blocked.
    return null;
  }
}

/** Storage key for one endpoint. */
function keyFor(endpoint: string): string {
  return `${STORAGE_KEY_PREFIX}${endpoint}`;
}

/**
 * When a WebSocket attempt to `endpoint` last failed at the transport level, as an epoch
 * milliseconds value, or 0 when nothing is remembered. A stamp in the future (the clock moved
 * backwards, or another tab wrote a bad value) is treated as unknown so it cannot pin the
 * client on long-polling indefinitely.
 */
export function readWebSocketFailureAt(endpoint: string): number {
  const store = storage();
  if (!store) return 0;
  try {
    const raw = store.getItem(keyFor(endpoint));
    if (raw === null) return 0;
    const stampedAt = Number(raw);
    if (!Number.isFinite(stampedAt) || stampedAt <= 0 || stampedAt > Date.now()) return 0;
    return stampedAt;
  } catch {
    return 0;
  }
}

/**
 * Record that a WebSocket attempt to `endpoint` just failed at the transport level, so the next
 * connection this browser builds starts on long-polling instead of paying the connect deadline.
 */
export function rememberWebSocketFailure(endpoint: string): void {
  const store = storage();
  if (!store) return;
  try {
    store.setItem(keyFor(endpoint), String(Date.now()));
  } catch {
    // Quota or a disabled store: the memory is an optimization, so losing it is harmless.
  }
}

/**
 * Forget any remembered failure for `endpoint`, called once a WebSocket connects. This is what
 * makes a network change heal immediately rather than at the next re-probe: a laptop moved off
 * the proxy that blocked upgrades stops starting new connections on long-polling as soon as one
 * WebSocket succeeds.
 */
export function forgetWebSocketFailure(endpoint: string): void {
  const store = storage();
  if (!store) return;
  try {
    store.removeItem(keyFor(endpoint));
  } catch {
    // Same as above: best effort.
  }
}
