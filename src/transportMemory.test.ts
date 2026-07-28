/**
 * Tests for the persisted transport memory. The point of this module is that it can only ever
 * save a connection time, never pin one to the slower transport, so these cover the reject
 * paths (missing, malformed, future-dated, unavailable storage) as carefully as the happy one.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { forgetWebSocketFailure, readWebSocketFailureAt, rememberWebSocketFailure } from './transportMemory.js';

const ENDPOINT = 'realtime.example.com';
const OTHER_ENDPOINT = 'realtime.other.com';

/** Minimal in-memory Storage stand-in; `failing` makes every operation throw like a blocked store. */
function fakeStorage(failing = false): Storage {
  const entries = new Map<string, string>();
  const guard = () => {
    if (failing) throw new Error('storage disabled');
  };
  return {
    get length() {
      return entries.size;
    },
    clear: () => {
      guard();
      entries.clear();
    },
    getItem: (key: string) => {
      guard();
      return entries.get(key) ?? null;
    },
    key: (index: number) => [...entries.keys()][index] ?? null,
    removeItem: (key: string) => {
      guard();
      entries.delete(key);
    },
    setItem: (key: string, value: string) => {
      guard();
      entries.set(key, value);
    },
  };
}

function useStorage(store: Storage | undefined): void {
  vi.stubGlobal('localStorage', store);
}

describe('transportMemory', () => {
  beforeEach(() => {
    useStorage(fakeStorage());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('reads back a remembered failure', () => {
    const before = Date.now();
    rememberWebSocketFailure(ENDPOINT);
    const stampedAt = readWebSocketFailureAt(ENDPOINT);
    expect(stampedAt).toBeGreaterThanOrEqual(before);
    expect(stampedAt).toBeLessThanOrEqual(Date.now());
  });

  it('reports nothing when no failure was ever recorded', () => {
    expect(readWebSocketFailureAt(ENDPOINT)).toBe(0);
  });

  it('keeps endpoints separate so one blocked host does not speak for another', () => {
    rememberWebSocketFailure(ENDPOINT);
    expect(readWebSocketFailureAt(ENDPOINT)).toBeGreaterThan(0);
    expect(readWebSocketFailureAt(OTHER_ENDPOINT)).toBe(0);
  });

  it('forgets a failure once a WebSocket connects', () => {
    rememberWebSocketFailure(ENDPOINT);
    forgetWebSocketFailure(ENDPOINT);
    expect(readWebSocketFailureAt(ENDPOINT)).toBe(0);
  });

  it('ignores a future-dated stamp so a backwards clock cannot pin the transport', () => {
    localStorage.setItem(`foony-realtime:ws-failed-at:${ENDPOINT}`, String(Date.now() + 60_000));
    expect(readWebSocketFailureAt(ENDPOINT)).toBe(0);
  });

  it('ignores malformed and non-positive stamps', () => {
    for (const value of ['not-a-number', '', '0', '-1', 'NaN', 'Infinity']) {
      localStorage.setItem(`foony-realtime:ws-failed-at:${ENDPOINT}`, value);
      expect(readWebSocketFailureAt(ENDPOINT), `stamp ${JSON.stringify(value)}`).toBe(0);
    }
  });

  it('degrades to nothing remembered when storage is missing (Node, SSR)', () => {
    useStorage(undefined);
    expect(() => rememberWebSocketFailure(ENDPOINT)).not.toThrow();
    expect(() => forgetWebSocketFailure(ENDPOINT)).not.toThrow();
    expect(readWebSocketFailureAt(ENDPOINT)).toBe(0);
  });

  it('degrades to nothing remembered when storage throws (blocked or full)', () => {
    useStorage(fakeStorage(true));
    expect(() => rememberWebSocketFailure(ENDPOINT)).not.toThrow();
    expect(() => forgetWebSocketFailure(ENDPOINT)).not.toThrow();
    expect(readWebSocketFailureAt(ENDPOINT)).toBe(0);
  });
});
