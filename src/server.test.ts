import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';

import { mintRealtimeToken } from './server.js';

/** Decode a base64url string into a UTF-8 string. */
function decodeSegment(segment: string): string {
  return Buffer.from(segment, 'base64url').toString('utf8');
}

describe('mintRealtimeToken', () => {
  const signingKey = 'integration-test-signing-key';

  it('produces a compact-encoded JWT with three segments', () => {
    const token = mintRealtimeToken({ signingKey, appId: 'app-1', clientId: 'alice' });
    const parts = token.split('.');
    expect(parts).toHaveLength(3);
  });

  it('encodes the documented claims', () => {
    const token = mintRealtimeToken({
      signingKey,
      appId: 'app-1',
      clientId: 'alice',
      capability: '{"chat:*":["subscribe"]}',
      ttlMs: 60_000,
    });
    const segments = token.split('.');
    const headerSegment = segments[0];
    const payloadSegment = segments[1];
    if (!headerSegment || !payloadSegment) throw new Error('malformed token');
    const header = JSON.parse(decodeSegment(headerSegment));
    const payload = JSON.parse(decodeSegment(payloadSegment));
    expect(header).toEqual({ alg: 'HS256', typ: 'JWT' });
    expect(payload.sub).toBe('alice');
    expect(payload.app).toBe('app-1');
    expect(payload.cap).toBe('{"chat:*":["subscribe"]}');
    expect(typeof payload.iat).toBe('number');
    expect(typeof payload.exp).toBe('number');
    expect(payload.exp - payload.iat).toBeGreaterThanOrEqual(59);
    expect(payload.exp - payload.iat).toBeLessThanOrEqual(60);
  });

  it('produces a signature that verifies with the same key', () => {
    const token = mintRealtimeToken({ signingKey, appId: 'app-1', clientId: 'alice' });
    const lastDotIndex = token.lastIndexOf('.');
    const signingInput = token.slice(0, lastDotIndex);
    const signature = token.slice(lastDotIndex + 1);
    const expected = createHmac('sha256', signingKey).update(signingInput).digest('base64url');
    expect(signature).toBe(expected);
  });

  it('defaults capability to the full wildcard', () => {
    const token = mintRealtimeToken({ signingKey, appId: 'a', clientId: 'c' });
    const payloadSegment = token.split('.')[1];
    if (!payloadSegment) throw new Error('malformed token');
    const payload = JSON.parse(decodeSegment(payloadSegment));
    expect(payload.cap).toBe('{"*":["*"]}');
  });

  it('rejects non-positive ttlMs', () => {
    expect(() => mintRealtimeToken({ signingKey, appId: 'a', clientId: 'c', ttlMs: 0 })).toThrow();
    expect(() => mintRealtimeToken({ signingKey, appId: 'a', clientId: 'c', ttlMs: -1 })).toThrow();
  });
});
