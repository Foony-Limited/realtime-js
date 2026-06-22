/** Unit tests for local JWT minting. Verifies the HMAC against Node's WebCrypto. */

import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { Auth, createJwt } from './auth.js';

const KEY = 'foony.kid_test:sk_super_secret_value';
const SECRET = 'sk_super_secret_value';

/** Decode a base64url JWT segment to its parsed JSON. */
function decodeSegment(segment: string): Record<string, unknown> {
  const base64 = segment.replace(/-/g, '+').replace(/_/g, '/');
  return JSON.parse(Buffer.from(base64, 'base64').toString('utf8')) as Record<string, unknown>;
}

describe('createJwt', () => {
  it('signs an HS256 token whose HMAC verifies against the key secret', async () => {
    const token = await createJwt({ capability: { 'chat:site:*': ['subscribe'] }, clientId: 'alice', ttlMs: 3_600_000 }, { key: KEY });
    const [headerSegment, payloadSegment, signatureSegment] = token.split('.');
    expect(headerSegment && payloadSegment && signatureSegment).toBeTruthy();

    const expectedMac = createHmac('sha256', SECRET)
      .update(`${headerSegment}.${payloadSegment}`)
      .digest('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    expect(signatureSegment).toBe(expectedMac);
  });

  it('puts the public key name in the kid header and no secret in the token', async () => {
    const token = await createJwt({ capability: { 'chat:dm:alice:*': ['subscribe'] }, clientId: 'alice' }, { key: KEY });
    const [headerSegment, payloadSegment] = token.split('.');
    const header = decodeSegment(headerSegment!);
    const payload = decodeSegment(payloadSegment!);

    expect(header).toMatchObject({ alg: 'HS256', typ: 'JWT', kid: 'foony.kid_test' });
    expect(payload['sub']).toBe('alice');
    expect(payload['cap']).toBe('{"chat:dm:alice:*":["subscribe"]}');
    expect(token).not.toContain(SECRET);
    expect(JSON.stringify(payload)).not.toContain(SECRET);
  });

  it('sets exp from ttl', async () => {
    const token = await createJwt({ capability: '{"*":["subscribe"]}', clientId: 'bob', ttlMs: 60_000 }, { key: KEY });
    const payload = decodeSegment(token.split('.')[1]!);
    const iat = payload['iat'] as number;
    const exp = payload['exp'] as number;
    expect(exp - iat).toBe(60);
  });

  it('accepts a pre-serialized capability string unchanged', async () => {
    const token = await createJwt({ capability: '{"chat:site:games":["subscribe"]}', clientId: 'c' }, { key: KEY });
    const payload = decodeSegment(token.split('.')[1]!);
    expect(payload['cap']).toBe('{"chat:site:games":["subscribe"]}');
  });

  it('rejects a malformed key', async () => {
    await expect(createJwt({ capability: { '*': ['subscribe'] }, clientId: 'a' }, { key: 'no-colon' })).rejects.toThrow(/malformed/);
    await expect(createJwt({ capability: { '*': ['subscribe'] }, clientId: 'a' }, { key: 'nodot:secret' })).rejects.toThrow(/malformed/);
  });
});

describe('Auth.createJwt', () => {
  it('signs with the client-configured key by default', async () => {
    const auth = new Auth(() => KEY);
    const token = await auth.createJwt({ capability: { '*': ['subscribe'] }, clientId: 'alice' });
    expect(decodeSegment(token.split('.')[0]!)['kid']).toBe('foony.kid_test');
  });

  it('throws when no key is available', async () => {
    const auth = new Auth(() => undefined);
    await expect(auth.createJwt({ capability: { '*': ['subscribe'] }, clientId: 'a' })).rejects.toThrow(/no API key/);
  });
});
