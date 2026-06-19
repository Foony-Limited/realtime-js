/** Unit tests for the AES-GCM channel cipher. Runs against Node's WebCrypto. */

import { describe, expect, it } from 'vitest';
import { Cipher, generateRandomKey, isCipherEncoding } from './crypto.js';

describe('Cipher', () => {
  it('round-trips a JSON value', async () => {
    const cipher = new Cipher({ key: await generateRandomKey() });
    const value = { text: 'hello', n: 42, nested: { ok: true }, list: [1, 2, 3] };
    const { encoding, data } = await cipher.encrypt(value);
    expect(encoding).toBe('cipher+aes-256-gcm/base64');
    expect(typeof data).toBe('string');
    expect(await cipher.decrypt(encoding, data)).toEqual(value);
  });

  it('produces a fresh IV (and thus different ciphertext) per encryption', async () => {
    const cipher = new Cipher({ key: await generateRandomKey() });
    const a = await cipher.encrypt('same');
    const b = await cipher.encrypt('same');
    expect(a.data).not.toBe(b.data);
  });

  it('fails to decrypt with the wrong key', async () => {
    const { encoding, data } = await new Cipher({ key: await generateRandomKey() }).encrypt({ secret: 1 });
    const other = new Cipher({ key: await generateRandomKey() });
    await expect(other.decrypt(encoding, data)).rejects.toBeDefined();
  });

  it('interoperates across cipher instances sharing a base64 key', async () => {
    const key = await generateRandomKey();
    const { encoding, data } = await new Cipher({ key }).encrypt('shared');
    expect(await new Cipher({ key }).decrypt(encoding, data)).toBe('shared');
  });

  it('supports AES-128 keys', async () => {
    const cipher = new Cipher({ key: await generateRandomKey(128) });
    const { encoding, data } = await cipher.encrypt('x');
    expect(encoding).toBe('cipher+aes-128-gcm/base64');
    expect(await cipher.decrypt(encoding, data)).toBe('x');
  });

  it('rejects keys that are not 16 or 32 bytes', () => {
    expect(() => new Cipher({ key: new Uint8Array(20) })).toThrow(/16 or 32 bytes/);
  });

  it('isCipherEncoding recognizes cipher encodings only', () => {
    expect(isCipherEncoding('cipher+aes-256-gcm/base64')).toBe(true);
    expect(isCipherEncoding('cipher+aes-128-gcm/base64')).toBe(true);
    expect(isCipherEncoding('base64')).toBe(false);
    expect(isCipherEncoding('json/utf-8')).toBe(false);
    expect(isCipherEncoding(undefined)).toBe(false);
  });
});
