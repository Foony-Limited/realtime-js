"use strict";
/**
 * Client-side payload encryption for channels — end-to-end in the sense that
 * the realtime edge only ever sees ciphertext; the key is shared between
 * clients out of band and never sent to the server.
 *
 * Uses AES-GCM (authenticated encryption) via WebCrypto, which is available as
 * `globalThis.crypto.subtle` in browsers and Node 20+, so this module needs no
 * imports and runs in both.
 *
 * The payload itself stays in the message's existing `data` field; how to read
 * it is described by a separate `encoding` string (HTTP `Content-Encoding`
 * style). For an encrypted message that's
 * `cipher+aes-256-gcm/base64`, and `data` is the base64 of `iv ‖ ciphertext ‖
 * tag`. Decoding unwinds the `/`-separated transforms right-to-left. The edge
 * passes `encoding` through opaquely — only the SDK interprets it.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Cipher = void 0;
exports.isCipherEncoding = isCipherEncoding;
exports.generateRandomKey = generateRandomKey;
/** Encoding token for an AES-256-GCM ciphered payload. */
const CIPHER_TOKEN_256 = 'cipher+aes-256-gcm';
/** Encoding token for an AES-128-GCM ciphered payload. */
const CIPHER_TOKEN_128 = 'cipher+aes-128-gcm';
/** Encoding token marking the payload as base64 text. */
const BASE64_TOKEN = 'base64';
/** AES-GCM nonce length in bytes (96-bit, the recommended size). */
const IV_BYTES = 12;
/** True when `encoding` indicates a ciphered payload that needs a {@link Cipher} to read. */
function isCipherEncoding(encoding) {
    if (encoding === undefined) {
        return false;
    }
    return encoding.split('/').some((token) => token === CIPHER_TOKEN_256 || token === CIPHER_TOKEN_128);
}
/**
 * Generate a random base64-encoded AES key. Share the returned string between
 * the clients that should be able to read a channel. Never send this to our backend.
 */
async function generateRandomKey(bits = 256) {
    const bytes = new Uint8Array(bits / 8);
    crypto.getRandomValues(bytes);
    return toBase64(bytes);
}
/**
 * AES-GCM cipher for one channel. Encrypts a JSON-serializable value into an
 * `(encoding, data)` pair and back. The key is imported lazily and cached, so
 * construction stays synchronous.
 */
class Cipher {
    keyBytes;
    importedKey = null;
    constructor(params) {
        // Copy into a fresh ArrayBuffer-backed view so WebCrypto's BufferSource types are satisfied.
        this.keyBytes = typeof params.key === 'string' ? fromBase64(params.key) : new Uint8Array(params.key);
        const length = this.keyBytes.byteLength;
        if (length !== 16 && length !== 32) {
            throw new Error(`Cipher: key must be 16 or 32 bytes (AES-128/256), got ${length}`);
        }
    }
    /** Encrypt a JSON-serializable value with a fresh random IV. */
    async encrypt(value) {
        const key = await this.key();
        const iv = new Uint8Array(IV_BYTES);
        crypto.getRandomValues(iv);
        const plaintext = new TextEncoder().encode(JSON.stringify(value ?? null));
        const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, plaintext));
        const blob = new Uint8Array(iv.length + ciphertext.length);
        blob.set(iv, 0);
        blob.set(ciphertext, iv.length);
        const token = this.keyBytes.byteLength === 32 ? CIPHER_TOKEN_256 : CIPHER_TOKEN_128;
        return { encoding: `${token}/${BASE64_TOKEN}`, data: toBase64(blob) };
    }
    /**
     * Decrypt a `data` value carried under `encoding` back to its original value.
     * Throws on a bad key, tampering, or an unsupported encoding.
     */
    async decrypt(encoding, data) {
        if (!isCipherEncoding(encoding)) {
            throw new Error(`Cipher: not a cipher encoding: "${encoding}"`);
        }
        if (typeof data !== 'string') {
            throw new Error('Cipher: encrypted data must be a base64 string');
        }
        const blob = fromBase64(data);
        const iv = blob.subarray(0, IV_BYTES);
        const ciphertext = blob.subarray(IV_BYTES);
        const key = await this.key();
        const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, ciphertext);
        return JSON.parse(new TextDecoder().decode(plaintext));
    }
    key() {
        if (!this.importedKey) {
            this.importedKey = crypto.subtle.importKey('raw', this.keyBytes, 'AES-GCM', false, ['encrypt', 'decrypt']);
        }
        return this.importedKey;
    }
}
exports.Cipher = Cipher;
/** Encode bytes as base64. Isomorphic via the global `btoa`. */
function toBase64(bytes) {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}
/** Decode base64 to bytes. Isomorphic via the global `atob`. */
function fromBase64(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
}
//# sourceMappingURL=crypto.js.map