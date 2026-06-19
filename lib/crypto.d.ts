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
 * style, the same approach Ably uses). For an encrypted message that's
 * `cipher+aes-256-gcm/base64`, and `data` is the base64 of `iv ‖ ciphertext ‖
 * tag`. Decoding unwinds the `/`-separated transforms right-to-left. The edge
 * passes `encoding` through opaquely — only the SDK interprets it.
 */
/** Algorithm label accepted in {@link CipherParams}. The key length picks 128 vs 256. */
export type CipherAlgorithm = 'aes-256-gcm' | 'aes-128-gcm';
/** Parameters for channel encryption. Pass to `channels.get(name, { cipher })`. */
export type CipherParams = {
    /** Secret key: raw bytes (16 or 32) or a base64 string of them. */
    readonly key: Uint8Array | string;
    /** Informational; the actual key length follows the supplied key. */
    readonly algorithm?: CipherAlgorithm;
};
/** The output of {@link Cipher.encrypt}: a transport `encoding` and the encrypted `data`. */
export type EncryptResult = {
    /** Transport encoding describing `data`, e.g. `cipher+aes-256-gcm/base64`. */
    readonly encoding: string;
    /** Base64 of `iv ‖ ciphertext ‖ tag`. */
    readonly data: string;
};
/** True when `encoding` indicates a ciphered payload that needs a {@link Cipher} to read. */
export declare function isCipherEncoding(encoding: string | undefined): encoding is string;
/**
 * Generate a random base64-encoded AES key. Share the returned string between
 * the clients that should be able to read a channel. Never send this to our backend.
 */
export declare function generateRandomKey(bits?: 128 | 256): Promise<string>;
/**
 * AES-GCM cipher for one channel. Encrypts a JSON-serializable value into an
 * `(encoding, data)` pair and back. The key is imported lazily and cached, so
 * construction stays synchronous.
 */
export declare class Cipher {
    private readonly keyBytes;
    private importedKey;
    constructor(params: CipherParams);
    /** Encrypt a JSON-serializable value with a fresh random IV. */
    encrypt(value: unknown): Promise<EncryptResult>;
    /**
     * Decrypt a `data` value carried under `encoding` back to its original value.
     * Throws on a bad key, tampering, or an unsupported encoding.
     */
    decrypt(encoding: string, data: unknown): Promise<unknown>;
    private key;
}
//# sourceMappingURL=crypto.d.ts.map