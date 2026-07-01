import { describe, expect, it } from 'vitest';

import { decodeBinaryMessages, encodeBinaryPublish } from './binary.js';
import type { PublishFrame } from './wire.js';

/** Turn a hex string into an ArrayBuffer (as a WebSocket binary message arrives). */
function hexToArrayBuffer(hex: string): ArrayBuffer {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes.buffer;
}

/** Hex-encode bytes for comparison against a Go golden. */
function toHex(bytes: Uint8Array): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

describe('binary publish encoding', () => {
  it('encodes a single publish to the Go format', () => {
    const frame: PublishFrame = { t: 'pub', channel: 'room:1', name: 'chat', data: { hi: 'there' }, ephemeral: true, messageId: 'm-1', id: 7 };
    // Golden from Go wire.EncodeBinaryPublish.
    expect(toHex(encodeBinaryPublish(frame))).toBe('01010706726f6f6d3a3104636861740e7b226869223a227468657265227d00036d2d310000');
  });

  it('encodes a batch publish (Messages) to the Go format', () => {
    const frame: PublishFrame = {
      t: 'pub', channel: 'room:1', name: '', data: null, messageId: 'm-2', id: 9,
      messages: [
        { name: 'a', data: { i: 1 } },
        { name: 'b', data: 'two', encoding: 'enc' },
      ],
    };
    expect(toHex(encodeBinaryPublish(frame))).toBe('01000906726f6f6d3a3100046e756c6c00036d2d3200020161077b2269223a317d000162052274776f2203656e63');
  });
});

describe('binary message decoding', () => {
  // Golden bytes produced by the Go encoder (wire.AppendBinaryRecord(wire.EncodeBinaryMessage))
  // for a known message — a count-1 record. Pins the SDK decoder to the actual server wire format
  // so the two cannot drift silently across languages.
  const golden =
    '54020101f0d6a183f23306726f6f6d3a310463686174157b226869223a22746865' +
    '7265222c226e223a34327d1c313738323935353134323030303030303030302d3161' +
    '32623363346408636c69656e742d3700b960';

  it('decodes a Go-encoded binary message frame', () => {
    const frames = decodeBinaryMessages(hexToArrayBuffer(golden));
    expect(frames).toHaveLength(1);
    const frame = frames[0]!;
    expect(frame.t).toBe('msg');
    expect(frame.channel).toBe('room:1');
    expect(frame.name).toBe('chat');
    expect(frame.data).toEqual({ hi: 'there', n: 42 });
    expect(frame.timestamp).toBe(1782955142000);
    expect(frame.messageId).toBe('1782955142000000000-1a2b3c4d');
    expect(frame.clientId).toBe('client-7');
    expect(frame.seq).toBe(12345);
    expect(frame.ephemeral).toBe(true);
    expect(frame.encoding).toBeUndefined();
  });

  it('decodes several coalesced records in one message', () => {
    // Two copies of the golden record back to back = a coalesced binary message.
    const frames = decodeBinaryMessages(hexToArrayBuffer(golden + golden));
    expect(frames).toHaveLength(2);
    expect(frames[0]!.channel).toBe('room:1');
    expect(frames[1]!.messageId).toBe('1782955142000000000-1a2b3c4d');
  });

  // Golden bytes from Go for a 2-member record (a bundle) on "room:1" — same format as a single,
  // count 2. Pins the multi-member decode to the server format.
  const goldenBundle =
    '3a0202006406726f6f6d3a310161077b2269223a317d0469642d310263310005' +
    '016506726f6f6d3a310162052274776f220469642d320263320006';

  it('decodes a Go-encoded binary bundle into a bundle frame', () => {
    const frames = decodeBinaryMessages(hexToArrayBuffer(goldenBundle));
    expect(frames).toHaveLength(1);
    const frame = frames[0]!;
    expect(frame.channel).toBe('room:1');
    expect(frame.bundle).toBeDefined();
    expect(frame.bundle).toHaveLength(2);
    expect(frame.bundle![0]).toMatchObject({ name: 'a', data: { i: 1 }, messageId: 'id-1', clientId: 'c1', seq: 5 });
    expect(frame.bundle![1]).toMatchObject({ name: 'b', data: 'two', messageId: 'id-2', clientId: 'c2', seq: 6, ephemeral: true });
  });

  it('returns whole frames decoded before a malformed tail', () => {
    // Valid record followed by a truncated length prefix.
    const frames = decodeBinaryMessages(hexToArrayBuffer(golden + 'ff'));
    expect(frames).toHaveLength(1);
  });
});
