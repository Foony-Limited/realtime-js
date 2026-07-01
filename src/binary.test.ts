import { describe, expect, it } from 'vitest';

import { decodeServerFrames, encodeClientFrame } from './binary.js';
import type { MessageFrame, PublishFrame } from './wire.js';

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
    // Golden from Go wire.EncodeBinaryPublish (opcode 0x04).
    expect(toHex(encodeClientFrame(frame))).toBe('04010706726f6f6d3a3104636861740e7b226869223a227468657265227d00036d2d310000');
  });

  it('encodes a batch publish (Messages) to the Go format', () => {
    const frame: PublishFrame = {
      t: 'pub', channel: 'room:1', name: '', data: null, messageId: 'm-2', id: 9,
      messages: [
        { name: 'a', data: { i: 1 } },
        { name: 'b', data: 'two', encoding: 'enc' },
      ],
    };
    expect(toHex(encodeClientFrame(frame))).toBe('04000906726f6f6d3a3100046e756c6c00036d2d3200020161077b2269223a317d000162052274776f2203656e63');
  });
});

describe('binary message decoding', () => {
  // Golden bytes from Go for a single message record (opcode 0x0d, count 1). Pins the SDK
  // decoder to the server format cross-language.
  const goldenMsg =
    '540d0101f0d6a183f23306726f6f6d3a310463686174157b226869223a22746865' +
    '7265222c226e223a34327d1c313738323935353134323030303030303030302d3161' +
    '32623363346408636c69656e742d3700b960';

  it('decodes a Go-encoded single message', () => {
    const frames = decodeServerFrames(hexToArrayBuffer(goldenMsg));
    expect(frames).toHaveLength(1);
    const frame = frames[0] as MessageFrame;
    expect(frame.t).toBe('msg');
    expect(frame.channel).toBe('room:1');
    expect(frame.name).toBe('chat');
    expect(frame.data).toEqual({ hi: 'there', n: 42 });
    expect(frame.timestamp).toBe(1782955142000);
    expect(frame.messageId).toBe('1782955142000000000-1a2b3c4d');
    expect(frame.clientId).toBe('client-7');
    expect(frame.seq).toBe(12345);
    expect(frame.ephemeral).toBe(true);
  });

  it('decodes several coalesced records in one message', () => {
    const frames = decodeServerFrames(hexToArrayBuffer(goldenMsg + goldenMsg));
    expect(frames).toHaveLength(2);
    expect((frames[1] as MessageFrame).messageId).toBe('1782955142000000000-1a2b3c4d');
  });

  // Golden bytes for a 2-member record (a bundle), opcode 0x0d, count 2.
  const goldenBundle =
    '3a0d02006406726f6f6d3a310161077b2269223a317d0469642d310263310005' +
    '016506726f6f6d3a310162052274776f220469642d320263320006';

  it('decodes a Go-encoded bundle into a bundle frame', () => {
    const frames = decodeServerFrames(hexToArrayBuffer(goldenBundle));
    expect(frames).toHaveLength(1);
    const frame = frames[0] as MessageFrame;
    expect(frame.channel).toBe('room:1');
    expect(frame.bundle).toHaveLength(2);
    expect(frame.bundle![0]).toMatchObject({ name: 'a', data: { i: 1 }, messageId: 'id-1', clientId: 'c1', seq: 5 });
    expect(frame.bundle![1]).toMatchObject({ name: 'b', data: 'two', messageId: 'id-2', clientId: 'c2', seq: 6, ephemeral: true });
  });

  it('returns whole frames decoded before a malformed tail', () => {
    const frames = decodeServerFrames(hexToArrayBuffer(goldenMsg + 'ff'));
    expect(frames).toHaveLength(1);
  });
});
