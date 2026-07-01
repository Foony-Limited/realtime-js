/**
 * Binary message-frame decoder — the client half of the edge's compact delivery codec. When
 * the SDK advertises `binaryDelivery` on the auth handshake, the edge sends single delivered
 * messages as length-prefixed binary records on the WebSocket binary opcode instead of JSON,
 * which is much cheaper for the edge to produce under heavy fan-out. This decodes them back
 * into {@link MessageFrame} values identical to the JSON path.
 *
 * The layout mirrors Go `wire.EncodeBinaryMessage` / `wire.AppendBinaryRecord`
 * (services/realtime-saas/internal/wire): a WebSocket binary message is a sequence of records,
 * each a uvarint length followed by that many bytes; each record is
 *   tag(0x02) flags uvarint(timestamp)
 *   lp(channel) lp(name) lp(data) lp(messageId) lp(clientId) lp(encoding) uvarint(seq)
 * where `lp` is a uvarint length followed by the bytes, `data` is raw JSON, and `seq` is the
 * trailing field (the server appends it after building the body).
 */

import type { BundledMessage, MessageFrame } from './wire.js';

const BIN_MESSAGE_TAG = 0x02;
const BIN_BUNDLE_TAG = 0x03;
const BIN_MSG_FLAG_EPHEMERAL = 1 << 0;

const textDecoder = new TextDecoder();

/** A forward cursor over a byte buffer, mirroring Go's wire.BinReader. */
class Reader {
  private offset = 0;
  constructor(private readonly bytes: Uint8Array) {}

  get done(): boolean {
    return this.offset >= this.bytes.length;
  }

  /** Read a base-128 varint. Throws on a truncated one. */
  uvarint(): number {
    let result = 0;
    let shift = 0;
    for (;;) {
      if (this.offset >= this.bytes.length) throw new Error('binary: truncated varint');
      const byte = this.bytes[this.offset++]!;
      result += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) return result;
      shift += 7;
    }
  }

  /** Read a length-prefixed byte slice (a view over the buffer, no copy). */
  lenPrefixed(): Uint8Array {
    const length = this.uvarint();
    if (this.offset + length > this.bytes.length) throw new Error('binary: field exceeds buffer');
    const slice = this.bytes.subarray(this.offset, this.offset + length);
    this.offset += length;
    return slice;
  }

  /** Read a length-prefixed record (used to split a coalesced message). */
  record(): Uint8Array {
    return this.lenPrefixed();
  }

  byte(): number {
    if (this.offset >= this.bytes.length) throw new Error('binary: truncated');
    return this.bytes[this.offset++]!;
  }
}

/**
 * decodeBinaryMessages splits a WebSocket binary message into its length-prefixed records and
 * decodes each into a MessageFrame. A malformed buffer yields whatever whole frames were
 * decoded before the error, so one bad record does not drop a whole batch.
 */
export function decodeBinaryMessages(buffer: ArrayBuffer): MessageFrame[] {
  const outer = new Reader(new Uint8Array(buffer));
  const frames: MessageFrame[] = [];
  while (!outer.done) {
    let record: Uint8Array;
    try {
      record = outer.record();
    } catch {
      break;
    }
    const frame = record.length > 0 && record[0] === BIN_BUNDLE_TAG ? decodeBundle(record) : decodeRecord(record);
    if (frame) frames.push(frame);
  }
  return frames;
}

/**
 * decodeBundle decodes a bundle record — a tag, a count, then that many length-prefixed member
 * message records — into one MessageFrame carrying `bundle`, mirroring the JSON bundle frame the
 * SDK already unwraps and dedups. Returns null if malformed.
 */
function decodeBundle(record: Uint8Array): MessageFrame | null {
  try {
    const reader = new Reader(record);
    if (reader.byte() !== BIN_BUNDLE_TAG) return null;
    const count = reader.uvarint();
    const bundle: BundledMessage[] = [];
    let channel = '';
    for (let i = 0; i < count; i++) {
      const member = decodeRecord(reader.lenPrefixed());
      if (!member) return null;
      channel = member.channel;
      bundle.push({
        name: member.name,
        data: member.data,
        timestamp: member.timestamp,
        messageId: member.messageId,
        ...(member.clientId ? { clientId: member.clientId } : {}),
        ...(member.encoding ? { encoding: member.encoding } : {}),
        ...(member.seq ? { seq: member.seq } : {}),
        ...(member.ephemeral ? { ephemeral: true } : {}),
      });
    }
    // A bundle frame's own name/data/timestamp/messageId are unused (the members carry them);
    // fill zero values to match the JSON bundle frame shape the SDK already unwraps.
    return { t: 'msg', channel, name: '', data: undefined, timestamp: 0, messageId: '', bundle };
  } catch {
    return null;
  }
}

/** decodeRecord decodes one binary message record, or null if it is malformed. */
function decodeRecord(record: Uint8Array): MessageFrame | null {
  try {
    const reader = new Reader(record);
    if (reader.byte() !== BIN_MESSAGE_TAG) return null;
    const flags = reader.byte();
    const timestamp = reader.uvarint();
    const channel = textDecoder.decode(reader.lenPrefixed());
    const name = textDecoder.decode(reader.lenPrefixed());
    const dataBytes = reader.lenPrefixed();
    const messageId = textDecoder.decode(reader.lenPrefixed());
    const clientId = textDecoder.decode(reader.lenPrefixed());
    const encoding = textDecoder.decode(reader.lenPrefixed());
    // Serial is the trailing field: a durable message's serial is appended by the server
    // after the body is built, so the decoder reads it last.
    const seq = reader.uvarint();
    const frame: MessageFrame = {
      t: 'msg',
      channel,
      name,
      data: dataBytes.length > 0 ? JSON.parse(textDecoder.decode(dataBytes)) : undefined,
      timestamp,
      messageId,
      ...(clientId ? { clientId } : {}),
      ...(encoding ? { encoding } : {}),
      ...(seq ? { seq } : {}),
      ...(flags & BIN_MSG_FLAG_EPHEMERAL ? { ephemeral: true } : {}),
    };
    return frame;
  } catch {
    return null;
  }
}
