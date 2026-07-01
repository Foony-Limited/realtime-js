/**
 * Binary message-frame decoder — the client half of the edge's compact delivery codec. When
 * the SDK advertises `binaryDelivery` on the auth handshake, the edge sends single delivered
 * messages as length-prefixed binary records on the WebSocket binary opcode instead of JSON,
 * which is much cheaper for the edge to produce under heavy fan-out. This decodes them back
 * into {@link MessageFrame} values identical to the JSON path.
 *
 * The layout mirrors Go `wire.EncodeBinaryMessage(s)` (services/realtime-saas/internal/wire): a
 * WebSocket binary message is a sequence of length-prefixed records; each record is
 *   tag(0x02) uvarint(count) member×count
 *   member := flags uvarint(timestamp) lp(channel) lp(name) lp(data)
 *             lp(messageId) lp(clientId) lp(encoding) uvarint(seq)
 * where `lp` is a uvarint length followed by the bytes, `data` is raw JSON, and `seq` is the
 * member's trailing field. There is no separate bundle shape: a normal message is a record of
 * count 1, a server-coalesced bundle a record of count > 1 (surfaced as a `bundle` frame the SDK
 * unwraps and dedups, exactly like the JSON path).
 */

import type { BundledMessage, MessageFrame, PublishFrame } from './wire.js';

const BIN_RECORD_TAG = 0x02;
const BIN_PUBLISH_TAG = 0x01;
const BIN_MSG_FLAG_EPHEMERAL = 1 << 0;

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

/** Append value as a base-128 varint. Values are non-negative and well within 2^53. */
function pushUvarint(out: number[], value: number): void {
  let remaining = value;
  while (remaining >= 0x80) {
    out.push((remaining % 0x80) + 0x80);
    remaining = Math.floor(remaining / 0x80);
  }
  out.push(remaining);
}

/** Append bytes prefixed by their uvarint length. */
function pushLenPrefixed(out: number[], bytes: Uint8Array): void {
  pushUvarint(out, bytes.length);
  for (const byte of bytes) out.push(byte);
}

/** Encode a payload as its raw JSON bytes (empty when undefined), matching the JSON `data` field. */
function jsonBytes(data: unknown): Uint8Array {
  return data === undefined ? new Uint8Array(0) : textEncoder.encode(JSON.stringify(data));
}

/**
 * encodeBinaryPublish encodes a publish frame as one binary publish record, mirroring Go
 * `wire.EncodeBinaryPublish`:
 *   tag(0x01) flags uvarint(id) lp(channel) lp(name) lp(data) lp(encoding) lp(messageId)
 *   uvarint(ttlMs) uvarint(memberCount) [lp(name) lp(data) lp(encoding)]×memberCount
 * A single publish has member count 0 (message in the top-level name/data); a batch has members.
 */
export function encodeBinaryPublish(frame: PublishFrame): Uint8Array {
  const out: number[] = [];
  out.push(BIN_PUBLISH_TAG);
  out.push(frame.ephemeral ? BIN_MSG_FLAG_EPHEMERAL : 0);
  pushUvarint(out, frame.id);
  pushLenPrefixed(out, textEncoder.encode(frame.channel));
  pushLenPrefixed(out, textEncoder.encode(frame.name ?? ''));
  pushLenPrefixed(out, jsonBytes(frame.data));
  pushLenPrefixed(out, textEncoder.encode(frame.encoding ?? ''));
  pushLenPrefixed(out, textEncoder.encode(frame.messageId));
  pushUvarint(out, frame.ttlMs ?? 0);
  const members = frame.messages ?? [];
  pushUvarint(out, members.length);
  for (const member of members) {
    pushLenPrefixed(out, textEncoder.encode(member.name));
    pushLenPrefixed(out, jsonBytes(member.data));
    pushLenPrefixed(out, textEncoder.encode(member.encoding ?? ''));
  }
  return Uint8Array.from(out);
}

/**
 * frameBinaryRecord prefixes a record with its uvarint length so several records concatenate in
 * one WebSocket binary message (and the server splits them the same way). The SDK sends one
 * publish per message, so this wraps a single record.
 */
export function frameBinaryRecord(record: Uint8Array): Uint8Array<ArrayBuffer> {
  const prefix: number[] = [];
  pushUvarint(prefix, record.length);
  const framed = new Uint8Array(prefix.length + record.length);
  framed.set(prefix);
  framed.set(record, prefix.length);
  return framed;
}

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
    const frame = decodeRecord(record);
    if (frame) frames.push(frame);
  }
  return frames;
}

/** One decoded member of a record (before it becomes a single frame or a bundle entry). */
type Member = {
  channel: string;
  name: string;
  data: unknown;
  timestamp: number;
  messageId: string;
  clientId: string;
  encoding: string;
  seq: number;
  ephemeral: boolean;
};

/**
 * decodeRecord decodes one record: a tag, a member count, then that many members. Count 1 becomes
 * a normal message frame; count > 1 becomes a bundle frame (members surfaced as `bundle`, mirroring
 * the JSON bundle the SDK unwraps and dedups). Returns null if malformed.
 */
function decodeRecord(record: Uint8Array): MessageFrame | null {
  try {
    const reader = new Reader(record);
    if (reader.byte() !== BIN_RECORD_TAG) return null;
    const count = reader.uvarint();
    if (count === 1) return memberToFrame(readMember(reader));
    const bundle: BundledMessage[] = [];
    let channel = '';
    for (let i = 0; i < count; i++) {
      const member = readMember(reader);
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

/** readMember reads one member's fields; serial is its trailing field. */
function readMember(reader: Reader): Member {
  const flags = reader.byte();
  const timestamp = reader.uvarint();
  const channel = textDecoder.decode(reader.lenPrefixed());
  const name = textDecoder.decode(reader.lenPrefixed());
  const dataBytes = reader.lenPrefixed();
  const messageId = textDecoder.decode(reader.lenPrefixed());
  const clientId = textDecoder.decode(reader.lenPrefixed());
  const encoding = textDecoder.decode(reader.lenPrefixed());
  const seq = reader.uvarint();
  return {
    channel,
    name,
    data: dataBytes.length > 0 ? JSON.parse(textDecoder.decode(dataBytes)) : undefined,
    timestamp,
    messageId,
    clientId,
    encoding,
    seq,
    ephemeral: (flags & BIN_MSG_FLAG_EPHEMERAL) !== 0,
  };
}

/** memberToFrame builds a single message frame from a decoded member. */
function memberToFrame(member: Member): MessageFrame {
  return {
    t: 'msg',
    channel: member.channel,
    name: member.name,
    data: member.data,
    timestamp: member.timestamp,
    messageId: member.messageId,
    ...(member.clientId ? { clientId: member.clientId } : {}),
    ...(member.encoding ? { encoding: member.encoding } : {}),
    ...(member.seq ? { seq: member.seq } : {}),
    ...(member.ephemeral ? { ephemeral: true } : {}),
  };
}
