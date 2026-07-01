/**
 * Binary opcode protocol codec — the client half of the edge's wire format. Every frame is a
 * 1-byte opcode then its fields (uvarints and length-prefixed byte slices); this replaces the
 * JSON `t` discriminator. Field orders mirror Go `internal/wire` (binframe.go / binary.go /
 * binmsg.go) exactly — the golden tests pin them.
 *
 * The browser only needs `encodeClientFrame` (frames it sends) and `decodeServerFrames` (frames
 * it receives). The reverse direction (`encodeServerFrame` / `decodeClientFrame`) exists for the
 * in-process fake edge in the tests and tree-shakes out of a browser build.
 */

import type {
  AckFrame,
  AuthFrame,
  BatchMember,
  BundledMessage,
  ClientFrame,
  ConnectedFrame,
  ErrorFrame,
  FetchFrame,
  FetchResponseFrame,
  HistoryFrame,
  HistoryResponseFrame,
  MessageFrame,
  PresenceAction,
  PresenceEventFrame,
  PresenceFrame,
  PublishFrame,
  ServerFrame,
  SubscribeFrame,
} from './wire.js';

/** Frame opcodes — one per frame type, matching Go wire.Op. */
const Op = {
  Auth: 1, Sub: 2, Unsub: 3, Pub: 4, Pres: 5, PresSub: 6, PresUnsub: 7, Hist: 8, Fetch: 9, Ping: 10,
  Connected: 11, Ack: 12, Msg: 13, PresEvt: 14, Err: 15, Pong: 16, HistRes: 17, FetchRes: 18, Batch: 19,
} as const;

const FLAG_EPHEMERAL = 1 << 0;
const FLAG_RESPONSE_SET = 1 << 0;

/** The byte after the auth opcode: a binary connection always coalesces and receives binary
 * delivery (both implied by speaking binary), so instead of flags it carries a protocol version —
 * 0 today, reserved for future connection-wide format changes. Matches Go wire.binAuthVersion. */
const AUTH_PROTOCOL_VERSION = 0;

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

// ---- write helpers ----

function pushUvarint(out: number[], value: number): void {
  let remaining = value;
  while (remaining >= 0x80) {
    out.push((remaining % 0x80) + 0x80);
    remaining = Math.floor(remaining / 0x80);
  }
  out.push(remaining);
}

function pushBytes(out: number[], bytes: Uint8Array): void {
  pushUvarint(out, bytes.length);
  for (const byte of bytes) out.push(byte);
}

function pushString(out: number[], value: string): void {
  pushBytes(out, textEncoder.encode(value));
}

/** Encode a payload as raw JSON bytes (empty when undefined), matching the JSON `data` field. */
function pushJson(out: number[], data: unknown): void {
  pushBytes(out, data === undefined ? new Uint8Array(0) : textEncoder.encode(JSON.stringify(data)));
}

function presenceActionByte(action: PresenceAction): number {
  return action === 'enter' ? 1 : action === 'leave' ? 2 : action === 'update' ? 3 : 0;
}

function presenceActionFrom(value: number): PresenceAction {
  return value === 2 ? 'leave' : value === 3 ? 'update' : 'enter';
}

// ---- read helpers ----

/** A forward cursor over a byte buffer, mirroring Go wire.BinReader. */
class Reader {
  private offset = 0;
  constructor(private readonly bytes: Uint8Array) {}

  get done(): boolean {
    return this.offset >= this.bytes.length;
  }

  byte(): number {
    if (this.offset >= this.bytes.length) throw new Error('binary: truncated');
    return this.bytes[this.offset++]!;
  }

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

  slice(): Uint8Array {
    const length = this.uvarint();
    if (this.offset + length > this.bytes.length) throw new Error('binary: field exceeds buffer');
    const out = this.bytes.subarray(this.offset, this.offset + length);
    this.offset += length;
    return out;
  }

  str(): string {
    return textDecoder.decode(this.slice());
  }

  json(): unknown {
    const bytes = this.slice();
    return bytes.length > 0 ? JSON.parse(textDecoder.decode(bytes)) : undefined;
  }

  /** Read a length-prefixed record (used to split coalesced frames in one WebSocket message). */
  record(): Uint8Array {
    return this.slice();
  }
}

// ---- message members (shared by msg delivery and history/fetch responses) ----

function pushMember(out: number[], message: MessageFrame | BundledMessage, channel: string): void {
  out.push(message.ephemeral ? FLAG_EPHEMERAL : 0);
  pushUvarint(out, message.timestamp);
  pushString(out, channel);
  pushString(out, message.name);
  pushJson(out, message.data);
  pushString(out, message.messageId);
  pushString(out, message.clientId ?? '');
  pushString(out, message.encoding ?? '');
  pushUvarint(out, message.seq ?? 0);
}

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

function readMember(reader: Reader): Member {
  const flags = reader.byte();
  const timestamp = reader.uvarint();
  const channel = reader.str();
  const name = reader.str();
  const data = reader.json();
  const messageId = reader.str();
  const clientId = reader.str();
  const encoding = reader.str();
  const seq = reader.uvarint();
  return { channel, name, data, timestamp, messageId, clientId, encoding, seq, ephemeral: (flags & FLAG_EPHEMERAL) !== 0 };
}

function memberToMessageFrame(member: Member): MessageFrame {
  return {
    t: 'msg', channel: member.channel, name: member.name, data: member.data, timestamp: member.timestamp,
    messageId: member.messageId,
    ...(member.clientId ? { clientId: member.clientId } : {}),
    ...(member.encoding ? { encoding: member.encoding } : {}),
    ...(member.seq ? { seq: member.seq } : {}),
    ...(member.ephemeral ? { ephemeral: true } : {}),
  };
}

function memberToBundled(member: Member): BundledMessage {
  return {
    name: member.name, data: member.data, timestamp: member.timestamp, messageId: member.messageId,
    ...(member.clientId ? { clientId: member.clientId } : {}),
    ...(member.encoding ? { encoding: member.encoding } : {}),
    ...(member.seq ? { seq: member.seq } : {}),
    ...(member.ephemeral ? { ephemeral: true } : {}),
  };
}

// ---- framing ----

/** Prefix a record with its uvarint length so several records concatenate in one WebSocket
 * message (and the server splits them the same way). The SDK sends one frame per message. */
export function frameBinaryRecord(record: Uint8Array): Uint8Array<ArrayBuffer> {
  const prefix: number[] = [];
  pushUvarint(prefix, record.length);
  const framed = new Uint8Array(prefix.length + record.length);
  framed.set(prefix);
  framed.set(record, prefix.length);
  return framed;
}

// ---- client frames: encode (browser -> edge) ----

/** encodeClientFrame encodes any client frame to its binary record (opcode + fields). */
export function encodeClientFrame(frame: ClientFrame): Uint8Array {
  switch (frame.t) {
    case 'auth':
      return encodeAuth(frame);
    case 'sub':
      return encodeSubscribe(frame);
    case 'unsub':
      return chanFrame(Op.Unsub, frame.id, frame.channel);
    case 'pub':
      return encodeBinaryPublish(frame);
    case 'pres':
      return encodePresence(frame);
    case 'presSub':
      return chanFrame(Op.PresSub, frame.id, frame.channel);
    case 'presUnsub':
      return chanFrame(Op.PresUnsub, frame.id, frame.channel);
    case 'hist':
      return encodeHistory(frame);
    case 'fetch':
      return encodeFetch(frame);
    case 'ping':
      return new Uint8Array([Op.Ping]);
  }
}

function encodeAuth(frame: AuthFrame): Uint8Array {
  const out: number[] = [Op.Auth, AUTH_PROTOCOL_VERSION];
  pushString(out, frame.token ?? '');
  pushString(out, frame.key ?? '');
  pushString(out, frame.clientId ?? '');
  pushString(out, frame.resumeConnectionId ?? '');
  return Uint8Array.from(out);
}

function encodeSubscribe(frame: SubscribeFrame): Uint8Array {
  const out: number[] = [Op.Sub];
  pushUvarint(out, frame.id);
  pushUvarint(out, frame.lastSerial ?? 0);
  pushString(out, frame.channel);
  pushString(out, frame.lastMessageId ?? '');
  return Uint8Array.from(out);
}

function chanFrame(op: number, id: number, channel: string): Uint8Array {
  const out: number[] = [op];
  pushUvarint(out, id);
  pushString(out, channel);
  return Uint8Array.from(out);
}

function encodePresence(frame: PresenceFrame): Uint8Array {
  const out: number[] = [Op.Pres, presenceActionByte(frame.action)];
  pushUvarint(out, frame.id);
  pushString(out, frame.channel);
  pushJson(out, frame.data);
  pushString(out, frame.encoding ?? '');
  return Uint8Array.from(out);
}

function encodeHistory(frame: HistoryFrame): Uint8Array {
  const out: number[] = [Op.Hist];
  pushUvarint(out, frame.id);
  pushUvarint(out, frame.limit ?? 0);
  pushString(out, frame.channel);
  pushString(out, frame.start ?? '');
  return Uint8Array.from(out);
}

function encodeFetch(frame: FetchFrame): Uint8Array {
  const out: number[] = [Op.Fetch];
  pushUvarint(out, frame.id);
  pushUvarint(out, frame.fromSerial);
  pushString(out, frame.channel);
  return Uint8Array.from(out);
}

/** encodeBinaryPublish encodes a publish (single or batch) — kept exported for direct use. */
export function encodeBinaryPublish(frame: PublishFrame): Uint8Array {
  const out: number[] = [Op.Pub, frame.ephemeral ? FLAG_EPHEMERAL : 0];
  pushUvarint(out, frame.id);
  pushString(out, frame.channel);
  pushString(out, frame.name ?? '');
  pushJson(out, frame.data);
  pushString(out, frame.encoding ?? '');
  pushString(out, frame.messageId);
  pushUvarint(out, frame.ttlMs ?? 0);
  const members = frame.messages ?? [];
  pushUvarint(out, members.length);
  for (const member of members) {
    pushString(out, member.name);
    pushJson(out, member.data);
    pushString(out, member.encoding ?? '');
  }
  return Uint8Array.from(out);
}

// ---- server frames: decode (edge -> browser) ----

/** decodeServerFrames splits a WebSocket binary message into records and decodes each server
 * frame by its opcode. A malformed tail yields the frames decoded before it. */
export function decodeServerFrames(buffer: ArrayBuffer): ServerFrame[] {
  const frames: ServerFrame[] = [];
  for (const record of splitBinaryRecords(buffer)) {
    const frame = decodeServerFrame(record);
    if (frame) frames.push(frame);
  }
  return frames;
}

/** splitBinaryRecords splits a WebSocket binary message into its length-prefixed records. */
export function splitBinaryRecords(buffer: ArrayBuffer): Uint8Array[] {
  const reader = new Reader(new Uint8Array(buffer));
  const records: Uint8Array[] = [];
  while (!reader.done) {
    try {
      records.push(reader.record());
    } catch {
      break;
    }
  }
  return records;
}

function decodeServerFrame(record: Uint8Array): ServerFrame | null {
  try {
    const reader = new Reader(record);
    switch (reader.byte()) {
      case Op.Connected:
        return { t: 'connected', connectionId: reader.str(), keepAliveMs: reader.uvarint(), clientId: reader.str() };
      case Op.Ack: {
        const flags = reader.byte();
        const id = reader.uvarint();
        const seq = reader.uvarint();
        const frame: AckFrame = { t: 'ack', id, ...(seq ? { seq } : {}), ...(flags & 1 ? { resumed: (flags & 2) !== 0 } : {}) };
        return frame;
      }
      case Op.Msg:
        return decodeMessage(reader);
      case Op.Batch:
        return decodeBatch(reader);
      case Op.PresEvt: {
        const action = presenceActionFrom(reader.byte());
        const timestamp = reader.uvarint();
        const channel = reader.str();
        const clientId = reader.str();
        const connectionId = reader.str();
        const data = reader.json();
        const encoding = reader.str();
        return { t: 'presEvt', action, timestamp, channel, clientId, connectionId, ...(data === undefined ? {} : { data }), ...(encoding ? { encoding } : {}) };
      }
      case Op.Err:
        return { t: 'err', id: reader.uvarint(), code: reader.uvarint(), message: reader.str() };
      case Op.Pong:
        return { t: 'pong' };
      case Op.HistRes: {
        const { id, channel, messages, flag } = decodeResponse(reader);
        return { t: 'histRes', id, channel, messages, ...(flag ? { more: true } : {}) };
      }
      case Op.FetchRes: {
        const { id, channel, messages, flag } = decodeResponse(reader);
        return { t: 'fetchRes', id, channel, messages, resumed: flag };
      }
      default:
        return null;
    }
  } catch {
    return null;
  }
}

/** decodeMessage reads an OpMsg record's members: one is a single message, several a bundle. */
function decodeMessage(reader: Reader): MessageFrame {
  const count = reader.uvarint();
  if (count === 1) return memberToMessageFrame(readMember(reader));
  const bundle: BundledMessage[] = [];
  let channel = '';
  for (let index = 0; index < count; index++) {
    const member = readMember(reader);
    channel = member.channel;
    bundle.push(memberToBundled(member));
  }
  return { t: 'msg', channel, name: '', data: undefined, timestamp: 0, messageId: '', bundle };
}

/** decodeBatch reads an OpBatch record: a shared header then its members, into a msg frame whose
 * `messages` the channel expands into individual messages. */
function decodeBatch(reader: Reader): MessageFrame {
  const flags = reader.byte();
  const timestamp = reader.uvarint();
  const channel = reader.str();
  const messageId = reader.str();
  const clientId = reader.str();
  const seq = reader.uvarint();
  const count = reader.uvarint();
  const messages: BatchMember[] = [];
  for (let index = 0; index < count; index++) {
    const name = reader.str();
    const data = reader.json();
    const encoding = reader.str();
    messages.push({ name, data, ...(encoding ? { encoding } : {}) });
  }
  return {
    t: 'msg', channel, name: '', data: undefined, timestamp, messageId,
    ...(clientId ? { clientId } : {}),
    ...(seq ? { seq } : {}),
    ...(flags & FLAG_EPHEMERAL ? { ephemeral: true } : {}),
    messages,
  };
}

function decodeResponse(reader: Reader): { id: number; channel: string; messages: MessageFrame[]; flag: boolean } {
  const flag = (reader.byte() & FLAG_RESPONSE_SET) !== 0;
  const id = reader.uvarint();
  const channel = reader.str();
  const count = reader.uvarint();
  const messages: MessageFrame[] = [];
  for (let index = 0; index < count; index++) messages.push(memberToMessageFrame(readMember(reader)));
  return { id, channel, messages, flag };
}

// ---- reverse direction (fake edge in tests only; tree-shaken from browser builds) ----

/** encodeServerFrame encodes a server frame for the test fake edge. */
export function encodeServerFrame(frame: ServerFrame): Uint8Array {
  switch (frame.t) {
    case 'connected': {
      const out: number[] = [Op.Connected];
      pushString(out, frame.connectionId);
      pushUvarint(out, frame.keepAliveMs);
      pushString(out, frame.clientId);
      return Uint8Array.from(out);
    }
    case 'ack': {
      const flags = frame.resumed === undefined ? 0 : 1 | (frame.resumed ? 2 : 0);
      const out: number[] = [Op.Ack, flags];
      pushUvarint(out, frame.id);
      pushUvarint(out, frame.seq ?? 0);
      return Uint8Array.from(out);
    }
    case 'msg':
      return encodeMessage(frame);
    case 'presEvt': {
      const out: number[] = [Op.PresEvt, presenceActionByte(frame.action)];
      pushUvarint(out, frame.timestamp);
      pushString(out, frame.channel);
      pushString(out, frame.clientId);
      pushString(out, frame.connectionId);
      pushJson(out, frame.data);
      pushString(out, frame.encoding ?? '');
      return Uint8Array.from(out);
    }
    case 'err': {
      const out: number[] = [Op.Err];
      pushUvarint(out, frame.id ?? 0);
      pushUvarint(out, frame.code);
      pushString(out, frame.message);
      return Uint8Array.from(out);
    }
    case 'pong':
      return new Uint8Array([Op.Pong]);
    case 'histRes':
      return encodeResponse(Op.HistRes, frame.id, frame.channel, frame.messages, frame.more ?? false);
    case 'fetchRes':
      return encodeResponse(Op.FetchRes, frame.id, frame.channel, frame.messages, frame.resumed);
  }
}

function encodeMessage(frame: MessageFrame): Uint8Array {
  if (frame.messages && frame.messages.length > 0) {
    return encodeBatch(frame);
  }
  const out: number[] = [Op.Msg];
  if (frame.bundle && frame.bundle.length > 0) {
    pushUvarint(out, frame.bundle.length);
    for (const member of frame.bundle) pushMember(out, member, frame.channel);
  } else {
    pushUvarint(out, 1);
    pushMember(out, frame, frame.channel);
  }
  return Uint8Array.from(out);
}

/** encodeBatch encodes a batch msg frame (`messages` set) as an OpBatch record: shared header
 * then its members. Mirrors Go wire.EncodeBinaryBatch. */
function encodeBatch(frame: MessageFrame): Uint8Array {
  const members = frame.messages ?? [];
  const out: number[] = [Op.Batch, frame.ephemeral ? FLAG_EPHEMERAL : 0];
  pushUvarint(out, frame.timestamp);
  pushString(out, frame.channel);
  pushString(out, frame.messageId);
  pushString(out, frame.clientId ?? '');
  pushUvarint(out, frame.seq ?? 0);
  pushUvarint(out, members.length);
  for (const member of members) {
    pushString(out, member.name);
    pushJson(out, member.data);
    pushString(out, member.encoding ?? '');
  }
  return Uint8Array.from(out);
}

function encodeResponse(op: number, id: number, channel: string, messages: readonly MessageFrame[], flag: boolean): Uint8Array {
  const out: number[] = [op, flag ? FLAG_RESPONSE_SET : 0];
  pushUvarint(out, id);
  pushString(out, channel);
  pushUvarint(out, messages.length);
  for (const message of messages) pushMember(out, message, channel);
  return Uint8Array.from(out);
}

/** decodeClientFrame decodes a client frame for the test fake edge. */
export function decodeClientFrame(record: Uint8Array): ClientFrame {
  const reader = new Reader(record);
  switch (reader.byte()) {
    case Op.Auth: {
      reader.byte(); // protocol version (AUTH_PROTOCOL_VERSION); 0 today, reserved
      const token = reader.str();
      const key = reader.str();
      const clientId = reader.str();
      const resumeConnectionId = reader.str();
      return {
        t: 'auth', ...(token ? { token } : {}), ...(key ? { key } : {}), ...(clientId ? { clientId } : {}),
        ...(resumeConnectionId ? { resumeConnectionId } : {}),
      };
    }
    case Op.Sub: {
      const id = reader.uvarint();
      const lastSerial = reader.uvarint();
      const channel = reader.str();
      const lastMessageId = reader.str();
      return { t: 'sub', id, channel, ...(lastSerial ? { lastSerial } : {}), ...(lastMessageId ? { lastMessageId } : {}) };
    }
    case Op.Unsub:
      return { t: 'unsub', id: reader.uvarint(), channel: reader.str() };
    case Op.Pub:
      return decodePublish(reader);
    case Op.Pres: {
      const action = presenceActionFrom(reader.byte());
      const id = reader.uvarint();
      const channel = reader.str();
      const data = reader.json();
      const encoding = reader.str();
      return { t: 'pres', action, id, channel, ...(data === undefined ? {} : { data }), ...(encoding ? { encoding } : {}) };
    }
    case Op.PresSub:
      return { t: 'presSub', id: reader.uvarint(), channel: reader.str() };
    case Op.PresUnsub:
      return { t: 'presUnsub', id: reader.uvarint(), channel: reader.str() };
    case Op.Hist: {
      const id = reader.uvarint();
      const limit = reader.uvarint();
      const channel = reader.str();
      const start = reader.str();
      return { t: 'hist', id, channel, ...(limit ? { limit } : {}), ...(start ? { start } : {}) };
    }
    case Op.Fetch:
      return { t: 'fetch', id: reader.uvarint(), fromSerial: reader.uvarint(), channel: reader.str() };
    case Op.Ping:
      return { t: 'ping' };
    default:
      throw new Error(`decodeClientFrame: unknown opcode ${record[0]}`);
  }
}

function decodePublish(reader: Reader): PublishFrame {
  const flags = reader.byte();
  const id = reader.uvarint();
  const channel = reader.str();
  const name = reader.str();
  const data = reader.json();
  const encoding = reader.str();
  const messageId = reader.str();
  const ttlMs = reader.uvarint();
  const memberCount = reader.uvarint();
  const messages: BatchMember[] = [];
  for (let index = 0; index < memberCount; index++) {
    const memberName = reader.str();
    const memberData = reader.json();
    const memberEncoding = reader.str();
    messages.push({ name: memberName, data: memberData, ...(memberEncoding ? { encoding: memberEncoding } : {}) });
  }
  return {
    t: 'pub', channel, name, data, id, messageId,
    ...(encoding ? { encoding } : {}), ...(ttlMs ? { ttlMs } : {}),
    ...(flags & FLAG_EPHEMERAL ? { ephemeral: true } : {}), ...(messages.length ? { messages } : {}),
  };
}
