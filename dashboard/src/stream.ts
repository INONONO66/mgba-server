const HEADER_SIZE = 34;
const MAGIC = 0x50534d47;
const STREAM_FORMAT = 2;
const FRAME_KEYFRAME = 1;
const FRAME_DELTA = 2;
const FLAG_DEFLATE_RAW = 1;

export type StreamFrameKind = "keyframe" | "delta";

export type StreamFrame = {
  readonly kind: StreamFrameKind;
  readonly instanceIndex: number;
  readonly sequence: number;
  readonly timestampMs: number;
  readonly width: number;
  readonly height: number;
  readonly tileSize: number;
  readonly rawBytes: number;
  readonly payload: Uint8Array;
  readonly metadata: Uint8Array;
  readonly isDeflated: boolean;
};

export class StreamFrameParseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StreamFrameParseError";
  }
}

export function dashboardWebSocketUrl(gatewayOrigin: string): string {
  const fallbackOrigin = globalThis.location?.origin ?? "http://127.0.0.1:8787";
  const rawOrigin =
    gatewayOrigin.trim().length > 0 ? gatewayOrigin : fallbackOrigin;
  const url = new URL(rawOrigin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = "/ws/dashboard";
  url.search = "";
  url.hash = "";
  return url.toString();
}

export function parseStreamFrame(buffer: ArrayBuffer): StreamFrame {
  if (buffer.byteLength < HEADER_SIZE) {
    throw new StreamFrameParseError("stream frame is shorter than the header");
  }

  const view = new DataView(buffer);
  if (view.getUint32(0, false) !== MAGIC) {
    throw new StreamFrameParseError("stream frame magic is not PSMG");
  }
  if (view.getUint8(4) !== STREAM_FORMAT) {
    throw new StreamFrameParseError("unsupported stream frame format");
  }

  const kind = parseFrameKind(view.getUint8(5));
  const metadataBytes = view.getUint32(30, false);
  const payloadBytes = view.getUint32(26, false);
  const metadataOffset = HEADER_SIZE;
  const payloadOffset = metadataOffset + metadataBytes;
  const expectedBytes = payloadOffset + payloadBytes;
  if (buffer.byteLength !== expectedBytes) {
    throw new StreamFrameParseError(
      "stream frame length does not match header",
    );
  }

  const metadata = copyBytes(buffer, metadataOffset, metadataBytes);
  const payload = copyBytes(buffer, payloadOffset, payloadBytes);
  const flags = view.getUint8(7);

  return {
    kind,
    instanceIndex: view.getUint8(6),
    sequence: view.getUint32(8, false),
    timestampMs: view.getUint32(12, false),
    width: view.getUint16(16, false),
    height: view.getUint16(18, false),
    tileSize: view.getUint16(20, false),
    rawBytes: view.getUint32(22, false),
    payload,
    metadata,
    isDeflated: hasFlag(flags, FLAG_DEFLATE_RAW),
  };
}

export async function inflatePayload(frame: StreamFrame): Promise<Uint8Array> {
  if (!frame.isDeflated) {
    return frame.payload;
  }

  const stream = new Blob([toArrayBuffer(frame.payload)])
    .stream()
    .pipeThrough(new DecompressionStream("deflate-raw"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

function parseFrameKind(value: number): StreamFrameKind {
  switch (value) {
    case FRAME_KEYFRAME:
      return "keyframe";
    case FRAME_DELTA:
      return "delta";
    default:
      throw new StreamFrameParseError("unsupported stream frame type");
  }
}

function hasFlag(flags: number, flag: number): boolean {
  return flags % (flag * 2) >= flag;
}

function copyBytes(
  buffer: ArrayBuffer,
  offset: number,
  length: number,
): Uint8Array {
  const result = new Uint8Array(length);
  result.set(new Uint8Array(buffer, offset, length));
  return result;
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(copy).set(bytes);
  return copy;
}
