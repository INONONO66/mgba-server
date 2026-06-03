// biome-ignore-all lint/style/useFilenamingConvention: Existing multi modules use PascalCase filenames.

const STREAM_MAGIC = "PSMG";
const STREAM_FORMAT = 2;
export const STREAM_HEADER_SIZE = 34;
export const VIEWER_CONTROL_MAX_BYTES = 4096;
const MAX_CLIENT_COUNTER = 10_000_000;
const MAX_CLIENT_FPS = 1000;

export const enum StreamFrameType {
  Keyframe = 1,
  Delta = 2,
}

export const enum StreamFrameFlags {
  None = 0,
  DeflateRaw = 1 << 0,
}

export interface StreamFrameCausalityMetadata {
  action?: string;
  actorPrincipalId?: string;
  button?: string;
  controlEventId: string;
  inputCompletedAtMs?: number;
  inputLatencyMs?: number;
  inputRequestedAtMs?: number;
  requestId?: string;
  source?: string;
}

export interface StreamFrameMetadata {
  causality?: StreamFrameCausalityMetadata;
  sourceCaptureStartedAtMs?: number;
  sourceCapturedAtMs?: number;
}

interface StreamFrameEnvelope {
  frameType: StreamFrameType;
  flags: number;
  height: number;
  instanceIndex: number;
  metadata?: StreamFrameMetadata;
  metadataBytes: number;
  payload: Buffer;
  payloadBytes: number;
  rawBytes: number;
  sequence: number;
  tileSize: number;
  timestampMs: number;
  format: number;
  width: number;
}

interface EncodableStreamFrame {
  frameType: StreamFrameType;
  flags?: number;
  height: number;
  instanceIndex: number;
  metadata?: StreamFrameMetadata;
  payload: Buffer;
  rawBytes: number;
  sequence: number;
  tileSize: number;
  timestampMs: number;
  width: number;
}

type ViewerControlMessage =
  | { type: "client-metrics"; metrics: ViewerClientMetrics }
  | { type: "keyframe" };

export interface ViewerClientMetrics {
  decodedFrames?: number;
  droppedFrames?: number;
  fps?: number;
  keyframeRecoveries?: number;
  reconnects?: number;
  renderedFrames?: number;
  sequenceGaps?: number;
}

export function encodeStreamFrame(frame: EncodableStreamFrame): Buffer {
  const metadata = encodeMetadata(frame.metadata);
  const header = Buffer.allocUnsafe(STREAM_HEADER_SIZE);
  header.write(STREAM_MAGIC, 0, "ascii");
  header.writeUInt8(STREAM_FORMAT, 4);
  header.writeUInt8(frame.frameType, 5);
  header.writeUInt8(frame.instanceIndex % 256, 6);
  header.writeUInt8(frame.flags ?? StreamFrameFlags.None, 7);
  header.writeUInt32BE(frame.sequence >>> 0, 8);
  header.writeUInt32BE(frame.timestampMs >>> 0, 12);
  header.writeUInt16BE(frame.width, 16);
  header.writeUInt16BE(frame.height, 18);
  header.writeUInt16BE(frame.tileSize, 20);
  header.writeUInt32BE(frame.rawBytes >>> 0, 22);
  header.writeUInt32BE(frame.payload.byteLength >>> 0, 26);
  header.writeUInt32BE(metadata.byteLength >>> 0, 30);
  return Buffer.concat([header, metadata, frame.payload]);
}

export function decodeStreamFrame(data: Buffer): StreamFrameEnvelope | undefined {
  if (data.byteLength < STREAM_HEADER_SIZE) {
    return undefined;
  }
  if (data.toString("ascii", 0, 4) !== STREAM_MAGIC) {
    return undefined;
  }

  const format = data.readUInt8(4);
  if (format !== STREAM_FORMAT) {
    return undefined;
  }

  const payloadBytes = data.readUInt32BE(26);
  const metadataBytes = data.readUInt32BE(30);
  const payloadOffset = STREAM_HEADER_SIZE + metadataBytes;
  const expectedLength = payloadOffset + payloadBytes;
  if (data.byteLength !== expectedLength) {
    return undefined;
  }

  const metadata = decodeMetadata(data.subarray(STREAM_HEADER_SIZE, payloadOffset));

  return {
    format,
    frameType: data.readUInt8(5) as StreamFrameType,
    instanceIndex: data.readUInt8(6),
    flags: data.readUInt8(7),
    sequence: data.readUInt32BE(8),
    timestampMs: data.readUInt32BE(12),
    width: data.readUInt16BE(16),
    height: data.readUInt16BE(18),
    tileSize: data.readUInt16BE(20),
    rawBytes: data.readUInt32BE(22),
    metadata,
    metadataBytes,
    payloadBytes,
    payload: data.subarray(payloadOffset),
  };
}

export function parseViewerControlMessage(data: Buffer): ViewerControlMessage | undefined {
  if (data.byteLength > VIEWER_CONTROL_MAX_BYTES) {
    return undefined;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(data.toString("utf8"));
  } catch {
    return undefined;
  }

  if (!parsed || typeof parsed !== "object" || !("type" in parsed)) {
    return undefined;
  }

  const candidate = parsed as { type?: unknown; metrics?: unknown };
  if (candidate.type === "keyframe") {
    return { type: "keyframe" };
  }

  if (candidate.type !== "client-metrics") {
    return undefined;
  }

  return {
    type: "client-metrics",
    metrics: sanitizeClientMetrics(candidate.metrics),
  };
}

function sanitizeClientMetrics(value: unknown): ViewerClientMetrics {
  if (!value || typeof value !== "object") {
    return {};
  }

  const metrics = value as Record<string, unknown>;
  return {
    decodedFrames: optionalNonNegativeNumber(metrics.decodedFrames),
    droppedFrames: optionalNonNegativeNumber(metrics.droppedFrames),
    fps: optionalNonNegativeNumber(metrics.fps, MAX_CLIENT_FPS),
    keyframeRecoveries: optionalNonNegativeNumber(metrics.keyframeRecoveries),
    reconnects: optionalNonNegativeNumber(metrics.reconnects),
    renderedFrames: optionalNonNegativeNumber(metrics.renderedFrames),
    sequenceGaps: optionalNonNegativeNumber(metrics.sequenceGaps),
  };
}

function optionalNonNegativeNumber(value: unknown, maxValue = MAX_CLIENT_COUNTER): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return undefined;
  }

  return Math.min(value, maxValue);
}


function encodeMetadata(metadata: StreamFrameMetadata | undefined): Buffer {
  if (metadata === undefined) {
    return Buffer.alloc(0);
  }

  return Buffer.from(JSON.stringify(metadata), "utf8");
}

function decodeMetadata(data: Buffer): StreamFrameMetadata | undefined {
  if (data.byteLength === 0) {
    return undefined;
  }

  try {
    const parsed: unknown = JSON.parse(data.toString("utf8"));
    if (!parsed || typeof parsed !== "object") {
      return undefined;
    }

    return parsed as StreamFrameMetadata;
  } catch {
    return undefined;
  }
}
