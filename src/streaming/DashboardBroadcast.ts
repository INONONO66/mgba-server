// biome-ignore-all lint/style/useFilenamingConvention: Existing multi modules use PascalCase filenames.
import type { IncomingMessage } from "node:http";

import type { RawData, WebSocket, WebSocketServer } from "ws";

import type { InstanceRegistry } from "../gateway/ApiRouter.js";
import type { CapturedFrame } from "./FrameCapture.js";
import { type InputLogBus, type InputLogEvent } from "./InputLog.js";
import type { StreamMetrics } from "./StreamMetrics.js";
import {
  encodeStreamFrame,
  parseViewerControlMessage,
  StreamFrameFlags,
  StreamFrameType,
  VIEWER_CONTROL_MAX_BYTES,
} from "./StreamProtocol.js";

const OPEN_READY_STATE = 1;
const KEYFRAME_REQUEST_THROTTLE_MS = 500;

interface DashboardBroadcastOptions {
  inputLog?: InputLogBus;
  requestKeyframe?: (token?: string) => void;
}

export class DashboardBroadcast {
  private readonly dashboardClients = new Set<WebSocket>();
  private readonly instanceClients = new Map<string, Set<WebSocket>>();
  private readonly inputLogClients = new Map<string, Set<WebSocket>>();
  private readonly keyframesByToken = new Map<string, Buffer>();
  private readonly recoveryTokensByClient = new WeakMap<WebSocket, Set<string>>();
  private readonly lastKeyframeRequestByClient = new WeakMap<WebSocket, Map<string, number>>();
  private readonly wss: WebSocketServer;
  private readonly registry: InstanceRegistry;
  private readonly backpressureLimit: number;
  private readonly metrics?: StreamMetrics;
  private readonly inputLog?: InputLogBus;
  private readonly requestKeyframe?: (token?: string) => void;

  constructor(
    wss: WebSocketServer,
    registry: InstanceRegistry,
    backpressureLimit: number,
    metrics?: StreamMetrics,
    options: DashboardBroadcastOptions = {}
  ) {
    this.wss = wss;
    this.registry = registry;
    this.backpressureLimit = backpressureLimit;
    this.metrics = metrics;
    this.inputLog = options.inputLog;
    this.requestKeyframe = options.requestKeyframe;
    this.setupWebSocketServer();
  }

  broadcastFrame(frame: CapturedFrame): void {
    const binary = encodeFrame(frame);
    if (frame.frameType === StreamFrameType.Keyframe) {
      this.keyframesByToken.set(frame.token, binary);
    }

    for (const ws of this.dashboardClients) {
      const delivered = this.sendFrameToClient(ws, frame, binary);
      this.metrics?.recordDelivery(frame, "dashboard", delivered);
    }

    const instanceSubscribers = this.instanceClients.get(frame.token);
    if (!instanceSubscribers) {
      return;
    }

    for (const ws of instanceSubscribers) {
      const delivered = this.sendFrameToClient(ws, frame, binary);
      this.metrics?.recordDelivery(frame, "instance", delivered);
    }
  }

  private setupWebSocketServer(): void {
    this.wss.on("connection", (ws, req) => {
      this.handleConnection(ws, req);
    });
  }

  private handleConnection(ws: WebSocket, req: IncomingMessage): void {
    const url = req.url ?? "";

    if (url.startsWith("/ws/dashboard")) {
      this.dashboardClients.add(ws);
      this.attachControlHandlers(ws);
      ws.on("close", () => this.dashboardClients.delete(ws));
      ws.on("error", () => this.dashboardClients.delete(ws));
      for (const [token, keyframe] of this.keyframesByToken.entries()) {
        if (!this.registry.has(token)) {
          this.keyframesByToken.delete(token);
          continue;
        }
        this.sendCachedKeyframe(ws, token, keyframe);
      }
      return;
    }


    if (url.startsWith("/ws/input-log/")) {
      const token = safeDecodeURIComponent(url.slice("/ws/input-log/".length));
      this.handleInputLogConnection(ws, token);
      return;
    }

    if (url.startsWith("/ws/logs/")) {
      const token = safeDecodeURIComponent(url.slice("/ws/logs/".length));
      this.handleInputLogConnection(ws, token);
      return;
    }
    if (url.startsWith("/ws/instance/")) {
      const token = safeDecodeURIComponent(url.slice("/ws/instance/".length));
      if (token === undefined) {
        ws.close(4000, "Invalid token");
        return;
      }

      if (!this.registry.has(token)) {
        ws.close(4001, "Unknown token");
        return;
      }

      let clients = this.instanceClients.get(token);
      if (!clients) {
        clients = new Set<WebSocket>();
        this.instanceClients.set(token, clients);
      }

      clients.add(ws);
      this.attachControlHandlers(ws, token);
      ws.on("close", () => {
        clients.delete(ws);
        if (clients.size === 0) {
          this.instanceClients.delete(token);
        }
      });
      ws.on("error", () => {
        clients.delete(ws);
        if (clients.size === 0) {
          this.instanceClients.delete(token);
        }
      });
      const keyframe = this.keyframesByToken.get(token);
      if (keyframe) {
        this.sendCachedKeyframe(ws, token, keyframe);
      } else {
        this.requestKeyframeThrottled(ws, token);
      }
      return;
    }

    ws.close(4000, "Unknown endpoint");
  }

  private handleInputLogConnection(ws: WebSocket, token: string | undefined): void {
    if (token === undefined) {
      ws.close(4000, "Invalid token");
      return;
    }

    const entry = this.registry.get(token);
    if (!entry) {
      ws.close(4001, "Unknown token");
      return;
    }

    let clients = this.inputLogClients.get(token);
    if (!clients) {
      clients = new Set<WebSocket>();
      this.inputLogClients.set(token, clients);
    }

    clients.add(ws);
    const cleanup = () => {
      clients?.delete(ws);
      if (clients?.size === 0) {
        this.inputLogClients.delete(token);
      }
      unsubscribe();
    };
    const unsubscribe = this.inputLog?.subscribe(entry.info.id, (event) => {
      this.sendInputLogEvent(ws, event);
    }) ?? (() => undefined);

    ws.on("close", cleanup);
    ws.on("error", cleanup);
    for (const event of this.inputLog?.recent(entry.info.id) ?? []) {
      this.sendInputLogEvent(ws, event);
    }
  }

  private sendInputLogEvent(ws: WebSocket, event: InputLogEvent): boolean {
    return sendJsonWithBackpressure(ws, { type: "input-log", event }, this.backpressureLimit);
  }

  private attachControlHandlers(ws: WebSocket, token?: string): void {
    ws.on("message", (data) => {
      if (rawDataByteLength(data) > VIEWER_CONTROL_MAX_BYTES) {
        return;
      }

      const message = parseViewerControlMessage(rawDataToBuffer(data));
      if (!message) {
        return;
      }

      if (message.type === "keyframe") {
        if (token !== undefined) {
          this.requestKeyframeThrottled(ws, token);
        }
        return;
      }

      const instanceId = token === undefined ? undefined : this.registry.get(token)?.info.id;
      this.metrics?.recordClientMetrics(instanceId, message.metrics);
    });
  }

  private sendCachedKeyframe(ws: WebSocket, token: string, keyframe: Buffer): void {
    const delivered = sendWithBackpressure(ws, keyframe, this.backpressureLimit);
    if (delivered) {
      this.recoveryTokensByClient.get(ws)?.delete(token);
      return;
    }

    this.markNeedsKeyframe(ws, token);
    this.requestKeyframeThrottled(ws, token);
  }

  private sendFrameToClient(ws: WebSocket, frame: CapturedFrame, binary: Buffer): boolean {
    const recoveryTokens = this.recoveryTokensByClient.get(ws);
    if (frame.frameType !== StreamFrameType.Keyframe && recoveryTokens?.has(frame.token)) {
      this.requestKeyframeThrottled(ws, frame.token);
      return false;
    }

    const delivered = sendWithBackpressure(ws, binary, this.backpressureLimit);
    if (!delivered) {
      this.markNeedsKeyframe(ws, frame.token);
      this.requestKeyframeThrottled(ws, frame.token);
      return false;
    }

    if (frame.frameType === StreamFrameType.Keyframe) {
      recoveryTokens?.delete(frame.token);
    }

    return true;
  }

  private markNeedsKeyframe(ws: WebSocket, token: string): void {
    let recoveryTokens = this.recoveryTokensByClient.get(ws);
    if (!recoveryTokens) {
      recoveryTokens = new Set<string>();
      this.recoveryTokensByClient.set(ws, recoveryTokens);
    }
    recoveryTokens.add(token);
  }

  private requestKeyframeThrottled(ws: WebSocket, token: string | undefined): void {
    const throttleKey = token ?? "*";
    let requests = this.lastKeyframeRequestByClient.get(ws);
    if (!requests) {
      requests = new Map<string, number>();
      this.lastKeyframeRequestByClient.set(ws, requests);
    }

    const now = Date.now();
    if (now - (requests.get(throttleKey) ?? 0) < KEYFRAME_REQUEST_THROTTLE_MS) {
      return;
    }

    requests.set(throttleKey, now);
    this.requestKeyframe?.(token);
  }
}

export function encodeFrame(frame: CapturedFrame): Buffer {
  return encodeStreamFrame({
    frameType: frame.frameType,
    flags: StreamFrameFlags.DeflateRaw,
    height: frame.height,
    instanceIndex: frame.instanceIndex,
    payload: frame.payload,
    metadata: frame.metadata,
    rawBytes: frame.rawBytes,
    sequence: frame.sequence,
    tileSize: frame.tileSize,
    timestampMs: frame.timestampMs,
    width: frame.width,
  });
}

function sendJsonWithBackpressure(ws: WebSocket, value: unknown, limit: number): boolean {
  return sendWithBackpressure(ws, Buffer.from(JSON.stringify(value), "utf8"), limit, false);
}

function sendWithBackpressure(
  ws: WebSocket,
  data: Buffer,
  limit: number,
  binary = true
): boolean {
  if (ws.readyState !== OPEN_READY_STATE) {
    return false;
  }

  if (ws.bufferedAmount > limit) {
    return false;
  }

  ws.send(data, { binary }, () => undefined);
  return true;
}

function safeDecodeURIComponent(value: string): string | undefined {
  try {
    return decodeURIComponent(value);
  } catch {
    return undefined;
  }
}

function rawDataByteLength(data: RawData): number {
  if (Buffer.isBuffer(data)) {
    return data.byteLength;
  }

  if (Array.isArray(data)) {
    return data.reduce((total, chunk) => total + chunk.byteLength, 0);
  }

  return data.byteLength;
}

function rawDataToBuffer(data: RawData): Buffer {
  if (Buffer.isBuffer(data)) {
    return data;
  }

  if (Array.isArray(data)) {
    return Buffer.concat(data);
  }

  return Buffer.from(data);
}
