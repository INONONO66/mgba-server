// biome-ignore-all lint/style/useFilenamingConvention: Existing multi modules use PascalCase filenames.
import type { IncomingMessage } from "node:http";

import type { RawData, WebSocket, WebSocketServer } from "ws";

import { bearerToken, defaultPrincipalAcl, type InstanceEntry, type InstanceRegistry, type PrincipalAccessControl, type PrincipalPermission } from "../gateway/ApiRouter.js";
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
  principalAcl?: PrincipalAccessControl;
  requestKeyframe?: (principalToken?: string) => void;
}

export class DashboardBroadcast {
  private readonly dashboardClients = new Set<WebSocket>();
  private readonly instanceClients = new Map<string, Set<WebSocket>>();
  private readonly inputLogClients = new Map<string, Set<WebSocket>>();
  private readonly keyframesByPrincipalToken = new Map<string, Buffer>();
  private readonly recoveryPrincipalTokensByClient = new WeakMap<WebSocket, Set<string>>();
  private readonly lastKeyframeRequestByClient = new WeakMap<WebSocket, Map<string, number>>();
  private readonly wss: WebSocketServer;
  private readonly registry: InstanceRegistry;
  private readonly backpressureLimit: number;
  private readonly metrics?: StreamMetrics;
  private readonly inputLog?: InputLogBus;
  private readonly principalAcl?: PrincipalAccessControl;
  private readonly requestKeyframe?: (principalToken?: string) => void;

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
    this.principalAcl = options.principalAcl;
    this.requestKeyframe = options.requestKeyframe;
    this.setupWebSocketServer();
  }

  broadcastFrame(frame: CapturedFrame): void {
    const binary = encodeFrame(frame);
    if (frame.frameType === StreamFrameType.Keyframe) {
      this.keyframesByPrincipalToken.set(frame.principalToken, binary);
    }

    for (const ws of this.dashboardClients) {
      const delivered = this.sendFrameToClient(ws, frame, binary);
      this.metrics?.recordDelivery(frame, "dashboard", delivered);
    }

    const instanceSubscribers = this.instanceClients.get(frame.principalToken);
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
      for (const [principalToken, keyframe] of this.keyframesByPrincipalToken.entries()) {
        if (!this.registry.has(principalToken)) {
          this.keyframesByPrincipalToken.delete(principalToken);
          continue;
        }
        this.sendCachedKeyframe(ws, principalToken, keyframe);
      }
      return;
    }


    const streamConnection = this.resolveSessionConnection(url, req, "/ws/sessions/", "/stream", "view-stream");
    if (streamConnection.matched) {
      if (!streamConnection.entry) {
        ws.close(streamConnection.closeCode, streamConnection.closeReason);
        return;
      }
      this.handleStreamConnection(ws, streamConnection.principalToken);
      return;
    }

    const inputLogConnection = this.resolveSessionConnection(url, req, "/ws/sessions/", "/input-log", "view-input-logs");
    if (inputLogConnection.matched) {
      if (!inputLogConnection.entry) {
        ws.close(inputLogConnection.closeCode, inputLogConnection.closeReason);
        return;
      }
      this.handleInputLogConnection(ws, inputLogConnection.entry, inputLogConnection.principalToken);
      return;
    }

    ws.close(4000, "Unknown endpoint");
  }


  private resolveSessionConnection(
    rawUrl: string,
    req: IncomingMessage,
    prefix: string,
    suffix: string,
    permission: PrincipalPermission,
  ): {
    closeCode: number;
    closeReason: string;
    entry?: InstanceEntry;
    matched: boolean;
    principalToken: string;
  } {
    const parsed = parseWsUrl(rawUrl);
    const pathname = parsed?.pathname ?? rawUrl.split("?", 1)[0] ?? "";
    if (!pathname.startsWith(prefix) || !pathname.endsWith(suffix)) {
      return { closeCode: 4000, closeReason: "Unknown endpoint", matched: false, principalToken: "" };
    }

    const sessionId = safeDecodeURIComponent(pathname.slice(prefix.length, -suffix.length));
    if (sessionId === undefined || sessionId === "") {
      return { closeCode: 4000, closeReason: "Invalid session", matched: true, principalToken: "" };
    }

    const principalToken = parsed?.searchParams.get("principal_token") ?? bearerToken(req.headers.authorization);
    if (principalToken === undefined || principalToken === "") {
      return { closeCode: 4001, closeReason: "Unauthorized", matched: true, principalToken: "" };
    }

    const entryRecord = Array.from(this.registry.entries()).find(([, candidate]) => candidate.info.id === sessionId);
    if (entryRecord === undefined) {
      return { closeCode: 4001, closeReason: "Unknown session", matched: true, principalToken: "" };
    }

    const [sessionPrincipalToken, entry] = entryRecord;
    const acl = this.principalAcl ?? defaultPrincipalAcl(this.registry);
    if (acl.authorize(principalToken, sessionId, permission) === undefined) {
      return { closeCode: 4001, closeReason: "Unauthorized", matched: true, principalToken: "" };
    }

    return { closeCode: 1000, closeReason: "", entry, matched: true, principalToken: sessionPrincipalToken };
  }


  private handleStreamConnection(ws: WebSocket, principalToken: string): void {
    let clients = this.instanceClients.get(principalToken);
    if (!clients) {
      clients = new Set<WebSocket>();
      this.instanceClients.set(principalToken, clients);
    }

    clients.add(ws);
    this.attachControlHandlers(ws, principalToken);
    ws.on("close", () => {
      clients.delete(ws);
      if (clients.size === 0) {
        this.instanceClients.delete(principalToken);
      }
    });
    ws.on("error", () => {
      clients.delete(ws);
      if (clients.size === 0) {
        this.instanceClients.delete(principalToken);
      }
    });
    const keyframe = this.keyframesByPrincipalToken.get(principalToken);
    if (keyframe) {
      this.sendCachedKeyframe(ws, principalToken, keyframe);
    } else {
      this.requestKeyframeThrottled(ws, principalToken);
    }
  }

  private handleInputLogConnection(ws: WebSocket, entry: InstanceEntry, principalToken: string): void {

    let clients = this.inputLogClients.get(principalToken);
    if (!clients) {
      clients = new Set<WebSocket>();
      this.inputLogClients.set(principalToken, clients);
    }

    clients.add(ws);
    const cleanup = () => {
      clients?.delete(ws);
      if (clients?.size === 0) {
        this.inputLogClients.delete(principalToken);
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

  private attachControlHandlers(ws: WebSocket, principalToken?: string): void {
    ws.on("message", (data) => {
      if (rawDataByteLength(data) > VIEWER_CONTROL_MAX_BYTES) {
        return;
      }

      const message = parseViewerControlMessage(rawDataToBuffer(data));
      if (!message) {
        return;
      }

      if (message.type === "keyframe") {
        if (principalToken !== undefined) {
          this.requestKeyframeThrottled(ws, principalToken);
        }
        return;
      }

      const instanceId = principalToken === undefined ? undefined : this.registry.get(principalToken)?.info.id;
      this.metrics?.recordClientMetrics(instanceId, message.metrics);
    });
  }

  private sendCachedKeyframe(ws: WebSocket, principalToken: string, keyframe: Buffer): void {
    const delivered = sendWithBackpressure(ws, keyframe, this.backpressureLimit);
    if (delivered) {
      this.recoveryPrincipalTokensByClient.get(ws)?.delete(principalToken);
      return;
    }

    this.markNeedsKeyframe(ws, principalToken);
    this.requestKeyframeThrottled(ws, principalToken);
  }

  private sendFrameToClient(ws: WebSocket, frame: CapturedFrame, binary: Buffer): boolean {
    const recoveryPrincipalTokens = this.recoveryPrincipalTokensByClient.get(ws);
    if (frame.frameType !== StreamFrameType.Keyframe && recoveryPrincipalTokens?.has(frame.principalToken)) {
      this.requestKeyframeThrottled(ws, frame.principalToken);
      return false;
    }

    const delivered = sendWithBackpressure(ws, binary, this.backpressureLimit);
    if (!delivered) {
      this.markNeedsKeyframe(ws, frame.principalToken);
      this.requestKeyframeThrottled(ws, frame.principalToken);
      return false;
    }

    if (frame.frameType === StreamFrameType.Keyframe) {
      recoveryPrincipalTokens?.delete(frame.principalToken);
    }

    return true;
  }

  private markNeedsKeyframe(ws: WebSocket, principalToken: string): void {
    let recoveryPrincipalTokens = this.recoveryPrincipalTokensByClient.get(ws);
    if (!recoveryPrincipalTokens) {
      recoveryPrincipalTokens = new Set<string>();
      this.recoveryPrincipalTokensByClient.set(ws, recoveryPrincipalTokens);
    }
    recoveryPrincipalTokens.add(principalToken);
  }

  private requestKeyframeThrottled(ws: WebSocket, principalToken: string | undefined): void {
    const throttleKey = principalToken ?? "*";
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
    this.requestKeyframe?.(principalToken);
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

function parseWsUrl(value: string): URL | undefined {
  try {
    return new URL(value, "http://localhost");
  } catch {
    return undefined;
  }
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
