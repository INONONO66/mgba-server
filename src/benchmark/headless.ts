import { writeFile } from "node:fs/promises";

import WebSocket, { type RawData } from "ws";
import { decodeStreamFrame, StreamFrameType } from "../streaming/StreamProtocol.js";
import {
  buildInstanceReport,
  finalizeBenchmarkReport,
  formatHumanSummary,
  type ResourceSampleReport,
} from "./report.js";
import { collectResourceSample } from "./resources.js";

const TRAILING_SLASH = /\/$/;

const DEFAULT_INSTANCES = 20;
const MAX_INSTANCES = 20;
const DEFAULT_DURATION_MS = 60_000;
const DEFAULT_WARMUP_MS = 5000;
const DEFAULT_SAMPLE_INTERVAL_MS = 5000;
const DEFAULT_MIN_P95_FPS = 60;
const DEFAULT_MAX_DROPPED_RATIO = 0.01;
const DEFAULT_MAX_RAM_BYTES = 16 * 1024 * 1024 * 1024;
const DEFAULT_LATE_FRAME_THRESHOLD_MS = (1000 / 60) * 1.5;
const DEFAULT_MAX_KEY_SEND_P95_MS = 100;
const DEFAULT_MAX_MEMORY_READ_P95_MS = 100;
const DEFAULT_MAX_SCREEN_REFLECTION_P95_MS = 200;
const DEFAULT_PROBE_INTERVAL_MS = 1000;

interface BenchmarkCliOptions {
  adminToken: string;
  baseUrl: string;
  cleanupCreated: boolean;
  createMissing: boolean;
  durationMs: number;
  gatewayPid?: number;
  instances: number;
  lateFrameThresholdMs: number;
  maxDroppedOrLateRatio: number;
  maxTotalRamBytes: number;
  maxKeySendP95Ms: number;
  maxMemoryReadP95Ms: number;
  maxScreenReflectionP95Ms: number;
  minP95Fps: number;
  output?: string;
  probeIntervalMs: number;
  sampleIntervalMs: number;
  strictAcceptance: boolean;
  summaryOutput?: string;
  warmupMs: number;
}

interface AdminInstance {
  containerId: string;
  id: string;
  status: string;
  principalToken: string;
}

interface PendingReflectionProbe {
  controlEventId: string;
  requestedAtMs: number;
  requestedAtPerfMs: number;
}

interface InstanceCapture {
  instance: AdminInstance;
  keySendLatencyMs: number[];
  keyframeRecoveries: number;
  lastSequence?: number;
  memoryReadLatencyMs: number[];
  needsKeyframe: boolean;
  pendingReflections: PendingReflectionProbe[];
  receivedAtMs: number[];
  reconnects: number;
  screenReflectionLatencyMs: number[];
  sequenceGaps: number;
  timestampChainFailures: number;
  plannedClose: boolean;
  ws?: WebSocket;
}

interface ServerStreamMetricsSnapshot {
  instances?: Array<{
    instanceId: string;
    producedFrames?: number;
    producedFps?: number;
    dashboardFramesDropped?: number;
    instanceFramesDropped?: number;
    sequenceGaps?: number;
    clientSequenceGaps?: number;
  }>;
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const createdInstances: AdminInstance[] = [];

  try {
    const instances = await ensureInstances(options, createdInstances);
    const selectedInstances = instances.slice(0, options.instances);
    const captures: InstanceCapture[] = selectedInstances.map((instance) => ({
      instance,
      receivedAtMs: [],
      reconnects: 0,
      plannedClose: false,
      keySendLatencyMs: [],
      keyframeRecoveries: 0,
      memoryReadLatencyMs: [],
      needsKeyframe: false,
      pendingReflections: [],
      screenReflectionLatencyMs: [],
      sequenceGaps: 0,
      timestampChainFailures: 0,
    }));

    await Promise.all(
      captures.map((capture) => openCaptureSocket(options, capture))
    );
    await sleep(options.warmupMs);
    for (const capture of captures) {
      resetMeasurementCounters(capture);
      requestMeasurementKeyframe(capture);
    }

    const serverStreamMetricsStart = await fetchServerStreamMetrics(
      options
    ).catch(() => undefined);
    const measurementStartedAt = new Date();
    const measurementStartedAtMs = performance.now();
    const resourceSamples = await collectSamplesAndProbesDuringRun(
      options,
      selectedInstances,
      captures
    );
    const measurementEndedAtMs = performance.now();
    const measurementEndedAt = new Date();
    const serverStreamMetricsEnd = await fetchServerStreamMetrics(options).catch(
      () => undefined
    );
    for (const capture of captures) {
      capture.plannedClose = true;
    }
    await Promise.all(captures.map((capture) => closeSocket(capture.ws)));

    const serverStreamMetricsDelta = diffServerStreamMetrics(
      serverStreamMetricsStart,
      serverStreamMetricsEnd,
      measurementEndedAt.getTime() - measurementStartedAt.getTime()
    );
    const serverMetricsByInstance = new Map(
      serverStreamMetricsDelta?.instances?.map((metrics) => [
        metrics.instanceId,
        metrics,
      ]) ?? []
    );

    const report = finalizeBenchmarkReport({
      baseUrl: options.baseUrl,
      measurementWindow: {
        startedAt: measurementStartedAt,
        endedAt: measurementEndedAt,
      },
      target: {
        instances: options.instances,
        durationMs: options.durationMs,
        minP95Fps: options.minP95Fps,
        maxDroppedOrLateRatio: options.maxDroppedOrLateRatio,
        maxTotalRamBytes: options.maxTotalRamBytes,
        maxKeySendP95Ms: options.maxKeySendP95Ms,
        maxMemoryReadP95Ms: options.maxMemoryReadP95Ms,
        maxScreenReflectionP95Ms: options.maxScreenReflectionP95Ms,
        requiresGatewayMemory: options.strictAcceptance,
        strictAcceptance: options.strictAcceptance,
      },
      instances: captures.map((capture) => {
        const serverMetrics = serverMetricsByInstance.get(capture.instance.id);
        return buildInstanceReport({
          instanceId: capture.instance.id,
          receivedAtMs: capture.receivedAtMs,
          durationMs: options.durationMs,
          windowStartedAtMs: measurementStartedAtMs,
          windowEndedAtMs: measurementEndedAtMs,
          lateFrameThresholdMs: options.lateFrameThresholdMs,
          minP95Fps: options.minP95Fps,
          maxDroppedOrLateRatio: options.maxDroppedOrLateRatio,
          keySendLatencyMs: capture.keySendLatencyMs,
          memoryReadLatencyMs: capture.memoryReadLatencyMs,
          screenReflectionLatencyMs: capture.screenReflectionLatencyMs,
          timestampChainFailures: capture.timestampChainFailures + capture.pendingReflections.length,
          maxKeySendP95Ms: options.maxKeySendP95Ms,
          maxMemoryReadP95Ms: options.maxMemoryReadP95Ms,
          maxScreenReflectionP95Ms: options.maxScreenReflectionP95Ms,
          serverProducedFrames: serverMetrics?.producedFrames,
          serverProducedFps: serverMetrics?.producedFps,
          serverDroppedFrames: Math.max(
            (serverMetrics?.dashboardFramesDropped ?? 0) +
              (serverMetrics?.instanceFramesDropped ?? 0),
            capture.sequenceGaps,
            serverMetrics?.sequenceGaps ?? 0,
            serverMetrics?.clientSequenceGaps ?? 0
          ),
          reconnects: capture.reconnects,
          keyframeRecoveries: capture.keyframeRecoveries,
          sequenceGaps: capture.sequenceGaps,
        });
      }),
      resourceSamples,
      serverStreamMetrics: {
        start: serverStreamMetricsStart,
        end: serverStreamMetricsEnd,
        delta: serverStreamMetricsDelta,
      },
    });

    const json = `${JSON.stringify(report, null, 2)}\n`;
    const summary = formatHumanSummary(report);

    if (options.output) {
      await writeFile(options.output, json);
    } else {
      process.stdout.write(json);
    }

    if (options.summaryOutput) {
      await writeFile(options.summaryOutput, summary);
    } else {
      process.stderr.write(summary);
    }

    if (!report.verdict.pass) {
      process.exitCode = 1;
    }
  } finally {
    if (options.cleanupCreated) {
      await Promise.all(
        createdInstances.map((instance) =>
          deleteInstance(options, instance.id).catch(() => undefined)
        )
      );
    }
  }
}

async function ensureInstances(
  options: BenchmarkCliOptions,
  createdInstances: AdminInstance[]
): Promise<AdminInstance[]> {
  let instances = await listInstances(options);
  const missing =
    options.instances -
    instances.filter((instance) => instance.status === "running").length;

  if (missing > 0 && !options.createMissing) {
    throw new Error(
      `Need ${options.instances} running instances, found ${instances.length}`
    );
  }

  for (let count = 0; count < missing; count += 1) {
    const created = await createInstance(options);
    createdInstances.push(created);
  }

  instances = await listInstances(options);
  const running = instances.filter((instance) => instance.status === "running");
  if (running.length < options.instances) {
    throw new Error(
      `Need ${options.instances} running instances, found ${running.length}`
    );
  }

  return running;
}

async function openCaptureSocket(
  options: BenchmarkCliOptions,
  capture: InstanceCapture
): Promise<void> {
  const ws = new WebSocket(
    `${toWsBaseUrl(options.baseUrl)}/ws/sessions/${encodeURIComponent(capture.instance.id)}/stream`,
    { headers: principalHeaders(capture.instance.principalToken) as Record<string, string> }
  );
  capture.ws = ws;

  ws.on("message", (data) => {
    const frame = decodeStreamFrame(rawDataToBuffer(data));
    if (!frame) {
      return;
    }

    if (capture.lastSequence !== undefined && frame.sequence > capture.lastSequence + 1) {
      capture.sequenceGaps += frame.sequence - capture.lastSequence - 1;
      capture.needsKeyframe = true;
    }
    capture.lastSequence = frame.sequence;

    const receivedAtPerfMs = performance.now();
    if (frame.metadata?.causality) {
      recordScreenReflection(capture, frame.metadata, receivedAtPerfMs);
    }

    if (frame.frameType === StreamFrameType.Keyframe) {
      capture.keyframeRecoveries += 1;
      capture.needsKeyframe = false;
    } else if (capture.needsKeyframe) {
      return;
    }

    capture.receivedAtMs.push(receivedAtPerfMs);
  });

  ws.on("close", () => {
    if (!capture.plannedClose) {
      capture.reconnects += 1;
    }
  });

  await new Promise<void>((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
}

function resetMeasurementCounters(capture: InstanceCapture): void {
  capture.receivedAtMs.length = 0;
  capture.keySendLatencyMs.length = 0;
  capture.keyframeRecoveries = 0;
  capture.memoryReadLatencyMs.length = 0;
  capture.needsKeyframe = true;
  capture.pendingReflections.length = 0;
  capture.screenReflectionLatencyMs.length = 0;
  capture.reconnects = 0;
  capture.sequenceGaps = 0;
  capture.timestampChainFailures = 0;
}

function requestMeasurementKeyframe(capture: InstanceCapture): void {
  if (capture.ws?.readyState === WebSocket.OPEN) {
    capture.ws.send(JSON.stringify({ type: "keyframe" }));
  }
}

async function collectSamplesAndProbesDuringRun(
  options: BenchmarkCliOptions,
  instances: AdminInstance[],
  captures: InstanceCapture[]
): Promise<ResourceSampleReport[]> {
  const deadline = Date.now() + options.durationMs;
  const [samples] = await Promise.all([
    collectResourceSamplesUntil(options, instances, deadline),
    collectLatencyProbesUntil(options, captures, deadline),
  ]);
  return samples;
}

async function collectResourceSamplesUntil(
  options: BenchmarkCliOptions,
  instances: AdminInstance[],
  deadline: number
): Promise<ResourceSampleReport[]> {
  const samples: ResourceSampleReport[] = [];

  while (Date.now() < deadline) {
    samples.push(
      await collectResourceSample({
        containerIds: instances.map((instance) => instance.containerId),
        gatewayPid: options.gatewayPid,
      })
    );
    await sleep(
      Math.min(options.sampleIntervalMs, Math.max(0, deadline - Date.now()))
    );
  }

  samples.push(
    await collectResourceSample({
      containerIds: instances.map((instance) => instance.containerId),
      gatewayPid: options.gatewayPid,
    })
  );

  return samples;
}

async function collectLatencyProbesUntil(
  options: BenchmarkCliOptions,
  captures: InstanceCapture[],
  deadline: number
): Promise<void> {
  while (Date.now() < deadline) {
    await Promise.all(captures.map((capture) => issueLatencyProbe(options, capture)));
    await sleep(Math.min(options.probeIntervalMs, Math.max(0, deadline - Date.now())));
  }
}

async function issueLatencyProbe(
  options: BenchmarkCliOptions,
  capture: InstanceCapture
): Promise<void> {
  const keySend = measureTextRequest(
    `${options.baseUrl}/api/sessions/${encodeURIComponent(capture.instance.id)}/mgba-http/button/tap?button=A`,
    { method: "POST", headers: principalHeaders(capture.instance.principalToken) },
    capture.keySendLatencyMs
  )
    .then((result) => {
      if (!result.controlEventId) {
        capture.timestampChainFailures += 1;
        return;
      }
      capture.pendingReflections.push({
        controlEventId: result.controlEventId,
        requestedAtMs: result.startedAtWallMs,
        requestedAtPerfMs: result.startedAtPerfMs,
      });
    })
    .catch(() => {
      capture.timestampChainFailures += 1;
    });

  const memoryRead = measureTextRequest(
    `${options.baseUrl}/api/sessions/${encodeURIComponent(capture.instance.id)}/core/read8?address=0x00000000`,
    { method: "GET", headers: principalHeaders(capture.instance.principalToken) },
    capture.memoryReadLatencyMs
  ).catch(() => {
    capture.timestampChainFailures += 1;
  });

  await Promise.all([
    keySend,
    memoryRead,
  ]);
}

async function measureTextRequest(
  url: string,
  init: RequestInit,
  samples: number[]
): Promise<{
  body: string;
  controlEventId?: string;
  startedAtPerfMs: number;
  startedAtWallMs: number;
}> {
  const startedAtWallMs = Date.now();
  const startedAtPerfMs = performance.now();
  const response = await fetch(url, init);
  const body = await response.text();
  const latencyMs = performance.now() - startedAtPerfMs;
  if (!response.ok) {
    throw new Error(`${init.method ?? "GET"} ${url} failed: ${response.status} ${body}`);
  }
  samples.push(latencyMs);
  return {
    body,
    controlEventId: response.headers.get("X-Control-Event-Id") ?? undefined,
    startedAtPerfMs,
    startedAtWallMs,
  };
}

function principalHeaders(principalToken: string): HeadersInit {
  return { Authorization: `Bearer ${principalToken}` };
}

function recordScreenReflection(
  capture: InstanceCapture,
  metadata: {
    causality?: {
      controlEventId?: string;
      inputCompletedAtMs?: number;
      inputLatencyMs?: number;
      inputRequestedAtMs?: number;
    };
    sourceCapturedAtMs?: number;
    sourceCaptureStartedAtMs?: number;
  },
  receivedAtPerfMs: number
): void {
  const causality = metadata.causality;
  if (!causality?.controlEventId) {
    capture.timestampChainFailures += 1;
    return;
  }
  const pendingIndex = capture.pendingReflections.findIndex((pending) =>
    pending.controlEventId === causality.controlEventId
  );
  if (pendingIndex === -1) {
    capture.timestampChainFailures += 1;
    return;
  }

  const pending = capture.pendingReflections[pendingIndex];
  capture.pendingReflections.splice(pendingIndex, 1);

  if (
    causality.inputRequestedAtMs === undefined ||
    causality.inputCompletedAtMs === undefined ||
    causality.inputLatencyMs === undefined ||
    metadata.sourceCaptureStartedAtMs === undefined ||
    metadata.sourceCapturedAtMs === undefined ||
    metadata.sourceCaptureStartedAtMs < causality.inputCompletedAtMs ||
    metadata.sourceCapturedAtMs < metadata.sourceCaptureStartedAtMs
  ) {
    capture.timestampChainFailures += 1;
    return;
  }

  capture.screenReflectionLatencyMs.push(receivedAtPerfMs - pending.requestedAtPerfMs);
}

function listInstances(options: BenchmarkCliOptions): Promise<AdminInstance[]> {
  return adminFetch(options, "/admin/instances", { method: "GET" });
}

function createInstance(options: BenchmarkCliOptions): Promise<AdminInstance> {
  return adminFetch(options, "/admin/instances", { method: "POST" });
}

async function deleteInstance(
  options: BenchmarkCliOptions,
  instanceId: string
): Promise<void> {
  await adminFetch(
    options,
    `/admin/instances/${encodeURIComponent(instanceId)}`,
    { method: "DELETE" }
  );
}

function fetchServerStreamMetrics(
  options: BenchmarkCliOptions
): Promise<ServerStreamMetricsSnapshot | undefined> {
  return adminFetch(options, "/admin/metrics/streams", { method: "GET" });
}

function diffServerStreamMetrics(
  start: ServerStreamMetricsSnapshot | undefined,
  end: ServerStreamMetricsSnapshot | undefined,
  durationMs: number
): ServerStreamMetricsSnapshot | undefined {
  if (!start || !end) {
    return;
  }

  const startByInstance = new Map(
    start?.instances?.map((metrics) => [metrics.instanceId, metrics]) ?? []
  );

  return {
    instances: end.instances?.map((endMetrics) => {
      const startMetrics = startByInstance.get(endMetrics.instanceId);
      const producedFrames = subtractMetric(
        endMetrics.producedFrames,
        startMetrics?.producedFrames
      );
      return {
        instanceId: endMetrics.instanceId,
        producedFrames,
        producedFps:
          producedFrames === undefined || durationMs <= 0
            ? undefined
            : producedFrames / (durationMs / 1000),
        dashboardFramesDropped: subtractMetric(
          endMetrics.dashboardFramesDropped,
          startMetrics?.dashboardFramesDropped
        ),
        instanceFramesDropped: subtractMetric(
          endMetrics.instanceFramesDropped,
          startMetrics?.instanceFramesDropped
        ),
        sequenceGaps: subtractMetric(
          endMetrics.sequenceGaps,
          startMetrics?.sequenceGaps
        ),
        clientSequenceGaps: subtractMetric(
          endMetrics.clientSequenceGaps,
          startMetrics?.clientSequenceGaps
        ),
      };
    }),
  };
}

function subtractMetric(
  endValue: number | undefined,
  startValue: number | undefined
): number | undefined {
  if (endValue === undefined) {
    return;
  }
  return Math.max(0, endValue - (startValue ?? 0));
}

async function adminFetch<T>(
  options: BenchmarkCliOptions,
  path: string,
  init: RequestInit
): Promise<T> {
  const response = await fetch(`${options.baseUrl}${path}`, {
    ...init,
    headers: { "X-Admin-Token": options.adminToken, ...init.headers },
  });

  if (!response.ok) {
    throw new Error(
      `${init.method ?? "GET"} ${path} failed: ${response.status} ${await response.text()}`
    );
  }

  return response.json() as Promise<T>;
}

function parseArgs(args: string[]): BenchmarkCliOptions {
  const values = new Map<string, string>();
  const flags = new Set<string>();

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--") {
      continue;
    }

    if (!arg?.startsWith("--")) {
      throw new Error(`Unexpected argument: ${arg}`);
    }

    const [rawName, inlineValue] = arg.slice(2).split("=", 2);
    if (rawName === "") {
      throw new Error(`Invalid option: ${arg}`);
    }

    if (inlineValue !== undefined) {
      values.set(rawName, inlineValue);
      continue;
    }

    const next = args[index + 1];
    if (next !== undefined && !next.startsWith("--")) {
      values.set(rawName, next);
      index += 1;
      continue;
    }

    flags.add(rawName);
  }

  const instances = readInteger(values, "instances", DEFAULT_INSTANCES);
  if (instances < 1 || instances > MAX_INSTANCES) {
    throw new Error("--instances must be an integer between 1 and 20");
  }
  const durationMs = readPositiveNumber(values, "duration-ms", DEFAULT_DURATION_MS);
  const warmupMs = readNonNegativeNumber(values, "warmup-ms", DEFAULT_WARMUP_MS);
  const sampleIntervalMs = readPositiveNumber(
    values,
    "sample-interval-ms",
    DEFAULT_SAMPLE_INTERVAL_MS
  );
  const lateFrameThresholdMs = readPositiveNumber(
    values,
    "late-frame-threshold-ms",
    DEFAULT_LATE_FRAME_THRESHOLD_MS
  );
  const minP95Fps = readPositiveNumber(values, "min-p95-fps", DEFAULT_MIN_P95_FPS);
  const maxDroppedOrLateRatio = readRatio(
    values,
    "max-dropped-late-ratio",
    DEFAULT_MAX_DROPPED_RATIO
  );
  const maxTotalRamBytes = readPositiveNumber(
    values,
    "max-total-ram-bytes",
    DEFAULT_MAX_RAM_BYTES
  );
  const maxKeySendP95Ms = readPositiveNumber(
    values,
    "max-key-send-p95-ms",
    DEFAULT_MAX_KEY_SEND_P95_MS
  );
  const maxMemoryReadP95Ms = readPositiveNumber(
    values,
    "max-memory-read-p95-ms",
    DEFAULT_MAX_MEMORY_READ_P95_MS
  );
  const maxScreenReflectionP95Ms = readPositiveNumber(
    values,
    "max-screen-reflection-p95-ms",
    DEFAULT_MAX_SCREEN_REFLECTION_P95_MS
  );
  const probeIntervalMs = readPositiveNumber(
    values,
    "probe-interval-ms",
    DEFAULT_PROBE_INTERVAL_MS
  );

  return {
    baseUrl: readString(
      values,
      "base-url",
      process.env.MULTI_BASE_URL ?? "http://127.0.0.1:8787"
    ),
    adminToken: readString(
      values,
      "admin-token",
      process.env.ADMIN_TOKEN ?? "dev-admin-token"
    ),
    instances,
    durationMs,
    warmupMs,
    sampleIntervalMs,
    lateFrameThresholdMs,
    minP95Fps,
    maxDroppedOrLateRatio,
    maxTotalRamBytes,
    maxKeySendP95Ms,
    maxMemoryReadP95Ms,
    maxScreenReflectionP95Ms,
    output: values.get("output"),
    probeIntervalMs,
    summaryOutput: values.get("summary-output"),
    gatewayPid: readOptionalPositiveInteger(values, "gateway-pid"),
    createMissing: !flags.has("no-create"),
    cleanupCreated: flags.has("cleanup-created"),
    strictAcceptance: !flags.has("allow-reduced-target"),
  };
}

function readString(
  values: Map<string, string>,
  name: string,
  defaultValue: string
): string {
  return values.get(name) ?? defaultValue;
}

function readInteger(
  values: Map<string, string>,
  name: string,
  defaultValue: number
): number {
  const value = readOptionalNumber(values, name) ?? defaultValue;
  if (!Number.isInteger(value)) {
    throw new Error(`--${name} must be an integer`);
  }
  return value;
}

function readPositiveNumber(
  values: Map<string, string>,
  name: string,
  defaultValue: number
): number {
  const value = readOptionalNumber(values, name) ?? defaultValue;
  if (value <= 0) {
    throw new Error(`--${name} must be greater than 0`);
  }
  return value;
}

function readNonNegativeNumber(
  values: Map<string, string>,
  name: string,
  defaultValue: number
): number {
  const value = readOptionalNumber(values, name) ?? defaultValue;
  if (value < 0) {
    throw new Error(`--${name} must be greater than or equal to 0`);
  }
  return value;
}

function readRatio(
  values: Map<string, string>,
  name: string,
  defaultValue: number
): number {
  const value = readNonNegativeNumber(values, name, defaultValue);
  if (value > 1) {
    throw new Error(`--${name} must be between 0 and 1`);
  }
  return value;
}

function readOptionalPositiveInteger(
  values: Map<string, string>,
  name: string
): number | undefined {
  const value = readOptionalNumber(values, name);
  if (value === undefined) {
    return;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`--${name} must be a positive integer`);
  }
  return value;
}

function readOptionalNumber(
  values: Map<string, string>,
  name: string
): number | undefined {
  const raw = values.get(name);
  if (raw === undefined) {
    return;
  }

  const value = Number(raw);
  if (!Number.isFinite(value)) {
    throw new Error(`--${name} must be a finite number`);
  }

  return value;
}

function toWsBaseUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString().replace(TRAILING_SLASH, "");
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

function closeSocket(ws: WebSocket | undefined): Promise<void> {
  return new Promise((resolve) => {
    if (!ws || ws.readyState === WebSocket.CLOSED) {
      resolve();
      return;
    }

    ws.once("close", () => resolve());
    ws.close();
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

await main();
