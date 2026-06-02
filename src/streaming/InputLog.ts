// biome-ignore-all lint/style/useFilenamingConvention: Existing multi modules use PascalCase filenames.
import { randomUUID } from "node:crypto";

export type InputAction = "button.tap" | "button.hold";
export type InputLogResult = "pending" | "success" | "error";

export interface InputLogEvent {
  action: InputAction;
  actorPrincipalId: string;
  button: string;
  completedAtMs?: number;
  duration?: string;
  error?: string;
  eventId: string;
  latencyMs?: number;
  requestId: string;
  requestedAtMs: number;
  result: InputLogResult;
  sessionId: string;
  source: "http" | "ws";
}

export interface StreamFrameCausality {
  action: InputAction;
  actorPrincipalId: string;
  button: string;
  controlEventId: string;
  inputCompletedAtMs?: number;
  inputLatencyMs?: number;
  inputRequestedAtMs: number;
  requestId: string;
  source: "http" | "ws";
}

type InputLogListener = (event: InputLogEvent) => void;

const MAX_RECENT_EVENTS_PER_SESSION = 100;

export class InputLogBus {
  private readonly listeners = new Map<string, Set<InputLogListener>>();
  private readonly pendingCausalityBySession = new Map<string, StreamFrameCausality[]>();
  private readonly recentEventsBySession = new Map<string, InputLogEvent[]>();
  private readonly eventsById = new Map<string, InputLogEvent>();

  beginInput(params: {
    action: InputAction;
    actorPrincipalId: string;
    button: string;
    duration?: string;
    requestId?: string;
    sessionId: string;
    source: "http" | "ws";
  }): InputLogEvent {
    const event: InputLogEvent = {
      action: params.action,
      actorPrincipalId: params.actorPrincipalId,
      button: params.button,
      duration: params.duration,
      eventId: randomUUID(),
      requestId: params.requestId ?? randomUUID(),
      requestedAtMs: Date.now(),
      result: "pending",
      sessionId: params.sessionId,
      source: params.source,
    };

    this.eventsById.set(event.eventId, event);
    this.emit(event);
    return event;
  }

  completeInput(eventId: string): InputLogEvent | undefined {
    return this.finishInput(eventId, "success");
  }

  failInput(eventId: string, error: unknown): InputLogEvent | undefined {
    return this.finishInput(eventId, "error", error instanceof Error ? error.message : String(error));
  }

  consumePendingCausality(sessionId: string): StreamFrameCausality | undefined {
    const queue = this.pendingCausalityBySession.get(sessionId);
    const causality = queue?.shift();
    if (queue && queue.length === 0) {
      this.pendingCausalityBySession.delete(sessionId);
    }
    return causality;
  }

  consumePendingCausalityCompletedBefore(
    sessionId: string,
    sourceCaptureStartedAtMs: number
  ): StreamFrameCausality | undefined {
    const queue = this.pendingCausalityBySession.get(sessionId);
    if (!queue || queue.length === 0) {
      return undefined;
    }

    const causality = queue[0];
    if (
      causality.inputCompletedAtMs === undefined ||
      causality.inputCompletedAtMs > sourceCaptureStartedAtMs
    ) {
      return undefined;
    }

    queue.shift();
    if (queue.length === 0) {
      this.pendingCausalityBySession.delete(sessionId);
    }
    return causality;
  }

  recent(sessionId: string): InputLogEvent[] {
    return [...(this.recentEventsBySession.get(sessionId) ?? [])];
  }

  subscribe(sessionId: string, listener: InputLogListener): () => void {
    let listeners = this.listeners.get(sessionId);
    if (!listeners) {
      listeners = new Set<InputLogListener>();
      this.listeners.set(sessionId, listeners);
    }
    listeners.add(listener);

    return () => {
      listeners?.delete(listener);
      if (listeners?.size === 0) {
        this.listeners.delete(sessionId);
      }
    };
  }

  private finishInput(
    eventId: string,
    result: Exclude<InputLogResult, "pending">,
    error?: string
  ): InputLogEvent | undefined {
    const existing = this.eventsById.get(eventId);
    if (!existing) {
      return undefined;
    }

    const completedAtMs = Date.now();
    const event: InputLogEvent = {
      ...existing,
      completedAtMs,
      error,
      latencyMs: completedAtMs - existing.requestedAtMs,
      result,
    };
    this.eventsById.set(eventId, event);
    if (result === "success") {
      this.pushPendingCausality(event);
    }
    this.emit(event);
    return event;
  }

  private pushPendingCausality(event: InputLogEvent): void {
    const queue = this.pendingCausalityBySession.get(event.sessionId) ?? [];
    queue.push(toCausality(event));
    this.pendingCausalityBySession.set(event.sessionId, queue);
  }

  private emit(event: InputLogEvent): void {
    const recent = this.recentEventsBySession.get(event.sessionId) ?? [];
    recent.push(event);
    if (recent.length > MAX_RECENT_EVENTS_PER_SESSION) {
      recent.splice(0, recent.length - MAX_RECENT_EVENTS_PER_SESSION);
    }
    this.recentEventsBySession.set(event.sessionId, recent);

    for (const listener of this.listeners.get(event.sessionId) ?? []) {
      listener(event);
    }
  }
}

function toCausality(event: InputLogEvent): StreamFrameCausality {
  return {
    action: event.action,
    actorPrincipalId: event.actorPrincipalId,
    button: event.button,
    controlEventId: event.eventId,
    inputCompletedAtMs: event.completedAtMs,
    inputLatencyMs: event.latencyMs,
    inputRequestedAtMs: event.requestedAtMs,
    requestId: event.requestId,
    source: event.source,
  };
}
