import {
  type Dispatch,
  type SetStateAction,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  type StreamFrame,
  dashboardWebSocketUrl,
  inflatePayload,
  parseStreamFrame,
} from "./stream";

const INSTANCE_COUNT = 20;
const NATIVE_WIDTH = 160;
const NATIVE_HEIGHT = 144;

type ConnectionState = "connected" | "connecting" | "disconnected" | "error";

type InstanceStatus = {
  readonly index: number;
  readonly fps: number;
  readonly frames: number;
  readonly live: boolean;
};

type Renderer = {
  canvas: HTMLCanvasElement;
  context: CanvasRenderingContext2D;
  frameTimestamps: number[];
};

export function App() {
  const configuredGateway =
    import.meta.env.VITE_GATEWAY_URL ?? "http://127.0.0.1:8787";
  const [gatewayInput, setGatewayInput] = useState(configuredGateway);
  const [gatewayUrl, setGatewayUrl] = useState(configuredGateway);
  const [connection, setConnection] = useState<ConnectionState>("disconnected");
  const [instances, setInstances] =
    useState<readonly InstanceStatus[]>(initialInstances);
  const renderers = useRef(new Map<number, Renderer>());
  const socketUrl = useMemo(
    () => dashboardWebSocketUrl(gatewayUrl),
    [gatewayUrl],
  );

  useEffect(() => {
    let closedByEffect = false;
    let reconnectTimer: number | undefined;

    const connect = () => {
      setConnection("connecting");
      const socket = new WebSocket(socketUrl);
      socket.binaryType = "arraybuffer";

      socket.onopen = () => {
        setConnection("connected");
        socket.send('{"type":"keyframe"}');
      };

      socket.onmessage = (event) => {
        void messageData(event)
          .then((buffer) =>
            renderFrame(buffer, renderers.current, setInstances),
          )
          .catch(reportRuntimeError);
      };

      socket.onerror = () => {
        setConnection("error");
      };

      socket.onclose = () => {
        if (closedByEffect) {
          return;
        }
        setConnection("disconnected");
        reconnectTimer = window.setTimeout(connect, 1200);
      };
    };

    connect();

    return () => {
      closedByEffect = true;
      if (reconnectTimer !== undefined) {
        window.clearTimeout(reconnectTimer);
      }
    };
  }, [socketUrl]);

  useEffect(() => {
    const interval = window.setInterval(() => {
      const now = performance.now();
      setInstances((current) =>
        current.map((status) => {
          const renderer = renderers.current.get(status.index);
          if (renderer === undefined) {
            return status;
          }
          const fps = framesSince(renderer.frameTimestamps, now, 1000);
          return { ...status, fps, live: fps > 0 };
        }),
      );
    }, 1000);

    return () => window.clearInterval(interval);
  }, []);

  const submitGateway = (event: { preventDefault: () => void }): void => {
    event.preventDefault();
    setInstances(initialInstances());
    setGatewayUrl(gatewayInput);
  };

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Grokemon</p>
          <h1>Multi-instance dashboard</h1>
        </div>
        <form className="gateway-form" onSubmit={submitGateway}>
          <label htmlFor="gateway-url">Gateway</label>
          <input
            id="gateway-url"
            value={gatewayInput}
            spellCheck={false}
            onChange={(event) => setGatewayInput(event.currentTarget.value)}
          />
          <button type="submit">Connect</button>
        </form>
        <div className={`status status-${connection}`}>
          <span aria-hidden="true" />
          {connection}
        </div>
      </header>

      <section className="metrics" aria-label="Stream metrics">
        <Metric label="WebSocket" value={socketUrl} />
        <Metric
          label="Live instances"
          value={String(instances.filter((item) => item.live).length)}
        />
        <Metric
          label="Rendered frames"
          value={String(instances.reduce((sum, item) => sum + item.frames, 0))}
        />
      </section>

      <section className="instance-grid" aria-label="Instances">
        {instances.map((instance) => (
          <article
            className={instance.live ? "instance live" : "instance"}
            key={instance.index}
          >
            <div className="screen">
              <canvas
                ref={(canvas) =>
                  registerCanvas(renderers.current, instance.index, canvas)
                }
                width={NATIVE_WIDTH}
                height={NATIVE_HEIGHT}
              />
              {!instance.live && (
                <div className="overlay">Instance {instance.index}</div>
              )}
            </div>
            <footer>
              <span>#{instance.index.toString().padStart(2, "0")}</span>
              <span>
                {instance.fps > 0 ? `${instance.fps} fps` : "waiting"}
              </span>
            </footer>
          </article>
        ))}
      </section>
    </main>
  );
}

function Metric(props: { readonly label: string; readonly value: string }) {
  return (
    <div className="metric">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}

function initialInstances(): readonly InstanceStatus[] {
  return Array.from({ length: INSTANCE_COUNT }, (_value, index) => ({
    index,
    fps: 0,
    frames: 0,
    live: false,
  }));
}

function registerCanvas(
  renderers: Map<number, Renderer>,
  index: number,
  canvas: HTMLCanvasElement | null,
): void {
  if (canvas === null) {
    renderers.delete(index);
    return;
  }
  const context = canvas.getContext("2d");
  if (context === null) {
    return;
  }
  renderers.set(index, { canvas, context, frameTimestamps: [] });
}

async function messageData(event: MessageEvent): Promise<ArrayBuffer> {
  const data: unknown = event.data;
  if (data instanceof ArrayBuffer) {
    return data;
  }
  if (data instanceof Blob) {
    return data.arrayBuffer();
  }
  throw new Error("dashboard stream sent a non-binary message");
}

async function renderFrame(
  buffer: ArrayBuffer,
  renderers: ReadonlyMap<number, Renderer>,
  setInstances: Dispatch<SetStateAction<readonly InstanceStatus[]>>,
): Promise<void> {
  const frame = parseStreamFrame(buffer);
  const renderer = renderers.get(frame.instanceIndex);
  if (renderer === undefined) {
    return;
  }

  switch (frame.kind) {
    case "keyframe":
      await renderKeyframe(frame, renderer);
      break;
    case "delta":
      await renderDelta(frame, renderer);
      break;
  }

  const now = performance.now();
  renderer.frameTimestamps.push(now);
  trimOldFrames(renderer.frameTimestamps, now, 2000);
  const fps = framesSince(renderer.frameTimestamps, now, 1000);
  setInstances((current) =>
    current.map((status) =>
      status.index === frame.instanceIndex
        ? {
            ...status,
            fps,
            frames: status.frames + 1,
            live: true,
          }
        : status,
    ),
  );
}

async function renderKeyframe(
  frame: StreamFrame,
  renderer: Renderer,
): Promise<void> {
  const rgba = await inflatePayload(frame);
  if (rgba.byteLength < frame.rawBytes) {
    throw new Error("keyframe payload is shorter than rawBytes");
  }
  if (
    renderer.canvas.width !== frame.width ||
    renderer.canvas.height !== frame.height
  ) {
    renderer.canvas.width = frame.width;
    renderer.canvas.height = frame.height;
  }
  const pixels = copyPixels(rgba, 0, frame.rawBytes);
  renderer.context.putImageData(
    new ImageData(pixels, frame.width, frame.height),
    0,
    0,
  );
}

async function renderDelta(
  frame: StreamFrame,
  renderer: Renderer,
): Promise<void> {
  const payload = await inflatePayload(frame);
  if (payload.byteLength < 2) {
    return;
  }

  const view = new DataView(
    payload.buffer,
    payload.byteOffset,
    payload.byteLength,
  );
  const tileCount = view.getUint16(0, false);
  let offset = 2;

  for (let tileIndex = 0; tileIndex < tileCount; tileIndex += 1) {
    if (offset + 8 > payload.byteLength) {
      return;
    }
    const x = view.getUint16(offset, false);
    const y = view.getUint16(offset + 2, false);
    const width = view.getUint16(offset + 4, false);
    const height = view.getUint16(offset + 6, false);
    offset += 8;

    const pixelBytes = width * height * 4;
    if (offset + pixelBytes > payload.byteLength) {
      return;
    }

    const pixels = copyPixels(payload, offset, pixelBytes);
    renderer.context.putImageData(new ImageData(pixels, width, height), x, y);
    offset += pixelBytes;
  }
}

function framesSince(
  frameTimestamps: readonly number[],
  now: number,
  windowMs: number,
): number {
  const cutoff = now - windowMs;
  return frameTimestamps.filter((timestamp) => timestamp >= cutoff).length;
}

function trimOldFrames(
  frameTimestamps: number[],
  now: number,
  windowMs: number,
): void {
  const cutoff = now - windowMs;
  while (
    frameTimestamps.length > 0 &&
    frameTimestamps[0] !== undefined &&
    frameTimestamps[0] < cutoff
  ) {
    frameTimestamps.shift();
  }
}

function reportRuntimeError(error: unknown): void {
  if (error instanceof Error) {
    console.warn(error.message);
    return;
  }
  console.warn("unknown dashboard stream error");
}

function copyPixels(
  source: Uint8Array,
  sourceOffset: number,
  length: number,
): Uint8ClampedArray<ArrayBuffer> {
  const result: Uint8ClampedArray<ArrayBuffer> = new Uint8ClampedArray(
    new ArrayBuffer(length),
  );
  result.set(source.subarray(sourceOffset, sourceOffset + length));
  return result;
}
