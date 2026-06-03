// biome-ignore-all lint/style/useFilenamingConvention: Existing multi modules use PascalCase filenames.
import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { getRequestListener } from "@hono/node-server";
import { Hono } from "hono";
import { logger } from "hono/logger";
import { WebSocketServer } from "ws";

import type { Config } from "../config.js";
import { DashboardBroadcast } from "../streaming/DashboardBroadcast.js";
import { FrameCapture } from "../streaming/FrameCapture.js";
import { InputLogBus } from "../streaming/InputLog.js";
import { StreamMetrics } from "../streaming/StreamMetrics.js";
import { createAdminRouter, type IInstanceManager } from "./AdminRouter.js";
import { createApiRouter, type InstanceRegistry } from "./ApiRouter.js";

export interface GatewayServer {
  httpServer: ReturnType<typeof createServer>;
  start(): void;
  stop(): Promise<void>;
  wss: WebSocketServer;
}

export function createGatewayServer(
  config: Config,
  registry: InstanceRegistry,
  instanceManager: IInstanceManager
): GatewayServer {
  const app = new Hono();

  app.use("*", logger());
  app.get("/health", (c) => c.json({ status: "ok" }));
  const dashboardHtml = loadDashboardHtml();
  app.get("/", (c) => c.html(dashboardHtml));

  const streamMetrics = new StreamMetrics();
  const inputLog = new InputLogBus();
  const frameCapture = new FrameCapture(
    registry,
    config.captureIntervalMs,
    config.sourceCaptureIntervalMs,
    config.streamKeyframeInterval,
    config.streamTileSize,
    { inputLog }
  );

  app.route(
    "/admin",
    createAdminRouter(config, registry, instanceManager, { streamMetrics })
  );
  const inputLogOptions = {
    inputLog,
    onInputCompleted: (principalToken: string) => frameCapture.forceKeyframe(principalToken),
  };
  app.route("/api/sessions/:sessionId", createApiRouter(registry, inputLogOptions));

  const httpServer = createServer(getRequestListener(app.fetch));
  const wss = new WebSocketServer({ server: httpServer });
  const broadcast = new DashboardBroadcast(
    wss,
    registry,
    config.wsBackpressureLimit,
    streamMetrics,
    { inputLog, requestKeyframe: (principalToken) => frameCapture.forceKeyframe(principalToken) }
  );
  frameCapture.onFrame((frame) => {
    streamMetrics.recordProduced(frame);
    broadcast.broadcastFrame(frame);
  });

  return {
    httpServer,
    wss,
    start() {
      frameCapture.start();
      httpServer.listen(config.port, () => {
        console.log(
          `Gateway server running on http://localhost:${config.port}`
        );
      });
    },
    stop() {
      frameCapture.stop();
      return new Promise<void>((resolvePromise, reject) => {
        wss.close(() => {
          httpServer.close((error) => {
            if (error) {
              reject(error);
              return;
            }

            resolvePromise();
          });
        });
      });
    },
  };
}

function loadDashboardHtml(): string {
  const currentDir = dirname(fileURLToPath(import.meta.url));
  const htmlPath = resolve(currentDir, "..", "dashboard", "index.html");
  return readFileSync(htmlPath, "utf8");
}
