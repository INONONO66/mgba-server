import { describe, expect, test } from "bun:test";
import { dashboardWebSocketUrl, parseStreamFrame } from "./stream";

describe("dashboardWebSocketUrl", () => {
  test("builds dashboard websocket URL from an explicit gateway origin", () => {
    expect(dashboardWebSocketUrl("http://127.0.0.1:8787")).toBe(
      "ws://127.0.0.1:8787/ws/dashboard",
    );
  });

  test("preserves secure websocket scheme for HTTPS gateways", () => {
    expect(dashboardWebSocketUrl("https://play.example.test/base/")).toBe(
      "wss://play.example.test/ws/dashboard",
    );
  });
});

describe("parseStreamFrame", () => {
  test("parses the PSMG header and payload boundaries", () => {
    const header = new Uint8Array(38);
    header.set([0x50, 0x53, 0x4d, 0x47], 0);
    header[4] = 2;
    header[5] = 1;
    header[6] = 7;
    header[7] = 1;
    new DataView(header.buffer).setUint32(8, 42);
    new DataView(header.buffer).setUint16(16, 160);
    new DataView(header.buffer).setUint16(18, 144);
    new DataView(header.buffer).setUint16(20, 16);
    new DataView(header.buffer).setUint32(22, 160 * 144 * 4);
    new DataView(header.buffer).setUint32(26, 4);
    new DataView(header.buffer).setUint32(30, 0);
    header.set([1, 2, 3, 4], 34);

    const frame = parseStreamFrame(header.buffer);

    expect(frame.kind).toBe("keyframe");
    expect(frame.instanceIndex).toBe(7);
    expect(frame.width).toBe(160);
    expect(frame.height).toBe(144);
    expect(frame.payload).toEqual(new Uint8Array([1, 2, 3, 4]));
    expect(frame.isDeflated).toBe(true);
  });
});
