# Streaming binary protocol spec

This document is the byte-accurate reference for the Rust streaming protocol implementation.
It must remain wire-compatible with the gateway and dashboard stream endpoints.

## Frame envelope

- Magic bytes: ASCII `PSMG` at offset `0` (`4` bytes)
- Header size: `34` bytes (`STREAM_HEADER_SIZE = 34`)
- Protocol format/version byte: `2`
- All multi-byte integer fields are **big-endian**

### Header layout

| Offset | Field | Length | Type / Endianness | Encoding / Notes |
|---:|---|---:|---|---|
| 0 | magic | 4 | ASCII | `PSMG` |
| 4 | format | 1 | `u8` | Must equal `2` (`STREAM_FORMAT`) |
| 5 | frameType | 1 | `u8` | `StreamFrameType` |
| 6 | instanceIndex | 1 | `u8` | `frame.instanceIndex % 256` |
| 7 | flags | 1 | `u8` | `StreamFrameFlags` bitmask |
| 8 | sequence | 4 | `u32be` | Per-instance sequence number |
| 12 | timestampMs | 4 | `u32be` | Milliseconds modulo `2^32` |
| 16 | width | 2 | `u16be` | Frame width |
| 18 | height | 2 | `u16be` | Frame height |
| 20 | tileSize | 2 | `u16be` | Tile edge size used by delta encoder |
| 22 | rawBytes | 4 | `u32be` | Uncompressed RGBA byte count |
| 26 | payloadBytes | 4 | `u32be` | Compressed payload byte count |
| 30 | metadataBytes | 4 | `u32be` | UTF-8 metadata JSON byte count |

## Enum values

### `StreamFrameType`

- `Keyframe = 1`
- `Delta = 2`

### `StreamFrameFlags`

- `None = 0x00`
- `DeflateRaw = 0x01`

Bit layout:

- bit 0 (`0x01`): payload is encoded with raw DEFLATE
- all other bits are reserved and currently zero

## Frame body layout

`encodeStreamFrame()` emits:

1. 34-byte fixed header
2. optional metadata JSON sidecar (`metadataBytes` bytes)
3. payload (`payloadBytes` bytes)

Decode validation rules:

- buffer length must be at least `34`
- magic must be `PSMG`
- format byte must be `2`
- total buffer length must equal `34 + metadataBytes + payloadBytes`
- metadata JSON is optional; empty metadata decodes as absent
- invalid metadata JSON is ignored by the decoder and treated as absent

## Payload formats

### Keyframe payload

Keyframes are raw deflate-compressed full RGBA frames at compression level 1.

### Delta payload

Delta payloads are raw deflate-compressed tile records at compression level 1 where:

1. `tileCount` is `u16be` changed-tile count
2. each tile record is:
   - `x` (`u16be`)
   - `y` (`u16be`)
   - `width` (`u16be`)
   - `height` (`u16be`)
   - raw RGBA bytes for that tile: `width * height * 4`

Tile comparison is row-wise over RGBA bytes (`4` bytes per pixel). A zero-tile delta is valid.

## Metadata JSON schema

The metadata sidecar is UTF-8 JSON produced by `JSON.stringify(metadata)`.
It is additive and optional.

### `StreamFrameMetadata`

```json
{
  "causality": {
    "action": "string",
    "actorPrincipalId": "string",
    "button": "string",
    "controlEventId": "string",
    "inputCompletedAtMs": 0,
    "inputLatencyMs": 0,
    "inputRequestedAtMs": 0,
    "requestId": "string",
    "source": "string"
  },
  "sourceCaptureStartedAtMs": 0,
  "sourceCapturedAtMs": 0
}
```

Rules:

- `sourceCaptureStartedAtMs` and `sourceCapturedAtMs` are optional numbers
- `causality` is optional
- if present, `causality.controlEventId` is required
- all other `causality` fields are optional
- JSON must remain UTF-8 encoded

### `StreamFrameCausalityMetadata`

Required:

- `controlEventId: string`

Optional:

- `action: string`
- `actorPrincipalId: string`
- `button: string`
- `inputCompletedAtMs: number`
- `inputLatencyMs: number`
- `inputRequestedAtMs: number`
- `requestId: string`
- `source: string`

## Viewer control message format

Viewer control messages are JSON text messages, not binary frames.
They must be `<= VIEWER_CONTROL_MAX_BYTES` (`4096`) bytes before parsing.

### Accepted messages

```json
{ "type": "keyframe" }
```

Exact compact form: `{"type":"keyframe"}`

```json
{
  "type": "client-metrics",
  "metrics": {
    "decodedFrames": 0,
    "droppedFrames": 0,
    "fps": 0,
    "keyframeRecoveries": 0,
    "reconnects": 0,
    "renderedFrames": 0,
    "sequenceGaps": 0
  }
}
```

Exact compact form: `{"type":"client-metrics",...}`

### Validation rules

- JSON must parse successfully
- parsed value must be an object
- `type` must exist
- `type = "keyframe"` is accepted with no extra payload
- `type = "client-metrics"` is accepted only with a metrics object (non-object metrics sanitize to `{}`)
- all metrics are sanitized as finite non-negative numbers only
- metric values are clamped:
  - `fps` max `1000`
  - all other metrics max `10_000_000`
- unknown `type` values are rejected
- messages larger than `4096` bytes are rejected

## Implementation notes

- `rawBytes` is the decoded raw RGBA size for the source frame
- `payloadBytes` is the compressed payload length only
- `metadataBytes` is the length of the UTF-8 JSON sidecar only
- `instanceIndex` is stored as a single byte and wraps modulo `256`
- delta payloads use tile-size derived from the capture pipeline (`STREAM_TILE_SIZE`)
