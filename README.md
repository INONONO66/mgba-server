# mGBA Multi-Instance Gateway

A high-performance Rust/Axum gateway for running multiple mGBA Game Boy Advance emulator instances concurrently, with real-time WebSocket streaming.

## Architecture

- **Gateway** (`src/bin/gateway.rs`): Axum HTTP/WebSocket server
- **Dashboard** (`dashboard/`): separate React/Vite client for `/ws/dashboard`
- **Worker** (`src/bin/worker.rs`): libretro worker process (one per instance)
- **IPC**: Unix socket communication between gateway and workers
- **Streaming**: Binary WebSocket protocol with zlib-compressed tile deltas

## Build

### Prerequisites

- Rust toolchain (stable, 1.75+)
- `mgba_libretro.so` — build from mGBA source:
  ```bash
  git clone https://github.com/mgba-emu/mgba
  cd mgba
  cmake -S . -B build -DBUILD_LIBRETRO=ON -DSKIP_LIBRARY=ON -DBUILD_QT=OFF -DBUILD_SDL=OFF
  cmake --build build
  # Result: build/mgba_libretro.so
  ```

### Compile

```bash
cargo build --release
# Produces: target/release/gateway, target/release/worker
```

## Running

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `BIND_HOST` | `127.0.0.1` | HTTP bind host; set explicitly for network exposure |
| `PORT` | `8787` | HTTP server port |
| `ADMIN_TOKEN` | `dev-admin-token` | Admin API token |
| `MAX_INSTANCES` | `20` | Maximum concurrent instances |
| `WORKER_BINARY_PATH` | `./target/debug/worker` | Path to worker binary |
| `LIBRETRO_CORE_PATH` | `` | Path to `mgba_libretro.so` |
| `WORKER_SOCKET_DIR` | `/tmp/mgba-workers` | Directory for worker Unix sockets |
| `WORKER_SHUTDOWN_TIMEOUT_MS` | `2000` | Worker shutdown grace period (ms) |
| `H264_ENABLED` | `false` | Enable H.264-over-WebSocket |
| `CAPTURE_INTERVAL_MS` | `8` | Stream frame cadence (ms) |
| `STREAM_KEYFRAME_INTERVAL` | `60` | Frames between forced keyframes |
| `STREAM_TILE_SIZE` | `16` | Tile edge size for delta encoding |
| `WS_BACKPRESSURE_LIMIT` | `262144` | WebSocket backpressure limit (bytes) |
| `ROM_PATH` | `` | Default ROM path for new instances |

### Start the gateway

```bash
export ADMIN_TOKEN=your-secret-token
export LIBRETRO_CORE_PATH=/path/to/mgba_libretro.so
export WORKER_BINARY_PATH=./target/release/worker
./target/release/gateway
```

### Health check

```bash
curl http://localhost:8787/health
# {"ok":true}
```

### Start the dashboard

The gateway is API/WebSocket only; it does not embed or serve dashboard HTML. Run the React dashboard as a separate deployable app:

```bash
cd dashboard
bun install
VITE_GATEWAY_URL=http://127.0.0.1:8787 bun run dev
```

For production:

```bash
cd dashboard
VITE_GATEWAY_URL=https://gateway.example.com bun run build
```

The built files are emitted to `dashboard/dist/` and can be hosted by any static web server. The dashboard connects to the gateway over `/ws/dashboard`.

## ROM Placement

Place ROM files anywhere accessible to the gateway process and set `ROM_PATH` to the file path, or pass the path when creating an instance via the admin API. The worker process receives the ROM path over IPC at startup.

## API

### Authentication

- **Admin routes**: `X-Admin-Token: <token>` header
- **Session routes**: `X-Principal-Token: <token>` or `Authorization: Bearer <token>`

### Admin Routes

| Method | Path | Description |
|---|---|---|
| `POST` | `/admin/instances` | Create instance |
| `GET` | `/admin/instances` | List instances |
| `GET` | `/admin/instances/:id` | Get instance |
| `DELETE` | `/admin/instances/:id` | Destroy instance |
| `GET` | `/admin/metrics/streams` | Stream metrics |

### Session Routes

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/sessions/:id/core/currentframe` | Current frame number |
| `GET` | `/api/sessions/:id/core/read8?address=` | Read 1 byte |
| `GET` | `/api/sessions/:id/core/read16?address=` | Read 2 bytes |
| `GET` | `/api/sessions/:id/core/readrange?address=&length=` | Read byte range |
| `POST` | `/api/sessions/:id/core/write8?address=&value=` | Write 1 byte |
| `POST` | `/api/sessions/:id/core/write16?address=&value=` | Write 2 bytes |
| `POST` | `/api/sessions/:id/core/write32?address=&value=` | Write 4 bytes |
| `POST` | `/api/sessions/:id/mgba-http/button/tap?button=` | Tap button |
| `POST` | `/api/sessions/:id/mgba-http/button/hold?button=&duration=` | Hold button |
| `POST` | `/api/sessions/:id/core/savestateslot?slot=` | Save state |
| `POST` | `/api/sessions/:id/core/loadstateslot?slot=` | Load state |
| `POST` | `/api/sessions/:id/core/reset` | Reset emulator |
| `POST` | `/api/sessions/:id/core/screenshot` | Take screenshot (PNG) |
| `GET` | `/api/sessions/:id/screenshot` | Take screenshot (PNG, alias) |

### WebSocket Endpoints

| Path | Description |
|---|---|
| `/ws/dashboard` | All-instance stream (no auth) |
| `/ws/sessions/:id/stream` | Per-instance stream (principal token) |
| `/ws/sessions/:id/input-log` | Input event log (principal token) |

## Stream Protocol

Dashboard and per-instance WebSocket clients receive binary `pss-mgba-stream` frames.

See `crates/streaming/STREAM_PROTOCOL_SPEC.md` for the full wire format specification.

### Frame header (34 bytes)

| Offset | Field | Size | Notes |
|---:|---|---:|---|
| 0 | magic | 4 | ASCII `PSMG` |
| 4 | format | 1 | Must be `2` |
| 5 | frameType | 1 | See frame types below |
| 6 | instanceIndex | 1 | `instanceIndex % 256` |
| 7 | flags | 1 | `0x01` = deflate_raw payload |
| 8 | sequence | 4 | `u32be` per-instance sequence |
| 12 | timestampMs | 4 | `u32be` ms modulo 2^32 |
| 16 | width | 2 | `u16be` frame width |
| 18 | height | 2 | `u16be` frame height |
| 20 | tileSize | 2 | `u16be` tile edge size |
| 22 | rawBytes | 4 | `u32be` uncompressed RGBA size |
| 26 | payloadBytes | 4 | `u32be` compressed payload size |
| 30 | metadataBytes | 4 | `u32be` metadata JSON size |

### Frame types

- `1` — Keyframe: full frame, zlib deflate_raw compressed RGBA
- `2` — Delta: changed tiles only, zlib deflate_raw compressed
- `3` — H.264 NAL units (when `H264_ENABLED=true`)

### Viewer control messages (JSON text, max 4096 bytes)

- `{"type":"keyframe"}` — request a new keyframe
- `{"type":"client-metrics","metrics":{...}}` — report client-side metrics

## H.264 Mode

Enable H.264-over-WebSocket alongside tile deltas:

```bash
export H264_ENABLED=true
./target/release/gateway
```

H.264 frames use `frameType=3` in the binary header. Clients that only handle types 1/2 will ignore type 3 frames. New clients can decode H.264 NAL units directly — no container, no Annex B start codes.

## Performance Targets

- 20 concurrent instances sustained for 60 seconds
- p95 FPS >= 60 per instance
- Dropped/late frames <= 1%
- Total RAM <= 16 GiB (gateway + all workers)

## Benchmark

Run sustained-load validation against a release-built Rust gateway with a real
libretro core and test ROM available to the worker process:

```bash
export ADMIN_TOKEN=your-token
export LIBRETRO_CORE_PATH=/path/to/mgba_libretro.so
export ROM_PATH=/path/to/test.gba
export WORKER_BINARY_PATH=./target/release/worker
./target/release/gateway
```

Strict benchmark evidence must use the production HTTP/WebSocket paths and a
real ROM-backed instance set. Local compatibility checks without
`LIBRETRO_CORE_PATH` and `ROM_PATH` do not prove the 20-instance sustained
performance target.

Strict acceptance settings:

- Use exactly 20 instances for the measured run.
- Measure for at least 60000 ms after any warmup period.
- Count late frames and sequence gaps against the dropped/late-frame target.
- Treat reduced-target or compatibility-only runs as local development checks,
  not strict performance evidence.

## Testing

```bash
cargo test                    # All unit + integration tests
cargo test --test integration # Integration tests only
cargo clippy -- -D warnings   # Lint
cargo build --release         # Release build
```
