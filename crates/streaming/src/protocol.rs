use flate2::{Compression, write::DeflateEncoder};
use serde::{Deserialize, Serialize};
use std::io::Write;

pub const STREAM_HEADER_SIZE: usize = 34;
pub const VIEWER_CONTROL_MAX_BYTES: usize = 4096;
pub const STREAM_FORMAT: u8 = 2;
pub const MAGIC: &[u8; 4] = b"PSMG";

const MAX_CLIENT_COUNTER: f64 = 10_000_000.0;
const MAX_CLIENT_FPS: f64 = 1000.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamFrameType {
    Keyframe = 1,
    Delta = 2,
    H264 = 3,
}

pub mod flags {
    pub const NONE: u8 = 0x00;
    pub const DEFLATE_RAW: u8 = 0x01;
    pub const H264_PAYLOAD: u8 = 0x02;
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamFrameMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causality: Option<CausalityMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_capture_started_at_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_captured_at_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CausalityMetadata {
    pub control_event_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_principal_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub button: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_completed_at_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_requested_at_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

pub struct EncodeParams {
    pub frame_type: StreamFrameType,
    pub instance_index: u8,
    pub sequence: u32,
    pub timestamp_ms: u32,
    pub width: u16,
    pub height: u16,
    pub tile_size: u16,
    pub raw_bytes: u32,
    pub metadata: StreamFrameMetadata,
    pub payload: Vec<u8>,
}

pub fn encode_stream_frame(params: EncodeParams) -> Vec<u8> {
    let metadata_json = if params.metadata.causality.is_some()
        || params.metadata.source_capture_started_at_ms.is_some()
        || params.metadata.source_captured_at_ms.is_some()
    {
        serde_json::to_vec(&params.metadata).unwrap_or_default()
    } else {
        Vec::new()
    };

    let payload_bytes = params.payload.len() as u32;
    let metadata_bytes = metadata_json.len() as u32;
    let mut buf = Vec::with_capacity(
        STREAM_HEADER_SIZE + metadata_json.len() + params.payload.len(),
    );

    buf.extend_from_slice(MAGIC);
    buf.push(STREAM_FORMAT);
    buf.push(params.frame_type as u8);
    buf.push(params.instance_index);
    buf.push(flags::DEFLATE_RAW);
    buf.extend_from_slice(&params.sequence.to_be_bytes());
    buf.extend_from_slice(&params.timestamp_ms.to_be_bytes());
    buf.extend_from_slice(&params.width.to_be_bytes());
    buf.extend_from_slice(&params.height.to_be_bytes());
    buf.extend_from_slice(&params.tile_size.to_be_bytes());
    buf.extend_from_slice(&params.raw_bytes.to_be_bytes());
    buf.extend_from_slice(&payload_bytes.to_be_bytes());
    buf.extend_from_slice(&metadata_bytes.to_be_bytes());
    buf.extend_from_slice(&metadata_json);
    buf.extend_from_slice(&params.payload);

    buf
}

pub fn encode_h264_frame(
    nal_data: Vec<u8>,
    instance_index: u8,
    sequence: u32,
    timestamp_ms: u32,
    width: u16,
    height: u16,
) -> Vec<u8> {
    let payload_bytes = nal_data.len() as u32;
    let mut buf = Vec::with_capacity(STREAM_HEADER_SIZE + nal_data.len());
    buf.extend_from_slice(MAGIC);
    buf.push(STREAM_FORMAT);
    buf.push(StreamFrameType::H264 as u8);
    buf.push(instance_index);
    buf.push(flags::H264_PAYLOAD);
    buf.extend_from_slice(&sequence.to_be_bytes());
    buf.extend_from_slice(&timestamp_ms.to_be_bytes());
    buf.extend_from_slice(&width.to_be_bytes());
    buf.extend_from_slice(&height.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // tileSize = 0 (not applicable)
    buf.extend_from_slice(&0u32.to_be_bytes()); // rawBytes = 0 (not applicable)
    buf.extend_from_slice(&payload_bytes.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes()); // metadataBytes = 0
    buf.extend_from_slice(&nal_data);
    buf
}

pub fn deflate_raw(data: &[u8]) -> Vec<u8> {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(1));
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

pub fn encode_keyframe(pixels: &[u8], width: u32, height: u32, pitch: u32) -> Vec<u8> {
    let mut rgba = Vec::with_capacity((width * height * 4) as usize);
    for row in 0..height {
        let row_start = (row * pitch) as usize;
        for col in 0..width {
            let px_start = row_start + (col as usize * 4);
            let b = pixels[px_start];
            let g = pixels[px_start + 1];
            let r = pixels[px_start + 2];
            rgba.push(r);
            rgba.push(g);
            rgba.push(b);
            rgba.push(255);
        }
    }
    deflate_raw(&rgba)
}

pub fn encode_delta(
    prev_pixels: &[u8],
    curr_pixels: &[u8],
    width: u32,
    height: u32,
    pitch: u32,
    tile_size: u32,
) -> Vec<u8> {
    let tiles_x = width.div_ceil(tile_size);
    let tiles_y = height.div_ceil(tile_size);
    let mut changed_tiles: Vec<(u32, u32, u32, u32, Vec<u8>)> = Vec::new();

    for ty in 0..tiles_y {
        for tx in 0..tiles_x {
            let tile_x = tx * tile_size;
            let tile_y = ty * tile_size;
            let tile_w = (tile_x + tile_size).min(width) - tile_x;
            let tile_h = (tile_y + tile_size).min(height) - tile_y;

            let mut changed = false;
            'outer: for row in 0..tile_h {
                let y = tile_y + row;
                for col in 0..tile_w {
                    let x = tile_x + col;
                    let px = (y * pitch + x * 4) as usize;
                    if prev_pixels[px..px + 4] != curr_pixels[px..px + 4] {
                        changed = true;
                        break 'outer;
                    }
                }
            }

            if changed {
                let mut tile_rgba = Vec::with_capacity((tile_w * tile_h * 4) as usize);
                for row in 0..tile_h {
                    let y = tile_y + row;
                    for col in 0..tile_w {
                        let x = tile_x + col;
                        let px = (y * pitch + x * 4) as usize;
                        let b = curr_pixels[px];
                        let g = curr_pixels[px + 1];
                        let r = curr_pixels[px + 2];
                        tile_rgba.push(r);
                        tile_rgba.push(g);
                        tile_rgba.push(b);
                        tile_rgba.push(255);
                    }
                }
                changed_tiles.push((tile_x, tile_y, tile_w, tile_h, tile_rgba));
            }
        }
    }

    let tile_count = changed_tiles.len() as u16;
    let mut raw = Vec::new();
    raw.extend_from_slice(&tile_count.to_be_bytes());
    for (x, y, w, h, rgba) in &changed_tiles {
        raw.extend_from_slice(&(*x as u16).to_be_bytes());
        raw.extend_from_slice(&(*y as u16).to_be_bytes());
        raw.extend_from_slice(&(*w as u16).to_be_bytes());
        raw.extend_from_slice(&(*h as u16).to_be_bytes());
        raw.extend_from_slice(rgba);
    }

    deflate_raw(&raw)
}

#[derive(Debug, Clone, PartialEq)]
pub enum ViewerControl {
    Keyframe,
    ClientMetrics(serde_json::Value),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("message too large")]
    TooLarge,
    #[error("invalid JSON: {0}")]
    InvalidJson(String),
    #[error("unknown message type: {0}")]
    UnknownType(String),
}

pub fn parse_viewer_control_message(bytes: &[u8]) -> Result<ViewerControl, ProtocolError> {
    if bytes.len() > VIEWER_CONTROL_MAX_BYTES {
        return Err(ProtocolError::TooLarge);
    }

    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| ProtocolError::InvalidJson(e.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| ProtocolError::InvalidJson("message must be an object".to_string()))?;
    let msg_type = object
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| ProtocolError::InvalidJson("missing type".to_string()))?;

    match msg_type {
        "keyframe" => Ok(ViewerControl::Keyframe),
        "client-metrics" => Ok(ViewerControl::ClientMetrics(sanitize_client_metrics(
            object.get("metrics"),
        ))),
        other => Err(ProtocolError::UnknownType(other.to_string())),
    }
}

pub fn decode_stream_frame(bytes: &[u8]) -> Result<(StreamFrameMetadata, Vec<u8>), ProtocolError> {
    if bytes.len() < STREAM_HEADER_SIZE {
        return Err(ProtocolError::InvalidJson("frame too short".to_string()));
    }
    if &bytes[0..4] != MAGIC {
        return Err(ProtocolError::InvalidJson("invalid magic".to_string()));
    }
    if bytes[4] != STREAM_FORMAT {
        return Err(ProtocolError::InvalidJson("invalid format".to_string()));
    }

    let payload_bytes = u32::from_be_bytes(bytes[26..30].try_into().unwrap()) as usize;
    let metadata_bytes = u32::from_be_bytes(bytes[30..34].try_into().unwrap()) as usize;
    if bytes.len() != STREAM_HEADER_SIZE + metadata_bytes + payload_bytes {
        return Err(ProtocolError::InvalidJson("length mismatch".to_string()));
    }

    let metadata_start = STREAM_HEADER_SIZE;
    let payload_start = metadata_start + metadata_bytes;
    let metadata = if metadata_bytes > 0 {
        serde_json::from_slice(&bytes[metadata_start..payload_start]).unwrap_or_default()
    } else {
        StreamFrameMetadata::default()
    };
    let payload = bytes[payload_start..].to_vec();

    Ok((metadata, payload))
}

fn sanitize_client_metrics(value: Option<&serde_json::Value>) -> serde_json::Value {
    let Some(metrics) = value.and_then(|value| value.as_object()) else {
        return serde_json::Value::Object(Default::default());
    };

    let mut sanitized = serde_json::Map::new();
    for (key, max_value) in [
        ("decodedFrames", MAX_CLIENT_COUNTER),
        ("droppedFrames", MAX_CLIENT_COUNTER),
        ("fps", MAX_CLIENT_FPS),
        ("keyframeRecoveries", MAX_CLIENT_COUNTER),
        ("reconnects", MAX_CLIENT_COUNTER),
        ("renderedFrames", MAX_CLIENT_COUNTER),
        ("sequenceGaps", MAX_CLIENT_COUNTER),
    ] {
        if let Some(number) = metrics.get(key).and_then(|value| value.as_f64()) {
            if number.is_finite() && number >= 0.0 {
                sanitized.insert(key.to_string(), serde_json::Value::from(number.min(max_value)));
            }
        }
    }

    serde_json::Value::Object(sanitized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_xrgb_frame(width: u32, height: u32, color: u32) -> Vec<u8> {
        let b = (color & 0xFF) as u8;
        let g = ((color >> 8) & 0xFF) as u8;
        let r = ((color >> 16) & 0xFF) as u8;
        let pixel = [b, g, r, 0xFF];
        pixel.repeat((width * height) as usize)
    }

    fn inflate(data: &[u8]) -> Vec<u8> {
        use flate2::read::DeflateDecoder;
        use std::io::Read;

        let mut decoder = DeflateDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        decompressed
    }

    #[test]
    fn header_size_is_34() {
        assert_eq!(STREAM_HEADER_SIZE, 34);
    }

    #[test]
    fn encode_known_frame_matches_wire_format_spec() {
        let payload = vec![0x01, 0x02, 0x03];
        let encoded = encode_stream_frame(EncodeParams {
            frame_type: StreamFrameType::Keyframe,
            instance_index: 260_u16 as u8,
            sequence: 0x0102_0304,
            timestamp_ms: 0x0506_0708,
            width: 0x0100,
            height: 0x00a0,
            tile_size: 0x0010,
            raw_bytes: 0x0002_5800,
            metadata: StreamFrameMetadata::default(),
            payload,
        });

        let expected = vec![
            b'P', b'S', b'M', b'G', 0x02, 0x01, 0x04, 0x01, 0x01, 0x02, 0x03, 0x04,
            0x05, 0x06, 0x07, 0x08, 0x01, 0x00, 0x00, 0xa0, 0x00, 0x10, 0x00, 0x02,
            0x58, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
            0x03,
        ];
        assert_eq!(encoded, expected);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let payload = deflate_raw(b"test payload");
        let params = EncodeParams {
            frame_type: StreamFrameType::Keyframe,
            instance_index: 0,
            sequence: 42,
            timestamp_ms: 1000,
            width: 240,
            height: 160,
            tile_size: 16,
            raw_bytes: 240 * 160 * 4,
            metadata: StreamFrameMetadata::default(),
            payload: payload.clone(),
        };
        let encoded = encode_stream_frame(params);
        assert_eq!(&encoded[0..4], b"PSMG");
        assert_eq!(encoded[4], STREAM_FORMAT);
        assert_eq!(encoded[5], StreamFrameType::Keyframe as u8);
        assert_eq!(encoded[7], flags::DEFLATE_RAW);
        assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 42);

        let (meta, decoded_payload) = decode_stream_frame(&encoded).unwrap();
        assert_eq!(decoded_payload, payload);
        assert!(meta.causality.is_none());
    }

    #[test]
    fn keyframe_encoding_produces_valid_deflate() {
        let pixels = make_xrgb_frame(240, 160, 0xFF0000);
        let compressed = encode_keyframe(&pixels, 240, 160, 240 * 4);
        assert!(!compressed.is_empty());
        let decompressed = inflate(&compressed);
        assert_eq!(decompressed.len(), 240 * 160 * 4);
        assert_eq!(decompressed[0], 255);
        assert_eq!(decompressed[1], 0);
        assert_eq!(decompressed[2], 0);
        assert_eq!(decompressed[3], 255);
    }

    #[test]
    fn delta_encoding_detects_changed_tiles() {
        let prev = make_xrgb_frame(240, 160, 0x000000);
        let mut curr = prev.clone();
        curr[0] = 0xFF;
        let delta = encode_delta(&prev, &curr, 240, 160, 240 * 4, 16);
        assert!(!delta.is_empty());
        let raw = inflate(&delta);
        let tile_count = u16::from_be_bytes(raw[0..2].try_into().unwrap());
        assert_eq!(tile_count, 1);
    }

    #[test]
    fn delta_encoding_empty_frame_is_valid() {
        let prev = make_xrgb_frame(4, 4, 0x000000);
        let delta = encode_delta(&prev, &prev, 4, 4, 4 * 4, 16);
        assert_eq!(inflate(&delta), vec![0, 0]);
    }

    #[test]
    fn metadata_serializes_as_camel_case_sidecar() {
        let encoded = encode_stream_frame(EncodeParams {
            frame_type: StreamFrameType::Delta,
            instance_index: 1,
            sequence: 2,
            timestamp_ms: 3,
            width: 4,
            height: 5,
            tile_size: 6,
            raw_bytes: 80,
            metadata: StreamFrameMetadata {
                causality: None,
                source_capture_started_at_ms: Some(12.0),
                source_captured_at_ms: Some(34.0),
            },
            payload: vec![9],
        });
        let metadata_bytes = u32::from_be_bytes(encoded[30..34].try_into().unwrap()) as usize;
        let metadata = std::str::from_utf8(&encoded[34..34 + metadata_bytes]).unwrap();
        assert_eq!(
            metadata,
            r#"{"sourceCaptureStartedAtMs":12.0,"sourceCapturedAtMs":34.0}"#
        );
    }

    #[test]
    fn viewer_control_keyframe_parsed() {
        let msg = br#"{"type":"keyframe"}"#;
        let ctrl = parse_viewer_control_message(msg).unwrap();
        assert_eq!(ctrl, ViewerControl::Keyframe);
    }

    #[test]
    fn viewer_control_client_metrics_sanitized() {
        let msg = br#"{"type":"client-metrics","metrics":{"fps":2000,"decodedFrames":2,"droppedFrames":-1,"ignored":7}}"#;
        let ctrl = parse_viewer_control_message(msg).unwrap();
        assert_eq!(
            ctrl,
            ViewerControl::ClientMetrics(serde_json::json!({"decodedFrames":2.0,"fps":1000.0}))
        );
    }

    #[test]
    fn viewer_control_too_large_rejected() {
        let msg = vec![b'x'; VIEWER_CONTROL_MAX_BYTES + 1];
        assert!(parse_viewer_control_message(&msg).is_err());
    }

    #[test]
    fn h264_frame_type_is_3() {
        assert_eq!(StreamFrameType::H264 as u8, 3);
    }

    #[test]
    fn h264_payload_flag_is_0x02() {
        assert_eq!(flags::H264_PAYLOAD, 0x02);
    }

    #[test]
    fn encode_h264_frame_has_correct_header() {
        let nal = vec![0u8; 100];
        let encoded = encode_h264_frame(nal.clone(), 0, 1, 1000, 240, 160);
        assert_eq!(&encoded[0..4], b"PSMG");
        assert_eq!(encoded[4], STREAM_FORMAT);
        assert_eq!(encoded[5], 3); // frameType = H264
        assert_eq!(encoded[6], 0); // instance_index
        assert_eq!(encoded[7], flags::H264_PAYLOAD); // flags
        assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 1); // sequence
        assert_eq!(u32::from_be_bytes(encoded[12..16].try_into().unwrap()), 1000); // timestamp_ms
        assert_eq!(u16::from_be_bytes(encoded[16..18].try_into().unwrap()), 240); // width
        assert_eq!(u16::from_be_bytes(encoded[18..20].try_into().unwrap()), 160); // height
        assert_eq!(u16::from_be_bytes(encoded[20..22].try_into().unwrap()), 0); // tile_size
        assert_eq!(u32::from_be_bytes(encoded[22..26].try_into().unwrap()), 0); // raw_bytes
        assert_eq!(u32::from_be_bytes(encoded[26..30].try_into().unwrap()), 100); // payloadBytes
        assert_eq!(u32::from_be_bytes(encoded[30..34].try_into().unwrap()), 0); // metadataBytes
        assert_eq!(encoded.len(), STREAM_HEADER_SIZE + 100);
        assert_eq!(&encoded[STREAM_HEADER_SIZE..], &nal[..]);
    }

    #[test]
    fn encode_h264_frame_preserves_nal_payload() {
        let nal: Vec<u8> = (0..50).collect();
        let encoded = encode_h264_frame(nal.clone(), 7, 42, 2000, 320, 240);
        assert_eq!(encoded[6], 7); // instance_index preserved
        assert_eq!(&encoded[STREAM_HEADER_SIZE..], &nal[..]);
    }

    #[test]
    fn magic_constant_is_psmg() {
        assert_eq!(MAGIC, b"PSMG");
    }

    #[test]
    fn frame_type_discriminants() {
        assert_eq!(StreamFrameType::Keyframe as u8, 1);
        assert_eq!(StreamFrameType::Delta as u8, 2);
        assert_eq!(StreamFrameType::H264 as u8, 3);
    }

    #[test]
    fn deflate_raw_flag_is_0x01() {
        assert_eq!(flags::DEFLATE_RAW, 0x01);
    }

    #[test]
    fn decode_rejects_empty_bytes() {
        let result = decode_stream_frame(&[]);
        assert!(result.is_err(), "expected error for empty input");
    }

    #[test]
    fn decode_rejects_non_magic_buffer() {
        let result = decode_stream_frame(b"JPEG-ish");
        assert!(result.is_err(), "expected error for non-magic input");
    }

    #[test]
    fn decode_rejects_truncated_frame() {
        let encoded = encode_stream_frame(EncodeParams {
            frame_type: StreamFrameType::Delta,
            instance_index: 0,
            sequence: 1,
            timestamp_ms: 1,
            width: 1,
            height: 1,
            tile_size: 16,
            raw_bytes: 4,
            metadata: StreamFrameMetadata::default(),
            payload: vec![1, 2],
        });
        let truncated = &encoded[..encoded.len() - 1];
        assert!(
            decode_stream_frame(truncated).is_err(),
            "expected error for truncated frame"
        );
    }

    #[test]
    fn metadata_roundtrip_with_causality() {
        let metadata = StreamFrameMetadata {
            causality: Some(CausalityMetadata {
                control_event_id: "event-1".to_string(),
                request_id: Some("request-1".to_string()),
                input_requested_at_ms: Some(100.0),
                input_completed_at_ms: Some(120.0),
                input_latency_ms: Some(20.0),
                action: None,
                actor_principal_id: None,
                button: None,
                source: None,
            }),
            source_capture_started_at_ms: None,
            source_captured_at_ms: Some(123.0),
        };
        let encoded = encode_stream_frame(EncodeParams {
            frame_type: StreamFrameType::Delta,
            instance_index: 1,
            sequence: 43,
            timestamp_ms: 456,
            width: 240,
            height: 160,
            tile_size: 16,
            raw_bytes: 240 * 160 * 4,
            metadata,
            payload: vec![9, 8, 7],
        });
        let (decoded_meta, decoded_payload) = decode_stream_frame(&encoded).unwrap();
        assert_eq!(decoded_payload, vec![9, 8, 7]);
        let causality = decoded_meta.causality.expect("causality preserved");
        assert_eq!(causality.control_event_id, "event-1");
        assert_eq!(causality.request_id.as_deref(), Some("request-1"));
        assert_eq!(causality.input_latency_ms, Some(20.0));
        assert_eq!(decoded_meta.source_captured_at_ms, Some(123.0));
    }

    #[test]
    fn viewer_control_unknown_type_rejected() {
        let msg = br#"{"type":"unknown-type-here"}"#;
        let result = parse_viewer_control_message(msg);
        assert!(matches!(result, Err(ProtocolError::UnknownType(_))));
    }

    #[test]
    fn viewer_control_invalid_json_rejected() {
        let msg = b"not json at all";
        let result = parse_viewer_control_message(msg);
        assert!(matches!(result, Err(ProtocolError::InvalidJson(_))));
    }

    #[test]
    fn viewer_control_missing_type_rejected() {
        let msg = br#"{"foo":"bar"}"#;
        let result = parse_viewer_control_message(msg);
        assert!(matches!(result, Err(ProtocolError::InvalidJson(_))));
    }

    #[test]
    fn encode_header_size_matches_constant() {
        let encoded = encode_stream_frame(EncodeParams {
            frame_type: StreamFrameType::Keyframe,
            instance_index: 0,
            sequence: 1,
            timestamp_ms: 1,
            width: 1,
            height: 1,
            tile_size: 16,
            raw_bytes: 4,
            metadata: StreamFrameMetadata::default(),
            payload: vec![1, 2, 3, 4],
        });
        assert_eq!(encoded.len(), STREAM_HEADER_SIZE + 4);
        assert_eq!(&encoded[0..4], MAGIC);
    }
}
