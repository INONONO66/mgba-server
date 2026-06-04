use crate::frame_hub::{PixelFormat, RawFrame};
use openh264::{
    OpenH264API,
    encoder::{Encoder, EncoderConfig},
    formats::YUVBuffer,
};

#[derive(Debug, thiserror::Error)]
pub enum H264Error {
    #[error("encoder init failed: {0}")]
    Init(String),
    #[error("unsupported pixel format: {0:?}")]
    UnsupportedPixelFormat(PixelFormat),
    #[error(
        "frame dimensions mismatch: expected {expected_width}x{expected_height}, got {actual_width}x{actual_height}"
    )]
    DimensionMismatch {
        expected_width: u32,
        expected_height: u32,
        actual_width: u32,
        actual_height: u32,
    },
    #[error("encode failed: {0}")]
    Encode(String),
}

pub struct H264Encoder {
    encoder: Encoder,
    width: u32,
    height: u32,
}

impl H264Encoder {
    pub fn new(width: u32, height: u32) -> Result<Self, H264Error> {
        let config = EncoderConfig::new().set_bitrate_bps(500_000).debug(false);
        let encoder = Encoder::with_api_config(OpenH264API::from_source(), config)
            .map_err(|e| H264Error::Init(e.to_string()))?;
        Ok(Self {
            encoder,
            width,
            height,
        })
    }

    pub fn encode(&mut self, frame: &RawFrame) -> Result<Vec<u8>, H264Error> {
        if frame.pixel_format != PixelFormat::XRGB8888 {
            return Err(H264Error::UnsupportedPixelFormat(frame.pixel_format));
        }

        if frame.width != self.width || frame.height != self.height {
            return Err(H264Error::DimensionMismatch {
                expected_width: self.width,
                expected_height: self.height,
                actual_width: frame.width,
                actual_height: frame.height,
            });
        }

        let yuv = xrgb8888_to_yuv420(&frame.data, frame.width, frame.height, frame.pitch);
        let mut yuv_data = Vec::with_capacity(yuv.y.len() + yuv.u.len() + yuv.v.len());
        yuv_data.extend_from_slice(&yuv.y);
        yuv_data.extend_from_slice(&yuv.u);
        yuv_data.extend_from_slice(&yuv.v);
        let yuv_buf = YUVBuffer::from_vec(yuv_data, frame.width as usize, frame.height as usize);

        let bitstream = self
            .encoder
            .encode(&yuv_buf)
            .map_err(|e| H264Error::Encode(e.to_string()))?;

        let mut nal_data = Vec::new();
        bitstream.write_vec(&mut nal_data);

        Ok(nal_data)
    }
}

struct YuvPlanes {
    y: Vec<u8>,
    u: Vec<u8>,
    v: Vec<u8>,
}

fn xrgb8888_to_yuv420(data: &[u8], width: u32, height: u32, pitch: u32) -> YuvPlanes {
    let w = width as usize;
    let h = height as usize;
    let p = pitch as usize;

    let mut y = vec![0u8; w * h];
    let mut u = vec![128u8; (w / 2) * (h / 2)];
    let mut v = vec![128u8; (w / 2) * (h / 2)];

    for row in 0..h {
        for col in 0..w {
            let px = row * p + col * 4;
            if px + 3 >= data.len() {
                break;
            }

            let b = data[px] as f32;
            let g = data[px + 1] as f32;
            let r = data[px + 2] as f32;

            y[row * w + col] = (0.299 * r + 0.587 * g + 0.114 * b) as u8;

            if row % 2 == 0 && col % 2 == 0 {
                let uv_idx = (row / 2) * (w / 2) + (col / 2);
                u[uv_idx] = (-0.169 * r - 0.331 * g + 0.500 * b + 128.0).clamp(0.0, 255.0) as u8;
                v[uv_idx] = (0.500 * r - 0.419 * g - 0.081 * b + 128.0).clamp(0.0, 255.0) as u8;
            }
        }
    }

    YuvPlanes { y, u, v }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame_hub::{PixelFormat, RawFrame};
    use std::time::Instant;

    fn make_test_frame(width: u32, height: u32) -> RawFrame {
        let mut data = vec![0u8; (width * height * 4) as usize];
        for i in (0..data.len()).step_by(4) {
            data[i] = 0;
            data[i + 1] = 0;
            data[i + 2] = 255;
            data[i + 3] = 0;
        }
        RawFrame {
            width,
            height,
            pitch: width * 4,
            pixel_format: PixelFormat::XRGB8888,
            data,
            sequence: 1,
            timestamp_ms: 0,
        }
    }

    #[test]
    fn encode_produces_nal_units() {
        let mut encoder = H264Encoder::new(240, 160).expect("encoder init");
        let frame = make_test_frame(240, 160);
        let nal = encoder.encode(&frame).expect("encode");
        assert!(!nal.is_empty(), "NAL units should not be empty");
    }

    #[test]
    fn encode_latency_under_5ms() {
        let mut encoder = H264Encoder::new(240, 160).expect("encoder init");
        let frame = make_test_frame(240, 160);

        let _ = encoder.encode(&frame);

        let start = Instant::now();
        for _ in 0..10 {
            let _ = encoder.encode(&frame).expect("encode");
        }
        let avg_ms = start.elapsed().as_millis() as f64 / 10.0;
        assert!(
            avg_ms < 5.0,
            "Average encode time {avg_ms}ms should be < 5ms"
        );
    }

    #[test]
    fn yuv420_conversion_correct_dimensions() {
        let frame = make_test_frame(240, 160);
        let yuv = xrgb8888_to_yuv420(&frame.data, 240, 160, 240 * 4);
        assert_eq!(yuv.y.len(), 240 * 160);
        assert_eq!(yuv.u.len(), 120 * 80);
        assert_eq!(yuv.v.len(), 120 * 80);
    }
}
