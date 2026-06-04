use crate::{
    frame_hub::{FrameHub, PixelFormat, RawFrame},
    h264::H264Encoder,
    metrics::StreamMetrics,
    protocol::{
        EncodeParams, StreamFrameMetadata, StreamFrameType, ViewerControl, deflate_raw,
        encode_delta, encode_h264_frame, encode_keyframe, encode_stream_frame,
        parse_viewer_control_message,
    },
};
use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, watch};

const KEYFRAME_INTERVAL: u32 = 60;
const BACKPRESSURE_LIMIT: usize = 262_144;
const KEYFRAME_THROTTLE_MS: u64 = 500;

#[derive(Clone)]
pub struct DashboardBroadcast {
    hub: Arc<FrameHub>,
    keyframe_cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    metrics: Arc<StreamMetrics>,
    config: BroadcastConfig,
}

#[derive(Clone)]
pub struct BroadcastConfig {
    pub keyframe_interval: u32,
    pub backpressure_limit: usize,
    pub tile_size: u16,
    pub h264_enabled: bool,
}

impl Default for BroadcastConfig {
    fn default() -> Self {
        Self {
            keyframe_interval: KEYFRAME_INTERVAL,
            backpressure_limit: BACKPRESSURE_LIMIT,
            tile_size: 16,
            h264_enabled: false,
        }
    }
}

impl DashboardBroadcast {
    pub fn new(hub: Arc<FrameHub>) -> Self {
        Self::with_config(hub, BroadcastConfig::default())
    }

    pub fn with_config(hub: Arc<FrameHub>, config: BroadcastConfig) -> Self {
        Self::with_config_and_metrics(hub, config, Arc::new(StreamMetrics::new()))
    }

    pub fn with_config_and_metrics(
        hub: Arc<FrameHub>,
        config: BroadcastConfig,
        metrics: Arc<StreamMetrics>,
    ) -> Self {
        Self {
            hub,
            keyframe_cache: Arc::new(RwLock::new(HashMap::new())),
            metrics,
            config,
        }
    }

    pub fn metrics(&self) -> &Arc<StreamMetrics> {
        &self.metrics
    }

    pub async fn handle_instance_stream(
        &self,
        socket: WebSocket,
        instance_id: String,
        instance_index: u8,
    ) {
        let Some(mut rx) = self.hub.subscribe(&instance_id).await else {
            return;
        };

        let (mut sender, mut receiver) = socket.split();
        let keyframe_cache = self.keyframe_cache.clone();
        let config = self.config.clone();
        let inst_id = instance_id.clone();

        {
            let cache = keyframe_cache.read().await;
            if let Some(kf) = cache.get(&instance_id)
                && send_binary_with_backpressure(&mut sender, kf.clone(), config.backpressure_limit)
                    .await
                    .is_err()
            {
                return;
            }
        }

        let mut prev_frame: Option<RawFrame> = None;
        let mut frame_count: u32 = 0;
        let mut recovery_mode = false;
        let mut h264_encoder: Option<H264Encoder> = None;

        let keyframe_cache_clone = keyframe_cache.clone();
        let inst_id_clone = inst_id.clone();
        tokio::spawn(async move {
            let mut last_keyframe_request =
                Instant::now() - Duration::from_millis(KEYFRAME_THROTTLE_MS);
            while let Some(Ok(msg)) = receiver.next().await {
                match msg {
                    Message::Binary(data) => {
                        handle_control_message(
                            &data,
                            &keyframe_cache_clone,
                            &inst_id_clone,
                            &mut last_keyframe_request,
                        )
                        .await;
                    }
                    Message::Text(text) => {
                        handle_control_message(
                            text.as_bytes(),
                            &keyframe_cache_clone,
                            &inst_id_clone,
                            &mut last_keyframe_request,
                        )
                        .await;
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        });

        loop {
            if rx.changed().await.is_err() {
                break;
            }
            let frame = rx.borrow_and_update().clone();
            let Some(frame) = frame else { continue };

            frame_count = frame_count.wrapping_add(1);
            let force_keyframe = frame_count % config.keyframe_interval == 1 || recovery_mode;
            let (frame_type, payload) = encode_frame_payload(
                &frame,
                prev_frame.as_ref(),
                force_keyframe,
                config.tile_size,
            );
            let encoded = encode_raw_frame(
                &frame,
                frame_type,
                payload,
                instance_index,
                config.tile_size,
            );

            if frame_type == StreamFrameType::Keyframe {
                keyframe_cache
                    .write()
                    .await
                    .insert(instance_id.clone(), encoded.clone());
                recovery_mode = false;
            }

            self.metrics
                .record_produced(
                    &instance_id,
                    encoded.len() as u64,
                    frame_type == StreamFrameType::Keyframe,
                )
                .await;
            match send_binary_with_backpressure(&mut sender, encoded, config.backpressure_limit)
                .await
            {
                Ok(true) => {
                    self.metrics
                        .record_delivery(&instance_id, true, false)
                        .await;
                    if config.h264_enabled {
                        send_h264_frame(
                            &mut sender,
                            &mut h264_encoder,
                            &frame,
                            instance_index,
                            config.backpressure_limit,
                        )
                        .await;
                    }
                    prev_frame = Some(frame);
                }
                Ok(false) => {
                    self.metrics
                        .record_delivery(&instance_id, false, false)
                        .await;
                    if frame_type == StreamFrameType::Delta {
                        recovery_mode = true;
                    }
                }
                Err(()) => break,
            }
        }
    }

    pub async fn handle_dashboard_stream(
        &self,
        socket: WebSocket,
        instance_ids: Vec<(String, u8)>,
    ) {
        let (mut sender, mut receiver) = socket.split();
        let keyframe_cache = self.keyframe_cache.clone();
        let config = self.config.clone();

        {
            let cache = keyframe_cache.read().await;
            for (id, _) in &instance_ids {
                if let Some(kf) = cache.get(id) {
                    match send_binary_with_backpressure(
                        &mut sender,
                        kf.clone(),
                        config.backpressure_limit,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(()) => return,
                    }
                }
            }
        }

        let mut receivers: Vec<(String, u8, watch::Receiver<Option<RawFrame>>)> = Vec::new();
        for (id, idx) in &instance_ids {
            if let Some(rx) = self.hub.subscribe(id).await {
                receivers.push((id.clone(), *idx, rx));
            }
        }

        if receivers.is_empty() {
            return;
        }

        let dashboard_cache = keyframe_cache.clone();
        tokio::spawn(async move {
            let mut last_keyframe_request =
                Instant::now() - Duration::from_millis(KEYFRAME_THROTTLE_MS);
            while let Some(Ok(msg)) = receiver.next().await {
                let bytes = match msg {
                    Message::Binary(data) => data.to_vec(),
                    Message::Text(text) => text.as_bytes().to_vec(),
                    Message::Close(_) => break,
                    _ => continue,
                };
                if let Ok(ViewerControl::Keyframe) = parse_viewer_control_message(&bytes)
                    && last_keyframe_request.elapsed()
                        >= Duration::from_millis(KEYFRAME_THROTTLE_MS)
                {
                    dashboard_cache.write().await.clear();
                    last_keyframe_request = Instant::now();
                }
            }
        });

        let mut prev_frames: HashMap<String, RawFrame> = HashMap::new();
        let mut frame_counts: HashMap<String, u32> = HashMap::new();
        let mut recovery_instances: HashSet<String> = HashSet::new();

        loop {
            let mut any_changed = false;
            for (id, idx, rx) in &mut receivers {
                if !rx.has_changed().unwrap_or(false) {
                    continue;
                }
                any_changed = true;
                let frame = rx.borrow_and_update().clone();
                let Some(frame) = frame else { continue };

                let count = frame_counts.entry(id.clone()).or_insert(0);
                *count = count.wrapping_add(1);
                let force_keyframe =
                    *count % config.keyframe_interval == 1 || recovery_instances.contains(id);
                let (frame_type, payload) = encode_frame_payload(
                    &frame,
                    prev_frames.get(id),
                    force_keyframe,
                    config.tile_size,
                );
                let encoded = encode_raw_frame(&frame, frame_type, payload, *idx, config.tile_size);

                if frame_type == StreamFrameType::Keyframe {
                    keyframe_cache
                        .write()
                        .await
                        .insert(id.clone(), encoded.clone());
                    recovery_instances.remove(id);
                }

                self.metrics
                    .record_produced(
                        id,
                        encoded.len() as u64,
                        frame_type == StreamFrameType::Keyframe,
                    )
                    .await;
                match send_binary_with_backpressure(&mut sender, encoded, config.backpressure_limit)
                    .await
                {
                    Ok(true) => {
                        self.metrics.record_delivery(id, true, true).await;
                        prev_frames.insert(id.clone(), frame);
                    }
                    Ok(false) => {
                        self.metrics.record_delivery(id, false, true).await;
                        if frame_type == StreamFrameType::Delta {
                            recovery_instances.insert(id.clone());
                        }
                    }
                    Err(()) => return,
                }
            }

            if !any_changed {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

fn encode_frame_payload(
    frame: &RawFrame,
    prev_frame: Option<&RawFrame>,
    force_keyframe: bool,
    tile_size: u16,
) -> (StreamFrameType, Vec<u8>) {
    if force_keyframe || prev_frame.is_none() {
        return (
            StreamFrameType::Keyframe,
            encode_keyframe(&frame.data, frame.width, frame.height, frame.pitch),
        );
    }

    let prev = prev_frame.unwrap();
    (
        StreamFrameType::Delta,
        encode_delta(
            &prev.data,
            &frame.data,
            frame.width,
            frame.height,
            frame.pitch,
            tile_size as u32,
        ),
    )
}

fn encode_raw_frame(
    frame: &RawFrame,
    frame_type: StreamFrameType,
    payload: Vec<u8>,
    instance_index: u8,
    tile_size: u16,
) -> Vec<u8> {
    encode_stream_frame(EncodeParams {
        frame_type,
        instance_index,
        sequence: frame.sequence as u32,
        timestamp_ms: (frame.timestamp_ms % (u32::MAX as u64 + 1)) as u32,
        width: frame.width as u16,
        height: frame.height as u16,
        tile_size,
        raw_bytes: frame.width * frame.height * 4,
        metadata: StreamFrameMetadata::default(),
        payload,
    })
}

async fn send_binary_with_backpressure<S>(
    sender: &mut S,
    bytes: Vec<u8>,
    backpressure_limit: usize,
) -> Result<bool, ()>
where
    S: futures::Sink<Message> + Unpin,
    S::Error: std::fmt::Debug,
{
    if bytes.len() > backpressure_limit {
        return Ok(false);
    }

    sender
        .send(Message::Binary(bytes.into()))
        .await
        .map_err(|_| ())?;
    Ok(true)
}

async fn send_h264_frame<S>(
    sender: &mut S,
    h264_encoder: &mut Option<H264Encoder>,
    frame: &RawFrame,
    instance_index: u8,
    backpressure_limit: usize,
) where
    S: futures::Sink<Message> + Unpin,
    S::Error: std::fmt::Debug,
{
    if h264_encoder.is_none() {
        match H264Encoder::new(frame.width, frame.height) {
            Ok(encoder) => *h264_encoder = Some(encoder),
            Err(_) => return,
        }
    }

    let Some(encoder) = h264_encoder.as_mut() else {
        return;
    };

    let Ok(nal_data) = encoder.encode(frame) else {
        return;
    };

    if nal_data.is_empty() {
        return;
    }

    let h264_bytes = encode_h264_frame(
        nal_data,
        instance_index,
        frame.sequence as u32,
        (frame.timestamp_ms % (u32::MAX as u64 + 1)) as u32,
        frame.width as u16,
        frame.height as u16,
    );

    let _ = send_binary_with_backpressure(sender, h264_bytes, backpressure_limit).await;
}

async fn handle_control_message(
    bytes: &[u8],
    keyframe_cache: &Arc<RwLock<HashMap<String, Vec<u8>>>>,
    instance_id: &str,
    last_keyframe_request: &mut Instant,
) {
    let Ok(ctrl) = parse_viewer_control_message(bytes) else {
        return;
    };

    match ctrl {
        ViewerControl::Keyframe => {
            if last_keyframe_request.elapsed() >= Duration::from_millis(KEYFRAME_THROTTLE_MS) {
                keyframe_cache.write().await.remove(instance_id);
                *last_keyframe_request = Instant::now();
            }
        }
        ViewerControl::ClientMetrics(_) => {}
    }
}

#[allow(dead_code)]
fn _keep_required_imports(_: PixelFormat) {
    let _ = deflate_raw(&[]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame_hub::{FrameHub, PixelFormat, RawFrame};
    use std::sync::Arc;

    fn make_frame(seq: u64) -> RawFrame {
        RawFrame {
            width: 240,
            height: 160,
            pitch: 240 * 4,
            pixel_format: PixelFormat::XRGB8888,
            data: vec![0u8; 240 * 160 * 4],
            sequence: seq,
            timestamp_ms: seq * 16,
        }
    }

    #[tokio::test]
    async fn keyframe_cached_on_push() {
        let hub = Arc::new(FrameHub::new());
        hub.register_instance("inst-1").await;
        let broadcast = DashboardBroadcast::new(hub.clone());

        hub.push_frame("inst-1", make_frame(1)).await;

        let cache = broadcast.keyframe_cache.read().await;
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn broadcast_config_defaults() {
        let config = BroadcastConfig::default();
        assert_eq!(config.keyframe_interval, 60);
        assert_eq!(config.backpressure_limit, 262_144);
        assert_eq!(config.tile_size, 16);
        assert!(!config.h264_enabled, "h264 must be disabled by default");
    }

    #[tokio::test]
    async fn broadcast_config_h264_opt_in() {
        let config = BroadcastConfig {
            h264_enabled: true,
            ..Default::default()
        };
        assert!(config.h264_enabled);
        assert_eq!(config.keyframe_interval, 60);
    }

    #[tokio::test]
    async fn broadcast_uses_injected_metrics_handle() {
        let hub = Arc::new(FrameHub::new());
        let metrics = Arc::new(StreamMetrics::new());
        let broadcast = DashboardBroadcast::with_config_and_metrics(
            hub,
            BroadcastConfig::default(),
            metrics.clone(),
        );

        broadcast
            .metrics()
            .record_delivery("inst-1", true, false)
            .await;

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.instances[0].instance_delivered, 1);
    }
}
