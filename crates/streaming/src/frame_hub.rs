use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::{RwLock, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PixelFormat {
    XRGB8888,
    RGB565,
}

#[derive(Debug, Clone)]
pub struct RawFrame {
    pub width: u32,
    pub height: u32,
    pub pitch: u32,
    pub pixel_format: PixelFormat,
    pub data: Vec<u8>,
    pub sequence: u64,
    pub timestamp_ms: u64,
}

type FrameChannel = watch::Sender<Option<RawFrame>>;

#[derive(Clone)]
pub struct FrameHub {
    channels: Arc<RwLock<HashMap<String, Arc<FrameChannel>>>>,
    sequences: Arc<RwLock<HashMap<String, u64>>>,
}

impl FrameHub {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            sequences: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_instance(&self, id: impl Into<String>) {
        let id = id.into();
        let (tx, _rx) = watch::channel(None);
        self.channels.write().await.insert(id.clone(), Arc::new(tx));
        self.sequences.write().await.insert(id, 0);
    }

    pub async fn unregister_instance(&self, id: &str) {
        self.channels.write().await.remove(id);
        self.sequences.write().await.remove(id);
    }

    pub async fn push_frame(&self, id: &str, mut frame: RawFrame) {
        let mut sequences = self.sequences.write().await;
        let seq = sequences.entry(id.to_string()).or_insert(0);
        *seq += 1;
        frame.sequence = *seq;
        frame.timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        drop(sequences);

        let channels = self.channels.read().await;
        if let Some(tx) = channels.get(id) {
            tx.send_replace(Some(frame));
        }
    }

    pub async fn subscribe(&self, id: &str) -> Option<watch::Receiver<Option<RawFrame>>> {
        let channels = self.channels.read().await;
        channels.get(id).map(|tx| tx.subscribe())
    }

    pub async fn latest_frame(&self, id: &str) -> Option<RawFrame> {
        let channels = self.channels.read().await;
        channels.get(id).and_then(|tx| tx.borrow().clone())
    }

    pub async fn registered_instances(&self) -> Vec<String> {
        let channels = self.channels.read().await;
        let mut ids: Vec<String> = channels.keys().cloned().collect();
        ids.sort();
        ids
    }
}

impl Default for FrameHub {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(width: u32, height: u32) -> RawFrame {
        RawFrame {
            width,
            height,
            pitch: width * 4,
            pixel_format: PixelFormat::XRGB8888,
            data: vec![0u8; (width * height * 4) as usize],
            sequence: 0,
            timestamp_ms: 0,
        }
    }

    #[tokio::test]
    async fn push_and_subscribe() {
        let hub = FrameHub::new();
        hub.register_instance("test-1").await;

        let mut rx = hub.subscribe("test-1").await.unwrap();

        let frame = make_frame(240, 160);
        hub.push_frame("test-1", frame).await;

        rx.changed().await.unwrap();
        let received = rx.borrow().clone().unwrap();
        assert_eq!(received.width, 240);
        assert_eq!(received.height, 160);
        assert_eq!(received.sequence, 1);

        hub.push_frame("test-1", make_frame(240, 160)).await;
        rx.changed().await.unwrap();
        let received2 = rx.borrow().clone().unwrap();
        assert_eq!(received2.sequence, 2);
    }

    #[tokio::test]
    async fn latest_frame_returns_current() {
        let hub = FrameHub::new();
        hub.register_instance("test-2").await;

        assert!(hub.latest_frame("test-2").await.is_none());

        hub.push_frame("test-2", make_frame(240, 160)).await;
        let frame = hub.latest_frame("test-2").await.unwrap();
        assert_eq!(frame.width, 240);
    }

    #[tokio::test]
    async fn backpressure_drops_old_frames() {
        let hub = FrameHub::new();
        hub.register_instance("test-3").await;

        let mut rx = hub.subscribe("test-3").await.unwrap();

        for _ in 0..100 {
            hub.push_frame("test-3", make_frame(240, 160)).await;
        }

        rx.changed().await.unwrap();
        let frame = rx.borrow().clone().unwrap();
        assert_eq!(frame.sequence, 100);
    }

    #[tokio::test]
    async fn unregister_removes_instance() {
        let hub = FrameHub::new();
        hub.register_instance("test-4").await;
        hub.unregister_instance("test-4").await;
        assert!(hub.subscribe("test-4").await.is_none());
        assert!(hub.latest_frame("test-4").await.is_none());
    }

    #[tokio::test]
    async fn registered_instances_returns_sorted_ids() {
        let hub = FrameHub::new();
        hub.register_instance("z").await;
        hub.register_instance("a").await;
        assert_eq!(hub.registered_instances().await, vec!["a", "z"]);
    }
}
