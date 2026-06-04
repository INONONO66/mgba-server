//! Per-instance streaming metrics tracker.
//!
//! Tracks produced frames, bytes, keyframe/delta counts, sequence gaps, and
//! delivery success/drops for dashboard and per-instance subscribers.

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstanceMetrics {
    pub instance_id: String,
    pub produced_frames: u64,
    pub produced_bytes: u64,
    pub keyframes: u64,
    pub deltas: u64,
    pub sequence_gaps: u64,
    pub dashboard_delivered: u64,
    pub dashboard_dropped: u64,
    pub instance_delivered: u64,
    pub instance_dropped: u64,
    pub compression_ratio: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub instances: Vec<InstanceMetrics>,
    pub total_produced_frames: u64,
    pub total_produced_bytes: u64,
    pub total_dashboard_delivered: u64,
    pub total_dashboard_dropped: u64,
}

#[derive(Clone)]
pub struct StreamMetrics {
    instances: Arc<RwLock<HashMap<String, InstanceMetrics>>>,
}

impl StreamMetrics {
    pub fn new() -> Self {
        Self {
            instances: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn record_produced(&self, instance_id: &str, bytes: u64, is_keyframe: bool) {
        let mut map = self.instances.write().await;
        let m = map
            .entry(instance_id.to_string())
            .or_insert_with(|| InstanceMetrics {
                instance_id: instance_id.to_string(),
                ..Default::default()
            });
        m.produced_frames += 1;
        m.produced_bytes += bytes;
        if is_keyframe {
            m.keyframes += 1;
        } else {
            m.deltas += 1;
        }
        if m.produced_frames > 0 {
            m.compression_ratio = m.produced_bytes as f64 / m.produced_frames as f64;
        }
    }

    pub async fn record_delivery(&self, instance_id: &str, delivered: bool, is_dashboard: bool) {
        let mut map = self.instances.write().await;
        let m = map
            .entry(instance_id.to_string())
            .or_insert_with(|| InstanceMetrics {
                instance_id: instance_id.to_string(),
                ..Default::default()
            });
        if is_dashboard {
            if delivered {
                m.dashboard_delivered += 1;
            } else {
                m.dashboard_dropped += 1;
            }
        } else if delivered {
            m.instance_delivered += 1;
        } else {
            m.instance_dropped += 1;
        }
    }

    pub async fn record_sequence_gap(&self, instance_id: &str) {
        let mut map = self.instances.write().await;
        let m = map
            .entry(instance_id.to_string())
            .or_insert_with(|| InstanceMetrics {
                instance_id: instance_id.to_string(),
                ..Default::default()
            });
        m.sequence_gaps += 1;
    }

    pub async fn snapshot(&self) -> MetricsSnapshot {
        let map = self.instances.read().await;
        let mut instances: Vec<InstanceMetrics> = map.values().cloned().collect();
        instances.sort_by(|a, b| a.instance_id.cmp(&b.instance_id));

        let total_produced_frames = instances.iter().map(|m| m.produced_frames).sum();
        let total_produced_bytes = instances.iter().map(|m| m.produced_bytes).sum();
        let total_dashboard_delivered = instances.iter().map(|m| m.dashboard_delivered).sum();
        let total_dashboard_dropped = instances.iter().map(|m| m.dashboard_dropped).sum();

        MetricsSnapshot {
            instances,
            total_produced_frames,
            total_produced_bytes,
            total_dashboard_delivered,
            total_dashboard_dropped,
        }
    }
}

impl Default for StreamMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn metrics_accumulate_correctly() {
        let metrics = StreamMetrics::new();

        for i in 0..10 {
            metrics.record_produced("i1", 1000, i == 0).await;
        }
        for _ in 0..8 {
            metrics.record_delivery("i1", true, true).await;
        }
        for _ in 0..2 {
            metrics.record_delivery("i1", false, true).await;
        }

        let snap = metrics.snapshot().await;
        assert_eq!(snap.instances.len(), 1);
        let m = &snap.instances[0];
        assert_eq!(m.produced_frames, 10);
        assert_eq!(m.produced_bytes, 10_000);
        assert_eq!(m.dashboard_delivered, 8);
        assert_eq!(m.dashboard_dropped, 2);
        assert_eq!(m.keyframes, 1);
        assert_eq!(m.deltas, 9);
        assert_eq!(snap.total_produced_frames, 10);
        assert_eq!(snap.total_produced_bytes, 10_000);
        assert_eq!(snap.total_dashboard_delivered, 8);
        assert_eq!(snap.total_dashboard_dropped, 2);
    }

    #[tokio::test]
    async fn snapshot_sorted_by_instance_id() {
        let metrics = StreamMetrics::new();
        metrics.record_produced("z-instance", 100, true).await;
        metrics.record_produced("a-instance", 100, true).await;
        let snap = metrics.snapshot().await;
        assert_eq!(snap.instances.len(), 2);
        assert_eq!(snap.instances[0].instance_id, "a-instance");
        assert_eq!(snap.instances[1].instance_id, "z-instance");
    }

    #[tokio::test]
    async fn instance_delivery_counts_split_from_dashboard() {
        let metrics = StreamMetrics::new();
        metrics.record_delivery("i1", true, false).await;
        metrics.record_delivery("i1", true, false).await;
        metrics.record_delivery("i1", false, false).await;
        metrics.record_delivery("i1", true, true).await;

        let snap = metrics.snapshot().await;
        let m = &snap.instances[0];
        assert_eq!(m.instance_delivered, 2);
        assert_eq!(m.instance_dropped, 1);
        assert_eq!(m.dashboard_delivered, 1);
        assert_eq!(m.dashboard_dropped, 0);
    }

    #[tokio::test]
    async fn sequence_gaps_accumulate() {
        let metrics = StreamMetrics::new();
        metrics.record_sequence_gap("i1").await;
        metrics.record_sequence_gap("i1").await;
        metrics.record_sequence_gap("i1").await;
        let snap = metrics.snapshot().await;
        assert_eq!(snap.instances[0].sequence_gaps, 3);
    }

    #[tokio::test]
    async fn compression_ratio_tracks_bytes_per_frame() {
        let metrics = StreamMetrics::new();
        metrics.record_produced("i1", 1000, true).await;
        metrics.record_produced("i1", 3000, false).await;
        let snap = metrics.snapshot().await;
        // 4000 bytes / 2 frames = 2000
        assert_eq!(snap.instances[0].compression_ratio, 2000.0);
    }

    #[tokio::test]
    async fn empty_snapshot_has_zero_totals() {
        let metrics = StreamMetrics::new();
        let snap = metrics.snapshot().await;
        assert_eq!(snap.instances.len(), 0);
        assert_eq!(snap.total_produced_frames, 0);
        assert_eq!(snap.total_produced_bytes, 0);
        assert_eq!(snap.total_dashboard_delivered, 0);
        assert_eq!(snap.total_dashboard_dropped, 0);
    }
}
