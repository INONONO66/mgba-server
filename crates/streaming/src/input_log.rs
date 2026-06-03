use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

const MAX_RECENT: usize = 100;
const BROADCAST_CAPACITY: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputEventStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputEvent {
    pub event_id: String,
    pub session_id: String,
    pub action: String,
    pub button: Option<String>,
    pub status: InputEventStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub latency_ms: Option<i64>,
}

struct SessionLog {
    recent: VecDeque<InputEvent>,
    pending: HashMap<String, InputEvent>,
    tx: broadcast::Sender<InputEvent>,
}

impl SessionLog {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            recent: VecDeque::new(),
            pending: HashMap::new(),
            tx,
        }
    }
}

#[derive(Clone)]
pub struct InputLogBus {
    sessions: Arc<RwLock<HashMap<String, SessionLog>>>,
}

impl InputLogBus {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn begin_input(
        &self,
        session_id: &str,
        action: &str,
        button: Option<&str>,
    ) -> String {
        let event_id = Uuid::new_v4().to_string();
        let event = InputEvent {
            event_id: event_id.clone(),
            session_id: session_id.to_string(),
            action: action.to_string(),
            button: button.map(|b| b.to_string()),
            status: InputEventStatus::Pending,
            started_at: Utc::now(),
            completed_at: None,
            latency_ms: None,
        };

        let mut sessions = self.sessions.write().await;
        let log = sessions
            .entry(session_id.to_string())
            .or_insert_with(SessionLog::new);
        log.pending.insert(event_id.clone(), event);
        event_id
    }

    pub async fn complete_input(&self, event_id: &str, latency_ms: i64) {
        self.finalize_input(event_id, InputEventStatus::Completed, Some(latency_ms))
            .await;
    }

    pub async fn fail_input(&self, event_id: &str) {
        self.finalize_input(event_id, InputEventStatus::Failed, None)
            .await;
    }

    async fn finalize_input(
        &self,
        event_id: &str,
        status: InputEventStatus,
        latency_ms: Option<i64>,
    ) {
        let mut sessions = self.sessions.write().await;
        for log in sessions.values_mut() {
            if let Some(mut event) = log.pending.remove(event_id) {
                event.status = status;
                event.completed_at = Some(Utc::now());
                event.latency_ms = latency_ms;

                if log.recent.len() >= MAX_RECENT {
                    log.recent.pop_front();
                }
                log.recent.push_back(event.clone());

                let _ = log.tx.send(event);
                return;
            }
        }
    }

    pub async fn recent(&self, session_id: &str) -> Vec<InputEvent> {
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .map(|log| log.recent.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn subscribe(&self, session_id: &str) -> broadcast::Receiver<InputEvent> {
        let mut sessions = self.sessions.write().await;
        let log = sessions
            .entry(session_id.to_string())
            .or_insert_with(SessionLog::new);
        log.tx.subscribe()
    }
}

impl Default for InputLogBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn input_event_lifecycle() {
        let bus = InputLogBus::new();
        let event_id = bus.begin_input("session-1", "tap", Some("A")).await;
        bus.complete_input(&event_id, 15).await;
        let recent = bus.recent("session-1").await;
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].status, InputEventStatus::Completed);
        assert_eq!(recent[0].latency_ms, Some(15));
        assert_eq!(recent[0].button, Some("A".to_string()));
    }

    #[tokio::test]
    async fn recent_capped_at_100() {
        let bus = InputLogBus::new();
        for i in 0..150 {
            let id = bus.begin_input("session-2", "tap", Some("A")).await;
            bus.complete_input(&id, i).await;
        }
        let recent = bus.recent("session-2").await;
        assert_eq!(recent.len(), 100);
        assert_eq!(recent.last().unwrap().latency_ms, Some(149));
    }

    #[tokio::test]
    async fn subscribe_receives_live_events() {
        let bus = InputLogBus::new();
        let mut rx = bus.subscribe("session-3").await;
        let event_id = bus.begin_input("session-3", "tap", Some("B")).await;
        bus.complete_input(&event_id, 5).await;
        let event = rx.recv().await.unwrap();
        assert_eq!(event.status, InputEventStatus::Completed);
    }

    #[tokio::test]
    async fn fail_input_marks_failed() {
        let bus = InputLogBus::new();
        let event_id = bus.begin_input("session-4", "tap", Some("A")).await;
        bus.fail_input(&event_id).await;
        let recent = bus.recent("session-4").await;
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].status, InputEventStatus::Failed);
        assert_eq!(recent[0].latency_ms, None);
    }
}
