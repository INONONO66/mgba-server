use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    Control,
    MemoryRead,
    FrameCapture,
    State,
    Health,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandPriority {
    Control = 0,
    MemoryRead = 1,
    Health = 2,
    State = 3,
    FrameCapture = 4,
}

impl CommandKind {
    pub fn default_priority(self) -> CommandPriority {
        match self {
            CommandKind::Control => CommandPriority::Control,
            CommandKind::MemoryRead => CommandPriority::MemoryRead,
            CommandKind::Health => CommandPriority::Health,
            CommandKind::State => CommandPriority::State,
            CommandKind::FrameCapture => CommandPriority::FrameCapture,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandTrace {
    pub request_id: Uuid,
    pub instance_id: String,
    pub kind: CommandKind,
    pub priority: CommandPriority,
    pub enqueue_at: DateTime<Utc>,
    pub dequeue_at: DateTime<Utc>,
    pub socket_write_at: DateTime<Utc>,
    pub response_at: DateTime<Utc>,
    pub caller_complete_at: DateTime<Utc>,
}

impl CommandTrace {
    pub fn queue_wait_ms(&self) -> i64 {
        (self.dequeue_at - self.enqueue_at).num_milliseconds()
    }
    pub fn socket_roundtrip_ms(&self) -> i64 {
        (self.response_at - self.socket_write_at).num_milliseconds()
    }
    pub fn caller_latency_ms(&self) -> i64 {
        (self.caller_complete_at - self.enqueue_at).num_milliseconds()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    pub response: String,
    pub trace: CommandTrace,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    #[error("scheduler queue is closed")]
    Closed,
    #[error("mGBA command timed out")]
    Timeout,
    #[error("mGBA command failed: {0}")]
    Transport(String),
}

#[async_trait]
pub trait MgbaTransport: Send + Sync + 'static {
    async fn send(&self, message: String) -> Result<String, SchedulerError>;
}

struct ScheduledCommand {
    request_id: Uuid,
    instance_id: String,
    kind: CommandKind,
    priority: CommandPriority,
    message: String,
    enqueue_at: DateTime<Utc>,
    timeout: Duration,
    response: oneshot::Sender<Result<CommandResult, SchedulerError>>,
}

#[derive(Clone)]
pub struct CommandScheduler {
    tx: mpsc::Sender<ScheduledCommand>,
}

impl CommandScheduler {
    pub fn new(
        instance_id: impl Into<String>,
        transport: Arc<dyn MgbaTransport>,
        capacity_per_priority: usize,
    ) -> Self {
        let instance_id = instance_id.into();
        let (tx, rx) = mpsc::channel(capacity_per_priority.saturating_mul(5).max(1));
        tokio::spawn(run_worker(
            instance_id,
            transport,
            rx,
            capacity_per_priority.max(1),
        ));
        Self { tx }
    }

    pub async fn submit(
        &self,
        kind: CommandKind,
        message: impl Into<String>,
        timeout: Duration,
    ) -> Result<CommandResult, SchedulerError> {
        self.submit_with_priority(kind, kind.default_priority(), message, timeout)
            .await
    }

    pub async fn submit_with_priority(
        &self,
        kind: CommandKind,
        priority: CommandPriority,
        message: impl Into<String>,
        timeout: Duration,
    ) -> Result<CommandResult, SchedulerError> {
        let (tx, rx) = oneshot::channel();
        let command = ScheduledCommand {
            request_id: Uuid::new_v4(),
            instance_id: String::new(),
            kind,
            priority,
            message: message.into(),
            enqueue_at: Utc::now(),
            timeout,
            response: tx,
        };
        self.tx
            .send(command)
            .await
            .map_err(|_| SchedulerError::Closed)?;
        rx.await.map_err(|_| SchedulerError::Closed)?
    }
}

async fn run_worker(
    instance_id: String,
    transport: Arc<dyn MgbaTransport>,
    mut rx: mpsc::Receiver<ScheduledCommand>,
    capacity_per_priority: usize,
) {
    let mut queues: [VecDeque<ScheduledCommand>; 5] = std::array::from_fn(|_| VecDeque::new());
    loop {
        let command = if let Some(command) = pop_next(&mut queues) {
            command
        } else {
            match rx.recv().await {
                Some(mut command) => {
                    command.instance_id = instance_id.clone();
                    command
                }
                None => return,
            }
        };

        drain_available(&mut rx, &mut queues, capacity_per_priority, &instance_id);
        execute(transport.clone(), command).await;
        drain_available(&mut rx, &mut queues, capacity_per_priority, &instance_id);
    }
}

fn drain_available(
    rx: &mut mpsc::Receiver<ScheduledCommand>,
    queues: &mut [VecDeque<ScheduledCommand>; 5],
    capacity_per_priority: usize,
    instance_id: &str,
) {
    while let Ok(mut command) = rx.try_recv() {
        command.instance_id = instance_id.to_string();
        let queue = &mut queues[command.priority as usize];
        if queue.len() >= capacity_per_priority {
            let _ = command.response.send(Err(SchedulerError::Closed));
        } else {
            queue.push_back(command);
        }
    }
}

fn pop_next(queues: &mut [VecDeque<ScheduledCommand>; 5]) -> Option<ScheduledCommand> {
    for queue in queues.iter_mut() {
        if let Some(command) = queue.pop_front() {
            return Some(command);
        }
    }
    None
}

async fn execute(transport: Arc<dyn MgbaTransport>, command: ScheduledCommand) {
    let dequeue_at = Utc::now();
    let socket_write_at = Utc::now();
    let response = tokio::time::timeout(command.timeout, transport.send(command.message)).await;
    let response_at = Utc::now();
    let result = match response {
        Ok(Ok(response)) => {
            let caller_complete_at = Utc::now();
            Ok(CommandResult {
                response,
                trace: CommandTrace {
                    request_id: command.request_id,
                    instance_id: command.instance_id,
                    kind: command.kind,
                    priority: command.priority,
                    enqueue_at: command.enqueue_at,
                    dequeue_at,
                    socket_write_at,
                    response_at,
                    caller_complete_at,
                },
            })
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(SchedulerError::Timeout),
    };
    let _ = command.response.send(result);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingTransport {
        seen: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl MgbaTransport for RecordingTransport {
        async fn send(&self, message: String) -> Result<String, SchedulerError> {
            self.seen.lock().unwrap().push(message.clone());
            Ok(format!("ok:{message}"))
        }
    }

    #[tokio::test]
    async fn prioritizes_control_and_memory_over_frame_capture_when_pending() {
        let transport = Arc::new(RecordingTransport {
            seen: Mutex::new(Vec::new()),
        });
        let scheduler = CommandScheduler::new("instance-a", transport.clone(), 16);

        let frame_1 =
            scheduler.submit(CommandKind::FrameCapture, "frame-1", Duration::from_secs(1));
        let frame_2 =
            scheduler.submit(CommandKind::FrameCapture, "frame-2", Duration::from_secs(1));
        let memory = scheduler.submit(CommandKind::MemoryRead, "read", Duration::from_secs(1));
        let control = scheduler.submit(CommandKind::Control, "tap", Duration::from_secs(1));

        let _ = tokio::join!(frame_1, frame_2, memory, control);
        let seen = transport.seen.lock().unwrap().clone();
        assert_eq!(seen[0], "frame-1");
        assert_eq!(seen[1], "tap");
        assert_eq!(seen[2], "read");
        assert_eq!(seen[3], "frame-2");
    }

    #[tokio::test]
    async fn emits_monotonic_latency_timestamps() {
        let transport = Arc::new(RecordingTransport {
            seen: Mutex::new(Vec::new()),
        });
        let scheduler = CommandScheduler::new("instance-a", transport, 16);
        let result = scheduler
            .submit(CommandKind::Control, "tap", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(result.trace.instance_id, "instance-a");
        assert!(result.trace.enqueue_at <= result.trace.dequeue_at);
        assert!(result.trace.dequeue_at <= result.trace.socket_write_at);
        assert!(result.trace.socket_write_at <= result.trace.response_at);
        assert!(result.trace.response_at <= result.trace.caller_complete_at);
        assert!(result.trace.caller_latency_ms() >= 0);
    }
}
