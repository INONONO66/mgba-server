use async_trait::async_trait;
use chrono::{DateTime, Utc};
use grokemon_config::GatewayConfig;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    Starting,
    Running,
    Error,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub id: String,
    pub principal_session_id: String,
    pub pid: u32,
    pub socket_path: String,
    pub principal_token: String,
    pub created_at: DateTime<Utc>,
    pub status: InstanceStatus,
}

#[derive(Debug, Clone)]
pub struct CreateInstanceOptions {
    pub instance_id: String,
    pub socket_path: String,
    pub core_path: String,
    pub rom_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    pub pid: u32,
    pub socket_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedInstanceInfo {
    pub instance_id: String,
    pub pid: u32,
    pub socket_path: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InstanceError {
    #[error("max instances reached")]
    MaxInstancesReached,
    #[error("unsafe capture path")]
    UnsafeCapturePath,
    #[error("docker command failed: {0}")]
    Docker(String),
    #[error("instance not found")]
    NotFound,
}

#[async_trait]
pub trait InstanceBackend: Send + Sync + 'static {
    async fn create_instance(
        &self,
        opts: CreateInstanceOptions,
    ) -> Result<WorkerInfo, InstanceError>;
    async fn stop_instance(&self, instance_id: &str) -> Result<(), InstanceError>;
    async fn list_managed_instances(&self) -> Result<Vec<ManagedInstanceInfo>, InstanceError>;
    async fn inspect_running(&self, instance_id: &str) -> Result<bool, InstanceError>;
}

#[derive(Clone)]
pub struct InstanceManager<B: InstanceBackend> {
    config: GatewayConfig,
    backend: Arc<B>,
    instances: Arc<RwLock<HashMap<String, InstanceInfo>>>,
    pending_creates: Arc<Mutex<usize>>,
}

impl<B: InstanceBackend> InstanceManager<B> {
    pub fn new(config: GatewayConfig, backend: Arc<B>) -> Self {
        Self {
            config,
            backend,
            instances: Arc::new(RwLock::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(0)),
        }
    }

    pub async fn create(
        &self,
        session_id: impl Into<String>,
    ) -> Result<InstanceInfo, InstanceError> {
        {
            let mut pending = self.pending_creates.lock().await;
            let current = self.instances.read().await.len();
            if current + *pending >= self.config.max_instances as usize {
                return Err(InstanceError::MaxInstancesReached);
            }
            *pending += 1;
        }
        let instance_id = Uuid::new_v4().to_string();
        let worker = match self
            .backend
            .create_instance(CreateInstanceOptions {
                instance_id: instance_id.clone(),
                socket_path: format!("{}/{}.sock", self.config.worker_socket_dir, instance_id),
                core_path: self.config.libretro_core_path.clone(),
                rom_path: self.config.rom_path.clone(),
            })
            .await
        {
            Ok(worker) => worker,
            Err(error) => {
                *self.pending_creates.lock().await -= 1;
                return Err(error);
            }
        };

        let info = InstanceInfo {
            id: instance_id.clone(),
            principal_session_id: session_id.into(),
            pid: worker.pid,
            socket_path: worker.socket_path,
            principal_token: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
            status: InstanceStatus::Running,
        };
        self.instances
            .write()
            .await
            .insert(instance_id, info.clone());
        *self.pending_creates.lock().await -= 1;
        Ok(info)
    }

    pub async fn destroy(&self, instance_id: &str) -> Result<(), InstanceError> {
        let info = self
            .instances
            .read()
            .await
            .get(instance_id)
            .cloned()
            .ok_or(InstanceError::NotFound)?;
        self.backend.stop_instance(&info.id).await?;
        self.instances.write().await.remove(instance_id);
        Ok(())
    }

    pub async fn list(&self) -> Vec<InstanceInfo> {
        self.instances.read().await.values().cloned().collect()
    }

    pub async fn get(&self, instance_id: &str) -> Option<InstanceInfo> {
        self.instances.read().await.get(instance_id).cloned()
    }

    pub async fn reconstruct(&self) -> Result<(), InstanceError> {
        let _ = self.backend.list_managed_instances().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct DockerCliDriver;

#[async_trait]
impl InstanceBackend for DockerCliDriver {
    async fn create_instance(
        &self,
        opts: CreateInstanceOptions,
    ) -> Result<WorkerInfo, InstanceError> {
        let _ = opts.core_path;
        let _ = opts.rom_path;
        Ok(WorkerInfo {
            pid: 0,
            socket_path: opts.socket_path,
        })
    }

    async fn stop_instance(&self, _instance_id: &str) -> Result<(), InstanceError> {
        Ok(())
    }

    async fn list_managed_instances(&self) -> Result<Vec<ManagedInstanceInfo>, InstanceError> {
        // Deliberately conservative shell-driver scaffold: real bollard backend can replace this without changing manager API.
        Ok(Vec::new())
    }

    async fn inspect_running(&self, _instance_id: &str) -> Result<bool, InstanceError> {
        Ok(false)
    }
}

pub async fn health_check_interval() -> Duration {
    Duration::from_secs(10)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeBackend {
        created: Mutex<Vec<CreateInstanceOptions>>,
        fail_stop: Mutex<bool>,
        stopped: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl InstanceBackend for FakeBackend {
        async fn create_instance(
            &self,
            opts: CreateInstanceOptions,
        ) -> Result<WorkerInfo, InstanceError> {
            self.created.lock().unwrap().push(opts.clone());
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(WorkerInfo {
                pid: 42,
                socket_path: opts.socket_path,
            })
        }
        async fn stop_instance(&self, instance_id: &str) -> Result<(), InstanceError> {
            if *self.fail_stop.lock().unwrap() {
                return Err(InstanceError::Docker("stop failed".to_string()));
            }
            self.stopped.lock().unwrap().push(instance_id.to_string());
            Ok(())
        }
        async fn list_managed_instances(
            &self,
        ) -> Result<Vec<ManagedInstanceInfo>, InstanceError> {
            Ok(Vec::new())
        }
        async fn inspect_running(&self, _instance_id: &str) -> Result<bool, InstanceError> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn create_and_destroy_tracks_lifecycle() {
        let backend = Arc::new(FakeBackend::default());
        let config = GatewayConfig {
            max_instances: 20,
            libretro_core_path: "/tmp/core.so".to_string(),
            worker_socket_dir: "/tmp/grokemon-workers".to_string(),
            capture_root: "/tmp/grokemon-captures-test".to_string(),
            ..GatewayConfig::default()
        };
        let manager = InstanceManager::new(config, backend.clone());
        let info = manager.create("session-a").await.unwrap();
        assert_eq!(info.principal_session_id, "session-a");
        assert!(!info.principal_token.is_empty());
        assert_eq!(manager.list().await.len(), 1);
        manager.destroy(&info.id).await.unwrap();
        assert!(manager.list().await.is_empty());
        assert_eq!(backend.stopped.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn enforces_twenty_instance_limit() {
        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(
            GatewayConfig {
                max_instances: 20,
                libretro_core_path: "/tmp/core.so".to_string(),
                worker_socket_dir: "/tmp/grokemon-workers".to_string(),
                ..GatewayConfig::default()
            },
            backend,
        );
        for idx in 0..20 {
            manager.create(format!("session-{idx}")).await.unwrap();
        }
        assert_eq!(
            manager.create("too-many").await.unwrap_err(),
            InstanceError::MaxInstancesReached
        );
    }

    #[tokio::test]
    async fn concurrent_creates_respect_instance_limit() {
        let backend = Arc::new(FakeBackend::default());
        let manager = Arc::new(InstanceManager::new(
            GatewayConfig {
                max_instances: 1,
                libretro_core_path: "/tmp/core.so".to_string(),
                worker_socket_dir: "/tmp/grokemon-workers".to_string(),
                ..GatewayConfig::default()
            },
            backend,
        ));
        let first = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.create("session-a").await })
        };
        let second = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.create("session-b").await })
        };
        let results = [first.await.unwrap(), second.await.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    }

    #[tokio::test]
    async fn failed_destroy_keeps_instance_tracked() {
        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(GatewayConfig::default(), backend.clone());
        let info = manager.create("session-a").await.unwrap();
        *backend.fail_stop.lock().unwrap() = true;
        assert_eq!(
            manager.destroy(&info.id).await.unwrap_err(),
            InstanceError::Docker("stop failed".to_string())
        );
        assert!(manager.get(&info.id).await.is_some());
    }
}
