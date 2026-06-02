use async_trait::async_trait;
use grokemon_config::GatewayConfig;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{process::Command, sync::RwLock};
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
    pub container_id: String,
    pub host: String,
    pub port: u16,
    pub capture_directory: PathBuf,
    pub status: InstanceStatus,
}

#[derive(Debug, Clone)]
pub struct CreateContainerOptions {
    pub image: String,
    pub instance_id: String,
    pub network_name: String,
    pub emulator_port: u16,
    pub emulator_memory_bytes: u64,
    pub capture_root: PathBuf,
    pub rom_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerInfo {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub capture_directory: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedContainerInfo {
    pub id: String,
    pub instance_id: String,
    pub host: String,
    pub port: u16,
    pub capture_directory: PathBuf,
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
pub trait DockerBackend: Send + Sync + 'static {
    async fn create_container(
        &self,
        opts: CreateContainerOptions,
    ) -> Result<ContainerInfo, InstanceError>;
    async fn stop_container(&self, container_id: &str) -> Result<(), InstanceError>;
    async fn list_managed_containers(&self) -> Result<Vec<ManagedContainerInfo>, InstanceError>;
    async fn inspect_running(&self, container_id: &str) -> Result<bool, InstanceError>;
}

#[derive(Clone)]
pub struct InstanceManager<B: DockerBackend> {
    config: GatewayConfig,
    backend: Arc<B>,
    instances: Arc<RwLock<HashMap<String, InstanceInfo>>>,
}

impl<B: DockerBackend> InstanceManager<B> {
    pub fn new(config: GatewayConfig, backend: Arc<B>) -> Self {
        Self {
            config,
            backend,
            instances: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create(
        &self,
        session_id: impl Into<String>,
    ) -> Result<InstanceInfo, InstanceError> {
        if self.instances.read().await.len() >= self.config.max_instances as usize {
            return Err(InstanceError::MaxInstancesReached);
        }
        let instance_id = Uuid::new_v4().to_string();
        let container = self
            .backend
            .create_container(CreateContainerOptions {
                image: self.config.emulator_image.clone(),
                instance_id: instance_id.clone(),
                network_name: self.config.network_name.clone(),
                emulator_port: self.config.emulator_port,
                emulator_memory_bytes: self.config.emulator_memory_bytes,
                capture_root: PathBuf::from(&self.config.capture_root),
                rom_path: self.config.rom_path.as_ref().map(PathBuf::from),
            })
            .await?;

        let info = InstanceInfo {
            id: instance_id.clone(),
            principal_session_id: session_id.into(),
            container_id: container.id,
            host: container.host,
            port: container.port,
            capture_directory: container.capture_directory,
            status: InstanceStatus::Running,
        };
        self.instances
            .write()
            .await
            .insert(instance_id, info.clone());
        Ok(info)
    }

    pub async fn destroy(&self, instance_id: &str) -> Result<(), InstanceError> {
        let info = self
            .instances
            .write()
            .await
            .remove(instance_id)
            .ok_or(InstanceError::NotFound)?;
        self.backend.stop_container(&info.container_id).await
    }

    pub async fn list(&self) -> Vec<InstanceInfo> {
        self.instances.read().await.values().cloned().collect()
    }

    pub async fn get(&self, instance_id: &str) -> Option<InstanceInfo> {
        self.instances.read().await.get(instance_id).cloned()
    }

    pub async fn reconstruct(&self) -> Result<(), InstanceError> {
        let containers = self.backend.list_managed_containers().await?;
        let mut instances = self.instances.write().await;
        for container in containers {
            if instances.len() >= self.config.max_instances as usize {
                break;
            }
            if !self.backend.inspect_running(&container.id).await? {
                continue;
            }
            if !is_safe_capture_directory(
                &container.capture_directory,
                Path::new(&self.config.capture_root),
            ) {
                continue;
            }
            instances.insert(
                container.instance_id.clone(),
                InstanceInfo {
                    id: container.instance_id.clone(),
                    principal_session_id: container.instance_id.clone(),
                    container_id: container.id,
                    host: container.host,
                    port: container.port,
                    capture_directory: container.capture_directory,
                    status: InstanceStatus::Running,
                },
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct DockerCliDriver;

#[async_trait]
impl DockerBackend for DockerCliDriver {
    async fn create_container(
        &self,
        opts: CreateContainerOptions,
    ) -> Result<ContainerInfo, InstanceError> {
        let capture_root = opts
            .capture_root
            .canonicalize()
            .unwrap_or(opts.capture_root.clone());
        let capture_directory = capture_root.join(&opts.instance_id);
        if !is_path_inside(&capture_root, &capture_directory) {
            return Err(InstanceError::UnsafeCapturePath);
        }
        tokio::fs::create_dir_all(&capture_directory)
            .await
            .map_err(|error| InstanceError::Docker(error.to_string()))?;

        let name = format!("grokemon-{}", opts.instance_id);
        let memory = opts.emulator_memory_bytes.to_string();
        let port_spec = format!("127.0.0.1::{}", opts.emulator_port);
        let mut command = Command::new("docker");
        command
            .arg("run")
            .arg("-d")
            .arg("--name")
            .arg(&name)
            .arg("--label")
            .arg("pss-mgba.managed=true")
            .arg("--label")
            .arg(format!("pss-mgba.instance-id={}", opts.instance_id))
            .arg("--label")
            .arg(format!(
                "pss-mgba.capture-directory={}",
                capture_directory.display()
            ))
            .arg("--network")
            .arg(opts.network_name)
            .arg("--memory")
            .arg(&memory)
            .arg("--memory-swap")
            .arg(&memory)
            .arg("--pids-limit")
            .arg("128")
            .arg("--mount")
            .arg(format!(
                "type=bind,source={},target=/capture",
                capture_directory.display()
            ))
            .arg("-p")
            .arg(port_spec);
        if let Some(rom) = opts.rom_path {
            command.arg("--mount").arg(format!(
                "type=bind,source={},target=/rom/game.gb,readonly",
                rom.display()
            ));
        }
        command
            .arg(opts.image)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = command
            .output()
            .await
            .map_err(|error| InstanceError::Docker(error.to_string()))?;
        if !output.status.success() {
            return Err(InstanceError::Docker(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(ContainerInfo {
            id,
            host: "127.0.0.1".to_string(),
            port: opts.emulator_port,
            capture_directory,
        })
    }

    async fn stop_container(&self, container_id: &str) -> Result<(), InstanceError> {
        let output = Command::new("docker")
            .arg("rm")
            .arg("-f")
            .arg(container_id)
            .output()
            .await
            .map_err(|error| InstanceError::Docker(error.to_string()))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(InstanceError::Docker(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    async fn list_managed_containers(&self) -> Result<Vec<ManagedContainerInfo>, InstanceError> {
        // Deliberately conservative shell-driver scaffold: real bollard backend can replace this without changing manager API.
        Ok(Vec::new())
    }

    async fn inspect_running(&self, _container_id: &str) -> Result<bool, InstanceError> {
        Ok(false)
    }
}

pub fn is_path_inside(root: &Path, candidate: &Path) -> bool {
    candidate.starts_with(root) && candidate != root
}

pub fn is_safe_capture_directory(candidate: &Path, root: &Path) -> bool {
    is_path_inside(root, candidate)
}

pub async fn health_check_interval() -> Duration {
    Duration::from_secs(10)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeDocker {
        created: Mutex<Vec<CreateContainerOptions>>,
        stopped: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl DockerBackend for FakeDocker {
        async fn create_container(
            &self,
            opts: CreateContainerOptions,
        ) -> Result<ContainerInfo, InstanceError> {
            self.created.lock().unwrap().push(opts.clone());
            Ok(ContainerInfo {
                id: format!("container-{}", opts.instance_id),
                host: "127.0.0.1".to_string(),
                port: opts.emulator_port,
                capture_directory: opts.capture_root.join(&opts.instance_id),
            })
        }
        async fn stop_container(&self, container_id: &str) -> Result<(), InstanceError> {
            self.stopped.lock().unwrap().push(container_id.to_string());
            Ok(())
        }
        async fn list_managed_containers(
            &self,
        ) -> Result<Vec<ManagedContainerInfo>, InstanceError> {
            Ok(Vec::new())
        }
        async fn inspect_running(&self, _container_id: &str) -> Result<bool, InstanceError> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn create_and_destroy_tracks_lifecycle() {
        let backend = Arc::new(FakeDocker::default());
        let config = GatewayConfig {
            max_instances: 20,
            capture_root: "/tmp/grokemon-captures-test".to_string(),
            ..GatewayConfig::default()
        };
        let manager = InstanceManager::new(config, backend.clone());
        let info = manager.create("session-a").await.unwrap();
        assert_eq!(info.principal_session_id, "session-a");
        assert_eq!(manager.list().await.len(), 1);
        manager.destroy(&info.id).await.unwrap();
        assert!(manager.list().await.is_empty());
        assert_eq!(backend.stopped.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn enforces_twenty_instance_limit() {
        let backend = Arc::new(FakeDocker::default());
        let manager = InstanceManager::new(
            GatewayConfig {
                max_instances: 20,
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
}
