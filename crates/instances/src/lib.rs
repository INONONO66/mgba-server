use async_trait::async_trait;
use chrono::{DateTime, Utc};
use grokemon_config::GatewayConfig;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    process::{Child, Command},
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
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
    pub frame_socket_path: String,
    pub principal_token: String,
    pub created_at: DateTime<Utc>,
    pub status: InstanceStatus,
}

#[derive(Debug, Clone)]
pub struct CreateInstanceOptions {
    pub instance_id: String,
    pub socket_path: String,
    pub frame_socket_path: String,
    pub core_path: String,
    pub rom_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    pub pid: u32,
    pub socket_path: String,
    pub frame_socket_path: String,
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
    #[error("backend error: {0}")]
    Backend(String),
    #[error("instance not found")]
    NotFound,
    #[error("process error: {0}")]
    Process(String),
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

pub struct InstanceManager<B: InstanceBackend> {
    config: GatewayConfig,
    backend: Arc<B>,
    instances: Arc<RwLock<HashMap<String, InstanceInfo>>>,
    pending_creates: Arc<Mutex<usize>>,
    health_check_handle: Arc<StdMutex<Option<JoinHandle<()>>>>,
}

impl<B: InstanceBackend> Clone for InstanceManager<B> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            backend: self.backend.clone(),
            instances: self.instances.clone(),
            pending_creates: self.pending_creates.clone(),
            health_check_handle: self.health_check_handle.clone(),
        }
    }
}

impl<B: InstanceBackend> InstanceManager<B> {
    pub fn new(config: GatewayConfig, backend: Arc<B>) -> Self {
        Self {
            config,
            backend,
            instances: Arc::new(RwLock::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(0)),
            health_check_handle: Arc::new(StdMutex::new(None)),
        }
    }

    pub async fn create(
        &self,
        session_id: impl Into<String>,
    ) -> Result<InstanceInfo, InstanceError> {
        self.create_with_rom_path(session_id, None).await
    }

    pub async fn create_with_rom_path(
        &self,
        session_id: impl Into<String>,
        rom_path: Option<String>,
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
        let socket_dir = Path::new(&self.config.worker_socket_dir);
        let worker = match self
            .backend
            .create_instance(CreateInstanceOptions {
                instance_id: instance_id.clone(),
                socket_path: socket_dir
                    .join(format!("mgba-{instance_id}.cmd.sock"))
                    .to_string_lossy()
                    .into_owned(),
                frame_socket_path: socket_dir
                    .join(format!("mgba-{instance_id}.frames.sock"))
                    .to_string_lossy()
                    .into_owned(),
                core_path: self.config.libretro_core_path.clone(),
                rom_path: rom_path.or_else(|| self.config.rom_path.clone()),
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
            frame_socket_path: worker.frame_socket_path,
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

    pub async fn destroy_all(&self) -> Result<(), InstanceError> {
        let ids: Vec<String> = self.instances.read().await.keys().cloned().collect();
        let mut first_error = None;

        for id in ids {
            if let Err(error) = self.destroy(&id).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        Ok(())
    }

    pub async fn list(&self) -> Vec<InstanceInfo> {
        self.instances.read().await.values().cloned().collect()
    }

    pub async fn get(&self, instance_id: &str) -> Option<InstanceInfo> {
        self.instances.read().await.get(instance_id).cloned()
    }

    pub async fn reconstruct(&self) -> Result<(), InstanceError> {
        let managed = self.backend.list_managed_instances().await?;
        for worker in managed {
            self.backend.stop_instance(&worker.instance_id).await?;
        }
        self.instances.write().await.clear();
        Ok(())
    }

    pub fn start_health_checks(&self) {
        let manager = self.clone();
        let interval_secs = 10u64;

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(interval_secs)).await;
                let _ = manager.refresh_health_once().await;
            }
        });

        *self.health_check_handle.lock().unwrap() = Some(handle);
    }

    pub fn stop_health_checks(&self) {
        if let Some(handle) = self.health_check_handle.lock().unwrap().take() {
            handle.abort();
        }
    }

    pub async fn mark_error(&self, instance_id: &str) {
        let mut map = self.instances.write().await;
        if let Some(info) = map.get_mut(instance_id) {
            info.status = InstanceStatus::Error;
        }
    }

    pub async fn refresh_health_once(&self) -> Result<(), InstanceError> {
        let instance_list: Vec<(String, String)> = {
            let map = self.instances.read().await;
            map.values()
                .filter(|info| info.status == InstanceStatus::Running)
                .map(|info| (info.id.clone(), info.socket_path.clone()))
                .collect()
        };

        for (id, socket_path) in instance_list {
            let process_alive = self.backend.inspect_running(&id).await?;
            let socket_alive = process_alive && check_worker_alive(&socket_path).await;
            if !process_alive || !socket_alive {
                self.mark_error(&id).await;
            }
        }

        Ok(())
    }

    pub async fn cleanup_stale_sockets(&self) {
        let socket_dir = &self.config.worker_socket_dir;
        if let Ok(mut entries) = tokio::fs::read_dir(socket_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.extension().map(|e| e == "sock").unwrap_or(false) {
                    let _ = tokio::fs::remove_file(&path).await;
                }
            }
        }
    }
}

async fn check_worker_alive(socket_path: &str) -> bool {
    use grokemon_ipc::transport::IpcClient;
    use grokemon_ipc::{WorkerCommand, WorkerCommandV1, WorkerResponse, WorkerResponseV1};

    let Ok(mut client) = IpcClient::connect(socket_path).await else {
        return false;
    };

    matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            client.call(WorkerCommand::V1(WorkerCommandV1::Ping)),
        )
        .await,
        Ok(Ok(WorkerResponse::V1(WorkerResponseV1::Pong)))
    )
}

struct ManagedChild {
    child: Child,
    socket_path: String,
    frame_socket_path: String,
}

pub struct ProcessBackend {
    worker_binary_path: String,
    worker_socket_dir: String,
    shutdown_timeout_ms: u64,
    children: Arc<RwLock<HashMap<String, ManagedChild>>>,
}

impl ProcessBackend {
    pub fn new(config: &GatewayConfig) -> Self {
        Self {
            worker_binary_path: config.worker_binary_path.clone(),
            worker_socket_dir: config.worker_socket_dir.clone(),
            shutdown_timeout_ms: config.worker_shutdown_timeout_ms,
            children: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl InstanceBackend for ProcessBackend {
    async fn create_instance(
        &self,
        opts: CreateInstanceOptions,
    ) -> Result<WorkerInfo, InstanceError> {
        tokio::fs::create_dir_all(&self.worker_socket_dir)
            .await
            .map_err(|error| InstanceError::Process(error.to_string()))?;

        let mut cmd = Command::new(&self.worker_binary_path);
        cmd.arg("--socket")
            .arg(&opts.socket_path)
            .arg("--frame-socket")
            .arg(&opts.frame_socket_path)
            .arg("--core")
            .arg(&opts.core_path);
        if let Some(rom) = &opts.rom_path {
            cmd.arg("--rom").arg(rom);
        }
        cmd.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());

        // SAFETY: pre_exec runs in the child after fork, before exec. The closure only
        // calls prctl, which is async-signal-safe and has no side effects on the parent.
        #[cfg(target_os = "linux")]
        unsafe {
            cmd.pre_exec(|| {
                // SAFETY: prctl is async-signal-safe and only affects the child process.
                unsafe {
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                }
                Ok(())
            });
        }

        let child = cmd
            .spawn()
            .map_err(|error| InstanceError::Process(error.to_string()))?;

        let pid = child.id().unwrap_or(0);

        let socket_path = opts.socket_path.clone();
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
        loop {
            if tokio::fs::metadata(&socket_path).await.is_ok() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err(InstanceError::Process(
                    "worker socket did not appear within 10s".to_string(),
                ));
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        self.children.write().await.insert(
            opts.instance_id.clone(),
            ManagedChild {
                child,
                socket_path: opts.socket_path.clone(),
                frame_socket_path: opts.frame_socket_path.clone(),
            },
        );

        Ok(WorkerInfo {
            pid,
            socket_path: opts.socket_path,
            frame_socket_path: opts.frame_socket_path,
        })
    }

    async fn stop_instance(&self, instance_id: &str) -> Result<(), InstanceError> {
        let entry = {
            let mut children = self.children.write().await;
            children.remove(instance_id)
        };

        if let Some(ManagedChild {
            mut child,
            socket_path,
            frame_socket_path,
        }) = entry
        {
            let pid = child.id().unwrap_or(0);

            #[cfg(unix)]
            if pid > 0 {
                // SAFETY: kill() is safe with a valid PID and signal number; SIGTERM
                // simply requests graceful termination.
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
            }

            let timeout = tokio::time::Duration::from_millis(self.shutdown_timeout_ms);
            match tokio::time::timeout(timeout, child.wait()).await {
                Ok(_) => {}
                Err(_) => {
                    #[cfg(unix)]
                    if pid > 0 {
                        // SAFETY: kill() is safe with a valid PID and SIGKILL.
                        unsafe {
                            libc::kill(pid as libc::pid_t, libc::SIGKILL);
                        }
                    }
                    let _ = child.wait().await;
                }
            }

            let _ = tokio::fs::remove_file(&socket_path).await;
            let _ = tokio::fs::remove_file(&frame_socket_path).await;
        }
        Ok(())
    }

    async fn list_managed_instances(&self) -> Result<Vec<ManagedInstanceInfo>, InstanceError> {
        let children = self.children.read().await;
        Ok(children
            .iter()
            .map(|(id, managed)| ManagedInstanceInfo {
                instance_id: id.clone(),
                pid: managed.child.id().unwrap_or(0),
                socket_path: managed.socket_path.clone(),
            })
            .collect())
    }

    async fn inspect_running(&self, instance_id: &str) -> Result<bool, InstanceError> {
        let children = self.children.read().await;
        if let Some(managed) = children.get(instance_id) {
            Ok(managed.child.id().is_some_and(pid_is_running))
        } else {
            Ok(false)
        }
    }
}

#[cfg(unix)]
fn pid_is_running(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };

    // SAFETY: kill with signal 0 does not deliver a signal. It only asks the
    // kernel whether this process may signal the PID, which is the Unix
    // liveness probe needed here.
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn pid_is_running(_pid: u32) -> bool {
    true
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
        running: Mutex<HashMap<String, bool>>,
        managed: Mutex<Vec<ManagedInstanceInfo>>,
        stopped: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl InstanceBackend for FakeBackend {
        async fn create_instance(
            &self,
            opts: CreateInstanceOptions,
        ) -> Result<WorkerInfo, InstanceError> {
            self.created.lock().unwrap().push(opts.clone());
            self.running
                .lock()
                .unwrap()
                .insert(opts.instance_id.clone(), true);
            self.managed.lock().unwrap().push(ManagedInstanceInfo {
                instance_id: opts.instance_id.clone(),
                pid: 42,
                socket_path: opts.socket_path.clone(),
            });
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(WorkerInfo {
                pid: 42,
                socket_path: opts.socket_path,
                frame_socket_path: opts.frame_socket_path,
            })
        }
        async fn stop_instance(&self, instance_id: &str) -> Result<(), InstanceError> {
            if *self.fail_stop.lock().unwrap() {
                return Err(InstanceError::Backend("stop failed".to_string()));
            }
            self.stopped.lock().unwrap().push(instance_id.to_string());
            self.running.lock().unwrap().remove(instance_id);
            Ok(())
        }
        async fn list_managed_instances(&self) -> Result<Vec<ManagedInstanceInfo>, InstanceError> {
            Ok(self.managed.lock().unwrap().clone())
        }
        async fn inspect_running(&self, instance_id: &str) -> Result<bool, InstanceError> {
            Ok(*self
                .running
                .lock()
                .unwrap()
                .get(instance_id)
                .unwrap_or(&false))
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
    async fn destroy_all_stops_every_tracked_instance() {
        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(
            GatewayConfig {
                max_instances: 20,
                libretro_core_path: "/tmp/core.so".to_string(),
                worker_socket_dir: "/tmp/grokemon-workers".to_string(),
                ..GatewayConfig::default()
            },
            backend.clone(),
        );
        let first = manager.create("session-a").await.unwrap();
        let second = manager.create("session-b").await.unwrap();

        manager.destroy_all().await.unwrap();

        assert!(manager.list().await.is_empty());
        let stopped = backend.stopped.lock().unwrap().clone();
        assert!(stopped.contains(&first.id));
        assert!(stopped.contains(&second.id));
    }

    #[tokio::test]
    async fn create_uses_mgba_prefixed_socket_path_under_worker_socket_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let worker_socket_dir = tmp.path().to_string_lossy().into_owned();
        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(
            GatewayConfig {
                max_instances: 20,
                libretro_core_path: "/tmp/core.so".to_string(),
                worker_socket_dir: worker_socket_dir.clone(),
                ..GatewayConfig::default()
            },
            backend.clone(),
        );

        let info = manager.create("session-a").await.unwrap();

        let created = backend.created.lock().unwrap();
        let opts = created.first().unwrap();
        let expected_socket_path = tmp
            .path()
            .join(format!("mgba-{}.cmd.sock", info.id))
            .to_string_lossy()
            .into_owned();
        let expected_frame_socket_path = tmp
            .path()
            .join(format!("mgba-{}.frames.sock", info.id))
            .to_string_lossy()
            .into_owned();
        assert_eq!(opts.socket_path, expected_socket_path);
        assert_eq!(info.socket_path, expected_socket_path);
        assert_eq!(opts.frame_socket_path, expected_frame_socket_path);
        assert_eq!(info.frame_socket_path, expected_frame_socket_path);
        assert!(opts.socket_path.starts_with(&worker_socket_dir));
        assert!(opts.frame_socket_path.starts_with(&worker_socket_dir));
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
            InstanceError::Backend("stop failed".to_string())
        );
        assert!(manager.get(&info.id).await.is_some());
    }

    #[tokio::test]
    async fn health_check_marks_dead_instance_as_error() {
        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(
            GatewayConfig {
                libretro_core_path: "/tmp/core.so".to_string(),
                worker_socket_dir: "/tmp/test-workers".to_string(),
                ..GatewayConfig::default()
            },
            backend,
        );

        let info = manager.create("session-health").await.unwrap();
        assert_eq!(info.status, InstanceStatus::Running);

        manager.mark_error(&info.id).await;

        let updated = manager.get(&info.id).await.unwrap();
        assert_eq!(updated.status, InstanceStatus::Error);
    }

    #[tokio::test]
    async fn refresh_health_once_marks_exited_worker_as_error() {
        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(
            GatewayConfig {
                libretro_core_path: "/tmp/core.so".to_string(),
                worker_socket_dir: "/tmp/test-workers".to_string(),
                ..GatewayConfig::default()
            },
            backend.clone(),
        );
        let info = manager.create("session-health").await.unwrap();
        backend
            .running
            .lock()
            .unwrap()
            .insert(info.id.clone(), false);

        manager.refresh_health_once().await.unwrap();

        let updated = manager.get(&info.id).await.unwrap();
        assert_eq!(updated.status, InstanceStatus::Error);
    }

    #[tokio::test]
    async fn reconstruct_stops_managed_workers_and_clears_instances() {
        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(
            GatewayConfig {
                libretro_core_path: "/tmp/core.so".to_string(),
                worker_socket_dir: "/tmp/test-workers".to_string(),
                ..GatewayConfig::default()
            },
            backend.clone(),
        );
        let info = manager.create("session-reconstruct").await.unwrap();

        manager.reconstruct().await.unwrap();

        assert!(manager.list().await.is_empty());
        assert_eq!(backend.stopped.lock().unwrap().as_slice(), &[info.id]);
    }

    #[tokio::test]
    async fn start_stop_health_checks() {
        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(GatewayConfig::default(), backend);

        manager.start_health_checks();
        manager.stop_health_checks();
    }

    #[tokio::test]
    async fn cleanup_stale_sockets_removes_dot_sock_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();
        let stale = tmp.path().join("stale.sock");
        let other = tmp.path().join("keep.txt");
        tokio::fs::write(&stale, b"").await.unwrap();
        tokio::fs::write(&other, b"hello").await.unwrap();

        let backend = Arc::new(FakeBackend::default());
        let manager = InstanceManager::new(
            GatewayConfig {
                worker_socket_dir: dir,
                ..GatewayConfig::default()
            },
            backend,
        );

        manager.cleanup_stale_sockets().await;

        assert!(!tokio::fs::try_exists(&stale).await.unwrap());
        assert!(tokio::fs::try_exists(&other).await.unwrap());
    }

    mod process_tests {
        use super::*;

        #[tokio::test]
        async fn process_backend_spawn_and_kill() {
            // Without a real worker that creates a socket, we cannot exercise the full
            // create_instance() lifecycle. Verify that ProcessBackend::new wires the
            // configured worker binary path, socket dir, and shutdown timeout.
            let config = GatewayConfig {
                worker_binary_path: "sleep".to_string(),
                worker_socket_dir: "/tmp/test-mgba-workers".to_string(),
                worker_shutdown_timeout_ms: 1000,
                ..GatewayConfig::default()
            };
            let backend = ProcessBackend::new(&config);
            assert_eq!(backend.worker_binary_path, "sleep");
            assert_eq!(backend.worker_socket_dir, "/tmp/test-mgba-workers");
            assert_eq!(backend.shutdown_timeout_ms, 1000);
            assert!(backend.children.read().await.is_empty());
        }

        #[cfg(unix)]
        #[tokio::test]
        async fn inspect_running_returns_false_when_tracked_pid_is_reaped() {
            let config = GatewayConfig {
                worker_socket_dir: "/tmp/test-mgba-workers".to_string(),
                ..GatewayConfig::default()
            };
            let backend = ProcessBackend::new(&config);
            let child = Command::new("sleep").arg("60").spawn().unwrap();
            let pid = child.id().unwrap();

            backend.children.write().await.insert(
                "reaped".to_string(),
                ManagedChild {
                    child,
                    socket_path: "/tmp/test-mgba-workers/reaped.sock".to_string(),
                    frame_socket_path: "/tmp/test-mgba-workers/reaped.frames.sock".to_string(),
                },
            );

            let pid = libc::pid_t::try_from(pid).unwrap();
            // SAFETY: the test owns this child process. SIGTERM requests termination,
            // and waitpid reaps that exact child PID before inspecting backend state.
            unsafe {
                assert_eq!(libc::kill(pid, libc::SIGTERM), 0);
                let mut status = 0;
                assert_eq!(libc::waitpid(pid, &mut status, 0), pid);
            }

            assert!(!backend.inspect_running("reaped").await.unwrap());
        }

        #[cfg(unix)]
        #[tokio::test]
        async fn stop_instance_removes_command_and_frame_sockets() {
            let dir = tempfile::tempdir().unwrap();
            let socket_path = dir.path().join("worker.cmd.sock");
            let frame_socket_path = dir.path().join("worker.frames.sock");
            tokio::fs::write(&socket_path, b"stale command socket")
                .await
                .unwrap();
            tokio::fs::write(&frame_socket_path, b"stale frame socket")
                .await
                .unwrap();

            let config = GatewayConfig {
                worker_socket_dir: dir.path().to_string_lossy().into_owned(),
                worker_shutdown_timeout_ms: 1000,
                ..GatewayConfig::default()
            };
            let backend = ProcessBackend::new(&config);
            let child = Command::new("sleep").arg("60").spawn().unwrap();

            backend.children.write().await.insert(
                "instance-a".to_string(),
                ManagedChild {
                    child,
                    socket_path: socket_path.to_string_lossy().into_owned(),
                    frame_socket_path: frame_socket_path.to_string_lossy().into_owned(),
                },
            );

            backend.stop_instance("instance-a").await.unwrap();

            assert!(!tokio::fs::try_exists(&socket_path).await.unwrap());
            assert!(!tokio::fs::try_exists(&frame_socket_path).await.unwrap());
        }

        #[tokio::test]
        async fn max_instances_enforced() {
            let backend = Arc::new(FakeBackend::default());
            let manager = InstanceManager::new(
                GatewayConfig {
                    max_instances: 2,
                    libretro_core_path: "/tmp/core.so".to_string(),
                    worker_socket_dir: "/tmp/grokemon-workers".to_string(),
                    ..GatewayConfig::default()
                },
                backend,
            );
            manager.create("session-1").await.unwrap();
            manager.create("session-2").await.unwrap();
            let err = manager.create("session-3").await.unwrap_err();
            assert_eq!(err, InstanceError::MaxInstancesReached);
        }
    }
}
