use async_trait::async_trait;
use grokemon_auth::SessionId;
use grokemon_instances::{InstanceBackend, InstanceManager};
use grokemon_mgba::{CommandKind, CommandResult, CommandScheduler, IpcTransport};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

use crate::SessionCommandService;

pub struct IpcSessionCommandService<B: InstanceBackend> {
    manager: Arc<InstanceManager<B>>,
    schedulers: RwLock<HashMap<String, Arc<CommandScheduler>>>,
}

#[async_trait]
impl<B: InstanceBackend> SessionCommandService for IpcSessionCommandService<B> {
    async fn send(
        &self,
        session_id: &SessionId,
        kind: CommandKind,
        command: String,
    ) -> Result<CommandResult, String> {
        let instances = self.manager.list().await;
        let instance = instances
            .into_iter()
            .find(|info| info.principal_session_id == session_id.as_str())
            .ok_or_else(|| format!("no instance for principal session {}", session_id.as_str()))?;

        let scheduler = self
            .get_or_create_scheduler(
                &instance.principal_session_id,
                &instance.id,
                &instance.socket_path,
            )
            .await?;
        scheduler
            .submit(kind, command, Duration::from_secs(5))
            .await
            .map_err(|error| error.to_string())
    }
}

impl<B: InstanceBackend> IpcSessionCommandService<B> {
    pub fn new(manager: Arc<InstanceManager<B>>) -> Self {
        Self {
            manager,
            schedulers: RwLock::new(HashMap::new()),
        }
    }

    async fn get_or_create_scheduler(
        &self,
        session_id: &str,
        instance_id: &str,
        socket_path: &str,
    ) -> Result<Arc<CommandScheduler>, String> {
        if let Some(scheduler) = self.read_cached_scheduler(session_id).await {
            return Ok(scheduler);
        }

        let mut schedulers = self.schedulers.write().await;
        if let Some(scheduler) = schedulers.get(session_id) {
            return Ok(scheduler.clone());
        }

        let transport = IpcTransport::connect(socket_path)
            .await
            .map_err(|error| error.to_string())?;

        let scheduler = Arc::new(CommandScheduler::new(
            instance_id.to_string(),
            Arc::new(transport),
            16,
        ));

        schedulers.insert(session_id.to_string(), scheduler.clone());
        Ok(scheduler)
    }

    async fn read_cached_scheduler(&self, session_id: &str) -> Option<Arc<CommandScheduler>> {
        let schedulers = self.schedulers.read().await;
        schedulers.get(session_id).cloned()
    }
}
