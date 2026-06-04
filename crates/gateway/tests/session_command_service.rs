use async_trait::async_trait;
use grokemon_auth::SessionId;
use grokemon_config::GatewayConfig;
use grokemon_gateway::{IpcSessionCommandService, SessionCommandService as _};
use grokemon_instances::{
    CreateInstanceOptions, InstanceBackend, InstanceError, InstanceManager, WorkerInfo,
};
use grokemon_ipc::{
    WorkerCommand, WorkerCommandV1, WorkerResponse, WorkerResponseV1, transport::IpcServer,
};
use grokemon_mgba::{CommandKind, SUCCESS_MARKER, format_message};
use std::sync::Arc;

#[derive(Default)]
struct FakeBackend;

#[async_trait]
impl InstanceBackend for FakeBackend {
    async fn create_instance(
        &self,
        opts: CreateInstanceOptions,
    ) -> Result<WorkerInfo, InstanceError> {
        Ok(WorkerInfo {
            pid: 1234,
            socket_path: opts.socket_path,
            frame_socket_path: opts.frame_socket_path,
        })
    }

    async fn stop_instance(&self, _instance_id: &str) -> Result<(), InstanceError> {
        Ok(())
    }

    async fn list_managed_instances(
        &self,
    ) -> Result<Vec<grokemon_instances::ManagedInstanceInfo>, InstanceError> {
        Ok(Vec::new())
    }

    async fn inspect_running(&self, _instance_id: &str) -> Result<bool, InstanceError> {
        Ok(true)
    }
}

#[tokio::test]
async fn session_service_routes_to_ipc_by_principal_session_id() {
    let socket_dir = tempfile::Builder::new()
        .prefix("gw-ipc-")
        .tempdir_in("/tmp")
        .unwrap();
    let socket_dir = socket_dir.path().to_string_lossy().to_string();
    let config = GatewayConfig {
        worker_socket_dir: socket_dir.clone(),
        libretro_core_path: "/tmp/core.so".to_string(),
        ..GatewayConfig::default()
    };
    let manager = Arc::new(InstanceManager::new(config, Arc::new(FakeBackend)));
    let info = manager.create("principal-session-id").await.unwrap();

    let server = IpcServer::bind(&info.socket_path).unwrap();
    let service = IpcSessionCommandService::new(manager);

    let worker = tokio::spawn(async move {
        let mut connection = server.accept().await.unwrap();
        let command = connection.recv_command().await.unwrap();
        assert_eq!(
            command,
            WorkerCommand::V1(WorkerCommandV1::ReadMemory {
                address: 0x0200_0000,
                size: 1
            })
        );

        connection
            .send_response(&WorkerResponse::V1(WorkerResponseV1::MemoryData {
                data: vec![42],
            }))
            .await
            .unwrap();
    });

    let result = service
        .send(
            &SessionId::new("principal-session-id"),
            CommandKind::MemoryRead,
            format_message("core.read8", &["0x02000000".to_string()]),
        )
        .await
        .unwrap();

    assert_eq!(result.response, format!("42{SUCCESS_MARKER}"));

    worker.await.unwrap();
}
