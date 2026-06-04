use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use grokemon_auth::{AclService, PrincipalId, Role, SessionId};
use grokemon_instances::{InstanceBackend, InstanceError, InstanceManager};
use grokemon_ipc::transport::{FrameConnection, RawFramePacket};
use grokemon_streaming::{FrameHub, PixelFormat, RawFrame, StreamMetrics};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::RwLock, task::JoinHandle};

pub struct AdminState<B: InstanceBackend> {
    pub admin_token: String,
    pub manager: Arc<InstanceManager<B>>,
    pub stream_metrics: Arc<StreamMetrics>,
    pub acl: Arc<RwLock<AclService>>,
    pub frame_hub: Arc<FrameHub>,
    frame_ingest_tasks: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
}

impl<B: InstanceBackend> Clone for AdminState<B> {
    fn clone(&self) -> Self {
        Self {
            admin_token: self.admin_token.clone(),
            manager: self.manager.clone(),
            stream_metrics: self.stream_metrics.clone(),
            acl: self.acl.clone(),
            frame_hub: self.frame_hub.clone(),
            frame_ingest_tasks: self.frame_ingest_tasks.clone(),
        }
    }
}

impl<B: InstanceBackend> AdminState<B> {
    pub fn new(
        admin_token: impl Into<String>,
        manager: Arc<InstanceManager<B>>,
        stream_metrics: Arc<StreamMetrics>,
        acl: Arc<RwLock<AclService>>,
        frame_hub: Arc<FrameHub>,
    ) -> Self {
        Self {
            admin_token: admin_token.into(),
            manager,
            stream_metrics,
            acl,
            frame_hub,
            frame_ingest_tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub fn admin_routes<B: InstanceBackend>() -> Router<AdminState<B>> {
    Router::new()
        .route("/instances", post(create_instance::<B>))
        .route("/instances", get(list_instances::<B>))
        .route("/instances/{id}", get(get_instance::<B>))
        .route("/instances/{id}", delete(destroy_instance::<B>))
        .route("/metrics/streams", get(stream_metrics::<B>))
}

fn check_admin_token(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get("x-admin-token")
        .and_then(|value| value.to_str().ok())
        .map(|token| token == expected)
        .unwrap_or(false)
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({ "error": "Unauthorized" })),
    )
        .into_response()
}

async fn create_instance<B: InstanceBackend>(
    State(state): State<AdminState<B>>,
    headers: HeaderMap,
) -> Response {
    if !check_admin_token(&headers, &state.admin_token) {
        return unauthorized();
    }
    let session_id = uuid::Uuid::new_v4().to_string();
    match state.manager.create(session_id).await {
        Ok(info) => {
            let principal_id = PrincipalId::new(&info.principal_session_id);
            let mut acl = state.acl.write().await;
            acl.register_principal_token(principal_id.clone(), &info.principal_token);
            acl.grant(
                principal_id,
                SessionId::new(&info.principal_session_id),
                Role::Owner,
            );
            state
                .frame_hub
                .register_instance(info.principal_session_id.clone())
                .await;
            let handle = spawn_frame_ingest(
                info.principal_session_id.clone(),
                info.frame_socket_path.clone(),
                state.frame_hub.clone(),
            );
            state
                .frame_ingest_tasks
                .write()
                .await
                .insert(info.id.clone(), handle);
            (StatusCode::OK, Json(info)).into_response()
        }
        Err(InstanceError::MaxInstancesReached) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "max instances reached" })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn list_instances<B: InstanceBackend>(
    State(state): State<AdminState<B>>,
    headers: HeaderMap,
) -> Response {
    if !check_admin_token(&headers, &state.admin_token) {
        return unauthorized();
    }
    let instances = state.manager.list().await;
    (StatusCode::OK, Json(instances)).into_response()
}

async fn get_instance<B: InstanceBackend>(
    State(state): State<AdminState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !check_admin_token(&headers, &state.admin_token) {
        return unauthorized();
    }
    match state.manager.get(&id).await {
        Some(info) => (StatusCode::OK, Json(info)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "not found" })),
        )
            .into_response(),
    }
}

async fn destroy_instance<B: InstanceBackend>(
    State(state): State<AdminState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !check_admin_token(&headers, &state.admin_token) {
        return unauthorized();
    }
    let session_id = state
        .manager
        .get(&id)
        .await
        .map(|info| info.principal_session_id);
    match state.manager.destroy(&id).await {
        Ok(()) => {
            if let Some(handle) = state.frame_ingest_tasks.write().await.remove(&id) {
                handle.abort();
            }
            if let Some(session_id) = session_id {
                state.frame_hub.unregister_instance(&session_id).await;
            }
            (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
        }
        Err(InstanceError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "not found" })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn stream_metrics<B: InstanceBackend>(
    State(state): State<AdminState<B>>,
    headers: HeaderMap,
) -> Response {
    if !check_admin_token(&headers, &state.admin_token) {
        return unauthorized();
    }
    let snapshot = state.stream_metrics.snapshot().await;
    (StatusCode::OK, Json(snapshot)).into_response()
}

fn spawn_frame_ingest(
    session_id: String,
    frame_socket_path: String,
    frame_hub: Arc<FrameHub>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        for _ in 0..100 {
            match FrameConnection::connect(&frame_socket_path).await {
                Ok(mut connection) => {
                    while let Ok(packet) = connection.recv_frame().await {
                        if let Some(frame) = frame_from_packet(packet) {
                            frame_hub.push_frame(&session_id, frame).await;
                        }
                    }
                    return;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
}

fn frame_from_packet(packet: RawFramePacket) -> Option<RawFrame> {
    let pixel_format = match packet.pixel_format {
        0 => PixelFormat::XRGB8888,
        1 => PixelFormat::RGB565,
        _ => return None,
    };
    Some(RawFrame {
        width: packet.width,
        height: packet.height,
        pitch: packet.pitch,
        pixel_format,
        data: packet.data,
        sequence: 0,
        timestamp_ms: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use grokemon_config::GatewayConfig;
    use grokemon_instances::{
        CreateInstanceOptions, InstanceBackend, InstanceError, InstanceManager,
        ManagedInstanceInfo, WorkerInfo,
    };
    use grokemon_ipc::transport::{FrameSocketServer, RawFramePacket};
    use tower::ServiceExt;

    #[derive(Default)]
    struct FakeBackend;

    #[async_trait]
    impl InstanceBackend for FakeBackend {
        async fn create_instance(
            &self,
            opts: CreateInstanceOptions,
        ) -> Result<WorkerInfo, InstanceError> {
            Ok(WorkerInfo {
                pid: 42,
                socket_path: opts.socket_path,
                frame_socket_path: opts.frame_socket_path,
            })
        }
        async fn stop_instance(&self, _instance_id: &str) -> Result<(), InstanceError> {
            Ok(())
        }
        async fn list_managed_instances(&self) -> Result<Vec<ManagedInstanceInfo>, InstanceError> {
            Ok(Vec::new())
        }
        async fn inspect_running(&self, _instance_id: &str) -> Result<bool, InstanceError> {
            Ok(true)
        }
    }

    fn make_app() -> (Router, String) {
        make_app_with_max(20)
    }

    fn make_app_with_max(max_instances: u16) -> (Router, String) {
        let config = GatewayConfig {
            admin_token: "test-token".to_string(),
            max_instances,
            libretro_core_path: "/tmp/core.so".to_string(),
            worker_socket_dir: "/tmp/test-workers".to_string(),
            ..GatewayConfig::default()
        };
        let manager = Arc::new(InstanceManager::new(config, Arc::new(FakeBackend)));
        let acl = Arc::new(RwLock::new(AclService::new()));
        let stream_metrics = Arc::new(StreamMetrics::new());
        let frame_hub = Arc::new(FrameHub::new());
        let state = AdminState::new("test-token", manager, stream_metrics, acl, frame_hub);
        let admin = admin_routes::<FakeBackend>().with_state(state);
        let app = Router::new().nest("/admin", admin);
        (app, "test-token".to_string())
    }

    async fn read_body(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn frame_ingest_pushes_socket_frames_into_hub() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir
            .path()
            .join("frames.sock")
            .to_string_lossy()
            .into_owned();
        let server = FrameSocketServer::bind(&socket_path).unwrap();
        let frame_hub = Arc::new(FrameHub::new());
        frame_hub.register_instance("session-a").await;
        let ingest = spawn_frame_ingest("session-a".to_string(), socket_path, frame_hub.clone());

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection
                .send_frame(&RawFramePacket {
                    width: 2,
                    height: 1,
                    pitch: 8,
                    pixel_format: 0,
                    data: vec![1, 2, 3, 4, 5, 6, 7, 8],
                })
                .await
                .unwrap();
        });

        let mut received = None;
        for _ in 0..20 {
            received = frame_hub.latest_frame("session-a").await;
            if received.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        ingest.abort();
        server_task.await.unwrap();
        let frame = received.unwrap();
        assert_eq!(frame.width, 2);
        assert_eq!(frame.height, 1);
        assert_eq!(frame.pitch, 8);
        assert_eq!(frame.pixel_format, PixelFormat::XRGB8888);
        assert_eq!(frame.data, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(frame.sequence, 1);
    }

    #[tokio::test]
    async fn missing_admin_token_returns_401() {
        let (app, _) = make_app();
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/admin/instances")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_admin_token_returns_401() {
        let (app, _) = make_app();
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/admin/instances")
                    .header("x-admin-token", "wrong-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn create_instance_with_valid_token_returns_200() {
        let (app, token) = make_app();
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/admin/instances")
                    .header("x-admin-token", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = read_body(response).await;
        assert!(body.contains("\"id\""));
        assert!(body.contains("\"principal_token\""));
    }

    #[tokio::test]
    async fn list_instances_returns_empty_array_initially() {
        let (app, token) = make_app();
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/admin/instances")
                    .header("x-admin-token", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = read_body(response).await;
        assert_eq!(body, "[]");
    }

    #[tokio::test]
    async fn get_nonexistent_instance_returns_404() {
        let (app, token) = make_app();
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/admin/instances/nonexistent-id")
                    .header("x-admin-token", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_nonexistent_instance_returns_404() {
        let (app, token) = make_app();
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("DELETE")
                    .uri("/admin/instances/nonexistent-id")
                    .header("x-admin-token", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn max_instances_reached_returns_503() {
        let (app, token) = make_app_with_max(1);
        let first = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/admin/instances")
                    .header("x-admin-token", token.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/admin/instances")
                    .header("x-admin-token", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn stream_metrics_returns_placeholder() {
        let (app, token) = make_app();
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/admin/metrics/streams")
                    .header("x-admin-token", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = read_body(response).await;
        assert!(body.contains("\"instances\""));
    }

    #[tokio::test]
    async fn stream_metrics_requires_token() {
        let (app, _) = make_app();
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/admin/metrics/streams")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn full_create_get_destroy_lifecycle() {
        let (app, token) = make_app();

        // Create
        let create = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/admin/instances")
                    .header("x-admin-token", token.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::OK);
        let body = read_body(create).await;
        let info: serde_json::Value = serde_json::from_str(&body).unwrap();
        let id = info["id"].as_str().unwrap().to_string();

        // Get
        let get = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .uri(format!("/admin/instances/{id}"))
                    .header("x-admin-token", token.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::OK);

        // Destroy
        let destroy = app
            .clone()
            .oneshot(
                http::Request::builder()
                    .method("DELETE")
                    .uri(format!("/admin/instances/{id}"))
                    .header("x-admin-token", token.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(destroy.status(), StatusCode::OK);

        // Get after destroy -> 404
        let post_destroy = app
            .oneshot(
                http::Request::builder()
                    .uri(format!("/admin/instances/{id}"))
                    .header("x-admin-token", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(post_destroy.status(), StatusCode::NOT_FOUND);
    }
}
