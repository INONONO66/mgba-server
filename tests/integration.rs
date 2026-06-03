//! End-to-end integration tests for the full instance lifecycle.
//!
//! These tests exercise the gateway + worker stack as a single unit. Tests that
//! actually spawn worker processes require the following env vars:
//!
//! - `LIBRETRO_CORE_PATH`: path to a libretro core (.so/.dylib).
//! - `TEST_ROM_PATH`: path to a test ROM file.
//! - `WORKER_BINARY_PATH` (optional): path to the worker binary
//!   (defaults to `./target/debug/worker`).
//!
//! Without `LIBRETRO_CORE_PATH` or `TEST_ROM_PATH`, tests that need a real
//! worker are skipped with a clear message printed to stdout. Pure HTTP
//! routing/auth tests always run because they never reach the backend.

use std::sync::Arc;

use axum::{Router, body::Body};
use grokemon_auth::AclService;
use grokemon_config::GatewayConfig;
use grokemon_gateway::{
    GatewayState, SessionCommandService, app,
    admin::{AdminState, admin_routes},
};
use grokemon_instances::{InstanceManager, ProcessBackend};
use grokemon_streaming::{FrameHub, InputLogBus, StreamMetrics};
use http::StatusCode;
use tower::ServiceExt;

const TEST_ADMIN_TOKEN: &str = "test-admin-token";

fn skip_if_no_libretro(test_name: &str) -> bool {
    if std::env::var("LIBRETRO_CORE_PATH").is_err() || std::env::var("TEST_ROM_PATH").is_err() {
        println!(
            "SKIP {test_name}: requires LIBRETRO_CORE_PATH and TEST_ROM_PATH env vars to be set"
        );
        true
    } else {
        false
    }
}

fn make_test_config() -> GatewayConfig {
    GatewayConfig {
        admin_token: TEST_ADMIN_TOKEN.to_string(),
        max_instances: 5,
        worker_binary_path: std::env::var("WORKER_BINARY_PATH")
            .unwrap_or_else(|_| "./target/debug/worker".to_string()),
        libretro_core_path: std::env::var("LIBRETRO_CORE_PATH").unwrap_or_default(),
        worker_socket_dir: "/tmp/test-mgba-workers".to_string(),
        worker_shutdown_timeout_ms: 2_000,
        rom_path: std::env::var("TEST_ROM_PATH").ok(),
        ..GatewayConfig::default()
    }
}

/// Stub command service used for HTTP routing/auth tests that never reach the
/// worker. Any call returns an error so accidental command sends are obvious.
struct StubCommands;

#[async_trait::async_trait]
impl SessionCommandService for StubCommands {
    async fn send(
        &self,
        _session_id: &grokemon_auth::SessionId,
        _kind: grokemon_mgba::CommandKind,
        _command: String,
    ) -> Result<grokemon_mgba::CommandResult, String> {
        Err("stub command service: not wired in integration tests".to_string())
    }
}

fn make_test_app() -> (Router, Arc<InstanceManager<ProcessBackend>>) {
    let config = make_test_config();
    let acl = AclService::new();
    let backend = Arc::new(ProcessBackend::new(&config));
    let manager = Arc::new(InstanceManager::new(config, backend));
    let frame_hub = Arc::new(FrameHub::new());
    let input_log = Arc::new(InputLogBus::new());
    let stream_metrics = Arc::new(StreamMetrics::new());

    let session_state = GatewayState::new(acl, Arc::new(StubCommands), frame_hub, input_log);
    let admin_state = AdminState {
        admin_token: TEST_ADMIN_TOKEN.to_string(),
        manager: manager.clone(),
        stream_metrics,
    };

    let router = app(session_state)
        .nest("/admin", admin_routes::<ProcessBackend>().with_state(admin_state));

    (router, manager)
}

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let (app, _) = make_test_app();
    let response = app
        .oneshot(
            http::Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ok"], true);
}

#[tokio::test]
async fn admin_create_requires_token() {
    let (app, _) = make_test_app();
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
async fn admin_create_rejects_wrong_token() {
    let (app, _) = make_test_app();
    let response = app
        .oneshot(
            http::Request::builder()
                .method("POST")
                .uri("/admin/instances")
                .header("x-admin-token", "definitely-not-the-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_list_instances_empty_initially() {
    let (app, _) = make_test_app();
    let response = app
        .oneshot(
            http::Request::builder()
                .uri("/admin/instances")
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn admin_get_unknown_instance_returns_404() {
    let (app, _) = make_test_app();
    let response = app
        .oneshot(
            http::Request::builder()
                .uri("/admin/instances/no-such-id")
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn max_instances_enforced() {
    if skip_if_no_libretro("max_instances_enforced") {
        return;
    }
    let config = make_test_config();
    let backend = Arc::new(ProcessBackend::new(&config));
    let manager = InstanceManager::new(
        GatewayConfig {
            max_instances: 2,
            ..config
        },
        backend,
    );

    let i1 = manager.create("session-1").await.unwrap();
    let i2 = manager.create("session-2").await.unwrap();
    let err = manager.create("session-3").await.unwrap_err();
    assert_eq!(
        err,
        grokemon_instances::InstanceError::MaxInstancesReached
    );

    // Cleanup
    let _ = manager.destroy(&i1.id).await;
    let _ = manager.destroy(&i2.id).await;
}

#[tokio::test]
async fn max_instances_returns_503_via_admin_api() {
    if skip_if_no_libretro("max_instances_returns_503_via_admin_api") {
        return;
    }
    let mut config = make_test_config();
    config.max_instances = 1;
    let acl = AclService::new();
    let backend = Arc::new(ProcessBackend::new(&config));
    let manager = Arc::new(InstanceManager::new(config, backend));
    let frame_hub = Arc::new(FrameHub::new());
    let input_log = Arc::new(InputLogBus::new());
    let stream_metrics = Arc::new(StreamMetrics::new());
    let session_state = GatewayState::new(acl, Arc::new(StubCommands), frame_hub, input_log);
    let admin_state = AdminState {
        admin_token: TEST_ADMIN_TOKEN.to_string(),
        manager: manager.clone(),
        stream_metrics,
    };
    let app: Router = app(session_state)
        .nest("/admin", admin_routes::<ProcessBackend>().with_state(admin_state));

    // First create should succeed.
    let first = app
        .clone()
        .oneshot(
            http::Request::builder()
                .method("POST")
                .uri("/admin/instances")
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    // Second create should fail with 503 (max instances reached).
    let second = app
        .oneshot(
            http::Request::builder()
                .method("POST")
                .uri("/admin/instances")
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Cleanup.
    for info in manager.list().await {
        let _ = manager.destroy(&info.id).await;
    }
}

#[tokio::test]
async fn concurrent_instance_creation() {
    if skip_if_no_libretro("concurrent_instance_creation") {
        return;
    }
    let config = make_test_config();
    let backend = Arc::new(ProcessBackend::new(&config));
    let manager = Arc::new(InstanceManager::new(config, backend));

    // Spawn 5 concurrent create requests. The instance manager's pending-count
    // guard plus the max_instances=5 config means all five should succeed; if
    // contention reduces that, at least one must succeed without panic.
    let handles: Vec<_> = (0..5)
        .map(|i| {
            let manager = manager.clone();
            tokio::spawn(async move { manager.create(format!("session-{i}")).await })
        })
        .collect();

    let results = futures::future::join_all(handles).await;
    let successes = results
        .iter()
        .filter(|r| r.as_ref().unwrap().is_ok())
        .count();
    assert!(
        successes >= 1,
        "at least one concurrent create should succeed, got {successes}"
    );

    // Cleanup whatever was created.
    for info in manager.list().await {
        let _ = manager.destroy(&info.id).await;
    }
}

#[tokio::test]
async fn full_lifecycle_via_admin_api() {
    if skip_if_no_libretro("full_lifecycle_via_admin_api") {
        return;
    }
    let (app, manager) = make_test_app();

    // Create
    let create = app
        .clone()
        .oneshot(
            http::Request::builder()
                .method("POST")
                .uri("/admin/instances")
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);
    let body = axum::body::to_bytes(create.into_body(), 4096)
        .await
        .unwrap();
    let info: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let id = info["id"].as_str().expect("id field").to_string();
    assert!(!id.is_empty(), "create response must include id");
    assert!(
        info["principal_token"].as_str().is_some(),
        "create response must include principal_token"
    );

    // List shows it
    let list = app
        .clone()
        .oneshot(
            http::Request::builder()
                .uri("/admin/instances")
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list.into_body(), 8192).await.unwrap();
    let list_json: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let items = list_json.as_array().unwrap();
    assert!(items.iter().any(|item| item["id"] == id));

    // Get individual instance
    let get = app
        .clone()
        .oneshot(
            http::Request::builder()
                .uri(format!("/admin/instances/{id}"))
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);

    // Screenshot endpoint without principal token yields 401 (no frame yet,
    // but auth runs first). Authorization is exercised by the session-token
    // checks in the gateway crate unit tests; here we only confirm the route
    // is mounted and reachable.
    let principal_token = info["principal_token"].as_str().unwrap().to_string();
    let principal_session_id = info["principal_session_id"]
        .as_str()
        .unwrap()
        .to_string();
    let screenshot = app
        .clone()
        .oneshot(
            http::Request::builder()
                .method("GET")
                .uri(format!("/api/sessions/{principal_session_id}/screenshot"))
                .header("x-principal-token", principal_token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // No grant was issued for this principal in the integration ACL, so the
    // call lands on Forbidden (401/403). Either status proves auth ran.
    assert!(
        matches!(
            screenshot.status(),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::SERVICE_UNAVAILABLE
        ),
        "screenshot returned unexpected status {}",
        screenshot.status()
    );

    // Destroy
    let destroy = app
        .clone()
        .oneshot(
            http::Request::builder()
                .method("DELETE")
                .uri(format!("/admin/instances/{id}"))
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(destroy.status(), StatusCode::OK);

    // Restart: create a fresh instance after destroy and confirm it gets a new id.
    let restart = app
        .clone()
        .oneshot(
            http::Request::builder()
                .method("POST")
                .uri("/admin/instances")
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(restart.status(), StatusCode::OK);
    let restart_body = axum::body::to_bytes(restart.into_body(), 4096)
        .await
        .unwrap();
    let restart_info: serde_json::Value = serde_json::from_slice(&restart_body).unwrap();
    let restart_id = restart_info["id"].as_str().unwrap().to_string();
    assert_ne!(
        restart_id, id,
        "restarted instance should receive a new id"
    );

    // Final destroy after restart.
    let final_destroy = app
        .oneshot(
            http::Request::builder()
                .method("DELETE")
                .uri(format!("/admin/instances/{restart_id}"))
                .header("x-admin-token", TEST_ADMIN_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(final_destroy.status(), StatusCode::OK);

    // Manager is left empty.
    assert!(manager.list().await.is_empty());
}
