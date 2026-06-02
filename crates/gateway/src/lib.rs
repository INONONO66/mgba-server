use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use grokemon_auth::{AclService, AuthError, Permission, PrincipalId, SessionId};
use grokemon_mgba::{CommandKind, CommandResult};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;

pub struct GatewayState<C> {
    acl: Arc<RwLock<AclService>>,
    commands: Arc<C>,
}

impl<C> Clone for GatewayState<C> {
    fn clone(&self) -> Self {
        Self {
            acl: self.acl.clone(),
            commands: self.commands.clone(),
        }
    }
}

impl<C> GatewayState<C> {
    pub fn new(acl: AclService, commands: Arc<C>) -> Self {
        Self {
            acl: Arc::new(RwLock::new(acl)),
            commands,
        }
    }
}

#[async_trait]
pub trait SessionCommandService: Send + Sync + 'static {
    async fn send(
        &self,
        session_id: &SessionId,
        kind: CommandKind,
        command: String,
    ) -> Result<CommandResult, String>;
}

pub fn app<C: SessionCommandService>(state: GatewayState<C>) -> Router {
    Router::new()
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({ "ok": true })) }),
        )
        .nest("/api/v2", v2_routes())
        .with_state(state)
}

fn v2_routes<C: SessionCommandService>() -> Router<GatewayState<C>> {
    Router::new()
        .route("/sessions/{session_id}/memory/read8", get(read8::<C>))
        .route("/sessions/{session_id}/memory/read16", get(read16::<C>))
        .route(
            "/sessions/{session_id}/memory/readrange",
            get(read_range::<C>),
        )
        .route("/sessions/{session_id}/input/tap", post(tap::<C>))
        .route("/sessions/{session_id}/input/hold", post(hold::<C>))
        .route("/sessions/{session_id}/logs", get(logs::<C>))
        .route("/sessions/{session_id}/stream", get(stream::<C>))
}

#[derive(Debug, Deserialize)]
struct AddressQuery {
    address: String,
}

#[derive(Debug, Deserialize)]
struct RangeQuery {
    address: String,
    length: String,
}

#[derive(Debug, Deserialize)]
struct ButtonQuery {
    button: String,
    duration: Option<String>,
}

#[derive(Debug, Serialize)]
struct CommandResponse {
    response: String,
    request_id: String,
    latency_ms: i64,
}

async fn read8<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<AddressQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::ReadMemory,
        CommandKind::MemoryRead,
        format!("core.read8,{}<|END|>", query.address),
    )
    .await
}

async fn read16<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<AddressQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::ReadMemory,
        CommandKind::MemoryRead,
        format!("core.read16,{}<|END|>", query.address),
    )
    .await
}

async fn read_range<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<RangeQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::ReadMemory,
        CommandKind::MemoryRead,
        format!("core.readRange,{},{}<|END|>", query.address, query.length),
    )
    .await
}

async fn tap<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<ButtonQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::SendKey,
        CommandKind::Control,
        format!("mgba-http.button.tap,{}<|END|>", query.button),
    )
    .await
}

async fn hold<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<ButtonQuery>,
) -> Response {
    let duration = query.duration.unwrap_or_else(|| "15".to_string());
    command_endpoint(
        state,
        headers,
        session,
        Permission::SendKey,
        CommandKind::Control,
        format!("mgba-http.button.hold,{},{}<|END|>", query.button, duration),
    )
    .await
}

async fn logs<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
) -> Response {
    match authorize(&state, &headers, &SessionId::new(session), Permission::ViewInputLogs).await {
        Ok(principal_id) => Json(serde_json::json!({ "ok": true, "principal_id": principal_id.as_str(), "transport": "sse_or_ws_pending" })).into_response(),
        Err(error) => auth_response(error),
    }
}

async fn stream<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
) -> Response {
    match authorize(&state, &headers, &SessionId::new(session), Permission::ViewStream).await {
        Ok(principal_id) => Json(serde_json::json!({ "ok": true, "principal_id": principal_id.as_str(), "transport": "ws_pending" })).into_response(),
        Err(error) => auth_response(error),
    }
}

async fn command_endpoint<C: SessionCommandService>(
    state: GatewayState<C>,
    headers: HeaderMap,
    session: String,
    permission: Permission,
    kind: CommandKind,
    command: String,
) -> Response {
    let session_id = SessionId::new(session);
    if let Err(error) = authorize(&state, &headers, &session_id, permission).await {
        return auth_response(error);
    }
    match tokio::time::timeout(
        Duration::from_secs(5),
        state.commands.send(&session_id, kind, command),
    )
    .await
    {
        Ok(Ok(result)) => (
            StatusCode::OK,
            Json(CommandResponse {
                response: result.response,
                request_id: result.trace.request_id.to_string(),
                latency_ms: result.trace.caller_latency_ms(),
            }),
        )
            .into_response(),
        Ok(Err(error)) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": error })),
        )
            .into_response(),
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(serde_json::json!({ "error": "timeout" })),
        )
            .into_response(),
    }
}

async fn authorize<C>(
    state: &GatewayState<C>,
    headers: &HeaderMap,
    session_id: &SessionId,
    permission: Permission,
) -> Result<PrincipalId, AuthError> {
    let token = principal_token(headers).ok_or(AuthError::InvalidToken)?;
    state
        .acl
        .read()
        .await
        .check_token(&token, session_id, permission)
}

fn principal_token(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers
        .get("x-principal-token")
        .and_then(|v| v.to_str().ok())
    {
        return Some(value.to_string());
    }
    let auth = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    auth.strip_prefix("Bearer ").map(ToOwned::to_owned)
}

fn auth_response(error: AuthError) -> Response {
    match error {
        AuthError::InvalidToken => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Unauthorized" })),
        )
            .into_response(),
        AuthError::MissingGrant | AuthError::Forbidden => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Forbidden" })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use grokemon_auth::{AclService, Role};
    use grokemon_mgba::{CommandPriority, CommandTrace};
    use std::sync::Mutex;
    use tower::ServiceExt;

    #[derive(Default)]
    struct FakeCommands {
        seen: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SessionCommandService for FakeCommands {
        async fn send(
            &self,
            session_id: &SessionId,
            kind: CommandKind,
            command: String,
        ) -> Result<CommandResult, String> {
            self.seen
                .lock()
                .unwrap()
                .push(format!("{}:{kind:?}:{command}", session_id.as_str()));
            let now = Utc::now();
            Ok(CommandResult {
                response: "ok".to_string(),
                trace: CommandTrace {
                    request_id: uuid::Uuid::new_v4(),
                    instance_id: session_id.as_str().to_string(),
                    kind,
                    priority: CommandPriority::Control,
                    enqueue_at: now,
                    dequeue_at: now,
                    socket_write_at: now,
                    response_at: now,
                    caller_complete_at: now,
                },
            })
        }
    }

    fn fixture(role: Role, session: &str) -> (Router, Arc<FakeCommands>, String) {
        let mut acl = AclService::new();
        let principal = grokemon_auth::PrincipalId::new("principal-a");
        let token = acl.issue_principal_token(principal.clone()).token;
        acl.grant(principal, SessionId::new(session), role);
        let commands = Arc::new(FakeCommands::default());
        (
            app(GatewayState::new(acl, commands.clone())),
            commands,
            token,
        )
    }

    #[tokio::test]
    async fn controller_can_send_v2_key_command() {
        let (app, commands, token) = fixture(Role::Controller, "session-a");
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/v2/sessions/session-a/input/tap?button=A")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:Control:mgba-http.button.tap,A<|END|>"
        );
    }

    #[tokio::test]
    async fn viewer_is_forbidden_from_memory_read() {
        let (app, commands, token) = fixture(Role::Viewer, "session-a");
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/api/v2/sessions/session-a/memory/read8?address=0xD35E")
                    .header("authorization", format!("Bearer {token}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(commands.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cross_session_token_fails_before_handler() {
        let (app, commands, token) = fixture(Role::Controller, "session-a");
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/v2/sessions/session-b/input/tap?button=A")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(commands.seen.lock().unwrap().is_empty());
    }
}
