pub mod admin;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use grokemon_auth::{AclService, AuthError, Permission, PrincipalId, SessionId};
use grokemon_instances::InstanceBackend;
use grokemon_mgba::{CommandKind, CommandResult, format_message};
use grokemon_streaming::{FrameHub, InputLogBus, PixelFormat, RawFrame};
use serde::{Deserialize, Serialize};
use std::{io::Cursor, sync::Arc, time::Duration};
use tokio::sync::RwLock;

pub struct GatewayState<C> {
    acl: Arc<RwLock<AclService>>,
    commands: Arc<C>,
    frame_hub: Arc<FrameHub>,
    input_log: Arc<InputLogBus>,
}

impl<C> Clone for GatewayState<C> {
    fn clone(&self) -> Self {
        Self {
            acl: self.acl.clone(),
            commands: self.commands.clone(),
            frame_hub: self.frame_hub.clone(),
            input_log: self.input_log.clone(),
        }
    }
}

impl<C> GatewayState<C> {
    pub fn new(
        acl: AclService,
        commands: Arc<C>,
        frame_hub: Arc<FrameHub>,
        input_log: Arc<InputLogBus>,
    ) -> Self {
        Self {
            acl: Arc::new(RwLock::new(acl)),
            commands,
            frame_hub,
            input_log,
        }
    }

    pub fn frame_hub(&self) -> &Arc<FrameHub> {
        &self.frame_hub
    }

    pub fn input_log(&self) -> &Arc<InputLogBus> {
        &self.input_log
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
        .route(
            "/ws/sessions/{session_id}/input-log",
            get(input_log_ws::<C>),
        )
        .nest("/api", api_routes())
        .with_state(state)
}

pub fn app_with_admin<C, B>(state: GatewayState<C>, admin: admin::AdminState<B>) -> Router
where
    C: SessionCommandService,
    B: InstanceBackend,
{
    let admin_router = admin::admin_routes::<B>().with_state(admin);
    app(state).nest("/admin", admin_router)
}

fn api_routes<C: SessionCommandService>() -> Router<GatewayState<C>> {
    Router::new()
        .route(
            "/sessions/{session_id}/core/currentframe",
            get(current_frame::<C>),
        )
        .route("/sessions/{session_id}/core/read8", get(read8::<C>))
        .route("/sessions/{session_id}/core/read16", get(read16::<C>))
        .route(
            "/sessions/{session_id}/core/readrange",
            get(read_range::<C>),
        )
        .route("/sessions/{session_id}/core/write8", post(write8::<C>))
        .route("/sessions/{session_id}/core/write16", post(write16::<C>))
        .route("/sessions/{session_id}/core/write32", post(write32::<C>))
        .route(
            "/sessions/{session_id}/core/savestateslot",
            post(save_state::<C>),
        )
        .route(
            "/sessions/{session_id}/core/loadstateslot",
            post(load_state::<C>),
        )
        .route("/sessions/{session_id}/core/reset", post(reset::<C>))
        .route(
            "/sessions/{session_id}/core/screenshot",
            post(screenshot::<C>),
        )
        .route("/sessions/{session_id}/screenshot", get(screenshot::<C>))
        .route(
            "/sessions/{session_id}/mgba-http/button/tap",
            post(tap::<C>),
        )
        .route(
            "/sessions/{session_id}/mgba-http/button/hold",
            post(hold::<C>),
        )
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

#[derive(Debug, Deserialize)]
struct WriteQuery {
    address: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct SlotQuery {
    slot: String,
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
        read_command("core.read8", &[query.address]),
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
        read_command("core.read16", &[query.address]),
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
        read_command("core.readRange", &[query.address, query.length]),
    )
    .await
}

async fn current_frame<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::ReadMemory,
        CommandKind::FrameCapture,
        format_message("core.currentFrame", &[] as &[&str]),
    )
    .await
}

async fn write8<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<WriteQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::SendKey,
        CommandKind::MemoryRead,
        format_message("core.write8", &[query.address, query.value]),
    )
    .await
}

async fn write16<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<WriteQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::SendKey,
        CommandKind::MemoryRead,
        format_message("core.write16", &[query.address, query.value]),
    )
    .await
}

async fn write32<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<WriteQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::SendKey,
        CommandKind::MemoryRead,
        format_message("core.write32", &[query.address, query.value]),
    )
    .await
}

async fn save_state<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<SlotQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::SendKey,
        CommandKind::State,
        format_message("core.saveStateSlot", &[query.slot]),
    )
    .await
}

async fn load_state<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    Query(query): Query<SlotQuery>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::SendKey,
        CommandKind::State,
        format_message("core.loadStateSlot", &[query.slot]),
    )
    .await
}

async fn reset<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
) -> Response {
    command_endpoint(
        state,
        headers,
        session,
        Permission::SendKey,
        CommandKind::Control,
        format_message("core.reset", &[] as &[&str]),
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
        button_command("mgba-http.button.tap", query.button, None),
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
        button_command("mgba-http.button.hold", query.button, Some(duration)),
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

async fn input_log_ws<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
    ws: axum::extract::WebSocketUpgrade,
) -> Response {
    let session_id = SessionId::new(session.clone());
    if let Err(error) = authorize(&state, &headers, &session_id, Permission::ViewInputLogs).await {
        return auth_response(error);
    }
    let input_log = state.input_log.clone();
    ws.on_upgrade(move |socket| async move {
        handle_input_log_ws(socket, session, input_log).await;
    })
}

async fn handle_input_log_ws(
    mut socket: axum::extract::ws::WebSocket,
    session_id: String,
    input_log: Arc<InputLogBus>,
) {
    let recent = input_log.recent(&session_id).await;
    for event in recent {
        if let Ok(json) = serde_json::to_string(&event)
            && socket
                .send(axum::extract::ws::Message::Text(json.into()))
                .await
                .is_err()
        {
            return;
        }
    }
    let mut rx = input_log.subscribe(&session_id).await;
    loop {
        match rx.recv().await {
            Ok(event) => {
                if let Ok(json) = serde_json::to_string(&event)
                    && socket
                        .send(axum::extract::ws::Message::Text(json.into()))
                        .await
                        .is_err()
                {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
        }
    }
}

async fn screenshot<C: SessionCommandService>(
    State(state): State<GatewayState<C>>,
    headers: HeaderMap,
    Path(session): Path<String>,
) -> Response {
    let session_id = SessionId::new(session.clone());
    if let Err(error) = authorize(&state, &headers, &session_id, Permission::ViewStream).await {
        return auth_response(error);
    }

    match state.frame_hub.latest_frame(&session).await {
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "no frame available yet" })),
        )
            .into_response(),
        Some(raw_frame) => match xrgb8888_to_png(&raw_frame) {
            Ok(png_bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "image/png")],
                png_bytes,
            )
                .into_response(),
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": error })),
            )
                .into_response(),
        },
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
    let command = match validate_command(command) {
        Ok(command) => command,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response();
        }
    };
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

fn read_command(command: &str, args: &[String]) -> String {
    format_message(command, args)
}

fn button_command(command: &str, button: String, duration: Option<String>) -> String {
    match duration {
        Some(duration) => format_message(command, &[button, duration]),
        None => format_message(command, &[button]),
    }
}

fn validate_command(command: String) -> Result<String, &'static str> {
    let body = command
        .strip_suffix(grokemon_mgba::TERMINATION_MARKER)
        .ok_or("invalid command terminator")?;
    let mut parts = body.split(',');
    let command_name = parts.next().ok_or("missing command")?;
    match command_name {
        "core.read8" | "core.read16" => {
            let address = parts.next().ok_or("missing address")?;
            if parts.next().is_some() || !is_numeric_arg(address) {
                return Err("invalid address");
            }
        }
        "core.readRange" => {
            let address = parts.next().ok_or("missing address")?;
            let length = parts.next().ok_or("missing length")?;
            if parts.next().is_some() || !is_numeric_arg(address) || !is_decimal_arg(length) {
                return Err("invalid range");
            }
        }
        "core.write8" | "core.write16" | "core.write32" => {
            let address = parts.next().ok_or("missing address")?;
            let value = parts.next().ok_or("missing value")?;
            if parts.next().is_some() || !is_numeric_arg(address) || !is_numeric_arg(value) {
                return Err("invalid write args");
            }
        }
        "core.saveStateSlot" | "core.loadStateSlot" => {
            let slot = parts.next().ok_or("missing slot")?;
            if parts.next().is_some() || !is_decimal_arg(slot) {
                return Err("invalid slot");
            }
        }
        "core.reset" => {
            if parts.next().is_some() {
                return Err("reset takes no args");
            }
        }
        "core.currentFrame" => {
            if parts.next().is_some() {
                return Err("currentFrame takes no args");
            }
        }
        "mgba-http.button.tap" => {
            let button = parts.next().ok_or("missing button")?;
            if parts.next().is_some() || !is_valid_button(button) {
                return Err("invalid button");
            }
        }
        "mgba-http.button.hold" => {
            let button = parts.next().ok_or("missing button")?;
            let duration = parts.next().ok_or("missing duration")?;
            if parts.next().is_some() || !is_valid_button(button) || !is_decimal_arg(duration) {
                return Err("invalid hold");
            }
        }
        _ => return Err("unsupported command"),
    }
    Ok(command)
}

fn is_numeric_arg(value: &str) -> bool {
    is_decimal_arg(value)
        || value
            .strip_prefix("0x")
            .is_some_and(|hex| !hex.is_empty() && hex.chars().all(|ch| ch.is_ascii_hexdigit()))
}

fn is_decimal_arg(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn is_valid_button(value: &str) -> bool {
    matches!(
        value,
        "A" | "B" | "L" | "R" | "Start" | "Select" | "Up" | "Down" | "Left" | "Right"
    )
}

fn xrgb8888_to_png(frame: &RawFrame) -> Result<Vec<u8>, String> {
    if frame.pixel_format != PixelFormat::XRGB8888 {
        return Err("unsupported pixel format".to_string());
    }

    let width = frame.width;
    let height = frame.height;
    let pitch = frame.pitch as usize;
    let data = &frame.data;

    let mut img = image::ImageBuffer::<image::Rgba<u8>, Vec<u8>>::new(width, height);

    for y in 0..height {
        for x in 0..width {
            let px = (y as usize * pitch) + (x as usize * 4);
            if px + 3 >= data.len() {
                return Err("frame buffer is too small".to_string());
            }
            let b = data[px];
            let g = data[px + 1];
            let r = data[px + 2];
            img.put_pixel(x, y, image::Rgba([r, g, b, 255]));
        }
    }

    let mut png_cursor = Cursor::new(Vec::new());
    img.write_to(&mut png_cursor, image::ImageFormat::Png)
        .map_err(|e| e.to_string())?;
    Ok(png_cursor.into_inner())
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
    use grokemon_streaming::{FrameHub, InputLogBus, PixelFormat, RawFrame};
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

    async fn fixture(
        role: Role,
        session: &str,
    ) -> (
        Router,
        Arc<FakeCommands>,
        String,
        Arc<FrameHub>,
        Arc<InputLogBus>,
    ) {
        let mut acl = AclService::new();
        let principal = grokemon_auth::PrincipalId::new("principal-a");
        let token = acl.issue_principal_token(principal.clone()).token;
        acl.grant(principal, SessionId::new(session), role);
        let commands = Arc::new(FakeCommands::default());
        let frame_hub = Arc::new(FrameHub::new());
        frame_hub.register_instance(session).await;
        let input_log = Arc::new(InputLogBus::new());
        (
            app(GatewayState::new(
                acl,
                commands.clone(),
                frame_hub.clone(),
                input_log.clone(),
            )),
            commands,
            token,
            frame_hub,
            input_log,
        )
    }

    #[tokio::test]
    async fn controller_can_send_key_command() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/mgba-http/button/tap?button=A")
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
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Viewer, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/api/sessions/session-a/core/read8?address=0xD35E")
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
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-b/mgba-http/button/tap?button=A")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(commands.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_control_args_are_rejected_before_socket_send() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/mgba-http/button/tap?button=A%2Ccore.read8")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(commands.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_memory_args_are_rejected_before_socket_send() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/api/sessions/session-a/core/readrange?address=0xD35E&length=1%2Ccore.read8")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(commands.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn controller_can_write8() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/write8?address=0xD35E&value=0x42")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:MemoryRead:core.write8,0xD35E,0x42<|END|>"
        );
    }

    #[tokio::test]
    async fn controller_can_save_state_slot() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/savestateslot?slot=3")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:State:core.saveStateSlot,3<|END|>"
        );
    }

    #[tokio::test]
    async fn controller_can_load_state_slot() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/loadstateslot?slot=1")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:State:core.loadStateSlot,1<|END|>"
        );
    }

    #[tokio::test]
    async fn controller_can_reset() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/reset")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:Control:core.reset<|END|>"
        );
    }

    #[tokio::test]
    async fn current_frame_returns_capture() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/api/sessions/session-a/core/currentframe")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:FrameCapture:core.currentFrame<|END|>"
        );
    }

    #[tokio::test]
    async fn missing_token_returns_unauthorized() {
        let (app, commands, _token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/reset")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(commands.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_write_args_rejected() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/write8?address=0xD35E&value=notanumber")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(commands.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_slot_rejected() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/savestateslot?slot=abc")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(commands.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn screenshot_returns_503_when_no_frame() {
        let (app, _commands, token, _frame_hub, _input_log) =
            fixture(Role::Viewer, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/screenshot")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn controller_can_read16() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/api/sessions/session-a/core/read16?address=0xD35E")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:MemoryRead:core.read16,0xD35E<|END|>"
        );
    }

    #[tokio::test]
    async fn controller_can_read_range() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/api/sessions/session-a/core/readrange?address=0xD35E&length=3")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:MemoryRead:core.readRange,0xD35E,3<|END|>"
        );
    }

    #[tokio::test]
    async fn controller_can_read8() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/api/sessions/session-a/core/read8?address=0xD35E")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:MemoryRead:core.read8,0xD35E<|END|>"
        );
    }

    #[tokio::test]
    async fn controller_can_button_hold() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/mgba-http/button/hold?button=B&duration=15")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:Control:mgba-http.button.hold,B,15<|END|>"
        );
    }

    #[tokio::test]
    async fn controller_button_hold_default_duration() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/mgba-http/button/hold?button=Start")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            commands.seen.lock().unwrap()[0],
            "session-a:Control:mgba-http.button.hold,Start,15<|END|>"
        );
    }

    #[tokio::test]
    async fn invalid_button_rejected() {
        let (app, commands, token, _frame_hub, _input_log) =
            fixture(Role::Controller, "session-a").await;
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/mgba-http/button/tap?button=NotAButton")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(commands.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn screenshot_returns_png_for_latest_frame() {
        let (app, _commands, token, frame_hub, _input_log) =
            fixture(Role::Viewer, "session-a").await;
        frame_hub
            .push_frame(
                "session-a",
                RawFrame {
                    width: 240,
                    height: 160,
                    pitch: 240 * 4,
                    pixel_format: PixelFormat::XRGB8888,
                    data: [0x00, 0x00, 0xff, 0xff].repeat(240 * 160),
                    sequence: 0,
                    timestamp_ms: 0,
                },
            )
            .await;

        let response = app
            .oneshot(
                http::Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/core/screenshot")
                    .header("x-principal-token", token)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/png"
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let image = image::load_from_memory(&body).unwrap();
        assert_eq!(image.width(), 240);
        assert_eq!(image.height(), 160);
    }
}
