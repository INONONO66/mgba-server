use axum::Router;
use grokemon_auth::AclService;
use grokemon_config::load_from_env;
use grokemon_gateway::admin::{AdminState, admin_routes};
use grokemon_gateway::{GatewayState, SessionCommandService, app};
use grokemon_instances::{InstanceManager, ProcessBackend};
use grokemon_streaming::FrameHub;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let config = match load_from_env() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Config error: {e}");
            std::process::exit(1);
        }
    };

    println!("Starting gateway on port {}", config.port);
    println!(
        "Admin token: {}",
        if config.admin_token.is_empty() {
            "(not set)"
        } else {
            "(set)"
        }
    );
    println!("Max instances: {}", config.max_instances);

    let acl = AclService::new();
    let backend = Arc::new(ProcessBackend::new(&config));
    let manager = Arc::new(InstanceManager::new(config.clone(), backend));
    let commands = Arc::new(StubCommandService);
    let frame_hub = Arc::new(FrameHub::new());

    let session_state = GatewayState::new(acl, commands, frame_hub);
    let admin_state = AdminState {
        admin_token: config.admin_token.clone(),
        manager: manager.clone(),
    };

    let app: Router = app(session_state).nest(
        "/admin",
        admin_routes::<ProcessBackend>().with_state(admin_state),
    );

    let addr = format!("0.0.0.0:{}", config.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Failed to bind to {addr}: {e}");
            std::process::exit(1);
        }
    };

    println!("Listening on {addr}");

    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            let mut sigint = signal(SignalKind::interrupt()).unwrap();
            tokio::select! {
                _ = sigterm.recv() => {
                    println!("Received SIGTERM, shutting down...");
                }
                _ = sigint.recv() => {
                    println!("Received SIGINT, shutting down...");
                }
            }
        }

        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.unwrap();
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .unwrap();

    println!("Gateway stopped");
}

struct StubCommandService;

#[async_trait::async_trait]
impl SessionCommandService for StubCommandService {
    async fn send(
        &self,
        _session_id: &grokemon_auth::SessionId,
        _kind: grokemon_mgba::CommandKind,
        _command: String,
    ) -> Result<grokemon_mgba::CommandResult, String> {
        Err("not yet implemented — IPC transport pending Task 15".to_string())
    }
}
