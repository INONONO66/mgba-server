use axum::Router;
use grokemon_auth::AclService;
use grokemon_config::load_from_env;
use grokemon_gateway::admin::{AdminState, admin_routes};
use grokemon_gateway::{GatewayState, IpcSessionCommandService, app};
use grokemon_instances::{InstanceManager, ProcessBackend};
use grokemon_streaming::{
    BroadcastConfig, DashboardBroadcast, FrameHub, InputLogBus, StreamMetrics,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    let config = match load_from_env() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Config error: {e}");
            std::process::exit(1);
        }
    };

    println!("Starting gateway on {}:{}", config.bind_host, config.port);
    println!(
        "Admin token: {}",
        if config.admin_token.is_empty() {
            "(not set)"
        } else {
            "(set)"
        }
    );
    println!("Max instances: {}", config.max_instances);

    let acl = Arc::new(RwLock::new(AclService::new()));
    let backend = Arc::new(ProcessBackend::new(&config));
    let manager = Arc::new(InstanceManager::new(config.clone(), backend));
    manager.start_health_checks();
    let commands = Arc::new(IpcSessionCommandService::new(manager.clone()));
    let frame_hub = Arc::new(FrameHub::new());
    let input_log = Arc::new(InputLogBus::new());

    let stream_metrics = Arc::new(StreamMetrics::new());
    let broadcast = Arc::new(DashboardBroadcast::with_config_and_metrics(
        frame_hub.clone(),
        BroadcastConfig {
            keyframe_interval: config.stream_keyframe_interval,
            backpressure_limit: config.ws_backpressure_limit,
            tile_size: config.stream_tile_size,
            h264_enabled: config.h264_enabled,
        },
        stream_metrics.clone(),
    ));
    let session_state = GatewayState::with_acl_and_broadcast(
        acl.clone(),
        commands,
        frame_hub.clone(),
        input_log,
        broadcast,
    );
    let admin_state = AdminState::new(
        config.admin_token.clone(),
        manager.clone(),
        stream_metrics,
        acl,
        frame_hub,
    );

    let app: Router = app(session_state).nest(
        "/admin",
        admin_routes::<ProcessBackend>().with_state(admin_state),
    );

    let addr = format!("{}:{}", config.bind_host, config.port);
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

    if let Err(error) = manager.destroy_all().await {
        eprintln!("Failed to stop all instances during shutdown: {error}");
    }
    manager.stop_health_checks();

    println!("Gateway stopped");
}
