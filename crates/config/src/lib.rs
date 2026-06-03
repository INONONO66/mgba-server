use serde::{Deserialize, Serialize};
use std::{env, num::ParseIntError};
use thiserror::Error;

const DEFAULT_PORT: u16 = 8787;
const DEFAULT_MAX_INSTANCES: u16 = 20;
const DEFAULT_EMULATOR_PORT: u16 = 8888;
const DEFAULT_EMULATOR_MEMORY_BYTES: u64 = 768 * 1024 * 1024;
const DEFAULT_CAPTURE_INTERVAL_MS: u64 = 8;
const DEFAULT_SOURCE_CAPTURE_INTERVAL_MS: u64 = 60_000;
const DEFAULT_STREAM_KEYFRAME_INTERVAL: u32 = 60;
const DEFAULT_STREAM_TILE_SIZE: u16 = 16;
const DEFAULT_WS_BACKPRESSURE_LIMIT: usize = 262_144;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub port: u16,
    pub admin_token: String,
    pub max_instances: u16,
    pub emulator_image: String,
    pub emulator_port: u16,
    pub emulator_memory_bytes: u64,
    pub capture_interval_ms: u64,
    pub source_capture_interval_ms: u64,
    pub capture_root: String,
    pub worker_binary_path: String,
    pub libretro_core_path: String,
    pub worker_socket_dir: String,
    pub worker_shutdown_timeout_ms: u64,
    pub h264_enabled: bool,
    pub stream_keyframe_interval: u32,
    pub stream_tile_size: u16,
    pub ws_backpressure_limit: usize,
    pub network_name: String,
    pub rom_path: Option<String>,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            admin_token: "dev-admin-token".to_string(),
            max_instances: DEFAULT_MAX_INSTANCES,
            emulator_image: "grokemon-emulator".to_string(),
            emulator_port: DEFAULT_EMULATOR_PORT,
            emulator_memory_bytes: DEFAULT_EMULATOR_MEMORY_BYTES,
            capture_interval_ms: DEFAULT_CAPTURE_INTERVAL_MS,
            source_capture_interval_ms: DEFAULT_SOURCE_CAPTURE_INTERVAL_MS,
            capture_root: "/tmp/grokemon-captures".to_string(),
            worker_binary_path: "./target/debug/worker".to_string(),
            libretro_core_path: "".to_string(),
            worker_socket_dir: "/tmp/mgba-workers".to_string(),
            worker_shutdown_timeout_ms: 2_000,
            h264_enabled: false,
            stream_keyframe_interval: DEFAULT_STREAM_KEYFRAME_INTERVAL,
            stream_tile_size: DEFAULT_STREAM_TILE_SIZE,
            ws_backpressure_limit: DEFAULT_WS_BACKPRESSURE_LIMIT,
            network_name: "grokemon-net".to_string(),
            rom_path: None,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{name} must be present and non-empty")]
    Empty { name: &'static str },
    #[error("{name} has invalid integer value: {detail}")]
    ParseInt { name: &'static str, detail: String },
    #[error("{name} must be in range {min}..={max}, got {value}")]
    Range {
        name: &'static str,
        min: u64,
        max: u64,
        value: u64,
    },
}

pub fn load_from_env() -> Result<GatewayConfig, ConfigError> {
    let defaults = GatewayConfig::default();
    Ok(GatewayConfig {
        port: read_u16("PORT", defaults.port, 1, u16::MAX)?,
        admin_token: read_string("ADMIN_TOKEN", defaults.admin_token)?,
        max_instances: read_u16("MAX_INSTANCES", defaults.max_instances, 1, 20)?,
        emulator_image: read_string("EMULATOR_IMAGE", defaults.emulator_image)?,
        emulator_port: read_u16("EMULATOR_PORT", defaults.emulator_port, 1, u16::MAX)?,
        emulator_memory_bytes: read_u64(
            "EMULATOR_MEMORY_BYTES",
            defaults.emulator_memory_bytes,
            1,
            u64::MAX,
        )?,
        capture_interval_ms: read_u64(
            "CAPTURE_INTERVAL_MS",
            defaults.capture_interval_ms,
            1,
            10_000,
        )?,
        source_capture_interval_ms: read_u64(
            "SOURCE_CAPTURE_INTERVAL_MS",
            defaults.source_capture_interval_ms,
            1,
            u64::MAX,
        )?,
        capture_root: read_string("CAPTURE_ROOT", defaults.capture_root)?,
        worker_binary_path: read_string("WORKER_BINARY_PATH", defaults.worker_binary_path)?,
        libretro_core_path: read_string_allow_empty(
            "LIBRETRO_CORE_PATH",
            defaults.libretro_core_path,
        )?,
        worker_socket_dir: read_string("WORKER_SOCKET_DIR", defaults.worker_socket_dir)?,
        worker_shutdown_timeout_ms: read_u64(
            "WORKER_SHUTDOWN_TIMEOUT_MS",
            defaults.worker_shutdown_timeout_ms,
            100,
            30_000,
        )?,
        h264_enabled: read_bool("H264_ENABLED", defaults.h264_enabled)?,
        stream_keyframe_interval: read_u32(
            "STREAM_KEYFRAME_INTERVAL",
            defaults.stream_keyframe_interval,
            1,
            u32::MAX,
        )?,
        stream_tile_size: read_u16("STREAM_TILE_SIZE", defaults.stream_tile_size, 4, 128)?,
        ws_backpressure_limit: read_usize(
            "WS_BACKPRESSURE_LIMIT",
            defaults.ws_backpressure_limit,
            1024,
            10_485_760,
        )?,
        network_name: read_string("NETWORK_NAME", defaults.network_name)?,
        rom_path: env::var("ROM_PATH").ok().filter(|value| !value.is_empty()),
    })
}

fn read_string(name: &'static str, default: String) -> Result<String, ConfigError> {
    match env::var(name) {
        Ok(value) if value.is_empty() => Err(ConfigError::Empty { name }),
        Ok(value) => Ok(value),
        Err(_) => Ok(default),
    }
}

fn read_string_allow_empty(name: &'static str, default: String) -> Result<String, ConfigError> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(_) => Ok(default),
    }
}

fn read_bool(name: &'static str, default: bool) -> Result<bool, ConfigError> {
    match env::var(name) {
        Ok(value) => match value.as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => Err(ConfigError::ParseInt {
                name,
                detail: format!("invalid boolean value: {value}"),
            }),
        },
        Err(_) => Ok(default),
    }
}

fn parse_u64(name: &'static str, value: &str) -> Result<u64, ConfigError> {
    value
        .parse::<u64>()
        .map_err(|source: ParseIntError| ConfigError::ParseInt {
            name,
            detail: source.to_string(),
        })
}

fn range(name: &'static str, value: u64, min: u64, max: u64) -> Result<u64, ConfigError> {
    if (min..=max).contains(&value) {
        Ok(value)
    } else {
        Err(ConfigError::Range {
            name,
            min,
            max,
            value,
        })
    }
}

fn read_u64(name: &'static str, default: u64, min: u64, max: u64) -> Result<u64, ConfigError> {
    match env::var(name) {
        Ok(value) => range(name, parse_u64(name, &value)?, min, max),
        Err(_) => Ok(default),
    }
}

fn read_u32(name: &'static str, default: u32, min: u32, max: u32) -> Result<u32, ConfigError> {
    Ok(read_u64(name, default as u64, min as u64, max as u64)? as u32)
}

fn read_u16(name: &'static str, default: u16, min: u16, max: u16) -> Result<u16, ConfigError> {
    Ok(read_u64(name, default as u64, min as u64, max as u64)? as u16)
}

fn read_usize(
    name: &'static str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, ConfigError> {
    Ok(read_u64(name, default as u64, min as u64, max as u64)? as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn defaults_to_twenty_instances() {
        let _guard = env_lock().lock().unwrap();
        // SAFETY: test-only, single-threaded test environment
        unsafe {
            env::remove_var("MAX_INSTANCES");
            env::remove_var("ADMIN_TOKEN");
        }
        let config = load_from_env().unwrap();
        assert_eq!(config.max_instances, 20);
        assert_eq!(config.admin_token, "dev-admin-token");
    }

    #[test]
    fn rejects_more_than_twenty_instances() {
        let _guard = env_lock().lock().unwrap();
        // SAFETY: test-only, single-threaded test environment
        unsafe {
            env::set_var("MAX_INSTANCES", "21");
        }
        let error = load_from_env().unwrap_err();
        // SAFETY: test-only, single-threaded test environment
        unsafe {
            env::remove_var("MAX_INSTANCES");
        }
        assert!(matches!(
            error,
            ConfigError::Range {
                name: "MAX_INSTANCES",
                ..
            }
        ));
    }

    #[test]
    fn loads_worker_and_streaming_config_from_env() {
        let _guard = env_lock().lock().unwrap();
        // SAFETY: test-only, single-threaded test environment
        unsafe {
            env::set_var("WORKER_BINARY_PATH", "/tmp/worker");
            env::set_var("LIBRETRO_CORE_PATH", "");
            env::set_var("WORKER_SOCKET_DIR", "/tmp/workers");
            env::set_var("WORKER_SHUTDOWN_TIMEOUT_MS", "2500");
            env::set_var("H264_ENABLED", "1");
        }

        let config = load_from_env().unwrap();

        // SAFETY: test-only, single-threaded test environment
        unsafe {
            env::remove_var("WORKER_BINARY_PATH");
            env::remove_var("LIBRETRO_CORE_PATH");
            env::remove_var("WORKER_SOCKET_DIR");
            env::remove_var("WORKER_SHUTDOWN_TIMEOUT_MS");
            env::remove_var("H264_ENABLED");
        }

        assert_eq!(config.worker_binary_path, "/tmp/worker");
        assert_eq!(config.libretro_core_path, "");
        assert_eq!(config.worker_socket_dir, "/tmp/workers");
        assert_eq!(config.worker_shutdown_timeout_ms, 2500);
        assert!(config.h264_enabled);
    }
}
