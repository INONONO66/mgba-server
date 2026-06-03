use async_trait::async_trait;
use grokemon_ipc::transport::IpcClient;
use grokemon_ipc::{WorkerCommand, WorkerCommandV1, WorkerResponse, WorkerResponseV1};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{MgbaTransport, SUCCESS_MARKER, SchedulerError, TERMINATION_MARKER};

pub struct IpcTransport {
    client: Arc<Mutex<IpcClient>>,
}

impl IpcTransport {
    pub fn new(client: IpcClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }

    pub async fn connect(socket_path: &str) -> Result<Self, std::io::Error> {
        let client = IpcClient::connect(socket_path)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(Self::new(client))
    }
}

#[async_trait]
impl MgbaTransport for IpcTransport {
    async fn send(&self, message: String) -> Result<String, SchedulerError> {
        let command = parse_message_to_command(&message)
            .ok_or_else(|| SchedulerError::Transport(format!("unknown command: {message}")))?;

        let mut client = self.client.lock().await;
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.call(command),
        )
        .await
        .map_err(|_| SchedulerError::Timeout)?
        .map_err(|e| SchedulerError::Transport(e.to_string()))?;

        Ok(response_to_string(response))
    }
}

fn parse_message_to_command(message: &str) -> Option<WorkerCommand> {
    // Strip termination marker; messages without it are rejected.
    let body = message.strip_suffix(TERMINATION_MARKER)?;
    let mut parts = body.splitn(10, ',');
    let cmd_name = parts.next()?;

    let cmd = match cmd_name {
        "core.read8" => {
            let address = parse_address(parts.next()?)?;
            WorkerCommandV1::ReadMemory { address, size: 1 }
        }
        "core.read16" => {
            let address = parse_address(parts.next()?)?;
            WorkerCommandV1::ReadMemory { address, size: 2 }
        }
        "core.readRange" => {
            let address = parse_address(parts.next()?)?;
            let size = parts.next()?.parse::<u32>().ok()?;
            WorkerCommandV1::ReadMemory { address, size }
        }
        "core.write8" => {
            let address = parse_address(parts.next()?)?;
            let value = parse_address(parts.next()?)? as u8;
            WorkerCommandV1::WriteMemory {
                address,
                data: vec![value],
            }
        }
        "core.write16" => {
            let address = parse_address(parts.next()?)?;
            let value = parse_address(parts.next()?)? as u16;
            WorkerCommandV1::WriteMemory {
                address,
                data: value.to_le_bytes().to_vec(),
            }
        }
        "core.write32" => {
            let address = parse_address(parts.next()?)?;
            let value = parse_address(parts.next()?)?;
            WorkerCommandV1::WriteMemory {
                address,
                data: value.to_le_bytes().to_vec(),
            }
        }
        "core.currentFrame" => WorkerCommandV1::GetCurrentFrame,
        "core.screenshot" => WorkerCommandV1::TakeScreenshot,
        "core.saveStateSlot" => {
            let slot = parts.next()?.parse::<u8>().ok()?;
            WorkerCommandV1::SaveState { slot }
        }
        "core.loadStateSlot" => {
            let slot = parts.next()?.parse::<u8>().ok()?;
            WorkerCommandV1::LoadState { slot }
        }
        "core.reset" | "restart" => WorkerCommandV1::Reset,
        "mgba-http.button.tap" => {
            let button = parts.next()?.to_string();
            WorkerCommandV1::ButtonTap { button }
        }
        "mgba-http.button.hold" => {
            let button = parts.next()?.to_string();
            let duration_frames = parts
                .next()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(15);
            WorkerCommandV1::ButtonHold {
                button,
                duration_frames,
            }
        }
        "health" => WorkerCommandV1::Ping,
        _ => return None,
    };

    Some(WorkerCommand::V1(cmd))
}

fn parse_address(s: &str) -> Option<u32> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u32::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u32>().ok()
    }
}

fn response_to_string(response: WorkerResponse) -> String {
    match response {
        WorkerResponse::V1(WorkerResponseV1::Pong) => format!("pong{SUCCESS_MARKER}"),
        WorkerResponse::V1(WorkerResponseV1::Ok) => format!("ok{SUCCESS_MARKER}"),
        WorkerResponse::V1(WorkerResponseV1::MemoryData { data }) => match data.len() {
            1 => format!("{}{SUCCESS_MARKER}", data[0]),
            2 => {
                let val = u16::from_le_bytes([data[0], data[1]]);
                format!("{val}{SUCCESS_MARKER}")
            }
            _ => {
                let hex: String = data.iter().map(|b| format!("{b:02x}")).collect();
                format!("{hex}{SUCCESS_MARKER}")
            }
        },
        WorkerResponse::V1(WorkerResponseV1::CurrentFrame { frame_number }) => {
            format!("{frame_number}{SUCCESS_MARKER}")
        }
        WorkerResponse::V1(WorkerResponseV1::StateData { .. }) => {
            format!("ok{SUCCESS_MARKER}")
        }
        WorkerResponse::V1(WorkerResponseV1::Screenshot { .. }) => {
            format!("ok{SUCCESS_MARKER}")
        }
        WorkerResponse::V1(WorkerResponseV1::Error { message }) => {
            format!("error:{message}")
        }
        _ => format!("ok{SUCCESS_MARKER}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_read8_command() {
        let msg = format!("core.read8,0x02000000{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(
            cmd,
            WorkerCommand::V1(WorkerCommandV1::ReadMemory {
                address: 0x02000000,
                size: 1
            })
        ));
    }

    #[test]
    fn parse_read16_command() {
        let msg = format!("core.read16,0x02000000{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(
            cmd,
            WorkerCommand::V1(WorkerCommandV1::ReadMemory {
                address: 0x02000000,
                size: 2
            })
        ));
    }

    #[test]
    fn parse_read_range_command() {
        let msg = format!("core.readRange,0x02000000,16{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(
            cmd,
            WorkerCommand::V1(WorkerCommandV1::ReadMemory {
                address: 0x02000000,
                size: 16
            })
        ));
    }

    #[test]
    fn parse_write8_command() {
        let msg = format!("core.write8,0x02000000,42{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        match cmd {
            WorkerCommand::V1(WorkerCommandV1::WriteMemory { address, data }) => {
                assert_eq!(address, 0x02000000);
                assert_eq!(data, vec![42]);
            }
            _ => panic!("expected WriteMemory"),
        }
    }

    #[test]
    fn parse_button_tap_command() {
        let msg = format!("mgba-http.button.tap,A{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(
            cmd,
            WorkerCommand::V1(WorkerCommandV1::ButtonTap { .. })
        ));
        if let WorkerCommand::V1(WorkerCommandV1::ButtonTap { button }) = cmd {
            assert_eq!(button, "A");
        }
    }

    #[test]
    fn parse_button_hold_command() {
        let msg = format!("mgba-http.button.hold,B,30{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(
            cmd,
            WorkerCommand::V1(WorkerCommandV1::ButtonHold {
                duration_frames: 30,
                ..
            })
        ));
    }

    #[test]
    fn parse_button_hold_uses_default_when_missing_duration() {
        let msg = format!("mgba-http.button.hold,B{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(
            cmd,
            WorkerCommand::V1(WorkerCommandV1::ButtonHold {
                duration_frames: 15,
                ..
            })
        ));
    }

    #[test]
    fn parse_save_state_command() {
        let msg = format!("core.saveStateSlot,1{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(
            cmd,
            WorkerCommand::V1(WorkerCommandV1::SaveState { slot: 1 })
        ));
    }

    #[test]
    fn parse_load_state_command() {
        let msg = format!("core.loadStateSlot,2{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(
            cmd,
            WorkerCommand::V1(WorkerCommandV1::LoadState { slot: 2 })
        ));
    }

    #[test]
    fn parse_reset_command() {
        let msg = format!("core.reset{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(cmd, WorkerCommand::V1(WorkerCommandV1::Reset)));
    }

    #[test]
    fn parse_restart_aliases_to_reset() {
        let msg = format!("restart{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(cmd, WorkerCommand::V1(WorkerCommandV1::Reset)));
    }

    #[test]
    fn parse_health_command() {
        let msg = format!("health{TERMINATION_MARKER}");
        let cmd = parse_message_to_command(&msg).unwrap();
        assert!(matches!(cmd, WorkerCommand::V1(WorkerCommandV1::Ping)));
    }

    #[test]
    fn unknown_command_returns_none() {
        let msg = format!("unknown.command{TERMINATION_MARKER}");
        assert!(parse_message_to_command(&msg).is_none());
    }

    #[test]
    fn message_without_termination_returns_none() {
        let msg = "core.read8,0x02000000".to_string();
        assert!(parse_message_to_command(&msg).is_none());
    }

    #[test]
    fn memory_data_single_byte_response() {
        let response = WorkerResponse::V1(WorkerResponseV1::MemoryData { data: vec![42] });
        let s = response_to_string(response);
        assert!(s.contains("42"));
        assert!(s.contains(SUCCESS_MARKER));
    }

    #[test]
    fn memory_data_two_byte_response_decodes_le() {
        let response =
            WorkerResponse::V1(WorkerResponseV1::MemoryData { data: vec![0x34, 0x12] });
        let s = response_to_string(response);
        assert!(s.contains("4660"));
    }

    #[test]
    fn memory_data_range_response_is_hex() {
        let response = WorkerResponse::V1(WorkerResponseV1::MemoryData {
            data: vec![0xde, 0xad, 0xbe, 0xef],
        });
        let s = response_to_string(response);
        assert!(s.contains("deadbeef"));
    }

    #[test]
    fn error_response_prefixed_with_error() {
        let response = WorkerResponse::V1(WorkerResponseV1::Error {
            message: "boom".to_string(),
        });
        let s = response_to_string(response);
        assert!(s.starts_with("error:"));
        assert!(s.contains("boom"));
    }

    #[test]
    fn current_frame_response_contains_frame_number() {
        let response =
            WorkerResponse::V1(WorkerResponseV1::CurrentFrame { frame_number: 12345 });
        let s = response_to_string(response);
        assert!(s.contains("12345"));
    }
}
