use serde::{Deserialize, Serialize};

pub mod transport;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PixelFormat {
    XRGB8888,
    RGB565,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkerCommand {
    V1(WorkerCommandV1),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkerCommandV1 {
    Ping,
    LoadRom {
        path: String,
    },
    SetInputState {
        buttons: u32,
    },
    ReadMemory {
        address: u32,
        size: u32,
    },
    WriteMemory {
        address: u32,
        data: Vec<u8>,
    },
    SaveState {
        slot: u8,
    },
    LoadState {
        slot: u8,
    },
    Reset,
    Shutdown,
    GetCurrentFrame,
    ButtonTap {
        button: String,
    },
    ButtonHold {
        button: String,
        duration_frames: u32,
    },
    TakeScreenshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkerResponse {
    V1(WorkerResponseV1),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkerResponseV1 {
    Pong,
    Frame {
        width: u32,
        height: u32,
        pitch: u32,
        pixel_format: PixelFormat,
        data: Vec<u8>,
    },
    MemoryData {
        data: Vec<u8>,
    },
    StateData {
        data: Vec<u8>,
    },
    CurrentFrame {
        frame_number: u64,
    },
    Screenshot {
        png_data: Vec<u8>,
    },
    Ok,
    Error {
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    >(
        value: &T,
    ) {
        let json = serde_json::to_string(value).unwrap();
        let decoded: T = serde_json::from_str(&json).unwrap();
        assert_eq!(value, &decoded);
    }

    #[test]
    fn ping_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::Ping));
    }

    #[test]
    fn read_memory_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::ReadMemory {
            address: 0x0200_0000,
            size: 4,
        }));
    }

    #[test]
    fn button_tap_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::ButtonTap {
            button: "A".to_string(),
        }));
    }

    #[test]
    fn frame_response_roundtrip() {
        roundtrip(&WorkerResponse::V1(WorkerResponseV1::Frame {
            width: 240,
            height: 160,
            pitch: 960,
            pixel_format: PixelFormat::XRGB8888,
            data: vec![0u8; 240 * 160 * 4],
        }));
    }

    #[test]
    fn error_response_roundtrip() {
        roundtrip(&WorkerResponse::V1(WorkerResponseV1::Error {
            message: "test error".to_string(),
        }));
    }

    #[test]
    fn pong_response_roundtrip() {
        roundtrip(&WorkerResponse::V1(WorkerResponseV1::Pong));
    }

    #[test]
    fn ok_response_roundtrip() {
        roundtrip(&WorkerResponse::V1(WorkerResponseV1::Ok));
    }

    #[test]
    fn memory_data_response_roundtrip() {
        roundtrip(&WorkerResponse::V1(WorkerResponseV1::MemoryData {
            data: vec![0xde, 0xad, 0xbe, 0xef],
        }));
    }

    #[test]
    fn state_data_response_roundtrip() {
        roundtrip(&WorkerResponse::V1(WorkerResponseV1::StateData {
            data: vec![0u8; 1024],
        }));
    }

    #[test]
    fn current_frame_response_roundtrip() {
        roundtrip(&WorkerResponse::V1(WorkerResponseV1::CurrentFrame {
            frame_number: 12345,
        }));
    }

    #[test]
    fn screenshot_response_roundtrip() {
        roundtrip(&WorkerResponse::V1(WorkerResponseV1::Screenshot {
            png_data: vec![0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a],
        }));
    }

    #[test]
    fn write_memory_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::WriteMemory {
            address: 0x0200_0000,
            data: vec![0xaa, 0xbb, 0xcc, 0xdd],
        }));
    }

    #[test]
    fn save_state_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::SaveState { slot: 3 }));
    }

    #[test]
    fn load_state_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::LoadState { slot: 1 }));
    }

    #[test]
    fn reset_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::Reset));
    }

    #[test]
    fn shutdown_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::Shutdown));
    }

    #[test]
    fn load_rom_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::LoadRom {
            path: "/roms/test.gb".to_string(),
        }));
    }

    #[test]
    fn set_input_state_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::SetInputState {
            buttons: 0b0000_0001,
        }));
    }

    #[test]
    fn get_current_frame_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::GetCurrentFrame));
    }

    #[test]
    fn button_hold_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::ButtonHold {
            button: "Start".to_string(),
            duration_frames: 30,
        }));
    }

    #[test]
    fn take_screenshot_command_roundtrip() {
        roundtrip(&WorkerCommand::V1(WorkerCommandV1::TakeScreenshot));
    }

    #[test]
    fn pixel_format_xrgb_roundtrip() {
        roundtrip(&PixelFormat::XRGB8888);
    }

    #[test]
    fn pixel_format_rgb565_roundtrip() {
        roundtrip(&PixelFormat::RGB565);
    }
}
