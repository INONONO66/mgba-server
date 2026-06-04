use std::collections::HashMap;
use std::ffi::CString;
use std::ops::Range;
use std::sync::mpsc;

use grokemon_ipc::WorkerResponseV1;
use grokemon_libretro::{LibretroCore, RetroGameInfo};

const RETRO_MEMORY_SYSTEM_RAM: u32 = 2;
const GBA_SYSTEM_RAM_BASE: u32 = 0x0200_0000;

pub type CoreRequest = (CoreCommand, mpsc::Sender<WorkerResponseV1>);

pub enum CoreCommand {
    LoadRom { path: String },
    ReadMemory { address: u32, size: u32 },
    WriteMemory { address: u32, data: Vec<u8> },
    SaveState { slot: u8 },
    LoadState { slot: u8 },
    Reset,
}

pub fn handle_core_command(
    core: &LibretroCore,
    save_states: &mut HashMap<u8, Vec<u8>>,
    command: CoreCommand,
) -> WorkerResponseV1 {
    match command {
        CoreCommand::LoadRom { path } => load_rom(core, &path),
        CoreCommand::ReadMemory { address, size } => read_memory(core, address, size),
        CoreCommand::WriteMemory { address, data } => write_memory(core, address, &data),
        CoreCommand::SaveState { slot } => save_state(core, save_states, slot),
        CoreCommand::LoadState { slot } => load_state(core, save_states, slot),
        CoreCommand::Reset => {
            // SAFETY: core commands run on the emulator OS thread after successful
            // initialization, preserving libretro thread affinity for reset.
            unsafe { (core.retro_reset)() };
            WorkerResponseV1::Ok
        }
    }
}

pub fn handle_pending_core_requests(
    core: &LibretroCore,
    save_states: &mut HashMap<u8, Vec<u8>>,
    requests: &mpsc::Receiver<CoreRequest>,
) {
    while let Ok((command, response_tx)) = requests.try_recv() {
        let response = handle_core_command(core, save_states, command);
        let _ = response_tx.send(response);
    }
}

fn load_rom(core: &LibretroCore, path: &str) -> WorkerResponseV1 {
    let rom_cstr = match CString::new(path) {
        Ok(value) => value,
        Err(error) => {
            return WorkerResponseV1::Error {
                message: format!("invalid ROM path: {error}"),
            };
        }
    };
    let game_info = RetroGameInfo {
        path: rom_cstr.as_ptr(),
        data: std::ptr::null(),
        size: 0,
        meta: std::ptr::null(),
    };

    // SAFETY: the CString-backed path and game_info remain valid for retro_load_game,
    // and both calls run on the initialized emulator thread.
    unsafe { (core.retro_unload_game)() };
    let loaded = unsafe { (core.retro_load_game)(&game_info) };
    if loaded {
        WorkerResponseV1::Ok
    } else {
        WorkerResponseV1::Error {
            message: format!("failed to load ROM: {path}"),
        }
    }
}

fn read_memory(core: &LibretroCore, address: u32, size: u32) -> WorkerResponseV1 {
    let (ptr, memory_size) = system_ram(core);
    if ptr.is_null() {
        return WorkerResponseV1::Error {
            message: "system RAM is unavailable".to_string(),
        };
    }

    let range = match system_ram_range(address, size, memory_size) {
        Ok(range) => range,
        Err(message) => return WorkerResponseV1::Error { message },
    };

    // SAFETY: system_ram_range verified this range fits inside the memory region
    // reported by the core, and the pointer was checked for null.
    let data = unsafe { std::slice::from_raw_parts(ptr.add(range.start), range.len()).to_vec() };
    WorkerResponseV1::MemoryData { data }
}

fn write_memory(core: &LibretroCore, address: u32, data: &[u8]) -> WorkerResponseV1 {
    let (ptr, memory_size) = system_ram(core);
    if ptr.is_null() {
        return WorkerResponseV1::Error {
            message: "system RAM is unavailable".to_string(),
        };
    }

    let range = match system_ram_range(address, data.len() as u32, memory_size) {
        Ok(range) => range,
        Err(message) => return WorkerResponseV1::Error { message },
    };

    // SAFETY: system_ram_range verified this range fits inside the memory region
    // reported by the core, and the pointer was checked for null.
    unsafe {
        std::slice::from_raw_parts_mut(ptr.add(range.start), range.len()).copy_from_slice(data);
    }
    WorkerResponseV1::Ok
}

fn save_state(
    core: &LibretroCore,
    save_states: &mut HashMap<u8, Vec<u8>>,
    slot: u8,
) -> WorkerResponseV1 {
    // SAFETY: save-state calls run on the initialized emulator thread.
    let size = unsafe { (core.retro_serialize_size)() };
    if size == 0 {
        return WorkerResponseV1::Error {
            message: "save states are unsupported by this core".to_string(),
        };
    }

    let mut data = vec![0u8; size];
    // SAFETY: data is a writable buffer sized from retro_serialize_size.
    let ok = unsafe { (core.retro_serialize)(data.as_mut_ptr().cast(), data.len()) };
    if !ok {
        return WorkerResponseV1::Error {
            message: format!("failed to save state slot {slot}"),
        };
    }

    save_states.insert(slot, data.clone());
    WorkerResponseV1::StateData { data }
}

fn load_state(
    core: &LibretroCore,
    save_states: &HashMap<u8, Vec<u8>>,
    slot: u8,
) -> WorkerResponseV1 {
    let Some(data) = save_states.get(&slot) else {
        return WorkerResponseV1::Error {
            message: format!("state slot {slot} is empty"),
        };
    };

    // SAFETY: saved bytes were produced by retro_serialize for this core instance.
    let ok = unsafe { (core.retro_unserialize)(data.as_ptr().cast(), data.len()) };
    if ok {
        WorkerResponseV1::Ok
    } else {
        WorkerResponseV1::Error {
            message: format!("failed to load state slot {slot}"),
        }
    }
}

fn system_ram(core: &LibretroCore) -> (*mut u8, usize) {
    // SAFETY: memory queries run on the initialized emulator thread; callers check
    // for null and bounds before dereferencing the returned pointer.
    let ptr = unsafe { (core.retro_get_memory_data)(RETRO_MEMORY_SYSTEM_RAM) }.cast::<u8>();
    // SAFETY: size query is paired with retro_get_memory_data on the same thread.
    let size = unsafe { (core.retro_get_memory_size)(RETRO_MEMORY_SYSTEM_RAM) };
    (ptr, size)
}

fn system_ram_range(address: u32, size: u32, memory_size: usize) -> Result<Range<usize>, String> {
    let start = if address >= GBA_SYSTEM_RAM_BASE {
        address - GBA_SYSTEM_RAM_BASE
    } else {
        address
    };
    let start = start as usize;
    let len = size as usize;
    let end = start
        .checked_add(len)
        .ok_or_else(|| "memory range overflow".to_string())?;

    if start > memory_size || end > memory_size {
        return Err(format!(
            "memory range 0x{address:08x}+{size} exceeds system RAM size {memory_size}"
        ));
    }

    Ok(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_ram_range_accepts_absolute_gba_address() {
        let range = system_ram_range(0x0200_0004, 4, 16).unwrap();
        assert_eq!(range, 4..8);
    }

    #[test]
    fn system_ram_range_accepts_relative_offset() {
        let range = system_ram_range(4, 4, 16).unwrap();
        assert_eq!(range, 4..8);
    }

    #[test]
    fn system_ram_range_rejects_out_of_bounds_access() {
        let error = system_ram_range(0x0200_000c, 8, 16).unwrap_err();
        assert!(error.contains("exceeds system RAM size"));
    }
}
