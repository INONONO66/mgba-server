use std::cell::RefCell;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use grokemon_ipc::{PixelFormat, WorkerCommand, WorkerCommandV1, WorkerResponse, WorkerResponseV1};
use grokemon_libretro::{LibretroCore, RetroGameInfo};

struct WorkerArgs {
    socket_path: String,
    core_path: String,
    rom_path: Option<String>,
}

fn parse_args() -> WorkerArgs {
    let args: Vec<String> = std::env::args().collect();
    let mut socket_path = None;
    let mut core_path = None;
    let mut rom_path = None;
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--socket" => {
                i += 1;
                socket_path = args.get(i).cloned();
            }
            "--core" => {
                i += 1;
                core_path = args.get(i).cloned();
            }
            "--rom" => {
                i += 1;
                rom_path = args.get(i).cloned();
            }
            _ => {}
        }
        i += 1;
    }

    WorkerArgs {
        socket_path: socket_path.expect("--socket required"),
        core_path: core_path.expect("--core required"),
        rom_path,
    }
}

struct WorkerState {
    input_state: Arc<Mutex<u32>>,
    latest_frame: Arc<Mutex<Option<RawFrame>>>,
    frame_counter: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Clone)]
struct RawFrame {
    width: u32,
    height: u32,
    pitch: usize,
    data: Vec<u8>,
}

thread_local! {
    static FRAME_CALLBACK_STATE: RefCell<Option<Arc<Mutex<Option<RawFrame>>>>> = const { RefCell::new(None) };
    static INPUT_CALLBACK_STATE: RefCell<Option<Arc<Mutex<u32>>>> = const { RefCell::new(None) };
}

unsafe extern "C" fn video_refresh_callback(
    data: *const libc::c_void,
    width: u32,
    height: u32,
    pitch: usize,
) {
    if data.is_null() {
        return;
    }

    let byte_count = height as usize * pitch;
    // SAFETY: libretro calls this with `data` pointing to at least `height * pitch`
    // bytes of pixel data that remain valid for the duration of the callback.
    let pixels = unsafe { std::slice::from_raw_parts(data as *const u8, byte_count) };
    let frame = RawFrame {
        width,
        height,
        pitch,
        data: pixels.to_vec(),
    };

    FRAME_CALLBACK_STATE.with(|state| {
        if let Some(ref latest) = *state.borrow() {
            *latest.lock().expect("latest frame mutex poisoned") = Some(frame);
        }
    });
}

unsafe extern "C" fn input_state_callback(_port: u32, _device: u32, _index: u32, id: u32) -> i16 {
    INPUT_CALLBACK_STATE.with(|state| {
        if let Some(ref input) = *state.borrow() {
            let buttons = *input.lock().expect("input mutex poisoned");
            if (buttons >> id) & 1 == 1 { 1 } else { 0 }
        } else {
            0
        }
    })
}

unsafe extern "C" fn environment_callback(cmd: u32, data: *mut libc::c_void) -> bool {
    match cmd {
        grokemon_libretro::RETRO_ENVIRONMENT_SET_PIXEL_FORMAT => {
            if data.is_null() {
                return false;
            }
            // SAFETY: the SET_PIXEL_FORMAT environment command passes a valid pointer to
            // a u32-compatible retro_pixel_format value for the duration of this call.
            let format = unsafe { *(data as *const u32) };
            format == grokemon_libretro::RetroPixelFormat::XRGB8888 as u32
        }
        grokemon_libretro::RETRO_ENVIRONMENT_GET_LOG_INTERFACE => false,
        _ => false,
    }
}

unsafe extern "C" fn audio_sample_callback(_left: i16, _right: i16) {}

unsafe extern "C" fn audio_sample_batch_callback(_data: *const i16, frames: usize) -> usize {
    frames
}

unsafe extern "C" fn input_poll_callback() {}

fn initialize_core(core: &LibretroCore, rom_path: Option<&str>) -> Result<(), String> {
    // SAFETY: all registered callbacks have the exact C ABI signatures expected by libretro
    // and are static function items that outlive the loaded core.
    unsafe {
        (core.retro_set_environment)(environment_callback);
        (core.retro_set_video_refresh)(video_refresh_callback);
        (core.retro_set_audio_sample)(audio_sample_callback);
        (core.retro_set_audio_sample_batch)(audio_sample_batch_callback);
        (core.retro_set_input_poll)(input_poll_callback);
        (core.retro_set_input_state)(input_state_callback);
        (core.retro_init)();
    }

    if let Some(rom_path) = rom_path {
        let rom_cstr = CString::new(rom_path).map_err(|e| format!("invalid ROM path: {e}"))?;
        let game_info = RetroGameInfo {
            path: rom_cstr.as_ptr(),
            data: std::ptr::null(),
            size: 0,
            meta: std::ptr::null(),
        };
        // SAFETY: `game_info` and its CString-backed path remain valid for the whole call.
        let loaded = unsafe { (core.retro_load_game)(&game_info) };
        if !loaded {
            return Err(format!("Failed to load ROM: {rom_path}"));
        }
    }

    Ok(())
}

fn run_emulator_thread(
    core_path: String,
    rom_path: Option<String>,
    state: Arc<WorkerState>,
    startup_tx: std::sync::mpsc::Sender<Result<(), String>>,
) {
    FRAME_CALLBACK_STATE.with(|s| {
        *s.borrow_mut() = Some(state.latest_frame.clone());
    });
    INPUT_CALLBACK_STATE.with(|s| {
        *s.borrow_mut() = Some(state.input_state.clone());
    });

    let core = match LibretroCore::load(&core_path) {
        Ok(core) => core,
        Err(e) => {
            let _ = startup_tx.send(Err(format!("Failed to load core: {e}")));
            return;
        }
    };

    if let Err(e) = initialize_core(&core, rom_path.as_deref()) {
        let _ = startup_tx.send(Err(e));
        return;
    }

    let _ = startup_tx.send(Ok(()));
    let frame_duration = Duration::from_nanos(16_750_000);

    loop {
        if state.shutdown.load(Ordering::Relaxed) {
            break;
        }

        let start = Instant::now();
        // SAFETY: callbacks are registered, the core is initialized, and a game was loaded
        // when a ROM path was provided; each call advances emulation by one frame.
        unsafe { (core.retro_run)() };
        state.frame_counter.fetch_add(1, Ordering::Relaxed);

        let elapsed = start.elapsed();
        if elapsed < frame_duration {
            std::thread::sleep(frame_duration - elapsed);
        }
    }

    // SAFETY: a game may have been loaded successfully above; unloading before Drop's
    // retro_deinit follows the libretro lifecycle.
    unsafe { (core.retro_unload_game)() };
    drop(core);
}

async fn handle_commands(
    socket_path: String,
    state: Arc<WorkerState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let server = grokemon_ipc::transport::IpcServer::bind(&socket_path)?;

    loop {
        let mut conn = server.accept().await?;

        loop {
            let cmd = match conn.recv_command().await {
                Ok(cmd) => cmd,
                Err(_) => break,
            };

            let response = match cmd {
                WorkerCommand::V1(WorkerCommandV1::Ping) => {
                    WorkerResponse::V1(WorkerResponseV1::Pong)
                }
                WorkerCommand::V1(WorkerCommandV1::GetCurrentFrame) => {
                    let frame_number = state.frame_counter.load(Ordering::Relaxed);
                    WorkerResponse::V1(WorkerResponseV1::CurrentFrame { frame_number })
                }
                WorkerCommand::V1(WorkerCommandV1::SetInputState { buttons }) => {
                    *state.input_state.lock().expect("input mutex poisoned") = buttons;
                    WorkerResponse::V1(WorkerResponseV1::Ok)
                }
                WorkerCommand::V1(WorkerCommandV1::ButtonTap { button }) => {
                    if let Some(bit) = button_to_bit(&button) {
                        *state.input_state.lock().expect("input mutex poisoned") |= 1 << bit;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        *state.input_state.lock().expect("input mutex poisoned") &= !(1 << bit);
                    }
                    WorkerResponse::V1(WorkerResponseV1::Ok)
                }
                WorkerCommand::V1(WorkerCommandV1::ButtonHold {
                    button,
                    duration_frames,
                }) => {
                    if let Some(bit) = button_to_bit(&button) {
                        *state.input_state.lock().expect("input mutex poisoned") |= 1 << bit;
                        let ms = (duration_frames as u64 * 1000) / 60;
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        *state.input_state.lock().expect("input mutex poisoned") &= !(1 << bit);
                    }
                    WorkerResponse::V1(WorkerResponseV1::Ok)
                }
                WorkerCommand::V1(WorkerCommandV1::Reset) => {
                    WorkerResponse::V1(WorkerResponseV1::Ok)
                }
                WorkerCommand::V1(WorkerCommandV1::Shutdown) => {
                    state.shutdown.store(true, Ordering::Relaxed);
                    let _ = conn
                        .send_response(&WorkerResponse::V1(WorkerResponseV1::Ok))
                        .await;
                    return Ok(());
                }
                WorkerCommand::V1(WorkerCommandV1::TakeScreenshot) => {
                    let response = match state
                        .latest_frame
                        .lock()
                        .expect("frame mutex poisoned")
                        .clone()
                    {
                        Some(frame) => WorkerResponse::V1(WorkerResponseV1::Frame {
                            width: frame.width,
                            height: frame.height,
                            pitch: frame.pitch as u32,
                            pixel_format: PixelFormat::XRGB8888,
                            data: frame.data,
                        }),
                        None => WorkerResponse::V1(WorkerResponseV1::Error {
                            message: "no frame captured yet".to_string(),
                        }),
                    };
                    response
                }
                WorkerCommand::V1(WorkerCommandV1::LoadRom { .. })
                | WorkerCommand::V1(WorkerCommandV1::ReadMemory { .. })
                | WorkerCommand::V1(WorkerCommandV1::WriteMemory { .. })
                | WorkerCommand::V1(WorkerCommandV1::SaveState { .. })
                | WorkerCommand::V1(WorkerCommandV1::LoadState { .. }) => {
                    WorkerResponse::V1(WorkerResponseV1::Error {
                        message: "command not yet implemented".to_string(),
                    })
                }
            };

            if conn.send_response(&response).await.is_err() {
                break;
            }
        }
    }
}

fn button_to_bit(button: &str) -> Option<u32> {
    match button {
        "B" => Some(0),
        "Y" => Some(1),
        "Select" => Some(2),
        "Start" => Some(3),
        "Up" => Some(4),
        "Down" => Some(5),
        "Left" => Some(6),
        "Right" => Some(7),
        "A" => Some(8),
        "X" => Some(9),
        "L" => Some(10),
        "R" => Some(11),
        _ => None,
    }
}

fn main() {
    let args = parse_args();
    let state = Arc::new(WorkerState {
        input_state: Arc::new(Mutex::new(0)),
        latest_frame: Arc::new(Mutex::new(None)),
        frame_counter: Arc::new(AtomicU64::new(0)),
        shutdown: Arc::new(AtomicBool::new(false)),
    });

    let (startup_tx, startup_rx) = std::sync::mpsc::channel();
    let state_clone = state.clone();
    let emulator_thread = std::thread::spawn(move || {
        run_emulator_thread(args.core_path, args.rom_path, state_clone, startup_tx);
    });

    match startup_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!("{e}");
            let _ = emulator_thread.join();
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Worker startup timed out: {e}");
            state.shutdown.store(true, Ordering::Relaxed);
            let _ = emulator_thread.join();
            std::process::exit(1);
        }
    }

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    rt.block_on(async {
        if let Err(e) = handle_commands(args.socket_path, state.clone()).await {
            eprintln!("IPC error: {e}");
            state.shutdown.store(true, Ordering::Relaxed);
        }
    });

    state.shutdown.store(true, Ordering::Relaxed);
    let _ = emulator_thread.join();
}
