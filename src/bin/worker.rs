use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use grokemon_ipc::{
    PixelFormat, WorkerCommand, WorkerCommandV1, WorkerResponse, WorkerResponseV1,
    transport::{FrameSocketServer, RawFramePacket},
};
use grokemon_libretro::{LibretroCore, RetroGameInfo};

#[path = "worker/core.rs"]
mod worker_core;

const SYSTEM_DIRECTORY: &[u8] = b".\0";
const SAVE_DIRECTORY: &[u8] = b".\0";
const EMULATOR_FRAME_DURATION: Duration = Duration::from_millis(16);
const FRAME_SOCKET_POLL_INTERVAL: Duration = Duration::from_millis(7);

struct WorkerArgs {
    socket_path: String,
    frame_socket_path: String,
    core_path: String,
    rom_path: Option<String>,
}

fn parse_args() -> WorkerArgs {
    let args: Vec<String> = std::env::args().collect();
    let mut socket_path = None;
    let mut frame_socket_path = None;
    let mut core_path = None;
    let mut rom_path = None;
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--socket" => {
                i += 1;
                socket_path = args.get(i).cloned();
            }
            "--frame-socket" => {
                i += 1;
                frame_socket_path = args.get(i).cloned();
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
        frame_socket_path: frame_socket_path.expect("--frame-socket required"),
        core_path: core_path.expect("--core required"),
        rom_path,
    }
}

struct WorkerState {
    input_state: Arc<Mutex<u32>>,
    latest_frame: Arc<Mutex<Option<RawFrame>>>,
    frame_counter: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    core_requests: mpsc::Sender<worker_core::CoreRequest>,
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
    static CURRENT_PIXEL_FORMAT: Cell<VideoPixelFormat> = const { Cell::new(VideoPixelFormat::Xrgb1555) };
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum VideoPixelFormat {
    Xrgb1555,
    Xrgb8888,
    Rgb565,
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

    let format = CURRENT_PIXEL_FORMAT.with(Cell::get);
    let Some(byte_count) = height_byte_count(height, pitch) else {
        return;
    };
    // SAFETY: libretro calls this with `data` pointing to at least `height * pitch`
    // bytes of pixel data that remain valid for the duration of the callback.
    let pixels = unsafe { std::slice::from_raw_parts(data as *const u8, byte_count) };
    let Some(data) = convert_video_frame_to_xrgb8888(pixels, width, height, pitch, format) else {
        return;
    };
    let frame = RawFrame {
        width,
        height,
        pitch: width as usize * 4,
        data,
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
    // SAFETY: libretro invokes this callback with the pointer shape defined by each
    // environment command. `handle_environment_callback` checks null pointers before
    // every command-specific dereference.
    unsafe { handle_environment_callback(cmd, data) }
}

unsafe fn handle_environment_callback(cmd: u32, data: *mut libc::c_void) -> bool {
    match cmd {
        grokemon_libretro::RETRO_ENVIRONMENT_SET_PIXEL_FORMAT => {
            if data.is_null() {
                return false;
            }
            // SAFETY: Category 8 - FFI boundary. SET_PIXEL_FORMAT passes a valid pointer
            // to a u32-compatible retro_pixel_format value for this callback call.
            let format = unsafe { *(data as *const u32) };
            match pixel_format_from_u32(format) {
                Some(format) => {
                    CURRENT_PIXEL_FORMAT.with(|current| current.set(format));
                    true
                }
                None => false,
            }
        }
        grokemon_libretro::RETRO_ENVIRONMENT_SET_MEMORY_MAPS => !data.is_null(),
        grokemon_libretro::RETRO_ENVIRONMENT_GET_SYSTEM_DIRECTORY => {
            write_c_string_pointer(data, SYSTEM_DIRECTORY.as_ptr().cast())
        }
        grokemon_libretro::RETRO_ENVIRONMENT_GET_SAVE_DIRECTORY => {
            write_c_string_pointer(data, SAVE_DIRECTORY.as_ptr().cast())
        }
        grokemon_libretro::RETRO_ENVIRONMENT_GET_LOG_INTERFACE => false,
        _ => false,
    }
}

fn pixel_format_from_u32(format: u32) -> Option<VideoPixelFormat> {
    match format {
        value if value == grokemon_libretro::RetroPixelFormat::XRGB1555 as u32 => {
            Some(VideoPixelFormat::Xrgb1555)
        }
        value if value == grokemon_libretro::RetroPixelFormat::XRGB8888 as u32 => {
            Some(VideoPixelFormat::Xrgb8888)
        }
        value if value == grokemon_libretro::RetroPixelFormat::RGB565 as u32 => {
            Some(VideoPixelFormat::Rgb565)
        }
        _ => None,
    }
}

fn height_byte_count(height: u32, pitch: usize) -> Option<usize> {
    (height as usize).checked_mul(pitch)
}

fn convert_video_frame_to_xrgb8888(
    pixels: &[u8],
    width: u32,
    height: u32,
    pitch: usize,
    format: VideoPixelFormat,
) -> Option<Vec<u8>> {
    let width = width as usize;
    let height = height as usize;
    let bytes_per_pixel = match format {
        VideoPixelFormat::Xrgb8888 => 4,
        VideoPixelFormat::Xrgb1555 | VideoPixelFormat::Rgb565 => 2,
    };
    let row_bytes = width.checked_mul(bytes_per_pixel)?;
    if pitch < row_bytes || pixels.len() < height.checked_mul(pitch)? {
        return None;
    }

    let output_len = width.checked_mul(height)?.checked_mul(4)?;
    let mut out = Vec::with_capacity(output_len);
    for row in 0..height {
        let row_start = row.checked_mul(pitch)?;
        let row_end = row_start.checked_add(row_bytes)?;
        let row = pixels.get(row_start..row_end)?;
        match format {
            VideoPixelFormat::Xrgb8888 => out.extend_from_slice(row),
            VideoPixelFormat::Xrgb1555 => convert_xrgb1555_row(row, &mut out),
            VideoPixelFormat::Rgb565 => convert_rgb565_row(row, &mut out),
        }
    }
    Some(out)
}

fn convert_xrgb1555_row(row: &[u8], out: &mut Vec<u8>) {
    for px in row.chunks_exact(2) {
        let value = u16::from_ne_bytes([px[0], px[1]]);
        let r = expand_5bit_to_8bit((value >> 10) & 0x1f);
        let g = expand_5bit_to_8bit((value >> 5) & 0x1f);
        let b = expand_5bit_to_8bit(value & 0x1f);
        out.extend_from_slice(&[b, g, r, 0xff]);
    }
}

fn convert_rgb565_row(row: &[u8], out: &mut Vec<u8>) {
    for px in row.chunks_exact(2) {
        let value = u16::from_ne_bytes([px[0], px[1]]);
        let r = expand_5bit_to_8bit((value >> 11) & 0x1f);
        let g = expand_6bit_to_8bit((value >> 5) & 0x3f);
        let b = expand_5bit_to_8bit(value & 0x1f);
        out.extend_from_slice(&[b, g, r, 0xff]);
    }
}

fn expand_5bit_to_8bit(value: u16) -> u8 {
    ((value << 3) | (value >> 2)) as u8
}

fn expand_6bit_to_8bit(value: u16) -> u8 {
    ((value << 2) | (value >> 4)) as u8
}

fn schedule_next_frame_sleep(
    next_frame_at: &mut Instant,
    now: Instant,
    frame_duration: Duration,
) -> Option<Duration> {
    *next_frame_at += frame_duration;
    if now < *next_frame_at {
        return Some(*next_frame_at - now);
    }
    if now.duration_since(*next_frame_at) > frame_duration {
        *next_frame_at = now;
    }
    None
}

fn write_c_string_pointer(data: *mut libc::c_void, value: *const libc::c_char) -> bool {
    if data.is_null() {
        return false;
    }
    // SAFETY: Category 8 - FFI boundary. Directory environment commands pass `data` as
    // `const char **`; the static NUL-terminated byte strings outlive the core.
    unsafe {
        *(data as *mut *const libc::c_char) = value;
    }
    true
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
    core_requests: mpsc::Receiver<worker_core::CoreRequest>,
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
    let frame_duration = EMULATOR_FRAME_DURATION;
    let mut next_frame_at = Instant::now();
    let mut save_states = std::collections::HashMap::new();

    loop {
        if state.shutdown.load(Ordering::Relaxed) {
            break;
        }

        worker_core::handle_pending_core_requests(&core, &mut save_states, &core_requests);

        // SAFETY: callbacks are registered, the core is initialized, and a game was loaded
        // when a ROM path was provided; each call advances emulation by one frame.
        unsafe { (core.retro_run)() };
        state.frame_counter.fetch_add(1, Ordering::Relaxed);

        if let Some(sleep_for) =
            schedule_next_frame_sleep(&mut next_frame_at, Instant::now(), frame_duration)
        {
            std::thread::sleep(sleep_for);
        }
    }

    // SAFETY: a game may have been loaded successfully above; unloading before Drop's
    // retro_deinit follows the libretro lifecycle.
    unsafe { (core.retro_unload_game)() };
    drop(core);
}

async fn send_core_command(
    state: &WorkerState,
    command: worker_core::CoreCommand,
) -> WorkerResponseV1 {
    let (response_tx, response_rx) = mpsc::channel();
    if state.core_requests.send((command, response_tx)).is_err() {
        return WorkerResponseV1::Error {
            message: "emulator thread is unavailable".to_string(),
        };
    }

    match tokio::task::spawn_blocking(move || response_rx.recv_timeout(Duration::from_secs(5)))
        .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => WorkerResponseV1::Error {
            message: format!("emulator command timed out: {error}"),
        },
        Err(error) => WorkerResponseV1::Error {
            message: format!("emulator command wait failed: {error}"),
        },
    }
}

async fn handle_commands(
    socket_path: String,
    state: Arc<WorkerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
                WorkerCommand::V1(WorkerCommandV1::Reset) => WorkerResponse::V1(
                    send_core_command(&state, worker_core::CoreCommand::Reset).await,
                ),
                WorkerCommand::V1(WorkerCommandV1::Shutdown) => {
                    state.shutdown.store(true, Ordering::Relaxed);
                    let _ = conn
                        .send_response(&WorkerResponse::V1(WorkerResponseV1::Ok))
                        .await;
                    return Ok(());
                }
                WorkerCommand::V1(WorkerCommandV1::TakeScreenshot) => {
                    match state
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
                    }
                }
                WorkerCommand::V1(WorkerCommandV1::LoadRom { path }) => WorkerResponse::V1(
                    send_core_command(&state, worker_core::CoreCommand::LoadRom { path }).await,
                ),
                WorkerCommand::V1(WorkerCommandV1::ReadMemory { address, size }) => {
                    WorkerResponse::V1(
                        send_core_command(
                            &state,
                            worker_core::CoreCommand::ReadMemory { address, size },
                        )
                        .await,
                    )
                }
                WorkerCommand::V1(WorkerCommandV1::WriteMemory { address, data }) => {
                    WorkerResponse::V1(
                        send_core_command(
                            &state,
                            worker_core::CoreCommand::WriteMemory { address, data },
                        )
                        .await,
                    )
                }
                WorkerCommand::V1(WorkerCommandV1::SaveState { slot }) => WorkerResponse::V1(
                    send_core_command(&state, worker_core::CoreCommand::SaveState { slot }).await,
                ),
                WorkerCommand::V1(WorkerCommandV1::LoadState { slot }) => WorkerResponse::V1(
                    send_core_command(&state, worker_core::CoreCommand::LoadState { slot }).await,
                ),
            };

            if conn.send_response(&response).await.is_err() {
                break;
            }
        }
    }
}

async fn handle_frame_socket(
    frame_socket_path: String,
    state: Arc<WorkerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = FrameSocketServer::bind(&frame_socket_path)?;

    loop {
        let mut connection = server.accept().await?;
        loop {
            if state.shutdown.load(Ordering::Relaxed) {
                return Ok(());
            }

            let frame = state
                .latest_frame
                .lock()
                .expect("frame mutex poisoned")
                .clone();
            if let Some(frame) = frame {
                let Ok(pitch) = u32::try_from(frame.pitch) else {
                    tokio::time::sleep(FRAME_SOCKET_POLL_INTERVAL).await;
                    continue;
                };
                let packet = RawFramePacket {
                    width: frame.width,
                    height: frame.height,
                    pitch,
                    pixel_format: 0,
                    data: frame.data,
                };
                if connection.send_frame(&packet).await.is_err() {
                    break;
                }
            }

            tokio::time::sleep(FRAME_SOCKET_POLL_INTERVAL).await;
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
    let socket_path = args.socket_path.clone();
    let frame_socket_path = args.frame_socket_path.clone();
    let (core_requests_tx, core_requests_rx) = mpsc::channel();
    let state = Arc::new(WorkerState {
        input_state: Arc::new(Mutex::new(0)),
        latest_frame: Arc::new(Mutex::new(None)),
        frame_counter: Arc::new(AtomicU64::new(0)),
        shutdown: Arc::new(AtomicBool::new(false)),
        core_requests: core_requests_tx,
    });

    let (startup_tx, startup_rx) = std::sync::mpsc::channel();
    let state_clone = state.clone();
    let emulator_thread = std::thread::spawn(move || {
        run_emulator_thread(
            args.core_path,
            args.rom_path,
            state_clone,
            core_requests_rx,
            startup_tx,
        );
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
        tokio::select! {
            result = handle_commands(socket_path, state.clone()) => {
                if let Err(e) = result {
                    eprintln!("IPC error: {e}");
                }
            }
            result = handle_frame_socket(frame_socket_path, state.clone()) => {
                if let Err(e) = result {
                    eprintln!("frame socket error: {e}");
                }
            }
        }
        state.shutdown.store(true, Ordering::Relaxed);
    });

    state.shutdown.store(true, Ordering::Relaxed);
    let _ = emulator_thread.join();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn environment_callback_accepts_xrgb8888_pixel_format() {
        let mut format = grokemon_libretro::RetroPixelFormat::XRGB8888 as u32;

        // SAFETY: The test passes a valid pointer to a u32 pixel format value for the
        // duration of the callback.
        let accepted = unsafe {
            handle_environment_callback(
                grokemon_libretro::RETRO_ENVIRONMENT_SET_PIXEL_FORMAT,
                std::ptr::from_mut(&mut format).cast(),
            )
        };

        assert!(accepted);
    }

    #[test]
    fn environment_callback_accepts_rgb565_pixel_format() {
        let mut format = grokemon_libretro::RetroPixelFormat::RGB565 as u32;

        // SAFETY: The test passes a valid pointer to a u32 pixel format value for the
        // duration of the callback.
        let accepted = unsafe {
            handle_environment_callback(
                grokemon_libretro::RETRO_ENVIRONMENT_SET_PIXEL_FORMAT,
                std::ptr::from_mut(&mut format).cast(),
            )
        };

        assert!(accepted);
    }

    #[test]
    fn rgb565_video_frame_converts_to_tight_xrgb8888_buffer() {
        let red = 0xf800_u16.to_ne_bytes();
        let green = 0x07e0_u16.to_ne_bytes();
        let blue = 0x001f_u16.to_ne_bytes();
        let white = 0xffff_u16.to_ne_bytes();
        let pixels = [
            red[0], red[1], green[0], green[1], 0xaa, 0xbb, blue[0], blue[1], white[0], white[1],
            0xcc, 0xdd,
        ];

        let converted = convert_video_frame_to_xrgb8888(&pixels, 2, 2, 6, VideoPixelFormat::Rgb565)
            .expect("RGB565 frame should convert");

        assert_eq!(
            converted,
            vec![
                0x00, 0x00, 0xff, 0xff, 0x00, 0xff, 0x00, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff,
                0xff, 0xff,
            ]
        );
    }

    #[test]
    fn xrgb1555_video_frame_converts_to_tight_xrgb8888_buffer() {
        let red = 0x7c00_u16.to_ne_bytes();
        let green = 0x03e0_u16.to_ne_bytes();
        let pixels = [red[0], red[1], green[0], green[1]];

        let converted =
            convert_video_frame_to_xrgb8888(&pixels, 2, 1, 4, VideoPixelFormat::Xrgb1555)
                .expect("XRGB1555 frame should convert");

        assert_eq!(
            converted,
            vec![0x00, 0x00, 0xff, 0xff, 0x00, 0xff, 0x00, 0xff]
        );
    }

    #[test]
    fn video_frame_conversion_rejects_short_buffer() {
        let converted =
            convert_video_frame_to_xrgb8888(&[0u8; 3], 2, 1, 4, VideoPixelFormat::Xrgb8888);

        assert!(converted.is_none());
    }

    #[test]
    fn frame_socket_poll_interval_is_shorter_than_frame_duration() {
        assert!(FRAME_SOCKET_POLL_INTERVAL <= Duration::from_millis(8));
    }

    #[test]
    fn emulator_frame_duration_targets_at_least_sixty_fps() {
        assert!(EMULATOR_FRAME_DURATION <= Duration::from_micros(16_666));
    }

    #[test]
    fn frame_scheduler_sleeps_until_next_deadline_when_on_time() {
        let frame_duration = Duration::from_millis(16);
        let start = Instant::now();
        let mut next_frame_at = start;

        let sleep_for = schedule_next_frame_sleep(
            &mut next_frame_at,
            start + Duration::from_millis(1),
            frame_duration,
        );

        assert_eq!(sleep_for, Some(Duration::from_millis(15)));
    }

    #[test]
    fn frame_scheduler_skips_sleep_to_catch_up_when_late() {
        let frame_duration = Duration::from_millis(16);
        let start = Instant::now();
        let mut next_frame_at = start;

        let sleep_for = schedule_next_frame_sleep(
            &mut next_frame_at,
            start + Duration::from_millis(20),
            frame_duration,
        );

        assert_eq!(sleep_for, None);
        assert_eq!(next_frame_at, start + frame_duration);
    }

    #[test]
    fn environment_callback_rejects_null_pixel_format() {
        // SAFETY: A null data pointer is a valid negative test input for the callback.
        let accepted = unsafe {
            handle_environment_callback(
                grokemon_libretro::RETRO_ENVIRONMENT_SET_PIXEL_FORMAT,
                std::ptr::null_mut(),
            )
        };

        assert!(!accepted);
    }

    #[test]
    fn environment_callback_provides_system_and_save_directories() {
        let mut system_dir: *const libc::c_char = std::ptr::null();
        let mut save_dir: *const libc::c_char = std::ptr::null();

        // SAFETY: Both calls pass valid pointers to `const char *` storage that the
        // callback writes with static NUL-terminated directory strings.
        unsafe {
            assert!(handle_environment_callback(
                grokemon_libretro::RETRO_ENVIRONMENT_GET_SYSTEM_DIRECTORY,
                std::ptr::from_mut(&mut system_dir).cast(),
            ));
            assert!(handle_environment_callback(
                grokemon_libretro::RETRO_ENVIRONMENT_GET_SAVE_DIRECTORY,
                std::ptr::from_mut(&mut save_dir).cast(),
            ));
        }

        assert_eq!(
            // SAFETY: The callback wrote a non-null pointer to a static NUL-terminated string.
            unsafe { CStr::from_ptr(system_dir) }.to_bytes(),
            b"."
        );
        assert_eq!(
            // SAFETY: The callback wrote a non-null pointer to a static NUL-terminated string.
            unsafe { CStr::from_ptr(save_dir) }.to_bytes(),
            b"."
        );
    }

    #[test]
    fn environment_callback_accepts_non_null_memory_map_notification() {
        let mut marker = 0u8;

        // SAFETY: SET_MEMORY_MAPS only checks that the frontend received a non-null
        // notification pointer; the worker does not dereference the memory map.
        let accepted = unsafe {
            handle_environment_callback(
                grokemon_libretro::RETRO_ENVIRONMENT_SET_MEMORY_MAPS,
                std::ptr::from_mut(&mut marker).cast(),
            )
        };

        assert!(accepted);
    }
}
