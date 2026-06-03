//! libretro FFI bindings and core loader.

pub const RETRO_ENVIRONMENT_SET_PIXEL_FORMAT: u32 = 10;
pub const RETRO_ENVIRONMENT_GET_LOG_INTERFACE: u32 = 27;
pub const RETRO_ENVIRONMENT_SET_MEMORY_MAPS: u32 = 36;
pub const RETRO_ENVIRONMENT_GET_SYSTEM_DIRECTORY: u32 = 9;
pub const RETRO_ENVIRONMENT_GET_SAVE_DIRECTORY: u32 = 31;

#[derive(Debug, thiserror::Error)]
pub enum LibretroError {
    #[error("failed to load library: {0}")]
    Load(#[from] libloading::Error),
    #[error("symbol not found: {name}")]
    Symbol { name: &'static str },
    #[error("retro_load_game failed")]
    LoadGameFailed,
    #[error("unsupported API version: {0}")]
    ApiVersion(u32),
}

#[repr(C)]
pub struct RetroGameInfo {
    pub path: *const libc::c_char,
    pub data: *const libc::c_void,
    pub size: libc::size_t,
    pub meta: *const libc::c_char,
}

#[repr(C)]
pub struct RetroSystemInfo {
    pub library_name: *const libc::c_char,
    pub library_version: *const libc::c_char,
    pub valid_extensions: *const libc::c_char,
    pub need_fullpath: bool,
    pub block_extract: bool,
}

#[repr(C)]
pub struct RetroAvInfo {
    pub geometry: RetroGameGeometry,
    pub timing: RetroSystemTiming,
}

#[repr(C)]
pub struct RetroGameGeometry {
    pub base_width: u32,
    pub base_height: u32,
    pub max_width: u32,
    pub max_height: u32,
    pub aspect_ratio: f32,
}

#[repr(C)]
pub struct RetroSystemTiming {
    pub fps: f64,
    pub sample_rate: f64,
}

#[repr(u32)]
pub enum RetroPixelFormat {
    XRGB1555 = 0,
    XRGB8888 = 1,
    RGB565 = 2,
}

pub type RetroEnvironmentFn = unsafe extern "C" fn(cmd: u32, data: *mut libc::c_void) -> bool;
pub type RetroVideoRefreshFn =
    unsafe extern "C" fn(data: *const libc::c_void, width: u32, height: u32, pitch: usize);
pub type RetroAudioSampleFn = unsafe extern "C" fn(left: i16, right: i16);
pub type RetroAudioSampleBatchFn = unsafe extern "C" fn(data: *const i16, frames: usize) -> usize;
pub type RetroInputPollFn = unsafe extern "C" fn();
pub type RetroInputStateFn =
    unsafe extern "C" fn(port: u32, device: u32, index: u32, id: u32) -> i16;

pub struct LibretroCore {
    _lib: libloading::Library,
    pub retro_api_version: unsafe extern "C" fn() -> u32,
    pub retro_init: unsafe extern "C" fn(),
    pub retro_deinit: unsafe extern "C" fn(),
    pub retro_set_environment: unsafe extern "C" fn(RetroEnvironmentFn),
    pub retro_set_video_refresh: unsafe extern "C" fn(RetroVideoRefreshFn),
    pub retro_set_audio_sample: unsafe extern "C" fn(RetroAudioSampleFn),
    pub retro_set_audio_sample_batch: unsafe extern "C" fn(RetroAudioSampleBatchFn),
    pub retro_set_input_poll: unsafe extern "C" fn(RetroInputPollFn),
    pub retro_set_input_state: unsafe extern "C" fn(RetroInputStateFn),
    pub retro_load_game: unsafe extern "C" fn(*const RetroGameInfo) -> bool,
    pub retro_unload_game: unsafe extern "C" fn(),
    pub retro_run: unsafe extern "C" fn(),
    pub retro_reset: unsafe extern "C" fn(),
    pub retro_serialize_size: unsafe extern "C" fn() -> usize,
    pub retro_serialize: unsafe extern "C" fn(data: *mut libc::c_void, size: usize) -> bool,
    pub retro_unserialize: unsafe extern "C" fn(data: *const libc::c_void, size: usize) -> bool,
    pub retro_get_memory_data: unsafe extern "C" fn(id: u32) -> *mut libc::c_void,
    pub retro_get_memory_size: unsafe extern "C" fn(id: u32) -> usize,
    pub retro_get_system_info: unsafe extern "C" fn(info: *mut RetroSystemInfo),
    pub retro_get_system_av_info: unsafe extern "C" fn(info: *mut RetroAvInfo),
}

impl LibretroCore {
    pub fn load(path: &str) -> Result<Self, LibretroError> {
        // SAFETY: We are loading a shared library. The library must be a valid libretro core.
        // The caller is responsible for ensuring the path points to a compatible library.
        let lib = unsafe { libloading::Library::new(path)? };

        macro_rules! sym {
            ($name:literal, $display_name:literal, $ty:ty) => {{
                // SAFETY: We are resolving a symbol from a loaded library. The symbol name
                // and type are the required signatures from the libretro API specification.
                let symbol: libloading::Symbol<'_, $ty> = unsafe {
                    lib.get($name)
                        .map_err(|_| LibretroError::Symbol { name: $display_name })?
                };
                *symbol
            }};
        }

        let core = Self {
            _lib: lib,
            retro_api_version: sym!(
                b"retro_api_version\0",
                "retro_api_version",
                unsafe extern "C" fn() -> u32
            ),
            retro_init: sym!(b"retro_init\0", "retro_init", unsafe extern "C" fn()),
            retro_deinit: sym!(b"retro_deinit\0", "retro_deinit", unsafe extern "C" fn()),
            retro_set_environment: sym!(
                b"retro_set_environment\0",
                "retro_set_environment",
                unsafe extern "C" fn(RetroEnvironmentFn)
            ),
            retro_set_video_refresh: sym!(
                b"retro_set_video_refresh\0",
                "retro_set_video_refresh",
                unsafe extern "C" fn(RetroVideoRefreshFn)
            ),
            retro_set_audio_sample: sym!(
                b"retro_set_audio_sample\0",
                "retro_set_audio_sample",
                unsafe extern "C" fn(RetroAudioSampleFn)
            ),
            retro_set_audio_sample_batch: sym!(
                b"retro_set_audio_sample_batch\0",
                "retro_set_audio_sample_batch",
                unsafe extern "C" fn(RetroAudioSampleBatchFn)
            ),
            retro_set_input_poll: sym!(
                b"retro_set_input_poll\0",
                "retro_set_input_poll",
                unsafe extern "C" fn(RetroInputPollFn)
            ),
            retro_set_input_state: sym!(
                b"retro_set_input_state\0",
                "retro_set_input_state",
                unsafe extern "C" fn(RetroInputStateFn)
            ),
            retro_load_game: sym!(
                b"retro_load_game\0",
                "retro_load_game",
                unsafe extern "C" fn(*const RetroGameInfo) -> bool
            ),
            retro_unload_game: sym!(
                b"retro_unload_game\0",
                "retro_unload_game",
                unsafe extern "C" fn()
            ),
            retro_run: sym!(b"retro_run\0", "retro_run", unsafe extern "C" fn()),
            retro_reset: sym!(b"retro_reset\0", "retro_reset", unsafe extern "C" fn()),
            retro_serialize_size: sym!(
                b"retro_serialize_size\0",
                "retro_serialize_size",
                unsafe extern "C" fn() -> usize
            ),
            retro_serialize: sym!(
                b"retro_serialize\0",
                "retro_serialize",
                unsafe extern "C" fn(*mut libc::c_void, usize) -> bool
            ),
            retro_unserialize: sym!(
                b"retro_unserialize\0",
                "retro_unserialize",
                unsafe extern "C" fn(*const libc::c_void, usize) -> bool
            ),
            retro_get_memory_data: sym!(
                b"retro_get_memory_data\0",
                "retro_get_memory_data",
                unsafe extern "C" fn(u32) -> *mut libc::c_void
            ),
            retro_get_memory_size: sym!(
                b"retro_get_memory_size\0",
                "retro_get_memory_size",
                unsafe extern "C" fn(u32) -> usize
            ),
            retro_get_system_info: sym!(
                b"retro_get_system_info\0",
                "retro_get_system_info",
                unsafe extern "C" fn(*mut RetroSystemInfo)
            ),
            retro_get_system_av_info: sym!(
                b"retro_get_system_av_info\0",
                "retro_get_system_av_info",
                unsafe extern "C" fn(*mut RetroAvInfo)
            ),
        };

        // SAFETY: `retro_api_version` is a libretro function with no arguments that reports
        // the core's supported ABI version. The function pointer was resolved above.
        let version = unsafe { (core.retro_api_version)() };
        if version != 1 {
            return Err(LibretroError::ApiVersion(version));
        }

        Ok(core)
    }
}

impl Drop for LibretroCore {
    fn drop(&mut self) {
        // SAFETY: `retro_deinit` must be called before unloading the library. Fields are
        // dropped after this method returns, so `_lib` is still loaded during this call.
        unsafe { (self.retro_deinit)() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_path_returns_error() {
        let result = LibretroCore::load("/nonexistent/path.so");
        assert!(result.is_err(), "expected error for nonexistent path");
    }

    #[test]
    #[ignore = "requires mgba_libretro.so at LIBRETRO_CORE_PATH"]
    fn loads_real_core() {
        let path = std::env::var("LIBRETRO_CORE_PATH").expect("LIBRETRO_CORE_PATH must be set");
        let core = LibretroCore::load(&path).expect("should load core");
        // SAFETY: `retro_api_version` is safe to call after a successful core load.
        let version = unsafe { (core.retro_api_version)() };
        assert_eq!(version, 1, "libretro API version must be 1");
    }
}
