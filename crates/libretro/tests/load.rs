use grokemon_libretro::LibretroCore;

#[test]
fn invalid_path_returns_error() {
    let result = LibretroCore::load("/nonexistent/path.so");
    assert!(result.is_err(), "expected error for nonexistent path");
}
