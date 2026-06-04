pub const TERMINATION_MARKER: &str = "<|END|>";
pub const SUCCESS_MARKER: &str = "<|SUCCESS|>";
pub const ERROR_MARKER: &str = "<|ERROR|>";
pub const ACK_MESSAGE: &str = "<|ACK|>";

pub fn format_message(message_type: &str, args: &[impl AsRef<str>]) -> String {
    if args.is_empty() {
        format!("{message_type}{TERMINATION_MARKER}")
    } else {
        let mut body = String::from(message_type);
        for arg in args {
            body.push(',');
            body.push_str(arg.as_ref());
        }
        format!("{body}{TERMINATION_MARKER}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_message_without_args() {
        let no_args: &[&str] = &[];
        assert_eq!(
            format_message("core.currentFrame", no_args),
            "core.currentFrame<|END|>"
        );
    }

    #[test]
    fn format_message_with_one_arg() {
        assert_eq!(
            format_message("mgba-http.button.tap", &["A"]),
            "mgba-http.button.tap,A<|END|>"
        );
    }

    #[test]
    fn format_message_with_multiple_args() {
        assert_eq!(
            format_message("core.readRange", &["0xD35E", "16"]),
            "core.readRange,0xD35E,16<|END|>"
        );
    }

    #[test]
    fn format_message_string_args() {
        let args: Vec<String> = vec!["System Bus".to_string(), "49152".to_string()];
        assert_eq!(
            format_message("memoryDomain.read8", &args),
            "memoryDomain.read8,System Bus,49152<|END|>"
        );
    }

    #[test]
    fn termination_marker_constant() {
        assert_eq!(TERMINATION_MARKER, "<|END|>");
    }

    #[test]
    fn success_marker_constant() {
        assert_eq!(SUCCESS_MARKER, "<|SUCCESS|>");
    }

    #[test]
    fn error_marker_constant() {
        assert_eq!(ERROR_MARKER, "<|ERROR|>");
    }

    #[test]
    fn ack_message_constant() {
        assert_eq!(ACK_MESSAGE, "<|ACK|>");
    }
}
