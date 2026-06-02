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
