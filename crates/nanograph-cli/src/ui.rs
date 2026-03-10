use std::io::{self, IsTerminal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusTone {
    Ok,
    Info,
    Warn,
    Error,
    Skip,
}

pub(crate) fn stdout_supports_color() -> bool {
    stream_supports_color(Stream::Stdout)
}

pub(crate) fn stderr_supports_color() -> bool {
    stream_supports_color(Stream::Stderr)
}

pub(crate) fn style_label(text: &str, enabled: bool) -> String {
    style_scalar(text, "1", enabled)
}

pub(crate) fn style_key(text: &str, enabled: bool) -> String {
    style_scalar(text, "1;36", enabled)
}

pub(crate) fn style_scalar(text: &str, code: &str, enabled: bool) -> String {
    if enabled {
        format!("\u{1b}[{code}m{text}\u{1b}[0m")
    } else {
        text.to_string()
    }
}

pub(crate) fn format_status_line(tone: StatusTone, message: &str, enabled: bool) -> String {
    format!("{} {}", style_status_tag(tone, enabled), message)
}

fn style_status_tag(tone: StatusTone, enabled: bool) -> String {
    let (text, code) = match tone {
        StatusTone::Ok => ("OK:", "1;32"),
        StatusTone::Info => ("INFO:", "1;34"),
        StatusTone::Warn => ("WARN:", "1;33"),
        StatusTone::Error => ("ERROR:", "1;31"),
        StatusTone::Skip => ("SKIP:", "1;35"),
    };
    style_scalar(text, code, enabled)
}

enum Stream {
    Stdout,
    Stderr,
}

fn stream_supports_color(stream: Stream) -> bool {
    let is_terminal = match stream {
        Stream::Stdout => io::stdout().is_terminal(),
        Stream::Stderr => io::stderr().is_terminal(),
    };
    is_terminal
        && std::env::var_os("NO_COLOR").is_none()
        && std::env::var("TERM")
            .map(|term| term != "dumb")
            .unwrap_or(true)
}
