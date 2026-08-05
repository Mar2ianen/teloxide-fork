use std::fmt;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TimeZoneError {
    #[error("invalid IANA time zone `{name}`: {source}")]
    Invalid {
        name: String,
        #[source]
        source: jiff::Error,
    },
}

#[derive(Debug, Error)]
pub enum TimeError {
    #[error("unknown time binding `${0}`")]
    UnknownBinding(String),
    #[error("time value cannot be converted to a Telegram instant: {0}")]
    InvalidCivil(#[source] jiff::Error),
    #[error("time arithmetic overflow: {0}")]
    Overflow(#[source] jiff::Error),
}

#[derive(Debug, Error)]
pub enum RenderError {
    #[error("invalid {dialect} time markup at {line}:{column}: {message}")]
    InvalidMarkup {
        dialect: &'static str,
        byte_offset: usize,
        line: usize,
        column: usize,
        literal: String,
        message: String,
    },
    #[error("unknown time binding `${name}` at {line}:{column}")]
    UnknownBinding { name: String, byte_offset: usize, line: usize, column: usize },
    #[error("time normalization failed at {line}:{column}: {source}")]
    Normalization {
        byte_offset: usize,
        line: usize,
        column: usize,
        #[source]
        source: TimeError,
    },
}

impl RenderError {
    #[cfg(feature = "rich-text")]
    pub(crate) fn invalid(
        dialect: &'static str,
        source: &str,
        byte_offset: usize,
        literal: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let (line, column) = line_column(source, byte_offset);
        Self::InvalidMarkup {
            dialect,
            byte_offset,
            line,
            column,
            literal: literal.into(),
            message: message.into(),
        }
    }

    #[cfg(feature = "rich-text")]
    pub(crate) fn from_time_error(source: &str, byte_offset: usize, error: TimeError) -> Self {
        let (line, column) = line_column(source, byte_offset);
        match error {
            TimeError::UnknownBinding(name) => {
                Self::UnknownBinding { name, byte_offset, line, column }
            }
            error => Self::Normalization { byte_offset, line, column, source: error },
        }
    }
}

#[cfg(feature = "rich-text")]
pub(crate) fn line_column(source: &str, byte_offset: usize) -> (usize, usize) {
    let prefix = &source[..byte_offset.min(source.len())];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix
        .rsplit_once('\n')
        .map_or(prefix.chars().count() + 1, |(_, line)| line.chars().count() + 1);
    (line, column)
}

impl fmt::Display for crate::utils::time::DateTimeFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.wire_value())
    }
}
