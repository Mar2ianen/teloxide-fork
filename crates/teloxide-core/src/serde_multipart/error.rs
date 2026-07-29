use std::{fmt, sync::Arc};

use serde::ser;

use crate::RequestError;

#[derive(Debug, derive_more::From)]
pub(crate) enum Error {
    Custom(String),
    TopLevelNotStruct,
    Fmt(std::fmt::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        Self::Custom(msg.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Custom(s) => write!(f, "Custom serde error: {s}"),
            Self::TopLevelNotStruct => write!(f, "Multipart supports only structs at top level"),
            Self::Fmt(inner) => write!(f, "Formatting error: {inner}"),
            Self::Io(inner) => write!(f, "Io error: {inner}"),
            Self::Json(inner) => write!(f, "Json (de)serialization error: {inner}"),
        }
    }
}

impl std::error::Error for Error {}

impl RequestError {
    /// Returns whether this error was caused while serializing a multipart
    /// request.
    #[must_use]
    pub fn is_multipart_serialization_error(&self) -> bool {
        matches!(
            self,
            Self::Io(error)
                if error.get_ref().is_some_and(|source| source.is::<Error>())
        )
    }
}

impl From<Error> for RequestError {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(ioerr) => RequestError::Io(Arc::new(ioerr)),

            error => RequestError::Io(Arc::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error,
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_errors_are_distinguished_from_file_io() {
        let serialization_error: RequestError = Error::TopLevelNotStruct.into();
        assert!(matches!(serialization_error, RequestError::Io(_)));
        assert!(serialization_error.is_multipart_serialization_error());

        let io_error: RequestError = Error::Io(std::io::Error::other("read failed")).into();
        assert!(matches!(io_error, RequestError::Io(_)));
        assert!(!io_error.is_multipart_serialization_error());
    }
}
