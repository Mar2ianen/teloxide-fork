use std::fmt;

/// A segment in a [`RequestFieldPath`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum RequestFieldPathSegment {
    /// A named request field.
    Field(&'static str),
    /// An item in a request collection.
    Index(usize),
}

/// A structured path to a request field.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct RequestFieldPath {
    segments: Vec<RequestFieldPathSegment>,
}

impl RequestFieldPath {
    /// Creates an empty path.
    #[must_use]
    pub const fn new() -> Self {
        Self { segments: Vec::new() }
    }

    /// Creates a path containing one field segment.
    #[must_use]
    pub fn field(name: &'static str) -> Self {
        let mut path = Self::new();
        path.push_field(name);
        path
    }

    /// Appends a field segment.
    pub fn push_field(&mut self, name: &'static str) {
        self.segments.push(RequestFieldPathSegment::Field(name));
    }

    /// Appends an index segment.
    pub fn push_index(&mut self, index: usize) {
        self.segments.push(RequestFieldPathSegment::Index(index));
    }

    /// Removes the last segment.
    pub fn pop(&mut self) {
        self.segments.pop();
    }

    /// Returns the path segments.
    #[must_use]
    pub fn segments(&self) -> &[RequestFieldPathSegment] {
        &self.segments
    }
}

impl fmt::Display for RequestFieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, segment) in self.segments.iter().enumerate() {
            match segment {
                RequestFieldPathSegment::Field(name) => {
                    if index != 0 {
                        f.write_str(".")?;
                    }
                    f.write_str(name)?;
                }
                RequestFieldPathSegment::Index(value) => write!(f, "[{value}]")?,
            }
        }

        if self.segments.is_empty() {
            f.write_str("request")?;
        }
        Ok(())
    }
}

/// A reason why a value is invalid before it is sent to Telegram.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum InvalidValueReason {
    /// The value must not be zero.
    MustBeNonZero,
}

impl fmt::Display for InvalidValueReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MustBeNonZero => f.write_str("must be non-zero"),
        }
    }
}

/// The rich-message sending context used by context-sensitive validation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RichMessageContext {
    /// `sendRichMessage` or another regular message send method.
    Send,
    /// `editMessageText` for a regular message.
    Edit,
    /// `editMessageText` for an inline message.
    EditInline,
    /// `sendRichMessageDraft`.
    Draft,
    /// An inline query result.
    InlineResult,
    /// An `answerGuestQuery` result.
    GuestResult,
    /// An `answerWebAppQuery` or `savePreparedInlineMessage` result.
    PreparedMessage,
}

impl fmt::Display for RichMessageContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Send => "rich message send",
            Self::Edit => "rich message edit",
            Self::EditInline => "inline rich message edit",
            Self::Draft => "rich message draft",
            Self::InlineResult => "inline result",
            Self::GuestResult => "guest result",
            Self::PreparedMessage => "prepared message",
        };
        f.write_str(name)
    }
}

/// A statically known reason why a request cannot be sent in its current form.
#[derive(Clone, Debug, Eq, Hash, PartialEq, thiserror::Error)]
pub enum RequestValidationError {
    /// A scalar value violates a method or Bot API constraint.
    #[error("invalid value at {path}: {reason}")]
    InvalidValue {
        /// Path to the invalid value.
        path: RequestFieldPath,
        /// The known violated constraint.
        reason: InvalidValueReason,
    },
    /// A valid object is not allowed by the selected method context.
    #[error("{path} is not supported in {context}")]
    UnsupportedInContext {
        /// Path to the unsupported object.
        path: RequestFieldPath,
        /// The method context that rejected it.
        context: RichMessageContext,
    },
    /// A new file upload is forbidden by the selected method.
    #[error("direct file upload is not allowed at {path}")]
    DirectUploadNotAllowed {
        /// Path to the forbidden file source.
        path: RequestFieldPath,
    },
}

/// Validates a request or another payload before serialization and dispatch.
///
/// Implementations must only check constraints that can be determined from the
/// value itself. A successful result does not guarantee that Telegram accepts
/// the request; permissions and server-side capabilities remain Telegram's
/// responsibility. A validation error means that no HTTP request was sent.
pub trait Validate {
    /// Checks static request constraints without serializing or consuming
    /// `self`.
    fn validate(&self) -> Result<(), RequestValidationError>;
}

/// Validates a value whose validity depends on a selected request context.
pub trait ValidateWith<C> {
    /// Checks static constraints for `self` in `context`.
    fn validate_with(&self, context: &C) -> Result<(), RequestValidationError>;
}

/// Validates the non-rich `sendMessageDraft` payload.
pub(crate) fn validate_send_message_draft(
    payload: &crate::payloads::SendMessageDraft,
) -> Result<(), RequestValidationError> {
    if payload.draft_id == 0 {
        return Err(RequestValidationError::InvalidValue {
            path: RequestFieldPath::field("draft_id"),
            reason: InvalidValueReason::MustBeNonZero,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payloads::SendMessageDraft,
        requests::{Payload, Request, Requester},
        types::UserId,
        Bot,
    };

    #[test]
    fn field_path_formats_nested_fields_and_indexes() {
        let mut path = RequestFieldPath::field("rich_message");
        path.push_field("blocks");
        path.push_index(2);
        path.push_field("details");
        path.push_field("blocks");
        path.push_index(1);

        assert_eq!(path.to_string(), "rich_message.blocks[2].details.blocks[1]");
    }

    #[test]
    fn send_message_draft_rejects_zero_draft_id() {
        let payload = SendMessageDraft::new(UserId(1), 0);

        assert_eq!(
            payload.validate(),
            Err(RequestValidationError::InvalidValue {
                path: RequestFieldPath::field("draft_id"),
                reason: InvalidValueReason::MustBeNonZero,
            })
        );
        assert_eq!(
            payload.validate().unwrap_err().to_string(),
            "invalid value at draft_id: must be non-zero"
        );
    }

    #[test]
    fn send_message_draft_accepts_non_zero_draft_id() {
        let payload = SendMessageDraft::new(UserId(1), -1);
        assert_eq!(payload.validate(), Ok(()));
    }

    #[tokio::test]
    async fn invalid_payload_is_rejected_before_http_dispatch() {
        let result = Bot::new("token").send_message_draft(UserId(1), 0).send().await;

        assert!(matches!(
            result,
            Err(crate::RequestError::Validation(RequestValidationError::InvalidValue { .. }))
        ));
    }
}
