//! A helpful trait for rendering text/caption and entities back to HTML or
//! Markdown.

use teloxide_core::types::Message;

use super::Renderer;

/// Generates HTML and Markdown representations of text and captions in a
/// Telegram message.
pub trait RenderMessageTextHelper {
    /// Returns the HTML representation of the message text, if the message
    /// contains text. This method will parse the text and any entities
    /// (such as bold, italic, links, etc.) and return the HTML-formatted
    /// string.
    #[must_use]
    fn html_text(&self) -> Option<String>;

    /// Returns the Markdown representation of the message text, if the message
    /// contains text. This method will parse the text and any entities
    /// (such as bold, italic, links, etc.) and return the
    /// Markdown-formatted string.
    #[must_use]
    fn markdown_text(&self) -> Option<String>;

    /// Returns the HTML representation of the message caption, if the message
    /// contains caption. This method will parse the caption and any
    /// entities (such as bold, italic, links, etc.) and return the
    /// HTML-formatted string.
    #[must_use]
    fn html_caption(&self) -> Option<String>;

    /// Returns the Markdown representation of the message caption, if the
    /// message contains caption. This method will parse the caption and any
    /// entities (such as bold, italic, links, etc.) and return the
    /// Markdown-formatted string.
    #[must_use]
    fn markdown_caption(&self) -> Option<String>;
}

impl RenderMessageTextHelper for Message {
    fn html_text(&self) -> Option<String> {
        self.text().map(|text| Renderer::new(text, self.entities().unwrap_or_default()).as_html())
    }

    fn markdown_text(&self) -> Option<String> {
        self.text()
            .map(|text| Renderer::new(text, self.entities().unwrap_or_default()).as_markdown())
    }

    fn html_caption(&self) -> Option<String> {
        self.caption()
            .map(|text| Renderer::new(text, self.caption_entities().unwrap_or_default()).as_html())
    }

    fn markdown_caption(&self) -> Option<String> {
        self.caption().map(|text| {
            Renderer::new(text, self.caption_entities().unwrap_or_default()).as_markdown()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teloxide_core::types::Message;

    fn plain_message(json: &str) -> Message {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn plain_text_without_entities_renders_some() {
        let message = plain_message(
            r#"{
                "message_id": 1,
                "date": 0,
                "chat": {"id": 1, "type": "private", "first_name": "A"},
                "text": "plain"
            }"#,
        );

        assert_eq!(message.html_text(), Some("plain".to_owned()));
        assert_eq!(message.markdown_text(), Some("plain".to_owned()));
    }

    #[test]
    fn plain_caption_without_entities_renders_some() {
        let message = plain_message(
            r#"{
                "message_id": 1,
                "date": 0,
                "chat": {"id": 1, "type": "private", "first_name": "A"},
                "photo": [{"file_id": "f", "file_unique_id": "u", "width": 1, "height": 1}],
                "caption": "cap"
            }"#,
        );

        assert_eq!(message.html_caption(), Some("cap".to_owned()));
        assert_eq!(message.markdown_caption(), Some("cap".to_owned()));
    }
}
