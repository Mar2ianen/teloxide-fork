use jiff::{civil, Timestamp};
use teloxide_core::types::{RichText, RichTextDateTime, RichTextObject};

use super::{
    DateTimeFormat, NormalizedDateTime, SignedTimeSpan, TimeContext, TimeError, TimeExpression,
};

#[derive(Clone, Debug)]
pub struct DateTimeToken {
    normalized: NormalizedDateTime,
}

impl DateTimeToken {
    pub fn instant(value: Timestamp, format: DateTimeFormat) -> Self {
        let fallback_text = fallback_in_utc(value, format);
        Self {
            normalized: NormalizedDateTime {
                timestamp: value,
                unix_time: value.as_second(),
                format,
                fallback_text,
            },
        }
    }

    pub fn instant_in(context: &TimeContext, value: Timestamp, format: DateTimeFormat) -> Self {
        Self {
            normalized: context
                .normalize(
                    &TimeExpression::Now { offset: SignedTimeSpan::ZERO },
                    format,
                    value,
                    &Default::default(),
                )
                .expect("an Instant cannot fail normalization"),
        }
    }

    pub fn civil_date(
        context: &TimeContext,
        value: civil::Date,
        format: DateTimeFormat,
    ) -> Result<Self, TimeError> {
        Self::from_expression(context, TimeExpression::CivilDate(value), format)
    }

    pub fn civil_datetime(
        context: &TimeContext,
        value: civil::DateTime,
        format: DateTimeFormat,
    ) -> Result<Self, TimeError> {
        Self::from_expression(context, TimeExpression::CivilDateTime(value), format)
    }

    pub fn clock_at(
        context: &TimeContext,
        value: civil::Time,
        anchor: civil::Date,
        format: DateTimeFormat,
    ) -> Result<Self, TimeError> {
        Self::from_expression(
            context,
            TimeExpression::CivilDateTime(anchor.to_datetime(value)),
            format,
        )
    }

    pub fn from_now(
        captured_now: Timestamp,
        offset: SignedTimeSpan,
        format: DateTimeFormat,
    ) -> Result<Self, TimeError> {
        let timestamp = captured_now
            .checked_add(jiff::SignedDuration::from_secs(offset.as_seconds()))
            .map_err(TimeError::Overflow)?;
        Ok(Self::instant(timestamp, format))
    }

    pub fn normalized(&self) -> &NormalizedDateTime {
        &self.normalized
    }

    pub fn to_markdown(&self) -> String {
        format!(
            "![{}](tg://time?unix={}&format={})",
            self.normalized.fallback_text,
            self.normalized.unix_time,
            self.normalized.format.wire_value()
        )
    }

    pub fn to_html(&self) -> String {
        format!(
            "<tg-time unix=\"{}\" format=\"{}\">{}</tg-time>",
            self.normalized.unix_time,
            self.normalized.format.wire_value(),
            escape_html(&self.normalized.fallback_text)
        )
    }

    pub fn to_rich_text(&self) -> RichText {
        RichText::Object(RichTextObject::DateTime(RichTextDateTime {
            text: Box::new(RichText::from(self.normalized.fallback_text.clone())),
            unix_time: self.normalized.unix_time,
            date_time_format: self.normalized.format.wire_value().to_owned(),
        }))
    }

    fn from_expression(
        context: &TimeContext,
        expression: TimeExpression,
        format: DateTimeFormat,
    ) -> Result<Self, TimeError> {
        let normalized =
            context.normalize(&expression, format, Timestamp::now(), &Default::default())?;
        Ok(Self { normalized })
    }
}

fn fallback_in_utc(timestamp: Timestamp, format: DateTimeFormat) -> String {
    let datetime = timestamp.to_zoned(jiff::tz::TimeZone::UTC).datetime();
    match format {
        DateTimeFormat::Time => format!("{:02}:{:02}", datetime.hour(), datetime.minute()),
        DateTimeFormat::Date => {
            format!("{:04}-{:02}-{:02}", datetime.year(), datetime.month(), datetime.day())
        }
        DateTimeFormat::DateTime | DateTimeFormat::Relative => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}",
            datetime.year(),
            datetime.month(),
            datetime.day(),
            datetime.hour(),
            datetime.minute()
        ),
    }
}

fn escape_html(value: &str) -> String {
    value.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_emitters_use_telegram_time_wire_values() {
        let timestamp: Timestamp = "2026-08-01T10:00:00Z".parse().unwrap();
        let token = DateTimeToken::instant(timestamp, DateTimeFormat::DateTime);
        assert!(token.to_markdown().contains("format=Dt"));
        assert!(token.to_html().contains("format=\"Dt\""));
        assert!(matches!(token.to_rich_text(), RichText::Object(RichTextObject::DateTime(_))));
    }
}
