use jiff::{civil, tz::TimeZone, SignedDuration, Timestamp};

use super::{
    model::SignedTimeSpan, DateTimeFormat, TimeBindings, TimeError, TimeExpression, TimeValue,
};

#[derive(Clone, Debug)]
pub struct TimeContext {
    zone: TimeZone,
}

impl TimeContext {
    pub fn from_name(name: &str) -> Result<Self, super::TimeZoneError> {
        TimeZone::get(name)
            .map(|zone| Self { zone })
            .map_err(|source| super::TimeZoneError::Invalid { name: name.to_owned(), source })
    }

    pub fn zone(&self) -> &TimeZone {
        &self.zone
    }

    pub fn normalize(
        &self,
        expression: &TimeExpression,
        format: DateTimeFormat,
        captured_now: Timestamp,
        bindings: &TimeBindings,
    ) -> Result<NormalizedDateTime, TimeError> {
        let timestamp = match expression {
            TimeExpression::Now { offset } => apply_offset(captured_now, *offset)?,
            TimeExpression::Variable { name, offset } => {
                let value =
                    bindings.get(name).ok_or_else(|| TimeError::UnknownBinding(name.clone()))?;
                let timestamp = self.normalize_value(value, captured_now)?;
                apply_offset(timestamp, *offset)?
            }
            TimeExpression::Clock(time) => self.clock_timestamp(*time, captured_now)?,
            TimeExpression::CivilDate(date) => {
                date.to_zoned(self.zone.clone()).map_err(TimeError::InvalidCivil)?.timestamp()
            }
            TimeExpression::CivilDateTime(datetime) => {
                self.zone.to_zoned(*datetime).map_err(TimeError::InvalidCivil)?.timestamp()
            }
        };
        Ok(NormalizedDateTime {
            unix_time: timestamp.as_second(),
            fallback_text: self.fallback_text(timestamp, format),
            timestamp,
            format,
        })
    }

    fn normalize_value(
        &self,
        value: &TimeValue,
        captured_now: Timestamp,
    ) -> Result<Timestamp, TimeError> {
        match value {
            TimeValue::Instant(timestamp) => Ok(*timestamp),
            TimeValue::CivilDate(date) => date
                .to_zoned(self.zone.clone())
                .map_err(TimeError::InvalidCivil)
                .map(|zoned| zoned.timestamp()),
            TimeValue::CivilDateTime(datetime) => self
                .zone
                .to_zoned(*datetime)
                .map_err(TimeError::InvalidCivil)
                .map(|zoned| zoned.timestamp()),
            TimeValue::Clock(time) => self.clock_timestamp(*time, captured_now),
        }
    }

    fn clock_timestamp(
        &self,
        time: civil::Time,
        captured_now: Timestamp,
    ) -> Result<Timestamp, TimeError> {
        // Telegram requires an absolute timestamp even when the source only
        // contains a clock time. Bare clocks use the captured date in the
        // configured zone; exact scheduled moments should use an Instant or a
        // complete CivilDateTime. Jiff's compatible resolution intentionally
        // handles DST gaps and folds deterministically.
        let anchor = captured_now.to_zoned(self.zone.clone()).date();
        self.zone
            .to_zoned(anchor.to_datetime(time))
            .map_err(TimeError::InvalidCivil)
            .map(|zoned| zoned.timestamp())
    }

    fn fallback_text(&self, timestamp: Timestamp, format: DateTimeFormat) -> String {
        let datetime = timestamp.to_zoned(self.zone.clone()).datetime();
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
}

#[derive(Clone, Debug)]
pub struct NormalizedDateTime {
    pub timestamp: Timestamp,
    pub unix_time: i64,
    pub format: DateTimeFormat,
    pub fallback_text: String,
}

fn apply_offset(timestamp: Timestamp, offset: SignedTimeSpan) -> Result<Timestamp, TimeError> {
    let duration = SignedDuration::from_secs(offset.as_seconds());
    timestamp.checked_add(duration).map_err(TimeError::Overflow)
}
