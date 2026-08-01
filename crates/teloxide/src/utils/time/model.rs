use std::collections::HashMap;

use jiff::civil::{Date, DateTime, Time};

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DateTimeFormat {
    Time,
    Date,
    DateTime,
    Relative,
}

impl DateTimeFormat {
    pub const fn wire_value(self) -> &'static str {
        match self {
            Self::Time => "t",
            Self::Date => "D",
            Self::DateTime => "Dt",
            Self::Relative => "r",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignedTimeSpan {
    seconds: i64,
}

impl SignedTimeSpan {
    pub const ZERO: Self = Self { seconds: 0 };

    pub const fn from_seconds(seconds: i64) -> Self {
        Self { seconds }
    }

    pub const fn as_seconds(self) -> i64 {
        self.seconds
    }

    pub(crate) fn parse_unsigned(input: &str) -> Result<Self, String> {
        let input = input.trim();
        if input.is_empty() {
            return Err("time span is empty".to_owned());
        }

        let bytes = input.as_bytes();
        let mut index = 0;
        let mut total = 0i128;
        let mut components = 0;
        while index < bytes.len() {
            let start = index;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            if start == index {
                return Err("time span must contain a positive integer and a unit".to_owned());
            }
            let number = input[start..index]
                .parse::<i128>()
                .map_err(|_| "time span number is too large".to_owned())?;
            let unit = bytes.get(index).copied().map(char::from);
            let seconds = match unit {
                Some('s') => 1,
                Some('m') => 60,
                Some('h') => 60 * 60,
                Some('d') => 24 * 60 * 60,
                Some('w') => 7 * 24 * 60 * 60,
                _ => return Err("unsupported time span unit".to_owned()),
            };
            index += 1;
            total = total
                .checked_add(number.checked_mul(seconds).ok_or("time span is too large")?)
                .ok_or("time span is too large")?;
            components += 1;
        }
        if components == 0 {
            return Err("time span is empty".to_owned());
        }
        let seconds = i64::try_from(total).map_err(|_| "time span is too large".to_owned())?;
        Ok(Self { seconds })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TimeValue {
    Instant(jiff::Timestamp),
    CivilDate(Date),
    CivilDateTime(DateTime),
    Clock(Time),
}

#[derive(Clone, Debug, PartialEq)]
pub enum TimeExpression {
    Clock(Time),
    CivilDate(Date),
    CivilDateTime(DateTime),
    Now { offset: SignedTimeSpan },
    Variable { name: String, offset: SignedTimeSpan },
}

#[derive(Clone, Debug, PartialEq)]
pub struct DateTimeNode {
    pub expression: TimeExpression,
    pub format: DateTimeFormat,
    pub source_range: std::ops::Range<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RichNode {
    Text(String),
    DateTime(DateTimeNode),
}

#[derive(Clone, Debug, Default)]
pub struct TimeBindings {
    values: HashMap<String, TimeValue>,
}

impl TimeBindings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: impl Into<String>, value: TimeValue) -> Option<TimeValue> {
        self.values.insert(name.into(), value)
    }

    pub fn get(&self, name: &str) -> Option<&TimeValue> {
        self.values.get(name)
    }
}

pub(crate) fn parse_signed_offset(input: &str) -> Result<SignedTimeSpan, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("time offset is empty".to_owned());
    }
    let (sign, span) = match input.as_bytes().first().copied() {
        Some(b'+') => (1i64, &input[1..]),
        Some(b'-') => (-1i64, &input[1..]),
        _ => return Err("time offset must start with `+` or `-`".to_owned()),
    };
    let span = SignedTimeSpan::parse_unsigned(span)?;
    Ok(SignedTimeSpan::from_seconds(
        span.as_seconds().checked_mul(sign).ok_or_else(|| "time offset is too large".to_owned())?,
    ))
}

pub(crate) fn parse_expression(input: &str) -> Result<TimeExpression, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("time expression is empty".to_owned());
    }

    if let Some(rest) = input.strip_prefix("now") {
        if rest.trim().is_empty() {
            return Ok(TimeExpression::Now { offset: SignedTimeSpan::ZERO });
        }
        return Ok(TimeExpression::Now { offset: parse_signed_offset(rest)? });
    }

    if let Some(rest) = input.strip_prefix('$') {
        let (name, offset) = split_variable(rest)?;
        return Ok(TimeExpression::Variable { name, offset });
    }

    if input.len() == 5 {
        let time = input.parse::<Time>().map_err(|_| "invalid clock value".to_owned())?;
        if time.second() != 0 || time.nanosecond() != 0 {
            return Err("seconds are not supported in civil literals".to_owned());
        }
        return Ok(TimeExpression::Clock(time));
    }
    if input.len() == 10 {
        let date = input.parse::<Date>().map_err(|_| "invalid civil date".to_owned())?;
        return Ok(TimeExpression::CivilDate(date));
    }
    if input.len() == 16 && input.as_bytes().get(10) == Some(&b'T') {
        let datetime =
            input.parse::<DateTime>().map_err(|_| "invalid civil datetime".to_owned())?;
        if datetime.second() != 0 || datetime.nanosecond() != 0 {
            return Err("seconds are not supported in civil literals".to_owned());
        }
        return Ok(TimeExpression::CivilDateTime(datetime));
    }
    Err("unsupported time expression".to_owned())
}

fn split_variable(input: &str) -> Result<(String, SignedTimeSpan), String> {
    let mut end = 0;
    for (index, byte) in input.bytes().enumerate() {
        if index == 0 && !(byte.is_ascii_alphabetic() || byte == b'_') {
            return Err("binding name must start with a letter or underscore".to_owned());
        }
        if byte.is_ascii_alphanumeric() || byte == b'_' {
            end = index + 1;
        } else {
            break;
        }
    }
    if end == 0 {
        return Err("binding name is empty".to_owned());
    }
    let name = input[..end].to_owned();
    let rest = input[end..].trim();
    let offset = if rest.is_empty() { SignedTimeSpan::ZERO } else { parse_signed_offset(rest)? };
    Ok((name, offset))
}
