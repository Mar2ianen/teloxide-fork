use serde::{Deserialize, Serialize};

use crate::types::Seconds;

/// Period in seconds for which the location can be updated, should be
/// between 60 and 86400, or 0x7FFFFFFF for live locations that can be
/// edited indefinitely.
#[derive(Clone, Copy)]
#[derive(Debug, derive_more::Display)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
#[derive(Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
// Its a wrapper for a wrapper (LivePeriod for Seconds), better to just tell the
// schemars that its a u32
#[cfg_attr(test, schemars(with = "u32"))]
#[serde(untagged)]
pub enum LivePeriod {
    Timeframe(Seconds),
    Indefinite,
}

impl LivePeriod {
    pub fn timeframe(&self) -> Option<Seconds> {
        self.try_into().ok()
    }

    pub fn is_indefinite(&self) -> bool {
        matches!(self, Self::Indefinite)
    }

    pub fn from_u32(seconds: u32) -> Self {
        seconds.into()
    }

    pub fn from_seconds(seconds: Seconds) -> Self {
        seconds.into()
    }
}

impl TryFrom<LivePeriod> for Seconds {
    type Error = &'static str;

    fn try_from(value: LivePeriod) -> Result<Self, Self::Error> {
        match value {
            LivePeriod::Timeframe(seconds) => Ok(seconds),
            LivePeriod::Indefinite => Err("indefinite live period"),
        }
    }
}

impl TryFrom<&LivePeriod> for Seconds {
    type Error = &'static str;

    fn try_from(value: &LivePeriod) -> Result<Self, Self::Error> {
        match value {
            LivePeriod::Timeframe(seconds) => Ok(*seconds),
            LivePeriod::Indefinite => Err("indefinite live period"),
        }
    }
}

impl From<Seconds> for LivePeriod {
    fn from(seconds: Seconds) -> Self {
        Self::Timeframe(seconds)
    }
}

impl From<u32> for LivePeriod {
    fn from(seconds: u32) -> Self {
        Self::Timeframe(Seconds::from_seconds(seconds))
    }
}

impl<'de> Deserialize<'de> for LivePeriod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;

        if value == 0x7FFFFFFF {
            Ok(LivePeriod::Indefinite)
        } else {
            Ok(LivePeriod::Timeframe(Seconds::from_seconds(value)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize)]
    struct Struct {
        live_period: Option<LivePeriod>,
    }

    #[test]
    fn deserialize_indefinite() {
        let json = r#"{"live_period": 2147483647}"#; // 0x7FFFFFFF
        let expected = LivePeriod::Indefinite;
        let Struct { live_period } = serde_json::from_str(json).unwrap();
        assert_eq!(live_period, Some(expected));
    }

    #[test]
    fn deserialize_900() {
        let json = r#"{"live_period": 900}"#;
        let expected = LivePeriod::from_u32(900);
        let Struct { live_period } = serde_json::from_str(json).unwrap();
        assert_eq!(live_period, Some(expected));
    }

    #[test]
    fn from_seconds_creates_timeframe() {
        let seconds = Seconds::from_seconds(900);

        assert_eq!(LivePeriod::from_seconds(seconds), LivePeriod::Timeframe(seconds));
    }

    #[test]
    fn seconds_into_live_period_creates_timeframe() {
        let seconds = Seconds::from_seconds(900);
        let period: LivePeriod = seconds.into();

        assert_eq!(period, LivePeriod::Timeframe(seconds));
    }

    #[test]
    fn u32_into_live_period_creates_timeframe() {
        let period: LivePeriod = 900_u32.into();

        assert_eq!(period, LivePeriod::Timeframe(Seconds::from_seconds(900)));
    }

    #[test]
    fn indefinite_cannot_be_converted_to_seconds() {
        assert_eq!(Seconds::try_from(LivePeriod::Indefinite), Err("indefinite live period"));
    }
}
