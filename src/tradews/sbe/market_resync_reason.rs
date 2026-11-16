#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum MarketResyncReason {
    INTERRUPTED = 0x1_u8,
    DISCONTINUITY = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for MarketResyncReason {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::INTERRUPTED,
            0x2_u8 => Self::DISCONTINUITY,
            _ => Self::NullVal,
        }
    }
}
impl From<MarketResyncReason> for u8 {
    #[inline]
    fn from(v: MarketResyncReason) -> Self {
        match v {
            MarketResyncReason::INTERRUPTED => 0x1_u8,
            MarketResyncReason::DISCONTINUITY => 0x2_u8,
            MarketResyncReason::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for MarketResyncReason {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "INTERRUPTED" => Ok(Self::INTERRUPTED),
            "DISCONTINUITY" => Ok(Self::DISCONTINUITY),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for MarketResyncReason {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::INTERRUPTED => write!(f, "INTERRUPTED"),
            Self::DISCONTINUITY => write!(f, "DISCONTINUITY"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
