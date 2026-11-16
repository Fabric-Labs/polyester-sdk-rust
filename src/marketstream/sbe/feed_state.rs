#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum FeedState {
    LIVE = 0x1_u8,
    STALE = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for FeedState {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::LIVE,
            0x2_u8 => Self::STALE,
            _ => Self::NullVal,
        }
    }
}
impl From<FeedState> for u8 {
    #[inline]
    fn from(v: FeedState) -> Self {
        match v {
            FeedState::LIVE => 0x1_u8,
            FeedState::STALE => 0x2_u8,
            FeedState::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for FeedState {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "LIVE" => Ok(Self::LIVE),
            "STALE" => Ok(Self::STALE),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for FeedState {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::LIVE => write!(f, "LIVE"),
            Self::STALE => write!(f, "STALE"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
