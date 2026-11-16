#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum BatchStatus {
    ADMITTED = 0x1_u8,
    PARTIALLY_ADMITTED = 0x2_u8,
    REJECTED = 0x3_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for BatchStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::ADMITTED,
            0x2_u8 => Self::PARTIALLY_ADMITTED,
            0x3_u8 => Self::REJECTED,
            _ => Self::NullVal,
        }
    }
}
impl From<BatchStatus> for u8 {
    #[inline]
    fn from(v: BatchStatus) -> Self {
        match v {
            BatchStatus::ADMITTED => 0x1_u8,
            BatchStatus::PARTIALLY_ADMITTED => 0x2_u8,
            BatchStatus::REJECTED => 0x3_u8,
            BatchStatus::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for BatchStatus {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "ADMITTED" => Ok(Self::ADMITTED),
            "PARTIALLY_ADMITTED" => Ok(Self::PARTIALLY_ADMITTED),
            "REJECTED" => Ok(Self::REJECTED),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for BatchStatus {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ADMITTED => write!(f, "ADMITTED"),
            Self::PARTIALLY_ADMITTED => write!(f, "PARTIALLY_ADMITTED"),
            Self::REJECTED => write!(f, "REJECTED"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
