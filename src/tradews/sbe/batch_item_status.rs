#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum BatchItemStatus {
    ACCEPTED = 0x1_u8,
    REJECTED = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for BatchItemStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::ACCEPTED,
            0x2_u8 => Self::REJECTED,
            _ => Self::NullVal,
        }
    }
}
impl From<BatchItemStatus> for u8 {
    #[inline]
    fn from(v: BatchItemStatus) -> Self {
        match v {
            BatchItemStatus::ACCEPTED => 0x1_u8,
            BatchItemStatus::REJECTED => 0x2_u8,
            BatchItemStatus::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for BatchItemStatus {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "ACCEPTED" => Ok(Self::ACCEPTED),
            "REJECTED" => Ok(Self::REJECTED),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for BatchItemStatus {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ACCEPTED => write!(f, "ACCEPTED"),
            Self::REJECTED => write!(f, "REJECTED"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
