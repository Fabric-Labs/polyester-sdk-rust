#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CancelAllListingStatus {
    UNSPECIFIED = 0x0_u8,
    INCLUDED = 0x1_u8,
    NOT_RETAINED_READ_OPEN_ORDERS = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for CancelAllListingStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::UNSPECIFIED,
            0x1_u8 => Self::INCLUDED,
            0x2_u8 => Self::NOT_RETAINED_READ_OPEN_ORDERS,
            _ => Self::NullVal,
        }
    }
}
impl From<CancelAllListingStatus> for u8 {
    #[inline]
    fn from(v: CancelAllListingStatus) -> Self {
        match v {
            CancelAllListingStatus::UNSPECIFIED => 0x0_u8,
            CancelAllListingStatus::INCLUDED => 0x1_u8,
            CancelAllListingStatus::NOT_RETAINED_READ_OPEN_ORDERS => 0x2_u8,
            CancelAllListingStatus::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for CancelAllListingStatus {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "UNSPECIFIED" => Ok(Self::UNSPECIFIED),
            "INCLUDED" => Ok(Self::INCLUDED),
            "NOT_RETAINED_READ_OPEN_ORDERS" => Ok(Self::NOT_RETAINED_READ_OPEN_ORDERS),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for CancelAllListingStatus {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UNSPECIFIED => write!(f, "UNSPECIFIED"),
            Self::INCLUDED => write!(f, "INCLUDED"),
            Self::NOT_RETAINED_READ_OPEN_ORDERS => write!(f, "NOT_RETAINED_READ_OPEN_ORDERS"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
