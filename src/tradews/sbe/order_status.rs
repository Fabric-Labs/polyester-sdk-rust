#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrderStatus {
    PENDING = 0x1_u8,
    PENDING_CANCEL = 0x2_u8,
    PENDING_REPLACE = 0x3_u8,
    WORKING = 0x4_u8,
    FILLED = 0x5_u8,
    CANCELED = 0x6_u8,
    REJECTED = 0x7_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for OrderStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::PENDING,
            0x2_u8 => Self::PENDING_CANCEL,
            0x3_u8 => Self::PENDING_REPLACE,
            0x4_u8 => Self::WORKING,
            0x5_u8 => Self::FILLED,
            0x6_u8 => Self::CANCELED,
            0x7_u8 => Self::REJECTED,
            _ => Self::NullVal,
        }
    }
}
impl From<OrderStatus> for u8 {
    #[inline]
    fn from(v: OrderStatus) -> Self {
        match v {
            OrderStatus::PENDING => 0x1_u8,
            OrderStatus::PENDING_CANCEL => 0x2_u8,
            OrderStatus::PENDING_REPLACE => 0x3_u8,
            OrderStatus::WORKING => 0x4_u8,
            OrderStatus::FILLED => 0x5_u8,
            OrderStatus::CANCELED => 0x6_u8,
            OrderStatus::REJECTED => 0x7_u8,
            OrderStatus::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for OrderStatus {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "PENDING" => Ok(Self::PENDING),
            "PENDING_CANCEL" => Ok(Self::PENDING_CANCEL),
            "PENDING_REPLACE" => Ok(Self::PENDING_REPLACE),
            "WORKING" => Ok(Self::WORKING),
            "FILLED" => Ok(Self::FILLED),
            "CANCELED" => Ok(Self::CANCELED),
            "REJECTED" => Ok(Self::REJECTED),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for OrderStatus {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PENDING => write!(f, "PENDING"),
            Self::PENDING_CANCEL => write!(f, "PENDING_CANCEL"),
            Self::PENDING_REPLACE => write!(f, "PENDING_REPLACE"),
            Self::WORKING => write!(f, "WORKING"),
            Self::FILLED => write!(f, "FILLED"),
            Self::CANCELED => write!(f, "CANCELED"),
            Self::REJECTED => write!(f, "REJECTED"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
