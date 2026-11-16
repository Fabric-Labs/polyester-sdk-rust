#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrderType {
    LIMIT = 0x1_u8,
    MARKET = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for OrderType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::LIMIT,
            0x2_u8 => Self::MARKET,
            _ => Self::NullVal,
        }
    }
}
impl From<OrderType> for u8 {
    #[inline]
    fn from(v: OrderType) -> Self {
        match v {
            OrderType::LIMIT => 0x1_u8,
            OrderType::MARKET => 0x2_u8,
            OrderType::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for OrderType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "LIMIT" => Ok(Self::LIMIT),
            "MARKET" => Ok(Self::MARKET),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for OrderType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::LIMIT => write!(f, "LIMIT"),
            Self::MARKET => write!(f, "MARKET"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
