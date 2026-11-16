#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum SubscribeResult {
    OK = 0x1_u8,
    UNKNOWN_SYMBOL = 0x2_u8,
    LIMIT_EXCEEDED = 0x3_u8,
    ALREADY_SUBSCRIBED = 0x4_u8,
    NOT_SUBSCRIBED = 0x5_u8,
    SYMBOL_DISABLED = 0x7_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for SubscribeResult {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::OK,
            0x2_u8 => Self::UNKNOWN_SYMBOL,
            0x3_u8 => Self::LIMIT_EXCEEDED,
            0x4_u8 => Self::ALREADY_SUBSCRIBED,
            0x5_u8 => Self::NOT_SUBSCRIBED,
            0x7_u8 => Self::SYMBOL_DISABLED,
            _ => Self::NullVal,
        }
    }
}
impl From<SubscribeResult> for u8 {
    #[inline]
    fn from(v: SubscribeResult) -> Self {
        match v {
            SubscribeResult::OK => 0x1_u8,
            SubscribeResult::UNKNOWN_SYMBOL => 0x2_u8,
            SubscribeResult::LIMIT_EXCEEDED => 0x3_u8,
            SubscribeResult::ALREADY_SUBSCRIBED => 0x4_u8,
            SubscribeResult::NOT_SUBSCRIBED => 0x5_u8,
            SubscribeResult::SYMBOL_DISABLED => 0x7_u8,
            SubscribeResult::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for SubscribeResult {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "OK" => Ok(Self::OK),
            "UNKNOWN_SYMBOL" => Ok(Self::UNKNOWN_SYMBOL),
            "LIMIT_EXCEEDED" => Ok(Self::LIMIT_EXCEEDED),
            "ALREADY_SUBSCRIBED" => Ok(Self::ALREADY_SUBSCRIBED),
            "NOT_SUBSCRIBED" => Ok(Self::NOT_SUBSCRIBED),
            "SYMBOL_DISABLED" => Ok(Self::SYMBOL_DISABLED),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for SubscribeResult {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OK => write!(f, "OK"),
            Self::UNKNOWN_SYMBOL => write!(f, "UNKNOWN_SYMBOL"),
            Self::LIMIT_EXCEEDED => write!(f, "LIMIT_EXCEEDED"),
            Self::ALREADY_SUBSCRIBED => write!(f, "ALREADY_SUBSCRIBED"),
            Self::NOT_SUBSCRIBED => write!(f, "NOT_SUBSCRIBED"),
            Self::SYMBOL_DISABLED => write!(f, "SYMBOL_DISABLED"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
