#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum RateLimitPolicyClass {
    TRADING_PLACE = 0x1_u8,
    TRADING_CANCEL = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for RateLimitPolicyClass {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::TRADING_PLACE,
            0x2_u8 => Self::TRADING_CANCEL,
            _ => Self::NullVal,
        }
    }
}
impl From<RateLimitPolicyClass> for u8 {
    #[inline]
    fn from(v: RateLimitPolicyClass) -> Self {
        match v {
            RateLimitPolicyClass::TRADING_PLACE => 0x1_u8,
            RateLimitPolicyClass::TRADING_CANCEL => 0x2_u8,
            RateLimitPolicyClass::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for RateLimitPolicyClass {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "TRADING_PLACE" => Ok(Self::TRADING_PLACE),
            "TRADING_CANCEL" => Ok(Self::TRADING_CANCEL),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for RateLimitPolicyClass {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TRADING_PLACE => write!(f, "TRADING_PLACE"),
            Self::TRADING_CANCEL => write!(f, "TRADING_CANCEL"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
