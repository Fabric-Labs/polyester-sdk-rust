#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum RateLimitScope {
    ACCOUNT = 0x1_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for RateLimitScope {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::ACCOUNT,
            _ => Self::NullVal,
        }
    }
}
impl From<RateLimitScope> for u8 {
    #[inline]
    fn from(v: RateLimitScope) -> Self {
        match v {
            RateLimitScope::ACCOUNT => 0x1_u8,
            RateLimitScope::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for RateLimitScope {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "ACCOUNT" => Ok(Self::ACCOUNT),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for RateLimitScope {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ACCOUNT => write!(f, "ACCOUNT"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
