#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum STPMode {
    EXPIRE_MAKER = 0x1_u8,
    EXPIRE_TAKER = 0x2_u8,
    EXPIRE_BOTH = 0x3_u8,
    DECREMENT = 0x4_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for STPMode {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::EXPIRE_MAKER,
            0x2_u8 => Self::EXPIRE_TAKER,
            0x3_u8 => Self::EXPIRE_BOTH,
            0x4_u8 => Self::DECREMENT,
            _ => Self::NullVal,
        }
    }
}
impl From<STPMode> for u8 {
    #[inline]
    fn from(v: STPMode) -> Self {
        match v {
            STPMode::EXPIRE_MAKER => 0x1_u8,
            STPMode::EXPIRE_TAKER => 0x2_u8,
            STPMode::EXPIRE_BOTH => 0x3_u8,
            STPMode::DECREMENT => 0x4_u8,
            STPMode::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for STPMode {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "EXPIRE_MAKER" => Ok(Self::EXPIRE_MAKER),
            "EXPIRE_TAKER" => Ok(Self::EXPIRE_TAKER),
            "EXPIRE_BOTH" => Ok(Self::EXPIRE_BOTH),
            "DECREMENT" => Ok(Self::DECREMENT),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for STPMode {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::EXPIRE_MAKER => write!(f, "EXPIRE_MAKER"),
            Self::EXPIRE_TAKER => write!(f, "EXPIRE_TAKER"),
            Self::EXPIRE_BOTH => write!(f, "EXPIRE_BOTH"),
            Self::DECREMENT => write!(f, "DECREMENT"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
