#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LiquidityRole {
    MAKER = 0x1_u8,
    TAKER = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for LiquidityRole {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::MAKER,
            0x2_u8 => Self::TAKER,
            _ => Self::NullVal,
        }
    }
}
impl From<LiquidityRole> for u8 {
    #[inline]
    fn from(v: LiquidityRole) -> Self {
        match v {
            LiquidityRole::MAKER => 0x1_u8,
            LiquidityRole::TAKER => 0x2_u8,
            LiquidityRole::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for LiquidityRole {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "MAKER" => Ok(Self::MAKER),
            "TAKER" => Ok(Self::TAKER),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for LiquidityRole {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MAKER => write!(f, "MAKER"),
            Self::TAKER => write!(f, "TAKER"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
