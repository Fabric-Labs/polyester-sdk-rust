#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum FeeAsset {
    QUOTE = 0x1_u8,
    BASE = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for FeeAsset {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::QUOTE,
            0x2_u8 => Self::BASE,
            _ => Self::NullVal,
        }
    }
}
impl From<FeeAsset> for u8 {
    #[inline]
    fn from(v: FeeAsset) -> Self {
        match v {
            FeeAsset::QUOTE => 0x1_u8,
            FeeAsset::BASE => 0x2_u8,
            FeeAsset::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for FeeAsset {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "QUOTE" => Ok(Self::QUOTE),
            "BASE" => Ok(Self::BASE),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for FeeAsset {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::QUOTE => write!(f, "QUOTE"),
            Self::BASE => write!(f, "BASE"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
