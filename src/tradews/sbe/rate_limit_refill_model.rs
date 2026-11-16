#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum RateLimitRefillModel {
    CONTINUOUS = 0x1_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for RateLimitRefillModel {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::CONTINUOUS,
            _ => Self::NullVal,
        }
    }
}
impl From<RateLimitRefillModel> for u8 {
    #[inline]
    fn from(v: RateLimitRefillModel) -> Self {
        match v {
            RateLimitRefillModel::CONTINUOUS => 0x1_u8,
            RateLimitRefillModel::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for RateLimitRefillModel {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "CONTINUOUS" => Ok(Self::CONTINUOUS),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for RateLimitRefillModel {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::CONTINUOUS => write!(f, "CONTINUOUS"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
