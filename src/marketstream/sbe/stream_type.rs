#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum StreamType {
    TRADES = 0x1_u8,
    BBO = 0x2_u8,
    DEPTH20 = 0x3_u8,
    DEPTH = 0x4_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for StreamType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::TRADES,
            0x2_u8 => Self::BBO,
            0x3_u8 => Self::DEPTH20,
            0x4_u8 => Self::DEPTH,
            _ => Self::NullVal,
        }
    }
}
impl From<StreamType> for u8 {
    #[inline]
    fn from(v: StreamType) -> Self {
        match v {
            StreamType::TRADES => 0x1_u8,
            StreamType::BBO => 0x2_u8,
            StreamType::DEPTH20 => 0x3_u8,
            StreamType::DEPTH => 0x4_u8,
            StreamType::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for StreamType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "TRADES" => Ok(Self::TRADES),
            "BBO" => Ok(Self::BBO),
            "DEPTH20" => Ok(Self::DEPTH20),
            "DEPTH" => Ok(Self::DEPTH),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for StreamType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TRADES => write!(f, "TRADES"),
            Self::BBO => write!(f, "BBO"),
            Self::DEPTH20 => write!(f, "DEPTH20"),
            Self::DEPTH => write!(f, "DEPTH"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
