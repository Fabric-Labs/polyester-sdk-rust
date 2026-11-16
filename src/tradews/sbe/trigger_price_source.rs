#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TriggerPriceSource {
    LAST_PRICE = 0x1_u8,
    INDEX_PRICE = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for TriggerPriceSource {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::LAST_PRICE,
            0x2_u8 => Self::INDEX_PRICE,
            _ => Self::NullVal,
        }
    }
}
impl From<TriggerPriceSource> for u8 {
    #[inline]
    fn from(v: TriggerPriceSource) -> Self {
        match v {
            TriggerPriceSource::LAST_PRICE => 0x1_u8,
            TriggerPriceSource::INDEX_PRICE => 0x2_u8,
            TriggerPriceSource::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for TriggerPriceSource {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "LAST_PRICE" => Ok(Self::LAST_PRICE),
            "INDEX_PRICE" => Ok(Self::INDEX_PRICE),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for TriggerPriceSource {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::LAST_PRICE => write!(f, "LAST_PRICE"),
            Self::INDEX_PRICE => write!(f, "INDEX_PRICE"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
