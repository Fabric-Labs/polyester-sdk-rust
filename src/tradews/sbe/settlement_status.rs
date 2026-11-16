#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum SettlementStatus {
    PENDING = 0x1_u8,
    SETTLED = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for SettlementStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::PENDING,
            0x2_u8 => Self::SETTLED,
            _ => Self::NullVal,
        }
    }
}
impl From<SettlementStatus> for u8 {
    #[inline]
    fn from(v: SettlementStatus) -> Self {
        match v {
            SettlementStatus::PENDING => 0x1_u8,
            SettlementStatus::SETTLED => 0x2_u8,
            SettlementStatus::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for SettlementStatus {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "PENDING" => Ok(Self::PENDING),
            "SETTLED" => Ok(Self::SETTLED),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for SettlementStatus {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PENDING => write!(f, "PENDING"),
            Self::SETTLED => write!(f, "SETTLED"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
