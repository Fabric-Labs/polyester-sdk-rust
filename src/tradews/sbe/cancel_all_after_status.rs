#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CancelAllAfterStatus {
    ARMED = 0x1_u8,
    DISABLED = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for CancelAllAfterStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::ARMED,
            0x2_u8 => Self::DISABLED,
            _ => Self::NullVal,
        }
    }
}
impl From<CancelAllAfterStatus> for u8 {
    #[inline]
    fn from(v: CancelAllAfterStatus) -> Self {
        match v {
            CancelAllAfterStatus::ARMED => 0x1_u8,
            CancelAllAfterStatus::DISABLED => 0x2_u8,
            CancelAllAfterStatus::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for CancelAllAfterStatus {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "ARMED" => Ok(Self::ARMED),
            "DISABLED" => Ok(Self::DISABLED),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for CancelAllAfterStatus {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ARMED => write!(f, "ARMED"),
            Self::DISABLED => write!(f, "DISABLED"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
