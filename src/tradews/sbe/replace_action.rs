#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ReplaceAction {
    AMENDED = 0x1_u8,
    REPLACED = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for ReplaceAction {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::AMENDED,
            0x2_u8 => Self::REPLACED,
            _ => Self::NullVal,
        }
    }
}
impl From<ReplaceAction> for u8 {
    #[inline]
    fn from(v: ReplaceAction) -> Self {
        match v {
            ReplaceAction::AMENDED => 0x1_u8,
            ReplaceAction::REPLACED => 0x2_u8,
            ReplaceAction::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for ReplaceAction {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "AMENDED" => Ok(Self::AMENDED),
            "REPLACED" => Ok(Self::REPLACED),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ReplaceAction {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AMENDED => write!(f, "AMENDED"),
            Self::REPLACED => write!(f, "REPLACED"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
