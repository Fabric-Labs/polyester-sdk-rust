#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ModifyBehavior {
    AMEND_OR_REPLACE = 0x1_u8,
    AMEND_ONLY = 0x2_u8,
    REPLACE_ONLY = 0x3_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for ModifyBehavior {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::AMEND_OR_REPLACE,
            0x2_u8 => Self::AMEND_ONLY,
            0x3_u8 => Self::REPLACE_ONLY,
            _ => Self::NullVal,
        }
    }
}
impl From<ModifyBehavior> for u8 {
    #[inline]
    fn from(v: ModifyBehavior) -> Self {
        match v {
            ModifyBehavior::AMEND_OR_REPLACE => 0x1_u8,
            ModifyBehavior::AMEND_ONLY => 0x2_u8,
            ModifyBehavior::REPLACE_ONLY => 0x3_u8,
            ModifyBehavior::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for ModifyBehavior {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "AMEND_OR_REPLACE" => Ok(Self::AMEND_OR_REPLACE),
            "AMEND_ONLY" => Ok(Self::AMEND_ONLY),
            "REPLACE_ONLY" => Ok(Self::REPLACE_ONLY),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ModifyBehavior {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AMEND_OR_REPLACE => write!(f, "AMEND_OR_REPLACE"),
            Self::AMEND_ONLY => write!(f, "AMEND_ONLY"),
            Self::REPLACE_ONLY => write!(f, "REPLACE_ONLY"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
