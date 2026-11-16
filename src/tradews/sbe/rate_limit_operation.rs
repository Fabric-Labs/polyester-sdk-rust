#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum RateLimitOperation {
    CREATE_ORDER = 0x1_u8,
    CANCEL_ORDER = 0x2_u8,
    MODIFY_ORDER = 0x3_u8,
    CANCEL_ALL_ORDERS = 0x4_u8,
    BATCH_CREATE_ORDERS = 0x5_u8,
    BATCH_REPLACE_ORDERS = 0x6_u8,
    BATCH_CANCEL_ORDERS = 0x7_u8,
    CANCEL_ALL_AFTER = 0x8_u8,
    CREATE_TRIGGER = 0x9_u8,
    CANCEL_TRIGGER = 0xa_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for RateLimitOperation {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::CREATE_ORDER,
            0x2_u8 => Self::CANCEL_ORDER,
            0x3_u8 => Self::MODIFY_ORDER,
            0x4_u8 => Self::CANCEL_ALL_ORDERS,
            0x5_u8 => Self::BATCH_CREATE_ORDERS,
            0x6_u8 => Self::BATCH_REPLACE_ORDERS,
            0x7_u8 => Self::BATCH_CANCEL_ORDERS,
            0x8_u8 => Self::CANCEL_ALL_AFTER,
            0x9_u8 => Self::CREATE_TRIGGER,
            0xa_u8 => Self::CANCEL_TRIGGER,
            _ => Self::NullVal,
        }
    }
}
impl From<RateLimitOperation> for u8 {
    #[inline]
    fn from(v: RateLimitOperation) -> Self {
        match v {
            RateLimitOperation::CREATE_ORDER => 0x1_u8,
            RateLimitOperation::CANCEL_ORDER => 0x2_u8,
            RateLimitOperation::MODIFY_ORDER => 0x3_u8,
            RateLimitOperation::CANCEL_ALL_ORDERS => 0x4_u8,
            RateLimitOperation::BATCH_CREATE_ORDERS => 0x5_u8,
            RateLimitOperation::BATCH_REPLACE_ORDERS => 0x6_u8,
            RateLimitOperation::BATCH_CANCEL_ORDERS => 0x7_u8,
            RateLimitOperation::CANCEL_ALL_AFTER => 0x8_u8,
            RateLimitOperation::CREATE_TRIGGER => 0x9_u8,
            RateLimitOperation::CANCEL_TRIGGER => 0xa_u8,
            RateLimitOperation::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for RateLimitOperation {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "CREATE_ORDER" => Ok(Self::CREATE_ORDER),
            "CANCEL_ORDER" => Ok(Self::CANCEL_ORDER),
            "MODIFY_ORDER" => Ok(Self::MODIFY_ORDER),
            "CANCEL_ALL_ORDERS" => Ok(Self::CANCEL_ALL_ORDERS),
            "BATCH_CREATE_ORDERS" => Ok(Self::BATCH_CREATE_ORDERS),
            "BATCH_REPLACE_ORDERS" => Ok(Self::BATCH_REPLACE_ORDERS),
            "BATCH_CANCEL_ORDERS" => Ok(Self::BATCH_CANCEL_ORDERS),
            "CANCEL_ALL_AFTER" => Ok(Self::CANCEL_ALL_AFTER),
            "CREATE_TRIGGER" => Ok(Self::CREATE_TRIGGER),
            "CANCEL_TRIGGER" => Ok(Self::CANCEL_TRIGGER),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for RateLimitOperation {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::CREATE_ORDER => write!(f, "CREATE_ORDER"),
            Self::CANCEL_ORDER => write!(f, "CANCEL_ORDER"),
            Self::MODIFY_ORDER => write!(f, "MODIFY_ORDER"),
            Self::CANCEL_ALL_ORDERS => write!(f, "CANCEL_ALL_ORDERS"),
            Self::BATCH_CREATE_ORDERS => write!(f, "BATCH_CREATE_ORDERS"),
            Self::BATCH_REPLACE_ORDERS => write!(f, "BATCH_REPLACE_ORDERS"),
            Self::BATCH_CANCEL_ORDERS => write!(f, "BATCH_CANCEL_ORDERS"),
            Self::CANCEL_ALL_AFTER => write!(f, "CANCEL_ALL_AFTER"),
            Self::CREATE_TRIGGER => write!(f, "CREATE_TRIGGER"),
            Self::CANCEL_TRIGGER => write!(f, "CANCEL_TRIGGER"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
