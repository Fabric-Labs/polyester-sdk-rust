#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CommandType {
    PING = 0x1_u8,
    CREATE_ORDER = 0x2_u8,
    CANCEL_ORDER = 0x3_u8,
    MODIFY_ORDER = 0x4_u8,
    CANCEL_ALL_ORDERS = 0x5_u8,
    SUBSCRIBE = 0x6_u8,
    BATCH_CREATE_ORDERS = 0x7_u8,
    BATCH_REPLACE_ORDERS = 0x8_u8,
    BATCH_CANCEL_ORDERS = 0x9_u8,
    CANCEL_ALL_AFTER = 0xa_u8,
    CREATE_OCO = 0xb_u8,
    CANCEL_OCO = 0xc_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for CommandType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::PING,
            0x2_u8 => Self::CREATE_ORDER,
            0x3_u8 => Self::CANCEL_ORDER,
            0x4_u8 => Self::MODIFY_ORDER,
            0x5_u8 => Self::CANCEL_ALL_ORDERS,
            0x6_u8 => Self::SUBSCRIBE,
            0x7_u8 => Self::BATCH_CREATE_ORDERS,
            0x8_u8 => Self::BATCH_REPLACE_ORDERS,
            0x9_u8 => Self::BATCH_CANCEL_ORDERS,
            0xa_u8 => Self::CANCEL_ALL_AFTER,
            0xb_u8 => Self::CREATE_OCO,
            0xc_u8 => Self::CANCEL_OCO,
            _ => Self::NullVal,
        }
    }
}
impl From<CommandType> for u8 {
    #[inline]
    fn from(v: CommandType) -> Self {
        match v {
            CommandType::PING => 0x1_u8,
            CommandType::CREATE_ORDER => 0x2_u8,
            CommandType::CANCEL_ORDER => 0x3_u8,
            CommandType::MODIFY_ORDER => 0x4_u8,
            CommandType::CANCEL_ALL_ORDERS => 0x5_u8,
            CommandType::SUBSCRIBE => 0x6_u8,
            CommandType::BATCH_CREATE_ORDERS => 0x7_u8,
            CommandType::BATCH_REPLACE_ORDERS => 0x8_u8,
            CommandType::BATCH_CANCEL_ORDERS => 0x9_u8,
            CommandType::CANCEL_ALL_AFTER => 0xa_u8,
            CommandType::CREATE_OCO => 0xb_u8,
            CommandType::CANCEL_OCO => 0xc_u8,
            CommandType::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for CommandType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "PING" => Ok(Self::PING),
            "CREATE_ORDER" => Ok(Self::CREATE_ORDER),
            "CANCEL_ORDER" => Ok(Self::CANCEL_ORDER),
            "MODIFY_ORDER" => Ok(Self::MODIFY_ORDER),
            "CANCEL_ALL_ORDERS" => Ok(Self::CANCEL_ALL_ORDERS),
            "SUBSCRIBE" => Ok(Self::SUBSCRIBE),
            "BATCH_CREATE_ORDERS" => Ok(Self::BATCH_CREATE_ORDERS),
            "BATCH_REPLACE_ORDERS" => Ok(Self::BATCH_REPLACE_ORDERS),
            "BATCH_CANCEL_ORDERS" => Ok(Self::BATCH_CANCEL_ORDERS),
            "CANCEL_ALL_AFTER" => Ok(Self::CANCEL_ALL_AFTER),
            "CREATE_OCO" => Ok(Self::CREATE_OCO),
            "CANCEL_OCO" => Ok(Self::CANCEL_OCO),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for CommandType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PING => write!(f, "PING"),
            Self::CREATE_ORDER => write!(f, "CREATE_ORDER"),
            Self::CANCEL_ORDER => write!(f, "CANCEL_ORDER"),
            Self::MODIFY_ORDER => write!(f, "MODIFY_ORDER"),
            Self::CANCEL_ALL_ORDERS => write!(f, "CANCEL_ALL_ORDERS"),
            Self::SUBSCRIBE => write!(f, "SUBSCRIBE"),
            Self::BATCH_CREATE_ORDERS => write!(f, "BATCH_CREATE_ORDERS"),
            Self::BATCH_REPLACE_ORDERS => write!(f, "BATCH_REPLACE_ORDERS"),
            Self::BATCH_CANCEL_ORDERS => write!(f, "BATCH_CANCEL_ORDERS"),
            Self::CANCEL_ALL_AFTER => write!(f, "CANCEL_ALL_AFTER"),
            Self::CREATE_OCO => write!(f, "CREATE_OCO"),
            Self::CANCEL_OCO => write!(f, "CANCEL_OCO"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
