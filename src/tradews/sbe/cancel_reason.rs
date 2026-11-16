#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CancelReason {
    USER_REQUEST = 0x1_u8,
    IOC_REMAINDER = 0x2_u8,
    MARKET_REMAINDER = 0x3_u8,
    STP_EXPIRE_MAKER = 0x4_u8,
    STP_EXPIRE_TAKER = 0x5_u8,
    STP_EXPIRE_BOTH = 0x6_u8,
    REPLACED = 0x7_u8,
    GTD_EXPIRED = 0x8_u8,
    QTY_AT_OR_BELOW_FILLED = 0x9_u8,
    PAIR_DRAIN = 0xa_u8,
    ATTACHED_RISK_REFUSED = 0xb_u8,
    CANCEL_ALL_AFTER = 0xc_u8,
    ACCOUNT_REPAIR = 0xd_u8,
    TWAP_ROLLOVER = 0xe_u8,
    STRATEGY_CANCEL = 0xf_u8,
    SYSTEM = 0x10_u8,
    MARKET_HALTED = 0x11_u8,
    ATTACHED_EXIT_FIRED = 0x12_u8,
    STP_DECREMENT = 0x13_u8,
    PRICE_BAND = 0x14_u8,
    CANCEL_ON_DISCONNECT = 0x15_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for CancelReason {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::USER_REQUEST,
            0x2_u8 => Self::IOC_REMAINDER,
            0x3_u8 => Self::MARKET_REMAINDER,
            0x4_u8 => Self::STP_EXPIRE_MAKER,
            0x5_u8 => Self::STP_EXPIRE_TAKER,
            0x6_u8 => Self::STP_EXPIRE_BOTH,
            0x7_u8 => Self::REPLACED,
            0x8_u8 => Self::GTD_EXPIRED,
            0x9_u8 => Self::QTY_AT_OR_BELOW_FILLED,
            0xa_u8 => Self::PAIR_DRAIN,
            0xb_u8 => Self::ATTACHED_RISK_REFUSED,
            0xc_u8 => Self::CANCEL_ALL_AFTER,
            0xd_u8 => Self::ACCOUNT_REPAIR,
            0xe_u8 => Self::TWAP_ROLLOVER,
            0xf_u8 => Self::STRATEGY_CANCEL,
            0x10_u8 => Self::SYSTEM,
            0x11_u8 => Self::MARKET_HALTED,
            0x12_u8 => Self::ATTACHED_EXIT_FIRED,
            0x13_u8 => Self::STP_DECREMENT,
            0x14_u8 => Self::PRICE_BAND,
            0x15_u8 => Self::CANCEL_ON_DISCONNECT,
            _ => Self::NullVal,
        }
    }
}
impl From<CancelReason> for u8 {
    #[inline]
    fn from(v: CancelReason) -> Self {
        match v {
            CancelReason::USER_REQUEST => 0x1_u8,
            CancelReason::IOC_REMAINDER => 0x2_u8,
            CancelReason::MARKET_REMAINDER => 0x3_u8,
            CancelReason::STP_EXPIRE_MAKER => 0x4_u8,
            CancelReason::STP_EXPIRE_TAKER => 0x5_u8,
            CancelReason::STP_EXPIRE_BOTH => 0x6_u8,
            CancelReason::REPLACED => 0x7_u8,
            CancelReason::GTD_EXPIRED => 0x8_u8,
            CancelReason::QTY_AT_OR_BELOW_FILLED => 0x9_u8,
            CancelReason::PAIR_DRAIN => 0xa_u8,
            CancelReason::ATTACHED_RISK_REFUSED => 0xb_u8,
            CancelReason::CANCEL_ALL_AFTER => 0xc_u8,
            CancelReason::ACCOUNT_REPAIR => 0xd_u8,
            CancelReason::TWAP_ROLLOVER => 0xe_u8,
            CancelReason::STRATEGY_CANCEL => 0xf_u8,
            CancelReason::SYSTEM => 0x10_u8,
            CancelReason::MARKET_HALTED => 0x11_u8,
            CancelReason::ATTACHED_EXIT_FIRED => 0x12_u8,
            CancelReason::STP_DECREMENT => 0x13_u8,
            CancelReason::PRICE_BAND => 0x14_u8,
            CancelReason::CANCEL_ON_DISCONNECT => 0x15_u8,
            CancelReason::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for CancelReason {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "USER_REQUEST" => Ok(Self::USER_REQUEST),
            "IOC_REMAINDER" => Ok(Self::IOC_REMAINDER),
            "MARKET_REMAINDER" => Ok(Self::MARKET_REMAINDER),
            "STP_EXPIRE_MAKER" => Ok(Self::STP_EXPIRE_MAKER),
            "STP_EXPIRE_TAKER" => Ok(Self::STP_EXPIRE_TAKER),
            "STP_EXPIRE_BOTH" => Ok(Self::STP_EXPIRE_BOTH),
            "REPLACED" => Ok(Self::REPLACED),
            "GTD_EXPIRED" => Ok(Self::GTD_EXPIRED),
            "QTY_AT_OR_BELOW_FILLED" => Ok(Self::QTY_AT_OR_BELOW_FILLED),
            "PAIR_DRAIN" => Ok(Self::PAIR_DRAIN),
            "ATTACHED_RISK_REFUSED" => Ok(Self::ATTACHED_RISK_REFUSED),
            "CANCEL_ALL_AFTER" => Ok(Self::CANCEL_ALL_AFTER),
            "ACCOUNT_REPAIR" => Ok(Self::ACCOUNT_REPAIR),
            "TWAP_ROLLOVER" => Ok(Self::TWAP_ROLLOVER),
            "STRATEGY_CANCEL" => Ok(Self::STRATEGY_CANCEL),
            "SYSTEM" => Ok(Self::SYSTEM),
            "MARKET_HALTED" => Ok(Self::MARKET_HALTED),
            "ATTACHED_EXIT_FIRED" => Ok(Self::ATTACHED_EXIT_FIRED),
            "STP_DECREMENT" => Ok(Self::STP_DECREMENT),
            "PRICE_BAND" => Ok(Self::PRICE_BAND),
            "CANCEL_ON_DISCONNECT" => Ok(Self::CANCEL_ON_DISCONNECT),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for CancelReason {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::USER_REQUEST => write!(f, "USER_REQUEST"),
            Self::IOC_REMAINDER => write!(f, "IOC_REMAINDER"),
            Self::MARKET_REMAINDER => write!(f, "MARKET_REMAINDER"),
            Self::STP_EXPIRE_MAKER => write!(f, "STP_EXPIRE_MAKER"),
            Self::STP_EXPIRE_TAKER => write!(f, "STP_EXPIRE_TAKER"),
            Self::STP_EXPIRE_BOTH => write!(f, "STP_EXPIRE_BOTH"),
            Self::REPLACED => write!(f, "REPLACED"),
            Self::GTD_EXPIRED => write!(f, "GTD_EXPIRED"),
            Self::QTY_AT_OR_BELOW_FILLED => write!(f, "QTY_AT_OR_BELOW_FILLED"),
            Self::PAIR_DRAIN => write!(f, "PAIR_DRAIN"),
            Self::ATTACHED_RISK_REFUSED => write!(f, "ATTACHED_RISK_REFUSED"),
            Self::CANCEL_ALL_AFTER => write!(f, "CANCEL_ALL_AFTER"),
            Self::ACCOUNT_REPAIR => write!(f, "ACCOUNT_REPAIR"),
            Self::TWAP_ROLLOVER => write!(f, "TWAP_ROLLOVER"),
            Self::STRATEGY_CANCEL => write!(f, "STRATEGY_CANCEL"),
            Self::SYSTEM => write!(f, "SYSTEM"),
            Self::MARKET_HALTED => write!(f, "MARKET_HALTED"),
            Self::ATTACHED_EXIT_FIRED => write!(f, "ATTACHED_EXIT_FIRED"),
            Self::STP_DECREMENT => write!(f, "STP_DECREMENT"),
            Self::PRICE_BAND => write!(f, "PRICE_BAND"),
            Self::CANCEL_ON_DISCONNECT => write!(f, "CANCEL_ON_DISCONNECT"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
