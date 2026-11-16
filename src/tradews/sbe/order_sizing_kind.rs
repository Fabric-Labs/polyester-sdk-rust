#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrderSizingKind {
    BASE_QUANTITY = 0x1_u8,
    MAX_QUOTE_DEBIT = 0x2_u8,
    QUOTE_PROCEEDS = 0x3_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for OrderSizingKind {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::BASE_QUANTITY,
            0x2_u8 => Self::MAX_QUOTE_DEBIT,
            0x3_u8 => Self::QUOTE_PROCEEDS,
            _ => Self::NullVal,
        }
    }
}
impl From<OrderSizingKind> for u8 {
    #[inline]
    fn from(v: OrderSizingKind) -> Self {
        match v {
            OrderSizingKind::BASE_QUANTITY => 0x1_u8,
            OrderSizingKind::MAX_QUOTE_DEBIT => 0x2_u8,
            OrderSizingKind::QUOTE_PROCEEDS => 0x3_u8,
            OrderSizingKind::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for OrderSizingKind {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "BASE_QUANTITY" => Ok(Self::BASE_QUANTITY),
            "MAX_QUOTE_DEBIT" => Ok(Self::MAX_QUOTE_DEBIT),
            "QUOTE_PROCEEDS" => Ok(Self::QUOTE_PROCEEDS),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for OrderSizingKind {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BASE_QUANTITY => write!(f, "BASE_QUANTITY"),
            Self::MAX_QUOTE_DEBIT => write!(f, "MAX_QUOTE_DEBIT"),
            Self::QUOTE_PROCEEDS => write!(f, "QUOTE_PROCEEDS"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
