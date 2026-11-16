#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventChannels(pub u32);
impl EventChannels {
    #[inline]
    pub fn new(value: u32) -> Self {
        EventChannels(value)
    }

    #[inline]
    pub fn clear(&mut self) -> &mut Self {
        self.0 = 0;
        self
    }

    #[inline]
    pub fn get_executions(&self) -> bool {
        0 != self.0 & (1 << 0)
    }

    #[inline]
    pub fn set_executions(&mut self, value: bool) -> &mut Self {
        self.0 = if value {
            self.0 | (1 << 0)
        } else {
            self.0 & !(1 << 0)
        };
        self
    }

    #[inline]
    pub fn get_orders(&self) -> bool {
        0 != self.0 & (1 << 1)
    }

    #[inline]
    pub fn set_orders(&mut self, value: bool) -> &mut Self {
        self.0 = if value {
            self.0 | (1 << 1)
        } else {
            self.0 & !(1 << 1)
        };
        self
    }

    #[inline]
    pub fn get_market_resync(&self) -> bool {
        0 != self.0 & (1 << 2)
    }

    #[inline]
    pub fn set_market_resync(&mut self, value: bool) -> &mut Self {
        self.0 = if value {
            self.0 | (1 << 2)
        } else {
            self.0 & !(1 << 2)
        };
        self
    }
}
impl core::fmt::Debug for EventChannels {
    #[inline]
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            fmt,
            "EventChannels[executions(0)={},orders(1)={},market_resync(2)={}]",
            self.get_executions(),
            self.get_orders(),
            self.get_market_resync(),
        )
    }
}
