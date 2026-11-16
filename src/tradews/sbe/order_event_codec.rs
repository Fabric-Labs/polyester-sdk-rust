use super::*;

pub use decoder::OrderEventDecoder;
pub use encoder::OrderEventEncoder;

pub use super::SBE_SCHEMA_ID;
pub use super::SBE_SCHEMA_VERSION;
pub use super::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 77;
pub const SBE_TEMPLATE_ID: u16 = 25;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct OrderEventEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for OrderEventEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for OrderEventEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> OrderEventEncoder<'a> {
        pub fn wrap(mut self, buf: WriteBuf<'a>, offset: usize) -> Self {
            let limit = offset + SBE_BLOCK_LENGTH as usize;
            self.buf = buf;
            self.initial_offset = offset;
            self.offset = offset;
            self.limit = limit;
            self
        }

        #[inline]
        pub fn encoded_length(&self) -> usize {
            self.limit - self.offset
        }

        pub fn header(self, offset: usize) -> MessageHeaderEncoder<Self> {
            let mut header = MessageHeaderEncoder::default().wrap(self, offset);
            header.block_length(SBE_BLOCK_LENGTH);
            header.template_id(SBE_TEMPLATE_ID);
            header.schema_id(SBE_SCHEMA_ID);
            header.version(SBE_SCHEMA_VERSION);
            header
        }

        /// primitive field 'symbolId'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn symbol_id(&mut self, value: u32) {
            let offset = self.offset;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// primitive field 'orderId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 4
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn order_id(&mut self, value: u64) {
            let offset = self.offset + 4;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'version'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 12
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn version(&mut self, value: u32) {
            let offset = self.offset + 12;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn status(&mut self, value: order_status::OrderStatus) {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'remainingQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 17
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn remaining_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 17;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'cumulativeQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 25
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn cumulative_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 25;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'eventTsNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 33
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn event_ts_ns(&mut self, value: u64) {
            let offset = self.offset + 33;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'gatewayTsNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 41
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn gateway_ts_ns(&mut self, value: u64) {
            let offset = self.offset + 41;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn time_in_force(&mut self, value: time_in_force::TimeInForce) {
            let offset = self.offset + 49;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'expireAtTsNs'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 50
        /// - encodedLength: 8
        /// - version: 11
        #[inline]
        pub fn expire_at_ts_ns(&mut self, value: i64) {
            let offset = self.offset + 50;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn reject_reason(&mut self, value: reject_code::RejectCode) {
            let offset = self.offset + 58;
            self.get_buf_mut().put_u16_at(offset, value as u16)
        }

        /// REQUIRED enum
        #[inline]
        pub fn cancel_reason(&mut self, value: cancel_reason::CancelReason) {
            let offset = self.offset + 60;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'displayQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 61
        /// - encodedLength: 8
        /// - version: 44
        #[inline]
        pub fn display_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 61;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'hiddenQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 69
        /// - encodedLength: 8
        /// - version: 44
        #[inline]
        pub fn hidden_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 69;
            self.get_buf_mut().put_i64_at(offset, value);
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct OrderEventDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for OrderEventDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for OrderEventDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for OrderEventDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> OrderEventDecoder<'a> {
        pub fn wrap(
            mut self,
            buf: ReadBuf<'a>,
            offset: usize,
            acting_block_length: u16,
            acting_version: u16,
        ) -> Self {
            let limit = offset + acting_block_length as usize;
            self.buf = buf;
            self.initial_offset = offset;
            self.offset = offset;
            self.limit = limit;
            self.acting_block_length = acting_block_length;
            self.acting_version = acting_version;
            self
        }

        #[inline]
        pub fn encoded_length(&self) -> usize {
            self.limit - self.offset
        }

        pub fn header(self, mut header: MessageHeaderDecoder<ReadBuf<'a>>, offset: usize) -> Self {
            debug_assert_eq!(SBE_TEMPLATE_ID, header.template_id());
            let acting_block_length = header.block_length();
            let acting_version = header.version();

            self.wrap(
                header.parent().unwrap(),
                offset + message_header_codec::ENCODED_LENGTH,
                acting_block_length,
                acting_version,
            )
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn symbol_id(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn order_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 4)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn version(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 12)
        }

        /// REQUIRED enum
        #[inline]
        pub fn status(&self) -> order_status::OrderStatus {
            self.get_buf().get_u8_at(self.offset + 16).into()
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn remaining_qty_scaled(&self) -> i64 {
            self.get_buf().get_i64_at(self.offset + 17)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn cumulative_qty_scaled(&self) -> i64 {
            self.get_buf().get_i64_at(self.offset + 25)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn event_ts_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 33)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn gateway_ts_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 41)
        }

        /// REQUIRED enum
        #[inline]
        pub fn time_in_force(&self) -> time_in_force::TimeInForce {
            if self.acting_version() < 11 {
                return time_in_force::TimeInForce::default();
            }

            self.get_buf().get_u8_at(self.offset + 49).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn expire_at_ts_ns(&self) -> Option<i64> {
            if self.acting_version() < 11 {
                return None;
            }

            let value = self.get_buf().get_i64_at(self.offset + 50);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// REQUIRED enum
        #[inline]
        pub fn reject_reason(&self) -> reject_code::RejectCode {
            if self.acting_version() < 33 {
                return reject_code::RejectCode::default();
            }

            self.get_buf().get_u16_at(self.offset + 58).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn cancel_reason(&self) -> cancel_reason::CancelReason {
            if self.acting_version() < 33 {
                return cancel_reason::CancelReason::default();
            }

            self.get_buf().get_u8_at(self.offset + 60).into()
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn display_qty_scaled(&self) -> i64 {
            if self.acting_version() < 44 {
                return -9223372036854775808_i64;
            }

            self.get_buf().get_i64_at(self.offset + 61)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn hidden_qty_scaled(&self) -> i64 {
            if self.acting_version() < 44 {
                return -9223372036854775808_i64;
            }

            self.get_buf().get_i64_at(self.offset + 69)
        }
    }
} // end decoder
