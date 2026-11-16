use super::*;

pub use decoder::CreateOrderDecoder;
pub use encoder::CreateOrderEncoder;

pub use super::SBE_SCHEMA_ID;
pub use super::SBE_SCHEMA_VERSION;
pub use super::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 111;
pub const SBE_TEMPLATE_ID: u16 = 2;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct CreateOrderEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for CreateOrderEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for CreateOrderEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> CreateOrderEncoder<'a> {
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

        /// primitive field 'seq'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn seq(&mut self, value: u64) {
            let offset = self.offset;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'correlationId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 8
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn correlation_id(&mut self, value: u64) {
            let offset = self.offset + 8;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'symbolId'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 16
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn symbol_id(&mut self, value: u32) {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn side(&mut self, value: side::Side) {
            let offset = self.offset + 20;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn order_type(&mut self, value: order_type::OrderType) {
            let offset = self.offset + 21;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn time_in_force(&mut self, value: time_in_force::TimeInForce) {
            let offset = self.offset + 22;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'baseQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 23
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn base_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 23;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'priceTicks'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 31
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn price_ticks(&mut self, value: i64) {
            let offset = self.offset + 31;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        #[inline]
        pub fn flags(&mut self, value: order_flags::OrderFlags) {
            let offset = self.offset + 39;
            self.get_buf_mut().put_u32_at(offset, value.0)
        }

        /// REQUIRED enum
        #[inline]
        pub fn fee_asset(&mut self, value: fee_asset::FeeAsset) {
            let offset = self.offset + 43;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn stp_mode(&mut self, value: stp_mode::STPMode) {
            let offset = self.offset + 44;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'marketClientRefPriceTicks'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 45
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn market_client_ref_price_ticks(&mut self, value: i64) {
            let offset = self.offset + 45;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'marketMaxSlippageTicks'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 53
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn market_max_slippage_ticks(&mut self, value: i64) {
            let offset = self.offset + 53;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'marketMaxSlippageBps'
        /// - min value: -2147483647
        /// - max value: 2147483647
        /// - null value: -2147483648_i32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 61
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn market_max_slippage_bps(&mut self, value: i32) {
            let offset = self.offset + 61;
            self.get_buf_mut().put_i32_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn sizing_kind(&mut self, value: order_sizing_kind::OrderSizingKind) {
            let offset = self.offset + 65;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'quoteQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 66
        /// - encodedLength: 8
        /// - version: 2
        #[inline]
        pub fn quote_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 66;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'expireAtTsNs'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 74
        /// - encodedLength: 8
        /// - version: 11
        #[inline]
        pub fn expire_at_ts_ns(&mut self, value: i64) {
            let offset = self.offset + 74;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'timestampMs'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 82
        /// - encodedLength: 8
        /// - version: 41
        #[inline]
        pub fn timestamp_ms(&mut self, value: u64) {
            let offset = self.offset + 82;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'recvWindowMs'
        /// - min value: 1
        /// - max value: 4294967294
        /// - null value: 0x0_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 90
        /// - encodedLength: 4
        /// - version: 41
        #[inline]
        pub fn recv_window_ms(&mut self, value: u32) {
            let offset = self.offset + 90;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// primitive field 'displayQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 94
        /// - encodedLength: 8
        /// - version: 44
        #[inline]
        pub fn display_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 94;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'pegReference'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 102
        /// - encodedLength: 1
        /// - version: 47
        #[inline]
        pub fn peg_reference(&mut self, value: u8) {
            let offset = self.offset + 102;
            self.get_buf_mut().put_u8_at(offset, value);
        }

        /// primitive field 'pegOffsetTicks'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 103
        /// - encodedLength: 8
        /// - version: 47
        #[inline]
        pub fn peg_offset_ticks(&mut self, value: i64) {
            let offset = self.offset + 103;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// VAR_DATA ENCODER - character encoding: 'US-ASCII'
        #[inline]
        pub fn client_order_id(&mut self, value: &[u8]) {
            let limit = self.get_limit();
            let data_length = value.len();
            self.set_limit(limit + 4 + data_length);
            self.get_buf_mut().put_u32_at(limit, data_length as u32);
            self.get_buf_mut().put_slice_at(limit + 4, value);
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct CreateOrderDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for CreateOrderDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for CreateOrderDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for CreateOrderDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> CreateOrderDecoder<'a> {
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
        pub fn seq(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn correlation_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 8)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn symbol_id(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 16)
        }

        /// REQUIRED enum
        #[inline]
        pub fn side(&self) -> side::Side {
            self.get_buf().get_u8_at(self.offset + 20).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn order_type(&self) -> order_type::OrderType {
            self.get_buf().get_u8_at(self.offset + 21).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn time_in_force(&self) -> time_in_force::TimeInForce {
            self.get_buf().get_u8_at(self.offset + 22).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn base_qty_scaled(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 23);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn price_ticks(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 31);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// BIT SET DECODER
        #[inline]
        pub fn flags(&self) -> order_flags::OrderFlags {
            order_flags::OrderFlags::new(self.get_buf().get_u32_at(self.offset + 39))
        }

        /// REQUIRED enum
        #[inline]
        pub fn fee_asset(&self) -> fee_asset::FeeAsset {
            self.get_buf().get_u8_at(self.offset + 43).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn stp_mode(&self) -> stp_mode::STPMode {
            self.get_buf().get_u8_at(self.offset + 44).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn market_client_ref_price_ticks(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 45);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn market_max_slippage_ticks(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 53);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '-2147483648_i32' }
        #[inline]
        pub fn market_max_slippage_bps(&self) -> Option<i32> {
            let value = self.get_buf().get_i32_at(self.offset + 61);
            if value == -2147483648_i32 {
                None
            } else {
                Some(value)
            }
        }

        /// REQUIRED enum
        #[inline]
        pub fn sizing_kind(&self) -> order_sizing_kind::OrderSizingKind {
            if self.acting_version() < 2 {
                return order_sizing_kind::OrderSizingKind::default();
            }

            self.get_buf().get_u8_at(self.offset + 65).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn quote_qty_scaled(&self) -> Option<i64> {
            if self.acting_version() < 2 {
                return None;
            }

            let value = self.get_buf().get_i64_at(self.offset + 66);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn expire_at_ts_ns(&self) -> Option<i64> {
            if self.acting_version() < 11 {
                return None;
            }

            let value = self.get_buf().get_i64_at(self.offset + 74);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn timestamp_ms(&self) -> Option<u64> {
            if self.acting_version() < 41 {
                return None;
            }

            let value = self.get_buf().get_u64_at(self.offset + 82);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u32' }
        #[inline]
        pub fn recv_window_ms(&self) -> Option<u32> {
            if self.acting_version() < 41 {
                return None;
            }

            let value = self.get_buf().get_u32_at(self.offset + 90);
            if value == 0x0_u32 { None } else { Some(value) }
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn display_qty_scaled(&self) -> i64 {
            if self.acting_version() < 44 {
                return -9223372036854775808_i64;
            }

            self.get_buf().get_i64_at(self.offset + 94)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn peg_reference(&self) -> u8 {
            if self.acting_version() < 47 {
                return 0xff_u8;
            }

            self.get_buf().get_u8_at(self.offset + 102)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn peg_offset_ticks(&self) -> i64 {
            if self.acting_version() < 47 {
                return -9223372036854775808_i64;
            }

            self.get_buf().get_i64_at(self.offset + 103)
        }

        /// VAR_DATA DECODER - character encoding: 'US-ASCII'
        #[inline]
        pub fn client_order_id_decoder(&mut self) -> (usize, usize) {
            let offset = self.get_limit();
            let data_length = self.get_buf().get_u32_at(offset) as usize;
            self.set_limit(offset + 4 + data_length);
            (offset + 4, data_length)
        }

        #[inline]
        pub fn client_order_id_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }
    }
} // end decoder
