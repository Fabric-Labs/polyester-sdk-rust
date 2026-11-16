use super::*;

pub use decoder::BatchCreateOrdersDecoder;
pub use encoder::BatchCreateOrdersEncoder;

pub use super::SBE_SCHEMA_ID;
pub use super::SBE_SCHEMA_VERSION;
pub use super::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 28;
pub const SBE_TEMPLATE_ID: u16 = 7;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct BatchCreateOrdersEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for BatchCreateOrdersEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for BatchCreateOrdersEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> BatchCreateOrdersEncoder<'a> {
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

        /// primitive field 'timestampMs'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 16
        /// - encodedLength: 8
        /// - version: 41
        #[inline]
        pub fn timestamp_ms(&mut self, value: u64) {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'recvWindowMs'
        /// - min value: 1
        /// - max value: 4294967294
        /// - null value: 0x0_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 24
        /// - encodedLength: 4
        /// - version: 41
        #[inline]
        pub fn recv_window_ms(&mut self, value: u32) {
            let offset = self.offset + 24;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// GROUP ENCODER (id=3, description='1 to 20 orders, answered in this order; items may name different trading pairs')
        #[inline]
        pub fn items_encoder(
            self,
            count: u16,
            items_encoder: ItemsEncoder<Self>,
        ) -> ItemsEncoder<Self> {
            items_encoder.wrap(self, count)
        }
    }

    #[derive(Debug, Default)]
    pub struct ItemsEncoder<P> {
        parent: Option<P>,
        count: u16,
        index: usize,
        offset: usize,
        initial_limit: usize,
    }

    impl<'a, P> Writer<'a> for ItemsEncoder<P>
    where
        P: Writer<'a> + Default,
    {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            if let Some(parent) = self.parent.as_mut() {
                parent.get_buf_mut()
            } else {
                panic!("parent was None")
            }
        }
    }

    impl<'a, P> Encoder<'a> for ItemsEncoder<P>
    where
        P: Encoder<'a> + Default,
    {
        #[inline]
        fn get_limit(&self) -> usize {
            self.parent.as_ref().expect("parent missing").get_limit()
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.parent
                .as_mut()
                .expect("parent missing")
                .set_limit(limit);
        }
    }

    impl<'a, P> ItemsEncoder<P>
    where
        P: Encoder<'a> + Default,
    {
        #[inline]
        pub fn wrap(mut self, mut parent: P, count: u16) -> Self {
            let initial_limit = parent.get_limit();
            parent.set_limit(initial_limit + 4);
            parent
                .get_buf_mut()
                .put_u16_at(initial_limit, Self::block_length());
            parent.get_buf_mut().put_u16_at(initial_limit + 2, count);
            self.parent = Some(parent);
            self.count = count;
            self.index = usize::MAX;
            self.offset = usize::MAX;
            self.initial_limit = initial_limit;
            self
        }

        #[inline]
        pub fn block_length() -> u16 {
            83
        }

        #[inline]
        pub fn parent(&mut self) -> SbeResult<P> {
            self.parent.take().ok_or(SbeErr::ParentNotSet)
        }

        /// will return Some(current index) when successful otherwise None
        #[inline]
        pub fn advance(&mut self) -> SbeResult<Option<usize>> {
            let index = self.index.wrapping_add(1);
            if index >= self.count as usize {
                return Ok(None);
            }
            if let Some(parent) = self.parent.as_mut() {
                self.offset = parent.get_limit();
                parent.set_limit(self.offset + Self::block_length() as usize);
                self.index = index;
                Ok(Some(index))
            } else {
                Err(SbeErr::ParentNotSet)
            }
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

        /// REQUIRED enum
        #[inline]
        pub fn side(&mut self, value: side::Side) {
            let offset = self.offset + 4;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn order_type(&mut self, value: order_type::OrderType) {
            let offset = self.offset + 5;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn time_in_force(&mut self, value: time_in_force::TimeInForce) {
            let offset = self.offset + 6;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'baseQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 7
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn base_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 7;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'priceTicks'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 15
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn price_ticks(&mut self, value: i64) {
            let offset = self.offset + 15;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        #[inline]
        pub fn flags(&mut self, value: order_flags::OrderFlags) {
            let offset = self.offset + 23;
            self.get_buf_mut().put_u32_at(offset, value.0)
        }

        /// REQUIRED enum
        #[inline]
        pub fn fee_asset(&mut self, value: fee_asset::FeeAsset) {
            let offset = self.offset + 27;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn stp_mode(&mut self, value: stp_mode::STPMode) {
            let offset = self.offset + 28;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'marketClientRefPriceTicks'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 29
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn market_client_ref_price_ticks(&mut self, value: i64) {
            let offset = self.offset + 29;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'marketMaxSlippageTicks'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 37
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn market_max_slippage_ticks(&mut self, value: i64) {
            let offset = self.offset + 37;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'marketMaxSlippageBps'
        /// - min value: -2147483647
        /// - max value: 2147483647
        /// - null value: -2147483648_i32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 45
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn market_max_slippage_bps(&mut self, value: i32) {
            let offset = self.offset + 45;
            self.get_buf_mut().put_i32_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn sizing_kind(&mut self, value: order_sizing_kind::OrderSizingKind) {
            let offset = self.offset + 49;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'quoteQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 50
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn quote_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 50;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'expireAtTsNs'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 58
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn expire_at_ts_ns(&mut self, value: i64) {
            let offset = self.offset + 58;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'displayQtyScaled'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 66
        /// - encodedLength: 8
        /// - version: 44
        #[inline]
        pub fn display_qty_scaled(&mut self, value: i64) {
            let offset = self.offset + 66;
            self.get_buf_mut().put_i64_at(offset, value);
        }

        /// primitive field 'pegReference'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 74
        /// - encodedLength: 1
        /// - version: 47
        #[inline]
        pub fn peg_reference(&mut self, value: u8) {
            let offset = self.offset + 74;
            self.get_buf_mut().put_u8_at(offset, value);
        }

        /// primitive field 'pegOffsetTicks'
        /// - min value: -9223372036854775807
        /// - max value: 9223372036854775807
        /// - null value: -9223372036854775808_i64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 75
        /// - encodedLength: 8
        /// - version: 47
        #[inline]
        pub fn peg_offset_ticks(&mut self, value: i64) {
            let offset = self.offset + 75;
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
    pub struct BatchCreateOrdersDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for BatchCreateOrdersDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for BatchCreateOrdersDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for BatchCreateOrdersDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> BatchCreateOrdersDecoder<'a> {
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

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn timestamp_ms(&self) -> Option<u64> {
            if self.acting_version() < 41 {
                return None;
            }

            let value = self.get_buf().get_u64_at(self.offset + 16);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u32' }
        #[inline]
        pub fn recv_window_ms(&self) -> Option<u32> {
            if self.acting_version() < 41 {
                return None;
            }

            let value = self.get_buf().get_u32_at(self.offset + 24);
            if value == 0x0_u32 { None } else { Some(value) }
        }

        /// GROUP DECODER (id=3, description='1 to 20 orders, answered in this order; items may name different trading pairs')
        #[inline]
        pub fn items_decoder(self) -> ItemsDecoder<Self> {
            ItemsDecoder::default().wrap(self)
        }
    }

    #[derive(Debug, Default)]
    pub struct ItemsDecoder<P> {
        parent: Option<P>,
        block_length: u16,
        count: u16,
        index: usize,
        offset: usize,
    }

    impl<'a, P> ActingVersion for ItemsDecoder<P>
    where
        P: Reader<'a> + ActingVersion + Default,
    {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }
    }

    impl<'a, P> Reader<'a> for ItemsDecoder<P>
    where
        P: Reader<'a> + Default,
    {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            self.parent.as_ref().expect("parent missing").get_buf()
        }
    }

    impl<'a, P> Decoder<'a> for ItemsDecoder<P>
    where
        P: Decoder<'a> + ActingVersion + Default,
    {
        #[inline]
        fn get_limit(&self) -> usize {
            self.parent.as_ref().expect("parent missing").get_limit()
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.parent
                .as_mut()
                .expect("parent missing")
                .set_limit(limit);
        }
    }

    impl<'a, P> ItemsDecoder<P>
    where
        P: Decoder<'a> + ActingVersion + Default,
    {
        pub fn wrap(mut self, mut parent: P) -> Self {
            let initial_offset = parent.get_limit();
            let block_length = parent.get_buf().get_u16_at(initial_offset);
            let count = parent.get_buf().get_u16_at(initial_offset + 2);
            parent.set_limit(initial_offset + 4);
            self.parent = Some(parent);
            self.block_length = block_length;
            self.count = count;
            self.index = usize::MAX;
            self.offset = 0;
            self
        }

        /// group token - Token{signal=BEGIN_GROUP, name='items', referencedName='null', description='1 to 20 orders, answered in this order; items may name different trading pairs', packageName='null', id=3, version=0, deprecated=0, encodedLength=83, offset=28, componentTokenCount=91, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        #[inline]
        pub fn parent(&mut self) -> SbeResult<P> {
            self.parent.take().ok_or(SbeErr::ParentNotSet)
        }

        #[inline]
        pub fn acting_version(&mut self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }

        #[inline]
        pub fn count(&self) -> u16 {
            self.count
        }

        /// will return Some(current index) when successful otherwise None
        pub fn advance(&mut self) -> SbeResult<Option<usize>> {
            let index = self.index.wrapping_add(1);
            if index >= self.count as usize {
                return Ok(None);
            }
            if let Some(parent) = self.parent.as_mut() {
                self.offset = parent.get_limit();
                parent.set_limit(self.offset + self.block_length as usize);
                self.index = index;
                Ok(Some(index))
            } else {
                Err(SbeErr::ParentNotSet)
            }
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn symbol_id(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset)
        }

        /// REQUIRED enum
        #[inline]
        pub fn side(&self) -> side::Side {
            self.get_buf().get_u8_at(self.offset + 4).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn order_type(&self) -> order_type::OrderType {
            self.get_buf().get_u8_at(self.offset + 5).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn time_in_force(&self) -> time_in_force::TimeInForce {
            self.get_buf().get_u8_at(self.offset + 6).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn base_qty_scaled(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 7);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn price_ticks(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 15);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// BIT SET DECODER
        #[inline]
        pub fn flags(&self) -> order_flags::OrderFlags {
            order_flags::OrderFlags::new(self.get_buf().get_u32_at(self.offset + 23))
        }

        /// REQUIRED enum
        #[inline]
        pub fn fee_asset(&self) -> fee_asset::FeeAsset {
            self.get_buf().get_u8_at(self.offset + 27).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn stp_mode(&self) -> stp_mode::STPMode {
            self.get_buf().get_u8_at(self.offset + 28).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn market_client_ref_price_ticks(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 29);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn market_max_slippage_ticks(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 37);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '-2147483648_i32' }
        #[inline]
        pub fn market_max_slippage_bps(&self) -> Option<i32> {
            let value = self.get_buf().get_i32_at(self.offset + 45);
            if value == -2147483648_i32 {
                None
            } else {
                Some(value)
            }
        }

        /// REQUIRED enum
        #[inline]
        pub fn sizing_kind(&self) -> order_sizing_kind::OrderSizingKind {
            self.get_buf().get_u8_at(self.offset + 49).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn quote_qty_scaled(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 50);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '-9223372036854775808_i64' }
        #[inline]
        pub fn expire_at_ts_ns(&self) -> Option<i64> {
            let value = self.get_buf().get_i64_at(self.offset + 58);
            if value == -9223372036854775808_i64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn display_qty_scaled(&self) -> i64 {
            if self.acting_version() < 44 {
                return -9223372036854775808_i64;
            }

            self.get_buf().get_i64_at(self.offset + 66)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn peg_reference(&self) -> u8 {
            if self.acting_version() < 47 {
                return 0xff_u8;
            }

            self.get_buf().get_u8_at(self.offset + 74)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn peg_offset_ticks(&self) -> i64 {
            if self.acting_version() < 47 {
                return -9223372036854775808_i64;
            }

            self.get_buf().get_i64_at(self.offset + 75)
        }

        /// VAR_DATA DECODER - character encoding: 'US-ASCII'
        #[inline]
        pub fn client_order_id_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u32_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 4 + data_length);
            (offset + 4, data_length)
        }

        #[inline]
        pub fn client_order_id_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }
    }
} // end decoder
