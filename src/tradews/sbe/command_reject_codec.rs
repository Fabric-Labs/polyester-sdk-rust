use super::*;

pub use decoder::CommandRejectDecoder;
pub use encoder::CommandRejectEncoder;

pub use super::SBE_SCHEMA_ID;
pub use super::SBE_SCHEMA_VERSION;
pub use super::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 70;
pub const SBE_TEMPLATE_ID: u16 = 21;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct CommandRejectEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for CommandRejectEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for CommandRejectEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> CommandRejectEncoder<'a> {
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

        /// REQUIRED enum
        #[inline]
        pub fn code(&mut self, value: reject_code::RejectCode) {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u16_at(offset, value as u16)
        }

        /// primitive field 'tsNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 18
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn ts_ns(&mut self, value: u64) {
            let offset = self.offset + 18;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'rateLimitWeight'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 26
        /// - encodedLength: 8
        /// - version: 3
        #[inline]
        pub fn rate_limit_weight(&mut self, value: u64) {
            let offset = self.offset + 26;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'rateLimitRemaining'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 34
        /// - encodedLength: 8
        /// - version: 3
        #[inline]
        pub fn rate_limit_remaining(&mut self, value: u64) {
            let offset = self.offset + 34;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'rateLimitRetryAfterNs'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 42
        /// - encodedLength: 8
        /// - version: 3
        #[inline]
        pub fn rate_limit_retry_after_ns(&mut self, value: u64) {
            let offset = self.offset + 42;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'rateLimitPolicyVersion'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 50
        /// - encodedLength: 8
        /// - version: 3
        #[inline]
        pub fn rate_limit_policy_version(&mut self, value: u64) {
            let offset = self.offset + 50;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn rate_limit_operation(&mut self, value: rate_limit_operation::RateLimitOperation) {
            let offset = self.offset + 58;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn rate_limit_policy_class(
            &mut self,
            value: rate_limit_policy_class::RateLimitPolicyClass,
        ) {
            let offset = self.offset + 59;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn rate_limit_scope(&mut self, value: rate_limit_scope::RateLimitScope) {
            let offset = self.offset + 60;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn rate_limit_refill_model(
            &mut self,
            value: rate_limit_refill_model::RateLimitRefillModel,
        ) {
            let offset = self.offset + 61;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'existingOrderId'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 62
        /// - encodedLength: 8
        /// - version: 32
        #[inline]
        pub fn existing_order_id(&mut self, value: u64) {
            let offset = self.offset + 62;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// VAR_DATA ENCODER - character encoding: 'US-ASCII'
        #[inline]
        pub fn detail(&mut self, value: &[u8]) {
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
    pub struct CommandRejectDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for CommandRejectDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for CommandRejectDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for CommandRejectDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> CommandRejectDecoder<'a> {
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

        /// REQUIRED enum
        #[inline]
        pub fn code(&self) -> reject_code::RejectCode {
            self.get_buf().get_u16_at(self.offset + 16).into()
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn ts_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 18)
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn rate_limit_weight(&self) -> Option<u64> {
            if self.acting_version() < 3 {
                return None;
            }

            let value = self.get_buf().get_u64_at(self.offset + 26);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0xffffffffffffffff_u64' }
        #[inline]
        pub fn rate_limit_remaining(&self) -> Option<u64> {
            if self.acting_version() < 3 {
                return None;
            }

            let value = self.get_buf().get_u64_at(self.offset + 34);
            if value == 0xffffffffffffffff_u64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn rate_limit_retry_after_ns(&self) -> Option<u64> {
            if self.acting_version() < 3 {
                return None;
            }

            let value = self.get_buf().get_u64_at(self.offset + 42);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn rate_limit_policy_version(&self) -> Option<u64> {
            if self.acting_version() < 3 {
                return None;
            }

            let value = self.get_buf().get_u64_at(self.offset + 50);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// REQUIRED enum
        #[inline]
        pub fn rate_limit_operation(&self) -> rate_limit_operation::RateLimitOperation {
            if self.acting_version() < 4 {
                return rate_limit_operation::RateLimitOperation::default();
            }

            self.get_buf().get_u8_at(self.offset + 58).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn rate_limit_policy_class(&self) -> rate_limit_policy_class::RateLimitPolicyClass {
            if self.acting_version() < 4 {
                return rate_limit_policy_class::RateLimitPolicyClass::default();
            }

            self.get_buf().get_u8_at(self.offset + 59).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn rate_limit_scope(&self) -> rate_limit_scope::RateLimitScope {
            if self.acting_version() < 4 {
                return rate_limit_scope::RateLimitScope::default();
            }

            self.get_buf().get_u8_at(self.offset + 60).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn rate_limit_refill_model(&self) -> rate_limit_refill_model::RateLimitRefillModel {
            if self.acting_version() < 4 {
                return rate_limit_refill_model::RateLimitRefillModel::default();
            }

            self.get_buf().get_u8_at(self.offset + 61).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn existing_order_id(&self) -> Option<u64> {
            if self.acting_version() < 32 {
                return None;
            }

            let value = self.get_buf().get_u64_at(self.offset + 62);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// VAR_DATA DECODER - character encoding: 'US-ASCII'
        #[inline]
        pub fn detail_decoder(&mut self) -> (usize, usize) {
            let offset = self.get_limit();
            let data_length = self.get_buf().get_u32_at(offset) as usize;
            self.set_limit(offset + 4 + data_length);
            (offset + 4, data_length)
        }

        #[inline]
        pub fn detail_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }
    }
} // end decoder
