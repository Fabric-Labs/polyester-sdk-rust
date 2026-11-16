use super::*;

pub use decoder::CancelAllAfterAckDecoder;
pub use encoder::CancelAllAfterAckEncoder;

pub use super::SBE_SCHEMA_ID;
pub use super::SBE_SCHEMA_VERSION;
pub use super::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 42;
pub const SBE_TEMPLATE_ID: u16 = 30;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct CancelAllAfterAckEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for CancelAllAfterAckEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for CancelAllAfterAckEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> CancelAllAfterAckEncoder<'a> {
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
        pub fn status(&mut self, value: cancel_all_after_status::CancelAllAfterStatus) {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'timeoutSec'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 17
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn timeout_sec(&mut self, value: u32) {
            let offset = self.offset + 17;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// primitive field 'expiresAtTsNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 21
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn expires_at_ts_ns(&mut self, value: u64) {
            let offset = self.offset + 21;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'symbolId'
        /// - min value: 1
        /// - max value: 4294967294
        /// - null value: 0x0_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 29
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn symbol_id(&mut self, value: u32) {
            let offset = self.offset + 29;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn side(&mut self, value: side::Side) {
            let offset = self.offset + 33;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'tsNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 34
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn ts_ns(&mut self, value: u64) {
            let offset = self.offset + 34;
            self.get_buf_mut().put_u64_at(offset, value);
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct CancelAllAfterAckDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for CancelAllAfterAckDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for CancelAllAfterAckDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for CancelAllAfterAckDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> CancelAllAfterAckDecoder<'a> {
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
        pub fn status(&self) -> cancel_all_after_status::CancelAllAfterStatus {
            self.get_buf().get_u8_at(self.offset + 16).into()
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn timeout_sec(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 17)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn expires_at_ts_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 21)
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u32' }
        #[inline]
        pub fn symbol_id(&self) -> Option<u32> {
            let value = self.get_buf().get_u32_at(self.offset + 29);
            if value == 0x0_u32 { None } else { Some(value) }
        }

        /// REQUIRED enum
        #[inline]
        pub fn side(&self) -> side::Side {
            self.get_buf().get_u8_at(self.offset + 33).into()
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn ts_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 34)
        }
    }
} // end decoder
