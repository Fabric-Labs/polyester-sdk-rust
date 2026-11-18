use super::*;

pub use decoder::BatchReplaceAckDecoder;
pub use encoder::BatchReplaceAckEncoder;

pub use super::SBE_SCHEMA_ID;
pub use super::SBE_SCHEMA_VERSION;
pub use super::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 73;
pub const SBE_TEMPLATE_ID: u16 = 27;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct BatchReplaceAckEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for BatchReplaceAckEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for BatchReplaceAckEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> BatchReplaceAckEncoder<'a> {
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

        /// primitive field 'tsNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 16
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn ts_ns(&mut self, value: u64) {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'batchRequestId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 24
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn batch_request_id(&mut self, value: u64) {
            let offset = self.offset + 24;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// REQUIRED enum
        #[inline]
        pub fn status(&mut self, value: batch_status::BatchStatus) {
            let offset = self.offset + 32;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'acceptedCount'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 33
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn accepted_count(&mut self, value: u32) {
            let offset = self.offset + 33;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// primitive field 'rejectedCount'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 37
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn rejected_count(&mut self, value: u32) {
            let offset = self.offset + 37;
            self.get_buf_mut().put_u32_at(offset, value);
        }

        /// primitive field 'rateLimitWeight'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 41
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn rate_limit_weight(&mut self, value: u64) {
            let offset = self.offset + 41;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'rateLimitRemaining'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 49
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn rate_limit_remaining(&mut self, value: u64) {
            let offset = self.offset + 49;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'rateLimitRetryAfterNs'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 57
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn rate_limit_retry_after_ns(&mut self, value: u64) {
            let offset = self.offset + 57;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'rateLimitPolicyVersion'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 65
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn rate_limit_policy_version(&mut self, value: u64) {
            let offset = self.offset + 65;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// GROUP ENCODER (id=12, description='One result per item, in request order')
        #[inline]
        pub fn results_encoder(
            self,
            count: u16,
            results_encoder: ResultsEncoder<Self>,
        ) -> ResultsEncoder<Self> {
            results_encoder.wrap(self, count)
        }
    }

    #[derive(Debug, Default)]
    pub struct ResultsEncoder<P> {
        parent: Option<P>,
        count: u16,
        index: usize,
        offset: usize,
        initial_limit: usize,
    }

    impl<'a, P> Writer<'a> for ResultsEncoder<P>
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

    impl<'a, P> Encoder<'a> for ResultsEncoder<P>
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

    impl<'a, P> ResultsEncoder<P>
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
            20
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

        /// REQUIRED enum
        #[inline]
        pub fn status(&mut self, value: batch_item_status::BatchItemStatus) {
            let offset = self.offset;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// REQUIRED enum
        #[inline]
        pub fn code(&mut self, value: reject_code::RejectCode) {
            let offset = self.offset + 1;
            self.get_buf_mut().put_u16_at(offset, value as u16)
        }

        /// REQUIRED enum
        #[inline]
        pub fn action(&mut self, value: replace_action::ReplaceAction) {
            let offset = self.offset + 3;
            self.get_buf_mut().put_u8_at(offset, value as u8)
        }

        /// primitive field 'oldOrderId'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 4
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn old_order_id(&mut self, value: u64) {
            let offset = self.offset + 4;
            self.get_buf_mut().put_u64_at(offset, value);
        }

        /// primitive field 'replacementOrderId'
        /// - min value: 1
        /// - max value: -2
        /// - null value: 0x0_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 12
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn replacement_order_id(&mut self, value: u64) {
            let offset = self.offset + 12;
            self.get_buf_mut().put_u64_at(offset, value);
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct BatchReplaceAckDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for BatchReplaceAckDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for BatchReplaceAckDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for BatchReplaceAckDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> BatchReplaceAckDecoder<'a> {
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
        pub fn ts_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 16)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn batch_request_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 24)
        }

        /// REQUIRED enum
        #[inline]
        pub fn status(&self) -> batch_status::BatchStatus {
            self.get_buf().get_u8_at(self.offset + 32).into()
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn accepted_count(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 33)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn rejected_count(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 37)
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn rate_limit_weight(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 41);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0xffffffffffffffff_u64' }
        #[inline]
        pub fn rate_limit_remaining(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 49);
            if value == 0xffffffffffffffff_u64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn rate_limit_retry_after_ns(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 57);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn rate_limit_policy_version(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 65);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// GROUP DECODER (id=12, description='One result per item, in request order')
        #[inline]
        pub fn results_decoder(self) -> ResultsDecoder<Self> {
            ResultsDecoder::default().wrap(self)
        }
    }

    #[derive(Debug, Default)]
    pub struct ResultsDecoder<P> {
        parent: Option<P>,
        block_length: u16,
        count: u16,
        index: usize,
        offset: usize,
    }

    impl<'a, P> ActingVersion for ResultsDecoder<P>
    where
        P: Reader<'a> + ActingVersion + Default,
    {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }
    }

    impl<'a, P> Reader<'a> for ResultsDecoder<P>
    where
        P: Reader<'a> + Default,
    {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            self.parent.as_ref().expect("parent missing").get_buf()
        }
    }

    impl<'a, P> Decoder<'a> for ResultsDecoder<P>
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

    impl<'a, P> ResultsDecoder<P>
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

        /// group token - Token{signal=BEGIN_GROUP, name='results', referencedName='null', description='One result per item, in request order', packageName='null', id=12, version=0, deprecated=0, encodedLength=20, offset=73, componentTokenCount=226, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
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

        /// REQUIRED enum
        #[inline]
        pub fn status(&self) -> batch_item_status::BatchItemStatus {
            self.get_buf().get_u8_at(self.offset).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn code(&self) -> reject_code::RejectCode {
            self.get_buf().get_u16_at(self.offset + 1).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn action(&self) -> replace_action::ReplaceAction {
            self.get_buf().get_u8_at(self.offset + 3).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn old_order_id(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 4);
            if value == 0x0_u64 { None } else { Some(value) }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0x0_u64' }
        #[inline]
        pub fn replacement_order_id(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 12);
            if value == 0x0_u64 { None } else { Some(value) }
        }
    }
} // end decoder
