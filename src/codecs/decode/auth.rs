//! Auth response decoders.

use crate::codecs::scalars::format_uint64_id;
use crate::models::MeResult;
use crate::proto::auth::v1::MeResponse;

pub fn me_from_proto(msg: &MeResponse) -> MeResult {
    MeResult {
        account_id: format_uint64_id(msg.account_id),
        api_key_id: msg.api_key_id.map(format_uint64_id),
        username: if msg.username.is_empty() {
            None
        } else {
            Some(msg.username.clone())
        },
        root_smart_account_address: if msg.root_smart_account_address.is_empty() {
            None
        } else {
            Some(msg.root_smart_account_address.clone())
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::scalars::format_uint64_id;
    use crate::proto::auth::v1::MeResponse;

    #[test]
    fn me_from_proto_formats_ids() {
        let msg = MeResponse {
            account_id: 42,
            api_key_id: Some(99),
            username: "alice".into(),
            root_smart_account_address: "0xabc".into(),
            ..Default::default()
        };
        let me = me_from_proto(&msg);
        assert_eq!(me.account_id, format_uint64_id(42));
        assert_eq!(
            me.api_key_id.as_deref(),
            Some(format_uint64_id(99).as_str())
        );
        assert_eq!(me.username.as_deref(), Some("alice"));
        assert_eq!(me.root_smart_account_address.as_deref(), Some("0xabc"));
    }
}
