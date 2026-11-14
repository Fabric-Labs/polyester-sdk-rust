//! Server build version and status events (`system.v1.SystemService`):
//! public, no credentials, never signed.

use super::changelog::{filter_labels_to_enums, paginate};
use super::unary;
use crate::codecs::decode::{server_version_from_proto, status_events_page_from_proto};
use crate::connect::system::v1::SystemServiceClient;
use crate::errors::Result;
use crate::models::{ServerVersion, StatusEvent, StatusEventsPage};
use crate::proto::system::v1::{
    GetVersionRequest, ListStatusEventsRequest, StatusEventKind, StatusEventState,
};
use crate::transport::Factory;
use futures_util::stream::Stream;

/// Filters for [`crate::Client::list_status_events`] and
/// [`crate::Client::status_events`]. Different filters combine with AND;
/// values within one filter combine with OR.
#[derive(Debug, Clone, Default)]
pub struct ListStatusEventsOptions {
    /// `STATUS_EVENT_STATE_*` labels. Empty means current and upcoming
    /// events (scheduled and in progress), as the server applies it.
    pub states: Vec<String>,
    /// `STATUS_EVENT_KIND_*` labels. Empty means every kind.
    pub kinds: Vec<String>,
    /// Page size; `None` uses the server default of 50 (maximum 200).
    pub limit: Option<u32>,
    /// Continue from a previous page's `next_page_token`. Reuse it only with
    /// the same `states` and `kinds`; another filter is refused with
    /// `invalid_argument`.
    pub page_token: Option<String>,
}

fn client(factory: &Factory) -> SystemServiceClient<crate::transport::SharedTransport> {
    SystemServiceClient::new(factory.transport(), factory.connect_config())
}

/// Read the build the API host serves; see [`crate::Client::get_version`].
pub(crate) async fn get_version(factory: &Factory) -> Result<ServerVersion> {
    // "/system.v1.SystemService/GetVersion"
    let resp = unary::await_public(client(factory).get_version(GetVersionRequest::default()))
        .await?
        .into_owned();
    Ok(server_version_from_proto(&resp))
}

fn list_status_events_request(opts: ListStatusEventsOptions) -> Result<ListStatusEventsRequest> {
    Ok(ListStatusEventsRequest {
        states: filter_labels_to_enums::<StatusEventState>(
            "list_status_events",
            &opts.states,
            "states",
        )?,
        kinds: filter_labels_to_enums::<StatusEventKind>(
            "list_status_events",
            &opts.kinds,
            "kinds",
        )?,
        limit: opts.limit.unwrap_or(0),
        page_token: opts.page_token.unwrap_or_default(),
        ..Default::default()
    })
}

/// One page of status events; see [`crate::Client::list_status_events`].
pub(crate) async fn list_status_events(
    factory: &Factory,
    opts: ListStatusEventsOptions,
) -> Result<StatusEventsPage> {
    let req = list_status_events_request(opts)?;
    // "/system.v1.SystemService/ListStatusEvents"
    let resp = unary::await_public(client(factory).list_status_events(req))
        .await?
        .into_owned();
    Ok(status_events_page_from_proto(&resp))
}

/// Every status event from `opts.page_token` on; see
/// [`crate::Client::status_events`].
pub(crate) fn status_events(
    factory: Factory,
    opts: ListStatusEventsOptions,
) -> impl Stream<Item = Result<StatusEvent>> + Send + 'static {
    paginate(
        "status event pagination",
        opts.page_token.clone(),
        move |page_token| {
            let factory = factory.clone();
            let opts = ListStatusEventsOptions {
                page_token: Some(page_token),
                ..opts.clone()
            };
            async move {
                let page = list_status_events(&factory, opts).await?;
                Ok((page.events, page.next_page_token))
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Error;

    #[test]
    fn list_status_events_request_maps_labels_without_expanding_defaults() {
        let req = list_status_events_request(ListStatusEventsOptions {
            states: vec!["scheduled".into(), "IN_PROGRESS".into()],
            kinds: vec![" maintenance ".into()],
            limit: Some(20),
            page_token: Some("t1".into()),
        })
        .unwrap();
        assert_eq!(
            req.states,
            vec![
                buffa::EnumValue::from(StatusEventState::SCHEDULED),
                buffa::EnumValue::from(StatusEventState::IN_PROGRESS)
            ]
        );
        assert_eq!(
            req.kinds,
            vec![buffa::EnumValue::from(StatusEventKind::MAINTENANCE)]
        );
        assert_eq!((req.limit, req.page_token.as_str()), (20, "t1"));

        // No states: the server applies its default; nothing is expanded.
        let bare = list_status_events_request(ListStatusEventsOptions::default()).unwrap();
        assert!(bare.states.is_empty() && bare.kinds.is_empty());
        assert_eq!((bare.limit, bare.page_token.as_str()), (0, ""));
    }

    #[test]
    fn list_status_events_request_rejects_unknown_labels() {
        for opts in [
            ListStatusEventsOptions {
                states: vec!["upcoming".into()],
                ..Default::default()
            },
            ListStatusEventsOptions {
                states: vec!["state_unspecified".into()],
                ..Default::default()
            },
            ListStatusEventsOptions {
                kinds: vec!["outage".into()],
                ..Default::default()
            },
        ] {
            let err = list_status_events_request(opts).unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{err:?}");
            assert!(err.to_string().contains("list_status_events"), "{err}");
        }
    }
}
