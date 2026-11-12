//! Public, read-only product changelog (`changelog.v1.ChangelogService`).
//!
//! No credentials are needed: every call works on a client without an API
//! key. The API host decides which environment's changelog is returned.

use super::{ServiceContext, unary};
use crate::codecs::decode::{
    changelog_changes_page_from_proto, changelog_post_from_proto, changelog_posts_page_from_proto,
};
use crate::connect::changelog::v1::ChangelogServiceClient;
use crate::errors::{Error, Result};
use crate::models::{
    CHANGELOG_CATEGORY_BREAKING_CHANGE, ChangelogChangeEntry, ChangelogChangesPage, ChangelogPost,
    ChangelogPostSummary, ChangelogPostsPage,
};
use crate::proto::changelog::v1::{
    Audience, ChangeCategory, GetPostRequest, ListChangesRequest, ListPostsRequest, SortDirection,
    Surface,
};
use crate::transport::SharedTransport;
use buffa::Enumeration;
use buffa_types::google::protobuf::Timestamp;
use futures_util::stream::{self, Stream, TryStreamExt};
use std::collections::HashSet;
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};

/// Page size of the `*_since` helpers: the server maximum, so a long history
/// costs as few rate-limited requests as possible.
const SINCE_PAGE_SIZE: u32 = 500;

/// Options for [`ChangelogService::list_posts`] and [`ChangelogService::posts`].
#[derive(Debug, Clone, Default)]
pub struct ListChangelogPostsOptions {
    /// Page size; `None` uses the server default of 20 (maximum 100).
    pub limit: Option<u32>,
    /// Continue from a previous page's `next_page_token`.
    pub page_token: Option<String>,
}

/// Filters for [`ChangelogService::list_changes`] and
/// [`ChangelogService::changes`]. Values within one filter combine with OR;
/// different filters combine with AND. Empty filters send nothing.
#[derive(Debug, Clone, Default)]
pub struct ListChangelogChangesOptions {
    /// `CHANGELOG_CATEGORY_*` labels.
    pub categories: Vec<String>,
    /// Keep only changes whose `action_required` matches.
    pub action_required: Option<bool>,
    /// `CHANGELOG_SURFACE_*` labels, at most 11.
    pub surfaces: Vec<String>,
    /// `CHANGELOG_AUDIENCE_*` labels. Changes addressed to all users always
    /// match.
    pub audiences: Vec<String>,
    /// Inclusive lower bound on post publication time.
    pub start_time: Option<SystemTime>,
    /// Exclusive upper bound on post publication time; must be after
    /// `start_time` when both are set.
    pub end_time: Option<SystemTime>,
    /// Restrict the result to one post.
    pub post_slug: Option<String>,
    /// `"desc"` (newest post first, the default) or `"asc"`. Changes keep
    /// publication order inside each post.
    pub sort: Option<String>,
    /// Page size; `None` uses the server default of 100 (maximum 500).
    pub limit: Option<u32>,
    /// Continue from a previous page's `next_page_token`. Reuse it only with
    /// the same filters, time bounds and sort.
    pub page_token: Option<String>,
}

#[derive(Clone)]
pub struct ChangelogService {
    ctx: ServiceContext,
}

impl ChangelogService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> ChangelogServiceClient<SharedTransport> {
        ChangelogServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    /// One page of post summaries, newest first.
    pub async fn list_posts(&self, opts: ListChangelogPostsOptions) -> Result<ChangelogPostsPage> {
        let req = ListPostsRequest {
            limit: opts.limit.unwrap_or(0),
            page_token: opts.page_token.unwrap_or_default(),
            ..Default::default()
        };
        // "/changelog.v1.ChangelogService/ListPosts"
        let resp = unary::await_public(self.client().list_posts(req))
            .await?
            .into_owned();
        Ok(changelog_posts_page_from_proto(&resp))
    }

    /// One full post by slug (`YYYY-MM-DD`, or `YYYY-MM-DD-2` for a later post
    /// that day). An unknown slug is an [`Error::Api`] with code `NotFound`.
    pub async fn get_post(&self, slug: &str) -> Result<ChangelogPost> {
        let req = GetPostRequest {
            slug: slug.to_owned(),
            ..Default::default()
        };
        // "/changelog.v1.ChangelogService/GetPost"
        let resp = unary::await_public(self.client().get_post(req))
            .await?
            .into_owned();
        changelog_post_from_proto(&resp)
    }

    /// One page of individual changes across posts.
    pub async fn list_changes(
        &self,
        opts: ListChangelogChangesOptions,
    ) -> Result<ChangelogChangesPage> {
        let req = list_changes_request(opts)?;
        // "/changelog.v1.ChangelogService/ListChanges"
        let resp = unary::await_public(self.client().list_changes(req))
            .await?
            .into_owned();
        Ok(changelog_changes_page_from_proto(&resp))
    }

    /// Every post summary from `opts.page_token` on, newest first, fetching
    /// pages as the stream is polled.
    pub fn posts(
        &self,
        opts: ListChangelogPostsOptions,
    ) -> impl Stream<Item = Result<ChangelogPostSummary>> + Send + 'static {
        let svc = self.clone();
        paginate(
            "changelog pagination",
            opts.page_token.clone(),
            move |page_token| {
                let svc = svc.clone();
                let opts = ListChangelogPostsOptions {
                    limit: opts.limit,
                    page_token: Some(page_token),
                };
                async move {
                    let page = svc.list_posts(opts).await?;
                    Ok((page.posts, page.next_page_token))
                }
            },
        )
    }

    /// Every change matching `opts` from `opts.page_token` on, fetching pages
    /// as the stream is polled.
    ///
    /// When a post is republished during the walk, the next page restarts at
    /// that post's first change, so changes already yielded can come again.
    /// Each change `id` is yielded once per stream, as first received; read
    /// the post with [`Self::get_post`] for its current wording.
    pub fn changes(
        &self,
        opts: ListChangelogChangesOptions,
    ) -> impl Stream<Item = Result<ChangelogChangeEntry>> + Send + 'static {
        let mut seen = HashSet::new();
        self.change_walk(opts)
            .try_filter(move |entry| std::future::ready(seen.insert(entry.change.id.clone())))
    }

    /// Every `list_changes` entry of one walk, repeats included.
    fn change_walk(
        &self,
        opts: ListChangelogChangesOptions,
    ) -> impl Stream<Item = Result<ChangelogChangeEntry>> + Send + 'static {
        let svc = self.clone();
        paginate(
            "changelog pagination",
            opts.page_token.clone(),
            move |page_token| {
                let svc = svc.clone();
                let opts = ListChangelogChangesOptions {
                    page_token: Some(page_token),
                    ..opts.clone()
                };
                async move {
                    let page = svc.list_changes(opts).await?;
                    Ok((page.changes, page.next_page_token))
                }
            },
        )
    }

    /// All breaking changes published at or after `since`, newest post first.
    ///
    /// It filters on the post's first publication time, which a
    /// republication does not move, so an edit to an older post is not
    /// reported: compare the `revision` of [`Self::posts`] summaries to find
    /// edited posts. A change met again because its post was republished
    /// during the call is returned once, at its first position, with the
    /// wording last received.
    pub async fn breaking_changes_since(
        &self,
        since: SystemTime,
    ) -> Result<Vec<ChangelogChangeEntry>> {
        collect_changes(self.change_walk(ListChangelogChangesOptions {
            categories: vec![CHANGELOG_CATEGORY_BREAKING_CHANGE.to_owned()],
            start_time: Some(since),
            limit: Some(SINCE_PAGE_SIZE),
            ..Default::default()
        }))
        .await
    }

    /// All changes that require action, published at or after `since`, newest
    /// post first. Republished posts are handled as in
    /// [`Self::breaking_changes_since`].
    pub async fn action_required_since(
        &self,
        since: SystemTime,
    ) -> Result<Vec<ChangelogChangeEntry>> {
        collect_changes(self.change_walk(ListChangelogChangesOptions {
            action_required: Some(true),
            start_time: Some(since),
            limit: Some(SINCE_PAGE_SIZE),
            ..Default::default()
        }))
        .await
    }
}

/// Collect one walk, keeping one entry per change `id`: a repeat replaces the
/// earlier entry in place, so the latest revision's wording wins.
async fn collect_changes(
    walk: impl Stream<Item = Result<ChangelogChangeEntry>>,
) -> Result<Vec<ChangelogChangeEntry>> {
    let mut index = std::collections::HashMap::new();
    walk.try_fold(Vec::new(), |mut changes, entry| {
        match index.get(&entry.change.id) {
            Some(&position) => changes[position] = entry,
            None => {
                index.insert(entry.change.id.clone(), changes.len());
                changes.push(entry);
            }
        }
        std::future::ready(Ok(changes))
    })
    .await
}

/// Flatten token-paged results into one item stream. A next page token this
/// stream already requested, including the caller's starting token, would
/// cycle forever, so it fails the stream with a response-contract error.
pub(crate) fn paginate<T, F, Fut>(
    context: &'static str,
    first_token: Option<String>,
    fetch: F,
) -> impl Stream<Item = Result<T>> + Send + 'static
where
    T: Send + 'static,
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(Vec<T>, String)>> + Send + 'static,
{
    let fetch = std::sync::Arc::new(fetch);
    let start = (Some(first_token.unwrap_or_default()), HashSet::new());
    stream::try_unfold(start, move |(token, mut requested)| {
        let fetch = fetch.clone();
        async move {
            let Some(token) = token else {
                return Ok(None);
            };
            if !token.is_empty() {
                requested.insert(token.clone());
            }
            let (items, next) = fetch(token).await?;
            if requested.contains(&next) {
                return Err(Error::response_contract(
                    context,
                    "next_page_token repeats an earlier page_token",
                ));
            }
            let next = (!next.is_empty()).then_some(next);
            Ok(Some((
                stream::iter(items.into_iter().map(Ok)),
                (next, requested),
            )))
        }
    })
    .try_flatten()
}

/// Most `surfaces` a `list_changes` filter takes, one per surface.
const MAX_SURFACE_FILTERS: usize = 11;

fn list_changes_request(opts: ListChangelogChangesOptions) -> Result<ListChangesRequest> {
    if opts.surfaces.len() > MAX_SURFACE_FILTERS {
        return Err(Error::validation(format!(
            "list_changes surfaces: at most {MAX_SURFACE_FILTERS} values, got {}",
            opts.surfaces.len()
        )));
    }
    Ok(ListChangesRequest {
        categories: labels_to_enums::<ChangeCategory>(&opts.categories, "categories")?,
        action_required: opts.action_required,
        surfaces: labels_to_enums::<Surface>(&opts.surfaces, "surfaces")?,
        audiences: labels_to_enums::<Audience>(&opts.audiences, "audiences")?,
        start_time: timestamp_field(opts.start_time),
        end_time: timestamp_field(opts.end_time),
        post_slug: opts.post_slug.unwrap_or_default(),
        sort: sort_direction(opts.sort.as_deref())?.into(),
        limit: opts.limit.unwrap_or(0),
        page_token: opts.page_token.unwrap_or_default(),
        ..Default::default()
    })
}

/// Map lowercase labels (`breaking_change`, `api`, ...) to known enum values.
/// The unspecified value and unknown labels are rejected.
fn labels_to_enums<T: Enumeration>(
    labels: &[String],
    field: &str,
) -> Result<Vec<buffa::EnumValue<T>>> {
    labels
        .iter()
        .map(|label| {
            T::from_proto_name(&label.trim().to_ascii_uppercase())
                .filter(|value| value.to_i32() != 0)
                .map(buffa::EnumValue::from)
                .ok_or_else(|| {
                    Error::validation(format!("list_changes {field}: unknown label {label:?}"))
                })
        })
        .collect()
}

fn sort_direction(sort: Option<&str>) -> Result<SortDirection> {
    match sort.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") => Ok(SortDirection::SORT_DIRECTION_UNSPECIFIED),
        Some("desc") => Ok(SortDirection::SORT_DESC),
        Some("asc") => Ok(SortDirection::SORT_ASC),
        Some(other) => Err(Error::validation(format!(
            "list_changes sort must be \"asc\" or \"desc\", got {other:?}"
        ))),
    }
}

/// An exact protobuf `Timestamp`, as protobuf's own conversions write one:
/// `seconds` rounds toward negative infinity and `nanos` is never negative.
fn timestamp_field(time: Option<SystemTime>) -> buffa::MessageField<Timestamp> {
    let Some(time) = time else {
        return buffa::MessageField::none();
    };
    let (seconds, nanos) = match time.duration_since(UNIX_EPOCH) {
        Ok(after) => (after.as_secs() as i64, after.subsec_nanos() as i32),
        Err(before) => {
            let before = before.duration();
            let seconds = -(before.as_secs() as i64);
            match before.subsec_nanos() {
                0 => (seconds, 0),
                nanos => (seconds - 1, (1_000_000_000 - nanos) as i32),
            }
        }
    };
    Timestamp {
        seconds,
        nanos,
        ..Default::default()
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn list_changes_request_maps_labels_and_times() {
        let req = list_changes_request(ListChangelogChangesOptions {
            categories: vec!["breaking_change".into(), "BUG_FIX".into()],
            surfaces: vec!["api".into()],
            audiences: vec!["market_makers".into()],
            action_required: Some(true),
            start_time: Some(UNIX_EPOCH + Duration::new(1_767_225_600, 5)),
            sort: Some("asc".into()),
            limit: Some(50),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            req.categories,
            vec![
                buffa::EnumValue::from(ChangeCategory::BREAKING_CHANGE),
                buffa::EnumValue::from(ChangeCategory::BUG_FIX)
            ]
        );
        assert_eq!(req.surfaces, vec![buffa::EnumValue::from(Surface::API)]);
        assert_eq!(
            req.audiences,
            vec![buffa::EnumValue::from(Audience::MARKET_MAKERS)]
        );
        assert_eq!(req.action_required, Some(true));
        let start = req.start_time.as_option().unwrap();
        assert_eq!((start.seconds, start.nanos), (1_767_225_600, 5));
        assert!(req.end_time.as_option().is_none());
        assert_eq!(req.sort, SortDirection::SORT_ASC);
        assert_eq!(req.limit, 50);
    }

    /// Time bounds are sent as exact protobuf timestamps across its range:
    /// `seconds` rounds toward negative infinity and `nanos` is never
    /// negative, as protobuf's own conversions do.
    #[test]
    fn list_changes_request_sends_exact_time_bounds() {
        let cases = [
            (
                "half a second before the epoch",
                UNIX_EPOCH - Duration::from_millis(500),
                (-1, 500_000_000),
            ),
            (
                "one nanosecond before the epoch",
                UNIX_EPOCH - Duration::from_nanos(1),
                (-1, 999_999_999),
            ),
            (
                "the first second of year 1",
                UNIX_EPOCH - Duration::from_secs(62_135_596_800),
                (-62_135_596_800, 0),
            ),
            ("the epoch", UNIX_EPOCH, (0, 0)),
            (
                "the last microsecond of year 9999",
                UNIX_EPOCH + Duration::new(253_402_300_799, 999_999_000),
                (253_402_300_799, 999_999_000),
            ),
        ];
        for (name, time, want) in cases {
            let req = list_changes_request(ListChangelogChangesOptions {
                start_time: Some(time),
                end_time: Some(time),
                ..Default::default()
            })
            .unwrap_or_else(|err| panic!("{name}: {err}"));
            for ts in [req.start_time.as_option(), req.end_time.as_option()] {
                let ts = ts.unwrap();
                assert_eq!((ts.seconds, ts.nanos), want, "{name}");
            }
        }
    }

    #[test]
    fn list_changes_request_filters_by_the_new_product_areas() {
        let req = list_changes_request(ListChangelogChangesOptions {
            surfaces: vec!["market_data".into(), "QUICKSWAP".into(), "rewards".into()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            req.surfaces,
            vec![
                buffa::EnumValue::from(Surface::MARKET_DATA),
                buffa::EnumValue::from(Surface::QUICKSWAP),
                buffa::EnumValue::from(Surface::REWARDS)
            ]
        );
        let err = list_changes_request(ListChangelogChangesOptions {
            surfaces: vec!["api".into(); 12],
            ..Default::default()
        })
        .unwrap_err();
        assert!(matches!(err, Error::Validation(_)), "{err:?}");
    }

    /// The assistant is the eleventh product area: a filter names it, and
    /// one surface each fits a single filter.
    #[test]
    fn list_changes_request_filters_by_the_assistant() {
        use buffa::Enumeration;

        let req = list_changes_request(ListChangelogChangesOptions {
            surfaces: vec![crate::models::CHANGELOG_SURFACE_ASSISTANT.into()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            req.surfaces,
            vec![buffa::EnumValue::from(Surface::ASSISTANT)]
        );
        assert_eq!(Surface::ASSISTANT.to_i32(), 11);

        let every: Vec<String> = Surface::values()
            .iter()
            .filter(|surface| **surface != Surface::SURFACE_UNSPECIFIED)
            .map(|surface| surface.proto_name().to_ascii_lowercase())
            .collect();
        assert_eq!(every.len(), 11);
        let req = list_changes_request(ListChangelogChangesOptions {
            surfaces: every,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(req.surfaces.len(), 11);
    }

    #[test]
    fn list_changes_request_rejects_unknown_labels() {
        for opts in [
            ListChangelogChangesOptions {
                categories: vec!["security".into()],
                ..Default::default()
            },
            ListChangelogChangesOptions {
                surfaces: vec!["surface_unspecified".into()],
                ..Default::default()
            },
            ListChangelogChangesOptions {
                sort: Some("newest".into()),
                ..Default::default()
            },
        ] {
            let err = list_changes_request(opts).unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{err:?}");
        }
    }
}
