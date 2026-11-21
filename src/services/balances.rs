use super::ServiceContext;
use super::limits::{LIST_TRANSFERS_LIMIT_MAX, check_limit, check_transfer_code};
use super::scope;
use super::unary;
use crate::codecs::decode::{
    balance_history_from_proto, balances_list_from_proto, equity_history_from_proto,
    transfers_list_from_proto,
};
use crate::connect::ledger::read::v1::LedgerReadServiceClient;
use crate::errors::Result;
use crate::models::{BalanceHistory, BalanceUpdate, BalancesList, EquityHistory, TransfersList};
use crate::proto::ledger::read::v1::{
    GetBalanceHistoryRequest, GetBalancesRequest, GetEquityHistorySeriesRequest,
};

#[derive(Clone)]
pub struct BalancesService {
    ctx: ServiceContext,
}

impl BalancesService {
    pub fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    fn client(&self) -> LedgerReadServiceClient<crate::transport::SharedTransport> {
        LedgerReadServiceClient::new(
            self.ctx.factory.transport(),
            self.ctx.factory.connect_config(),
        )
    }

    /// Read the account's balances (`GetBalances`) and the position their
    /// Trading values are at: [`BalancesList::trading_epoch`],
    /// [`BalancesList::trading_sequence`] and
    /// [`BalancesList::trading_applied_through_ts_ns`].
    ///
    /// The Trading values of every asset are taken at that one position, and
    /// the answer reflects every balance update the stream published before
    /// the call began. When the server cannot answer that yet it refuses the
    /// call as `unavailable` (HTTP 503); retry it with a short backoff. See
    /// [`Self::subscribe`] for keeping the balances exact with the balance
    /// stream. An API key needs `READ_BALANCES` on its policy.
    pub async fn list(&self, mut req: GetBalancesRequest) -> Result<BalancesList> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/GetBalances",
            req,
            |req, opts| client.get_balances_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(balances_list_from_proto(&resp))
    }

    pub async fn get_balance_history(
        &self,
        mut req: GetBalanceHistoryRequest,
    ) -> Result<BalanceHistory> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/GetBalanceHistory",
            req,
            |req, opts| client.get_balance_history_with_options(req, opts),
        )
        .await?
        .into_owned();
        balance_history_from_proto(&resp)
    }

    pub async fn get_equity_history(
        &self,
        mut req: GetEquityHistorySeriesRequest,
    ) -> Result<EquityHistory> {
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/GetEquityHistorySeries",
            req,
            |req, opts| client.get_equity_history_series_with_options(req, opts),
        )
        .await?
        .into_owned();
        Ok(equity_history_from_proto(&resp))
    }

    /// Read a page of ledger transfers (`ListTransfers`).
    ///
    /// `reversed` lists newest first; `false` lists oldest first.
    /// `transfer_code` lists only transfers with that reason: a
    /// `ledger.v1.TransferCode`, the number a row carries in
    /// [`crate::models::LedgerTransfer::transfer_type`] (for example 1000 for
    /// `DEPOSIT`); unset (0) lists every reason. A value this SDK does not
    /// name is sent as given, but a negative one is refused with
    /// [`crate::Error::Validation`] before sending. A page token is bound to
    /// the filters it was returned for: continue with the same subaccount,
    /// `transfer_code`, time bounds and `reversed`. An empty
    /// `next_page_token` ends paging.
    pub async fn list_transfers(
        &self,
        mut req: crate::proto::ledger::read::v1::ListTransfersRequest,
    ) -> Result<TransfersList> {
        check_limit(
            "balances.list_transfers",
            req.limit,
            LIST_TRANSFERS_LIMIT_MAX,
        )?;
        check_transfer_code("balances.list_transfers", req.transfer_code)?;
        req.subaccount_id = scope::optional_subaccount(&self.ctx, req.subaccount_id)?;
        let client = self.client();
        let resp = unary::await_auth(
            &self.ctx.factory,
            "/ledger.read.v1.LedgerReadService/ListTransfers",
            req,
            |req, opts| client.list_transfers_with_options(req, opts),
        )
        .await?
        .into_owned();
        transfers_list_from_proto(&resp)
    }

    /// Subscribe to the account's balance stream,
    /// `private:ledger:balances:{account}:proto`.
    ///
    /// Each item is one publication, a `ledger.read.v1.BalanceUpdate`
    /// decoded as it arrived: the SDK does not buffer, reorder, merge or
    /// reconcile updates. A TRADING update is one numbered step of the
    /// account's Trading balances; a FUNDING update carries one asset's
    /// Funding balance (see [`BalanceUpdate`]). `account_id` defaults to the
    /// client's `default_account_id`.
    ///
    /// # Keeping balances exact
    ///
    /// Combine the stream with [`Self::list`] as follows:
    ///
    /// 1. Subscribe first, and buffer the updates as they arrive.
    /// 2. Call [`Self::list`] and take its balances. Remember its position
    ///    `(E, Q)`: [`BalancesList::trading_epoch`] and
    ///    [`BalancesList::trading_sequence`]. Retry an `unavailable` answer
    ///    with a backoff.
    /// 3. Drop every TRADING update of epoch `E` with a sequence at or below
    ///    `Q`, buffered or later (a reconnect can repeat one). Apply the
    ///    sequences `Q + 1`, `Q + 2`, and so on, in order.
    /// 4. Keep a set of dropped epochs: every epoch you held before `E`, and
    ///    every epoch other than `E` seen on an update received before the
    ///    [`Self::list`] call that answered `E` began. The server never
    ///    answers a dropped epoch again, so drop a TRADING update of a
    ///    dropped epoch without reloading: after an ownership move, late
    ///    updates of the earlier epoch can still arrive. Reload from step 2
    ///    when:
    ///    - a TRADING update's epoch is neither `E` nor dropped, including
    ///      one buffered while the call ran: the numbering restarted;
    ///    - its sequence skips a number: an update was missed;
    ///    - after an update of `E` with `complete` false, `E`'s next sequence
    ///      does not arrive within 2 seconds (FUNDING updates, repeats and
    ///      updates of dropped epochs do not stop that timer);
    ///    - the subscription reconnected, as
    ///      [`crate::realtime::TypedSubscription::take_resubscribed`] (or a
    ///      growing [`crate::realtime::TypedSubscription::resubscribes`])
    ///      reports: publications missed meanwhile are not recovered. Check it
    ///      on a timer of its own, not only when an update arrives: an idle
    ///      account may publish nothing after the reconnect.
    /// 5. Apply a FUNDING update to an asset only when its
    ///    [`crate::models::BalanceUpdateAsset::funding_revision`] is higher
    ///    than the one you hold for it.
    ///
    /// Take only the Trading fields from a TRADING update and only the
    /// Funding fields from a FUNDING update; keep the other fields from your
    /// earlier state. Tell the two kinds apart by
    /// [`BalanceUpdate::account`], never by a zero epoch.
    ///
    /// Show Trading values only after an update with
    /// [`BalanceUpdate::complete`] set: until then the same step continues in
    /// the next update. The updates of a new epoch up to its first complete
    /// one carry every asset the account holds in Trading.
    ///
    /// [`Self::list`] reflects every update published before the call began,
    /// so a reload covers anything the stream missed, even the last update of
    /// an idle account: no heartbeat is needed. After an ownership move, a
    /// new epoch's Trading values can briefly be older than the previous
    /// epoch's last ones, and their transfer legs can arrive a second time in
    /// the new epoch: (`transfer_id`, `is_debit`) identifies a leg. Count a
    /// transfer as applied only by a receipt in the current epoch: its leg,
    /// or a `trading_applied_through_ts_ns` at or past its
    /// [`crate::models::LedgerTransfer::ts_ns`].
    ///
    /// Read the stream and [`Self::list`] for the same account. An unset
    /// `account_id` here takes the client's `default_account_id`, while
    /// [`Self::list`] with an unset `subaccount_id` takes the configured
    /// default sub-account, or the main account when none is configured. So
    /// name the account in both: for a sub-account, `subscribe` with its
    /// public ID and `list` with its `subaccount_id`; for the main account,
    /// `subscribe` with the main account's public ID and `list` with no
    /// `subaccount_id` on a client without a default sub-account.
    ///
    /// Requires the `realtime` feature. An API key needs `READ_BALANCES` on
    /// its policy.
    ///
    /// ```rust,no_run
    /// # async fn example(client: polyester::Client) -> polyester::Result<()> {
    /// use polyester::models::BalanceUpdateAccount;
    /// use polyester::proto::ledger::read::v1::GetBalancesRequest;
    /// use std::time::Duration;
    ///
    /// // A sub-account: the stream and the snapshot name the same account.
    /// let (sub_public_id, sub_id) = ("<sub-account public ID>", 42_u64);
    /// // 1. Subscribe before reading the snapshot.
    /// let mut stream = client.balances.subscribe(Some(sub_public_id)).await?;
    /// // 2. The snapshot and its position (E, Q).
    /// let snapshot = client
    ///     .balances
    ///     .list(GetBalancesRequest {
    ///         subaccount_id: Some(sub_id),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// let (epoch, mut sequence) = (snapshot.trading_epoch, snapshot.trading_sequence);
    /// // A reconnect is checked on its own timer, not only with an update.
    /// let mut reconnect_check = tokio::time::interval(Duration::from_millis(500));
    /// loop {
    ///     tokio::select! {
    ///         _ = reconnect_check.tick() => {
    ///             if stream.take_resubscribed() {
    ///                 break; // publications may have been missed: reload
    ///             }
    ///         }
    ///         update = stream.recv() => {
    ///             let Some(update) = update else { break };
    ///             match update.account {
    ///                 BalanceUpdateAccount::Trading if update.trading_epoch == epoch => {
    ///                     if update.trading_sequence <= sequence {
    ///                         continue; // already in the snapshot, or a repeat
    ///                     }
    ///                     if update.trading_sequence != sequence + 1 {
    ///                         break; // a gap: reload from step 2
    ///                     }
    ///                     sequence = update.trading_sequence;
    ///                     // Apply the Trading fields of `update.balances`.
    ///                 }
    ///                 BalanceUpdateAccount::Trading => {
    ///                     // Another epoch: drop it if dropped, otherwise reload.
    ///                 }
    ///                 BalanceUpdateAccount::Funding => {
    ///                     // Apply each asset whose funding_revision is higher.
    ///                 }
    ///                 BalanceUpdateAccount::Unknown(_) => {}
    ///             }
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe(
        &self,
        account_id: Option<&str>,
    ) -> Result<crate::realtime::TypedSubscription<BalanceUpdate>> {
        let account = scope::resolve_account_id(&self.ctx, account_id)?;
        let channel = format!("private:ledger:balances:{account}:proto");
        self.ctx
            .realtime
            .subscribe_proto(&channel, crate::codecs::decode::balance_update_from_bytes)
            .await
    }
}
