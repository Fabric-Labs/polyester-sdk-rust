/// The account a call acts on: the client's default sub-account, the main
/// account, or a named sub-account.
///
/// ```
/// use polyester::models::SubAccountScope;
///
/// let default = SubAccountScope::Default; // also `SubAccountScope::default()`
/// let main = SubAccountScope::Main;
/// let named: SubAccountScope = 42.into(); // SubAccountScope::Id(42)
/// # let _ = (default, main, named);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SubAccountScope {
    /// [`crate::Config::default_sub_account_id`], or the main account when
    /// none is configured.
    #[default]
    Default,
    /// Always the main account, whatever default sub-account is configured.
    Main,
    /// This sub-account, by its numeric ID. 0 is refused with
    /// [`crate::Error::Validation`] before anything is sent: on the wire it
    /// would mean the main account, so use [`Self::Main`] for that.
    Id(u64),
}

impl From<u64> for SubAccountScope {
    fn from(id: u64) -> Self {
        Self::Id(id)
    }
}
