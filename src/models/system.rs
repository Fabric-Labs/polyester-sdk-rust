//! Server build version (`system.v1.SystemService`).

/// The exact build an environment serves, from
/// [`crate::Client::get_version`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerVersion {
    /// Version of the build: `MAJOR.MINOR.PATCH` for a release, such as
    /// `"0.10.0"`; a build made after a release appends the number of source
    /// revisions since it and `g` with the first 12 hexadecimal digits of its
    /// own revision, such as `"0.10.0-12-g4e21a2f09670"`. It is not a semantic
    /// version: compare [`ServerVersion::release`] instead of parsing it.
    pub version: String,
    /// Full 40-character lowercase hexadecimal source revision of the build.
    pub commit: String,
    /// Environment that answered, such as `"devnet"`, `"testnet"`,
    /// `"mainnet"` or `"local"`. Kept as the server sends it, so a value this
    /// SDK version does not know is not refused.
    pub environment: String,
}

impl ServerVersion {
    /// The release the build follows: [`ServerVersion::version`] up to its
    /// first `-`, such as `"0.10.0"` for `"0.10.0-12-g4e21a2f09670"`.
    pub fn release(&self) -> &str {
        self.version
            .split_once('-')
            .map_or(self.version.as_str(), |(release, _)| release)
    }
}
