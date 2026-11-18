//! The TLS client configuration every connection of the crate uses.
//!
//! Each connector is built with an explicit crypto provider (ring), as rustls
//! recommends for libraries, so no connection depends on a process-level
//! default provider and the crate never installs one for the application.

use std::sync::{Arc, OnceLock};

use rustls::ClientConfig;

use crate::errors::{Error, Result};

/// The client configuration for HTTPS and `wss://`: ring, rustls's safe
/// default protocol versions and the webpki root certificates.
pub(crate) fn client_config() -> Result<Arc<ClientConfig>> {
    static CONFIG: OnceLock<std::result::Result<Arc<ClientConfig>, rustls::Error>> =
        OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
            Ok(Arc::new(config))
        })
        .clone()
        .map_err(|e| Error::transport(format!("TLS client configuration: {e}")))
}

/// The connector for a WebSocket upgrade: [`client_config`] for `wss://`;
/// a `ws://` URL connects in plain text whatever the connector.
pub(crate) fn websocket_connector() -> Result<tokio_tungstenite::Connector> {
    Ok(tokio_tungstenite::Connector::Rustls(client_config()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_config_names_its_own_provider() {
        let config = client_config().unwrap();
        assert!(Arc::ptr_eq(&config, &client_config().unwrap()));
        assert!(
            config
                .crypto_provider()
                .cipher_suites
                .iter()
                .any(|suite| suite.version() == &rustls::version::TLS13)
        );
    }
}
