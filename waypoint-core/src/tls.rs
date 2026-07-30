//! TLS trust configuration, single-sourced across both engines.
//!
//! [`SslMode`] carries libpq's meanings, and this module is the one place that
//! turns a mode plus an optional CA file into an actual trust decision — as a
//! `rustls::ClientConfig` for PostgreSQL, or `mysql_async::SslOpts` for MySQL.
//! Keeping both mappings here is what stops the two engines from drifting, the
//! same rule `commands::migrate::select_pending` applies to pending selection.
//!
//! The ladder, matching libpq:
//!
//! | Mode | TLS | Chain | Hostname | Plaintext fallback |
//! |---|---|---|---|---|
//! | `disable` | no | — | — | — |
//! | `prefer` | opportunistic | no | no | yes, with a warning |
//! | `require` | mandatory | no | no | no |
//! | `verify-ca` | mandatory | yes | no | no |
//! | `verify-full` | mandatory | yes | yes | no |
//!
//! Note that `require` **encrypts without authenticating** — that is what
//! libpq means by it. Reach for `verify-full` when you want a server you can
//! actually trust.

use crate::config::SslMode;
use std::path::Path;

#[cfg(feature = "postgres")]
use crate::error::{Result, WaypointError};
#[cfg(feature = "postgres")]
use std::sync::Arc;

// ── Connection-string sslmode extraction ─────────────────────────────────────

/// libpq TLS parameters lifted out of a connection string.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct EmbeddedSslParams {
    /// The `sslmode=` value, if it parsed.
    pub mode: Option<SslMode>,
    /// The `sslrootcert=` path, if present.
    pub root_cert: Option<std::path::PathBuf>,
}

/// Pull libpq's `sslmode=` and `sslrootcert=` out of a connection string,
/// returning the remainder alongside the extracted values.
///
/// This exists because `tokio_postgres`'s parser understands only
/// `disable`/`prefer`/`require` for `sslmode` — `verify-ca` and `verify-full`
/// are a hard `InvalidValue` — and it rejects *any* key it does not recognise,
/// which includes `sslrootcert`. So `postgres://…?sslmode=verify-full&sslrootcert=/ca.pem`,
/// which is ordinary libpq and exactly what a JDBC-shaped connection string
/// looks like, cannot be parsed at all. We take both values ourselves and hand
/// tokio-postgres a string it accepts.
///
/// Handles both the URL form and the libpq `key=value` form, since
/// `WaypointConfig::connection_string` emits the latter for field-based
/// PostgreSQL configs. An unparseable `sslmode` is warned about and dropped
/// rather than passed through to fail more confusingly later.
///
/// The `key=value` scan is quote-aware, so a parameter name appearing inside a
/// quoted password value is left alone.
pub fn parse_url_sslmode(conn_string: &str) -> (String, EmbeddedSslParams) {
    if let Some(q) = conn_string.find('?') {
        parse_query_form(conn_string, q)
    } else {
        parse_keyvalue_form(conn_string)
    }
}

/// Is this a libpq TLS key we consume ourselves?
fn is_ssl_key(k: &str) -> bool {
    k.eq_ignore_ascii_case("sslmode") || k.eq_ignore_ascii_case("sslrootcert")
}

fn store_param(out: &mut EmbeddedSslParams, key: &str, raw: &str) {
    let value = raw.trim().trim_matches('\'');
    if key.eq_ignore_ascii_case("sslmode") {
        match value.parse::<SslMode>() {
            Ok(mode) => out.mode = Some(mode),
            Err(e) => log::warn!("{} (from the connection string); ignoring it.", e),
        }
    } else if !value.is_empty() {
        out.root_cert = Some(std::path::PathBuf::from(value));
    }
}

fn parse_query_form(conn_string: &str, q: usize) -> (String, EmbeddedSslParams) {
    let (base, query) = conn_string.split_at(q);
    let query = &query[1..];

    let mut kept: Vec<&str> = Vec::new();
    let mut out = EmbeddedSslParams::default();
    let mut found = false;

    for pair in query.split('&') {
        match pair.split_once('=') {
            Some((k, v)) if is_ssl_key(k) => {
                found = true;
                store_param(&mut out, k, v);
            }
            // Anything we do not consume is re-emitted verbatim, so percent
            // encoding in values such as `options=-c%20search_path%3Dfoo`
            // round-trips untouched.
            _ => kept.push(pair),
        }
    }

    if !found {
        return (conn_string.to_string(), out);
    }

    let rebuilt = if kept.is_empty() {
        base.to_string()
    } else {
        format!("{}?{}", base, kept.join("&"))
    };
    (rebuilt, out)
}

fn parse_keyvalue_form(conn_string: &str) -> (String, EmbeddedSslParams) {
    let lowered = conn_string.to_lowercase();
    // Only rebuild the string when there is something to remove — that keeps
    // the overwhelmingly common no-TLS-params case byte-for-byte untouched.
    if !lowered.contains("sslmode=") && !lowered.contains("sslrootcert=") {
        return (conn_string.to_string(), EmbeddedSslParams::default());
    }

    let mut kept: Vec<&str> = Vec::new();
    let mut out = EmbeddedSslParams::default();
    let mut found = false;
    let mut quoted = false;
    let mut consumed = 0usize;

    for token in conn_string.split_whitespace() {
        // Track whether we are inside a single-quoted value (libpq quotes
        // passwords containing spaces), so `password='a sslmode=b'` is not
        // mistaken for a real parameter.
        let inside = quoted;
        let offset = conn_string[consumed..].find(token).unwrap_or(0);
        consumed += offset + token.len();
        quoted ^= token.matches('\'').count() % 2 == 1;

        if !inside
            && let Some((k, v)) = token.split_once('=')
            && is_ssl_key(k)
        {
            found = true;
            store_param(&mut out, k, v);
            continue;
        }
        kept.push(token);
    }

    if found {
        (kept.join(" "), out)
    } else {
        (conn_string.to_string(), out)
    }
}

/// Reconcile a `sslrootcert=` found in the connection string with the
/// configured path, using the same precedence rule as [`reconcile_ssl_mode`].
pub fn reconcile_root_cert(
    configured: Option<&Path>,
    from_url: Option<std::path::PathBuf>,
) -> Option<std::path::PathBuf> {
    match configured {
        Some(p) => Some(p.to_path_buf()),
        None => from_url,
    }
}

/// Reconcile a `sslmode=` found in the connection string with the configured
/// [`SslMode`].
///
/// A configured mode other than the default `prefer` is taken as a deliberate
/// choice and wins. Otherwise the connection string's value applies — which is
/// what makes a bare `postgres://…?sslmode=verify-full` behave the way its
/// author expects.
pub fn reconcile_ssl_mode(configured: SslMode, from_url: Option<SslMode>) -> SslMode {
    match from_url {
        Some(url_mode) if configured == SslMode::Prefer => {
            if url_mode != configured {
                log::debug!(
                    "Using sslmode '{}' from the connection string (ssl_mode is at its default).",
                    url_mode
                );
            }
            url_mode
        }
        Some(url_mode) if url_mode != configured => {
            log::debug!(
                "Connection string requests sslmode '{}', but ssl_mode is set to '{}'; \
                 the configured value wins.",
                url_mode,
                configured
            );
            configured
        }
        _ => configured,
    }
}

// ── PostgreSQL: rustls ───────────────────────────────────────────────────────

/// Load the trust anchors to verify the server against.
///
/// With no `ssl_root_cert`, this is the compiled-in Mozilla bundle. With one,
/// it is **only** the certificates in that file — matching libpq's
/// `sslrootcert`, which replaces the default trust store rather than adding to
/// it.
///
/// Every failure is an error naming the path. Falling back to the default
/// roots when a CA file cannot be read would silently verify against the wrong
/// trust anchors, which is precisely the class of bug this module exists to
/// remove.
#[cfg(feature = "postgres")]
pub fn load_root_store(ssl_root_cert: Option<&Path>) -> Result<rustls::RootCertStore> {
    let Some(path) = ssl_root_cert else {
        return Ok(rustls::RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ));
    };

    let pem = std::fs::read(path).map_err(|e| {
        WaypointError::ConfigError(format!(
            "Failed to read ssl_root_cert '{}': {}",
            path.display(),
            e
        ))
    })?;

    // rustls re-exports `pki_types`, whose PEM iterator filters by section
    // kind — so a bundle carrying a private key alongside the CA loads
    // cleanly, and a file holding *only* a key correctly reports no
    // certificates. That saves pulling in `rustls-pemfile` as a dependency.
    use rustls::pki_types::pem::PemObject;

    let mut store = rustls::RootCertStore::empty();
    let mut count = 0usize;

    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&pem) {
        let cert = cert.map_err(|e| {
            WaypointError::ConfigError(format!(
                "Failed to parse ssl_root_cert '{}': {}",
                path.display(),
                e
            ))
        })?;
        store.add(cert).map_err(|e| {
            WaypointError::ConfigError(format!(
                "Certificate in ssl_root_cert '{}' was rejected: {}",
                path.display(),
                e
            ))
        })?;
        count += 1;
    }

    if count == 0 {
        return Err(WaypointError::ConfigError(format!(
            "ssl_root_cert '{}' contains no certificates. It must be a PEM file \
             with at least one CERTIFICATE block.",
            path.display()
        )));
    }

    log::debug!(
        "Loaded {} CA certificate(s) from {}; the built-in trust store is not used.",
        count,
        path.display()
    );
    Ok(store)
}

/// A verifier that accepts any certificate.
///
/// Backs `prefer` and `require`, which libpq defines as encrypting without
/// authenticating the server. Signature checking is still delegated to the
/// crypto provider — it is the *identity* of the peer that goes unchecked, not
/// the integrity of the handshake.
#[cfg(feature = "postgres")]
#[derive(Debug)]
struct NoVerifier {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

#[cfg(feature = "postgres")]
impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A verifier that checks the certificate chain but not the hostname.
///
/// Backs `verify-ca`. Everything is delegated to the standard webpki verifier;
/// only a name mismatch is converted into success.
#[cfg(feature = "postgres")]
#[derive(Debug)]
struct NoHostnameVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

#[cfg(feature = "postgres")]
impl rustls::client::danger::ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(v) => Ok(v),
            // rustls carries the name mismatch in two shapes — the bare
            // variant and the newer one with diagnostic context. Matching only
            // the first would leave verify-ca failing closed on exactly the
            // certificates it is supposed to accept.
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => {
                log::debug!(
                    "Server certificate is not valid for the requested name; \
                     accepted anyway because ssl_mode is 'verify-ca'."
                );
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Build the rustls client configuration for a given mode.
///
/// The provider is pinned to `ring` — see the `deny.toml` ban on `aws-lc-sys`
/// and the note in CLAUDE.md about why rustls' default feature set is off.
#[cfg(feature = "postgres")]
pub fn make_rustls_config(
    ssl_mode: SslMode,
    ssl_root_cert: Option<&Path>,
) -> Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            WaypointError::ConfigError(format!("Failed to configure TLS protocol versions: {}", e))
        })?;

    let config = match ssl_mode {
        // `disable` never reaches here — the caller uses NoTls.
        SslMode::Disable | SslMode::Prefer | SslMode::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier { provider }))
            .with_no_client_auth(),
        SslMode::VerifyCa => {
            let roots = Arc::new(load_root_store(ssl_root_cert)?);
            let inner =
                rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider)
                    .build()
                    .map_err(|e| {
                        WaypointError::ConfigError(format!(
                            "Failed to build certificate verifier: {}",
                            e
                        ))
                    })?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoHostnameVerifier { inner }))
                .with_no_client_auth()
        }
        SslMode::VerifyFull => builder
            .with_root_certificates(load_root_store(ssl_root_cert)?)
            .with_no_client_auth(),
    };

    Ok(config)
}

// ── MySQL: mysql_async SslOpts ───────────────────────────────────────────────

/// Map an [`SslMode`] onto `mysql_async`'s TLS options.
///
/// `None` means plaintext. Note that a `Some(_)` makes TLS **mandatory** —
/// mysql_async has no opportunistic mode — so `prefer` relies on the caller
/// probing the connection and retrying without TLS; see
/// `db::connect_mysql_pool`.
#[cfg(feature = "mysql")]
pub fn make_mysql_ssl_opts(
    ssl_mode: SslMode,
    ssl_root_cert: Option<&std::path::Path>,
) -> Option<mysql_async::SslOpts> {
    if ssl_mode == SslMode::Disable {
        return None;
    }

    let mut opts = mysql_async::SslOpts::default();

    if ssl_mode.verifies_certificate() {
        if let Some(path) = ssl_root_cert {
            opts = opts
                .with_root_certs(vec![path.to_path_buf().into()])
                // Replace the built-in roots rather than supplement them, so
                // this matches libpq's sslrootcert and the PostgreSQL path.
                .with_disable_built_in_roots(true);
        }
        opts = opts.with_danger_accept_invalid_certs(false);
        // verify-ca is *asked* to check the chain but not the name. The call
        // below is currently a no-op: mysql_async 0.37 detects a name mismatch
        // by testing whether the rustls error's Display contains
        // "NotValidForName", and rustls 0.23 renders that error as
        // "certificate not valid for name …" — the literal never appears. So
        // verify-ca on MySQL actually behaves like verify-full.
        //
        // We keep the call (it costs nothing and becomes correct the moment
        // mysql_async matches on the enum instead of the string) and warn, so
        // an operator whose certificate CN does not match gets told why the
        // connection failed instead of being quietly refused. Failing closed
        // is the right direction; failing closed *silently* is not.
        opts = opts.with_danger_skip_domain_validation(ssl_mode == SslMode::VerifyCa);
        if ssl_mode == SslMode::VerifyCa {
            log::warn!(
                "ssl_mode = 'verify-ca' on MySQL: the driver cannot currently skip \
                 hostname validation, so the certificate name will be checked too \
                 (as if 'verify-full'). This is stricter than requested and may \
                 reject a certificate issued to a different name."
            );
        }
    } else {
        // prefer / require: encrypt, do not authenticate.
        opts = opts
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true);
    }

    Some(opts)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_url_sslmode ────────────────────────────────────────────────────

    #[test]
    fn test_parse_url_sslmode_absent_is_untouched() {
        let s = "postgres://u:p@host:5432/db";
        let (out, p) = parse_url_sslmode(s);
        assert_eq!(out, s);
        assert_eq!(p, EmbeddedSslParams::default());
    }

    #[test]
    fn test_parse_url_sslmode_query_form() {
        let (out, p) = parse_url_sslmode("postgres://u@host/db?sslmode=require");
        assert_eq!(out, "postgres://u@host/db");
        assert_eq!(p.mode, Some(SslMode::Require));
    }

    #[test]
    fn test_parse_url_sslmode_extracts_verify_full() {
        // The whole point: tokio-postgres cannot parse this value itself.
        let (out, p) = parse_url_sslmode("postgres://u@host/db?sslmode=verify-full");
        assert_eq!(out, "postgres://u@host/db");
        assert_eq!(p.mode, Some(SslMode::VerifyFull));
    }

    #[test]
    fn test_parse_url_sslmode_extracts_root_cert() {
        // tokio-postgres rejects `sslrootcert` as an unknown option, so this
        // has to come out of the string too.
        let (out, p) = parse_url_sslmode("postgres://u@host/db?sslrootcert=/etc/ssl/ca.pem");
        assert_eq!(out, "postgres://u@host/db");
        assert_eq!(
            p.root_cert,
            Some(std::path::PathBuf::from("/etc/ssl/ca.pem"))
        );
    }

    #[test]
    fn test_parse_url_sslmode_extracts_both_params() {
        let (out, p) = parse_url_sslmode(
            "postgres://u@host/db?sslmode=verify-full&sslrootcert=/ca.pem&application_name=wp",
        );
        assert_eq!(out, "postgres://u@host/db?application_name=wp");
        assert_eq!(p.mode, Some(SslMode::VerifyFull));
        assert_eq!(p.root_cert, Some(std::path::PathBuf::from("/ca.pem")));
    }

    #[test]
    fn test_parse_url_sslmode_keeps_other_query_params() {
        let (out, p) =
            parse_url_sslmode("postgres://u@host/db?sslmode=verify-ca&keepalives=1&foo=bar");
        assert_eq!(out, "postgres://u@host/db?keepalives=1&foo=bar");
        assert_eq!(p.mode, Some(SslMode::VerifyCa));
    }

    #[test]
    fn test_parse_url_sslmode_preserves_encoded_values_verbatim() {
        // Kept pairs are re-emitted byte-for-byte, so percent encoding in an
        // unrelated value survives the round trip.
        let (out, _) =
            parse_url_sslmode("postgres://h/db?options=-c%20search_path%3Dfoo&sslmode=require");
        assert_eq!(out, "postgres://h/db?options=-c%20search_path%3Dfoo");
    }

    #[test]
    fn test_parse_url_sslmode_is_case_insensitive() {
        let (out, p) = parse_url_sslmode("postgres://u@host/db?SSLMode=Verify-Full");
        assert_eq!(out, "postgres://u@host/db");
        assert_eq!(p.mode, Some(SslMode::VerifyFull));
    }

    #[test]
    fn test_parse_url_sslmode_keyvalue_form() {
        let (out, p) =
            parse_url_sslmode("host=db port=5432 sslmode=verify-full user=admin dbname=app");
        assert_eq!(out, "host=db port=5432 user=admin dbname=app");
        assert_eq!(p.mode, Some(SslMode::VerifyFull));
    }

    #[test]
    fn test_parse_url_sslmode_keyvalue_extracts_root_cert() {
        let (out, p) = parse_url_sslmode("host=db sslrootcert=/ca.pem dbname=app");
        assert_eq!(out, "host=db dbname=app");
        assert_eq!(p.root_cert, Some(std::path::PathBuf::from("/ca.pem")));
    }

    #[test]
    fn test_parse_url_sslmode_keyvalue_ignores_quoted_value() {
        // A password that happens to contain "sslmode=" must not be treated as
        // a parameter, and nothing should be stripped.
        let s = "host=db user=admin password='a sslmode=require b' dbname=app";
        let (out, p) = parse_url_sslmode(s);
        assert_eq!(out, s);
        assert_eq!(p, EmbeddedSslParams::default());
    }

    #[test]
    fn test_parse_url_sslmode_unparseable_is_dropped() {
        let (out, p) = parse_url_sslmode("postgres://u@host/db?sslmode=banana");
        assert_eq!(out, "postgres://u@host/db");
        assert_eq!(p.mode, None);
    }

    #[test]
    fn test_parse_url_sslmode_allow_is_dropped() {
        // `allow` is rejected by FromStr, so it is warned about and ignored
        // rather than being silently treated as `prefer`.
        let (_, p) = parse_url_sslmode("postgres://u@host/db?sslmode=allow");
        assert_eq!(p.mode, None);
    }

    /// The bug the pre-pass exists to fix: tokio-postgres cannot parse either
    /// of these, and the stripped remainder must be something it accepts.
    #[cfg(feature = "postgres")]
    #[test]
    fn test_stripped_string_is_parseable_by_tokio_postgres() {
        for raw in [
            "postgres://u@host/db?sslmode=verify-full",
            "postgres://u@host/db?sslrootcert=/ca.pem",
            "host=db sslmode=verify-ca dbname=app",
        ] {
            assert!(
                raw.parse::<tokio_postgres::Config>().is_err(),
                "expected tokio-postgres to reject {raw}"
            );
            let (cleaned, _) = parse_url_sslmode(raw);
            assert!(
                cleaned.parse::<tokio_postgres::Config>().is_ok(),
                "tokio-postgres rejected the cleaned string {cleaned}"
            );
        }
    }

    #[test]
    fn test_reconcile_root_cert_config_wins() {
        let configured = std::path::PathBuf::from("/config/ca.pem");
        let from_url = std::path::PathBuf::from("/url/ca.pem");
        assert_eq!(
            reconcile_root_cert(Some(&configured), Some(from_url.clone())),
            Some(configured.clone())
        );
        assert_eq!(
            reconcile_root_cert(None, Some(from_url.clone())),
            Some(from_url)
        );
        assert_eq!(reconcile_root_cert(None, None), None);
    }

    // ── reconcile_ssl_mode ───────────────────────────────────────────────────

    #[test]
    fn test_reconcile_url_wins_when_config_is_default() {
        assert_eq!(
            reconcile_ssl_mode(SslMode::Prefer, Some(SslMode::VerifyFull)),
            SslMode::VerifyFull
        );
    }

    #[test]
    fn test_reconcile_config_wins_when_set() {
        assert_eq!(
            reconcile_ssl_mode(SslMode::VerifyFull, Some(SslMode::Require)),
            SslMode::VerifyFull
        );
        assert_eq!(
            reconcile_ssl_mode(SslMode::Disable, Some(SslMode::Require)),
            SslMode::Disable
        );
    }

    #[test]
    fn test_reconcile_without_url_mode_keeps_config() {
        assert_eq!(reconcile_ssl_mode(SslMode::Require, None), SslMode::Require);
        assert_eq!(reconcile_ssl_mode(SslMode::Prefer, None), SslMode::Prefer);
    }

    // ── load_root_store ──────────────────────────────────────────────────────

    #[cfg(feature = "postgres")]
    mod root_store {
        use super::*;
        use std::io::Write;

        /// A syntactically valid self-signed certificate, for trust-store
        /// loading tests only — it is never presented to a server.
        const TEST_CA_PEM: &str = include_str!("../tests/fixtures/test-ca.pem");

        fn write_temp(contents: &str) -> tempfile::NamedTempFile {
            let mut f = tempfile::NamedTempFile::new().unwrap();
            f.write_all(contents.as_bytes()).unwrap();
            f.flush().unwrap();
            f
        }

        #[test]
        fn test_load_root_store_defaults_to_builtin_roots() {
            let store = load_root_store(None).unwrap();
            assert!(
                !store.is_empty(),
                "the built-in Mozilla bundle should be non-empty"
            );
        }

        #[test]
        fn test_load_root_store_reads_custom_ca() {
            let f = write_temp(TEST_CA_PEM);
            let store = load_root_store(Some(f.path())).unwrap();
            // Exactly the supplied certificate — the built-in roots are
            // replaced, not supplemented.
            assert_eq!(store.len(), 1);
        }

        #[test]
        fn test_load_root_store_missing_file_errors() {
            let err = load_root_store(Some(Path::new("/nonexistent/ca.pem"))).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("ssl_root_cert"), "got: {}", msg);
            assert!(msg.contains("/nonexistent/ca.pem"), "got: {}", msg);
        }

        #[test]
        fn test_load_root_store_empty_file_errors() {
            let f = write_temp("");
            let err = load_root_store(Some(f.path())).unwrap_err();
            assert!(err.to_string().contains("no certificates"), "got: {}", err);
        }

        #[test]
        fn test_load_root_store_pem_without_certificates_errors() {
            // A well-formed PEM carrying the wrong kind of block.
            let f = write_temp(
                "-----BEGIN PRIVATE KEY-----\nMIIBVQIBADANBg==\n-----END PRIVATE KEY-----\n",
            );
            let err = load_root_store(Some(f.path())).unwrap_err();
            assert!(err.to_string().contains("no certificates"), "got: {}", err);
        }

        #[test]
        fn test_load_root_store_malformed_pem_errors() {
            let f = write_temp("-----BEGIN CERTIFICATE-----\nnot base64 at all!!\n");
            let err = load_root_store(Some(f.path())).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("parse") || msg.contains("no certificates"),
                "got: {}",
                msg
            );
        }

        #[test]
        fn test_make_rustls_config_all_modes_build() {
            let f = write_temp(TEST_CA_PEM);
            for mode in [
                SslMode::Prefer,
                SslMode::Require,
                SslMode::VerifyCa,
                SslMode::VerifyFull,
            ] {
                assert!(
                    make_rustls_config(mode, Some(f.path())).is_ok(),
                    "mode {} failed to build",
                    mode
                );
            }
        }

        #[test]
        fn test_make_rustls_config_propagates_ca_errors() {
            // A verifying mode must refuse to build rather than quietly fall
            // back to the built-in roots.
            let bad = Path::new("/nonexistent/ca.pem");
            assert!(make_rustls_config(SslMode::VerifyFull, Some(bad)).is_err());
            assert!(make_rustls_config(SslMode::VerifyCa, Some(bad)).is_err());
            // Non-verifying modes never read the file, so they still build.
            assert!(make_rustls_config(SslMode::Require, Some(bad)).is_ok());
        }
    }

    // ── MySQL SslOpts mapping ────────────────────────────────────────────────

    #[cfg(feature = "mysql")]
    mod mysql_opts {
        use super::*;

        #[test]
        fn test_mysql_ssl_opts_disable_is_none() {
            assert!(make_mysql_ssl_opts(SslMode::Disable, None).is_none());
        }

        #[test]
        fn test_mysql_ssl_opts_non_verifying_modes_skip_checks() {
            for mode in [SslMode::Prefer, SslMode::Require] {
                let opts = make_mysql_ssl_opts(mode, None).unwrap();
                assert!(opts.accept_invalid_certs(), "mode {}", mode);
                assert!(opts.skip_domain_validation(), "mode {}", mode);
            }
        }

        #[test]
        fn test_mysql_ssl_opts_verify_ca_checks_chain_not_name() {
            let opts = make_mysql_ssl_opts(SslMode::VerifyCa, None).unwrap();
            assert!(!opts.accept_invalid_certs());
            assert!(opts.skip_domain_validation());
        }

        #[test]
        fn test_mysql_ssl_opts_verify_full_checks_everything() {
            let opts = make_mysql_ssl_opts(SslMode::VerifyFull, None).unwrap();
            assert!(!opts.accept_invalid_certs());
            assert!(!opts.skip_domain_validation());
        }

        #[test]
        fn test_mysql_ssl_opts_custom_ca_replaces_builtin_roots() {
            let path = std::path::Path::new("/etc/ssl/my-ca.pem");
            let opts = make_mysql_ssl_opts(SslMode::VerifyFull, Some(path)).unwrap();
            assert_eq!(opts.root_certs().len(), 1);
            assert!(opts.disable_built_in_roots());

            // Without a CA the built-in roots stay in play.
            let opts = make_mysql_ssl_opts(SslMode::VerifyFull, None).unwrap();
            assert!(opts.root_certs().is_empty());
            assert!(!opts.disable_built_in_roots());
        }

        #[test]
        fn test_mysql_ssl_opts_ignores_ca_for_non_verifying_modes() {
            let path = std::path::Path::new("/etc/ssl/my-ca.pem");
            let opts = make_mysql_ssl_opts(SslMode::Require, Some(path)).unwrap();
            assert!(opts.root_certs().is_empty());
        }
    }
}
