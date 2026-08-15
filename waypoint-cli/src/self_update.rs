//! Self-update mechanism via the GitHub Releases API.
//!
//! Downloads the platform-specific release tarball and performs an atomic
//! in-place replacement. Two verification steps gate the install:
//!
//! 1. `SHA256SUMS` is verified against its Sigstore bundle with `cosign`, if
//!    `cosign` is on PATH. A *failed* signature always aborts; a *missing*
//!    cosign downgrades to step 2 with a warning, or aborts under
//!    `--require-signature`.
//! 2. The tarball's SHA-256 must match its entry in `SHA256SUMS`.
//!
//! On failure it prints manual instructions rather than piping a remote script
//! into a shell.

use std::env;
use std::fs;
use std::io::Read;
use std::process::{Command, Stdio};

use colored::Colorize;
use flate2::read::GzDecoder;
use semver::Version;
use tar::Archive;
use waypoint_core::error::WaypointError;

const REPO: &str = "tensorbee/waypoint";
const INSTALL_SH_URL: &str = "https://raw.githubusercontent.com/tensorbee/waypoint/main/install.sh";

/// Minimal representation of a GitHub release for version checking.
#[derive(serde::Deserialize)]
struct GitHubRelease {
    tag_name: String,
}

/// Parse the compile-time crate version into a semver Version.
fn current_version() -> Result<Version, WaypointError> {
    Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse current version: {e}")))
}

/// Fetch the latest release metadata from the GitHub API.
fn fetch_latest_release() -> Result<GitHubRelease, WaypointError> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let resp: GitHubRelease = ureq::get(&url)
        .header("User-Agent", "waypoint-self-update")
        .call()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to fetch latest release: {e}")))?
        .body_mut()
        .read_json()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse release JSON: {e}")))?;
    Ok(resp)
}

/// Parse a GitHub release tag (with optional 'v' prefix) into a semver Version.
fn parse_version(tag: &str) -> Result<Version, WaypointError> {
    let v = tag.strip_prefix('v').unwrap_or(tag);
    Version::parse(v)
        .map_err(|e| WaypointError::UpdateError(format!("Failed to parse version '{tag}': {e}")))
}

/// Detect the current OS and architecture for release asset selection.
fn platform_target() -> Result<(&'static str, &'static str), WaypointError> {
    let os = match env::consts::OS {
        "linux" => "linux",
        "macos" => "macos",
        _ => {
            return Err(WaypointError::UpdateError(format!(
                "Unsupported OS: {}",
                env::consts::OS
            )));
        }
    };
    let arch = match env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => {
            return Err(WaypointError::UpdateError(format!(
                "Unsupported architecture: {}",
                env::consts::ARCH
            )));
        }
    };
    Ok((os, arch))
}

/// Download a release asset into memory.
fn fetch_asset(tag: &str, asset: &str) -> Result<Vec<u8>, WaypointError> {
    let url = format!("https://github.com/{REPO}/releases/download/{tag}/{asset}");
    let mut resp = ureq::get(&url)
        .header("User-Agent", "waypoint-self-update")
        .call()
        .map_err(|e| {
            WaypointError::UpdateError(format!("Failed to fetch {asset} for {tag}: {e}"))
        })?;
    resp.body_mut()
        .read_to_vec()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to read {asset}: {e}")))
}

/// How strongly the release manifest was authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    /// cosign verified the Sigstore bundle: certificate chain to the Fulcio
    /// root, signer identity, and Rekor transparency-log inclusion.
    Signature,
    /// `cosign` was not on PATH. The tarball still had to match the SHA-256 in
    /// `SHA256SUMS`, but nothing proves `SHA256SUMS` itself is authentic.
    ChecksumOnly,
}

impl Verification {
    fn as_str(self) -> &'static str {
        match self {
            Verification::Signature => "cosign-signature",
            Verification::ChecksumOnly => "sha256-only",
        }
    }
}

/// The signer identity cosign must match, as a regex over the certificate SAN.
///
/// Real Fulcio SANs for this project look like:
/// `https://github.com/tensorbee/waypoint/.github/workflows/release.yml@refs/tags/v0.5.0`
///
/// Anchored at the start and with literal dots escaped, so `github.com` cannot
/// be satisfied by `githubXcom`, and a lookalike owner such as
/// `tensorbee-evil` cannot match.
fn certificate_identity_pattern() -> String {
    format!(r"^https://github\.com/{REPO}/\.github/workflows/")
}

/// Verify `SHA256SUMS` against its Sigstore bundle using the `cosign` binary.
///
/// # Why shell out
///
/// Keyless Sigstore verification is not just "check a signature". Fulcio leaf
/// certificates are valid for roughly ten minutes, so proving one was valid
/// *at signing time* requires the Rekor transparency log — a released artifact
/// is almost always older than its certificate's validity window. An
/// in-process implementation therefore needs bundle parsing, a Fulcio trust
/// root, certificate path validation and Rekor inclusion proofs.
///
/// The `sigstore-verify` crate does all of that, but pulls ~166 transitive
/// crates including a second HTTP stack and `aws-lc-sys` — the cmake-built C
/// dependency `deny.toml` bans. Hand-rolling the subset we could do without
/// Rekor would skip exactly the check that makes keyless signing sound, which
/// is worse than not claiming it: verification that looks complete but isn't.
///
/// So we delegate to the real implementation when it is available, and report
/// honestly when it is not.
fn verify_signature(tag: &str, sha256sums: &[u8]) -> Result<Verification, WaypointError> {
    if Command::new("cosign")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_err()
    {
        return Ok(Verification::ChecksumOnly);
    }

    let dir = tempfile::tempdir()
        .map_err(|e| WaypointError::UpdateError(format!("Cannot create temp dir: {e}")))?;
    let sums_path = dir.path().join("SHA256SUMS");
    fs::write(&sums_path, sha256sums)
        .map_err(|e| WaypointError::UpdateError(format!("Cannot stage SHA256SUMS: {e}")))?;

    // Pin *who* signed it, not merely that it carries a valid signature. Any
    // Fulcio identity can produce a well-formed bundle; only this repository's
    // release workflow, run by GitHub's OIDC issuer, is acceptable.
    let identity = certificate_identity_pattern();
    let mut cmd = Command::new("cosign");
    cmd.arg("verify-blob");

    // Prefer the Sigstore bundle; fall back to the detached signature +
    // certificate pair. Releases up to and including v0.5.0 predate the bundle
    // and publish only `.sig`/`.pem`, so requiring the bundle would make
    // signature verification impossible for exactly the releases people are
    // updating *from* today. Both forms are verified by cosign itself with the
    // same trust root, identity pin and Rekor lookup.
    match fetch_asset(tag, "SHA256SUMS.cosign.bundle") {
        Ok(bundle) => {
            let bundle_path = dir.path().join("SHA256SUMS.cosign.bundle");
            fs::write(&bundle_path, &bundle)
                .map_err(|e| WaypointError::UpdateError(format!("Cannot stage bundle: {e}")))?;
            cmd.arg("--bundle").arg(&bundle_path);
        }
        Err(_) => {
            let sig = fetch_asset(tag, "SHA256SUMS.sig").map_err(|e| {
                WaypointError::UpdateError(format!(
                    "{tag} publishes neither SHA256SUMS.cosign.bundle nor SHA256SUMS.sig, \
                     so its signature cannot be verified ({e}). Re-run without \
                     --require-signature to accept SHA-256-only verification."
                ))
            })?;
            let cert = fetch_asset(tag, "SHA256SUMS.pem")?;
            let sig_path = dir.path().join("SHA256SUMS.sig");
            let cert_path = dir.path().join("SHA256SUMS.pem");
            fs::write(&sig_path, &sig)
                .map_err(|e| WaypointError::UpdateError(format!("Cannot stage signature: {e}")))?;
            fs::write(&cert_path, &cert).map_err(|e| {
                WaypointError::UpdateError(format!("Cannot stage certificate: {e}"))
            })?;
            cmd.arg("--signature")
                .arg(&sig_path)
                .arg("--certificate")
                .arg(&cert_path);
        }
    }

    let output = cmd
        .arg("--certificate-identity-regexp")
        .arg(&identity)
        .arg("--certificate-oidc-issuer")
        .arg("https://token.actions.githubusercontent.com")
        .arg(&sums_path)
        .output()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to run cosign: {e}")))?;

    if output.status.success() {
        return Ok(Verification::Signature);
    }

    // cosign ran and said no. That is never downgraded to a warning.
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(WaypointError::UpdateError(format!(
        "cosign signature verification FAILED for {tag} SHA256SUMS. \
         Refusing to install. cosign said: {}",
        stderr.trim()
    )))
}

/// Look up the expected hex digest for `asset` in a `sha256sum`-format manifest.
///
/// Lines look like `<64-hex>  <filename>`. A missing entry is an error, never a
/// skip: "no checksum published" and "checksum does not match" both mean we
/// must not execute the download.
fn expected_digest(manifest: &str, asset: &str) -> Result<String, WaypointError> {
    for line in manifest.lines() {
        let mut parts = line.split_whitespace();
        let (Some(digest), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        // `sha256sum` writes binary-mode entries as `*name`.
        let name = name.strip_prefix('*').unwrap_or(name);
        if name == asset {
            if digest.len() != 64 || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(WaypointError::UpdateError(format!(
                    "Malformed digest for {asset} in SHA256SUMS"
                )));
            }
            return Ok(digest.to_ascii_lowercase());
        }
    }
    Err(WaypointError::UpdateError(format!(
        "No SHA256SUMS entry for {asset}; refusing to install an unverified binary"
    )))
}

/// Hex-encoded SHA-256 of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Download a release tarball and atomically replace the current binary.
fn download_and_replace(
    version: &str,
    require_signature: bool,
) -> Result<Verification, WaypointError> {
    let (os, arch) = platform_target()?;
    let tag = if version.starts_with('v') {
        version.to_string()
    } else {
        format!("v{version}")
    };
    let tarball_name = format!("waypoint-{tag}-{os}-{arch}.tar.gz");
    let url = format!("https://github.com/{REPO}/releases/download/{tag}/{tarball_name}");

    eprintln!("Downloading {}...", url);

    let mut resp = ureq::get(&url)
        .header("User-Agent", "waypoint-self-update")
        .call()
        .map_err(|e| WaypointError::UpdateError(format!("Download failed: {e}")))?;

    let bytes = resp
        .body_mut()
        .read_to_vec()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to read response body: {e}")))?;

    // Verify the download *before* unpacking or executing anything from it.
    // TLS authenticates the host; it says nothing about whether the artifact is
    // the one that was built and signed.
    //
    // Order matters: authenticate the manifest first, then check the tarball
    // against it. Checking a tarball against an unauthenticated manifest only
    // proves the download was not corrupted in transit.
    let manifest_bytes = fetch_asset(&tag, "SHA256SUMS")?;
    let verification = verify_signature(&tag, &manifest_bytes)?;

    match verification {
        Verification::Signature => {
            eprintln!(
                "{} SHA256SUMS signature verified (cosign, Sigstore bundle).",
                "✓".green()
            );
        }
        Verification::ChecksumOnly if require_signature => {
            return Err(WaypointError::UpdateError(
                "--require-signature was given but `cosign` is not installed, so the \
                 release signature cannot be verified. Install cosign \
                 (https://docs.sigstore.dev/cosign/installation/) or drop the flag to \
                 proceed with SHA-256 verification only."
                    .into(),
            ));
        }
        Verification::ChecksumOnly => {
            eprintln!(
                "{} `cosign` not found — verifying SHA-256 only. The checksum file itself \
                 is unauthenticated, so this does not protect against tampered release \
                 assets. Install cosign, or pass --require-signature to make this a hard \
                 failure.",
                "!".yellow().bold()
            );
        }
    }

    let manifest = String::from_utf8(manifest_bytes)
        .map_err(|e| WaypointError::UpdateError(format!("SHA256SUMS is not valid UTF-8: {e}")))?;
    let expected = expected_digest(&manifest, &tarball_name)?;
    let actual = sha256_hex(&bytes);
    if actual != expected {
        return Err(WaypointError::UpdateError(format!(
            "Checksum mismatch for {tarball_name}: expected {expected}, got {actual}. \
             Refusing to install."
        )));
    }
    eprintln!("{} SHA-256 of {tarball_name} matches.", "✓".green());

    // Extract the binary from the tar.gz
    let gz = GzDecoder::new(&bytes[..]);
    let mut archive = Archive::new(gz);
    let mut binary_data = None;

    for entry in archive
        .entries()
        .map_err(|e| WaypointError::UpdateError(format!("Failed to read tar entries: {e}")))?
    {
        let mut entry =
            entry.map_err(|e| WaypointError::UpdateError(format!("Bad tar entry: {e}")))?;
        let path = entry
            .path()
            .map_err(|e| WaypointError::UpdateError(format!("Bad path in tar: {e}")))?
            .to_path_buf();

        if path.file_name().and_then(|n| n.to_str()) == Some("waypoint") {
            let mut buf = Vec::new();
            entry
                .read_to_end(&mut buf)
                .map_err(|e| WaypointError::UpdateError(format!("Failed to read binary: {e}")))?;
            binary_data = Some(buf);
            break;
        }
    }

    let binary_data = binary_data
        .ok_or_else(|| WaypointError::UpdateError("Binary not found in archive".into()))?;

    // Atomic replace: write to temp file in same directory, then rename
    let current_exe = env::current_exe()
        .map_err(|e| WaypointError::UpdateError(format!("Cannot determine current exe: {e}")))?;
    let exe_dir = current_exe
        .parent()
        .ok_or_else(|| WaypointError::UpdateError("Cannot determine exe directory".into()))?;

    let tmp = tempfile::NamedTempFile::new_in(exe_dir)
        .map_err(|e| WaypointError::UpdateError(format!("Cannot create temp file: {e}")))?;

    fs::write(tmp.path(), &binary_data)
        .map_err(|e| WaypointError::UpdateError(format!("Failed to write new binary: {e}")))?;

    // Set executable permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o755))
            .map_err(|e| WaypointError::UpdateError(format!("Failed to set permissions: {e}")))?;
    }

    // Validate the downloaded binary by running --version on it
    let tmp_path = tmp.into_temp_path();
    let output = std::process::Command::new(AsRef::<std::path::Path>::as_ref(&tmp_path))
        .arg("--version")
        .output();
    match output {
        Ok(o) if o.status.success() => {
            // Binary is valid, proceed with replacement
        }
        _ => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(WaypointError::UpdateError(
                "Downloaded binary failed validation (--version check)".into(),
            ));
        }
    }

    // Create a backup of the current binary before replacing
    let backup_path = current_exe.with_extension("backup");
    if current_exe.exists() {
        fs::copy(&current_exe, &backup_path).map_err(|e| {
            WaypointError::UpdateError(format!("Failed to create backup of current binary: {e}"))
        })?;
    }

    // Persist (disables auto-cleanup) and rename atomically
    if let Err(e) = tmp_path.persist(&current_exe) {
        // Try to restore from backup. If *that* also fails the installed binary
        // may be missing entirely, which is the worst moment to say nothing —
        // tell the operator exactly where their old binary is so they can put
        // it back by hand.
        if backup_path.exists()
            && let Err(restore_err) = fs::rename(&backup_path, &current_exe)
        {
            return Err(WaypointError::UpdateError(format!(
                "Failed to replace binary: {e}. Restoring the previous version also \
                 failed: {restore_err}. Your previous binary is still at {} — move it \
                 back to {} to recover.",
                backup_path.display(),
                current_exe.display()
            )));
        }
        return Err(WaypointError::UpdateError(format!(
            "Failed to replace binary: {e}. The previous version was restored."
        )));
    }

    // Success — remove backup
    let _ = fs::remove_file(&backup_path);

    Ok(verification)
}

/// Print manual recovery instructions when the direct update fails.
///
/// This deliberately does *not* run `curl … | sh` for you. Piping a remote
/// script into a shell executes unverified remote code, and doing it
/// automatically means a transient 404 or network blip escalates into
/// arbitrary code execution. The direct path above verifies a SHA-256 against
/// the signed release manifest; silently falling back to something weaker
/// would defeat that. Printing the command keeps the choice with the operator.
fn print_manual_update_instructions() {
    eprintln!();
    eprintln!("{}", "To update manually:".bold());
    eprintln!("  curl -sSfL {INSTALL_SH_URL} -o install.sh");
    eprintln!("  less install.sh   # review before running");
    eprintln!("  sh install.sh");
    eprintln!();
    eprintln!("{}", "Release artifacts are signed; verify with:".dimmed());
    eprintln!(
        "{}",
        format!(
            "  cosign verify-blob --bundle SHA256SUMS.cosign.bundle \\\n    \
             --certificate-identity-regexp 'https://github.com/{REPO}/' \\\n    \
             --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS"
        )
        .dimmed()
    );
}

/// Check for and optionally install the latest waypoint release.
pub fn self_update(
    check_only: bool,
    json_output: bool,
    require_signature: bool,
) -> Result<(), WaypointError> {
    let current = current_version()?;
    let release = fetch_latest_release()?;
    let latest = parse_version(&release.tag_name)?;

    if json_output && check_only {
        println!(
            "{}",
            serde_json::json!({
                "current_version": current.to_string(),
                "latest_version": latest.to_string(),
                "update_available": latest > current,
            })
        );
        return Ok(());
    }

    if current >= latest {
        if json_output {
            println!(
                "{}",
                serde_json::json!({
                    "current_version": current.to_string(),
                    "latest_version": latest.to_string(),
                    "update_available": false,
                    "message": "Already up to date.",
                })
            );
        } else {
            eprintln!(
                "{} You are already on the latest version ({}).",
                "✓".green().bold(),
                current
            );
        }
        return Ok(());
    }

    if check_only {
        if !json_output {
            eprintln!(
                "{} Update available: {} → {}",
                "!".yellow().bold(),
                current.to_string().dimmed(),
                latest.to_string().green().bold()
            );
            eprintln!("Run {} to update.", "waypoint self-update".bold());
        }
        return Ok(());
    }

    eprintln!(
        "Updating waypoint {} → {}...",
        current.to_string().dimmed(),
        latest.to_string().green().bold()
    );

    match download_and_replace(&latest.to_string(), require_signature) {
        Ok(verification) => {
            if json_output {
                println!(
                    "{}",
                    serde_json::json!({
                        "current_version": current.to_string(),
                        "latest_version": latest.to_string(),
                        "updated": true,
                        "verification": verification.as_str(),
                        "message": format!("Successfully updated to {}.", latest),
                    })
                );
            } else {
                eprintln!("{} Successfully updated to {}.", "✓".green().bold(), latest);
            }
        }
        Err(e) => {
            if json_output {
                println!(
                    "{}",
                    serde_json::json!({
                        "current_version": current.to_string(),
                        "latest_version": latest.to_string(),
                        "updated": false,
                        "error": e.to_string(),
                    })
                );
            } else {
                eprintln!("{} Update failed: {}", "✗".red().bold(), e);
                print_manual_update_instructions();
            }
            return Err(e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = "\
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  waypoint-v1.0.0-linux-amd64.tar.gz
BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB  waypoint-v1.0.0-macos-arm64.tar.gz
cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc *waypoint-v1.0.0-linux-arm64.tar.gz
";

    #[test]
    fn finds_digest_for_asset() {
        assert_eq!(
            expected_digest(MANIFEST, "waypoint-v1.0.0-linux-amd64.tar.gz").unwrap(),
            "a".repeat(64)
        );
    }

    #[test]
    fn digest_lookup_is_lowercased() {
        assert_eq!(
            expected_digest(MANIFEST, "waypoint-v1.0.0-macos-arm64.tar.gz").unwrap(),
            "b".repeat(64)
        );
    }

    #[test]
    fn handles_binary_mode_star_prefix() {
        assert_eq!(
            expected_digest(MANIFEST, "waypoint-v1.0.0-linux-arm64.tar.gz").unwrap(),
            "c".repeat(64)
        );
    }

    #[test]
    fn missing_entry_is_an_error_not_a_skip() {
        // A release that publishes no checksum for our platform must fail the
        // update, never fall through to installing an unverified binary.
        let err = expected_digest(MANIFEST, "waypoint-v1.0.0-windows-amd64.tar.gz").unwrap_err();
        assert!(err.to_string().contains("No SHA256SUMS entry"));
    }

    #[test]
    fn malformed_digest_is_rejected() {
        let bad = "notahexdigest  waypoint-v1.0.0-linux-amd64.tar.gz\n";
        assert!(expected_digest(bad, "waypoint-v1.0.0-linux-amd64.tar.gz").is_err());
    }

    #[test]
    fn sha256_matches_known_vector() {
        // NIST/RFC 6234 test vector for "abc".
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// Minimal regex evaluation for the anchored, literal-dot patterns we
    /// generate — enough to assert matching behaviour without a regex crate.
    fn matches_identity(pattern: &str, san: &str) -> bool {
        let literal = pattern.trim_start_matches('^').replace("\\.", "\u{0}");
        let literal = literal.replace('.', "\u{1}"); // any unescaped dot = wildcard marker
        let literal = literal.replace('\u{0}', ".");
        // No unescaped dots are expected in our pattern; if one appears the
        // test should fail loudly rather than silently pass.
        assert!(
            !literal.contains('\u{1}'),
            "pattern has an unescaped '.' metacharacter: {pattern}"
        );
        san.starts_with(&literal)
    }

    #[test]
    fn identity_pattern_matches_the_real_fulcio_san() {
        // Taken verbatim from the SAN of the v0.5.0 release certificate.
        let real =
            "https://github.com/tensorbee/waypoint/.github/workflows/release.yml@refs/tags/v0.5.0";
        assert!(matches_identity(&certificate_identity_pattern(), real));
    }

    #[test]
    fn identity_pattern_rejects_lookalike_signers() {
        let pattern = certificate_identity_pattern();
        for bad in [
            // Different owner.
            "https://github.com/attacker/waypoint/.github/workflows/release.yml@refs/tags/v9",
            // Owner prefix collision.
            "https://github.com/tensorbee-evil/waypoint/.github/workflows/release.yml@refs/tags/v9",
            // Different repo under the right owner.
            "https://github.com/tensorbee/other/.github/workflows/release.yml@refs/tags/v9",
            // Not anchored at the start.
            "https://evil.example/https://github.com/tensorbee/waypoint/.github/workflows/x.yml",
            // Non-GitHub issuer host.
            "https://gitlab.com/tensorbee/waypoint/.github/workflows/release.yml@refs/tags/v9",
        ] {
            assert!(
                !matches_identity(&pattern, bad),
                "pattern must reject {bad}"
            );
        }
    }

    #[test]
    fn identity_pattern_escapes_literal_dots() {
        // `github.com` must not be satisfiable by `githubXcom`.
        let p = certificate_identity_pattern();
        assert!(p.starts_with(r"^https://github\.com/"));
        assert!(p.contains(r"/\.github/workflows/"));
    }

    #[test]
    fn verification_levels_are_reported_distinctly() {
        // The JSON consumer has to be able to tell the two apart.
        assert_eq!(Verification::Signature.as_str(), "cosign-signature");
        assert_eq!(Verification::ChecksumOnly.as_str(), "sha256-only");
        assert_ne!(
            Verification::Signature.as_str(),
            Verification::ChecksumOnly.as_str()
        );
    }
}
