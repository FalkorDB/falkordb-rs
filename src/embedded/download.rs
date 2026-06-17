/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Download and caching of FalkorDB module binaries.
//!
//! Binaries are fetched over HTTPS from the official FalkorDB GitHub releases,
//! verified against the pinned SHA-256 checksum (see [`super::provision`]) and
//! installed atomically into a per-version, per-platform cache directory so that
//! concurrent processes and subsequent runs reuse the same file safely.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::provision::{self, Platform};
use crate::{FalkorDBError, FalkorResult};

/// Upper bound on a downloaded module to guard against a runaway/hostile server.
/// The largest real (non-debug) asset is ~55 MiB; 512 MiB is comfortably above.
const MAX_DOWNLOAD_BYTES: u64 = 512 * 1024 * 1024;

/// Counter for unique temp filenames within a single process.
static DOWNLOAD_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Get the cache directory for FalkorDB binaries.
///
/// Uses the following resolution order:
/// 1. Explicit `override_dir`
/// 2. `FALKORDB_RS_CACHE_DIR` environment variable
/// 3. `~/Library/Caches/falkordb-rs/` (macOS) or `~/.cache/falkordb-rs/` (other)
/// 4. System temp directory as fallback
fn get_cache_dir(override_dir: Option<&Path>) -> FalkorResult<PathBuf> {
    if let Some(dir) = override_dir {
        return Ok(dir.to_path_buf());
    }

    if let Ok(dir) = std::env::var("FALKORDB_RS_CACHE_DIR") {
        return Ok(PathBuf::from(dir));
    }

    #[cfg(target_os = "macos")]
    {
        if let Ok(home) = std::env::var("HOME") {
            return Ok(PathBuf::from(home).join("Library/Caches/falkordb-rs"));
        }
    }

    #[cfg(not(target_os = "macos"))]
    {
        if let Ok(home) = std::env::var("HOME") {
            return Ok(PathBuf::from(home).join(".cache/falkordb-rs"));
        }
    }

    Ok(std::env::temp_dir().join("falkordb-rs-cache"))
}

/// Validate a user-supplied version string before it is used as a cache path
/// segment or in the download URL.
///
/// Rejects empty/oversized values and anything outside `[A-Za-z0-9._-]` (in
/// particular path separators) or containing a `..` traversal sequence, so a
/// crafted `falkordb_version` cannot escape the cache root or corrupt the URL.
fn validate_version(version: &str) -> FalkorResult<()> {
    let safe = !version.is_empty()
        && version.len() <= 64
        && version != "."
        && !version.contains("..")
        && version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if safe {
        Ok(())
    } else {
        Err(FalkorDBError::EmbeddedServerError(format!(
            "Invalid FalkorDB version '{version}': only ASCII letters, digits, '.', '_' and '-' \
             are allowed (no path separators or '..')."
        )))
    }
}

/// Get the path a cached FalkorDB module would occupy for `version`/`platform`.
pub fn cached_module_path(
    platform: &Platform,
    version: &str,
    cache_dir: Option<&Path>,
) -> FalkorResult<PathBuf> {
    validate_version(version)?;
    let cache_root = get_cache_dir(cache_dir)?;
    // Evaluate `asset_filename()` before `tag()`: for unsupported platforms it
    // carries the richer, more actionable error (supported platforms / arm64
    // guidance), which would otherwise be masked by `tag()`'s short message.
    let asset_filename = platform.asset_filename()?;
    let platform_tag = platform.tag()?;

    Ok(cache_root
        .join(version)
        .join(platform_tag)
        .join(asset_filename))
}

/// Build the GitHub release download URL for a module asset.
fn module_url(
    platform: &Platform,
    version: &str,
) -> FalkorResult<String> {
    validate_version(version)?;
    Ok(format!(
        "https://github.com/FalkorDB/FalkorDB/releases/download/{}/{}",
        version,
        platform.asset_filename()?
    ))
}

/// Compute the lowercase-hex SHA-256 of a file's contents.
fn sha256_hex_of_file(path: &Path) -> FalkorResult<String> {
    use sha2::{Digest, Sha256};

    let mut file = std::fs::File::open(path).map_err(|e| {
        FalkorDBError::EmbeddedServerError(format!(
            "Failed to open {} for checksum: {}",
            path.display(),
            e
        ))
    })?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buf).map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!(
                "Failed reading {} for checksum: {}",
                path.display(),
                e
            ))
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hex_encode(hasher.finalize().as_slice()))
}

/// Return the cached module path only if it exists **and**, when a checksum is
/// pinned for `version`, its contents match that checksum.
///
/// A cached file that fails verification is deleted and reported as a cache miss
/// (`Ok(None)`) so the caller re-downloads a clean copy — this keeps the
/// integrity guarantee on cache hits, not just on the original download.
pub fn cached_verified_module(
    platform: &Platform,
    version: &str,
    cache_dir: Option<&Path>,
) -> FalkorResult<Option<PathBuf>> {
    let path = cached_module_path(platform, version, cache_dir)?;
    if !path.exists() {
        return Ok(None);
    }
    if let Some(expected) = provision::module_checksum(version, platform.tag()?) {
        if !sha256_hex_of_file(&path)?.eq_ignore_ascii_case(expected) {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "Cached module at {} failed checksum verification; removing it",
                path.display()
            );
            let _ = std::fs::remove_file(&path);
            return Ok(None);
        }
    }
    Ok(Some(path))
}

/// Download (or reuse the cache for) the FalkorDB module for `platform`/`version`.
///
/// On a cache miss the asset is streamed from the official GitHub release,
/// verified against the pinned SHA-256 checksum when one is known for `version`,
/// and atomically moved into the cache. Returns the path to the cached module.
pub fn download_falkordb_module(
    platform: &Platform,
    version: &str,
    cache_dir: Option<&Path>,
    timeout: Duration,
) -> FalkorResult<PathBuf> {
    let final_path = cached_module_path(platform, version, cache_dir)?;
    if let Some(path) = cached_verified_module(platform, version, cache_dir)? {
        #[cfg(feature = "tracing")]
        tracing::debug!("Using cached FalkorDB module at: {}", path.display());
        return Ok(path);
    }

    let url = module_url(platform, version)?;
    #[cfg(feature = "tracing")]
    tracing::info!("Downloading FalkorDB module from: {}", url);

    install_from_url(&url, platform, version, &final_path, timeout)
}

/// Download `url`, verify it, and atomically install it at `final_path`.
///
/// Split out from [`download_falkordb_module`] (which derives `url` from the
/// official release) so the full download/verify/atomic-install path can be
/// exercised offline in tests against a local server.
fn install_from_url(
    url: &str,
    platform: &Platform,
    version: &str,
    final_path: &Path,
    timeout: Duration,
) -> FalkorResult<PathBuf> {
    let parent = final_path.parent().ok_or_else(|| {
        FalkorDBError::EmbeddedServerError("Invalid cache path (no parent directory)".to_string())
    })?;
    fs_create_dir_all(parent)?;

    // Unique, hard-to-predict temp name in the target dir. Combined with the
    // O_EXCL open below (`create_new`), this resists symlink pre-creation races
    // when the cache dir is shared (e.g. a custom FALKORDB_RS_CACHE_DIR in /tmp).
    let counter = DOWNLOAD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let temp_path = parent.join(format!(
        ".{}.{}.{}.{}.part",
        platform.asset_filename()?,
        std::process::id(),
        counter,
        nonce
    ));

    // Fetch+verify into a temp file; clean it up on any failure.
    if let Err(err) = download_and_verify(
        url,
        platform,
        version,
        &temp_path,
        timeout,
        MAX_DOWNLOAD_BYTES,
    ) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(err);
    }

    // Atomic publish: rename within the same directory (same filesystem).
    std::fs::rename(&temp_path, final_path).map_err(|e| {
        let _ = std::fs::remove_file(&temp_path);
        FalkorDBError::EmbeddedServerError(format!(
            "Failed to install downloaded module into cache at {}: {}",
            final_path.display(),
            e
        ))
    })?;
    set_module_permissions(final_path)?;

    #[cfg(feature = "tracing")]
    tracing::info!("Cached FalkorDB module at: {}", final_path.display());
    Ok(final_path.to_path_buf())
}

/// Stream `url` into `temp_path`, hashing as it goes, and verify the digest
/// against the pinned checksum for `version` (when known). `max_bytes` caps the
/// amount read to guard against a runaway/hostile server.
fn download_and_verify(
    url: &str,
    platform: &Platform,
    version: &str,
    temp_path: &Path,
    timeout: Duration,
    max_bytes: u64,
) -> FalkorResult<()> {
    use sha2::{Digest, Sha256};

    let response = ureq::AgentBuilder::new()
        .timeout_connect(timeout.min(Duration::from_secs(30)))
        .timeout(timeout)
        .build()
        .get(url)
        .call()
        .map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!(
                "Failed to download FalkorDB module from {url}: {e}"
            ))
        })?;

    let mut reader = response.into_reader();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)
        .map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!(
                "Failed to create temp file {}: {}",
                temp_path.display(),
                e
            ))
        })?;

    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total: u64 = 0;
    loop {
        let read = reader.read(&mut buf).map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!("Failed while downloading {url}: {e}"))
        })?;
        if read == 0 {
            break;
        }
        total += read as u64;
        if total > max_bytes {
            return Err(FalkorDBError::EmbeddedServerError(format!(
                "Download from {url} exceeded the {max_bytes} byte limit"
            )));
        }
        hasher.update(&buf[..read]);
        file.write_all(&buf[..read]).map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!(
                "Failed writing module to {}: {}",
                temp_path.display(),
                e
            ))
        })?;
    }
    file.flush().map_err(|e| {
        FalkorDBError::EmbeddedServerError(format!("Failed flushing downloaded module: {e}"))
    })?;

    let actual = hex_encode(hasher.finalize().as_slice());
    verify_checksum(
        provision::module_checksum(version, platform.tag()?),
        &actual,
        platform.asset_filename()?,
        version,
    )
}

/// Compare a downloaded asset's `actual` digest to its `expected` pinned digest.
///
/// `expected` is `None` for versions without a pinned checksum (an overridden
/// version), in which case the asset is accepted as-is.
fn verify_checksum(
    expected: Option<&str>,
    actual: &str,
    asset: &str,
    version: &str,
) -> FalkorResult<()> {
    match expected {
        Some(expected) if !actual.eq_ignore_ascii_case(expected) => {
            Err(FalkorDBError::EmbeddedServerError(format!(
                "Checksum mismatch for {asset}: expected {expected}, got {actual}. \
                 The download may be corrupted or tampered with."
            )))
        }
        Some(_) => Ok(()),
        None => {
            // No pinned checksum for this (overridden) version; accept as-is.
            #[cfg(feature = "tracing")]
            tracing::warn!("No pinned checksum for version {version}; skipping integrity check");
            let _ = version;
            Ok(())
        }
    }
}

/// Create a directory tree, with a friendly embedded-server error on failure.
fn fs_create_dir_all(dir: &Path) -> FalkorResult<()> {
    std::fs::create_dir_all(dir).map_err(|e| {
        FalkorDBError::EmbeddedServerError(format!(
            "Failed to create cache directory {}: {}",
            dir.display(),
            e
        ))
    })
}

/// Make the cached module group/other-readable on Unix (loaded by redis-server).
fn set_module_permissions(path: &Path) -> FalkorResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!(
                "Failed to set permissions on {}: {}",
                path.display(),
                e
            ))
        })?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Lowercase hex encoding (avoids pulling in a hex crate for one call site).
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::net::TcpListener;
    use std::sync::Mutex;

    /// Serializes tests that mutate process-global environment variables
    /// (`HOME`, `FALKORDB_RS_CACHE_DIR`) so they don't race under the parallel
    /// test runner.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn unique_temp(name: &str) -> PathBuf {
        let n = DOWNLOAD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "falkordb_test_{}_{}_{}",
            std::process::id(),
            n,
            name
        ))
    }

    /// Minimal one-shot HTTP/1.1 server that returns `body` for a single
    /// request, so the real `ureq` download path can be exercised offline.
    fn serve_once(body: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut scratch = [0u8; 1024];
                let _ = stream.read(&mut scratch); // consume request headers
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        format!("http://127.0.0.1:{port}/falkordb-x64.so")
    }

    #[test]
    fn test_get_cache_dir_with_override() {
        let override_dir = PathBuf::from("/tmp/test-cache");
        let result = get_cache_dir(Some(&override_dir));
        assert_eq!(result.unwrap(), override_dir);
    }

    #[test]
    fn test_get_cache_dir_env_var() {
        let _guard = env_guard();
        let original = std::env::var("FALKORDB_RS_CACHE_DIR").ok();
        std::env::set_var("FALKORDB_RS_CACHE_DIR", "/tmp/falkordb-test");

        let result = get_cache_dir(None);
        assert_eq!(result.unwrap(), PathBuf::from("/tmp/falkordb-test"));

        match original {
            Some(val) => std::env::set_var("FALKORDB_RS_CACHE_DIR", val),
            None => std::env::remove_var("FALKORDB_RS_CACHE_DIR"),
        }
    }

    #[test]
    fn test_get_cache_dir_with_home_env() {
        let _guard = env_guard();
        let original_home = std::env::var("HOME").ok();
        let original_cache = std::env::var("FALKORDB_RS_CACHE_DIR").ok();

        std::env::remove_var("FALKORDB_RS_CACHE_DIR");
        std::env::set_var("HOME", "/test/home");

        let path = get_cache_dir(None).unwrap();
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("falkordb-rs"), "got {path_str}");
        assert!(path_str.starts_with("/test/home"), "got {path_str}");

        match original_home {
            Some(val) => std::env::set_var("HOME", val),
            None => std::env::remove_var("HOME"),
        }
        if let Some(val) = original_cache {
            std::env::set_var("FALKORDB_RS_CACHE_DIR", val);
        }
    }

    #[test]
    fn test_get_cache_dir_fallback_to_temp() {
        let _guard = env_guard();
        let original_home = std::env::var("HOME").ok();
        let original_cache = std::env::var("FALKORDB_RS_CACHE_DIR").ok();

        std::env::remove_var("HOME");
        std::env::remove_var("FALKORDB_RS_CACHE_DIR");

        let path = get_cache_dir(None).unwrap();
        assert!(
            path.to_string_lossy().contains("falkordb-rs-cache"),
            "got {}",
            path.display()
        );

        if let Some(val) = original_home {
            std::env::set_var("HOME", val);
        }
        if let Some(val) = original_cache {
            std::env::set_var("FALKORDB_RS_CACHE_DIR", val);
        }
    }

    #[test]
    fn test_cached_module_path() {
        let cache_dir = PathBuf::from("/tmp/test-cache");
        let path = cached_module_path(
            &Platform::LinuxX64Glibc,
            provision::FALKORDB_VERSION,
            Some(&cache_dir),
        )
        .unwrap();
        let s = path.to_string_lossy();
        assert!(s.contains("v4.18.10"));
        assert!(s.contains("linux-x64-glibc"));
        assert!(s.contains("falkordb-x64.so"));
    }

    #[test]
    fn test_cached_module_path_honors_version_override() {
        let cache_dir = PathBuf::from("/tmp/test-cache");
        let path = cached_module_path(&Platform::MacOSArm64, "v9.9.9", Some(&cache_dir)).unwrap();
        let s = path.to_string_lossy();
        assert!(s.contains("v9.9.9"), "version not threaded into path: {s}");
        assert!(!s.contains("v4.18.10"));
    }

    #[test]
    fn test_cached_module_path_all_platforms() {
        let cache_dir = PathBuf::from("/tmp/test-cache");
        for platform in [
            Platform::LinuxX64Glibc,
            Platform::LinuxArm64Glibc,
            Platform::LinuxX64Musl,
            Platform::LinuxArm64Musl,
            Platform::AmazonLinux2023X64,
            Platform::Rhel8X64,
            Platform::Rhel9X64,
            Platform::MacOSArm64,
        ] {
            let path = cached_module_path(&platform, provision::FALKORDB_VERSION, Some(&cache_dir))
                .unwrap_or_else(|_| panic!("path failed for {platform:?}"));
            assert!(path.to_string_lossy().contains("v4.18.10"));
            assert!(!path.exists());
        }
    }

    #[test]
    fn test_cached_verified_module_absent() {
        let result = cached_verified_module(
            &Platform::LinuxX64Glibc,
            provision::FALKORDB_VERSION,
            Some(Path::new("/nonexistent/path")),
        );
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_module_url() {
        let url = module_url(&Platform::MacOSArm64, provision::FALKORDB_VERSION).unwrap();
        assert_eq!(
            url,
            "https://github.com/FalkorDB/FalkorDB/releases/download/v4.18.10/falkordb-macos-arm64v8.so"
        );
        // Unsupported platforms have no asset and thus no URL.
        assert!(module_url(&Platform::Unsupported, provision::FALKORDB_VERSION).is_err());
    }

    #[test]
    fn test_validate_version() {
        // Realistic versions are accepted.
        for ok in ["v4.18.10", "v0.0.0-test", "4.18.10", "v4.18.10-rc.1"] {
            assert!(validate_version(ok).is_ok(), "{ok} should be valid");
        }
        // Path traversal / separators / empty / oversized are rejected.
        for bad in [
            "",
            "..",
            "../../tmp",
            "v1/../../etc",
            "a/b",
            "a\\b",
            ".",
            "v1 2",
            "with space",
        ] {
            assert!(validate_version(bad).is_err(), "{bad:?} should be rejected");
        }
        assert!(
            validate_version(&"v".repeat(65)).is_err(),
            "oversized rejected"
        );
    }

    #[test]
    fn test_cached_module_path_rejects_unsafe_version() {
        // A traversal version must not produce a path (it is rejected up front).
        let result = cached_module_path(&Platform::LinuxX64Glibc, "../../etc", None);
        assert!(result.is_err());
        assert!(
            format!("{}", result.unwrap_err()).contains("Invalid FalkorDB version"),
            "should report an invalid version"
        );
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0x00, 0x0f, 0xff]), "000fff");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn test_verify_checksum() {
        // Matching pinned checksum (case-insensitive) → accepted.
        assert!(verify_checksum(Some("abc123"), "ABC123", "asset.so", "v1").is_ok());
        // Mismatching pinned checksum → rejected.
        let err = verify_checksum(Some("abc123"), "deadbeef", "asset.so", "v1").unwrap_err();
        assert!(format!("{err}").contains("Checksum mismatch"), "got: {err}");
        // No pinned checksum (overridden version) → accepted as-is.
        assert!(verify_checksum(None, "anything", "asset.so", "v1").is_ok());
    }

    #[test]
    fn test_unsupported_platforms_error() {
        for platform in [Platform::Unsupported, Platform::MacOSX64Unsupported] {
            assert!(cached_module_path(&platform, provision::FALKORDB_VERSION, None).is_err());
            assert!(cached_verified_module(&platform, provision::FALKORDB_VERSION, None).is_err());
            assert!(download_falkordb_module(
                &platform,
                provision::FALKORDB_VERSION,
                None,
                Duration::from_secs(1)
            )
            .is_err());
        }

        // The richer `asset_filename()` guidance surfaces (not `tag()`'s short message).
        let unsupported_msg = format!(
            "{}",
            cached_module_path(&Platform::Unsupported, provision::FALKORDB_VERSION, None)
                .unwrap_err()
        );
        assert!(
            unsupported_msg.contains("Supported platforms"),
            "got: {unsupported_msg}"
        );
        let macos_msg = format!(
            "{}",
            cached_module_path(
                &Platform::MacOSX64Unsupported,
                provision::FALKORDB_VERSION,
                None
            )
            .unwrap_err()
        );
        assert!(
            macos_msg.contains("aarch64") || macos_msg.contains("arm64"),
            "got: {macos_msg}"
        );
    }

    #[test]
    fn test_download_returns_cached_without_network() {
        // Pre-populate the cache; download must short-circuit (no network). An
        // unpinned version is used so the fake bytes aren't checksum-rejected.
        let cache_dir = unique_temp("cachehit");
        let platform = Platform::LinuxX64Glibc;
        let version = "v0.0.0-cachehit";
        let cached = cached_module_path(&platform, version, Some(&cache_dir)).unwrap();
        fs::create_dir_all(cached.parent().unwrap()).unwrap();
        fs::write(&cached, b"cached module").unwrap();

        let returned =
            download_falkordb_module(&platform, version, Some(&cache_dir), Duration::from_secs(1))
                .unwrap();
        assert_eq!(returned, cached);

        let _ = fs::remove_dir_all(&cache_dir);
    }

    #[test]
    fn test_cached_verified_module_rejects_bad_pinned_checksum() {
        // A cached file for the PINNED version that doesn't match its pinned
        // checksum must be rejected (and deleted) rather than trusted.
        let cache_dir = unique_temp("badcache");
        let platform = Platform::LinuxX64Glibc;
        let cached =
            cached_module_path(&platform, provision::FALKORDB_VERSION, Some(&cache_dir)).unwrap();
        fs::create_dir_all(cached.parent().unwrap()).unwrap();
        fs::write(&cached, b"tampered bytes").unwrap();

        let result =
            cached_verified_module(&platform, provision::FALKORDB_VERSION, Some(&cache_dir))
                .unwrap();
        assert!(result.is_none(), "bad pinned cache should be rejected");
        assert!(!cached.exists(), "rejected cache file should be removed");

        let _ = fs::remove_dir_all(&cache_dir);
    }

    #[test]
    fn test_cached_verified_module_accepts_unpinned_version() {
        // Without a pinned checksum (overridden version) any cached file is
        // accepted as-is.
        let cache_dir = unique_temp("unpinnedcache");
        let platform = Platform::LinuxX64Glibc;
        let version = "v0.0.0-unpinned";
        let cached = cached_module_path(&platform, version, Some(&cache_dir)).unwrap();
        fs::create_dir_all(cached.parent().unwrap()).unwrap();
        fs::write(&cached, b"whatever").unwrap();

        let result = cached_verified_module(&platform, version, Some(&cache_dir)).unwrap();
        assert_eq!(result, Some(cached));

        let _ = fs::remove_dir_all(&cache_dir);
    }

    #[test]
    fn test_sha256_hex_of_file() {
        // SHA-256 of the empty input is a well-known constant.
        let path = unique_temp("empty");
        fs::write(&path, b"").unwrap();
        assert_eq!(
            sha256_hex_of_file(&path).unwrap(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_sha256_hex_of_file_missing() {
        // Hashing a path that doesn't exist surfaces an actionable error.
        let err = sha256_hex_of_file(Path::new("/nonexistent/falkordb/module.so")).unwrap_err();
        assert!(format!("{err}").contains("for checksum"), "got: {err}");
    }

    #[test]
    fn test_sha256_hex_of_file_unreadable() {
        // A directory opens but fails to read (EISDIR), exercising the read
        // error path of the streaming hasher.
        let dir = unique_temp("hashdir");
        fs::create_dir_all(&dir).unwrap();
        let err = sha256_hex_of_file(&dir).unwrap_err();
        assert!(
            format!("{err}").contains("reading") || format!("{err}").contains("for checksum"),
            "got: {err}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn test_install_from_url_temp_create_error() {
        use std::os::unix::fs::PermissionsExt;
        // root bypasses directory write permissions, so this check is meaningless then.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let cache_dir = unique_temp("readonly");
        let platform = Platform::LinuxX64Glibc;
        let version = "v0.0.0-ro";
        let final_path = cached_module_path(&platform, version, Some(&cache_dir)).unwrap();
        let parent = final_path.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        // Read+execute but not writable, so creating the temp file fails.
        fs::set_permissions(parent, fs::Permissions::from_mode(0o500)).unwrap();

        let url = serve_once(b"bytes".to_vec());
        let err = install_from_url(
            &url,
            &platform,
            version,
            &final_path,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("Failed to create temp file"),
            "got: {err}"
        );

        let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
        let _ = fs::remove_dir_all(&cache_dir);
    }

    /// Reserve then release a local port so connecting to it is refused.
    fn closed_port_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("http://127.0.0.1:{port}/falkordb-x64.so")
    }

    #[test]
    fn test_download_and_verify_connection_error() {
        // A failed HTTP request is mapped to an embedded-server error.
        let url = closed_port_url();
        let temp = unique_temp("connerr.part");
        let err = download_and_verify(
            &url,
            &Platform::LinuxX64Glibc,
            "v0.0.0-conn",
            &temp,
            Duration::from_secs(2),
            MAX_DOWNLOAD_BYTES,
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("Failed to download"),
            "got: {err}"
        );
        let _ = fs::remove_file(&temp);
    }

    #[test]
    fn test_install_from_url_cleans_up_on_failure() {
        // On a download failure nothing is left behind: no installed module and
        // no stray temp/.part file in the cache dir.
        let url = closed_port_url();
        let cache_dir = unique_temp("installfail");
        let platform = Platform::LinuxX64Glibc;
        let version = "v0.0.0-fail";
        let final_path = cached_module_path(&platform, version, Some(&cache_dir)).unwrap();

        let result = install_from_url(
            &url,
            &platform,
            version,
            &final_path,
            Duration::from_secs(2),
        );
        assert!(result.is_err());
        assert!(
            !final_path.exists(),
            "no module should be installed on failure"
        );
        let leftover = fs::read_dir(final_path.parent().unwrap())
            .map(|rd| {
                rd.filter_map(Result::ok)
                    .any(|e| e.file_name().to_string_lossy().ends_with(".part"))
            })
            .unwrap_or(false);
        assert!(!leftover, "temp .part file should be cleaned up on failure");

        let _ = fs::remove_dir_all(&cache_dir);
    }

    #[test]
    fn test_install_from_url_create_dir_error() {
        // When the cache parent can't be created (here it sits under a regular
        // file), installation fails with an actionable error.
        let blocker = unique_temp("blocker-file");
        fs::write(&blocker, b"i am a file, not a directory").unwrap();
        let final_path = blocker
            .join("v0.0.0")
            .join("linux-x64-glibc")
            .join("falkordb-x64.so");

        let err = install_from_url(
            "http://127.0.0.1:1/falkordb-x64.so",
            &Platform::LinuxX64Glibc,
            "v0.0.0",
            &final_path,
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("Failed to create cache directory"),
            "got: {err}"
        );
        let _ = fs::remove_file(&blocker);
    }

    #[test]
    fn test_download_and_verify_detects_checksum_mismatch() {
        // A known platform/version has a pinned checksum; serving the wrong
        // bytes must be rejected.
        let url = serve_once(b"definitely not the real module".to_vec());
        let temp = unique_temp("mismatch.part");
        let err = download_and_verify(
            &url,
            &Platform::LinuxX64Glibc,
            provision::FALKORDB_VERSION,
            &temp,
            Duration::from_secs(10),
            MAX_DOWNLOAD_BYTES,
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("Checksum mismatch"),
            "unexpected error: {err}"
        );
        let _ = fs::remove_file(&temp);
    }

    #[test]
    fn test_download_and_verify_skips_unknown_version() {
        // An unknown (overridden) version has no pinned checksum, so the body
        // is accepted and written verbatim.
        let body = b"arbitrary module bytes".to_vec();
        let url = serve_once(body.clone());
        let temp = unique_temp("unknown.part");
        download_and_verify(
            &url,
            &Platform::LinuxX64Glibc,
            "v0.0.0-test",
            &temp,
            Duration::from_secs(10),
            MAX_DOWNLOAD_BYTES,
        )
        .expect("unknown version should skip checksum verification");
        assert_eq!(fs::read(&temp).unwrap(), body);
        let _ = fs::remove_file(&temp);
    }

    #[test]
    fn test_download_and_verify_enforces_size_limit() {
        // A body larger than the allowed maximum is rejected.
        let url = serve_once(vec![0u8; 4096]);
        let temp = unique_temp("toolarge.part");
        let err = download_and_verify(
            &url,
            &Platform::LinuxX64Glibc,
            "v0.0.0-big",
            &temp,
            Duration::from_secs(10),
            1024, // 1 KiB cap, body is 4 KiB
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("exceeded the 1024 byte limit"),
            "got: {err}"
        );
        let _ = fs::remove_file(&temp);
    }

    #[test]
    fn test_download_full_install_and_cache_reuse() {
        // End-to-end against a local server: install_from_url downloads, verifies
        // and atomically installs into the cache; download_falkordb_module then
        // reuses it without contacting a server. Uses an unpinned version so no
        // real checksum is required.
        let body = b"local module payload".to_vec();
        let url = serve_once(body.clone());
        let cache_dir = unique_temp("install");
        let platform = Platform::LinuxX64Glibc;
        let version = "v0.0.0-local";
        let final_path = cached_module_path(&platform, version, Some(&cache_dir)).unwrap();

        // Exercise the real install path (temp file + verify + atomic rename + perms).
        let installed = install_from_url(
            &url,
            &platform,
            version,
            &final_path,
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(installed, final_path);
        assert_eq!(fs::read(&final_path).unwrap(), body);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&final_path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o644);
        }
        // No leftover temp/part files in the cache dir.
        let leftover = fs::read_dir(final_path.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .any(|e| e.file_name().to_string_lossy().ends_with(".part"));
        assert!(!leftover, "temp .part file was not cleaned up");

        // Now download_falkordb_module must reuse it without contacting a server.
        let reused =
            download_falkordb_module(&platform, version, Some(&cache_dir), Duration::from_secs(1))
                .unwrap();
        assert_eq!(reused, final_path);

        let _ = fs::remove_dir_all(&cache_dir);
    }

    #[cfg(unix)]
    #[test]
    fn test_set_module_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let path = unique_temp("perms.so");
        fs::write(&path, b"x").unwrap();
        set_module_permissions(&path).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o644);
        let _ = fs::remove_file(&path);
    }
}
