/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Build-time-bundled FalkorDB module (`embedded-bundle` feature).
//!
//! With `embedded-bundle`, `build.rs` fetches the module for the build target
//! and embeds it here via [`include_bytes!`]. At runtime the bytes are written
//! once to a content-addressed cache file (keyed by the module's build-time
//! SHA-256) and that path is handed to `redis-server --loadmodule`. This path
//! performs **no network access**, which is the whole point of the feature.
//!
//! No SHA-256 is computed at runtime (the feature pulls in no hashing crate):
//! the cache directory name is the build-time hash, and an existing extraction
//! is reused only when its bytes are exactly the embedded module.

use std::path::{Path, PathBuf};

use crate::{FalkorDBError, FalkorResult};

/// The FalkorDB module bytes, embedded at build time for the build target.
pub const BUNDLED_MODULE: &[u8] = include_bytes!(env!("FALKORDB_EMBEDDED_MODULE_FILE"));

/// The FalkorDB release version the bundled module was fetched from.
pub const BUNDLED_MODULE_VERSION: &str = env!("FALKORDB_EMBEDDED_MODULE_VERSION");

/// The platform tag (e.g. `linux-x64-glibc`) the bundled module targets.
pub const BUNDLED_MODULE_PLATFORM: &str = env!("FALKORDB_EMBEDDED_MODULE_PLATFORM");

/// The SHA-256 (hex) of the bundled module, computed at build time.
pub const BUNDLED_MODULE_SHA256: &str = env!("FALKORDB_EMBEDDED_MODULE_SHA256");

/// Write the embedded module to a content-addressed cache file and return its
/// path, reusing an existing extraction when present and intact.
///
/// The cache directory is `cache_dir` if given, else `FALKORDB_RS_CACHE_DIR`,
/// else the platform cache dir, else the system temp dir. The file lives at
/// `<cache>/bundled/<sha256>/falkordb.so`, so different module versions never
/// collide and an unchanged module is extracted only once.
pub fn materialize_bundled_module(cache_dir: Option<&Path>) -> FalkorResult<PathBuf> {
    let dir = cache_root(cache_dir)
        .join("bundled")
        .join(BUNDLED_MODULE_SHA256);
    let dest = dir.join("falkordb.so");

    // Reuse a prior extraction only when its bytes are exactly the embedded
    // module. The directory is keyed by the build-time content hash, but a shared
    // cache root (e.g. the world-writable temp fallback) could otherwise let
    // another local process pre-plant a same-length `.so` that redis would
    // dlopen — so verify the content, dependency-free, rather than trusting the
    // length alone.
    if extracted_matches(&dest) {
        return Ok(dest);
    }

    create_private_dir(&dir)?;
    write_atomic(&dir, &dest)?;
    Ok(dest)
}

/// Whether `dest` already holds exactly [`BUNDLED_MODULE`].
fn extracted_matches(dest: &Path) -> bool {
    file_len(dest) == Some(BUNDLED_MODULE.len() as u64)
        && std::fs::read(dest).is_ok_and(|bytes| bytes == BUNDLED_MODULE)
}

/// Create `dir` (and parents), then restrict it to the owner on Unix so other
/// local users can't pre-create the content-addressed path with a hostile file.
/// A failure to apply owner-only permissions is treated as an error: it usually
/// means the path is pre-owned by another user, which is exactly the planted
/// module we must not extract into / trust.
fn create_private_dir(dir: &Path) -> FalkorResult<()> {
    std::fs::create_dir_all(dir).map_err(|e| {
        FalkorDBError::EmbeddedServerError(format!(
            "Failed to create bundled-module cache dir {}: {e}",
            dir.display()
        ))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(|e| {
            FalkorDBError::EmbeddedServerError(format!(
                "Failed to secure bundled-module cache dir {} with owner-only permissions: {e}",
                dir.display()
            ))
        })?;
    }
    Ok(())
}

/// Length of `path` in bytes, or `None` if it does not exist / can't be stat'd.
fn file_len(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.len())
}

/// Write [`BUNDLED_MODULE`] to `dest` via a uniquely-named temp file in `dir`
/// plus an atomic rename, so a partial write is never observed and concurrent
/// first-runs don't tear each other's output.
fn write_atomic(
    dir: &Path,
    dest: &Path,
) -> FalkorResult<()> {
    use std::io::Write;

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dir.join(format!("falkordb.so.tmp.{}.{unique}", std::process::id()));

    let write_result = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(BUNDLED_MODULE)?;
        file.flush()?;
        // World-readable, owner-writable; redis only needs to read+dlopen it.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o644))?;
        }
        Ok(())
    })();

    if let Err(e) = write_result {
        let _ = std::fs::remove_file(&tmp);
        return Err(FalkorDBError::EmbeddedServerError(format!(
            "Failed to extract bundled FalkorDB module to {}: {e}",
            tmp.display()
        )));
    }

    match std::fs::rename(&tmp, dest) {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            // Lost a race with another process (or the dest already exists and is
            // intact): clean up our temp file and accept the existing extraction
            // only if its content is exactly the bundled module.
            let _ = std::fs::remove_file(&tmp);
            if extracted_matches(dest) {
                Ok(())
            } else {
                Err(FalkorDBError::EmbeddedServerError(format!(
                    "Failed to finalize bundled FalkorDB module at {}: {rename_err}",
                    dest.display()
                )))
            }
        }
    }
}

/// Resolve the cache root: explicit `cache_dir`, else `FALKORDB_RS_CACHE_DIR`,
/// else the platform cache dir, else the system temp dir.
fn cache_root(cache_dir: Option<&Path>) -> PathBuf {
    if let Some(dir) = cache_dir {
        return dir.to_path_buf();
    }
    if let Some(dir) = std::env::var_os("FALKORDB_RS_CACHE_DIR") {
        return PathBuf::from(dir);
    }
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        #[cfg(target_os = "macos")]
        let base = home.join("Library").join("Caches");
        #[cfg(not(target_os = "macos"))]
        let base = std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".cache"));
        return base.join("falkordb-rs");
    }
    std::env::temp_dir().join("falkordb-rs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundled_metadata_is_present() {
        // The build script always sets these; they must be non-empty and the
        // SHA-256 must be 64 hex chars.
        assert!(!BUNDLED_MODULE.is_empty());
        assert!(BUNDLED_MODULE_VERSION.starts_with('v'));
        assert!(!BUNDLED_MODULE_PLATFORM.is_empty());
        assert_eq!(BUNDLED_MODULE_SHA256.len(), 64);
        assert!(BUNDLED_MODULE_SHA256.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_materialize_extracts_and_reuses() {
        let tmp = std::env::temp_dir().join(format!("falkordb-bundle-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let first = materialize_bundled_module(Some(&tmp)).expect("extract");
        assert!(first.exists());
        assert_eq!(
            std::fs::metadata(&first).unwrap().len(),
            BUNDLED_MODULE.len() as u64
        );
        assert_eq!(std::fs::read(&first).unwrap(), BUNDLED_MODULE);

        // Second call reuses the same path without rewriting.
        let second = materialize_bundled_module(Some(&tmp)).expect("reuse");
        assert_eq!(first, second);

        // A truncated cache file is repaired on the next call.
        std::fs::write(&first, b"corrupt").unwrap();
        let third = materialize_bundled_module(Some(&tmp)).expect("repair");
        assert_eq!(third, first);
        assert_eq!(std::fs::read(&third).unwrap(), BUNDLED_MODULE);

        // A same-length but different file is NOT trusted: it is re-extracted, so
        // a planted module at the content-addressed path can't be loaded.
        std::fs::write(&first, vec![0u8; BUNDLED_MODULE.len()]).unwrap();
        let repaired = materialize_bundled_module(Some(&tmp)).expect("re-extract on tamper");
        assert_eq!(std::fs::read(&repaired).unwrap(), BUNDLED_MODULE);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_cache_root_prefers_explicit() {
        let explicit = PathBuf::from("/tmp/explicit-cache");
        assert_eq!(cache_root(Some(&explicit)), explicit);
    }

    // Serializes env-mutating tests so they don't race the parallel runner.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Snapshots an env var and restores it on drop, even if the test panics.
    struct EnvVarGuard {
        key: &'static str,
        original: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn capture(key: &'static str) -> Self {
            Self {
                key,
                original: std::env::var_os(key),
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn test_cache_root_env_and_home_fallbacks() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Guards restore the originals on drop, even on panic.
        let _cache = EnvVarGuard::capture("FALKORDB_RS_CACHE_DIR");
        let _home = EnvVarGuard::capture("HOME");
        let _xdg = EnvVarGuard::capture("XDG_CACHE_HOME");

        // `FALKORDB_RS_CACHE_DIR` is used when no explicit dir is given.
        std::env::set_var("FALKORDB_RS_CACHE_DIR", "/custom/cache");
        assert_eq!(cache_root(None), PathBuf::from("/custom/cache"));

        // Otherwise fall back to the platform cache directory under `HOME`.
        std::env::remove_var("FALKORDB_RS_CACHE_DIR");
        std::env::remove_var("XDG_CACHE_HOME");
        std::env::set_var("HOME", "/test/home");
        let path = cache_root(None);
        assert!(
            path.to_string_lossy().contains("falkordb-rs"),
            "got {path:?}"
        );
        assert!(path.starts_with("/test/home"), "got {path:?}");

        // With neither variable nor HOME set, fall back to the system temp dir.
        std::env::remove_var("HOME");
        let path = cache_root(None);
        assert!(path.ends_with("falkordb-rs"), "got {path:?}");
    }

    #[test]
    fn test_materialize_errors_when_cache_path_is_a_file() {
        // A regular file where a directory is expected makes the cache-dir
        // creation fail, surfacing an actionable error.
        let file =
            std::env::temp_dir().join(format!("falkordb-bundle-file-{}", std::process::id()));
        std::fs::write(&file, b"not a dir").unwrap();
        let err = materialize_bundled_module(Some(&file))
            .expect_err("materialize should fail when the cache path is a file");
        assert!(
            err.to_string().contains("cache dir") || err.to_string().contains("Failed"),
            "unexpected error: {err}"
        );
        let _ = std::fs::remove_file(&file);
    }
}
