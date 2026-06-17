/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Download and caching of FalkorDB module binaries.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::{FalkorDBError, FalkorResult};
use super::provision::Platform;

/// Get the cache directory for FalkorDB binaries.
///
/// Uses the following resolution order:
/// 1. `FALKORDB_RS_CACHE_DIR` environment variable
/// 2. `~/.cache/falkordb-rs/` (Linux/BSD)
/// 3. `~/Library/Caches/falkordb-rs/` (macOS)
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

/// Get the path to a cached FalkorDB module binary.
pub fn cached_module_path(
    platform: &Platform,
    cache_dir: Option<&Path>,
) -> FalkorResult<PathBuf> {
    let cache_root = get_cache_dir(cache_dir)?;
    let platform_tag = platform.tag()?;
    let asset_filename = platform.asset_filename()?;

    let path = cache_root
        .join(super::provision::FALKORDB_VERSION)
        .join(platform_tag)
        .join(asset_filename);

    Ok(path)
}

/// Check if a cached FalkorDB module exists.
pub fn has_cached_module(
    platform: &Platform,
    cache_dir: Option<&Path>,
) -> FalkorResult<bool> {
    let path = cached_module_path(platform, cache_dir)?;
    Ok(path.exists())
}

/// Download FalkorDB module from GitHub releases.
pub fn download_falkordb_module(
    platform: &Platform,
    cache_dir: Option<&Path>,
    _timeout: Duration,
) -> FalkorResult<PathBuf> {
    // Check if already cached
    if let Ok(true) = has_cached_module(platform, cache_dir) {
        let cached_path = cached_module_path(platform, cache_dir)?;
        tracing::debug!("Using cached FalkorDB module at: {}", cached_path.display());
        return Ok(cached_path);
    }

    let asset_filename = platform.asset_filename()?;
    let download_url = format!(
        "https://github.com/FalkorDB/FalkorDB/releases/download/{}/{}",
        super::provision::FALKORDB_VERSION, asset_filename
    );

    tracing::info!("Downloading FalkorDB module from: {}", download_url);

    // For now, return a placeholder error indicating this step needs HTTP client implementation
    Err(FalkorDBError::EmbeddedServerError(
        format!("FalkorDB module download not yet implemented. Please manually download from: {}", download_url)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_cache_dir_with_override() {
        let override_dir = PathBuf::from("/tmp/test-cache");
        let result = get_cache_dir(Some(&override_dir));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), override_dir);
    }

    #[test]
    fn test_get_cache_dir_env_var() {
        // Set env var and verify it's used
        let original = std::env::var("FALKORDB_RS_CACHE_DIR").ok();
        std::env::set_var("FALKORDB_RS_CACHE_DIR", "/tmp/falkordb-test");
        
        let result = get_cache_dir(None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("/tmp/falkordb-test"));

        // Restore original
        if let Some(val) = original {
            std::env::set_var("FALKORDB_RS_CACHE_DIR", val);
        } else {
            std::env::remove_var("FALKORDB_RS_CACHE_DIR");
        }
    }

    #[test]
    fn test_cached_module_path() {
        let platform = Platform::LinuxX64Glibc;
        let cache_dir = PathBuf::from("/tmp/test-cache");
        
        let result = cached_module_path(&platform, Some(&cache_dir));
        assert!(result.is_ok());
        
        let path = result.unwrap();
        assert!(path.to_string_lossy().contains("v4.18.10"));
        assert!(path.to_string_lossy().contains("linux-x64-glibc"));
        assert!(path.to_string_lossy().contains("falkordb-x64.so"));
    }
}
