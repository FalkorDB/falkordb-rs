/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Download and caching of FalkorDB module binaries.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::provision::Platform;
use crate::{FalkorDBError, FalkorResult};

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
        super::provision::FALKORDB_VERSION,
        asset_filename
    );

    tracing::info!("Downloading FalkorDB module from: {}", download_url);

    // For now, return a placeholder error indicating this step needs HTTP client implementation
    Err(FalkorDBError::EmbeddedServerError(format!(
        "FalkorDB module download not yet implemented. Please manually download from: {}",
        download_url
    )))
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

    #[test]
    fn test_cached_module_path_all_platforms() {
        let cache_dir = PathBuf::from("/tmp/test-cache");

        // Test all supported platforms
        let platforms = [
            Platform::LinuxX64Glibc,
            Platform::LinuxArm64Glibc,
            Platform::LinuxX64Musl,
            Platform::LinuxArm64Musl,
            Platform::AmazonLinux2023X64,
            Platform::Rhel8X64,
            Platform::Rhel9X64,
            Platform::MacOSArm64,
        ];

        for platform in platforms.iter() {
            let result = cached_module_path(platform, Some(&cache_dir));
            assert!(result.is_ok(), "Failed for platform: {:?}", platform);
            let path = result.unwrap();
            assert!(path.to_string_lossy().contains("v4.18.10"));
            assert!(path.exists() == false); // Path doesn't need to exist in unit test
        }
    }

    #[test]
    fn test_has_cached_module_nonexistent() {
        let platform = Platform::LinuxX64Glibc;
        let cache_dir = PathBuf::from("/nonexistent/path");

        let result = has_cached_module(&platform, Some(&cache_dir));
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Module should not exist
    }

    #[test]
    fn test_download_falkordb_module_not_implemented() {
        let platform = Platform::LinuxX64Glibc;
        let timeout = Duration::from_secs(60);

        let result = download_falkordb_module(&platform, None, timeout);
        assert!(result.is_err()); // Should return placeholder error
        let err_msg = format!("{:?}", result);
        assert!(err_msg.contains("not yet implemented"));
    }

    #[test]
    fn test_download_falkordb_module_all_platforms() {
        let timeout = Duration::from_secs(60);
        let platforms = [
            Platform::LinuxX64Glibc,
            Platform::LinuxArm64Glibc,
            Platform::LinuxX64Musl,
            Platform::LinuxArm64Musl,
            Platform::AmazonLinux2023X64,
            Platform::Rhel8X64,
            Platform::Rhel9X64,
            Platform::MacOSArm64,
        ];

        for platform in platforms.iter() {
            let result = download_falkordb_module(platform, None, timeout);
            // All should fail with "not implemented" for now
            assert!(
                result.is_err(),
                "Expected error for platform: {:?}",
                platform
            );
        }
    }

    #[test]
    fn test_get_cache_dir_with_home_env() {
        // Test HOME directory resolution when HOME is set
        let original_home = std::env::var("HOME").ok();
        let original_cache_dir = std::env::var("FALKORDB_RS_CACHE_DIR").ok();

        // Clear cache dir env var to test HOME fallback
        std::env::remove_var("FALKORDB_RS_CACHE_DIR");
        std::env::set_var("HOME", "/test/home");

        let result = get_cache_dir(None);
        assert!(result.is_ok());
        let path = result.unwrap();

        // Should include .cache/falkordb-rs or Library/Caches/falkordb-rs depending on OS
        let path_str = path.to_string_lossy();
        assert!(
            path_str.contains("falkordb-rs"),
            "Path should contain 'falkordb-rs': {}",
            path_str
        );
        assert!(path_str.starts_with("/test/home"));

        // Restore originals
        if let Some(val) = original_home {
            std::env::set_var("HOME", val);
        } else {
            std::env::remove_var("HOME");
        }
        if let Some(val) = original_cache_dir {
            std::env::set_var("FALKORDB_RS_CACHE_DIR", val);
        }
    }

    #[test]
    fn test_get_cache_dir_fallback_to_temp() {
        // Test fallback to temp directory when HOME is not set
        let original_home = std::env::var("HOME").ok();
        let original_cache_dir = std::env::var("FALKORDB_RS_CACHE_DIR").ok();

        // Clear both HOME and cache dir env var to force temp_dir fallback
        std::env::remove_var("HOME");
        std::env::remove_var("FALKORDB_RS_CACHE_DIR");

        let result = get_cache_dir(None);
        assert!(result.is_ok());
        let path = result.unwrap();

        // Should use temp directory with falkordb-rs-cache suffix
        let path_str = path.to_string_lossy();
        assert!(
            path_str.contains("falkordb-rs-cache"),
            "Path should contain 'falkordb-rs-cache': {}",
            path_str
        );

        // Restore originals
        if let Some(val) = original_home {
            std::env::set_var("HOME", val);
        }
        if let Some(val) = original_cache_dir {
            std::env::set_var("FALKORDB_RS_CACHE_DIR", val);
        }
    }

    #[test]
    fn test_download_falkordb_module_with_cached_module() {
        // Test that download_falkordb_module returns cached path if it exists
        use std::fs;

        let temp_dir =
            std::env::temp_dir().join(format!("falkordb_test_cache_{}", std::process::id()));
        let cache_dir = &temp_dir;

        // Create the cache structure
        let platform = Platform::LinuxX64Glibc;
        let cached_path = cached_module_path(&platform, Some(cache_dir)).unwrap();

        // Create parent directories and the cached file
        let _ = fs::create_dir_all(cached_path.parent().unwrap());
        let _ = fs::File::create(&cached_path);

        // Verify the cached module exists
        if cached_path.exists() {
            // Call download_falkordb_module with the cache_dir
            let timeout = Duration::from_secs(60);
            let result = download_falkordb_module(&platform, Some(cache_dir), timeout);

            // Should return the cached path instead of trying to download
            assert!(result.is_ok());
            let returned_path = result.unwrap();
            assert_eq!(returned_path, cached_path);

            // Cleanup
            let _ = fs::remove_file(&cached_path);
            let _ = fs::remove_dir_all(&temp_dir);
        }
    }

    #[test]
    fn test_download_falkordb_module_constructs_correct_url() {
        // Test that download_falkordb_module constructs correct URL even if download not implemented
        let platform = Platform::MacOSArm64;
        let timeout = Duration::from_secs(60);

        let result = download_falkordb_module(&platform, None, timeout);

        // Should fail with "not implemented" but should include proper URL
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("github.com/FalkorDB/FalkorDB"),
            "Error message should contain GitHub URL: {}",
            err_msg
        );
        assert!(
            err_msg.contains("v4.18.10"),
            "Error message should contain version: {}",
            err_msg
        );
    }

    #[test]
    fn test_unsupported_platform_error_in_download() {
        // Test that unsupported platform returns error in download functions
        let unsupported = Platform::Unsupported;

        let result = cached_module_path(&unsupported, None);
        assert!(result.is_err());

        let result = has_cached_module(&unsupported, None);
        assert!(result.is_err());

        let result = download_falkordb_module(&unsupported, None, Duration::from_secs(60));
        assert!(result.is_err());
    }

    #[test]
    fn test_macos_x64_unsupported_in_download() {
        // Test that macOS x86_64 returns error in download functions
        let macos_x64 = Platform::MacOSX64Unsupported;

        let result = cached_module_path(&macos_x64, None);
        assert!(result.is_err());

        let result = has_cached_module(&macos_x64, None);
        assert!(result.is_err());

        let result = download_falkordb_module(&macos_x64, None, Duration::from_secs(60));
        assert!(result.is_err());
    }
}
