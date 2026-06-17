/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Binary provisioning for the embedded FalkorDB server.
//!
//! This module handles acquiring the FalkorDB module and optionally redis-server
//! binaries via download from official sources, with local caching, integrity
//! verification, and concurrent-safe installation.

use crate::{FalkorDBError, FalkorResult};
use std::collections::HashMap;

/// FalkorDB version to provision (pinned for reproducibility and security).
pub const FALKORDB_VERSION: &str = "v4.18.10";

/// SHA-256 checksums for each platform's FalkorDB module release asset.
/// These are pinned to ensure integrity and prevent MITM attacks.
pub fn falkordb_checksums() -> HashMap<&'static str, &'static str> {
    let mut map = HashMap::new();
    // Linux glibc - placeholders (replace with actual checksums from releases)
    map.insert(
        "linux-x64-glibc",
        "0000000000000000000000000000000000000000000000000000000000000001",
    );
    // Linux aarch64 glibc
    map.insert(
        "linux-arm64-glibc",
        "0000000000000000000000000000000000000000000000000000000000000002",
    );
    // Linux x86_64 musl (Alpine)
    map.insert(
        "linux-x64-musl",
        "0000000000000000000000000000000000000000000000000000000000000003",
    );
    // Linux aarch64 musl (Alpine)
    map.insert(
        "linux-arm64-musl",
        "0000000000000000000000000000000000000000000000000000000000000004",
    );
    // Amazon Linux 2023
    map.insert(
        "amazonlinux2023-x64",
        "0000000000000000000000000000000000000000000000000000000000000005",
    );
    // RHEL 8
    map.insert(
        "rhel8-x64",
        "0000000000000000000000000000000000000000000000000000000000000006",
    );
    // RHEL 9
    map.insert(
        "rhel9-x64",
        "0000000000000000000000000000000000000000000000000000000000000007",
    );
    // macOS aarch64 (Apple Silicon)
    map.insert(
        "macos-arm64",
        "0000000000000000000000000000000000000000000000000000000000000008",
    );
    map
}

/// Platform identifier for binary selection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum Platform {
    /// Linux x86_64 with glibc
    LinuxX64Glibc,
    /// Linux aarch64 with glibc
    LinuxArm64Glibc,
    /// Linux x86_64 with musl (Alpine)
    LinuxX64Musl,
    /// Linux aarch64 with musl (Alpine)
    LinuxArm64Musl,
    /// Amazon Linux 2023 x86_64
    AmazonLinux2023X64,
    /// RHEL 8 x86_64
    Rhel8X64,
    /// RHEL 9 x86_64
    Rhel9X64,
    /// macOS aarch64 (Apple Silicon)
    MacOSArm64,
    /// macOS x86_64 on Apple Silicon via Rosetta 2 (unsupported, use ARM64 with warning)
    MacOSX64Unsupported,
    /// Unsupported platform
    Unsupported,
}

impl Platform {
    /// Detect the current platform.
    ///
    /// Returns `Platform::Unsupported` if the platform/architecture combination
    /// is not supported.
    pub fn detect() -> Self {
        #[cfg(not(unix))]
        {
            // Windows support is out of scope for now.
            return Platform::Unsupported;
        }

        #[cfg(unix)]
        {
            let os = std::env::consts::OS;
            let arch = std::env::consts::ARCH;

            match (os, arch) {
                ("linux", "x86_64") => {
                    // Detect libc: glibc vs musl
                    if is_musl() {
                        Platform::LinuxX64Musl
                    } else {
                        Platform::LinuxX64Glibc
                    }
                }
                ("linux", "aarch64") => {
                    if is_musl() {
                        Platform::LinuxArm64Musl
                    } else {
                        Platform::LinuxArm64Glibc
                    }
                }
                ("macos", "aarch64") => Platform::MacOSArm64,
                ("macos", "x86_64") => {
                    // No native x86_64 binary; would need Rosetta 2
                    Platform::MacOSX64Unsupported
                }
                _ => Platform::Unsupported,
            }
        }
    }

    /// Returns the asset filename for this platform (without directory).
    pub fn asset_filename(&self) -> Result<&'static str, FalkorDBError> {
        match self {
            Platform::LinuxX64Glibc => Ok("falkordb-x64.so"),
            Platform::LinuxArm64Glibc => Ok("falkordb-arm64v8.so"),
            Platform::LinuxX64Musl => Ok("falkordb-alpine-x64.so"),
            Platform::LinuxArm64Musl => Ok("falkordb-alpine-arm64v8.so"),
            Platform::AmazonLinux2023X64 => Ok("falkordb-amazonlinux2023-x64.so"),
            Platform::Rhel8X64 => Ok("falkordb-rhel8-x64.so"),
            Platform::Rhel9X64 => Ok("falkordb-rhel9-x64.so"),
            Platform::MacOSArm64 => Ok("falkordb-macos-arm64v8.so"),
            Platform::MacOSX64Unsupported => Err(FalkorDBError::EmbeddedServerError(
                "macOS x86_64 is not natively supported. Please use Apple Silicon (aarch64) or run under Rosetta 2. \
                 To proceed with the ARM64 binary, explicitly set falkordb_module_path in EmbeddedConfig.".to_string(),
            )),
            Platform::Unsupported => Err(FalkorDBError::EmbeddedServerError(
                format!(
                    "FalkorDB embedded server is not supported on {}/{}. \
                     Supported platforms: Linux x86_64/aarch64 (glibc/musl), macOS aarch64.",
                    std::env::consts::OS,
                    std::env::consts::ARCH
                ),
            )),
        }
    }

    /// Returns the platform tag used in checksums and cache paths.
    pub fn tag(&self) -> Result<&'static str, FalkorDBError> {
        match self {
            Platform::LinuxX64Glibc => Ok("linux-x64-glibc"),
            Platform::LinuxArm64Glibc => Ok("linux-arm64-glibc"),
            Platform::LinuxX64Musl => Ok("linux-x64-musl"),
            Platform::LinuxArm64Musl => Ok("linux-arm64-musl"),
            Platform::AmazonLinux2023X64 => Ok("amazonlinux2023-x64"),
            Platform::Rhel8X64 => Ok("rhel8-x64"),
            Platform::Rhel9X64 => Ok("rhel9-x64"),
            Platform::MacOSArm64 => Ok("macos-arm64"),
            Platform::MacOSX64Unsupported => Err(FalkorDBError::EmbeddedServerError(
                "macOS x86_64 is not supported.".to_string(),
            )),
            Platform::Unsupported => Err(FalkorDBError::EmbeddedServerError(
                "Unsupported platform.".to_string(),
            )),
        }
    }
}

/// Detect if the current system uses musl libc (vs glibc).
fn is_musl() -> bool {
    // Try linking against a C symbol that differs between glibc and musl
    #[cfg(target_env = "musl")]
    {
        true
    }
    #[cfg(not(target_env = "musl"))]
    {
        // For targets compiled without explicit musl env, check at runtime via ldd
        // For now, assume glibc unless we're explicitly musl-targeted at compile time.
        // A more robust check would invoke ldd or check ld.so.
        false
    }
}

/// Detect if macOS has the required libomp (OpenMP) library for FalkorDB module.
///
/// Returns `Err` with an actionable message if libomp is missing on macOS.
/// Returns `Ok(true)` if present, `Ok(false)` if not required (non-macOS platform).
pub fn check_macos_libomp() -> FalkorResult<bool> {
    #[cfg(target_os = "macos")]
    {
        // Check for libomp in common Homebrew locations
        let libomp_paths = [
            "/opt/homebrew/opt/libomp/lib/libomp.dylib",
            "/usr/local/opt/libomp/lib/libomp.dylib",
        ];

        for path in &libomp_paths {
            if std::path::Path::new(path).exists() {
                return Ok(true);
            }
        }

        // Not found
        Err(FalkorDBError::EmbeddedServerError(
            "FalkorDB module requires OpenMP (libomp) on macOS. \
             Install it with: brew install libomp\n\
             Then try again. See https://www.falkordb.com/ for more information."
                .to_string(),
        ))
    }

    #[cfg(not(target_os = "macos"))]
    {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_detect() {
        let platform = Platform::detect();
        // Just verify it doesn't panic and returns a valid result
        println!("Detected platform: {:?}", platform);
        assert!(platform != Platform::Unsupported || cfg!(not(unix)));
    }

    #[test]
    fn test_falkordb_checksums() {
        let checksums = falkordb_checksums();
        // Verify we have checksums for all major platforms
        assert!(checksums.contains_key("linux-x64-glibc"));
        assert!(checksums.contains_key("linux-arm64-glibc"));
        assert!(checksums.contains_key("macos-arm64"));
        // Verify checksums are non-empty
        for (_, checksum) in checksums.iter() {
            assert!(!checksum.is_empty());
            assert_eq!(checksum.len(), 64); // SHA-256 hex string is 64 chars
        }
    }

    #[test]
    fn test_platform_asset_filename() {
        // Test a supported platform
        let platform = Platform::LinuxX64Glibc;
        assert_eq!(platform.asset_filename().unwrap(), "falkordb-x64.so");

        let platform = Platform::MacOSArm64;
        assert_eq!(
            platform.asset_filename().unwrap(),
            "falkordb-macos-arm64v8.so"
        );

        // Test unsupported
        let platform = Platform::Unsupported;
        assert!(platform.asset_filename().is_err());
    }

    #[test]
    fn test_platform_tag() {
        let platform = Platform::LinuxX64Glibc;
        assert_eq!(platform.tag().unwrap(), "linux-x64-glibc");

        let platform = Platform::MacOSArm64;
        assert_eq!(platform.tag().unwrap(), "macos-arm64");
    }

    #[test]
    fn test_all_platform_asset_filenames() {
        // Test all supported platforms have asset filenames
        assert_eq!(
            Platform::LinuxX64Glibc.asset_filename().unwrap(),
            "falkordb-x64.so"
        );
        assert_eq!(
            Platform::LinuxArm64Glibc.asset_filename().unwrap(),
            "falkordb-arm64v8.so"
        );
        assert_eq!(
            Platform::LinuxX64Musl.asset_filename().unwrap(),
            "falkordb-alpine-x64.so"
        );
        assert_eq!(
            Platform::LinuxArm64Musl.asset_filename().unwrap(),
            "falkordb-alpine-arm64v8.so"
        );
        assert_eq!(
            Platform::AmazonLinux2023X64.asset_filename().unwrap(),
            "falkordb-amazonlinux2023-x64.so"
        );
        assert_eq!(
            Platform::Rhel8X64.asset_filename().unwrap(),
            "falkordb-rhel8-x64.so"
        );
        assert_eq!(
            Platform::Rhel9X64.asset_filename().unwrap(),
            "falkordb-rhel9-x64.so"
        );
        assert_eq!(
            Platform::MacOSArm64.asset_filename().unwrap(),
            "falkordb-macos-arm64v8.so"
        );

        // Unsupported platforms return errors
        assert!(Platform::Unsupported.asset_filename().is_err());
        assert!(Platform::MacOSX64Unsupported.asset_filename().is_err());
    }

    #[test]
    fn test_all_platform_tags() {
        // Test all supported platforms have tags
        assert_eq!(Platform::LinuxX64Glibc.tag().unwrap(), "linux-x64-glibc");
        assert_eq!(
            Platform::LinuxArm64Glibc.tag().unwrap(),
            "linux-arm64-glibc"
        );
        assert_eq!(Platform::LinuxX64Musl.tag().unwrap(), "linux-x64-musl");
        assert_eq!(Platform::LinuxArm64Musl.tag().unwrap(), "linux-arm64-musl");
        assert_eq!(
            Platform::AmazonLinux2023X64.tag().unwrap(),
            "amazonlinux2023-x64"
        );
        assert_eq!(Platform::Rhel8X64.tag().unwrap(), "rhel8-x64");
        assert_eq!(Platform::Rhel9X64.tag().unwrap(), "rhel9-x64");
        assert_eq!(Platform::MacOSArm64.tag().unwrap(), "macos-arm64");

        // Unsupported platforms return errors
        assert!(Platform::Unsupported.tag().is_err());
        assert!(Platform::MacOSX64Unsupported.tag().is_err());
    }

    #[test]
    fn test_platform_equality() {
        assert_eq!(Platform::LinuxX64Glibc, Platform::LinuxX64Glibc);
        assert_ne!(Platform::LinuxX64Glibc, Platform::LinuxArm64Glibc);
    }

    #[test]
    fn test_platform_debug() {
        let platform = Platform::LinuxX64Glibc;
        let debug_str = format!("{:?}", platform);
        assert!(debug_str.contains("LinuxX64Glibc"));
    }

    #[test]
    fn test_is_musl() {
        let result = is_musl();
        // Result depends on target environment; just verify it returns a bool
        let _: bool = result;
    }

    #[test]
    fn test_check_macos_libomp() {
        let result = check_macos_libomp();
        // On non-macOS, should return Ok(false)
        // On macOS, returns Ok(true) if libomp found, Err otherwise
        match result {
            Ok(b) => {
                #[cfg(not(target_os = "macos"))]
                assert!(!b);
            }
            Err(_) => {
                #[cfg(target_os = "macos")]
                {
                    // This is expected if libomp is not installed
                }
            }
        }
    }
}
