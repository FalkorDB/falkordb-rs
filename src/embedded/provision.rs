/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Binary provisioning for the embedded FalkorDB server.
//!
//! Maps the current system to the matching FalkorDB module release asset
//! (filename, cache tag and pinned SHA-256 checksum) and checks platform
//! prerequisites. The actual download/caching lives in [`super::download`].
//!
//! The asset/checksum/platform machinery is only needed by the **runtime
//! download** path (`embedded` feature). The pure, dependency-free mapping lives
//! in `provision_shared.rs` so it can be shared verbatim with `build.rs` (the
//! `embedded-bundle` build-time fetch). The macOS libomp check is needed by both
//! the download and bundle paths, so it is always available.

#[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
use crate::FalkorDBError;
use crate::FalkorResult;

#[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
#[path = "provision_shared.rs"]
mod shared;

#[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
pub use shared::{module_checksum, Platform, FALKORDB_VERSION};

#[cfg(all(feature = "embedded", not(feature = "embedded-bundle")))]
impl Platform {
    /// Detect the current platform.
    ///
    /// On Linux this reads `/etc/os-release` to map RHEL/Amazon Linux to their
    /// dedicated assets and detects musl (Alpine) vs glibc. Returns
    /// `Platform::Unsupported` for any unsupported OS/architecture.
    pub fn detect() -> Self {
        let os = std::env::consts::OS;
        let arch = std::env::consts::ARCH;
        // `/etc/os-release` is only meaningful on Linux; reading it elsewhere
        // just yields `None`.
        let os_release = if os == "linux" {
            std::fs::read_to_string("/etc/os-release").ok()
        } else {
            None
        };
        Self::classify(os, arch, os_release.as_deref(), cfg!(target_env = "musl"))
    }

    /// Returns the asset filename for this platform, erroring (with actionable
    /// guidance) on unsupported platforms.
    pub fn asset_filename(&self) -> Result<&'static str, FalkorDBError> {
        self.asset_filename_opt().ok_or_else(|| match self {
            Platform::MacOSX64Unsupported => FalkorDBError::EmbeddedServerError(
                "macOS on x86_64 is not supported for auto-provisioning: there is no x86_64 \
                 FalkorDB module. On Apple Silicon, build and run your application as aarch64 \
                 (arm64) rather than under Rosetta 2. Alternatively, set falkordb_module_path to \
                 an arm64 `falkordb.so` and use an arm64 redis-server."
                    .to_string(),
            ),
            _ => FalkorDBError::EmbeddedServerError(format!(
                "FalkorDB embedded server is not supported on {}/{}. \
                     Supported platforms: Linux x86_64/aarch64 (glibc/musl), macOS aarch64.",
                std::env::consts::OS,
                std::env::consts::ARCH
            )),
        })
    }

    /// Returns the platform tag used in checksums and cache paths, erroring on
    /// unsupported platforms.
    pub fn tag(&self) -> Result<&'static str, FalkorDBError> {
        self.tag_opt().ok_or_else(|| match self {
            Platform::MacOSX64Unsupported => {
                FalkorDBError::EmbeddedServerError("macOS x86_64 is not supported.".to_string())
            }
            _ => FalkorDBError::EmbeddedServerError("Unsupported platform.".to_string()),
        })
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
        Err(crate::FalkorDBError::EmbeddedServerError(
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

#[cfg(all(test, feature = "embedded", not(feature = "embedded-bundle")))]
mod tests {
    use super::*;

    #[test]
    fn test_detect_returns_a_platform() {
        // `detect()` must classify the host into one of the known variants
        // without panicking; the exact value is host-dependent.
        let platform = Platform::detect();
        // Either resolves to a real asset, or the tag/asset are errors on an
        // unsupported host. Either way the calls are total.
        let _ = platform.asset_filename();
        let _ = platform.tag();
    }

    #[test]
    fn test_asset_filename_and_tag_errors_on_unsupported() {
        assert!(Platform::Unsupported.asset_filename().is_err());
        assert!(Platform::Unsupported.tag().is_err());
        assert!(Platform::MacOSX64Unsupported.asset_filename().is_err());
        assert!(Platform::MacOSX64Unsupported.tag().is_err());
    }

    #[test]
    fn test_asset_filename_ok_for_supported() {
        assert_eq!(
            Platform::LinuxX64Glibc.asset_filename().unwrap(),
            "falkordb-x64.so"
        );
        assert_eq!(
            Platform::MacOSArm64.asset_filename().unwrap(),
            "falkordb-macos-arm64v8.so"
        );
        assert_eq!(Platform::Rhel8X64.tag().unwrap(), "rhel8-x64");
    }
}

#[cfg(test)]
mod libomp_tests {
    use super::*;

    #[test]
    fn test_check_macos_libomp_is_total() {
        // Returns Ok(false) off macOS; on macOS it's Ok(true)/Err depending on
        // whether libomp is installed. Must never panic.
        let _ = check_macos_libomp();
    }
}
