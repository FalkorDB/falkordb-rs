/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Binary provisioning for the embedded FalkorDB server.
//!
//! This module maps the current system to the matching FalkorDB module release
//! asset (filename, cache tag and pinned SHA-256 checksum) and checks platform
//! prerequisites. The actual download/caching lives in [`super::download`].

use crate::{FalkorDBError, FalkorResult};

/// FalkorDB version to provision (pinned for reproducibility and security).
pub const FALKORDB_VERSION: &str = "v4.18.10";

/// SHA-256 checksums of each platform's FalkorDB module asset for
/// [`FALKORDB_VERSION`], keyed by [`Platform::tag`]. Sourced from the GitHub
/// release asset digests; used to verify downloads and prevent tampering.
const MODULE_CHECKSUMS: &[(&str, &str)] = &[
    (
        "linux-x64-glibc",
        "1326869d2b7f1669f8ff540045a38a9741f366caf522e1bf496393566c964ecd",
    ),
    (
        "linux-arm64-glibc",
        "48ca64c54b75c9223d369308482492c2559e09369a1e1594690ae40881bd5614",
    ),
    (
        "linux-x64-musl",
        "33cc3935bb4b8ed32ab5400da70202e8467aaf26f81431ab0808690b28a04400",
    ),
    (
        "linux-arm64-musl",
        "9fc919fd7b599a30ba660b9b2535f5101ec04b737a3f3185af5c34374856ba2d",
    ),
    (
        "amazonlinux2023-x64",
        "426a53de07911ca01797c8f67f5149c422a6ec093664d0416fd38d21b2138bfc",
    ),
    (
        "rhel8-x64",
        "b46828737fcc079f5ecef0fb6524259117d80ade1b997e390cb49afd5c880b40",
    ),
    (
        "rhel9-x64",
        "d1fa2b4c8c42ff0aa5db8f2ec9e1d711a41a9271e17421e411b43f588b6ddd6e",
    ),
    (
        "macos-arm64",
        "6a49901df745d599cdd3b49cdb39a34e01b15291376e32befb8b8cd4ec6fe94e",
    ),
];

/// Returns the pinned SHA-256 checksum for a platform `tag` at `version`.
///
/// Checksums are only known for the [`FALKORDB_VERSION`] this client ships
/// against, so any other (user-overridden) version returns `None` and the
/// download is accepted without checksum verification.
pub fn module_checksum(
    version: &str,
    tag: &str,
) -> Option<&'static str> {
    if version != FALKORDB_VERSION {
        return None;
    }
    MODULE_CHECKSUMS
        .iter()
        .find(|(known_tag, _)| *known_tag == tag)
        .map(|(_, checksum)| *checksum)
}

/// Platform identifier for binary selection.
///
/// Some variants are only constructed on the matching OS (e.g. the Linux
/// variants are never built on macOS), so the enum is allowed to carry
/// per-platform "dead" variants.
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
        Self::classify(os, arch, os_release.as_deref(), is_musl())
    }

    /// Pure OS/arch → [`Platform`] mapping, split out from [`detect`] so every
    /// branch is unit-testable on any host (not just the one running the tests).
    fn classify(
        os: &str,
        arch: &str,
        os_release: Option<&str>,
        compile_musl: bool,
    ) -> Self {
        match (os, arch) {
            ("linux", arch) => classify_linux(arch, compile_musl, os_release),
            ("macos", "aarch64") => Platform::MacOSArm64,
            // No native x86_64 macOS binary; would need Rosetta 2.
            ("macos", "x86_64") => Platform::MacOSX64Unsupported,
            _ => Platform::Unsupported,
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

/// Read a `KEY=value` field from `/etc/os-release`-style contents.
///
/// Values may be optionally quoted; surrounding single/double quotes are
/// stripped. Returns `None` when the key is absent.
fn os_release_field(
    contents: &str,
    key: &str,
) -> Option<String> {
    contents.lines().find_map(|line| {
        let rest = line.strip_prefix(key)?.strip_prefix('=')?;
        Some(rest.trim().trim_matches(['"', '\'']).to_string())
    })
}

/// Map a Linux `arch` to the best-matching [`Platform`].
///
/// `compile_musl` is the compile-time libc of this binary; `os_release` is the
/// contents of `/etc/os-release` when available. Alpine is also treated as musl
/// via `os-release`. RHEL-family (incl. CentOS/Rocky/Alma) 8/9 and Amazon Linux
/// 2023 on x86_64 map to their dedicated assets; everything else falls back to
/// the generic glibc asset. Pure and side-effect free so it is unit-testable.
fn classify_linux(
    arch: &str,
    compile_musl: bool,
    os_release: Option<&str>,
) -> Platform {
    let id = os_release
        .and_then(|c| os_release_field(c, "ID"))
        .unwrap_or_default()
        .to_lowercase();
    let id_like = os_release
        .and_then(|c| os_release_field(c, "ID_LIKE"))
        .unwrap_or_default()
        .to_lowercase();
    let version_id = os_release
        .and_then(|c| os_release_field(c, "VERSION_ID"))
        .unwrap_or_default();

    // The module must match the *host's* libc (the redis-server it loads into),
    // so the running distro takes precedence: a known non-Alpine distro means
    // glibc even if this client was built static-musl. `compile_musl` is only a
    // fallback when /etc/os-release is unavailable or has no recognizable ID.
    let alpine = id == "alpine" || id_like.split_whitespace().any(|w| w == "alpine");
    let musl = alpine || (id.is_empty() && compile_musl);
    if musl {
        return match arch {
            "x86_64" => Platform::LinuxX64Musl,
            "aarch64" => Platform::LinuxArm64Musl,
            _ => Platform::Unsupported,
        };
    }

    match arch {
        "x86_64" => {
            let major = version_id.split('.').next().unwrap_or_default();
            if id == "amzn" && version_id.starts_with("2023") {
                return Platform::AmazonLinux2023X64;
            }
            let rhel_family = matches!(
                id.as_str(),
                "rhel" | "centos" | "rocky" | "almalinux" | "ol" | "fedora"
            ) || id_like
                .split_whitespace()
                .any(|w| w == "rhel" || w == "fedora" || w == "centos");
            if rhel_family {
                match major {
                    "8" => return Platform::Rhel8X64,
                    "9" => return Platform::Rhel9X64,
                    _ => {}
                }
            }
            Platform::LinuxX64Glibc
        }
        "aarch64" => Platform::LinuxArm64Glibc,
        _ => Platform::Unsupported,
    }
}

/// Whether this binary was compiled against musl libc (vs glibc).
///
/// Used only as a fallback signal by [`classify_linux`] when `/etc/os-release`
/// is unavailable; the running distro is otherwise authoritative for which
/// module asset matches the host `redis-server`.
fn is_musl() -> bool {
    cfg!(target_env = "musl")
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
        // `detect()` must never panic. `Unsupported` is a valid outcome on
        // platforms/arches without a FalkorDB asset (e.g. BSD, macOS x86_64,
        // non-x86_64/aarch64 Linux), so it is accepted here.
        let platform = Platform::detect();
        println!("Detected platform: {:?}", platform);
    }

    #[test]
    fn test_classify_all_os_arches() {
        // Every OS/arch arm is reachable here regardless of the host running
        // the tests.
        assert_eq!(
            Platform::classify("macos", "aarch64", None, false),
            Platform::MacOSArm64
        );
        assert_eq!(
            Platform::classify("macos", "x86_64", None, false),
            Platform::MacOSX64Unsupported
        );
        assert_eq!(
            Platform::classify("linux", "x86_64", None, false),
            Platform::LinuxX64Glibc
        );
        assert_eq!(
            Platform::classify("linux", "aarch64", None, false),
            Platform::LinuxArm64Glibc
        );
        // Unsupported OSes and arches.
        assert_eq!(
            Platform::classify("freebsd", "x86_64", None, false),
            Platform::Unsupported
        );
        assert_eq!(
            Platform::classify("windows", "x86_64", None, false),
            Platform::Unsupported
        );
        assert_eq!(
            Platform::classify("linux", "riscv64", None, false),
            Platform::Unsupported
        );
    }

    #[test]
    fn test_module_checksum() {
        // Every supported tag has a 64-char hex SHA-256 for the pinned version.
        let tags = [
            "linux-x64-glibc",
            "linux-arm64-glibc",
            "linux-x64-musl",
            "linux-arm64-musl",
            "amazonlinux2023-x64",
            "rhel8-x64",
            "rhel9-x64",
            "macos-arm64",
        ];
        for tag in tags {
            let checksum = module_checksum(FALKORDB_VERSION, tag)
                .unwrap_or_else(|| panic!("missing checksum for {tag}"));
            assert_eq!(checksum.len(), 64, "tag {tag} checksum not 64 hex chars");
            assert!(checksum.chars().all(|c| c.is_ascii_hexdigit()));
            assert_ne!(
                checksum,
                "0".repeat(64),
                "tag {tag} still has a placeholder checksum"
            );
        }
    }

    #[test]
    fn test_module_checksum_unknown_version_or_tag() {
        // Unknown version → no checksum (download accepted without verification).
        assert!(module_checksum("v9.9.9", "linux-x64-glibc").is_none());
        // Unknown tag for the pinned version → no checksum.
        assert!(module_checksum(FALKORDB_VERSION, "solaris-sparc").is_none());
    }

    #[test]
    fn test_checksums_cover_every_supported_platform() {
        // The checksum table must stay in sync with the asset matrix: every
        // platform that yields a tag must have a pinned checksum.
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
            let tag = platform.tag().unwrap();
            assert!(
                module_checksum(FALKORDB_VERSION, tag).is_some(),
                "no checksum pinned for {platform:?} (tag {tag})"
            );
        }
    }

    #[test]
    fn test_os_release_field() {
        let contents = "NAME=\"Ubuntu\"\nID=ubuntu\nID_LIKE=debian\nVERSION_ID=\"22.04\"\n";
        assert_eq!(os_release_field(contents, "ID").as_deref(), Some("ubuntu"));
        assert_eq!(
            os_release_field(contents, "ID_LIKE").as_deref(),
            Some("debian")
        );
        assert_eq!(
            os_release_field(contents, "VERSION_ID").as_deref(),
            Some("22.04")
        );
        assert_eq!(os_release_field(contents, "MISSING"), None);
    }

    #[test]
    fn test_classify_linux_generic_glibc() {
        let ubuntu = "ID=ubuntu\nID_LIKE=debian\nVERSION_ID=\"22.04\"\n";
        assert_eq!(
            classify_linux("x86_64", false, Some(ubuntu)),
            Platform::LinuxX64Glibc
        );
        assert_eq!(
            classify_linux("aarch64", false, Some(ubuntu)),
            Platform::LinuxArm64Glibc
        );
        // No os-release available → generic glibc.
        assert_eq!(
            classify_linux("x86_64", false, None),
            Platform::LinuxX64Glibc
        );
    }

    #[test]
    fn test_classify_linux_musl() {
        let alpine = "ID=alpine\nVERSION_ID=3.20\n";
        // Detected via os-release even when compiled for glibc.
        assert_eq!(
            classify_linux("x86_64", false, Some(alpine)),
            Platform::LinuxX64Musl
        );
        assert_eq!(
            classify_linux("aarch64", false, Some(alpine)),
            Platform::LinuxArm64Musl
        );
        // Detected via compile-time target_env when os-release is unavailable.
        assert_eq!(classify_linux("x86_64", true, None), Platform::LinuxX64Musl);
    }

    #[test]
    fn test_classify_linux_host_distro_overrides_compile_musl() {
        // A static-musl client running on a glibc host must pick the glibc
        // asset: the module is loaded by the host's glibc redis-server.
        let ubuntu = "ID=ubuntu\nID_LIKE=debian\nVERSION_ID=\"22.04\"\n";
        assert_eq!(
            classify_linux("x86_64", true, Some(ubuntu)),
            Platform::LinuxX64Glibc
        );
    }

    #[test]
    fn test_classify_linux_rhel_and_amazon() {
        let rhel8 = "ID=rhel\nVERSION_ID=\"8.9\"\n";
        let rocky9 = "ID=rocky\nID_LIKE=\"rhel centos fedora\"\nVERSION_ID=\"9.3\"\n";
        let amzn = "ID=amzn\nVERSION_ID=2023\n";
        assert_eq!(
            classify_linux("x86_64", false, Some(rhel8)),
            Platform::Rhel8X64
        );
        assert_eq!(
            classify_linux("x86_64", false, Some(rocky9)),
            Platform::Rhel9X64
        );
        assert_eq!(
            classify_linux("x86_64", false, Some(amzn)),
            Platform::AmazonLinux2023X64
        );
        // RHEL/Amazon on aarch64 have no dedicated asset → generic glibc arm64.
        assert_eq!(
            classify_linux("aarch64", false, Some(rhel8)),
            Platform::LinuxArm64Glibc
        );
        // Amazon Linux 2 (no 2023 asset) → generic glibc.
        let amzn2 = "ID=amzn\nVERSION_ID=2\n";
        assert_eq!(
            classify_linux("x86_64", false, Some(amzn2)),
            Platform::LinuxX64Glibc
        );
        // RHEL-family but an unsupported major (e.g. 7) → generic glibc.
        let rhel7 = "ID=rhel\nVERSION_ID=\"7.9\"\n";
        assert_eq!(
            classify_linux("x86_64", false, Some(rhel7)),
            Platform::LinuxX64Glibc
        );
    }

    #[test]
    fn test_classify_linux_unsupported_arch() {
        assert_eq!(
            classify_linux("riscv64", false, None),
            Platform::Unsupported
        );
        assert_eq!(classify_linux("ppc64le", true, None), Platform::Unsupported);
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
            Ok(_b) => {
                #[cfg(not(target_os = "macos"))]
                assert!(!_b);
            }
            Err(_) => {
                #[cfg(target_os = "macos")]
                {
                    // This is expected if libomp is not installed
                }
            }
        }
    }

    #[test]
    fn test_unsupported_platform_error_messages() {
        // Test error messages for unsupported platforms
        // This ensures the error message construction code is exercised
        let unsupported_err = Platform::Unsupported.asset_filename();
        assert!(unsupported_err.is_err());
        let msg = format!("{}", unsupported_err.unwrap_err());
        assert!(msg.contains("FalkorDB embedded server is not supported"));

        let unsupported_tag_err = Platform::Unsupported.tag();
        assert!(unsupported_tag_err.is_err());

        let macos_x64_err = Platform::MacOSX64Unsupported.asset_filename();
        assert!(macos_x64_err.is_err());
        let msg = format!("{}", macos_x64_err.unwrap_err());
        assert!(msg.contains("macOS x86_64"));

        let macos_x64_tag_err = Platform::MacOSX64Unsupported.tag();
        assert!(macos_x64_tag_err.is_err());
    }
}
