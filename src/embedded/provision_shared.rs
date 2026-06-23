/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Pure, dependency-free platform/asset/checksum mapping for the embedded server.
//!
//! This file is shared verbatim by two compilation contexts:
//!
//! * the library, via `#[path = "provision_shared.rs"] mod shared;` in
//!   [`super::provision`], which wraps the pure `*_opt` helpers in the crate's
//!   `FalkorResult` API; and
//! * `build.rs`, via `#[path = "src/embedded/provision_shared.rs"]`, so the
//!   build-time module fetch (`embedded-bundle`) selects the **same** asset and
//!   checksum the runtime would, with no risk of the two drifting apart.
//!
//! Because `build.rs` cannot depend on the crate, everything here uses only
//! `std`. The library adds the richer, error-typed surface in `provision.rs`.

/// FalkorDB version to provision (pinned for reproducibility and security).
pub const FALKORDB_VERSION: &str = "v4.18.10";

/// SHA-256 checksums of each platform's FalkorDB module asset for
/// [`FALKORDB_VERSION`], keyed by [`Platform::tag_opt`]. Sourced from the GitHub
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
/// against, so any other (user-overridden) version returns `None`.
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

/// Lowercase-hex SHA-256 of `bytes` — a dependency-free implementation of the
/// FIPS 180-4 algorithm.
///
/// This lives here, alongside the checksum table, so `build.rs` (which
/// `#[path]`-includes this file) can verify a downloaded module without a
/// build-dependency: declaring `sha2` under a second name as a build-dep breaks
/// `cargo-nextest`, and pulling the real `sha2`/TLS stack into the offline
/// `embedded-bundle` runtime would defeat its purpose. The library itself hashes
/// via the `sha2` crate (see [`super::download`]), so this is unused there.
#[allow(dead_code)]
pub fn sha256_hex(bytes: &[u8]) -> String {
    #[rustfmt::skip]
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    // Pad: message || 0x80 || 0x00… || 64-bit big-endian bit length.
    let bit_len = (bytes.len() as u64).wrapping_mul(8);
    let mut msg = bytes.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    let mut w = [0u32; 64];
    for block in msg.chunks_exact(64) {
        for (word, src) in w.iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes([src[0], src[1], src[2], src[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }

        for (slot, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *slot = slot.wrapping_add(v);
        }
    }

    let mut out = String::with_capacity(64);
    use std::fmt::Write as _;
    for word in h {
        let _ = write!(out, "{word:08x}");
    }
    out
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
    /// Pure OS/arch → [`Platform`] mapping, split out so every branch is
    /// unit-testable on any host (not just the one running the tests).
    pub fn classify(
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

    /// Build a [`Platform`] from a release-asset platform tag (the inverse of
    /// [`tag_opt`](Self::tag_opt)). Used by `build.rs` to honor an explicit
    /// `FALKORDB_EMBEDDED_MODULE_PLATFORM` override. Returns `None` for an
    /// unknown tag.
    #[allow(dead_code)] // used by build.rs and tests, not the plain library build
    pub fn from_tag(tag: &str) -> Option<Self> {
        Some(match tag {
            "linux-x64-glibc" => Platform::LinuxX64Glibc,
            "linux-arm64-glibc" => Platform::LinuxArm64Glibc,
            "linux-x64-musl" => Platform::LinuxX64Musl,
            "linux-arm64-musl" => Platform::LinuxArm64Musl,
            "amazonlinux2023-x64" => Platform::AmazonLinux2023X64,
            "rhel8-x64" => Platform::Rhel8X64,
            "rhel9-x64" => Platform::Rhel9X64,
            "macos-arm64" => Platform::MacOSArm64,
            _ => return None,
        })
    }

    /// Returns the asset filename for this platform, or `None` when the platform
    /// has no FalkorDB module asset (unsupported).
    pub fn asset_filename_opt(&self) -> Option<&'static str> {
        Some(match self {
            Platform::LinuxX64Glibc => "falkordb-x64.so",
            Platform::LinuxArm64Glibc => "falkordb-arm64v8.so",
            Platform::LinuxX64Musl => "falkordb-alpine-x64.so",
            Platform::LinuxArm64Musl => "falkordb-alpine-arm64v8.so",
            Platform::AmazonLinux2023X64 => "falkordb-amazonlinux2023-x64.so",
            Platform::Rhel8X64 => "falkordb-rhel8-x64.so",
            Platform::Rhel9X64 => "falkordb-rhel9-x64.so",
            Platform::MacOSArm64 => "falkordb-macos-arm64v8.so",
            Platform::MacOSX64Unsupported | Platform::Unsupported => return None,
        })
    }

    /// Returns the platform tag used in checksums and cache paths, or `None` for
    /// an unsupported platform.
    pub fn tag_opt(&self) -> Option<&'static str> {
        Some(match self {
            Platform::LinuxX64Glibc => "linux-x64-glibc",
            Platform::LinuxArm64Glibc => "linux-arm64-glibc",
            Platform::LinuxX64Musl => "linux-x64-musl",
            Platform::LinuxArm64Musl => "linux-arm64-musl",
            Platform::AmazonLinux2023X64 => "amazonlinux2023-x64",
            Platform::Rhel8X64 => "rhel8-x64",
            Platform::Rhel9X64 => "rhel9-x64",
            Platform::MacOSArm64 => "macos-arm64",
            Platform::MacOSX64Unsupported | Platform::Unsupported => return None,
        })
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
/// `compile_musl` is the musl libc signal of the *target* (the binary's
/// `target_env` in the library, `CARGO_CFG_TARGET_ENV` in `build.rs`);
/// `os_release` is the contents of `/etc/os-release` when available (runtime
/// only). Alpine is also treated as musl via `os-release`. RHEL-family (incl.
/// CentOS/Rocky/Alma) 8/9 and Amazon Linux 2023 on x86_64 map to their
/// dedicated assets; everything else falls back to the generic glibc asset.
/// Pure and side-effect free so it is unit-testable.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hex_nist_vectors() {
        // FIPS 180-4 / NIST known-answer tests.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // A multi-block input (1000 bytes) exercises the message-schedule loop.
        assert_eq!(
            sha256_hex(&[0x61u8; 1000]),
            "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"
        );
    }

    #[test]
    fn test_classify_all_os_arches() {
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
    fn test_from_tag_roundtrips() {
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
            let tag = platform.tag_opt().unwrap();
            assert_eq!(Platform::from_tag(tag), Some(platform));
        }
        assert_eq!(Platform::from_tag("nonexistent"), None);
    }

    #[test]
    fn test_module_checksum() {
        for tag in [
            "linux-x64-glibc",
            "linux-arm64-glibc",
            "linux-x64-musl",
            "linux-arm64-musl",
            "amazonlinux2023-x64",
            "rhel8-x64",
            "rhel9-x64",
            "macos-arm64",
        ] {
            let checksum = module_checksum(FALKORDB_VERSION, tag)
                .unwrap_or_else(|| panic!("missing checksum for {tag}"));
            assert_eq!(checksum.len(), 64, "sha-256 hex is 64 chars for {tag}");
            assert!(checksum.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn test_module_checksum_unknown_version_or_tag() {
        assert!(module_checksum("v9.9.9", "linux-x64-glibc").is_none());
        assert!(module_checksum(FALKORDB_VERSION, "solaris-sparc").is_none());
    }

    #[test]
    fn test_all_supported_platforms_have_checksums() {
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
            let tag = platform.tag_opt().unwrap();
            assert!(
                module_checksum(FALKORDB_VERSION, tag).is_some(),
                "missing checksum for {tag}"
            );
            assert!(platform.asset_filename_opt().is_some());
        }
        assert!(Platform::Unsupported.tag_opt().is_none());
        assert!(Platform::Unsupported.asset_filename_opt().is_none());
        assert!(Platform::MacOSX64Unsupported.asset_filename_opt().is_none());
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
        assert_eq!(
            classify_linux("x86_64", false, None),
            Platform::LinuxX64Glibc
        );
    }

    #[test]
    fn test_classify_linux_musl() {
        let alpine = "ID=alpine\nVERSION_ID=3.19\n";
        assert_eq!(
            classify_linux("x86_64", false, Some(alpine)),
            Platform::LinuxX64Musl
        );
        assert_eq!(
            classify_linux("aarch64", false, Some(alpine)),
            Platform::LinuxArm64Musl
        );
        // No os-release but compiled musl → musl asset.
        assert_eq!(classify_linux("x86_64", true, None), Platform::LinuxX64Musl);
    }

    #[test]
    fn test_classify_linux_rhel_and_amazon() {
        let rhel8 = "ID=rhel\nVERSION_ID=\"8.9\"\n";
        let rhel9 = "ID=rhel\nVERSION_ID=\"9.3\"\n";
        let rocky9 = "ID=rocky\nID_LIKE=\"rhel centos fedora\"\nVERSION_ID=\"9.3\"\n";
        let amzn = "ID=amzn\nVERSION_ID=\"2023\"\n";
        assert_eq!(
            classify_linux("x86_64", false, Some(rhel8)),
            Platform::Rhel8X64
        );
        assert_eq!(
            classify_linux("x86_64", false, Some(rhel9)),
            Platform::Rhel9X64
        );
        assert_eq!(
            classify_linux("x86_64", false, Some(rocky9)),
            Platform::Rhel9X64
        );
        assert_eq!(
            classify_linux("x86_64", false, Some(amzn)),
            Platform::AmazonLinux2023X64
        );
        // RHEL on aarch64 has no dedicated asset → generic glibc arm64.
        assert_eq!(
            classify_linux("aarch64", false, Some(rhel9)),
            Platform::LinuxArm64Glibc
        );
    }
}
