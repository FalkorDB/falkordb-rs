/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Build script for the `embedded-bundle` feature.
//!
//! When `embedded-bundle` is enabled, this fetches the FalkorDB module (`.so`)
//! for the **build target** and writes it to `OUT_DIR`, so the library can
//! `include_bytes!` it and run the embedded server with **no runtime network
//! access**. The fetch happens here at build time precisely so the deployed
//! binary needs no network.
//!
//! Inputs (all optional; env vars):
//! * `FALKORDB_EMBEDDED_MODULE_VERSION` — release tag to fetch (default: pinned).
//! * `FALKORDB_EMBEDDED_MODULE_PLATFORM` — override the asset platform tag
//!   (e.g. `rhel9-x64`) when the target triple is ambiguous (distro-specific).
//! * `FALKORDB_EMBEDDED_MODULE_PATH` — use a local `.so` instead of downloading
//!   (fully offline builds / unsupported platforms).
//! * `FALKORDB_EMBEDDED_MODULE_SHA256` — expected SHA-256; required to download a
//!   non-pinned version (we never embed unchecked downloaded native code).
//!
//! When `embedded-bundle` is off (the default), this script does nothing beyond
//! registering rerun triggers, and pulls in no dependencies.

fn main() {
    // Re-run when any bundle input changes (cheap; always registered).
    for var in [
        "FALKORDB_EMBEDDED_MODULE_VERSION",
        "FALKORDB_EMBEDDED_MODULE_PLATFORM",
        "FALKORDB_EMBEDDED_MODULE_PATH",
        "FALKORDB_EMBEDDED_MODULE_SHA256",
    ] {
        println!("cargo:rerun-if-env-changed={var}");
    }
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/embedded/provision_shared.rs");

    #[cfg(feature = "embedded-bundle")]
    bundle::run();
}

// The pure platform/asset/checksum mapping shared with the library, so the
// build-time asset selection can never drift from the runtime's. Declared at the
// build-script root so its `#[path]` resolves relative to the crate root.
#[cfg(feature = "embedded-bundle")]
#[allow(dead_code)]
#[path = "src/embedded/provision_shared.rs"]
mod provision_shared;

#[cfg(feature = "embedded-bundle")]
mod bundle {
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use super::provision_shared::{module_checksum, sha256_hex, Platform, FALKORDB_VERSION};

    /// Print a build error and abort the build.
    fn fail(msg: &str) -> ! {
        // Cargo doesn't surface build-script panics verbatim, so emit the message
        // as a warning for visibility, then panic to halt the build.
        println!("cargo:warning=embedded-bundle: {msg}");
        panic!("embedded-bundle: {msg}");
    }

    fn env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.is_empty())
    }

    pub fn run() {
        let version =
            env("FALKORDB_EMBEDDED_MODULE_VERSION").unwrap_or_else(|| FALKORDB_VERSION.to_string());
        let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
        let dest = out_dir.join("falkordb_module.so");

        // docs.rs must never download. Embed an empty placeholder so the crate
        // still compiles for docs (include_bytes! + the metadata env vars) with no
        // network. (docs.rs also excludes this feature via package metadata.)
        if env("DOCS_RS").is_some() {
            write_atomic(&dest, b"");
            emit(&dest, &version, "docs-rs", &sha256_hex(b""));
            return;
        }

        // A local module is the offline / unsupported-platform escape hatch: embed
        // it without requiring a downloadable asset for the build target.
        if let Some(local) = env("FALKORDB_EMBEDDED_MODULE_PATH") {
            let sha256 = embed_local(&local, &dest);
            let tag = env("FALKORDB_EMBEDDED_MODULE_PLATFORM")
                .or_else(|| resolve_platform().tag_opt().map(str::to_string))
                .unwrap_or_else(|| "local".to_string());
            emit(&dest, &version, &tag, &sha256);
            return;
        }

        // Download path: requires a supported target platform.
        let platform = resolve_platform();
        let tag = platform
            .tag_opt()
            .unwrap_or_else(|| fail(&unsupported_platform_message()));
        let asset = platform
            .asset_filename_opt()
            .unwrap_or_else(|| fail(&unsupported_platform_message()));
        let sha256 = download(&version, tag, asset, &dest);
        emit(&dest, &version, tag, &sha256);
    }

    /// Expose the embedded file + metadata to the library via `env!()` /
    /// `include_bytes!`.
    fn emit(
        dest: &Path,
        version: &str,
        tag: &str,
        sha256: &str,
    ) {
        println!(
            "cargo:rustc-env=FALKORDB_EMBEDDED_MODULE_FILE={}",
            dest.display()
        );
        println!("cargo:rustc-env=FALKORDB_EMBEDDED_MODULE_VERSION={version}");
        println!("cargo:rustc-env=FALKORDB_EMBEDDED_MODULE_PLATFORM={tag}");
        println!("cargo:rustc-env=FALKORDB_EMBEDDED_MODULE_SHA256={sha256}");
    }

    /// Resolve the target [`Platform`] from cargo's target cfg, or an explicit
    /// `FALKORDB_EMBEDDED_MODULE_PLATFORM` override.
    ///
    /// Cargo exposes arch/os/libc-flavor but not the Linux distro, so
    /// distro-specific assets (RHEL/Amazon) need the override; otherwise a
    /// generic glibc/musl asset is chosen.
    fn resolve_platform() -> Platform {
        if let Some(tag) = env("FALKORDB_EMBEDDED_MODULE_PLATFORM") {
            return Platform::from_tag(&tag).unwrap_or_else(|| {
                fail(&format!(
                    "FALKORDB_EMBEDDED_MODULE_PLATFORM='{tag}' is not a known platform tag. \
                     Known tags: linux-x64-glibc, linux-arm64-glibc, linux-x64-musl, \
                     linux-arm64-musl, amazonlinux2023-x64, rhel8-x64, rhel9-x64, macos-arm64."
                ))
            });
        }
        let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
        let is_musl = std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("musl");
        // No /etc/os-release at build time → generic glibc/musl (distro override above).
        Platform::classify(&os, &arch, None, is_musl)
    }

    fn unsupported_platform_message() -> String {
        let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
        format!(
            "no FalkorDB module asset for target {os}/{arch}. Set \
             FALKORDB_EMBEDDED_MODULE_PLATFORM to a supported tag, or \
             FALKORDB_EMBEDDED_MODULE_PATH to a local `.so` to embed."
        )
    }

    /// Embed a user-provided local `.so`. Verifies its SHA-256 only when the
    /// user explicitly sets `FALKORDB_EMBEDDED_MODULE_SHA256` — a local file is
    /// the user's own artifact (possibly a custom build that legitimately differs
    /// from the official release), so it is otherwise trusted as-is.
    fn embed_local(
        local: &str,
        dest: &PathBuf,
    ) -> String {
        println!("cargo:rerun-if-changed={local}");
        let bytes = std::fs::read(local).unwrap_or_else(|e| {
            fail(&format!(
                "cannot read FALKORDB_EMBEDDED_MODULE_PATH '{local}': {e}"
            ))
        });
        let actual = sha256_hex(&bytes);

        if let Some(expected) = env("FALKORDB_EMBEDDED_MODULE_SHA256") {
            if !actual.eq_ignore_ascii_case(&expected) {
                fail(&format!(
                    "SHA-256 mismatch for local module '{local}': expected {expected}, got {actual}"
                ));
            }
        }
        write_atomic(dest, &bytes);
        actual
    }

    /// Download the module from the official releases and embed it. Always
    /// verifies a checksum: the pinned one for the default version, or an
    /// explicit `FALKORDB_EMBEDDED_MODULE_SHA256` for any other version. We never
    /// embed unchecked downloaded native code.
    fn download(
        version: &str,
        tag: &str,
        asset: &str,
        dest: &PathBuf,
    ) -> String {
        let expected = env("FALKORDB_EMBEDDED_MODULE_SHA256")
            .or_else(|| module_checksum(version, tag).map(str::to_string))
            .unwrap_or_else(|| {
                fail(&format!(
                    "no pinned checksum for version '{version}' on platform '{tag}'. Set \
                     FALKORDB_EMBEDDED_MODULE_SHA256 to the expected SHA-256, or \
                     FALKORDB_EMBEDDED_MODULE_PATH to embed a local file instead."
                ))
            });

        let url =
            format!("https://github.com/FalkorDB/FalkorDB/releases/download/{version}/{asset}");
        let bytes = http_get(&url);
        let actual = sha256_hex(&bytes);
        if !actual.eq_ignore_ascii_case(&expected) {
            fail(&format!(
                "SHA-256 mismatch for {url}: expected {expected}, got {actual}"
            ));
        }
        write_atomic(dest, &bytes);
        actual
    }

    /// Download `url` with the host `curl`, returning the bytes. Using `curl`
    /// keeps `build.rs` dependency-free — declaring an HTTP client as a second
    /// build-dependency of a runtime crate breaks `cargo-nextest`, and pulling a
    /// TLS stack into the offline `embedded-bundle` runtime would defeat its
    /// purpose. Hosts without `curl` (or network) use
    /// `FALKORDB_EMBEDDED_MODULE_PATH` instead.
    fn http_get(url: &str) -> Vec<u8> {
        // Up to 100 MiB — the module is ~10-20 MiB.
        const MAX_BYTES: u64 = 100 * 1024 * 1024;
        let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
        let tmp = out_dir.join(format!("download.tmp.{}", std::process::id()));

        let status = std::process::Command::new("curl")
            .args([
                "--fail",
                "--silent",
                "--show-error",
                "--location",
                "--proto",
                "=https",
                "--proto-redir",
                "=https",
                "--max-time",
                "300",
                "--retry",
                "3",
                "--max-filesize",
            ])
            .arg(MAX_BYTES.to_string())
            .arg("--output")
            .arg(&tmp)
            .arg(url)
            .status()
            .unwrap_or_else(|e| {
                fail(&format!(
                    "failed to run `curl` to download {url}: {e}. Install curl, or set \
                     FALKORDB_EMBEDDED_MODULE_PATH to a local module."
                ))
            });
        if !status.success() {
            let _ = std::fs::remove_file(&tmp);
            fail(&format!("`curl` failed to download {url} (exit {status})"));
        }

        let bytes = std::fs::read(&tmp)
            .unwrap_or_else(|e| fail(&format!("cannot read downloaded {}: {e}", tmp.display())));
        let _ = std::fs::remove_file(&tmp);
        if bytes.len() as u64 > MAX_BYTES {
            fail(&format!("download from {url} exceeded {MAX_BYTES} bytes"));
        }
        bytes
    }

    /// Write `bytes` to `dest` via a temp file + rename, so a partial write is
    /// never observed (and concurrent builds don't tear each other's output).
    fn write_atomic(
        dest: &PathBuf,
        bytes: &[u8],
    ) {
        let tmp = dest.with_extension(format!("so.tmp.{}", std::process::id()));
        let mut file = std::fs::File::create(&tmp)
            .unwrap_or_else(|e| fail(&format!("cannot create {}: {e}", tmp.display())));
        file.write_all(bytes)
            .and_then(|()| file.flush())
            .unwrap_or_else(|e| fail(&format!("cannot write {}: {e}", tmp.display())));
        std::fs::rename(&tmp, dest).unwrap_or_else(|e| {
            let _ = std::fs::remove_file(&tmp);
            fail(&format!("cannot finalize {}: {e}", dest.display()))
        });
    }
}
