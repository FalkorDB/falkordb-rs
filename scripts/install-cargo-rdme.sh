#!/usr/bin/env bash
# install-cargo-rdme.sh — install the exact cargo-rdme used to generate README.md.
#
# README.md is generated from the crate docs with cargo-rdme (`just readme`), and the
# `check-readme` CI gate fails if the committed README drifts. cargo-rdme's output is not
# identical across versions, so CI must use a fixed version. taiki-e/install-action's manifest
# lags behind (it ships 1.4.2, which doesn't strip intra-doc links cleanly), so we install a
# checksum-verified prebuilt binary of the pinned version straight from GitHub Releases.
#
# Keep VERSION in sync with the version documented in CONTRIBUTING.md. Locally, install the same
# version with `cargo install cargo-rdme --version <VERSION>`.

set -euo pipefail

VERSION="2.0.0"
SHA256="658007bb3d501a4c53e681e0d135c19924ac0c88b225d581932de8206705f177"
URL="https://github.com/orium/cargo-rdme/releases/download/v${VERSION}/cargo-rdme_v${VERSION}_x86_64-unknown-linux-musl.tar.bz2"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

curl -fsSL --retry 3 --retry-delay 2 -o "$tmp/cargo-rdme.tar.bz2" "$URL"
echo "${SHA256}  $tmp/cargo-rdme.tar.bz2" | sha256sum -c -

mkdir -p "$HOME/.cargo/bin"
tar -xjf "$tmp/cargo-rdme.tar.bz2" -C "$HOME/.cargo/bin" cargo-rdme

cargo-rdme --version
