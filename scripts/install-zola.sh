#!/usr/bin/env bash
# install-zola.sh — install the exact Zola used to build the blog (the "Dev Log" -> GitHub Pages).
#
# The site under blog/ is built with Zola (`just blog-build` / `blog-check`). Zola's config schema
# and rendered output vary across versions, so CI must use a fixed version. taiki-e/install-action's
# pinned manifest lags behind new Zola releases (its manifest predates 0.22.x), so — exactly like
# scripts/install-cargo-rdme.sh — we install a checksum-verified prebuilt binary straight from
# GitHub Releases.
#
# Keep VERSION in sync with `zola_version` in the Justfile. Locally, install the same version from
# https://github.com/getzola/zola/releases.

set -euo pipefail

VERSION="0.22.1"
SHA256="0ca09aa40376aaa9ddfb512ff9ad963262ef95edb0d0f2d5ec6961b6f5cf22ef"
URL="https://github.com/getzola/zola/releases/download/v${VERSION}/zola-v${VERSION}-x86_64-unknown-linux-gnu.tar.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

curl -fsSL --retry 3 --retry-delay 2 -o "$tmp/zola.tar.gz" "$URL"
echo "${SHA256}  $tmp/zola.tar.gz" | sha256sum -c -

mkdir -p "$HOME/.cargo/bin"
tar -xzf "$tmp/zola.tar.gz" -C "$HOME/.cargo/bin" zola

zola --version
