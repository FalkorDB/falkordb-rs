#!/usr/bin/env bash
# bench-export.sh — run the async-strategies benchmark and regenerate blog/data/benchmarks.toml,
# a dated SAMPLE RUN with provenance metadata that the blog's `bench_table` shortcode renders.
#
# This is NOT a CI gate: criterion numbers vary by machine, run, and server image, so we never
# diff them. Readers reproduce the underlying measurement with `just bench`. We only run the
# connection-count-8 slice (pooled vs multiplexed across concurrency) to keep it quick and
# legible; the harness itself (benches/async_strategies.rs) is what's embedded in the post.
#
# Run via `just bench-export` (needs a reachable server) or `just bench-export-local` (Docker).

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

host="${FALKORDB_HOST:-127.0.0.1}"
port="${FALKORDB_PORT:-6379}"

echo "bench-export: running async_read_throughput (8 connections) against ${host}:${port} ..."
FALKORDB_HOST="$host" FALKORDB_PORT="$port" \
    cargo bench --features tokio --bench async_strategies -- 'async_read_throughput/(pooled|multiplexed)_8/'

echo "bench-export: parsing criterion estimates -> blog/data/benchmarks.toml"
python3 scripts/bench_export.py

echo "bench-export: done."
