# Justfile — dev-cycle automation for falkordb-rs.
#
# Run `just` (or `just --list`) to see every available recipe.
# Most test/coverage/bench recipes need a running FalkorDB server; use the
# `db-*` recipes (or the `*-local` wrappers) to manage one via Docker.

set shell := ["bash", "-uc"]

# --- Configuration (override on the CLI, e.g. `just port=6380 test`) ---------

# Host/port the tests, benches and DB helpers target.
host := env_var_or_default("FALKORDB_HOST", "127.0.0.1")
port := env_var_or_default("FALKORDB_PORT", "6379")

# Docker image and container name used by the `db-*` helpers.
image := "falkordb/falkordb:edge"
container := "falkordb-rs-dev"

# Feature set exercised by the full local suite (mirrors the coverage CI job).
features := "tokio,embedded,serde"

# Default recipe: list everything.
default:
    @just --list

# === Format ==================================================================

# Format all code in place.
fmt:
    cargo fmt --all

# Check formatting without modifying files (CI gate).
fmt-check:
    cargo fmt --all --check

# === Lint ====================================================================

# Clippy over the whole workspace (matches the `check-clippy` CI gate).
clippy:
    cargo clippy --all

# Strict clippy over all targets and dev features, warnings denied.
clippy-all:
    cargo clippy --all-targets --features {{features}} -- -D warnings

# Dependency/license/advisory audit (matches the `check-deny` CI gate).
deny:
    cargo deny check bans licenses sources

# === Build ===================================================================

# Default build (matches the `check-build` CI gate).
build:
    cargo build

# Build every target with the dev feature set.
build-all:
    cargo build --all-targets --features {{features}}

# Build the API docs (matches the `check-doc` CI gate).
doc:
    cargo doc --all

# Build docs (no deps) and open them in a browser.
doc-open:
    cargo doc --all --no-deps --open

# === Test ====================================================================
# These need a reachable FalkorDB server. Use `just test-local` to manage one.

# Run the full unit + integration suite (nextest) against a running server.
test:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo nextest run --all --features {{features}}

# Run only the cross-strategy parity/regression suite.
test-parity:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo nextest run --features {{features}} --test multiplexing_parity

# Run only the embedded-server integration suite.
test-embedded:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo nextest run --features {{features}} --test embedded_integration

# Run the integration_tests binary with the given cargo args, exactly as the
# integration CI jobs do. Examples: `just integration`,
# `just integration --features tokio`, `just integration --all-features`.
integration *args:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo test --test integration_tests {{args}} --verbose

# Run a single test by name filter, e.g. `just test-one test_borrow_connection`.
test-one filter:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo nextest run --all --features {{features}} {{filter}}

# Run only the `serde` property-based tests (no server); `cases` sets PROPTEST_CASES (default 256).
proptest cases="256":
    PROPTEST_CASES={{cases}} cargo nextest run --lib --features serde proptest

# Spin up a server, populate the fixture, run the full suite, then tear it down.
test-local: db-up db-populate
    @just test || (just db-down && exit 1)
    @just db-down

# === Coverage ================================================================

# Generate Codecov JSON coverage (matches the `coverage` CI job).
coverage:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo llvm-cov nextest --all --features {{features}} \
        --codecov --output-path codecov.json

# Generate an HTML coverage report and open it.
coverage-html:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo llvm-cov nextest --all --features {{features}} --open

# Spin up a server, populate the fixture, collect coverage, then tear it down.
coverage-local: db-up db-populate
    @just coverage || (just db-down && exit 1)
    @just db-down

# === Benchmarks ==============================================================
# Benchmarks need the `tokio` feature and a reachable server. They are
# developer/PR tools, not a CI gate. See the README "Benchmarks" section.

# Throughput benchmark: Pooled vs Multiplexed across counts/concurrency.
bench:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo bench --features tokio --bench async_strategies

# Run a single throughput case, e.g. `just bench-one 'async_read_throughput/multiplexed_8/8'`.
bench-one filter:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo bench --features tokio --bench async_strategies -- '{{filter}}'

# Peak memory (RSS) and CPU-time benchmark per strategy.
bench-resource:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo bench --features tokio --bench resource_usage

# Run both benchmark harnesses.
bench-all: bench bench-resource

# Spin up a server, run all benchmarks, then tear it down.
bench-local: db-up
    @just bench-all || (just db-down && exit 1)
    @just db-down

# === Spellcheck ==============================================================

# Spellcheck the Markdown docs (CI gate). Requires `pyspelling` and `aspell` locally.
spellcheck:
    pyspelling -c .github/spellcheck-settings.yml

# === Docker / FalkorDB lifecycle =============================================

# Start a FalkorDB server in Docker on the configured port.
db-up:
    docker rm -f {{container}} >/dev/null 2>&1 || true
    docker run -d --name {{container}} -p {{port}}:6379 {{image}} >/dev/null
    @echo "Waiting for FalkorDB on {{host}}:{{port}}..."
    @for i in $(seq 1 30); do \
        if docker exec {{container}} redis-cli ping >/dev/null 2>&1; then \
            echo "FalkorDB is ready."; exit 0; \
        fi; sleep 1; \
    done; echo "FalkorDB did not become ready in time" >&2; exit 1

# Stop and remove the FalkorDB container.
db-down:
    docker rm -f {{container}} >/dev/null 2>&1 || true

# Populate the IMDB fixture graph the lib tests rely on (needs python + falkordb).
db-populate:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} python3 resources/populate_graph.py

# === Aggregates ==============================================================

# Fast pre-commit loop: format, lint and build (no server required).
check: fmt clippy build

# Run every required CI gate locally (no server required).
ci: fmt-check clippy build doc deny

# Full post-task gate (no server required): strict clippy-all plus every CI gate.
# Must be green before a task is declared done.
done:
    ./scripts/post-checks.sh

# Full local validation: CI gates plus the server-backed test suite.
verify: ci test-local

# === Housekeeping ============================================================

# Remove build artifacts and the generated coverage file.
clean:
    cargo clean
    rm -f codecov.json
