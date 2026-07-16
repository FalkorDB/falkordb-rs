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
features := "tokio,embedded,serde,tracing,metrics"

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

# Compile every example under the features it needs. CI gate; no server needed. Examples aren't
# built by `just build`, so this is what keeps them compiling. The TLS example is built in a
# separate pass with the sync `rustls` backend, which can't coexist with `tokio` in one build.
build-examples:
    cargo build --examples --features {{features}}
    cargo build --example tls --features rustls

# Build the API docs (matches the `check-doc` CI gate).
doc:
    cargo doc --all

# Build docs (no deps) and open them in a browser.
doc-open:
    cargo doc --all --no-deps --open

# Run documentation tests — the crate-level `//!` docs in src/lib.rs (the source the README is
# generated from) and every doc-comment code block — with the dev feature set. No server needed
# (blocks are `no_run`/pure).
doctest:
    cargo test --doc --features {{features}}

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

# Validate the `embedded-bundle` feature (build-time module embed) hermetically:
# build the lib, example and tests with a fixture `.so` via FALKORDB_EMBEDDED_MODULE_PATH
# (no network, no live server) and run the bundle unit tests. This exercises build.rs,
# the feature wiring, `include_bytes!` and runtime extraction. CI runs this same recipe.
check-embedded-bundle:
    #!/usr/bin/env bash
    set -euo pipefail
    fixture_dir="$(mktemp -d)"
    trap 'rm -rf "$fixture_dir"' EXIT
    fixture="$fixture_dir/falkordb-fixture.so"
    # Deterministic placeholder bytes; the bundle path never loads it into redis.
    printf 'falkordb-embedded-bundle-fixture-module\n' > "$fixture"
    export FALKORDB_EMBEDDED_MODULE_PATH="$fixture"
    cargo build --features embedded-bundle
    cargo build --features embedded-bundle --example embedded_usage
    cargo test --features embedded-bundle --lib embedded::bundle

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
#
# Two merged passes so every feature's lines are measured: (1) the full suite
# against a live server with the dev features, and (2) the build-time
# `embedded-bundle` path, hermetically (a fixture `.so` via
# FALKORDB_EMBEDDED_MODULE_PATH — no server, no network). `embedded-bundle`
# bundles `bundle.rs` and the bundle-mode resolution branch that the dev feature
# set compiles out.
coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    fixture_dir="$(mktemp -d)"
    trap 'rm -rf "$fixture_dir"' EXIT
    printf 'falkordb-embedded-bundle-fixture-module\n' > "$fixture_dir/falkordb-fixture.so"
    cargo llvm-cov clean --workspace
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} \
        cargo llvm-cov nextest --no-report --all --features {{features}}
    FALKORDB_EMBEDDED_MODULE_PATH="$fixture_dir/falkordb-fixture.so" \
        cargo llvm-cov nextest --no-report --lib --features embedded-bundle
    cargo llvm-cov report --codecov --output-path codecov.json

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

# Regenerate blog/data/benchmarks.toml from a focused async_strategies run (a dated SAMPLE RUN
# with provenance metadata rendered by the blog). Needs a reachable server. Not a CI gate —
# criterion numbers vary by machine/run. Use `just bench-export-local` to manage Docker.
bench-export:
    FALKORDB_HOST={{host}} FALKORDB_PORT={{port}} ./scripts/bench-export.sh

# Spin up a server, regenerate the blog benchmark numbers, then tear it down.
bench-export-local: db-up
    @just bench-export || (just db-down && exit 1)
    @just db-down

# Spin up a server, run all benchmarks, then tear it down.
bench-local: db-up
    @just bench-all || (just db-down && exit 1)
    @just db-down

# === Spellcheck ==============================================================

# Spellcheck the Markdown docs (CI gate). Requires `pyspelling` and `aspell` locally.
spellcheck:
    pyspelling -c .github/spellcheck-settings.yml -n Markdown

# Spellcheck the blog posts (CI gate). Same wordlist; a context filter strips TOML front matter
# and Zola shortcodes so only prose is checked. Requires `pyspelling` and `aspell` locally.
spellcheck-blog:
    pyspelling -c .github/spellcheck-settings.yml -n Blog

# Spellcheck a pull-request title exactly as the Spellcheck CI gate does. Catches
# technical words (e.g. type names) that release-plz would later copy verbatim into
# the changelog and fail the release PR. Set PR_TITLE first, e.g.
# `PR_TITLE='fix: handle ConnectionDown' just spellcheck-pr-title`.
spellcheck-pr-title:
    printf '# %s\n' "${PR_TITLE:?set PR_TITLE to the pull-request title}" > .pr-title.md && pyspelling -c .github/spellcheck-settings.yml -n PRTitle && rm -f .pr-title.md || { rm -f .pr-title.md; exit 1; }

# === PR title (Conventional Commits) =========================================

# Conventional-Commit types accepted in a PR title. The release-triggering subset
# `feat|fix|docs` mirrors release-plz's `release_commits` in release-plz.toml (so a fix/feat/docs
# PR actually cuts a release); the remaining types are valid but ride along with the next release.
pr_title_pattern := '^(feat|fix|docs|ci|chore|refactor|perf|test|build|style|revert)(\(.+\))?!?: .+'

# Validate a PR title as a Conventional Commit, exactly as the `PR title format` CI gate does.
# The title becomes the squash-merge subject and its prefix drives the release, so a malformed
# title silently skips it — as happened with PR #297's "Fix spurious connection errors: ...",
# which matched none of release-plz's release_commits and cut no release. Set PR_TITLE first,
# e.g. `PR_TITLE='fix: handle ConnectionDown' just check-pr-title`.
check-pr-title: test-pr-title
    #!/usr/bin/env bash
    set -euo pipefail
    title="${PR_TITLE:?set PR_TITLE to the pull-request title}"
    if ! grep -Eq '{{pr_title_pattern}}' <<<"$title"; then
        printf '::error::PR title is not a Conventional Commit: %s\n' "$title" >&2
        printf 'Expected "<type>[(scope)][!]: <subject>", where <type> is one of:\n' >&2
        printf '  feat, fix, docs                                       (trigger a release via release-plz)\n' >&2
        printf '  ci, chore, refactor, perf, test, build, style, revert (ride along with the next release)\n' >&2
        printf 'e.g. "fix: override redis-rs default response timeout"\n' >&2
        exit 1
    fi
    printf 'PR title OK: %s\n' "$title"

# Self-test the check-pr-title pattern against known-good/known-bad titles (incl. PR #297's subject
# that slipped through and skipped a release). Needs no PR_TITLE; runs as part of check-pr-title.
test-pr-title:
    #!/usr/bin/env bash
    set -euo pipefail
    pattern='{{pr_title_pattern}}'
    good=(
        "fix: derive Clone for FalkorAsyncClient"
        "feat!: make replica routing for read-only queries opt-in"
        "feat(blog): add diagram zoom, a livelier theme"
        "docs: generate README from crate docs via cargo-rdme"
        "ci: compile README doctests and examples in CI"
        "build(deps): bump codeql-action to 4.37.0 and regex to 1.13.0"
        "chore: release v0.10.2"
    )
    bad=(
        "Fix spurious connection errors: override redis-rs 1.x default 500ms response timeout"
        "Bump redis from 1.2.0 to 1.3.0"
        "update the readme"
        "fix:no space after the colon"
        "feature: not a recognized type"
    )
    rc=0
    for t in "${good[@]}"; do
        if ! grep -Eq "$pattern" <<<"$t"; then printf 'want PASS, got FAIL: %s\n' "$t" >&2; rc=1; fi
    done
    for t in "${bad[@]}"; do
        if grep -Eq "$pattern" <<<"$t"; then printf 'want FAIL, got PASS: %s\n' "$t" >&2; rc=1; fi
    done
    if [ "$rc" -ne 0 ]; then exit 1; fi
    printf 'test-pr-title: all %d cases passed\n' "$(( ${#good[@]} + ${#bad[@]} ))"

# === llms.txt (AI-readable API surface) ======================================

# Regenerate the repo-root llms.txt from docs/llms.template.md + the public API parsed
# from src/lib.rs (via the standalone tools/llms-gen crate; stable toolchain, deterministic).
llms:
    cargo run --quiet --locked --manifest-path tools/llms-gen/Cargo.toml

# Unit-test the llms.txt generator (parsing, feature detection, marker splicing).
test-llms:
    cargo test --quiet --locked --manifest-path tools/llms-gen/Cargo.toml

# CI drift gate: run the generator's tests, regenerate llms.txt, and fail if the committed
# copy is stale or missing. If this fails you changed the public API — run `just llms` and
# commit the updated llms.txt.
check-llms: test-llms llms
    git ls-files --error-unmatch llms.txt > /dev/null
    git diff --exit-code -- llms.txt

# === README (generated from the crate docs) ==================================

# Regenerate README.md from the crate-level `//!` docs in src/lib.rs via cargo-rdme: it strips
# hidden `#` doctest lines and intra-doc links and annotates code blocks as `rust` so GitHub
# highlights them. Edit the docs in src/lib.rs (not README.md), then run this and commit the result.
# Install the pinned tool with `cargo install cargo-rdme --version 2.0.0` (must match CI; see
# scripts/install-cargo-rdme.sh).
readme:
    cargo rdme --force

# CI drift gate: regenerate README.md and fail if the committed copy is stale. If this fails you
# edited the crate docs — run `just readme` and commit README.md.
check-readme:
    cargo rdme --check

# === Blog (Zola "Dev Log" -> GitHub Pages) ===================================
# A static site under blog/, deployed by .github/workflows/pages.yml. Every Rust snippet in a
# post is a real examples/*.rs / benches/*.rs file (compiled by `just build-examples` / the
# benches build); `blog-sync` copies those byte-for-byte into blog/snippets/ so what the reader
# sees is exactly what CI compiled. Keep `zola_version` in sync with the CI install step in
# pr-checks.yml and pages.yml. Install locally from https://github.com/getzola/zola/releases.
zola_version := "0.22.1"

# Copy the canonical, CI-compiled example & bench sources into blog/snippets/ (gitignored).
blog-sync:
    ./scripts/blog-sync.sh

# Build the static site into blog/public (syncs snippets first).
blog-build: blog-sync
    cd blog && zola build

# Serve the blog locally with live reload at http://127.0.0.1:1111 (syncs snippets first).
blog-serve: blog-sync
    cd blog && zola serve

# CI gate: sync snippets then build the site, failing on bad templates, missing snippets or
# broken internal links. Mirrors the `check-blog` job in pr-checks.yml.
blog-check: blog-sync
    cd blog && zola build

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
ci: fmt-check clippy build doc doctest build-examples deny check-llms check-readme

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
