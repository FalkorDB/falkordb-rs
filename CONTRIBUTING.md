# Contributing to falkordb-rs

Thanks for contributing! This guide covers the local development workflow, the
[`just`](https://github.com/casey/just) recipes that mirror CI, and how to run the test
suites and benchmarks.

Before opening a PR, run `just done` (every required CI gate, no server needed) — plus
`just verify` if you have a server — and keep the documentation in sync with your changes.
See also the repository engineering conventions in
[`.github/copilot-instructions.md`](.github/copilot-instructions.md).

## Development

This repository ships a [`just`](https://github.com/casey/just) file that automates the
whole development cycle — formatting, linting, building, docs, tests, coverage,
benchmarks, the dependency audit and a Dockerized FalkorDB server. It is the recommended
entry point for day-to-day work and mirrors the commands the CI gates run.

Install the runner once with `cargo install just` (or `brew install just`), then list
every available recipe:

```bash
just            # or: just --list
```

### Common recipes

```bash
# Fast inner loop (no server needed): format, lint and build.
just check

# Run every required CI gate locally (no server needed):
# fmt-check, clippy, build, doc, doctest, build-examples, deny, check-llms, check-readme.
just ci

# Post-task gate: every CI gate PLUS strict clippy over all targets/features
# (examples, tests, benches). Run this before declaring work done.
just done

# Format / lint / docs individually.
just fmt
just clippy
just doc

# Full validation including the server-backed test suite (manages Docker for you):
# spins up FalkorDB, populates the fixture, runs the suite, tears it down.
just verify
```

### Server-backed recipes

Tests, coverage and benchmarks need a reachable FalkorDB instance. The `db-*` recipes
manage one via Docker, and the `*-local` wrappers do it for you automatically:

```bash
# Manage a FalkorDB container yourself.
just db-up          # start a server (and wait until it is ready)
just db-populate    # load the IMDB fixture graph the lib tests use
just db-down        # stop and remove the container

# Or let a single recipe manage the container lifecycle end-to-end.
just test-local       # start DB, populate, run the full suite, tear down
just coverage-local   # same, but produce Codecov JSON
just bench-local      # start DB, run all benchmarks, tear down
```

Targeted recipes are available too, e.g. `just test-parity`, `just test-embedded`,
`just test-one <filter>`, `just proptest`, `just bench-one '<criterion-id>'`, and
`just coverage-html`.

The host, port, Docker image and feature set can be overridden on the command line, for
example `just port=6380 test` or `just image=falkordb/falkordb:latest db-up`.

### Editing the README

`README.md` is **generated** from the crate-level `//!` documentation in
[`src/lib.rs`](src/lib.rs) with [cargo-rdme](https://github.com/orium/cargo-rdme) — so its code
blocks double as compiled doctests (`just doctest`). **Do not edit `README.md` by hand** below the
`<!-- cargo-rdme -->` marker; edit the `//!` docs instead, then regenerate and commit the result:

```bash
cargo install cargo-rdme --version 2.0.0  # one-time; pinned to match CI (scripts/install-cargo-rdme.sh)
just readme               # regenerate README.md from the crate docs
just check-readme         # drift gate: fails if the committed README.md is stale
```

cargo-rdme strips the hidden `# ` doctest lines and intra-doc links and tags code blocks as
`rust` so GitHub highlights them. Only the hand-written header above the marker (badges, title,
"Try Free" badge) is edited directly. A `check-readme` CI job runs `just check-readme` on every pull
request (and before a release), so a stale `README.md` fails the build.

### Regenerating `llms.txt`

The repository ships an [`llms.txt`](llms.txt) — a curated, machine-readable summary of the public
API, idioms and pitfalls for AI coding assistants (the [llmstxt.org](https://llmstxt.org)
convention). Its narrative lives in [`docs/llms.template.md`](docs/llms.template.md); the
`## Public API` block is generated from `src/lib.rs`. **Whenever you change the public API,
regenerate it and commit the result:**

```bash
just llms        # rewrite llms.txt from the template + the current public API
just check-llms  # drift gate: fails if the committed llms.txt is stale
```

A `check-llms` CI job runs `just check-llms` on every pull request (and before a release), so a
stale `llms.txt` fails the build.

### Reproducing CI locally

The GitHub Actions workflows invoke these same recipes, so a failing CI job can be
reproduced with a single command:

| CI job | Recipe |
| --- | --- |
| `check-fmt` | `just fmt-check` |
| `check-clippy` | `just clippy` |
| `check-build` | `just build` |
| `check-doc` | `just doc` |
| `check-doctest` | `just doctest` |
| `check-examples` | `just build-examples` |
| `check-deny` | `just deny` |
| `check-proptest` | `just proptest` |
| `integration-tests` | `just integration` and `just integration --all-features` |
| `integration-tests-tokio` | `just integration --features tokio` |
| `coverage` | `just coverage` |
| `check-llms` | `just check-llms` |
| `check-readme` | `just check-readme` |

Run `just ci` to execute every required no-server gate at once, or `just verify` to also
run the server-backed suite. The integration and coverage recipes need a reachable
FalkorDB instance (use `just db-up` first, or the `*-local` wrappers).

## Testing

### Running Tests

This project includes both unit tests and integration tests.

#### Unit Tests

Unit tests don't require a running FalkorDB instance:

```bash
# Run all unit tests
cargo test --lib

# Run unit tests with embedded feature
cargo test --lib --features embedded
```

#### Property-Based Tests

The crate ships [`proptest`](https://docs.rs/proptest) suites that need no running server:
`src/value/param_proptest.rs` checks query-parameter encoding (encoding arbitrary values never
panics, string escaping is lossless, NUL is rejected), and `src/value/de_proptest.rs` checks the
optional `serde` mapping (agreement with `serde_json`, no panics, malformed-row rejection). Run
just these:

```bash
# 256 cases per property (the proptest default)
just proptest

# crank the generated case count up (or set PROPTEST_CASES yourself)
just proptest 4096

# equivalent raw cargo command
cargo nextest run --lib --features serde proptest
```

They also run in CI: as the dedicated `check-proptest` job, and within the `coverage` job.

#### Integration Tests

Integration tests require a running FalkorDB instance. The easiest way to run them is using Docker:

```bash
# Using the provided script (requires Docker)
./run_integration_tests.sh

# Or manually start FalkorDB and run tests
docker run -d --name falkordb-test -p 6379:6379 falkordb/falkordb:latest
cargo test --test integration_tests

# With async support
cargo test --test integration_tests --features tokio

# Clean up
docker stop falkordb-test && docker rm falkordb-test
```

#### CI Integration Tests

Integration tests are automatically run in GitHub Actions using Docker services. See `.github/workflows/integration-tests.yml` for the CI configuration.

### Benchmarks

The crate ships a [criterion](https://docs.rs/criterion) benchmark,
`benches/async_strategies.rs`, that compares the two async connection strategies
(`Pooled` vs `Multiplexed`) across a range of connection counts (1, 8, 32) and
concurrency levels (1, 8, 64, 256). Benchmarks are developer/PR-time tools and are **not**
part of the required CI gates.

They require a running FalkorDB instance and the `tokio` feature:

```bash
# Start a server (configure with FALKORDB_HOST / FALKORDB_PORT; defaults to 127.0.0.1:6379)
docker run -d --name falkordb-bench -p 6379:6379 falkordb/falkordb:latest

# Run the full benchmark suite
cargo bench --features tokio --bench async_strategies

# Run a single case (criterion accepts a filter on the benchmark id)
cargo bench --features tokio --bench async_strategies -- 'async_read_throughput/multiplexed_8/8'

# Clean up
docker stop falkordb-bench && docker rm falkordb-bench
```

When no server is reachable the benchmark prints a notice and skips its work, so it stays
runnable in serverless CI.

#### Interpreting the results

Each case reports the wall-clock time to complete a batch of `concurrency` read queries,
and the corresponding throughput (`Kelem/s`). criterion writes a full HTML report to
`target/criterion/report/index.html`.

What to expect:

- **At concurrency = 1** the two strategies are close: a single in-flight command cannot
  benefit from multiplexing, so the per-request latency dominates.
- **As concurrency rises (64, 256)** the `multiplexed` strategy should pull ahead of
  `pooled` at the same connection count, because many commands are pipelined over each
  shared socket instead of waiting for an exclusive connection from the pool. The gap is
  largest at low connection counts (e.g. `multiplexed_1` vs `pooled_1`), where the pool
  becomes a hard bottleneck while a single multiplexed socket keeps absorbing work.
- **Higher connection counts narrow the gap**: a large pool (e.g. `pooled_32`) hides much
  of its borrow/return cost, approaching multiplexed throughput at the expense of holding
  more sockets open.

Absolute numbers depend heavily on your hardware, the server, and network latency, so
treat them as relative comparisons between strategies on the *same* machine rather than
portable figures.

#### Memory and CPU usage

`benches/async_strategies.rs` measures wall-clock throughput. A second, non-criterion
harness, `benches/resource_usage.rs`, measures **peak memory (RSS)** and **CPU time**
(user/system) per strategy. Because peak RSS is a process-wide high-water mark that cannot
be reset between iterations, the harness runs each strategy in its own subprocess and
prints a table:

```bash
docker run -d --name falkordb-bench -p 6379:6379 falkordb/falkordb:latest
cargo bench --features tokio --bench resource_usage
docker stop falkordb-bench && docker rm falkordb-bench
```

Example output (numbers are illustrative — they vary by machine and server):

```text
strategy         peak_rss_MiB  cpu_user_ms   cpu_sys_ms      wall_ms    queries/sec
pooled:1               ...
multiplexed:1          ...
...
```

What to expect:

- **Memory:** at the *same* connection count, peak RSS is comparable — both strategies hold
  that many sockets. The real saving is that `multiplexed` sustains high concurrency with
  *far fewer* connections (e.g. `multiplexed:1` vs a large `pooled:N`), and each connection
  carries its own read/write buffers, so cutting connection count cuts RSS.
- **CPU:** `multiplexed` removes the borrow/return machinery — the `mpsc` channel, the
  `Mutex`, and the per-command task spawn the pool uses to return a connection — so it
  generally spends less CPU per request and produces less transient allocation churn.

When no server is reachable the harness prints a notice and exits cleanly, so it stays
runnable in serverless CI.

