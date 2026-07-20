# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Other

- Bump the `futures` and `futures-core` crates (0.3.32 → 0.3.33), the `serde` crate
  (1.0.228 → 1.0.229), the `redis` crate (1.4.0 → 1.4.1), the `tokio` crate (1.52.3 → 1.53.0) and
  the `github/codeql-action` (`init` and `analyze`) GitHub Action (`99df26d` → `7188fc3`, v4),
  combining the Dependabot updates from
  [#307](https://github.com/FalkorDB/falkordb-rs/pull/307),
  [#308](https://github.com/FalkorDB/falkordb-rs/pull/308),
  [#309](https://github.com/FalkorDB/falkordb-rs/pull/309),
  [#310](https://github.com/FalkorDB/falkordb-rs/pull/310),
  [#311](https://github.com/FalkorDB/falkordb-rs/pull/311),
  [#312](https://github.com/FalkorDB/falkordb-rs/pull/312) and
  [#313](https://github.com/FalkorDB/falkordb-rs/pull/313) into one change so the `init` and
  `analyze` steps stay pinned to the same `github/codeql-action` version
  ([#314](https://github.com/FalkorDB/falkordb-rs/pull/314))

## [0.10.3](https://github.com/FalkorDB/falkordb-rs/compare/v0.10.2...v0.10.3) - 2026-07-16

### Added

- `FalkorClientBuilder::with_response_timeout(Option<Duration>)` to configure an optional
  client-side response timeout for async connections. It defaults to `None` (no client-side
  deadline) — matching `falkordb-py`'s `socket_timeout` — so the server's `TIMEOUT` /
  `TIMEOUT_DEFAULT` configuration governs how long a query may run
  ([#297](https://github.com/FalkorDB/falkordb-rs/pull/297))

### Fixed

- Override the built-in 500ms default response timeout that `redis-rs 1.x` applies to every async
  connection path (multiplexed, sentinel, replica, and `ConnectionManager`). Queries that took
  longer than 500ms (for example `LOAD CSV`, deep traversals, or heavy aggregations) previously
  failed client-side with a spurious connection error while the server kept executing them, and
  the ensuing retries could duplicate writes. The response timeout is now unset by default
  ([#297](https://github.com/FalkorDB/falkordb-rs/pull/297))

### Other

- Bump the `github/codeql-action` (`init` and `analyze`, 4.36.2 → 4.36.3) and
  `rojopolis/spellcheck-github-actions` (0.62.0 → 0.63.0) GitHub Actions, combining the Dependabot
  updates from [#289](https://github.com/FalkorDB/falkordb-rs/pull/289),
  [#290](https://github.com/FalkorDB/falkordb-rs/pull/290) and
  [#291](https://github.com/FalkorDB/falkordb-rs/pull/291) into one change so the `init` and
  `analyze` steps stay pinned to the same `github/codeql-action` version
  ([#292](https://github.com/FalkorDB/falkordb-rs/pull/292))
- Bump the `github/codeql-action` (`init` and `analyze`, 4.36.3 → 4.37.0) GitHub Actions and the
  `regex` crate (1.12.4 → 1.13.0), combining the Dependabot updates from
  [#293](https://github.com/FalkorDB/falkordb-rs/pull/293),
  [#294](https://github.com/FalkorDB/falkordb-rs/pull/294) and
  [#295](https://github.com/FalkorDB/falkordb-rs/pull/295) into one change so the `init` and
  `analyze` steps stay pinned to the same `github/codeql-action` version
  ([#296](https://github.com/FalkorDB/falkordb-rs/pull/296))
- Add a `PR title format` CI gate (`just check-pr-title`, in `spellcheck.yml`) that rejects a
  pull-request title which isn't a Conventional Commit, so a `fix` / `feat` / `docs` change is
  titled to match `release-plz`'s `release_commits` and actually cuts a release (PR #297's
  non-conventional title matched nothing and released nothing). Also sets a Conventional-Commit
  `commit-message` prefix for Dependabot and documents the rule in
  `.github/copilot-instructions.md` ([#298](https://github.com/FalkorDB/falkordb-rs/pull/298))
- Bump the `redis` crate (1.3.0 → 1.4.0), the `regex` crate (1.13.0 → 1.13.1), the `which` crate
  (8.0.4 → 8.0.5) and the `MarcoIeni/release-plz-action` GitHub Action (0.5.130 → 0.5.131),
  combining the Dependabot updates from
  [#301](https://github.com/FalkorDB/falkordb-rs/pull/301),
  [#302](https://github.com/FalkorDB/falkordb-rs/pull/302),
  [#303](https://github.com/FalkorDB/falkordb-rs/pull/303) and
  [#304](https://github.com/FalkorDB/falkordb-rs/pull/304) into one change

## [0.10.2](https://github.com/FalkorDB/falkordb-rs/compare/v0.10.1...v0.10.2) - 2026-07-05

### Fixed

- Derive `Clone` for `FalkorAsyncClient`, matching `FalkorSyncClient` and its own thread-safety
  documentation, which already stated the client can be cloned and shared across threads
  ([#287](https://github.com/FalkorDB/falkordb-rs/pull/287))

## [0.10.1](https://github.com/FalkorDB/falkordb-rs/compare/v0.10.0...v0.10.1) - 2026-06-29

### Other

- Added a "Dev Log" technical blog under `blog/`, built with [Zola](https://www.getzola.org/) and
  deployed to GitHub Pages. Every Rust snippet in a post is a real `examples/*.rs` / `benches/*.rs`
  file that CI already compiles, copied in verbatim at build time by `just blog-sync`, so the
  published code cannot drift from the crate; the async connection-strategy benchmark figures are a
  dated, reproducible sample run (`just bench-export`). Adds `just blog-build` / `blog-serve` /
  `blog-check` recipes, a `check-blog` pull-request gate, a Pages deploy workflow, and a `Blog`
  spellcheck task. The release workflow skips blog-only pushes (`paths-ignore`), so blog edits
  don't cut a crate release ([#279](https://github.com/FalkorDB/falkordb-rs/pull/279))

## [0.10.0](https://github.com/FalkorDB/falkordb-rs/compare/v0.9.0...v0.10.0) - 2026-06-23

### Added

- Build-time embedded-module bundling via a new `embedded-bundle` feature. A `build.rs` fetches the
  FalkorDB `falkordb.so` module for the build target at **compile time** and embeds it in the binary
  (`include_bytes!`), so the embedded server starts with **no runtime network access** — for
  network-isolated deployments. Build-time environment variables control the bundle:
  `FALKORDB_EMBEDDED_MODULE_VERSION` (release tag; defaults to a pinned version),
  `FALKORDB_EMBEDDED_MODULE_PLATFORM` (asset override for distro-specific Linux targets),
  `FALKORDB_EMBEDDED_MODULE_PATH` (embed a local `.so` for fully offline builds) and
  `FALKORDB_EMBEDDED_MODULE_SHA256` (required to embed a non-default downloaded version). Adds
  `EmbeddedServer::bundled_module_version()` / `bundled_module_platform()` accessors and a new
  `embedded-core` feature for the shared embedded logic. The embedded server now also pre-flights
  `redis-server --version` and fails early when it is older than the required 8.0
  ([#278](https://github.com/FalkorDB/falkordb-rs/pull/278))
- Opt-in replica routing for read-only queries via a new `ReadPreference` enum (`Primary`, the
  default, and `PreferReplica`). Set a client-wide default with
  `FalkorClientBuilder::with_read_preference`, or override per request with the `with_read_preference`
  method (and `prefer_replica()` / `primary_only()` shortcuts) on `QueryBuilder`,
  `ProcedureQueryBuilder` and `BatchBuilder`. Adds `FalkorSyncClient`/`FalkorAsyncClient` accessors
  `replica_reads_available()` (whether replica connections exist) and `read_preference()` (the
  client default) ([#277](https://github.com/FalkorDB/falkordb-rs/pull/277))

### Changed

- **Breaking (behavior):** read-only queries (`ro_query` / `call_procedure_ro`) and all-read batches
  now run on the **primary by default** instead of being routed to a replica automatically. Replicas
  apply writes only after the primary, so replica reads can be stale; routing them is now opt-in for
  accuracy. **To restore the previous replica offload, build the client with
  `.with_read_preference(ReadPreference::PreferReplica)`** (or opt in per request with
  `prefer_replica()`). Requesting a replica for a writable query, procedure or batch now fails with
  the new `FalkorDBError::ReadPreferenceNotReadOnly`. See the
  [0.10 migration guide](https://github.com/FalkorDB/falkordb-rs/blob/main/docs/migrating-to-0.10.md)
  ([#277](https://github.com/FalkorDB/falkordb-rs/pull/277))
- **Deprecated:** `FalkorSyncClient::reads_from_replicas` and `FalkorAsyncClient::reads_from_replicas`
  in favor of `replica_reads_available()` (replica capability) plus `read_preference()` (the routing
  policy), since a replica pool can now exist without reads being routed to it
  ([#277](https://github.com/FalkorDB/falkordb-rs/pull/277))

### Other

- The `README.md` is now generated from the crate-level `//!` documentation in `src/lib.rs` with
  [`cargo-rdme`](https://github.com/orium/cargo-rdme) (run `just readme`; a `check-readme` CI drift
  gate keeps it in sync). GitHub now syntax-highlights the Rust code blocks in the README and no
  longer shows the hidden `#` doctest lines, while those blocks still compile as doctests from the
  crate docs ([#275](https://github.com/FalkorDB/falkordb-rs/pull/275))
- Bump dependencies: `sha2` 0.10 → 0.11 and `which` 8.0.3 → 8.0.4 (runtime, `embedded` feature),
  `criterion` 0.5 → 0.8, `libc` 0.2.183 → 0.2.186 and `serde_json` 1.0.149 → 1.0.150
  (`dev-dependencies`), plus the `actions/checkout` (v6 → v7) and
  `rojopolis/spellcheck-github-actions` (0.61.0 → 0.62.0)
  GitHub Actions ([#274](https://github.com/FalkorDB/falkordb-rs/pull/274))

## [0.9.0](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.9...v0.9.0) - 2026-06-21

### Added

- Decode FalkorDB temporal values (`datetime`, `date`, `time`/`localtime`, `duration`) into the new
  typed `DateTime`, `Date`, `Time` and `Duration` values instead of surfacing them as `Unparseable`.
  Each exposes its scalar as a typed `Seconds` (`.seconds()`), and `DateTime`/`Duration` support a
  type-safe algebra (`DateTime - DateTime → Duration`, `DateTime ± Duration → DateTime`, and
  `Duration` add/subtract/negate) with overflow-checked `checked_*` variants; `Duration` also offers
  `.as_std_duration()` ([#265](https://github.com/FalkorDB/falkordb-rs/pull/265))
- Typed vector-index helpers `create_node_vector_index` / `create_edge_vector_index` (sync and
  async) plus a `VectorSimilarity` enum, so a correct `OPTIONS { dimension: N, similarityFunction:
  '…' }` clause is generated for you. Matching `create_node_vector_index_op` /
  `create_edge_vector_index_op` builders integrate with the wait ergonomics (`.wait()` blocks until
  the vector index is operational) ([#265](https://github.com/FalkorDB/falkordb-rs/pull/265))

### Fixed

- Vector index creation generated invalid Cypher (`OPTIONS { 'dimension':'4' }`) that FalkorDB
  rejected with a parse error; index `OPTIONS` now use unquoted identifier keys and emit numeric
  values (such as a vector `dimension`) unquoted, so vector indexes can actually be created
  ([#265](https://github.com/FalkorDB/falkordb-rs/pull/265))

### Changed

- **Breaking:** `FalkorValue` is now `#[non_exhaustive]` and gained `DateTime`, `Date`, `Time` and
  `Duration` variants. Exhaustive `match` expressions over a `FalkorValue` must add a wildcard
  (`_ => …`) arm ([#265](https://github.com/FalkorDB/falkordb-rs/pull/265))

## [0.8.9](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.8...v0.8.9) - 2026-06-20

### Other

- restructure the README into chapters with a table of contents, a Cargo feature-flags table, and a
  runnable-examples index ([#262](https://github.com/FalkorDB/falkordb-rs/pull/262))
- compile the README doctests and every example in CI so the documentation and examples can't
  silently drift ([#261](https://github.com/FalkorDB/falkordb-rs/pull/261))

## [0.8.8](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.7...v0.8.8) - 2026-06-19

### Other

- stop `release-plz` from emitting a duplicate Added section on release; it now only stamps the
  version header and entries are hand-written under the Unreleased section ([#258](https://github.com/FalkorDB/falkordb-rs/pull/258))

## [0.8.7](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.6...v0.8.7) - 2026-06-19

### Added

- record returned rows and server execution time on the execute span ([#255](https://github.com/FalkorDB/falkordb-rs/pull/255))
- add retry, in-flight and pool-wait observability signals ([#254](https://github.com/FalkorDB/falkordb-rs/pull/254))
- add optional `metrics` feature emitting bounded counters and histograms ([#253](https://github.com/FalkorDB/falkordb-rs/pull/253))
- enrich tracing spans with structured fields and a redacting query fingerprint ([#252](https://github.com/FalkorDB/falkordb-rs/pull/252))
- add opt-in RetryPolicy for transient connection failures ([#250](https://github.com/FalkorDB/falkordb-rs/pull/250))
- Optional `metrics` feature: emits per-operation counters, histograms and a gauge through the
  [`metrics`](https://docs.rs/metrics) facade (so the application installs any exporter) —
  `falkordb_queries_total`, `falkordb_query_duration_seconds`, `falkordb_query_errors_total`,
  `falkordb_retries_total`, `falkordb_connections_in_flight`, and `falkordb_connection_pool_wait_seconds`.
  All labels are **bounded, low-cardinality** (`command` allowlist, `operation`, `strategy`,
  `error_kind`, `route`); the graph name, query text and fingerprint are never used as labels. Retries
  also emit a low-cardinality `tracing` debug event. No-op until a recorder is installed.
- Observability: when the `tracing` feature is enabled, the query- and procedure-execution spans are
  enriched with structured, low-cardinality fields (named after the OpenTelemetry database semantic
  conventions): `db.system.name`, `db.namespace` (graph), `db.operation.name`, `db.falkordb.read_only`,
  `db.falkordb.strategy`, a privacy-safe `db.query.fingerprint`, and `error.type` on failure. The outer
  `execute` span additionally records `db.response.returned_rows` and `db.falkordb.server_time_ms` (the
  server's internal execution time, when reported). The raw query text and parameter values are
  **never** recorded by default — the fingerprint is a hash of the query with all literals redacted.
  Opt in to recording the raw Cypher (`db.query.text`) with the new
  `FalkorClientBuilder::with_query_logging(true)`.
- Opt-in, client-wide `RetryPolicy` that automatically re-issues *eligible* operations on *transient*
  connection failures with bounded backoff. **Disabled by default**, so existing behavior is
  byte-for-byte unchanged. Configure it on either builder with
  `FalkorClientBuilder::with_retry_policy(..)`; the available scope (`RetryScope::ReadOnly`) retries
  only read-only / idempotent operations, so enabling a policy never re-issues a write. New public
  types: `RetryPolicy`, `RetryScope`, `Backoff`. Backoff/jitter is powered internally by `backon`.

### Other

- require explicit human approval before merging to main ([#256](https://github.com/FalkorDB/falkordb-rs/pull/256))

## [0.8.6](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.5...v0.8.6) - 2026-06-18

### Added

- add FalkorDBError::mitigation_hint() for actionable error guidance ([#248](https://github.com/FalkorDB/falkordb-rs/pull/248))
- `FalkorDBError::mitigation_hint()` returns a short, actionable remediation hint for common,
  recognizable errors (or `None`). It is additive — `Display`/`Debug` and the raw message are
  unchanged — and hints are fixed `&'static str`s, so they never echo text from the underlying
  error.

## [0.8.5](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.4...v0.8.5) - 2026-06-18

### Added

- add an llms.txt AI-readable API surface with a drift gate ([#246](https://github.com/FalkorDB/falkordb-rs/pull/246))
- An `llms.txt` at the repository root ([#246](https://github.com/FalkorDB/falkordb-rs/pull/246)) —
  a curated, machine-readable summary of the public API, feature flags, idioms and pitfalls for AI
  coding assistants (the `llmstxt.org` convention). The narrative lives in `docs/llms.template.md`;
  the `## Public API` section is generated from `src/lib.rs`. Run `just llms` to regenerate it, and
  a `check-llms` CI gate keeps it from drifting as the public API changes.

## [0.8.4](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.3...v0.8.4) - 2026-06-18

### Other

- harden cargo network resilience and cache dependencies ([#244](https://github.com/FalkorDB/falkordb-rs/pull/244))

## [0.8.3](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.2...v0.8.3) - 2026-06-18

### Other

- add agent instructions for repository engineering conventions ([#242](https://github.com/FalkorDB/falkordb-rs/pull/242))
- check pull request titles for spelling before merge ([#241](https://github.com/FalkorDB/falkordb-rs/pull/241))

## [0.8.2](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.1...v0.8.2) - 2026-06-18

### Fixed

- *(test)* fix flaky copy-graph tests by retrying transient `ConnectionDown` ([#239](https://github.com/FalkorDB/falkordb-rs/pull/239))

## [0.8.1](https://github.com/FalkorDB/falkordb-rs/compare/v0.8.0...v0.8.1) - 2026-06-18

### Added

- batch & pipelined execution (`graph.batch()`) ([#236](https://github.com/FalkorDB/falkordb-rs/pull/236))
- batch / pipelined execution: `graph.batch()` queues several queries and dispatches them over a
  single Redis pipeline in **one round-trip**, returning one result per query in submission order
  (`BatchResult = FalkorResult<Vec<BatchItemResult>>`, where `BatchItemResult =
  FalkorResult<QueryResult<Vec<Row>>>`). Queue with `query`/`ro_query` (or `push` an owned
  `BatchQuery`), set per-query params/timeout, then `execute()` (sync) / `execute().await` (async).
  A failing query (bad Cypher or a parameter that cannot be encoded) is isolated to its own slot;
  the rest still run. A pipeline is not a transaction. Available on both `SyncGraph` and `AsyncGraph`.

### Fixed

- surface `slowlog` parse errors instead of silently dropping entries ([#237](https://github.com/FalkorDB/falkordb-rs/pull/237))
- `slowlog()` (sync and async) now surfaces a parse error for a malformed entry instead of
  silently dropping it (the previous `flat_map` swallowed per-entry errors). Behavior for valid
  replies is unchanged.


## [0.8.0](https://github.com/FalkorDB/falkordb-rs/compare/v0.7.0...v0.8.0) - 2026-06-17

### Added

- [**breaking**] async-native streaming results (Stream<Item = FalkorResult<Row>>) ([#234](https://github.com/FalkorDB/falkordb-rs/pull/234))

### Added

- [**breaking**] async-native streaming results: on the async client, `QueryResult::data` is now a
  `RowStream` — an owned `Stream<Item = FalkorResult<Row>>` that is `Send + 'static`. It can be
  moved into a spawned task and driven with the full `futures::StreamExt` / `TryStreamExt` toolbox
  (`.next().await`, `.try_collect().await`, `.map(..).buffer_unordered(n)`, etc.). With `serde`,
  `query_as::<T>()` yields a `TypedRowStream<T>` (a `Stream<Item = FalkorResult<T>>`).
- `RowStream::into_values_lossy`: opt-in escape hatch yielding `Vec<FalkorValue>` rows (mirrors the
  sync `LazyResultSet::into_values_lossy`).

### Changed

- **Not backward compatible (async only):** the async result iterator is now a `Stream`, not an
  `Iterator`. Pulling rows requires `.await` plus the relevant extension trait in scope
  (`use futures::StreamExt;` / `use futures::TryStreamExt;`). The synchronous client is unchanged
  (`LazyResultSet` is still a fallible `Iterator`). See the
  **[0.8 migration guide](docs/migrating-to-0.8.md)** for step-by-step upgrade instructions.
  Quick reference:

  | Before (≤ 0.7, async) | After (0.8, async) |
  | --- | --- |
  | `while let Some(row) = result.data.next() {` | `while let Some(row) = result.data.next().await {` (with `use futures::StreamExt;`) |
  | `result.data.collect::<FalkorResult<Vec<_>>>()` | `result.data.try_collect::<Vec<_>>().await` (with `use futures::TryStreamExt;`) |
  | wrap the graph in `Arc<Mutex<_>>` to share across tasks | clone the graph (cheap; shares the schema cache) |
  | `let result = graph.query(..).execute().await?;` borrowed the graph for the result's lifetime | the `RowStream` is owned (`Send + 'static`); move it into a task freely |

- **Not backward compatible (async only):** `AsyncGraph` now holds its schema cache behind a shared
  handle, so cloning an `AsyncGraph` shares one schema cache (previously each clone was independent).
  This makes concurrent streams from clones consistent and is what lets a cloned handle be used from
  a spawned task without `Arc<Mutex<_>>`.


## [0.7.0](https://github.com/FalkorDB/falkordb-rs/compare/v0.6.0...v0.7.0) - 2026-06-17

### Added

- [**breaking**] header-aware Row API with fallible result iteration ([#232](https://github.com/FalkorDB/falkordb-rs/pull/232))

### Added

- header-aware result rows: the default `QueryResult::data` now yields `FalkorResult<Row>`, where
  a `Row` pairs the result header with that row's values. Columns can be read by name or index,
  untyped (`get`, `get_at`, `get_all`) or typed (`try_get::<T>`, `try_get_at::<T>`), plus
  `columns`, `len`, `is_empty`, `into_values`, `into_map`, and (with `serde`)
  `Row::deserialize::<T>`. Duplicate column aliases are handled explicitly (first-match `get`,
  every-match `get_all`, last-wins `into_map`).
- `FromFalkorValue`: a trait for strict, fallible conversion of a `FalkorValue` into a Rust type
  (scalars, graph entities, `Option<T>`, `Vec<T>`, `HashMap<String, T>`); the bound behind
  `Row::try_get`. Conversions never coerce silently; the one lossless widening allowed is
  `i64` → `f64` within `±2^53`.
- `LazyResultSet::into_values_lossy`: opt-in escape hatch that reproduces the pre-0.7
  `Iterator<Item = Vec<FalkorValue>>` behavior.
- new `FalkorDBError` variants: `MissingColumn`, `ColumnIndexOutOfBounds`, `RowShapeMismatch`,
  and `TypeError`.

### Changed

- **Not backward compatible:** the default result iterator (`QueryResult::data`, i.e.
  `LazyResultSet`) now yields `FalkorResult<Row>` instead of `Vec<FalkorValue>`. A row that fails
  to parse is surfaced as an `Err` (which you can `?` or `collect::<FalkorResult<Vec<Row>>>()`)
  rather than being silently swallowed into a `[FalkorValue::Unparseable]` row. `QueryResult::header`
  is now `Arc<[String]>` (was `Vec<String>`), shared cheaply with every `Row`. See the
  **[0.7 migration guide](docs/migrating-to-0.7.md)** for step-by-step upgrade instructions.
  Quick reference:

  | Before (≤ 0.6) | After (0.7) |
  | --- | --- |
  | `for row in result.data { /* row: Vec<FalkorValue> */ }` | `for row in result.data { let row = row?; /* row: Row */ }` |
  | `&row[i]` / `row.into_iter()` | `row.get_at(i)` / `row.into_values()` |
  | align columns by header index by hand | `row.try_get::<T>("alias")` |
  | a silently swallowed `Unparseable` row | a real `Err` you `?` or handle |
  | the old lossy `Vec<FalkorValue>` rows | `result.data.into_values_lossy()` |
  | `result.header: Vec<String>` | `result.header: Arc<[String]>` (`&result.header[..]` still works) |

## [0.6.0](https://github.com/FalkorDB/falkordb-rs/compare/v0.5.0...v0.6.0) - 2026-06-17

### Added

- [**breaking**] type-safe, injection-proof query parameters ([#230](https://github.com/FalkorDB/falkordb-rs/pull/230)):
  `QueryBuilder::with_param`, `try_with_param`, `with_params`, and `with_raw_param`, backed by the
  sealed `IntoFalkorParam` / `IntoFalkorParams` traits and the `to_cypher_param` helper. Values
  (integers, floats, boolean values, strings, `Option`, arrays/`Vec`, string-keyed maps,
  `Point`/`Vec32`, `FalkorValue`) are encoded as escaped Cypher literals, and parameter names are
  validated.

### Changed

- **Not backward compatible:** `QueryBuilder::with_params` no longer takes
  `&HashMap<String, String>` of raw, pre-quoted values. Pass typed values instead — e.g.
  `.with_param("title", "The Matrix").with_param("year", 1999)` or
  `.with_params([("year", 1999)])`. String values are now encoded as Cypher *strings* (quoted and
  escaped); numbers that used to be passed as strings (`"30"`) should be passed as numbers (`30`),
  and any value that was a raw Cypher expression should use `with_raw_param`. Procedure-call
  arguments are likewise encoded safely.

## [0.5.0](https://github.com/FalkorDB/falkordb-rs/compare/v0.4.0...v0.5.0) - 2026-06-17

### Added

- [**breaking**] self-contained embedded FalkorDB with platform detection and caching ([#226](https://github.com/FalkorDB/falkordb-rs/pull/226))

### Other

- use `cargo add` for install instructions ([#228](https://github.com/FalkorDB/falkordb-rs/pull/228))

## [0.4.0](https://github.com/FalkorDB/falkordb-rs/compare/v0.3.0...v0.4.0) - 2026-06-17

### Added

- typed result mapping via `serde` ([#227](https://github.com/FalkorDB/falkordb-rs/pull/227)):
  an opt-in `serde` feature with `FalkorValue::deserialize_into` / `from_falkor_value`, a
  header-aware `QueryBuilder::query_as::<T>()` returning a `TypedLazyResultSet<T>`, and the
  `from_falkor_row` helper. Covered by property-based tests (run with `just proptest`).
- async client connection multiplexing via an explicit `ConnectionStrategy`
  (`Pooled` / `Multiplexed`), with `with_connection_strategy`, `with_max_inflight`, and a
  `connection_strategy()` accessor ([#224](https://github.com/FalkorDB/falkordb-rs/pull/224)).

### Changed

- **Not fully backward compatible:** the asynchronous client now defaults to
  `ConnectionStrategy::Multiplexed` instead of an exclusive borrow pool. This is
  source-compatible but behavior-changing. To keep the previous behavior, set it explicitly,
  e.g. `FalkorClientBuilder::new_async().with_connection_strategy(ConnectionStrategy::Pooled { size })`.
  Sentinel deployments transparently fall back to pooling.

## [0.3.0](https://github.com/FalkorDB/falkordb-rs/compare/v0.2.1...v0.3.0) - 2026-06-16

### Added

- route read-only queries to replica nodes via Sentinel ([#220](https://github.com/FalkorDB/falkordb-rs/pull/220))
- ergonomic builder API for waiting on background operations ([#221](https://github.com/FalkorDB/falkordb-rs/pull/221))
- *(builder)* expose TCP keepalive / TcpSettings ([#189](https://github.com/FalkorDB/falkordb-rs/pull/189))

### Other

- bump README install examples to 0.3.0 ([#222](https://github.com/FalkorDB/falkordb-rs/pull/222))
- Fix flaky test_copy_graph and make embedded tests pass on macOS ([#219](https://github.com/FalkorDB/falkordb-rs/pull/219))
- *(deps)* combine dependabot dependency bumps ([#218](https://github.com/FalkorDB/falkordb-rs/pull/218))
- *(deps)* combine dependabot bumps (codecov-action 7.0.0, which 8.0.3) ([#213](https://github.com/FalkorDB/falkordb-rs/pull/213))
- *(deps)* bump github/codeql-action from 4.36.0 to 4.36.2 ([#211](https://github.com/FalkorDB/falkordb-rs/pull/211))
- *(deps)* bump actions/checkout from 6.0.2 to 6.0.3 ([#209](https://github.com/FalkorDB/falkordb-rs/pull/209))
- *(deps)* bump cargo-deny-action to 2.0.20 and redis to 1.2.2 ([#208](https://github.com/FalkorDB/falkordb-rs/pull/208))
- *(deps)* combined dependabot updates ([#207](https://github.com/FalkorDB/falkordb-rs/pull/207))
- *(deps)* bump openssl in the cargo group across 1 directory ([#197](https://github.com/FalkorDB/falkordb-rs/pull/197))
- *(deps)* bump github/codeql-action from 4.35.2 to 4.35.3 ([#195](https://github.com/FalkorDB/falkordb-rs/pull/195))
- *(deps)* bump the cargo group across 1 directory with 2 updates
- *(deps)* combine all dependency updates ([#190](https://github.com/FalkorDB/falkordb-rs/pull/190))
- *(workflows)* pin GitHub Actions dependencies to commit SHAs ([#175](https://github.com/FalkorDB/falkordb-rs/pull/175))
- *(deps)* bump codecov/codecov-action from 5 to 6 ([#176](https://github.com/FalkorDB/falkordb-rs/pull/176))
- *(deps)* consolidate all dependency updates ([#177](https://github.com/FalkorDB/falkordb-rs/pull/177))

### Added

- Route read-only queries (`ro_query` / `call_procedure_ro`) to replica nodes
  when connected to a Redis Sentinel deployment with readable replicas. Adds
  `FalkorSyncClient::reads_from_replicas` / `FalkorAsyncClient::reads_from_replicas`
  to check whether replica routing is active. Fully backward compatible:
  deployments without replicas transparently fall back to the primary
  ([#127](https://github.com/FalkorDB/falkordb-rs/issues/127)).

## [0.2.1](https://github.com/FalkorDB/falkordb-rs/compare/v0.2.0...v0.2.1) - 2026-01-18

### Other

- Update wordlist.txt
- release v0.2.0 ([#112](https://github.com/FalkorDB/falkordb-rs/pull/112))

## [0.2.0](https://github.com/FalkorDB/falkordb-rs/releases/tag/v0.2.0) - 2026-01-18

### Other

- update cargo lock ([#154](https://github.com/FalkorDB/falkordb-rs/pull/154))
- Expose UDF API for loading, listing, and managing user-defined functions ([#152](https://github.com/FalkorDB/falkordb-rs/pull/152))
- *(deps)* bump strum from 0.27.1 to 0.27.2
- Merge branch 'main' into dependabot/cargo/main/regex-1.12.2
- *(deps)* bump which from 7.0.3 to 8.0.0
- Merge branch 'main' into dependabot/cargo/main/thiserror-2.0.17
- Merge branch 'main' into dependabot/github_actions/main/actions/checkout-6
- *(deps)* bump actions/checkout from 4 to 6
- Fix cargo-deny action to skip advisory database check ([#146](https://github.com/FalkorDB/falkordb-rs/pull/146))
- Add support for embedded FalkorDB server with comprehensive test coverage ([#135](https://github.com/FalkorDB/falkordb-rs/pull/135))
- Update wordlist.txt
- Create spellcheck.yml ([#107](https://github.com/FalkorDB/falkordb-rs/pull/107))
- clean deny errors
- update deps

## [0.1.11](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.10...v0.1.11) - 2025-02-13

### Fixed

- fix deny errors and warns

### Other

- *(deps)* bump thiserror from 2.0.6 to 2.0.11 (#81)
- Fix example add mut to node ([#88](https://github.com/FalkorDB/falkordb-rs/pull/88))
- update lock file
- *(deps)* bump redis from 0.28.1 to 0.28.2
- Merge branch 'main' into dependabot/cargo/main/redis-0.28.1
- Update coverage.yml
- *(deps)* bump tokio from 1.42.0 to 1.43.0

## [0.1.10](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.9...v0.1.10) - 2024-12-10

### Fixed

- fix read only query builder ([#77](https://github.com/FalkorDB/falkordb-rs/pull/77))

### Other

- *(deps)* bump codecov/codecov-action from 4 to 5 (#71)

## [0.1.9](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.8...v0.1.9) - 2024-12-04

### Other

- Update dependencies ([#74](https://github.com/FalkorDB/falkordb-rs/pull/74))
- Fix async connection leak and co ([#72](https://github.com/FalkorDB/falkordb-rs/pull/72))

## [0.1.8](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.7...v0.1.8) - 2024-11-14

### Other

- Allow to trigger github workflow manually ([#68](https://github.com/FalkorDB/falkordb-rs/pull/68))
- Update version in readme

## [0.1.7](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.6...v0.1.7) - 2024-11-14

### Other

- Update coverage.yml ([#55](https://github.com/FalkorDB/falkordb-rs/pull/55))
- Add test for when sending cypher query with syntax error the result s… ([#67](https://github.com/FalkorDB/falkordb-rs/pull/67))
- Update dependencies ([#66](https://github.com/FalkorDB/falkordb-rs/pull/66))
- Pass redis error to upstream instead of sending the generic FalkorDBError::ParsingArray ([#64](https://github.com/FalkorDB/falkordb-rs/pull/64))

## [0.1.6](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.5...v0.1.6) - 2024-10-14

### Other

- Prepare for release v0.1.6
- Re add fields to FalkorIndex, test.
- Fix error message of ParsingArrayToStructElementCount
- Improve Vec32 parsing and add comprehensive tests
- Fix failing unit tests, add more vec32 test
- add support with the new vec32

## [0.1.5](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.4...v0.1.5) - 2024-08-15

### Fixed
- Update crates to avoid CVEs ([#35](https://github.com/FalkorDB/falkordb-rs/pull/35))

### Other
- crates io bug
- Update deny action
- Update redis and tokio version, and fix compatibility issues
- Update redis and tokio version, and fix compatibility issues
- Update README
- Remove deny from needs
- *(deps)* bump thiserror from 1.0.61 to 1.0.62 ([#30](https://github.com/FalkorDB/falkordb-rs/pull/30))
- *(deps)* bump strum from 0.26.2 to 0.26.3 ([#28](https://github.com/FalkorDB/falkordb-rs/pull/28))

## [0.1.4](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.3...v0.1.4) - 2024-06-20

### Fixed
- Use MIT license ([#25](https://github.com/FalkorDB/falkordb-rs/pull/25))

## [0.1.3](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.2...v0.1.3) - 2024-06-18

### Fixed
- Add codecov yaml
- outdated README

## [0.1.2](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.1...v0.1.2) - 2024-06-18

### Added
- Implement Async Graph API ([#23](https://github.com/FalkorDB/falkordb-rs/pull/23))
- implement QueryResult properly ([#19](https://github.com/FalkorDB/falkordb-rs/pull/19))

### Fixed
- Avert Parsing Latency By Rewriting Parser ([#22](https://github.com/FalkorDB/falkordb-rs/pull/22))

### Other
- *(deps)* bump regex from 1.10.4 to 1.10.5 ([#17](https://github.com/FalkorDB/falkordb-rs/pull/17))

## [0.1.1](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.0...v0.1.1) - 2024-06-09

### Added
- LazyResultSet implementation, allowing one-by-one parsing ([#14](https://github.com/FalkorDB/falkordb-rs/pull/14))

### Fixed
- Update badges

## [0.1.0](https://github.com/FalkorDB/falkordb-rs/releases/tag/v0.1.0) - 2024-06-06

### Fixed
- Update readme file name before releasing, update job name ([#7](https://github.com/FalkorDB/falkordb-rs/pull/7))
- cleaned CI and fixed Code Coverage job, which was queued indefinitely ([#5](https://github.com/FalkorDB/falkordb-rs/pull/5))

### Other
- Fix doctests, some more code shrinking ([#9](https://github.com/FalkorDB/falkordb-rs/pull/9))
- Initial Development ([#1](https://github.com/FalkorDB/falkordb-rs/pull/1))
- gitignore
