# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
