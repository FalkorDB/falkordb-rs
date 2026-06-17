# Next steps — 10 ideas for falkordb-rs

This document collects ten high-value, concrete improvement ideas for the next phase of
`falkordb-rs`. They span API ergonomics, performance, memory, ease of use, accessibility to
AI tools, and documentation.

## How these were chosen

The ideas were produced with a **multi-LLM brainstorm** (a "rubber-duck review" with more
than one model). Three models brainstormed independently from the same grounded notes about
the codebase, and a critic pass flagged low-value or risky directions:

- **GPT-5.5** — 15 candidate ideas.
- **Gemini 3.1 Pro** — 14 candidate ideas.
- **Rubber-duck critic** — 14 candidates plus a "traps to avoid" critique.

Seven ideas surfaced independently in all three runs (typed result mapping, type-safe
parameters, batching/pipelining, MCP server, `llms.txt`, MSRV policy, richer observability),
which is a strong signal. The notes below were grounded in the current code:

- Query results are an untyped `FalkorValue` enum; there is no `serde` integration and no
  derive macro, so callers hand-match every value.
- `QueryBuilder::with_params` takes `&HashMap<String, String>`: callers must pre-stringify
  and escape values themselves, which is error-prone and an injection risk.
- `LazyResultSet` turns parse failures into `FalkorValue::Unparseable(String)`, silently
  hiding real errors.
- Result headers (`Vec<String>`) are exposed separately from row data, so callers align
  columns by hand.
- There is no batch or pipeline API: one network round-trip per query.
- `AsyncGraph` borrows `&mut` state and is awkward to move into spawned tasks.
- `Cargo.toml` declares no `rust-version` (MSRV).
- There is no `llms.txt`, MCP server, or other AI-tool-facing surface.
- Labels, property keys, and schema names are stored as heavily allocated `String`s.

Each idea lists the problem it solves, a sketch of the approach, and rough **impact**
(H/M/L) and **effort** (S/M/L), plus the main risk. They are ordered roughly by
value-for-effort.

---

## 1. Typed result mapping (serde + derive)

- **Problem:** Mapping a query result into an application struct means manually walking the
  `FalkorValue` enum for every field — the single biggest ergonomics gap in the library.
- **Approach:** Add an optional `serde` feature and a `FromFalkorRow` / `FromFalkorValue`
  trait, then a `query_as::<T>()` entry point. Provide a companion `falkordb-derive` crate
  with `#[derive(FromFalkorRow)]` and `#[derive(FromFalkorNode)]` supporting field renames,
  `Option<T>` for missing properties, and label hints. Keep `FalkorValue` first-class —
  mapping is additive, never the only API.
- **Impact:** H — **Effort:** L.
- **Risk:** Graph entities are heterogeneous; start narrow (scalars, maps, arrays,
  `Option<T>`, structs from row headers / node properties) and grow from there.

## 2. Type-safe, injection-proof query parameters

- **Problem:** `with_params(&HashMap<String, String>)` forces callers to stringify and
  escape values themselves, inviting quoting bugs and Cypher injection.
- **Approach:** Introduce `IntoFalkorParam` covering integers, floats, bools, strings,
  arrays, maps, null, points, and vectors, with exact Cypher literal encoding. Offer both
  `with_param("age", 30)` and a `params! { "age" => 30, "tags" => ["a", "b"] }` macro in
  the style of `serde_json::json!`.
- **Impact:** H — **Effort:** M.
- **Risk:** Escaping must be exact; cover quotes, backslashes, unicode, nested arrays/maps,
  and adversarial strings with thorough tests.

## 3. Header-aware `Row` API and fallible iteration

- **Problem:** Headers are returned separately from row data, and parse failures are
  silently folded into `FalkorValue::Unparseable(String)`, hiding data corruption and
  schema-cache bugs.
- **Approach:** Add a `Row` abstraction that zips headers with values:
  `row.get("alias")`, `row.try_get::<T>("alias")`, `row.into_map()`. Add a
  `try_next() -> Option<FalkorResult<Row>>` (or a `TryLazyResultSet`) that surfaces parse
  errors instead of `Unparseable`. Keep the existing iterator for compatibility but
  document it as lossy.
- **Impact:** H — **Effort:** S/M.
- **Risk:** Duplicate column aliases need a defined rule (reject for name lookup, or return
  the first deterministically).

## 4. Batch and pipelined execution

- **Problem:** Ingesting or reading many graphs/queries incurs one round-trip each;
  multiplexing helps concurrency but there is no single-batch API.
- **Approach:** Add `graph.batch()` that queues multiple queries and dispatches them via
  `redis::Pipeline` in one round-trip, returning results in order with per-item error
  attribution. Begin with read-only / scalar batches (or require an eager schema prefetch),
  because compact entity parsing depends on the schema cache.
- **Impact:** H — **Effort:** L.
- **Risk:** Error attribution and mid-batch schema refresh make compact-result batching
  tricky; design schema handling up front.

## 5. Async-native results: `Stream` plus a cloneable graph handle

- **Problem:** Async result sets do not implement `futures::Stream`, and `AsyncGraph`
  borrows `&mut` state, so it is awkward to combine with `StreamExt` or move into tasks.
- **Approach:** Implement `Stream<Item = FalkorResult<Row>>` for async result sets, and
  introduce a cheap `Send + Clone` graph handle that keeps the schema cache behind an
  `Arc<RwLock<_>>` so queries can take `&self`. Keep the current API during transition.
- **Impact:** H — **Effort:** L.
- **Risk:** Schema refresh must never perform network I/O while holding a write lock;
  check-under-read, fetch, then update-under-write.

## 6. Memory diet: intern schema strings and cut allocations

- **Problem:** Every node/edge allocates `String`s for labels, property keys, and schema
  names, creating real pressure on large result sets.
- **Approach:** Intern schema/label/key strings as `Arc<str>` (shared from the schema
  cache), pre-size maps from header width, and avoid intermediate collections in the
  parser. Add allocation-aware benchmarks (extend the existing `resource_usage` bench) to
  guide the work. Defer a full borrowed `FalkorValue<'a>` / `Cow` redesign — see the traps
  appendix.
- **Impact:** M/H — **Effort:** M.
- **Risk:** Without measurements the gains are marginal; benchmark before and after.

## 7. FalkorDB MCP server companion crate

- **Problem:** AI coding agents cannot explore a FalkorDB instance to help write or debug
  queries — a major differentiator in the current tooling landscape.
- **Approach:** Ship a **separate** `falkordb-mcp` crate/binary built on this client that
  exposes Model Context Protocol tools: `execute_cypher_read`, `get_schema` (labels and
  relationship types), `explain` / `profile`, and a guarded write tool. Keep it out of the
  core library so the client stays lean.
- **Impact:** H — **Effort:** M.
- **Risk:** A separate binary to maintain; keep its tool surface thin and version-aligned.

## 8. AI-readable surface: `llms.txt` and structured error diagnostics

- **Problem:** No machine-readable API summary exists, so AI tools hallucinate methods, and
  raw database errors are opaque strings with no remediation guidance.
- **Approach:** Add a CI-generated `llms.txt` summarizing the public API, feature flags,
  idioms ("always parameterize", "how to map `FalkorValue`"), and pitfalls. Parse server
  errors into a richer `FalkorDBError` with a `.mitigation_hint()` (for example: "graph not
  found — create it first"). Regenerate `llms.txt` in CI so it cannot drift.
- **Impact:** M/H — **Effort:** S for `llms.txt`, M for diagnostics.
- **Risk:** Hints depend on server error formats; keep the raw message and treat unknown
  shapes as non-fatal.

## 9. Declare an MSRV and tighten feature-flag hygiene

- **Problem:** `Cargo.toml` has no `rust-version`, so downstream users cannot tell when a
  Rust upgrade is required, and the feature matrix (sync / tokio / TLS / tracing / embedded)
  is under-documented.
- **Approach:** Pick and publish an MSRV, add a CI job that builds against it (pinned
  toolchain or `cargo msrv`), audit features so the minimal sync client stays lean, and
  document supported feature combinations in the README.
- **Impact:** M — **Effort:** S/M.
- **Risk:** Dependencies (for example `redis`, `which`) may force a higher MSRV than
  desired; prefer a realistic modern baseline over compatibility theater.

## 10. Production observability and opt-in resilience

- **Problem:** The `tracing` feature exists but offers little operational signal, and retry
  behavior is limited and not user-tunable.
- **Approach:** Enrich spans with low-cardinality fields (graph name, command, read/write,
  connection strategy, pool wait time, in-flight count) and a query **fingerprint** rather
  than raw text. Add an optional `metrics` feature emitting counters/histograms. Add an
  opt-in `RetryPolicy` on the builder (max attempts, backoff, retryable-error
  classification) that defaults to connection setup and read-only operations only.
- **Impact:** M/H — **Effort:** M.
- **Risk:** Never log raw query literals by default (PII/secrets), and never auto-retry
  writes (duplicate side effects).

---

## Cross-cutting: task-oriented cookbook docs

Alongside the above, a "cookbook" guide of compile-tested recipes would multiply their
value: map a node to a struct, run concurrent read-only queries, choose pooled vs.
multiplexed, connect over rustls, test with the embedded server, handle schema refresh, and
run a vector search. Prefer examples that compile in CI so they cannot rot.

## Appendix: traps to avoid

The critic pass flagged directions that sound attractive but are low-value or risky for
this library:

1. **A full Cypher query builder / ORM** — high maintenance and always incomplete. Typed
   parameters (#2) plus result mapping (#1) capture most of the value with far less risk.
2. **Making serde mapping the only result API** — graph data is heterogeneous; keep
   `FalkorValue`, `Node`, `Edge`, and raw rows first-class.
3. **Auto-retrying all failed queries** — dangerous for writes (`CREATE`/`DELETE` duplicate
   effects). Default retries to connection setup and read-only operations.
4. **Logging raw query strings by default** — queries may carry PII or secrets; default to
   fingerprints, make raw text opt-in.
5. **Shipping MCP inside the core crate** — it would bloat dependencies and confuse feature
   flags. Keep it a separate crate/binary.
6. **Over-investing in zero-copy lifetimes early** — a borrowed `FalkorValue<'a>` would make
   the public API virally complex. First reduce obvious clones via interning (#6) and
   benchmark.
7. **Promising full browser/WASM support** — Redis TCP, TLS, and tokio assumptions make a
   networked WASM client unrealistic. At most, keep value/parsing/mapping utilities
   WASM-compatible.
8. **Batching without a schema/parsing design** — compact results can trigger schema
   refreshes mid-parse; design this before promising throughput wins (#4).
