# falkordb

> Rust client for [FalkorDB](https://www.falkordb.com/), a Redis-module graph database. Sync and
> async (tokio) clients, type-safe injection-proof query parameters, header-aware fallible result
> rows, async `Stream` results, pipelined batches, optional `serde` result mapping, and an optional
> embedded server for tests. Errors are `FalkorResult<T> = Result<T, FalkorDBError>`.

## Install & features

- `cargo add falkordb`
- Cargo features (all off by default): `tokio` (async client + `Stream` results), `serde` (map rows
  into your own types), `embedded` (spawn a local FalkorDB, e.g. for tests), `rustls` / `native-tls`
  (with `tokio-rustls` / `tokio-native-tls` for the async client) for TLS, and `tracing`.

## Core idioms (do this)

- Build a client, then select a graph handle:
  `let client = FalkorClientBuilder::new().build()?;` then `let mut graph = client.select_graph("g");`
- Always parameterize — never `format!` user input into Cypher:
  `graph.query("MATCH (n {id: $id}) RETURN n").with_param("id", 42).execute()?`
  (`with_param` / `try_with_param` / `with_params`; optionally `with_timeout(ms)`).
- Iterate the fallible result rows by column alias:
  `for row in result.data { let n: Node = row?.try_get("n")?; }`
  (`data` is the result set; `Row::try_get::<T>("alias")` / `try_get_at::<T>(index)`).
- Read-only queries route to replicas: `graph.ro_query("MATCH …")`.
- Map a result straight into your own type with `serde`:
  `graph.query("…").query_as::<MyRow>().execute()?` (or `FalkorValue::deserialize_into::<T>()`).
- Async (`tokio` feature): build with `FalkorClientBuilder::new_async().build().await?` →
  `FalkorAsyncClient`, then `select_graph(...)` → `AsyncGraph`. Every terminal is awaited
  (`graph.query("…").execute().await?`), and the async `data` is a `Stream` (`RowStream`) — bring
  `use futures::StreamExt;` and `while let Some(row) = stream.next().await { … }`. Share an
  `AsyncGraph` across tasks by `clone()` (it shares one schema cache); no `Arc<Mutex<_>>` needed.
- Run many queries in one round-trip with a batch:
  `let mut b = graph.batch(); b.query("…"); b.ro_query("…"); let results = b.execute()?;` —
  one result per query, in submission order, each independently fallible.
- On an error, `err.mitigation_hint()` may return a short, actionable remediation tip (or `None`);
  the raw `FalkorDBError` is always preserved.

## Pitfalls (avoid)

- Don't `format!`/concatenate user input into Cypher — use `with_param` / `with_params` (sealed
  `IntoFalkorParam`, injection-proof).
- A read against a graph that doesn't exist yet errors — create it with a write query first.
- The async client's `data` is a `Stream`, not a sync `Iterator` / `LazyResultSet`; don't loop it
  without `StreamExt` + `.next().await`.
- A batch is one pipeline, not a transaction: a failing query is isolated to its own slot while the
  rest still run.
- `query_as` and the other `serde` items require the `serde` feature; the async items require
  `tokio`; the embedded server requires `embedded`.

## Public API

The names a downstream crate can import (`use falkordb::…`), each annotated with the Cargo feature
that gates it. This section is auto-generated from `src/lib.rs` by `just llms` — do not edit it by
hand; if you change the public API, run `just llms` and commit the updated `llms.txt`.

<!-- BEGIN API -->
<!-- END API -->

## Links

- API docs: <https://docs.rs/falkordb>
- Repository & README: <https://github.com/FalkorDB/falkordb-rs>
- Examples (in the `examples/` directory): `basic_usage`, `typed_params`, `rows`,
  `typed_mapping`, `async_api`, `async_stream`, `multiplexed_async`, `readonly_replica`, `batch`,
  `embedded_usage`, `udf_usage`.
- FalkorDB: <https://www.falkordb.com/>
