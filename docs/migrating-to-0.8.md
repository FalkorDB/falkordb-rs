# Migrating to 0.8

Version 0.8 makes **async** query results stream-native. On the async client, `QueryResult::data`
is now a `Stream` (`RowStream`) instead of an `Iterator`, so it composes with the
`StreamExt` / `TryStreamExt` ecosystem, is `Send + 'static`, and can be moved into a
spawned task. The **synchronous** client is unchanged.

If you are upgrading from 0.6 or earlier you will also cross the 0.7 header-aware-row change; see
[Coming from 0.7 or earlier](#coming-from-07-or-earlier) at the end.

## Why it changed

The async result set used to be an `Iterator`, which forced two awkward patterns:

- **You could not use the async ecosystem.** Driving results meant a blocking-style `while let
  Some(row) = data.next()` loop; you could not `.try_collect().await`, fan out with
  `.buffer_unordered(n)`, or otherwise compose with `futures`/`tokio` combinators.
- **Sharing a graph across tasks needed `Arc<Mutex<_>>`.** The result borrowed the graph, and an
  `AsyncGraph` could not be moved into a spawned task, so concurrent work required wrapping the
  handle in `Arc<Mutex<_>>`.

Making `data` a `RowStream` (an owned `Stream<Item = FalkorResult<Row>>` that is `Send + 'static`)
solves both: results compose with `StreamExt`/`TryStreamExt`, and because cloning an `AsyncGraph`
now shares one schema cache, you clone the handle to use it from several tasks — no `Arc<Mutex<_>>`.

## At a glance (async client)

| Before (≤ 0.7, async) | After (0.8, async) |
| --- | --- |
| `while let Some(row) = result.data.next() {` | `while let Some(row) = result.data.next().await {` (with `use futures::StreamExt;`) |
| `result.data.collect::<FalkorResult<Vec<_>>>()` | `result.data.try_collect::<Vec<_>>().await` (with `use futures::TryStreamExt;`) |
| `result.data.map(..).collect::<Result<_,_>>()` | `result.data.map(..).try_collect().await` |
| wrap the graph in `Arc<Mutex<_>>` to share across tasks | clone the graph (cheap; shares the schema cache) |
| the result borrowed the graph for its lifetime | the `RowStream` is owned (`Send + 'static`); move it into a task |
| async `query_as::<T>()` → typed `Iterator` | async `query_as::<T>()` → `TypedRowStream<T>` (a `Stream`) |

> The synchronous client is **not** affected: `LazyResultSet` is still a fallible
> `Iterator<Item = FalkorResult<Row>>`, and synchronous code needs no changes for 0.8.

## 1. Iterating async results

`data` is now a `Stream`, so pulling rows needs `.await` and the `StreamExt` extension trait in
scope.

Before:

```rust
let mut result = graph.query("MATCH (m:Movie) RETURN m.title AS title").execute().await?;
while let Some(row) = result.data.next() {
    let row = row?;
    let title: String = row.try_get("title")?;
    println!("{title}");
}
```

After:

```rust
use futures::StreamExt; // brings `.next().await`

let mut result = graph.query("MATCH (m:Movie) RETURN m.title AS title").execute().await?;
while let Some(row) = result.data.next().await {
    let row = row?;
    let title: String = row.try_get("title")?;
    println!("{title}");
}
```

## 2. Collecting a whole async result set

Use `TryStreamExt::try_collect`, which short-circuits on the first `Err` just like the old
`collect::<FalkorResult<Vec<_>>>()`.

Before:

```rust
let rows: Vec<Row> = result.data.collect::<FalkorResult<Vec<Row>>>()?;
```

After:

```rust
use futures::TryStreamExt;

let rows: Vec<Row> = result.data.try_collect().await?;
```

Mapping each row to a typed value composes the same way:

```rust
use futures::TryStreamExt;

let titles: Vec<String> = result
    .data
    .map(|row| row?.try_get::<String>("title"))
    .try_collect()
    .await?;
```

## 3. The result stream is owned and `Send + 'static`

A `RowStream` no longer borrows the graph, so you can move it into a spawned task:

```rust
use futures::StreamExt;

let mut stream = graph.query("MATCH (n) RETURN n").execute().await?.data;
let count = tokio::spawn(async move {
    let mut n = 0usize;
    while let Some(row) = stream.next().await {
        row?;
        n += 1;
    }
    Ok::<_, falkordb::FalkorDBError>(n)
})
.await
.unwrap()?;
```

## 4. Share a graph across tasks by cloning (no more `Arc<Mutex<_>>`)

Cloning an `AsyncGraph` is cheap and **shares one schema cache**, so concurrent tasks stay
consistent. Clone the handle into each unit of work instead of wrapping it in `Arc<Mutex<_>>`:

```rust
use futures::{StreamExt, TryStreamExt};

let enriched: Vec<i64> = graph
    .query("MATCH (m:Movie) RETURN m.year AS year")
    .execute()
    .await?
    .data
    .map(|row| {
        let mut g = graph.clone(); // shares the schema cache
        async move {
            let year: i64 = row?.try_get("year")?;
            let mut r = g.query(format!("RETURN {year} + 1 AS next")).execute().await?;
            r.data.try_next().await?.expect("a row").try_get::<i64>("next")
        }
    })
    .buffer_unordered(8)
    .try_collect()
    .await?;
```

Query methods still take `&mut self`, so a single task drives one query at a time on a given handle;
concurrency comes from cloning the handle (each clone can run a query independently while sharing the
schema cache). A runnable end-to-end example lives in
[`examples/async_stream.rs`](../examples/async_stream.rs).

## 5. Typed mapping with `serde` (`query_as`) is now a stream

On the async client, `query_as::<T>()` yields a `TypedRowStream<T>` — a
`Stream<Item = FalkorResult<T>>`. Collect it with `try_collect().await`:

Before:

```rust
let movies: Vec<Movie> = graph
    .query("MATCH (m:Movie) RETURN m")
    .query_as::<Movie>()
    .execute()
    .await?
    .data
    .collect::<Result<_, _>>()?;
```

After:

```rust
use futures::TryStreamExt;

let movies: Vec<Movie> = graph
    .query("MATCH (m:Movie) RETURN m")
    .query_as::<Movie>()
    .execute()
    .await?
    .data
    .try_collect()
    .await?;
```

## 6. Keeping `Vec<FalkorValue>` rows (escape hatch)

As with the sync client, `RowStream::into_values_lossy()` yields `Vec<FalkorValue>` rows (parse
errors collapsed to a single `FalkorValue::Unparseable`) if you are not ready to adopt header-aware
`Row` values:

```rust
use futures::StreamExt;

let mut rows = result.data.into_values_lossy();
while let Some(row) = rows.next().await {
    // row: Vec<FalkorValue>
}
```

Prefer migrating to `Row`; this is a temporary bridge.

## Coming from 0.7 or earlier

Upgrading from 0.6 or earlier also crosses the **0.7** header-aware-row change (`data` yields
`FalkorResult<Row>` instead of `Vec<FalkorValue>`). The row-level steps are identical for the sync
and async clients; only the iteration mechanism differs (async needs `.await` + `StreamExt`). See
the **[0.7 migration guide](migrating-to-0.7.md)** for the row API, then apply the async changes
above.
