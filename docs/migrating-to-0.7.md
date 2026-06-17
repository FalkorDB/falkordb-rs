# Migrating to 0.7

Version 0.7 makes query results **header-aware** and **fallible**: iterating `QueryResult::data`
now yields `FalkorResult<Row>` instead of `Vec<FalkorValue>`. This guide walks through every change
and shows the before/after for each pattern.

If you are upgrading from 0.5 or earlier you will also cross the 0.6 query-parameter change; see
[Coming from 0.6 or earlier](#coming-from-06-or-earlier) at the end.

## Why it changed

Two long-standing warts are fixed at once:

- **Errors were swallowed.** A row that failed to parse used to be turned into a fake single-column
  `[FalkorValue::Unparseable(message)]` row instead of an error. It was easy to miss, lossy, and it
  distorted the row shape.
- **The header was separate from the values.** Column names lived in `QueryResult::header` and the
  values in each row were related to them only by position, so there was no `row.get("alias")`.

Making `data` yield `FalkorResult<Row>` solves both: a parse failure is a real `Err`, and every
`Row` carries its header so you can read columns by name.

## At a glance

| Before (≤ 0.6) | After (0.7) |
| --- | --- |
| `for row in result.data { /* row: Vec<FalkorValue> */ }` | `for row in result.data { let row = row?; /* row: Row */ }` |
| `row[i]` / `row.into_iter().next()` | `row.get_at(i)` / `row.try_get_at::<T>(i)` |
| align a value to its column by hand | `row.try_get::<T>("alias")` |
| a silently swallowed `Unparseable` row | a real `Err` you `?` or handle |
| keep the old lossy `Vec<FalkorValue>` rows | `result.data.into_values_lossy()` |
| `result.header: Vec<String>` | `result.header: Arc<[String]>` (`&result.header[..]` still works) |

## 1. Iterating results

`data` is now an iterator of `FalkorResult<Row>`. The smallest change is to unwrap each row with
`?` (or `.expect(...)` / `match`).

Before:

```rust
let mut result = graph.query("MATCH (m:Movie) RETURN m.title, m.year").execute()?;
for row in result.data.by_ref() {
    // row: Vec<FalkorValue>
    let title = row[0].as_string().cloned();
    let year = row[1].to_i64();
    println!("{title:?} {year:?}");
}
```

After:

```rust
let mut result = graph
    .query("MATCH (m:Movie) RETURN m.title AS title, m.year AS year")
    .execute()?;
for row in result.data.by_ref() {
    let row = row?; // row: Row
    let title: String = row.try_get("title")?;
    let year: i64 = row.try_get("year")?;
    println!("{title} {year}");
}
```

Two things to note:

- Add `AS` aliases in the query so columns have stable names to read by (`row.try_get("title")`).
  Without an alias the column name is whatever the server returns (for example `m.title`).
- `try_get::<T>` converts the value for you using the new [`FromFalkorValue`] trait, so you no longer
  match on `FalkorValue` variants by hand.

If you prefer index access (no alias needed), use `get_at` (borrowing) or `try_get_at::<T>`
(converting):

```rust
let row = row?;
let title: String = row.try_get_at(0)?;
let raw_year = row.get_at(1); // Option<&FalkorValue>
```

## 2. Collecting a whole result set

`collect` short-circuits on the first `Err`, so a result set gathers into a single
`FalkorResult<Vec<_>>`.

Before:

```rust
let rows: Vec<Vec<FalkorValue>> = result.data.collect();
```

After:

```rust
let rows: Vec<Row> = result.data.collect::<FalkorResult<Vec<Row>>>()?;
```

Mapping each row to a typed value works the same way:

```rust
let titles: Vec<String> = result
    .data
    .map(|row| row?.try_get::<String>("title"))
    .collect::<Result<_, _>>()?;
```

## 3. Errors are no longer swallowed

Previously a corrupt row appeared as `[FalkorValue::Unparseable(message)]` and was easy to miss.
Now it is an `Err` you handle explicitly:

```rust
for row in result.data.by_ref() {
    match row {
        Ok(row) => { /* use row */ }
        Err(err) => eprintln!("skipping unreadable row: {err}"),
    }
}
```

A row whose value count does not match the header length is rejected with
`FalkorDBError::RowShapeMismatch` rather than silently truncated.

## 4. Keeping the old behavior (escape hatch)

If you cannot adopt the fallible, header-aware iteration yet, call `into_values_lossy()` on the
result set to get back the pre-0.7 iterator (bare `Vec<FalkorValue>` rows, parse errors collapsed
to a single `FalkorValue::Unparseable`):

```rust
for row in result.data.into_values_lossy() {
    // row: Vec<FalkorValue>, exactly as in 0.6
}
```

This is meant as a temporary bridge; prefer migrating to `Row`.

## 5. `QueryResult::header` is now `Arc<[String]>`

The header changed from `Vec<String>` to `Arc<[String]>` so it can be shared cheaply with every
`Row`. Most read-only uses are unchanged because `Arc<[String]>` dereferences to `[String]`:

```rust
let n = result.header.len();
for name in result.header.iter() { /* ... */ }
let first = &result.header[0];
let slice: &[String] = &result.header;
```

If you previously took ownership of the `Vec<String>` (for example `let h: Vec<String> =
result.header;`), clone it from the slice instead: `let h: Vec<String> = result.header.to_vec();`.

## 6. Typed result mapping with `serde` (`query_as`) is unchanged

`query_as::<T>()` still works exactly as before and is the most concise option when you have a
target type:

```rust
let movies: Vec<Movie> = graph
    .query("MATCH (m:Movie) RETURN m")
    .query_as::<Movie>()
    .execute()?
    .data
    .collect::<Result<_, _>>()?;
```

Internally it now maps each `Row` with `Row::deserialize`, but the public behavior is the same. You
can also deserialize a single row on demand with `row.deserialize::<T>()`.

## Duplicate column names

The server rejects a query that returns two columns with the same name, so in practice a `Row` has
unique columns. The API still defines the behavior for completeness: `get` / `try_get` return the
**first** match, `get_all` returns **every** match, and `into_map` keeps the **last**. Prefer
distinct aliases.

## Coming from 0.6 or earlier

Upgrading from 0.5 or earlier also crosses the **0.6** query-parameter change: `with_params` no
longer takes a `&HashMap<String, String>` of raw, pre-quoted values. Pass typed values and let the
client encode them:

```rust
graph
    .query("CREATE (:Movie {title: $title, year: $year})")
    .with_param("title", "The Matrix")
    .with_param("year", 1999)
    .execute()?;
```

See the [CHANGELOG](../CHANGELOG.md) `0.6.0` entry and the
[type-safe query parameters](../README.md#type-safe-query-parameters) section for the full details.

[`FromFalkorValue`]: https://docs.rs/falkordb/latest/falkordb/trait.FromFalkorValue.html
