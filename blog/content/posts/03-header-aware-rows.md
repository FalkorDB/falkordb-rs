+++
title = "Rows that don't lie: header-aware, fallible result iteration"
date = 2026-06-09
description = "A result API where every row is a FalkorResult<Row>, columns are read by name, and a value that fails to parse is an Err you can handle — not a panic."
[taxonomies]
tags = ["rust", "api-design", "error-handling"]
[extra]
part = "Part 3 of 8"
author = "Barak"
+++

In [Part 2](@/posts/02-typed-params.md) we made the data going *in* honest. This post is about the
data coming *out*.

The old result API had two original sins, and if you've used a database driver in a dynamically-typed
mood you'll recognize both: you addressed columns by **position** (`row[0]`, `row[1]` — good luck when
someone reorders the `RETURN`), and a value that didn't match your expectation tended to surface as a
panic somewhere deep in iteration. Both of those are the kind of bug that passes code review and then
pages you at 2am.

## The new shape

Two changes, and they compose:

1. **Result sets are header-aware.** A row knows its column names, so you read by alias —
   `row.try_get("year")` — not by a magic index.
2. **Iteration is fallible.** The result stream yields `FalkorResult<Row>`. A row that fails to parse
   is a real `Err` you can `?`, not a landmine.

Here's the real `examples/rows.rs`:

{{ code_file(file="examples/rows.rs") }}

## Why `FalkorResult<Row>` is the important part

Look at the iterator item type. It's not `Row`, it's `FalkorResult<Row>`. That one decision ripples
outward in the nicest way:

- **`?` works mid-iteration.** `let row = row?;` and the unhappy path is handled the same way you
  handle every other error in Rust. No special "did this row secretly fail?" dance.
- **`collect` short-circuits.** Collecting into a `Result<Vec<_>, _>` stops at the first bad row and
  hands you the error. One malformed value doesn't get silently dropped or turned into a default.
- **Typed access is explicit.** `try_get::<i64>("year")` says exactly what you expect, and converts
  in one step. If the column is actually a string, you get an `Err` describing the mismatch — at the
  call site, where you can do something about it.

And when you *don't* want to convert, `get_at` borrows the raw `FalkorValue` without cloning, so the
escape hatch is there when you need to inspect the wire value directly. Ergonomics shouldn't cost you
control.

## The "by name" thing is not a small thing

I want to dwell on reading columns by alias for a second, because it's the feature that quietly
prevents the most bugs. Positional access couples your Rust code to the *order* of your `RETURN`
clause. The day someone adds a column, or reorders two for readability, every positional index
downstream is now silently pointing at the wrong data — and nothing fails, it just returns
nonsense. Reading `row.try_get("year")` is immune to that. The query can grow and shuffle; your code
keeps asking for `year` and keeps getting `year`.

There's also a tidy `into_map()` when you genuinely want the whole row as a `column → value` map —
handy for logging or for shoving a row into something generic.

This is about as far as the *untyped* row API goes. The natural next question is "can I skip the
per-column `try_get` and just deserialize a row into my own struct?" Yes — that's the `serde` story
in [Part 6](@/posts/06-serde-and-temporal.md). But first, the feature I had the most fun building:
making the whole thing **async and streaming**.
