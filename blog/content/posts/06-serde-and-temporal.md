+++
title = "Typed mapping with serde, and temporal values that do real math"
date = 2026-06-18
description = "Deserialize rows straight into your own structs with serde, and decode FalkorDB date/time/duration values into types with a checked, type-safe algebra."
[taxonomies]
tags = ["rust", "serde", "api-design"]
[extra]
part = "Part 6 of 8"
author = "Barak"
+++

[Part 3](@/posts/03-header-aware-rows.md) gave us honest rows you read column-by-column. This post is
about two ways to stop reading column-by-column: map a whole row into your own type with `serde`, and
get temporal values back as real types instead of opaque scalars.

## Rows into structs, with `serde`

Per-column `try_get` is great until you're pulling ten fields off a node, at which point it's ten
lines of boilerplate that all say the same thing. With the `serde` feature you derive `Deserialize`
on your struct and let the row map itself:

{{ code_file(file="examples/typed_mapping.rs") }}

What I like here is that it works at three levels without three different APIs:

- **Value-level:** `node.deserialize_into::<Movie>()` turns a single returned node into a struct.
- **Row-level with `query_as`:** `RETURN m` maps the node's properties onto your fields; a
  multi-column `RETURN m.title AS title, m.year AS year` maps the **aliases** onto the same fields.
  Your `RETURN` and your struct meet in the middle, by name.
- **Scalars:** `RETURN count(m)` deserializes straight into `i64`. Even the degenerate case is
  consistent.

And because it's `serde`, the usual tools just work — `#[serde(default)]` on `rating: Option<f64>`
means a missing property is `None` instead of an error. You already know this vocabulary; the client
just speaks it.

## Temporal values that aren't lying integers

Now the one that's secretly hard. FalkorDB has real temporal types — `date`, `time`/`localtime`,
`duration`, `datetime`. For a while the client surfaced some of these as `Unparseable`, which is a
polite way of saying "here's a blob, you figure it out." That's a trap: the moment you turn a date
into a bare `i64` to do arithmetic, you've thrown away the type system's ability to stop you from
adding a *duration* to a *count of bananas*.

So we decode them into proper `Date` / `Time` / `Duration` / `DateTime` value types, each exposing its
scalar as a typed `Seconds` — not a raw integer — and `DateTime`/`Duration` get a small, **type-safe
algebra**:

{{ code_file(file="examples/temporal.rs") }}

The algebra is the point, and it's deliberately restrictive:

- **`DateTime − DateTime → Duration`.** The difference between two instants is a span. ✓
- **`DateTime ± Duration → DateTime`.** Shifting an instant by a span gives another instant. ✓
- **`DateTime + DateTime`?** That has no meaning, so it has *no impl* and won't compile. You can't
  add two Tuesdays together, and now the compiler agrees with you.

There are `checked_*` variants that return `None` instead of panicking on overflow, because temporal
arithmetic at the extremes is exactly where the silent wraparound bugs hide. `DateTime::new(i64::MAX)
.checked_add(dur)` is `None`, not a wrapped-around timestamp from the Bronze Age.

This is a tiny domain, but it's a perfect showcase for the whole thesis of this series: encode the
rules of your domain into types, and a category of bugs simply stops being expressible. Next, we take
that same "make failure honest" energy to the network itself —
[retries, tracing, and metrics](@/posts/07-resilience-and-observability.md).
