+++
title = "Stop hand-quoting Cypher: type-safe, injection-proof parameters"
date = 2026-06-06
description = "Bound query parameters that take real Rust values, encode them correctly, and make Cypher injection a compile-time non-event."
[taxonomies]
tags = ["rust", "api-design", "security"]
[extra]
part = "Part 2 of 8"
author = "Barak"
+++

Every database client eventually has the same coming-of-age moment: the day it stops letting you
build queries with string concatenation. Today's the day for this one.

## The bad old way

Here's the shape of code I never want to write again — and rather than hand-type it (this blog has a
rule: no uncompiled Rust), I'll point you at the **commented-out "before" block right inside the real
example below**. It builds a query with `format!`, quoting `'The Matrix'` by hand and pasting `year`
straight into the string.

Two problems, one fatal. The annoying one: I'm responsible for quoting and escaping every value, and
I *will* get it wrong the day a title contains an apostrophe. The fatal one: if that title ever comes
from a user, they can close my string and append their own Cypher. `'; MATCH (n) DETACH DELETE n //`
and the graph is gone. This is injection, and "remember to escape everything, forever" is not a
security strategy.

## The way that ships now

Parameters are **bound**, not interpolated. You hand the client real Rust values and it encodes them
into proper Cypher literals for you. The example below shows both the commented-out "before" and the
real "after":

{{ code_file(file="examples/typed_params.rs") }}

A few things I want to point at, because they're the whole point:

- **`with_param` takes typed Rust values.** `"The Matrix"` (a `&str`), `1999` (an `i64`), `8.7` (an
  `f64`) — each is encoded as the correct Cypher literal. No quotes in your format string, because
  there is no format string.
- **Injection becomes a non-event.** The line that binds `"'; MATCH (n) DETACH DELETE n //"` as a
  title stores that whole thing as an inert string property. There is no parser context in which it
  can "break out," because it was never concatenated into the query text in the first place.
- **Collections just work.** `WHERE m.year IN $years` with `[1999, 2003]` does what you'd hope — the
  array is bound as a Cypher list. Great for the `IN`-list query you write fifteen times a week.
- **The type system catches the edges.** Some values (a spatial `point`) can't be bound as a raw
  literal, so the API makes you pass the components as a map and wrap them with `point()`. That's the
  compiler steering you toward the one correct encoding instead of letting you guess.

The ergonomic version and the safe version are the same version. That's the bit I'm proud of: you
don't *choose* safety here, you just write the obvious code and safety falls out.

## Why a graph client especially needs this

Cypher is expressive, which is a polite way of saying there are a *lot* of places to inject. Node
labels, relationship types, property maps, list predicates, path patterns. Bound parameters keep your
**data** out of the **query structure**, permanently. Your query text becomes a fixed template the
server can even cache and re-plan efficiently, and your values ride alongside it where they can't do
any harm.

It also makes the *next* feature possible. Once values are typed on the way in, it's a short hop to
making results typed on the way out — which is exactly where
[Part 3](@/posts/03-header-aware-rows.md) goes: rows that don't lie about what they contain.
