+++
title = "Async all the way down: streaming results and connection multiplexing"
date = 2026-06-12
description = "Result sets that are Streams you compose with the futures toolbox, and a multiplexed connection strategy — with a real pooled-vs-multiplexed benchmark."
[taxonomies]
tags = ["rust", "async", "performance"]
[extra]
part = "Part 4 of 8"
author = "Barak"
+++

This is the post I'd been looking forward to writing, because it's where the Rust bet from
[Part 1](@/posts/01-why-rust.md) really pays rent. Two things landed: result sets became **async
streams**, and the async client learned to **multiplex** many concurrent queries over a few shared
sockets. Then I benchmarked it, because claims without numbers are just vibes.

## Results are streams

On the async client, a result set's `data` is a `Stream<Item = FalkorResult<Row>>`. Same fallible,
header-aware row from [Part 3](@/posts/03-header-aware-rows.md) — now it arrives lazily and composes
with the entire `futures` toolbox.

{{ mermaid(path="async-stream.mmd", caption="Rows arrive lazily; errors stay first-class.") }}

Here's the real `examples/async_stream.rs`:

{{ code_file(file="examples/async_stream.rs") }}

The bit that makes me happy is the second half: a stream of rows, each fanning out into a follow-up
query, with `buffer_unordered(8)` keeping at most eight in flight. That's backpressure-aware
concurrency in about five lines, and it falls out of treating results as a `Stream` instead of
inventing a bespoke cursor API. Reuse beats reinvention.

## Many queries, few sockets: multiplexing

The other half is *how* those concurrent queries hit the wire. The async client supports two
connection strategies:

- **Pooled** — a fixed set of connections; each query borrows one exclusively for its duration.
- **Multiplexed** — a few auto-reconnecting sockets, each carrying *several in-flight commands at
  once*. Queries are pipelined and their replies correlated back to the right caller.

{{ mermaid(path="multiplexing.mmd", caption="Multiplexing: 64 tasks, 4 sockets, replies correlated back.") }}

`examples/multiplexed_async.rs` fires 64 independent queries across 4 shared sockets:

{{ code_file(file="examples/multiplexed_async.rs") }}

## Now, the numbers

Here's where I have to be honest instead of triumphant. Multiplexing is not free, and it is not always
faster — it's faster *under contention*. The benchmark is the real `benches/async_strategies.rs`
harness; it runs the same batch of concurrent `RETURN 1` reads against both strategies at a fixed
8 connections, across rising concurrency. (`RETURN 1` is deliberate: it isolates the client's
connection behavior from query-planning cost.)

{{ bench_table(name="async_strategies") }}

Read that table honestly:

- **At concurrency 1, pooling wins.** One query at a time has nothing to multiplex, and the
  multiplexer's bookkeeping is pure overhead. The speedup is *below* 1.0 — multiplexing is slightly
  slower here, and pretending otherwise would be lying to you.
- **As concurrency climbs, multiplexing pulls ahead.** By 64 concurrent reads it's ~1.5× faster, and
  by 256 it's ~1.7×, because the pooled strategy is serializing waves of queries through 8 exclusive
  connections while the multiplexer keeps many commands in flight per socket.

So the guidance writes itself: a request/response app doing one query at a time? Pooled is simple and
great. A server fanning out hundreds of concurrent graph reads? Multiplex. The default for the async
client is multiplexed with 8 sockets, which is the right call for the server workloads people
actually point this at.

The harness, by the way, skips itself cleanly when no server is reachable, so it stays runnable in CI
without flaking — a small thing I care about a lot, and the subject of more than one bug fix.

You can reproduce the table with `just bench` against your own server; your numbers will differ, which
is exactly why they're labeled with the machine and date. Next up, the feature that means you don't
even need a server running to try any of this: the [embedded FalkorDB](@/posts/05-embedded-server.md).
