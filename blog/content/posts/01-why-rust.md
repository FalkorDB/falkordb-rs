+++
title = "Why I keep betting on Rust for a graph database client"
date = 2026-06-02
description = "Kicking off a dev log about the FalkorDB Rust client — the vision, the Rust bet, and a promise: every snippet on this blog compiles in CI."
[taxonomies]
tags = ["rust", "vision", "falkordb"]
[extra]
part = "Part 1 of 8"
author = "Barak"
+++

Welcome to the dev log for the **[FalkorDB](https://www.falkordb.com/) Rust client**. Over the
next few posts I'm going to walk through a month of changes that took this crate from "it works"
to "I'd actually hand this to a stranger." Type-safe parameters, header-aware rows, async streaming,
an embedded server you can `cargo run`, temporal values, retries, metrics, replica routing — the
whole tour.

But first, the obvious question: why pour this much effort into a *client library*, in *Rust*, for a
graph database? Let me make the case.

## What FalkorDB is trying to be

FalkorDB is a fast, low-latency graph database. Under the hood it represents the graph as sparse
matrices and answers queries with linear algebra (GraphBLAS), which is a genuinely different bet
from the pointer-chasing most graph databases do. The result is a database that stays quick when
your graph gets big and your queries get hairy — exactly the workload that knowledge graphs and
GenAI retrieval throw at you.

A database is only as good as the distance between *your code* and *its speed*. You can have the
fastest engine on earth, and if the client makes you hand-concatenate query strings and pattern-match
untyped blobs, people will write slow, buggy code on top of it. The client **is** the product, as
far as most developers ever experience it. That's the part I work on, and that's why I care about it
this much.

## Why Rust

I want a client that's **fast**, **correct**, and **honest about failure** — and Rust lets me have
all three without picking two.

- **Correctness you can feel at compile time.** Rust's type system is the best code reviewer I've
  ever had. A whole category of "oops" — SQL/Cypher injection, mismatched result columns, forgetting
  that the network can fail — can be turned from runtime surprises into compile errors. Most of this
  series is really just me handing more of my mistakes to the compiler.
- **Performance without a runtime tax.** Zero-cost abstractions mean the ergonomic API and the fast
  API are the same API. A typed row doesn't cost you a heap allocation it didn't need.
- **One library, two worlds.** The same crate ships a **blocking** client and a **`tokio` async**
  client with the same ergonomics. (More on how, and what it costs, in [Part 4](@/posts/04-async-streaming-and-multiplexing.md).)
- **It embeds anywhere.** Rust drops into CLIs, servers, and other languages' runtimes without
  dragging a VM along. We even ship an *embedded FalkorDB server* — see
  [Part 5](@/posts/05-embedded-server.md).

Here's the whole thing in miniature — connect, query, iterate. This is the real
`examples/basic_usage.rs` from the repo:

{{ code_file(file="examples/basic_usage.rs") }}

Notice what you *don't* see: no string-escaping, no `unwrap()` confetti, no "parse this `Vec<Vec<u8>>`
yourself." Reading a result is just iterating typed rows. We'll earn each of those niceties in later
posts.

## The promise of this blog: the code can't lie

I have a personal grudge against tech blogs whose code doesn't compile. You copy a snippet from a
post, paste it in, and discover the API moved on two releases ago and nobody told the prose.

So I built this blog to make that impossible. **Every Rust sample you'll see is a real file in the
repository** — an `examples/*.rs` program or a `benches/*.rs` harness — that CI compiles on every
push. At build time the site copies those exact files in and renders them. There is no hand-typed
code on this blog. If a snippet is here, it built.

{{ mermaid(path="proof-pipeline.mmd", caption="The snippet you read is the file CI compiled.") }}

The same goes for numbers. When I show you a benchmark, it's a real `criterion` harness, and the
figures are a dated sample run you can reproduce with one command. No vibes-based performance claims.

It's a small obsession, but it's the one that makes the rest of this series trustworthy. Next time,
the first place I taught the compiler to catch my mistakes: **query parameters**.

> This is a developer dev log; opinions are mine and occasionally wrong in interesting ways. Corrections
> and heckling welcome on [GitHub](https://github.com/FalkorDB/falkordb-rs).
