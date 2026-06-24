+++
title = "Read your writes: opt-in replica routing via Sentinel"
date = 2026-06-24
description = "Routing read-only queries to replicas is a correctness decision, not just a throughput knob. Here's why we made it opt-in, per-client and per-query."
[taxonomies]
tags = ["rust", "distributed-systems", "api-design"]
[extra]
part = "Part 8 of 8"
author = "Barak"
+++

We made it to the finale. This one is short, but it's the post where a one-word default decision is
actually a *correctness* decision in disguise — and getting it wrong is the kind of bug that doesn't
show up until production, under load, intermittently. My favorite kind. (It is not my favorite kind.)

## The setup

Behind Redis Sentinel, a FalkorDB deployment has a primary and one or more replicas. Reads *can* be
served from replicas to spread load. The catch: a replica applies a write only *after* the primary has
committed it. So a read from a replica can be **slightly stale** — it might not yet reflect a write you
just made.

{{ mermaid(path="replica.mmd", caption="Writes to the primary; reads where you ask — replica or primary.") }}

## The decision: stale-by-accident is not allowed

Here's the part I want to defend, because it was a **breaking change** and I made people opt back in.

The client *used* to route read-only queries to replicas automatically when they were available. That's
great for throughput and quietly terrible for correctness: the classic failure is "create a thing, then
immediately read it back, and it's not there" — because the read went to a replica that hadn't caught
up. That's a heisenbug. It reproduces under load, on a good day, once.

So the default flipped: **reads go to the primary** unless you explicitly opt in. You don't get
replication lag unless you ask for it. Throughput is a knob you turn on purpose, with your eyes open —
not a default that silently trades correctness for a benchmark number. Routing is now controllable at
two levels:

- **Per client** — `with_read_preference(ReadPreference::PreferReplica)` makes every read prefer a
  replica.
- **Per query** — `ro_query(..).prefer_replica()` opts a single query in, and `.primary_only()` forces
  a single query back onto the primary. That last one is the read-your-writes escape hatch: even on a
  replica-preferring client, the one read that has to be fresh can demand the primary.

Here's the real `examples/readonly_replica.rs`:

{{ code_file(file="examples/readonly_replica.rs") }}

## The ergonomic details that make it safe to live with

- **It degrades gracefully.** `PreferReplica` transparently falls back to the primary when no replica
  exists — a single-node dev box, say — so the *same code* runs in every environment. You don't write
  one path for laptops and another for production.
- **Capability and policy are separate questions.** `replica_reads_available()` tells you whether a
  replica pool even exists; `read_preference()` tells you the client's default policy. Conflating
  "can I?" with "will I?" is how you get confusing behavior, so they're two honest accessors.
- **Asking for a replica on a write fails loudly.** Request a replica for a writable query and you get
  a real error, not a quietly-wrong route to a node that can't serve it.

## That's a wrap

Eight posts, one theme: **push correctness into the type system and the API defaults, so the easy path
is the safe path.** Typed parameters, honest rows, streaming async, an embedded server, temporal math,
retries that can't double-write, privacy-safe telemetry, and now reads that don't accidentally read the
past. That's a month.

Every snippet you read across this series is a real file the CI compiled, and the benchmark in
[Part 4](@/posts/04-async-streaming-and-multiplexing.md) is a real harness you can re-run. That wasn't
an accident — it's the same principle as the code: make the trustworthy thing the default thing.

Thanks for reading the dev log. The code's on [GitHub](https://github.com/FalkorDB/falkordb-rs);
come argue with me about any of it.
