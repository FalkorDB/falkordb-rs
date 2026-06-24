+++
title = "Production-grade: retries, tracing spans, metrics, and error hints"
date = 2026-06-21
description = "Opt-in retries that never duplicate a write, OpenTelemetry-aligned tracing with a privacy-safe query fingerprint, bounded metrics, and errors that tell you how to fix them."
[taxonomies]
tags = ["rust", "observability", "resilience"]
[extra]
part = "Part 7 of 8"
author = "Barak"
+++

A client library spends most of its life in production, where the network is hostile and the only
thing you have at 3am is whatever the library decided to tell you. This batch of work was all about
that life: surviving transient failures, explaining what's happening, and — when something does break —
pointing at the fix.

## Retries that can't duplicate a write

Retries are a loaded gun. Re-issue a read and worst case you did some redundant work; re-issue a write
and you might have just charged a customer twice. So the retry policy here has one inviolable rule:
**writes are never retried.**

{{ mermaid(path="retry.mmd", caption="Read-only + transient → retried with backoff. A write fails fast.") }}

It's **off by default** — without a policy, every operation runs exactly once, same as before. When
you do opt in, only read-only / idempotent operations that fail with a *transient connection* error
get retried, with bounded exponential backoff. Here's the real `examples/retry.rs`:

{{ code_file(file="examples/retry.rs") }}

The classification is by the **API you call**, which is what makes it safe: `query()` is always
treated as a write and never retried; `ro_query()` is eligible. There's no heuristic sniffing your
Cypher to guess intent — you told the client what this is by which method you called, and it believes
you. Enabling a policy can therefore never turn one write into two. That property mattered more to me
than any amount of cleverness.

## Spans that tell you something, without leaking everything

With the `tracing` feature, every query- and procedure-execution span carries structured,
low-cardinality fields you can actually slice on — the graph (`db.namespace`), the command
(`db.operation.name`), whether it was read-only, the connection strategy, and the server-reported
execution time and returned row count. The naming is OpenTelemetry-aligned on purpose, so this drops
into your existing dashboards instead of inventing private conventions.

{{ code_file(file="examples/observability.rs") }}

Here's the design decision I'll defend hardest: **the raw query text and parameter values are not
recorded by default.** Instead the span carries a `db.query.fingerprint` — a redacted identity for the
query *shape* that's independent of the inlined literals. So `WHERE m.title = 'The Matrix'` and
`WHERE m.title = 'Heat'` share a fingerprint, and you can aggregate "this query shape is slow" without
spraying user data — or secrets — across your tracing backend. You can opt into full query logging with
`with_query_logging(true)` when you're debugging locally, but you have to *ask*. Observability should
never be the reason you failed an audit.

## Metrics and actionable errors

Two more pieces round it out:

- **A `metrics` feature** emits **bounded** counters and histograms — executions, retries, in-flight
  requests, pool-wait time. "Bounded" is the key word: the cardinality is fixed, so your metrics bill
  doesn't explode because someone parameterized a label with a user ID. Plays directly into the
  `metrics` ecosystem.
- **`FalkorDBError::mitigation_hint()`** — because an error that only tells you *what* went wrong is
  half an error. The hint tells you what to *do* about it. It's the difference between `ConnectionDown`
  and "`ConnectionDown` — the server may be restarting or unreachable; check connectivity and consider a
  retry policy." Future-me, staring at a log line, is the customer for that sentence.

None of this is glamorous. All of it is the difference between a library you *use* and a library you
*trust*. The finale, [Part 8](@/posts/08-replica-routing.md), is about trust of a subtler kind:
reading from replicas without accidentally reading the past.
