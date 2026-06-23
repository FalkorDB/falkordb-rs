# Migrating to 0.10

Version 0.10 makes **replica routing opt-in**. Read-only queries (`ro_query` /
`call_procedure_ro`) and all-read batches now run on the **primary by default** instead of being
sent to a replica automatically. Nothing else changes — the query methods, results and the
synchronous/async split are untouched.

> **Read this if you connect to a Redis Sentinel deployment with readable replicas.** On a single
> node (or any deployment without readable replicas) behavior is unchanged: reads already used the
> primary, and they still do.

## Why it changed

A FalkorDB replica applies writes only **after** the primary has committed them, so a read served
from a replica can return slightly **stale** data. Previously `ro_query` / `call_procedure_ro` were
routed to a replica *automatically* whenever one was available, with no way to opt out per query and
no warning about staleness. That conflated two separate ideas:

- the **read-only command** (`GRAPH.RO_QUERY`) — whether the server allows the query to write, and
- **replica routing** — which node answers, which is a consistency/accuracy choice.

0.10 separates them. The command stays the same; *where* a read runs is now an explicit
[`ReadPreference`], defaulting to the primary so you never observe replication lag unless you ask
for it.

## At a glance

| Before (≤ 0.9) | After (0.10) |
| --- | --- |
| `ro_query` auto-used a replica when one existed | `ro_query` uses the **primary** unless you opt in |
| no way to keep a specific read on the primary | `ro_query(..).primary_only()` |
| no per-query replica opt-in | `ro_query(..).prefer_replica()` |
| `client.reads_from_replicas()` | `client.replica_reads_available()` + `client.read_preference()` |

## 1. Restore the old replica offload (one line)

If you relied on read-only queries being served from replicas, set the client default to
`PreferReplica`:

```rust
use falkordb::{FalkorClientBuilder, ReadPreference};

let client = FalkorClientBuilder::new()
    .with_connection_info("falkor://127.0.0.1:26379".try_into()?)
    .with_read_preference(ReadPreference::PreferReplica) // restore pre-0.10 behavior
    .build()?;
```

Every `ro_query` / `call_procedure_ro` from this client now prefers a replica again, transparently
falling back to the primary when none is available.

## 2. Or opt in per query

Leave the client on the default (`Primary`) and opt individual reads into replicas:

```rust
// Offload this analytics read to a replica (stale data is acceptable here).
let mut rows = graph
    .ro_query("MATCH (a:Actor) RETURN count(a)")
    .prefer_replica()
    .execute()?;
```

With a `PreferReplica` client default, do the opposite for read-your-writes paths that must see the
freshest data:

```rust
// Force the primary for this read, overriding the client default.
let mut rows = graph
    .ro_query("MATCH (a:Actor {name: $name}) RETURN a")
    .primary_only()
    .execute()?;
```

Batches take the same methods (`graph.batch().prefer_replica()`); routing applies to the whole
pipeline, which must be all-read.

## 3. Replica preference on a write is now an error

A write can never run on a replica, so requesting one on a writable `query`, `call_procedure`, or a
batch that contains a write fails fast with `FalkorDBError::ReadPreferenceNotReadOnly` (before any
round-trip) instead of being silently ignored:

```rust
let err = graph
    .query("CREATE (:Actor {name: 'Tom Hanks'})")
    .prefer_replica()
    .execute()
    .unwrap_err(); // FalkorDBError::ReadPreferenceNotReadOnly
```

## 4. `reads_from_replicas()` is deprecated

It conflated capability with policy. Replace it with whichever you actually need:

| Before | After |
| --- | --- |
| `client.reads_from_replicas()` (a replica pool exists) | `client.replica_reads_available()` |
| (no equivalent) — the active routing policy | `client.read_preference()` |

`reads_from_replicas()` still works for now but emits a deprecation warning; it will be removed in a
future release.
