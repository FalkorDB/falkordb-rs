+++
title = "Batteries included: an embedded FalkorDB you can cargo run"
date = 2026-06-15
description = "Spin up a real FalkorDB from your Rust process — auto-provisioned module, cached or downloaded, or embedded at build time for fully offline runtime."
[taxonomies]
tags = ["rust", "embedded", "testing"]
[extra]
part = "Part 5 of 8"
author = "Barak"
+++

Every "getting started" guide has the same speed bump on line one: *first, run a server*. It's a
small ask, but it's friction at the exact moment someone is deciding whether your library is worth
their afternoon. So we removed it. With the `embedded` feature, the client can start a real FalkorDB
for you, in-process, and shut it down when you're done.

## What "embedded" actually does

The embedded server spins up `redis-server` as a child process with the FalkorDB module loaded, waits
until it answers `PING`, hands you a connected client, and tears the whole thing down on drop. The
interesting part is where the module comes from.

{{ mermaid(path="embedded-startup.mmd", caption="Resolve the module, spawn the server, wait for ready, hand back a client.") }}

There are three ways the `falkordb.so` module gets resolved, in increasing order of "I have no network
and I like it that way":

1. **Auto-download (default).** If the module isn't already present, it's fetched from the official
   releases, **verified against a pinned SHA-256 checksum**, and cached locally. First run pays the
   download; every run after is instant. The `redis-server` binary itself is *not* downloaded — it's
   found on your `PATH` — so this leans on a tool you already have rather than smuggling in a binary.
2. **Already-installed / offline.** Point at an existing module, or disable downloading and let it
   search common system locations. No network at all.
3. **Build-time bundle.** Enable the `embedded-bundle` feature instead, and a `build.rs` fetches the
   module *for your build target at compile time* and bakes it into your binary with `include_bytes!`.
   The running process then needs **zero network** — it extracts the embedded bytes and goes. This is
   the one for air-gapped and network-isolated deployments, and it's why the feature exists.

Here's the real `examples/embedded_usage.rs` — note that the *usage* is identical regardless of how
the module is sourced; only the `EmbeddedConfig` differs:

{{ code_file(file="examples/embedded_usage.rs") }}

## The detail I'm quietly proud of

Two, actually.

First, the embedded server now **pre-flights `redis-server --version`** and fails early, with a clear
message, if it's older than the 8.0 the module requires. The failure mode it replaces — a server that
starts and then mysteriously can't load the module — is exactly the kind of thing that eats an hour of
someone's day. Fail fast, fail legible.

Second, the build-time bundle is genuinely hermetic. The checksum verification on the download path
isn't decoration: a graph module is native code you're loading into your process, and "download some
bytes and run them" without verifying them is how you end up in an incident review. Pinned version,
pinned checksum, or embedded-at-build-time bytes. Pick your paranoia level.

## Why this matters more for a graph database

Graph code is fiddly to test. You want a real engine — Cypher semantics, real planning, real
results — not a mock that lies to you. Embedded mode means your integration tests boot an actual
FalkorDB per run with no external setup, and your CI doesn't need a service container just to check
that a `MATCH` returns what you think. It collapses the distance between "I have an idea" and "I ran
it against the real thing" to a single `cargo run`. That distance is where adoption lives or dies.

Next: the values *inside* those nodes. [Part 6](@/posts/06-serde-and-temporal.md) is about mapping
rows straight into your structs with `serde`, and decoding temporal values into types with actual
arithmetic.
