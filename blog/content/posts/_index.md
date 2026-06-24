+++
title = "Posts"
sort_by = "date"
template = "section.html"
page_template = "page.html"
+++

The full series. Start at the bottom if you like a story with a beginning.

<h2 id="colophon">Colophon: how the code on this blog can't lie</h2>

Tech blogs rot. You copy a snippet from a two-year-old post, paste it in, and it doesn't compile —
the API moved on and the prose didn't. I refuse to do that to you.

So every Rust sample here is a **real file in the [repository](https://github.com/FalkorDB/falkordb-rs)** —
an `examples/*.rs` program or a `benches/*.rs` harness — that CI compiles on every push. At build
time the site copies those exact files in and renders them. There's no hand-typed code on this blog;
if a snippet is here, it built. Benchmarks are real `criterion` harnesses too; the numbers are a
dated sample run you can reproduce with one `just` command.
