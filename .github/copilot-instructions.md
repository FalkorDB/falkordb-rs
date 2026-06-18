# Copilot / AI agent instructions for `falkordb-rs`

Guidance for GitHub Copilot and other AI agents working in this repository. It encodes the
team's engineering conventions so changes land clean on the first try. Human contributors
should follow it too.

`falkordb-rs` is the Rust client for **FalkorDB** (a Redis-module graph database). It offers a
sync and an async (tokio) client, typed query parameters, header-aware fallible result rows,
async `Stream` results, and pipelined batches. Dev feature set: `tokio,embedded,serde`.

## Golden rule: drive everything through `just`

For **any** action that CI performs (format, lint, build, docs, deny, tests, coverage,
spellcheck, …), run the **exact same `just` recipe CI uses** — never a raw `cargo …` command.
If a check needs changing, update the `just` recipe **and** the CI workflow together so they
stay identical. Run `just --list` to see every recipe.

Key recipes:

| Recipe | Purpose |
| --- | --- |
| `just check` | Fast pre-commit loop: `fmt clippy build` (no server). |
| `just ci` | Required no-server gates: `fmt-check clippy build doc deny`. |
| `just done` | Definition-of-done static gates (`fmt-check`, `clippy`, `clippy-all`, `build`, `doc`, `deny`). Must be green before declaring a task done. |
| `just verify` | `ci` **plus** the server-backed test suite (`test-local`). |
| `just coverage` | Codecov JSON coverage (matches the `coverage` CI job). |
| `just test-local` | Spin up Docker FalkorDB + populate the IMDB fixture, run the full suite, tear down. |
| `just test-one <filter>` | Run a single test by name. |
| `just proptest` | `serde` property tests (no server). |
| `just spellcheck` | Spellcheck the Markdown docs. |
| `just spellcheck-pr-title` | Spellcheck a PR title (`PR_TITLE='…' just spellcheck-pr-title`). |

Tests need a running server; prefer the `*-local` wrappers, which manage Docker and the fixture.

## Definition of done for a change

1. **Design first** for non-trivial work, and **rubber-duck review** the design before coding.
2. **Implement** the change with: code **+ tests + docs** (doc-comments, a doctest or example
   where it helps) **+ a `CHANGELOG.md` entry**.
3. **Validate locally via `just`** — all relevant gates green (`just done` / `just verify`,
   plus `just coverage`, `just spellcheck`, `just proptest`, `just integration` as applicable).
4. Open a PR on a `feat:` / `fix:` / `ci:` / `docs:` branch.
5. **Resolve every AI review thread** (Copilot **and** CodeRabbit) — reply *and* mark resolved —
   before merge. Copilot auto-reviews on push here.
6. A human merges; `release-plz` handles the release.

## Flaky tests are a hard no

Fix a flaky test **immediately, as top priority**, regardless of the current task or whether the
flake is a pre-existing / non-regression issue. Flaky tests slow everyone down. Find the root
cause (e.g. retry only the genuinely transient error and fail fast on everything else) rather
than papering over it.

## Coverage

Patch coverage must be **≥ 95%**. Measure with the exact CI command (`just coverage` →
`codecov.json`), not an ad-hoc line count. Codecov counts a `?`-operator's **untaken error
branch as a partial** that lowers patch coverage; in tests, prefer combinator chains
(`.and_then`/`.map`) or a single shared helper over scattered `?`/`match` so those partials
don't pile up.

## Spellcheck, commit subjects & PR titles

- **PR titles must be spellcheck-clean** — the `PRTitle` CI task checks every PR title against the
  same wordlist and fails it at PR time. This matters because `release-plz` copies merged commit
  subjects (squashed PR titles) **verbatim** into `CHANGELOG.md` (itself spellchecked), so an
  unknown word would otherwise only surface — too late — on the release PR. Keep titles clean at
  the source.
- When you add or rename a **public type / term**, add its name to **`.github/wordlist.txt`**.
- In Markdown/docs, **backtick** code and type names (`` `ConnectionDown` ``) — backticked spans
  are ignored by the spellchecker.

## CHANGELOG & releases

- Keep a `## [Unreleased]` section; `release-plz` promotes it on release. **Don't hardcode the
  next version** — let `release-plz` compute it.
- Entry style (per the existing changelog): the `release-plz` short line + PR link, **followed by
  detailed manual entries** under the same `### Added` / `### Fixed` heading.
- Use **Conventional Commits** (`feat`, `fix`, `ci`, `docs`, `perf`, `chore`, …). Mark breaking
  changes with `feat!` / a `[**breaking**]` note.
- Include a `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>` trailer on
  agent-authored commits.

## Code style

Keep code **tidy, simple, and efficient**. Comment only what genuinely needs clarification —
not the obvious. Match the surrounding style; prefer the smallest change that fully solves the
problem.
