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
   where it helps) **+ a `CHANGELOG.md` entry**. On every change, **check and align all
   documentation** with it — see "Keep documentation in sync" below.
3. **Validate locally via `just`** — all relevant gates green (`just done` / `just verify`,
   plus `just coverage`, `just spellcheck`, `just proptest`, `just integration` as applicable).
4. Open a PR on a `feat:` / `fix:` / `ci:` / `docs:` branch.
5. **Resolve every AI review thread** (Copilot **and** CodeRabbit) — reply *and* mark resolved —
   before merge. Copilot auto-reviews on push here.
6. **Never merge to `main` yourself — wait for explicit human approval.** Do **not** run any
   `gh pr merge …` variant (e.g. `gh pr merge --admin --squash`) to self-merge, even when every
   check is green and all AI threads are resolved. Open the PR, get it green, and **stop** until the
   maintainer reviews and approves the merge. After a human merges, `release-plz` handles the release.

## Keep documentation in sync

On **every** change, check and align **all** documentation so it never drifts from the code —
treat "the docs match the code" as part of the definition of done, not a follow-up:

- **`README.md`** and **`docs/*.md`** — update any affected prose, code snippets, feature lists,
  and migration notes.
- **`llms.txt`** (the AI-readable API surface) — regenerate with **`just llms`** whenever the
  public API changes, and commit the result. The narrative lives in `docs/llms.template.md`; the
  `## Public API` block is auto-generated from `src/lib.rs`. CI enforces this with
  **`just check-llms`**, a drift gate that fails if `llms.txt` is stale.
- **doc-comments, doctests and `examples/`** — keep them accurate and compiling.

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
  same wordlist and fails it at PR time. Titles become the squashed-merge commit subject (git
  history) and their Conventional-Commit prefix drives the release, so keep them clean at the
  source. **`CHANGELOG.md` entries are hand-written and spellchecked too** — an unknown word there
  fails the `spellcheck` gate, so backtick code/type names or add them to the wordlist.
- When you add or rename a **public type / term**, add its name to **`.github/wordlist.txt`**.
- In Markdown/docs, **backtick** code and type names (`` `ConnectionDown` ``) — backticked spans
  are ignored by the spellchecker.

## CHANGELOG & releases

- Keep a `## [Unreleased]` section and **hand-write every entry there yourself**, under the right
  `### Added` / `### Changed` / `### Fixed` / `### Other` heading (Keep a Changelog). On release,
  `release-plz` only stamps the version header below `## [Unreleased]` — it does **not** generate
  its own bullets from commits (see the `[changelog]` `body` in `release-plz.toml`). This is why a
  single hand-written `### Added` is the only source of truth; there is no second auto-generated one.
- **Every entry must carry its PR link** — `… ([#123](https://github.com/FalkorDB/falkordb-rs/pull/123))` —
  because `release-plz` no longer appends it for you. Add it once the PR number is known.
- **Never put literal Markdown-header markup inside a bullet** — a `## [` or `###` in the *text* of an
  entry. `release-plz`'s changelog parser matches those anywhere (not just at line start) and treats
  them as a version/section boundary, so it stamps the new release header in the wrong place and
  leaves the release section empty. Write "the Added section", not a backticked `###`-prefixed name.
- **Don't hardcode the next version** — let `release-plz` compute it from the commits.
- Use **Conventional Commits** (`feat`, `fix`, `ci`, `docs`, `perf`, `chore`, …) — they drive the
  version bump. Mark breaking changes with `feat!` / a `[**breaking**]` note.
- **Only `feat:` / `fix:` commits cut a release** (`release_commits` in `release-plz.toml`);
  `ci` / `docs` / `chore` / `refactor` changes ride along with the next feature/fix release instead
  of cutting an empty one. To force a release without a `feat`/`fix`, push to a `release-plz-…` branch.
- Include a `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>` trailer on
  agent-authored commits.

## Code style

Keep code **tidy, simple, and efficient**. Comment only what genuinely needs clarification —
not the obvious. Match the surrounding style; prefer the smallest change that fully solves the
problem.
