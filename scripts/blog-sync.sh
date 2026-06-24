#!/usr/bin/env bash
# blog-sync.sh — copy the canonical, CI-compiled example & benchmark sources into the
# Zola site tree so blog posts can embed them verbatim.
#
# WHY: Zola's load_data() refuses to read files outside the site directory (blog/), and the
# blog's "every snippet you see compiles in CI" guarantee comes from these snippets being the
# SAME files that `just build-examples` (and the benches build) compile. This script copies
# them byte-for-byte into blog/snippets/ (which is .gitignored and regenerated on every build),
# so what a reader sees is exactly what CI built — no drift is possible. It also extracts
# `// ANCHOR: name` ... `// ANCHOR_END: name` regions into blog/snippets/anchors/ for focused
# excerpts (the full file still compiles; the markers are ordinary comments).
#
# Run via `just blog-sync` (called by `just blog-build` / `blog-serve` / `blog-check`).

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

dest="blog/snippets"
rm -rf "$dest"
mkdir -p "$dest/examples" "$dest/benches" "$dest/anchors"

shopt -s nullglob

copy_and_anchor() {
    local src="$1" rel="$2"
    cp "$src" "$dest/$rel"
    # Emit one file per `// ANCHOR: name` region (markers stripped). Portable POSIX awk only
    # (no gawk 3-arg match): ubuntu's default awk is mawk. The whole source file is still what
    # CI compiles; the markers are ordinary comments.
    awk -v dest="$dest" -v rel="$rel" '
        /\/\/[ \t]*ANCHOR_END:/ {
            n = $0; sub(/.*ANCHOR_END:[ \t]*/, "", n); sub(/[ \t\r]+$/, "", n)
            active[n] = 0; next
        }
        /\/\/[ \t]*ANCHOR:/ {
            n = $0; sub(/.*ANCHOR:[ \t]*/, "", n); sub(/[ \t\r]+$/, "", n)
            active[n] = 1; seen[n] = 1; next
        }
        {
            for (k in active) if (active[k] == 1) buf[k] = buf[k] $0 "\n"
        }
        END {
            slug = rel; gsub(/[\/.]/, "_", slug)
            for (k in seen) {
                out = dest "/anchors/" slug "__" k ".rs"
                printf "%s", buf[k] > out
            }
        }
    ' "$src"
}

for f in examples/*.rs; do
    copy_and_anchor "$f" "examples/$(basename "$f")"
done
for f in benches/*.rs; do
    copy_and_anchor "$f" "benches/$(basename "$f")"
done

count=$(find "$dest" -name '*.rs' | wc -l | tr -d ' ')
echo "blog-sync: synced $count snippet file(s) into $dest"
