#!/usr/bin/env bash
# post-checks.sh — the gate that must be green before a task is declared done.
#
# Runs every no-server validation step, including the *strict* clippy pass over
# all targets and dev features (`just clippy-all`) which the lightweight
# `check-clippy` CI gate does not cover. Run it from the repo root:
#
#     ./scripts/post-checks.sh      # or: just done
#
# Each step is run in order; the script aborts on the first failure so the
# offending command is the last thing printed.

set -euo pipefail

run() {
    echo "==> $*"
    "$@"
}

run just fmt-check
run just clippy
run just clippy-all
run just build
run just doc
run just deny

echo
echo "All post-checks passed. Safe to declare the task done."
