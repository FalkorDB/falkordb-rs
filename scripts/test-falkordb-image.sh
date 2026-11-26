#!/usr/bin/env bash
# Self-test for scripts/falkordb-image.sh: the newest tag of a line is picked by version, not by
# string, and suffixed variants and other lines are ignored. Needs no network.
set -euo pipefail
cd "$(dirname "$0")"

fail=0
check() {
    local expected=$1 major=$2 tags=$3 actual
    actual=$(printf '%s\n' $tags | ./falkordb-image.sh --pick "$major" || true)
    if [ "$actual" != "$expected" ]; then
        echo "FAIL: --pick $major on [$tags]: expected '$expected', got '$actual'" >&2
        fail=1
    fi
}

check v4.10.0 4 "v4.9.0 v4.10.0 v4.2.11"
check v4.22.4 4 "v4.22.4 v4.22.3 v4.9.12 v4.22.10-alpine v4.22.5-x64 v4.22.4-rhel9-x64"
check v4.22.10 4 "v4.22.9 v4.22.10"
check v4.22.4 4 "6.2.1 latest edge v6.0.4 v4.22.4 v40.0.0-x64"
check "" 4 "v4.22.4-alpine latest 6.2.1"
check v6.2.1 6 "v6.2.0 v6.2.1 v4.22.4"

for line in 6 edge; do
    case "$(./falkordb-image.sh "$line")" in
        falkordb/falkordb:latest | falkordb/falkordb:edge) ;;
        *) echo "FAIL: line $line" >&2; fail=1 ;;
    esac
done
if ./falkordb-image.sh 5 >/dev/null 2>&1; then
    echo "FAIL: an unknown line must be an error" >&2
    fail=1
fi

[ "$fail" -eq 0 ] && echo "falkordb-image: all cases passed"
exit "$fail"
