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

# The 4.x listing comes from Docker Hub in pages. A fake `curl` serves page 1 (v4.22.3) and either
# page 2 (v4.30.0) or a failure for it: a partial listing must fall back to the pinned tag, not
# pick from the pages that arrived.
fake=$(mktemp -d)
trap 'rm -rf "$fake"' EXIT
cat >"$fake/curl" <<'FAKE'
#!/usr/bin/env bash
url="${*: -1}"
case "$url" in
    *page=2*)
        [ "${FAIL_PAGE_2:-}" = 1 ] && exit 22
        echo '{"next": null, "results": [{"name": "v4.30.0"}]}' ;;
    *)
        echo '{"next": "https://hub.docker.com/next?page=2", "results": [{"name": "v4.22.3"}]}' ;;
esac
FAKE
chmod +x "$fake/curl"
# `fail_page_2` goes into the environment of the resolver itself, which runs the fake curl.
expect_line_4() {
    local fail_page_2=$1 expected=$2 actual
    actual=$(FAIL_PAGE_2="$fail_page_2" CURL="$fake/curl" ./falkordb-image.sh 4 2>/dev/null)
    if [ "$actual" != "$expected" ]; then
        echo "FAIL: line 4 with page 2 failing=$fail_page_2: expected '$expected', got '$actual'" >&2
        fail=1
    fi
}
expect_line_4 0 falkordb/falkordb:v4.30.0
expect_line_4 1 falkordb/falkordb:v4.22.4

[ "$fail" -eq 0 ] && echo "falkordb-image: all cases passed"
exit "$fail"
