#!/usr/bin/env bash
# Print the Docker image of a FalkorDB release line, for the server-backed test suites.
#
#   scripts/falkordb-image.sh 4      newest released 4.x image (`falkordb/falkordb:v4.Y.Z`)
#   scripts/falkordb-image.sh 6      newest released 6.x image (`falkordb/falkordb:latest`)
#   scripts/falkordb-image.sh edge   the image built from FalkorDB's main branch
#   scripts/falkordb-image.sh --pick 4 < tags   newest 4.x tag among the tag names on stdin
#
# 4.x has no floating tag, so its newest release comes from the Docker Hub tags API. If that is
# unreachable, the pinned FALLBACK_4 is printed with a warning on stderr, so a Docker Hub outage
# doesn't fail CI. `latest` is 6.x today; CI checks the server it starts against the line
# (see `just check-server-line`), so a 7.0 `latest` fails loudly instead of silently moving the
# 6.x leg.
set -euo pipefail

REPOSITORY="falkordb/falkordb"
FALLBACK_4="v4.22.4"

# Newest `v<major>.Y.Z` among the tag names on stdin, without suffixed variants such as
# `v4.22.4-alpine` or `v4.22.4-x64`. Versions sort numerically (v4.10.0 after v4.9.0).
pick() {
    local major=$1
    grep -E "^v${major}\.[0-9]+\.[0-9]+$" | sed 's/^v//' | sort -t. -k1,1n -k2,2n -k3,3n \
        | tail -n 1 | sed 's/^/v/'
}

# Every tag name of the repository that starts with `v<major>.`, following the API's pagination.
hub_tags() {
    local major=$1
    local url="https://hub.docker.com/v2/repositories/${REPOSITORY}/tags?page_size=100&name=v${major}."
    while [ -n "$url" ] && [ "$url" != "null" ]; do
        local page
        page=$(curl -fsS --max-time 20 "$url")
        jq -r '.results[].name' <<<"$page"
        url=$(jq -r '.next' <<<"$page")
    done
}

case "${1:-}" in
    --pick)
        pick "${2:?usage: --pick <major>}"
        ;;
    4)
        if tag=$(hub_tags 4 | pick 4) && [ -n "$tag" ]; then
            echo "${REPOSITORY}:${tag}"
        else
            echo "warning: could not list ${REPOSITORY} tags; using ${FALLBACK_4}" >&2
            echo "${REPOSITORY}:${FALLBACK_4}"
        fi
        ;;
    6)
        echo "${REPOSITORY}:latest"
        ;;
    edge)
        echo "${REPOSITORY}:edge"
        ;;
    *)
        echo "usage: $0 4|6|edge, or $0 --pick <major> < tags" >&2
        exit 2
        ;;
esac
