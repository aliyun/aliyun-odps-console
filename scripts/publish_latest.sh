#!/usr/bin/env bash
# Flip the OSS pointer file versions/latest to a new version.
#
# Pre-condition: all 5 supported platforms for $VERSION must already be in OSS
# (windows-arm64 is intentionally excluded — see .github/workflows/maxc-release.yml).
# This script verifies that pre-condition (HEAD each artifact) before flipping
# so the cliext launcher never sees versions/latest pointing at an
# incomplete release.
#
# Per spec docs/superpowers/specs/2026-05-19-maxc-cliext-integration-design.md § 3.
#
# Required env:
#   VERSION                 — semver string to flip to, e.g. 0.2.5
#   OSS_BUCKET              — bucket name
#   OSS_REGION              — e.g. cn-hangzhou
#   OSS_ACCESS_KEY_ID       — RAM AK with PutObject on the bucket prefix
#   OSS_ACCESS_KEY_SECRET   — corresponding SK
# Optional:
#   OSS_PREFIX    — path prefix inside the bucket (no leading/trailing slash),
#                   e.g. "maxc-cli". Empty by default.
#   OSS_ENDPOINT  — override; default derived from OSS_REGION

set -euo pipefail

: "${VERSION:?VERSION env required}"
: "${OSS_BUCKET:?OSS_BUCKET env required}"
: "${OSS_REGION:?OSS_REGION env required}"
: "${OSS_ACCESS_KEY_ID:?OSS_ACCESS_KEY_ID env required}"
: "${OSS_ACCESS_KEY_SECRET:?OSS_ACCESS_KEY_SECRET env required}"

ENDPOINT="${OSS_ENDPOINT:-oss-${OSS_REGION}.aliyuncs.com}"

PREFIX="${OSS_PREFIX:-}"
PREFIX="${PREFIX#/}"
PREFIX="${PREFIX%/}"
if [ -n "$PREFIX" ]; then PREFIX="${PREFIX}/"; fi

BASE_URL="https://${OSS_BUCKET}.${ENDPOINT}/${PREFIX}"

PLATFORMS=(linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64)

echo "==> verifying all ${#PLATFORMS[@]} platforms exist for v${VERSION}"
missing=()
for p in "${PLATFORMS[@]}"; do
  for f in maxc.tar.gz maxc.tar.gz.sha256; do
    if ! curl -fsI "${BASE_URL}${VERSION}/${p}/${f}" >/dev/null; then
      missing+=("${p}/${f}")
    fi
  done
done

if [ ${#missing[@]} -gt 0 ]; then
  echo "ERR: refusing to flip latest, missing artifacts:" >&2
  printf '  - %s\n' "${missing[@]}" >&2
  exit 1
fi

# All present — write the pointer
TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
printf '%s' "$VERSION" > "$TMP"

# Pass AK/SK via flags (see upload_to_oss.sh for why env vars don't work
# with ossutil 1.7.x).
ossutil cp "$TMP" "oss://${OSS_BUCKET}/${PREFIX}versions/latest" \
  --endpoint "$ENDPOINT" \
  --access-key-id "${OSS_ACCESS_KEY_ID}" \
  --access-key-secret "${OSS_ACCESS_KEY_SECRET}" \
  --acl public-read --force

echo "==> versions/latest is now ${VERSION}"
