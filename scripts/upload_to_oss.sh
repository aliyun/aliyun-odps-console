#!/usr/bin/env bash
# Upload a built {maxc.tar.gz, maxc.tar.gz.sha256} pair to the OSS bucket
# under the canonical path: {version}/{platform}/.
#
# Per spec docs/superpowers/specs/2026-05-19-maxc-cliext-integration-design.md § 3.
#
# Required env:
#   VERSION                 — semver string, e.g. 0.2.5
#   PLATFORM                — one of: linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64 windows-arm64
#   OSS_BUCKET              — bucket name (e.g. maxcompute-repo)
#   OSS_REGION              — e.g. cn-hangzhou
#   INPUT_DIR               — directory containing maxc.tar.gz + maxc.tar.gz.sha256
#   OSS_ACCESS_KEY_ID       — RAM AK with PutObject on the bucket prefix
#   OSS_ACCESS_KEY_SECRET   — corresponding SK
# Optional:
#   OSS_PREFIX    — path prefix inside the bucket (no leading/trailing slash),
#                   e.g. "maxc-cli". Empty by default. Required when the
#                   bucket is shared with other projects.
#   OSS_ENDPOINT  — override; default derived from OSS_REGION
#   DRY_RUN=1     — print ossutil commands without executing

set -euo pipefail

: "${VERSION:?VERSION env required}"
: "${PLATFORM:?PLATFORM env required}"
: "${OSS_BUCKET:?OSS_BUCKET env required}"
: "${OSS_REGION:?OSS_REGION env required}"
: "${INPUT_DIR:?INPUT_DIR env required}"
: "${OSS_ACCESS_KEY_ID:?OSS_ACCESS_KEY_ID env required}"
: "${OSS_ACCESS_KEY_SECRET:?OSS_ACCESS_KEY_SECRET env required}"

case "$PLATFORM" in
  linux-amd64|linux-arm64|darwin-amd64|darwin-arm64|windows-amd64|windows-arm64) ;;
  *) echo "ERR: unsupported PLATFORM=${PLATFORM}" >&2; exit 1 ;;
esac

ENDPOINT="${OSS_ENDPOINT:-oss-${OSS_REGION}.aliyuncs.com}"

# Normalize OSS_PREFIX: strip surrounding slashes, then add a trailing slash
# only if non-empty. This keeps the DEST path clean whether the user passes
# "maxc-cli", "/maxc-cli/", or leaves it unset.
PREFIX="${OSS_PREFIX:-}"
PREFIX="${PREFIX#/}"
PREFIX="${PREFIX%/}"
if [ -n "$PREFIX" ]; then PREFIX="${PREFIX}/"; fi

DEST="oss://${OSS_BUCKET}/${PREFIX}${VERSION}/${PLATFORM}/"

TARBALL="${INPUT_DIR}/maxc.tar.gz"
SHA="${INPUT_DIR}/maxc.tar.gz.sha256"

test -f "$TARBALL" || { echo "ERR: missing $TARBALL" >&2; exit 1; }
test -f "$SHA"     || { echo "ERR: missing $SHA" >&2; exit 1; }

run() {
  if [ "${DRY_RUN:-0}" = "1" ]; then
    # Mask credentials in the dry-run echo so they don't leak into local stdout.
    masked=$(echo "$*" | sed -E \
      -e 's/(--access-key-id )[^ ]+/\1<redacted>/' \
      -e 's/(--access-key-secret )[^ ]+/\1<redacted>/')
    echo "DRY: ${masked}"
  else
    "$@"
  fi
}

# Pass AK/SK explicitly via flags rather than env, because ossutil 1.7.x
# does NOT honor OSS_ACCESS_KEY_ID/SECRET env vars by default — it expects
# either ~/.ossutilconfig, the -i/-k flags, or the longer --access-key-id /
# --access-key-secret flags. The env failure mode shows up as
# "accessKeyID and ecsUrl are both empty".
OSSUTIL_CRED=(--access-key-id "${OSS_ACCESS_KEY_ID}" \
              --access-key-secret "${OSS_ACCESS_KEY_SECRET}")

run ossutil cp "$TARBALL" "$DEST" --endpoint "$ENDPOINT" "${OSSUTIL_CRED[@]}" --acl public-read --force
run ossutil cp "$SHA"     "$DEST" --endpoint "$ENDPOINT" "${OSSUTIL_CRED[@]}" --acl public-read --force

echo "==> uploaded ${PLATFORM} v${VERSION} to ${DEST}"
