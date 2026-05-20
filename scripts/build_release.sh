#!/usr/bin/env bash
# Build maxc PyInstaller onedir, tar it, compute sha256.
#
# Outputs:
#   ${OUTPUT_DIR:-dist/release}/maxc.tar.gz
#   ${OUTPUT_DIR:-dist/release}/maxc.tar.gz.sha256
#
# Per spec docs/superpowers/specs/2026-05-19-maxc-cliext-integration-design.md § 3.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${REPO_DIR}/dist/release}"
mkdir -p "${OUTPUT_DIR}"

cd "${REPO_DIR}"

# Clean prior build artifacts so PyInstaller doesn't reuse stale caches.
rm -rf dist/maxc build

pyinstaller --noconfirm maxc.spec

test -d dist/maxc || { echo "ERR: dist/maxc not produced" >&2; exit 1; }
if [ -f dist/maxc/maxc.exe ]; then
  BIN="maxc.exe"
elif [ -f dist/maxc/maxc ]; then
  BIN="maxc"
else
  echo "ERR: no maxc[.exe] in dist/maxc/" >&2
  exit 1
fi

# Tar the onedir, preserving the top-level 'maxc/' directory (OSS contract).
# COPYFILE_DISABLE=1 suppresses macOS AppleDouble (._foo) resource-fork entries
# that would otherwise pollute the tarball when built on darwin.
( cd dist && COPYFILE_DISABLE=1 tar -czf "${OUTPUT_DIR}/maxc.tar.gz" maxc )

# Compute sha256 (single-line hex digest, no filename).
cd "${OUTPUT_DIR}"
if command -v shasum >/dev/null 2>&1; then
  shasum -a 256 maxc.tar.gz | awk '{print $1}' > maxc.tar.gz.sha256
elif command -v sha256sum >/dev/null 2>&1; then
  sha256sum maxc.tar.gz | awk '{print $1}' > maxc.tar.gz.sha256
else
  echo "ERR: neither shasum nor sha256sum available" >&2
  exit 1
fi

echo "==> built: ${OUTPUT_DIR}/maxc.tar.gz"
echo "==> sha256: $(cat maxc.tar.gz.sha256)"
echo "==> entry binary: ${BIN}"
