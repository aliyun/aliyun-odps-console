#!/usr/bin/env bash
# Build maxc PyInstaller onedir, tar it, compute sha256.
#
# Outputs:
#   ${OUTPUT_DIR:-dist/release}/maxc.tar.gz
#   ${OUTPUT_DIR:-dist/release}/maxc.tar.gz.sha256
#
# Set MAXC_MACOS_CODESIGN_IDENTITY and MAXC_MACOS_NOTARY_PROFILE for a
# signed build using credentials in the current macOS Keychain.
#
# Per spec docs/superpowers/specs/2026-05-19-maxc-cliext-integration-design.md § 3.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${REPO_DIR}/dist/release}"
KERNEL="$(uname -s)"
CODESIGN_IDENTITY="${MAXC_MACOS_CODESIGN_IDENTITY:-}"
NOTARY_PROFILE="${MAXC_MACOS_NOTARY_PROFILE:-}"

if [[ -n "$CODESIGN_IDENTITY" || -n "$NOTARY_PROFILE" ]]; then
  if [[ "$KERNEL" != Darwin || -z "$CODESIGN_IDENTITY" || -z "$NOTARY_PROFILE" ]]; then
    echo "ERR: macOS signing requires Darwin, a signing identity and a notary profile" >&2
    exit 1
  fi
fi

mkdir -p "${OUTPUT_DIR}"
# Resolve to absolute so the `cd dist` below doesn't break the path.
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"

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

if [[ -n "$CODESIGN_IDENTITY" ]]; then
  NOTARY_DIR="$(mktemp -d)"
  trap 'rm -rf "$NOTARY_DIR"' EXIT

  if ! codesign --display --verbose=4 "dist/maxc/$BIN" 2>&1 |
      awk '/^Authority=Developer ID Application: / { found=1 } END { exit !found }'; then
    echo "ERR: maxc requires a Developer ID Application signature" >&2
    exit 1
  fi

  find dist/maxc -type f -print0 > "$NOTARY_DIR/files"
  while IFS= read -r -d '' binary; do
    if [[ "$(file -b "$binary")" == *Mach-O* ]]; then
      codesign --verify --strict -R='anchor apple generic' "$binary"
    fi
  done < "$NOTARY_DIR/files"

  ditto -c -k --keepParent dist/maxc "$NOTARY_DIR/maxc.zip"
  notary_options=(--keychain-profile "$NOTARY_PROFILE" --wait --output-format json)
  if [[ -n "${MAXC_MACOS_NOTARY_KEYCHAIN:-}" ]]; then
    notary_options+=(--keychain "$MAXC_MACOS_NOTARY_KEYCHAIN")
  fi
  notary_status=0
  xcrun notarytool submit "$NOTARY_DIR/maxc.zip" "${notary_options[@]}" \
    > "$NOTARY_DIR/result.json" || notary_status=$?
  python3 - "$NOTARY_DIR/result.json" "$notary_status" <<'PY'
import json
import sys

try:
    with open(sys.argv[1]) as submission:
        result = json.load(submission)
except (OSError, ValueError) as error:
    raise SystemExit(f"ERR: could not read Apple notarization result: {error}") from error
if not isinstance(result, dict):
    raise SystemExit("ERR: Apple notarization returned an invalid result")
print(f"Apple notarization {result.get('id', '(no submission ID)')}: {result.get('status')}")
if sys.argv[2] != "0" or result.get("status") != "Accepted":
    raise SystemExit("ERR: Apple did not accept the notarization submission")
PY
fi

# Tar the onedir, preserving the top-level 'maxc/' directory (OSS contract).
# macOS BSD tar archives xattrs by default, so suppress AppleDouble entries and
# provenance/quarantine PAX headers there. manylinux2014 still ships GNU tar
# 1.26 (before --no-xattrs existed), and GNU tar does not archive xattrs unless
# explicitly asked, so the portable default command is already clean on Linux.
if [ "$KERNEL" = "Darwin" ]; then
  ( cd dist && COPYFILE_DISABLE=1 tar --no-xattrs -czf "${OUTPUT_DIR}/maxc.tar.gz" maxc )
else
  ( cd dist && tar -czf "${OUTPUT_DIR}/maxc.tar.gz" maxc )
fi

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
