#!/usr/bin/env bash
# Load the CI signing material into a temporary Keychain and build a signed release.
set -euo pipefail
set +x

if [[ "$(uname -s)" != Darwin ]]; then
  echo "ERR: the macOS signing job requires a Darwin runner" >&2
  exit 1
fi
: "${RUNNER_TEMP:?missing CI runner temporary directory}"
: "${MAXC_MACOS_CERTIFICATE_P12:?missing base64 Developer ID Application certificate and key}"
: "${MAXC_MACOS_CERTIFICATE_PASSWORD:?missing certificate import password}"
: "${MAXC_MACOS_NOTARY_APPLE_ID:?missing Apple ID for notarization}"
: "${MAXC_MACOS_NOTARY_TEAM_ID:?missing Apple Developer Team ID}"
: "${MAXC_MACOS_NOTARY_PASSWORD:?missing Apple app-specific password for notarization}"

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORIGINAL_UMASK="$(umask)"
umask 077
SIGNING_DIR="$(mktemp -d "$RUNNER_TEMP/maxc-signing.XXXXXXXX")"
KEYCHAIN="$SIGNING_DIR/signing.keychain-db"
ORIGINAL_KEYCHAINS=()
SEARCH_LIST_CHANGED=0

cleanup() {
  if [[ "$SEARCH_LIST_CHANGED" = 1 ]]; then
    if (( ${#ORIGINAL_KEYCHAINS[@]} )); then
      /usr/bin/security list-keychains -d user -s "${ORIGINAL_KEYCHAINS[@]}" || true
    else
      /usr/bin/security list-keychains -d user -s || true
    fi
  fi
  /usr/bin/security delete-keychain "$KEYCHAIN" >/dev/null 2>&1 || true
  rm -rf "$SIGNING_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

/usr/bin/security list-keychains -d user > "$SIGNING_DIR/keychains"
while IFS= read -r line; do
  if [[ "$line" == *\"*\" ]]; then
    line="${line#*\"}"
    ORIGINAL_KEYCHAINS+=("${line%\"}")
  fi
done < "$SIGNING_DIR/keychains"

KEYCHAIN_PASSWORD="$(/usr/bin/openssl rand -hex 32)"
/usr/bin/security create-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN"
/usr/bin/security set-keychain-settings -lut 21600 "$KEYCHAIN"
/usr/bin/security unlock-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN"

printf '%s' "$MAXC_MACOS_CERTIFICATE_P12" | /usr/bin/base64 -D > "$SIGNING_DIR/certificate.p12"
/usr/bin/security import "$SIGNING_DIR/certificate.p12" -f pkcs12 -k "$KEYCHAIN" \
  -P "$MAXC_MACOS_CERTIFICATE_PASSWORD" -T /usr/bin/codesign
/usr/bin/security set-key-partition-list -S apple-tool:,apple: -s \
  -k "$KEYCHAIN_PASSWORD" "$KEYCHAIN" >/dev/null

SEARCH_LIST_CHANGED=1
if (( ${#ORIGINAL_KEYCHAINS[@]} )); then
  /usr/bin/security list-keychains -d user -s "$KEYCHAIN" "${ORIGINAL_KEYCHAINS[@]}"
else
  /usr/bin/security list-keychains -d user -s "$KEYCHAIN"
fi

MAXC_MACOS_CODESIGN_IDENTITY="$(/usr/bin/security find-identity -v -p codesigning "$KEYCHAIN" |
  awk '/"Developer ID Application: / { print $2 }')"
if [[ ! "$MAXC_MACOS_CODESIGN_IDENTITY" =~ ^[[:xdigit:]]{40}$ ]]; then
  echo "ERR: expected one valid Developer ID Application identity with a private key" >&2
  exit 1
fi

MAXC_MACOS_NOTARY_PROFILE="maxc-ci"
/usr/bin/xcrun notarytool store-credentials "$MAXC_MACOS_NOTARY_PROFILE" \
  --apple-id "$MAXC_MACOS_NOTARY_APPLE_ID" --team-id "$MAXC_MACOS_NOTARY_TEAM_ID" \
  --password "$MAXC_MACOS_NOTARY_PASSWORD" --keychain "$KEYCHAIN" >/dev/null
rm -f "$SIGNING_DIR/certificate.p12"
unset MAXC_MACOS_CERTIFICATE_P12 MAXC_MACOS_CERTIFICATE_PASSWORD
unset MAXC_MACOS_NOTARY_APPLE_ID MAXC_MACOS_NOTARY_TEAM_ID MAXC_MACOS_NOTARY_PASSWORD KEYCHAIN_PASSWORD

umask "$ORIGINAL_UMASK"
export MAXC_MACOS_CODESIGN_IDENTITY MAXC_MACOS_NOTARY_PROFILE
export MAXC_MACOS_NOTARY_KEYCHAIN="$KEYCHAIN"
bash "$REPO_DIR/scripts/build_release.sh"
