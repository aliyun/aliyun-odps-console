#!/usr/bin/env bash
# Install maxc-cli skill for Cursor/Windsurf
set -euo pipefail

SKILL_DIR="${HOME}/.cursor/skills/use-maxc-cli"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_DIR="${SCRIPT_DIR}/../../skills/use-maxc-cli"

mkdir -p "${SKILL_DIR}"
cp -r "${SOURCE_DIR}/"* "${SKILL_DIR}/"

echo "✅ Installed use-maxc-cli skill to ${SKILL_DIR}"
echo "Restart Cursor to pick up the new skill."
