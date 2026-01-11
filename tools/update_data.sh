#!/usr/bin/env bash
set -euo pipefail

# SAFETY LATCH: prevent accidental production updates
# To run for real: RELEASE=1 CONFIRM_PUBLISH=YES ./tools/update_data.sh
if [[ "${RELEASE:-0}" != "1" || "${CONFIRM_PUBLISH:-}" != "YES" ]]; then
  echo "Blocked: update_data.sh would publish to production (git push / GitHub Pages)."
  echo "If you intend to publish, run: RELEASE=1 CONFIRM_PUBLISH=YES ./tools/update_data.sh"
  exit 1
fi

# Run from repo root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Always sync with remote before making changes
git pull --rebase origin main

# Load local env (not committed)
if [ -f .env ]; then
  # shellcheck disable=SC1091
  source .env
fi

# Required env vars
if [ -z "${RIGHTCARD_CSV_URL:-}" ]; then
  echo "ERROR: RIGHTCARD_CSV_URL is not set."
  echo "Create a local .env (ignored by git) with:"
  echo 'RIGHTCARD_CSV_URL="https://docs.google.com/spreadsheets/.../pub?output=csv&gid=..."'
  exit 1
fi

if [ -z "${RIGHTCARD_PROGRAMS_CSV_URL:-}" ]; then
  echo "ERROR: RIGHTCARD_PROGRAMS_CSV_URL is not set."
  echo "Add to your local .env:"
  echo 'RIGHTCARD_PROGRAMS_CSV_URL="https://docs.google.com/spreadsheets/.../pub?output=csv&gid=..."'
  exit 1
fi

if [ -z "${RIGHTCARD_PROGRAM_QUARTERS_CSV_URL:-}" ]; then
  echo "ERROR: RIGHTCARD_PROGRAM_QUARTERS_CSV_URL is not set."
  echo "Add to your local .env:"
  echo 'RIGHTCARD_PROGRAM_QUARTERS_CSV_URL="https://docs.google.com/spreadsheets/.../pub?output=csv&gid=..."'
  exit 1
fi

echo "Generating JSON from canonical sheets (cards + programs + program_quarters)..."
python3 tools/generate_cards_json.py \
  --cards-csv-url "$RIGHTCARD_CSV_URL" \
  --programs-csv-url "$RIGHTCARD_PROGRAMS_CSV_URL" \
  --program-quarters-csv-url "$RIGHTCARD_PROGRAM_QUARTERS_CSV_URL" \
  --out-dir "."

echo "Staging outputs..."
git add cards.json programs.json program_quarters.json cards_version.json

if git diff --cached --quiet; then
  echo "No data changes detected. Nothing to commit."
  exit 0
fi

echo "Committing and pushing..."
git commit -m "Update card data"
git push

echo "Done."
