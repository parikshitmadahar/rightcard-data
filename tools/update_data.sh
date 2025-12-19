#!/usr/bin/env bash
set -euo pipefail

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

if [ -z "${RIGHTCARD_CSV_URL:-}" ]; then
  echo "ERROR: RIGHTCARD_CSV_URL is not set."
  echo "Create a local .env (ignored by git) with:"
  echo 'RIGHTCARD_CSV_URL="https://docs.google.com/.../output=csv"'
  exit 1
fi

echo "Generating JSON from canonical sheet..."
python3 tools/generate_cards_json.py --csv-url "$RIGHTCARD_CSV_URL" --out-dir "."

echo "Staging outputs..."
git add cards.json cards_version.json

if git diff --cached --quiet; then
  echo "No data changes detected. Nothing to commit."
  exit 0
fi

echo "Committing and pushing..."
git commit -m "Update card data"
git push

echo "Done."
