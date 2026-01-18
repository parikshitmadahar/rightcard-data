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

# Required outputs (bundle must be complete)
required_files=(
  "cards.json"
  "programs.json"
  "program_quarters.json"
  "cards_version.json"
)

echo "Validating generator outputs..."
for f in "${required_files[@]}"; do
  if [[ ! -f "$REPO_ROOT/$f" ]]; then
    echo "ERROR: missing output file: $f"
    echo "Generation did not produce the full bundle. Aborting publish."
    exit 1
  fi
done

# Optional sanity check: conditions should exist at least once (warn-only)
# This prevents the common failure mode where generator stops emitting conditions.
if ! grep -q '"conditions"' "$REPO_ROOT/cards.json"; then
  echo "WARN: cards.json contains no \"conditions\" field anywhere."
  echo "      If you expected condition data, verify generator export before releasing."
fi

echo "Staging outputs..."
git add "${required_files[@]}"

if git diff --cached --quiet; then
  echo "No data changes detected. Nothing to commit."
  exit 0
fi

# Build a more informative commit message from cards_version.json (best-effort).
# Example fields your generator prints: version, cards_count, conditions_count, programs_count, program_quarters_count
commit_msg="Update card data"
if command -v python3 >/dev/null 2>&1; then
  version_line="$(python3 - <<'PY'
import json
try:
    with open("cards_version.json","r",encoding="utf-8") as f:
        p=json.load(f)
    v=p.get("version","")
    cc=p.get("cards_count","")
    pc=p.get("programs_count","")
    pq=p.get("program_quarters_count","")
    # conditions_count may or may not be present; handle both
    cond=p.get("conditions_count","")
    parts=[]
    if v: parts.append(f"v={v}")
    if cc != "": parts.append(f"cards={cc}")
    if cond != "": parts.append(f"conditions={cond}")
    if pc != "": parts.append(f"programs={pc}")
    if pq != "": parts.append(f"quarters={pq}")
    print(" ".join(parts))
except Exception:
    print("")
PY
)"
  if [[ -n "${version_line:-}" ]]; then
    commit_msg="Update card data (${version_line})"
  fi
fi

echo "Committing and pushing..."
git commit -m "$commit_msg"
git push

echo "Done."
