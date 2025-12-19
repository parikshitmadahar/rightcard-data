#!/usr/bin/env python3
"""
RightCard - Canonical_Cards CSV -> validated cards.json + cards_version.json

Usage:
  python3 tools/generate_cards_json.py \
    --csv-url "https://docs.google.com/.../output=csv" \
    --out-dir "./data"

Outputs:
  data/cards.json
  data/cards_version.json
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import urlopen, Request


REQUIRED_HEADERS = [
    "card_key",
    "card_name",
    "issuer",
    "issuer_url",
    "verified_date",
    "dining_multiplier",
    "grocery_multiplier",
    "gas_multiplier",
    "travel_multiplier",
    "other_multiplier",
    "reward_currency",
    "notes",
]

ALLOWED_REWARD_CURRENCY = {"points", "miles", "cashback"}
NOTE_PREFIXES = ("portal_note:", "conditional_note:")

# Locked mapping decision from Step 6.1
CATEGORY_MAPPING = {"travel_includes_transit": True}


@dataclass(frozen=True)
class CardRow:
    card_key: str
    card_name: str
    issuer: str
    issuer_url: str
    verified_date: str
    reward_currency: str
    multipliers: Dict[str, float]
    notes: List[Dict[str, str]]


class ValidationError(Exception):
    pass


def fetch_csv_text(csv_url: str, timeout_seconds: int = 30) -> str:
    req = Request(csv_url, headers={"User-Agent": "RightCard-Generator/1.0"})
    with urlopen(req, timeout=timeout_seconds) as resp:
        raw = resp.read()
    # Google CSV is typically UTF-8
    return raw.decode("utf-8", errors="replace")


def parse_number(value: str, field_name: str, row_id: str) -> float:
    v = value.strip()
    if v == "":
        raise ValidationError(f"[{row_id}] {field_name} is blank (must be a number).")
    try:
        n = float(v)
    except ValueError:
        raise ValidationError(f"[{row_id}] {field_name}='{value}' is not a valid number.")
    if n < 0 or n > 10:
        raise ValidationError(f"[{row_id}] {field_name}={n} out of allowed range 0..10.")
    # Normalize -0.0 to 0.0
    if n == 0:
        n = 0.0
    return n


def validate_date_yyyy_mm_dd(value: str, row_id: str) -> str:
    v = value.strip()
    try:
        dt = datetime.strptime(v, "%Y-%m-%d").date()
    except ValueError:
        raise ValidationError(f"[{row_id}] verified_date='{value}' must be YYYY-MM-DD.")
    today = datetime.now(timezone.utc).date()
    if dt > today:
        raise ValidationError(f"[{row_id}] verified_date='{v}' is in the future.")
    return v


def validate_url_https(value: str, field_name: str, row_id: str) -> str:
    v = value.strip()
    if not v.startswith("https://"):
        raise ValidationError(f"[{row_id}] {field_name} must start with https:// (got '{value}').")
    return v


def parse_notes(notes_cell: str) -> List[Dict[str, str]]:
    """
    Accepts:
      - empty -> []
      - 'portal_note: ...'
      - 'conditional_note: ...'
      - both in same cell, separated by anything, as long as prefixes exist
    Produces structured list: [{"type":"portal_note","text":"..."} ...]
    """
    raw = (notes_cell or "").strip()
    if raw == "":
        return []

    # Split by known prefixes in order of appearance.
    # We scan the string and carve segments starting at each prefix occurrence.
    lower = raw.lower()

    positions: List[Tuple[int, str]] = []
    for prefix in NOTE_PREFIXES:
        p = prefix.lower()
        start = 0
        while True:
            idx = lower.find(p, start)
            if idx == -1:
                break
            positions.append((idx, p))
            start = idx + len(p)

    if not positions:
        # No recognized prefix; treat as generic conditional_note to avoid losing info,
        # but keep it explicit (still parseable).
        return [{"type": "conditional_note", "text": raw}]

    positions.sort(key=lambda t: t[0])

    notes: List[Dict[str, str]] = []
    for i, (pos, pfx) in enumerate(positions):
        seg_start = pos + len(pfx)
        seg_end = positions[i + 1][0] if i + 1 < len(positions) else len(raw)
        text = raw[seg_start:seg_end].strip(" \t\r\n.;")
        if text:
            note_type = "portal_note" if pfx.startswith("portal_note") else "conditional_note"
            notes.append({"type": note_type, "text": text})

    return notes


def validate_headers(headers: List[str]) -> None:
    missing = [h for h in REQUIRED_HEADERS if h not in headers]
    extra = [h for h in headers if h not in REQUIRED_HEADERS]
    if missing:
        raise ValidationError(f"Missing required headers: {missing}")
    if extra:
        # Not fatal, but you said strict validation; keep this strict.
        raise ValidationError(f"Unexpected extra headers: {extra}")


def parse_rows(csv_text: str) -> List[CardRow]:
    reader = csv.DictReader(StringIO(csv_text))
    if reader.fieldnames is None:
        raise ValidationError("CSV has no header row.")
    headers = [h.strip() for h in reader.fieldnames]
    validate_headers(headers)

    seen_keys: set[str] = set()
    cards: List[CardRow] = []

    for idx, row in enumerate(reader, start=2):  # row 1 is header
        card_key = (row.get("card_key") or "").strip()
        row_id = f"row{idx}:{card_key or '(missing card_key)'}"

        if card_key == "":
            raise ValidationError(f"[{row_id}] card_key is blank.")
        if card_key in seen_keys:
            raise ValidationError(f"[{row_id}] duplicate card_key '{card_key}'.")
        seen_keys.add(card_key)

        card_name = (row.get("card_name") or "").strip()
        issuer = (row.get("issuer") or "").strip()
        issuer_url = validate_url_https(row.get("issuer_url") or "", "issuer_url", row_id)
        verified_date = validate_date_yyyy_mm_dd(row.get("verified_date") or "", row_id)
        reward_currency = (row.get("reward_currency") or "").strip().lower()

        if card_name == "":
            raise ValidationError(f"[{row_id}] card_name is blank.")
        if issuer == "":
            raise ValidationError(f"[{row_id}] issuer is blank.")
        if reward_currency not in ALLOWED_REWARD_CURRENCY:
            raise ValidationError(
                f"[{row_id}] reward_currency='{reward_currency}' not in {sorted(ALLOWED_REWARD_CURRENCY)}."
            )

        multipliers = {
            "dining": parse_number(row.get("dining_multiplier") or "", "dining_multiplier", row_id),
            "grocery": parse_number(row.get("grocery_multiplier") or "", "grocery_multiplier", row_id),
            "gas": parse_number(row.get("gas_multiplier") or "", "gas_multiplier", row_id),
            "travel": parse_number(row.get("travel_multiplier") or "", "travel_multiplier", row_id),
            "other": parse_number(row.get("other_multiplier") or "", "other_multiplier", row_id),
        }

        # Additional safety rules:
        # - If it's a no-rewards card (all zeros), ensure it's explicitly noted.
        if all(multipliers[k] == 0.0 for k in multipliers):
            notes_list = parse_notes(row.get("notes") or "")
            if not any(n["type"] == "conditional_note" and "no rewards" in n["text"].lower() for n in notes_list):
                raise ValidationError(
                    f"[{row_id}] all multipliers are 0 but notes does not clearly say 'No rewards'."
                )
        else:
            notes_list = parse_notes(row.get("notes") or "")

        cards.append(
            CardRow(
                card_key=card_key,
                card_name=card_name,
                issuer=issuer,
                issuer_url=issuer_url,
                verified_date=verified_date,
                reward_currency=reward_currency,
                multipliers=multipliers,
                notes=notes_list,
            )
        )

    if not cards:
        raise ValidationError("CSV contains zero card rows.")

    return cards


def build_cards_json(cards: List[CardRow]) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "category_mapping": CATEGORY_MAPPING,
        "cards": [
            {
                "card_key": c.card_key,
                "card_name": c.card_name,
                "issuer": c.issuer,
                "issuer_url": c.issuer_url,
                "verified_date": c.verified_date,
                "reward_currency": c.reward_currency,
                "multipliers": c.multipliers,
                "notes": c.notes,
            }
            for c in cards
        ],
    }



def write_json(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")


def compute_sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-url", required=True, help="Published CSV URL for Canonical_Cards")
    ap.add_argument("--out-dir", required=True, help="Output directory for cards.json and cards_version.json")
    args = ap.parse_args()

    csv_text = fetch_csv_text(args.csv_url)
    cards = parse_rows(csv_text)

    cards_json = build_cards_json(cards)

    out_dir = args.out_dir.rstrip("/")

    cards_json_path = f"{out_dir}/cards.json"
    cards_version_path = f"{out_dir}/cards_version.json"

    # Write cards.json first
    write_json(cards_json_path, cards_json)

    # Deterministic version from file content
    digest = compute_sha256_file(cards_json_path)
    version = f"sha256:{digest[:12]}"

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    cards_version = {
        "schema_version": 1,
        "version": version,
        "generated_at": generated_at,
        "cards_count": len(cards),
    }


    write_json(cards_version_path, cards_version)

    print(f"OK: wrote {cards_json_path}")
    print(f"OK: wrote {cards_version_path}")
    print(f"version={version} cards_count={len(cards)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as e:
        print(f"VALIDATION ERROR: {e}", file=sys.stderr)
        raise SystemExit(2)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
