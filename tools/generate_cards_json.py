#!/usr/bin/env python3
"""
RightCard - Canonical_Cards CSV -> validated cards.json + cards_version.json

Usage:
  python3 tools/generate_cards_json.py \
    --csv-url "https://docs.google.com/.../output=csv" \
    --out-dir "."

Outputs:
  cards.json
  cards_version.json

Key behavior:
- cards.json is deterministic for identical CSV input.
- cards_version.json is ONLY rewritten when cards.json content changes (version changes),
  so running the script without sheet changes will NOT create noise commits.

Schema v2:
- multipliers.grocery and multipliers.travel can now be either:
  - number (legacy scalar), or
  - object {"default": <number>, "<sub>": <number>, ...}
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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.request import Request, urlopen


SCHEMA_VERSION = 2

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

# Optional new sub-category columns (v1 taxonomy)
OPTIONAL_SUBCATEGORY_HEADERS = [
    "grocery_default",
    "grocery_online",
    "grocery_in_store",
    "travel_default",
    "travel_flight",
    "travel_hotel",
]

# Columns that may exist in the Canonical_Cards sheet for internal workflows
# but are ignored by the JSON generator (must not affect output schema).
IGNORED_HEADERS = {
    "ai_check_date (when verifier last checked this card)",
    "ai_status",
    "ai_confidence_overall",
}


ALLOWED_REWARD_CURRENCY = {"points", "miles", "cashback"}
NOTE_PREFIXES = ("portal_note:", "conditional_note:")

# Locked mapping decision from Step 6.1
CATEGORY_MAPPING = {"travel_includes_transit": True}


MultiplierValue = Union[float, Dict[str, float]]


@dataclass(frozen=True)
class CardRow:
    card_key: str
    card_name: str
    issuer: str
    issuer_url: str
    verified_date: str
    reward_currency: str
    multipliers: Dict[str, MultiplierValue]
    notes: List[Dict[str, str]]


class ValidationError(Exception):
    pass


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json_if_exists(path: str) -> Optional[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def fetch_csv_text(csv_url: str, timeout_seconds: int = 30) -> str:
    req = Request(csv_url, headers={"User-Agent": "RightCard-Generator/1.0"})
    with urlopen(req, timeout=timeout_seconds) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def parse_number(value: str, field_name: str, row_id: str) -> float:
    v = (value or "").strip()
    if v == "":
        raise ValidationError(f"[{row_id}] {field_name} is blank (must be a number).")
    try:
        n = float(v)
    except ValueError:
        raise ValidationError(f"[{row_id}] {field_name}='{value}' is not a valid number.")
    if n < 0 or n > 10:
        raise ValidationError(f"[{row_id}] {field_name}={n} out of allowed range 0..10.")
    if n == 0:
        n = 0.0
    return n


def parse_optional_number(value: str) -> Optional[float]:
    v = (value or "").strip()
    if v == "":
        return None
    try:
        return float(v)
    except ValueError:
        raise ValidationError(f"Invalid number: '{value}'")


def validate_date_yyyy_mm_dd(value: str, row_id: str) -> str:
    v = (value or "").strip()
    try:
        dt = datetime.strptime(v, "%Y-%m-%d").date()
    except ValueError:
        raise ValidationError(f"[{row_id}] verified_date='{value}' must be YYYY-MM-DD.")
    today = datetime.now(timezone.utc).date()
    if dt > today:
        raise ValidationError(f"[{row_id}] verified_date='{v}' is in the future.")
    return v


def validate_url_https(value: str, field_name: str, row_id: str) -> str:
    v = (value or "").strip()
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
        return [{"type": "conditional_note", "text": raw}]

    positions.sort(key=lambda t: t[0])

    notes: List[Dict[str, str]] = []
    for i, (pos, pfx) in enumerate(positions):
        seg_start = pos + len(pfx)
        seg_end = positions[i + 1][0] if i + 1 < len(positions) else len(raw)
        text = raw[seg_start:seg_end].strip(" \t\r\n.;|")
        if text:
            note_type = "portal_note" if pfx.startswith("portal_note") else "conditional_note"
            notes.append({"type": note_type, "text": text})

    return notes


def validate_headers(headers: List[str]) -> None:
    # Ignore internal workflow/metadata headers
    effective_headers = [h for h in headers if h not in IGNORED_HEADERS]

    # Required base headers must exist (sub-category headers are optional)
    missing = [h for h in REQUIRED_HEADERS if h not in effective_headers]
    if missing:
        raise ValidationError(f"Missing required headers: {missing}")

    # Validate there are no unexpected columns other than:
    # - required headers
    # - optional sub-category headers
    allowed = set(REQUIRED_HEADERS) | set(OPTIONAL_SUBCATEGORY_HEADERS)
    extra = [h for h in effective_headers if h not in allowed]
    if extra:
        raise ValidationError(f"Unexpected extra headers: {extra}")


def build_subcategory_multiplier(
    row: Dict[str, str],
    *,
    row_id: str,
    legacy_key: str,
    default_key: str,
    sub_keys: List[str],
) -> MultiplierValue:
    """
    Builds grocery/travel multiplier structure.

    Resolution rules:
    1) If any sub_key is present (non-empty), emit an object:
         {"default": <resolved_default>, "<sub>": <val>, ...}
       where resolved_default uses:
         - default_key if provided, else legacy_key.
       If still missing, default falls back to legacy scalar value already validated elsewhere,
       and if that is missing (shouldn't happen), fallback to 1.0 conservatively.

    2) If no sub_key is present:
       - If default_key is provided (non-empty), emit {"default": <default>}
       - Else emit legacy scalar (float) from legacy_key
    """
    # Legacy scalar is always required by REQUIRED_HEADERS and validated elsewhere.
    legacy_scalar = parse_number(row.get(legacy_key) or "", legacy_key, row_id)

    default_val = parse_optional_number(row.get(default_key) or "")
    sub_vals: Dict[str, float] = {}
    any_sub_present = False

    for sk in sub_keys:
        v = parse_optional_number(row.get(sk) or "")
        if v is not None:
            any_sub_present = True
            # "grocery_online" -> "online", "travel_hotel" -> "hotel"
            # sk like "grocery_in_store" or "travel_hotel"
            if sk.startswith("grocery_"):
                sub_name = sk[len("grocery_"):]
            elif sk.startswith("travel_"):
                sub_name = sk[len("travel_"):]
            else:
                sub_name = sk

            if v < 0 or v > 10:
                raise ValidationError(f"[{row_id}] {sk}={v} out of allowed range 0..10.")
            sub_vals[sub_name] = v

    if any_sub_present:
        resolved_default = default_val if default_val is not None else legacy_scalar
        if resolved_default is None:
            resolved_default = 1.0
        out: Dict[str, float] = {"default": float(resolved_default)}
        for name in sorted(sub_vals.keys()):
            out[name] = float(sub_vals[name])
        return out

    # No subcategory fields present; emit default object only if explicitly set.
    if default_val is not None:
        if default_val < 0 or default_val > 10:
            raise ValidationError(f"[{row_id}] {default_key}={default_val} out of allowed range 0..10.")
        return {"default": float(default_val)}

    # Pure legacy fallback
    return float(legacy_scalar)


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

        # Base categories remain scalar floats (legacy)
        dining = parse_number(row.get("dining_multiplier") or "", "dining_multiplier", row_id)
        gas = parse_number(row.get("gas_multiplier") or "", "gas_multiplier", row_id)
        other = parse_number(row.get("other_multiplier") or "", "other_multiplier", row_id)

        # Grocery + Travel: subcategory-aware
        grocery = build_subcategory_multiplier(
            row,
            row_id=row_id,
            legacy_key="grocery_multiplier",
            default_key="grocery_default",
            sub_keys=["grocery_online", "grocery_in_store"],
        )
        travel = build_subcategory_multiplier(
            row,
            row_id=row_id,
            legacy_key="travel_multiplier",
            default_key="travel_default",
            sub_keys=["travel_flight", "travel_hotel"],
        )

        multipliers: Dict[str, MultiplierValue] = {
            "dining": float(dining),
            "grocery": grocery,
            "gas": float(gas),
            "travel": travel,
            "other": float(other),
        }

        notes_list = parse_notes(row.get("notes") or "")

        # Additional safety rule: if all legacy scalar categories are zero,
        # notes should clearly indicate no rewards.
        # (For grocery/travel objects, treat "default" as the comparable scalar if present.)
        def _scalar_for_check(v: MultiplierValue) -> float:
            if isinstance(v, dict):
                return float(v.get("default", 0.0))
            return float(v)

        scalar_check = {
            "dining": _scalar_for_check(multipliers["dining"]),
            "grocery": _scalar_for_check(multipliers["grocery"]),
            "gas": _scalar_for_check(multipliers["gas"]),
            "travel": _scalar_for_check(multipliers["travel"]),
            "other": _scalar_for_check(multipliers["other"]),
        }

        if all(scalar_check[k] == 0.0 for k in scalar_check):
            if not any(
                n["type"] == "conditional_note" and "no rewards" in n["text"].lower()
                for n in notes_list
            ):
                raise ValidationError(f"[{row_id}] all multipliers are 0 but notes does not clearly say 'No rewards'.")

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
    # Ensure deterministic order: cards are already in CSV order.
    return {
        "schema_version": SCHEMA_VERSION,
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
    # Stable formatting + newline at end
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def compute_sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-url", required=True, help="Published CSV URL for Canonical_Cards")
    ap.add_argument("--out-dir", default=".", help="Output directory for cards.json and cards_version.json (default: .)")
    args = ap.parse_args()

    csv_text = fetch_csv_text(args.csv_url)
    cards = parse_rows(csv_text)

    cards_json = build_cards_json(cards)

    out_dir = args.out_dir.rstrip("/")
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    cards_json_path = f"{out_dir}/cards.json"
    cards_version_path = f"{out_dir}/cards_version.json"


    # 1) Write cards.json deterministically
    write_json(cards_json_path, cards_json)

    # 2) Compute deterministic version from cards.json bytes
    digest = compute_sha256_file(cards_json_path)
    version = f"sha256:{digest[:12]}"

    # 3) Only rewrite cards_version.json when version changes
    existing = _read_json_if_exists(cards_version_path)
    existing_version = existing.get("version") if isinstance(existing, dict) else None

    if existing_version == version:
        print(f"OK: wrote {cards_json_path}")
        print(f"OK: cards_version.json unchanged (version={version})")
        print(f"version={version} cards_count={len(cards)}")
        return 0

    cards_version = {
        "schema_version": SCHEMA_VERSION,
        "version": version,
        "generated_at": _utc_now_z(),
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
