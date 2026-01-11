#!/usr/bin/env python3
"""
RightCard - Canonical CSVs -> validated cards.json + programs.json + program_quarters.json + cards_version.json

Usage (new, recommended):
  python3 tools/generate_cards_json.py \
    --cards-csv-url "https://docs.google.com/...Canonical_Cards...output=csv" \
    --programs-csv-url "https://docs.google.com/...programs...output=csv" \
    --program-quarters-csv-url "https://docs.google.com/...program_quarters...output=csv" \
    --out-dir "."

Back-compat:
  --csv-url is accepted as an alias for --cards-csv-url (cards only).

Outputs:
  cards.json
  programs.json
  program_quarters.json
  cards_version.json

Key behavior:
- JSON outputs are deterministic for identical CSV inputs.
- cards_version.json is ONLY rewritten when the combined data bundle content changes.
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

# -------------------------
# Canonical Cards schema
# -------------------------
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

OPTIONAL_HEADERS = [
    # subcategory columns (v1 taxonomy)
    "grocery_default",
    "grocery_online",
    "grocery_in_store",
    "travel_default",
    "travel_flight",
    "travel_hotel",
    # NEW: Optional column in Canonical_Cards to link rotating programs
    "program_links",
]

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
    program_links: List[str]


# -------------------------
# Programs schema
# -------------------------
PROGRAMS_REQUIRED_HEADERS = [
    "program_key",
    "program_name",
    "issuer",
    "source_url",
    "requires_activation",
    "cap_amount",
    "cap_period",
    "base_rate",
    "bonus_rate",
    "notes",
    "status",
    "last_verified",
]

PROGRAM_QUARTERS_REQUIRED_HEADERS = [
    "program_key",
    "start_date",
    "end_date",
    "category",
    "status",
    "last_verified",
]


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


def parse_optional_int(value: str) -> Optional[int]:
    v = (value or "").strip()
    if v == "":
        return None
    try:
        return int(float(v))
    except ValueError:
        raise ValidationError(f"Invalid integer: '{value}'")


def validate_date_yyyy_mm_dd(value: str, field_name: str, row_id: str) -> str:
    v = (value or "").strip()
    try:
        dt = datetime.strptime(v, "%Y-%m-%d").date()
    except ValueError:
        raise ValidationError(f"[{row_id}] {field_name}='{value}' must be YYYY-MM-DD.")
    today = datetime.now(timezone.utc).date()
    if field_name == "verified_date" and dt > today:
        raise ValidationError(f"[{row_id}] {field_name}='{v}' is in the future.")
    return v


def validate_url_https(value: str, field_name: str, row_id: str) -> str:
    v = (value or "").strip()
    if not v.startswith("https://"):
        raise ValidationError(f"[{row_id}] {field_name} must start with https:// (got '{value}').")
    return v


def parse_bool_true_false(value: str, field_name: str, row_id: str) -> bool:
    v = (value or "").strip().lower()
    if v in {"true", "t", "yes", "y", "1"}:
        return True
    if v in {"false", "f", "no", "n", "0"}:
        return False
    raise ValidationError(f"[{row_id}] {field_name} must be TRUE/FALSE (got '{value}').")


def parse_program_links(cell: str) -> List[str]:
    """
    Accepts comma or pipe separated program_keys.
    Examples:
      "" -> []
      "discover_5pct_rotating" -> ["discover_5pct_rotating"]
      "discover_5pct_rotating, chase_5pct_rotating" -> [...]
      "discover_5pct_rotating|chase_5pct_rotating" -> [...]
    """
    raw = (cell or "").strip()
    if raw == "":
        return []
    normalized = raw.replace("|", ",")
    parts = [p.strip() for p in normalized.split(",")]
    out = [p for p in parts if p]
    # deterministic order + de-dup
    return sorted(set(out))


def parse_notes(notes_cell: str) -> List[Dict[str, str]]:
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
    effective_headers = [h for h in headers if h not in IGNORED_HEADERS]

    missing = [h for h in REQUIRED_HEADERS if h not in effective_headers]
    if missing:
        raise ValidationError(f"Missing required headers: {missing}")

    allowed = set(REQUIRED_HEADERS) | set(OPTIONAL_HEADERS)
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
    legacy_scalar = parse_number(row.get(legacy_key) or "", legacy_key, row_id)

    default_val = parse_optional_number(row.get(default_key) or "")
    sub_vals: Dict[str, float] = {}
    any_sub_present = False

    for sk in sub_keys:
        v = parse_optional_number(row.get(sk) or "")
        if v is not None:
            any_sub_present = True
            if sk.startswith("grocery_"):
                sub_name = sk[len("grocery_") :]
            elif sk.startswith("travel_"):
                sub_name = sk[len("travel_") :]
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

    if default_val is not None:
        if default_val < 0 or default_val > 10:
            raise ValidationError(f"[{row_id}] {default_key}={default_val} out of allowed range 0..10.")
        return {"default": float(default_val)}

    return float(legacy_scalar)


def parse_cards(csv_text: str) -> List[CardRow]:
    reader = csv.DictReader(StringIO(csv_text))
    if reader.fieldnames is None:
        raise ValidationError("cards CSV has no header row.")

    headers = [h.strip() for h in reader.fieldnames]
    validate_headers(headers)

    seen_keys: set[str] = set()
    cards: List[CardRow] = []

    for idx, row in enumerate(reader, start=2):
        card_key = (row.get("card_key") or "").strip()
        row_id = f"cards_row{idx}:{card_key or '(missing card_key)'}"

        if card_key == "":
            raise ValidationError(f"[{row_id}] card_key is blank.")
        if card_key in seen_keys:
            raise ValidationError(f"[{row_id}] duplicate card_key '{card_key}'.")
        seen_keys.add(card_key)

        card_name = (row.get("card_name") or "").strip()
        issuer = (row.get("issuer") or "").strip()
        issuer_url = validate_url_https(row.get("issuer_url") or "", "issuer_url", row_id)
        verified_date = validate_date_yyyy_mm_dd(row.get("verified_date") or "", "verified_date", row_id)
        reward_currency = (row.get("reward_currency") or "").strip().lower()

        if card_name == "":
            raise ValidationError(f"[{row_id}] card_name is blank.")
        if issuer == "":
            raise ValidationError(f"[{row_id}] issuer is blank.")
        if reward_currency not in ALLOWED_REWARD_CURRENCY:
            raise ValidationError(
                f"[{row_id}] reward_currency='{reward_currency}' not in {sorted(ALLOWED_REWARD_CURRENCY)}."
            )

        dining = parse_number(row.get("dining_multiplier") or "", "dining_multiplier", row_id)
        gas = parse_number(row.get("gas_multiplier") or "", "gas_multiplier", row_id)
        other = parse_number(row.get("other_multiplier") or "", "other_multiplier", row_id)

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
        program_links = parse_program_links(row.get("program_links") or "")

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
                program_links=program_links,
            )
        )

    if not cards:
        raise ValidationError("cards CSV contains zero card rows.")

    return cards


def build_cards_json(cards: List[CardRow]) -> Dict[str, Any]:
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
                **({"program_links": c.program_links} if c.program_links else {}),
            }
            for c in cards
        ],
    }


# -------------------------
# Programs parsing/building
# -------------------------
def _validate_required_headers(reader: csv.DictReader, required: List[str], label: str) -> List[str]:
    if reader.fieldnames is None:
        raise ValidationError(f"{label} CSV has no header row.")
    headers = [h.strip() for h in reader.fieldnames]
    missing = [h for h in required if h not in headers]
    if missing:
        raise ValidationError(f"{label} missing required headers: {missing}")
    return headers


def parse_programs(csv_text: str) -> List[Dict[str, Any]]:
    reader = csv.DictReader(StringIO(csv_text))
    _validate_required_headers(reader, PROGRAMS_REQUIRED_HEADERS, "programs")

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for idx, row in enumerate(reader, start=2):
        key = (row.get("program_key") or "").strip()
        row_id = f"programs_row{idx}:{key or '(missing program_key)'}"
        if key == "":
            raise ValidationError(f"[{row_id}] program_key is blank.")
        if key in seen:
            raise ValidationError(f"[{row_id}] duplicate program_key '{key}'.")
        seen.add(key)

        program: Dict[str, Any] = {
            "program_key": key,
            "program_name": (row.get("program_name") or "").strip(),
            "issuer": (row.get("issuer") or "").strip(),
            "source_url": validate_url_https(row.get("source_url") or "", "source_url", row_id),
            "requires_activation": parse_bool_true_false(
                row.get("requires_activation") or "", "requires_activation", row_id
            ),
            "cap_amount": parse_optional_int(row.get("cap_amount") or ""),
            "cap_period": (row.get("cap_period") or "").strip().lower() or None,
            "base_rate": int(parse_number(row.get("base_rate") or "", "base_rate", row_id)),
            "bonus_rate": int(parse_number(row.get("bonus_rate") or "", "bonus_rate", row_id)),
            "notes": (row.get("notes") or "").strip(),
            "status": (row.get("status") or "").strip().lower(),
            "last_verified": (row.get("last_verified") or "").strip(),
        }

        if program["program_name"] == "":
            raise ValidationError(f"[{row_id}] program_name is blank.")
        if program["issuer"] == "":
            raise ValidationError(f"[{row_id}] issuer is blank.")
        if program["status"] not in {"verified", "draft", "deprecated", ""}:
            raise ValidationError(f"[{row_id}] status='{program['status']}' invalid.")

        # Normalize empties out for stability
        if program.get("cap_amount") is None:
            program.pop("cap_amount", None)
        if program.get("cap_period") is None:
            program.pop("cap_period", None)
        if program.get("last_verified") == "":
            program.pop("last_verified", None)
        if program.get("notes") == "":
            program.pop("notes", None)

        out.append(program)

    out.sort(key=lambda x: x["program_key"])
    return out


def parse_program_quarters(csv_text: str) -> List[Dict[str, Any]]:
    reader = csv.DictReader(StringIO(csv_text))
    _validate_required_headers(reader, PROGRAM_QUARTERS_REQUIRED_HEADERS, "program_quarters")

    out: List[Dict[str, Any]] = []

    for idx, row in enumerate(reader, start=2):
        key = (row.get("program_key") or "").strip()
        row_id = f"program_quarters_row{idx}:{key or '(missing program_key)'}"
        if key == "":
            raise ValidationError(f"[{row_id}] program_key is blank.")

        start_date = validate_date_yyyy_mm_dd(row.get("start_date") or "", "start_date", row_id)
        end_date = validate_date_yyyy_mm_dd(row.get("end_date") or "", "end_date", row_id)
        if end_date < start_date:
            raise ValidationError(f"[{row_id}] end_date {end_date} is before start_date {start_date}.")

        category = (row.get("category") or "").strip().lower()
        if category == "":
            raise ValidationError(f"[{row_id}] category is blank.")

        status = (row.get("status") or "").strip().lower()
        if status not in {"verified", "draft", "deprecated", ""}:
            raise ValidationError(f"[{row_id}] status='{status}' invalid.")

        entry: Dict[str, Any] = {
            "program_key": key,
            "start_date": start_date,
            "end_date": end_date,
            "category": category,
            "status": status,
        }

        last_verified = (row.get("last_verified") or "").strip()
        if last_verified:
            entry["last_verified"] = last_verified

        out.append(entry)

    out.sort(key=lambda x: (x["program_key"], x["start_date"], x["category"]))
    return out


def build_programs_json(programs: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "programs": programs}


def build_program_quarters_json(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "program_quarters": entries}


def write_json(path: str, data: Dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def compute_sha256_files(paths: List[str]) -> str:
    h = hashlib.sha256()
    for p in paths:
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        h.update(b"\n")  # separator
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()

    # Back-compat alias
    ap.add_argument("--csv-url", help="(Legacy) Published CSV URL for Canonical_Cards (alias for --cards-csv-url)")

    ap.add_argument("--cards-csv-url", help="Published CSV URL for Canonical_Cards")
    ap.add_argument("--programs-csv-url", help="Published CSV URL for programs tab")
    ap.add_argument("--program-quarters-csv-url", help="Published CSV URL for program_quarters tab")
    ap.add_argument("--out-dir", default=".", help="Output directory (default: .)")
    args = ap.parse_args()

    cards_csv_url = args.cards_csv_url or args.csv_url
    if not cards_csv_url:
        raise ValidationError("You must pass --cards-csv-url (or legacy --csv-url).")

    out_dir = args.out_dir.rstrip("/")
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    cards_json_path = f"{out_dir}/cards.json"
    programs_json_path = f"{out_dir}/programs.json"
    program_quarters_json_path = f"{out_dir}/program_quarters.json"
    cards_version_path = f"{out_dir}/cards_version.json"

    # 1) Cards
    cards_csv_text = fetch_csv_text(cards_csv_url)
    cards = parse_cards(cards_csv_text)
    cards_json = build_cards_json(cards)
    write_json(cards_json_path, cards_json)

    programs_count = 0
    program_quarters_count = 0

    bundle_paths = [cards_json_path]

    # 2) Programs (optional but recommended)
    if args.programs_csv_url:
        programs_csv_text = fetch_csv_text(args.programs_csv_url)
        programs = parse_programs(programs_csv_text)
        programs_json = build_programs_json(programs)
        write_json(programs_json_path, programs_json)
        programs_count = len(programs)
        bundle_paths.append(programs_json_path)

    # 3) Program quarters (optional but recommended)
    if args.program_quarters_csv_url:
        pq_csv_text = fetch_csv_text(args.program_quarters_csv_url)
        pq_entries = parse_program_quarters(pq_csv_text)
        pq_json = build_program_quarters_json(pq_entries)
        write_json(program_quarters_json_path, pq_json)
        program_quarters_count = len(pq_entries)
        bundle_paths.append(program_quarters_json_path)

    # 4) Bundle version (cards + optional programs + optional program_quarters)
    digest = compute_sha256_files(bundle_paths)
    version = f"sha256:{digest[:12]}"

    existing = _read_json_if_exists(cards_version_path)
    existing_version = existing.get("version") if isinstance(existing, dict) else None

    if existing_version == version:
        print(f"OK: wrote {cards_json_path}")
        if args.programs_csv_url:
            print(f"OK: wrote {programs_json_path}")
        if args.program_quarters_csv_url:
            print(f"OK: wrote {program_quarters_json_path}")
        print(f"OK: cards_version.json unchanged (version={version})")
        print(
            f"version={version} cards_count={len(cards)} programs_count={programs_count} program_quarters_count={program_quarters_count}"
        )
        return 0

    cards_version = {
        "schema_version": SCHEMA_VERSION,
        "version": version,
        "generated_at": _utc_now_z(),
        "cards_count": len(cards),
        "programs_count": programs_count,
        "program_quarters_count": program_quarters_count,
        "bundle_files": [Path(p).name for p in bundle_paths],
    }
    write_json(cards_version_path, cards_version)

    print(f"OK: wrote {cards_json_path}")
    if args.programs_csv_url:
        print(f"OK: wrote {programs_json_path}")
    if args.program_quarters_csv_url:
        print(f"OK: wrote {program_quarters_json_path}")
    print(f"OK: wrote {cards_version_path}")
    print(
        f"version={version} cards_count={len(cards)} programs_count={programs_count} program_quarters_count={program_quarters_count}"
    )
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
