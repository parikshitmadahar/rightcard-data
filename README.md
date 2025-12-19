# rightcard-data

## Data update workflow (maintainers)

This repository publishes `cards.json` and `cards_version.json` via GitHub Pages.
The RightCard iOS app checks `cards_version.json` and downloads new data only
when the version changes.

The system is designed to be:
- deterministic (no noise commits)
- auditable (sheet → JSON)
- safe (no secrets in git)
- low-maintenance (one command)

---

## Source of truth

- Canonical data lives in a **private Google Sheet**
- The sheet is published as **CSV (read-only)**
- The published CSV URL is **never committed to this repo**

---

## Local maintainer setup (one-time)

1. Clone this repository
2. Create a local `.env` file in the repo root (gitignored) with:

