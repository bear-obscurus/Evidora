#!/usr/bin/env python3
"""Data-Freshness-Check für die Static-First-Quellen.

Prüft das `fetched_at_iso` Feld in jeder data/*.json und warnt bei
> 90 Tagen Alter. Cron-tauglich, Output ist menschen-lesbar.

Hintergrund: Statt für jede Static-First-Quelle einen vollen Live-API-
Pfad zu bauen (was pro Quelle 4–8 h kostet), erinnert dieser Job
einmal pro Woche an manuellen Refresh-Bedarf. Die meisten Static-
Quellen sind ohnehin nur 1–4× pro Jahr aktualisiert (Eurostat-Crime,
RKI-TB, OECD Health, OeNB-Wohnindex). Eine wöchentliche Cron-Mahnung
ist genug.

Aufruf:
  python3 tools/data_freshness_check.py [--max-age-days N] [--strict]

Cron (auf prod):
  0 4 * * 1   cd /opt/Evidora/website/backend && \
              python3 tools/data_freshness_check.py --max-age-days 120 \
              >> /var/log/evidora_data_freshness.log 2>&1
"""
import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _parse_iso_date(s: str) -> date | None:
    if not s:
        return None
    s = s.strip()
    # akzeptiere "2026-04-29" oder "2026-04-29T..."
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None


def _scan_json(path: Path) -> tuple[date | None, str]:
    """Returns (fetched_at_date, source_label) — both possibly None/empty."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return (None, f"<read error: {e}>")
    if not isinstance(data, dict):
        return (None, "<not a dict — list-style data>")
    fetched_at = _parse_iso_date(data.get("fetched_at_iso", ""))
    label = data.get("source_label", "")
    return (fetched_at, label)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age-days", type=int, default=120,
        help="Schwellwert in Tagen (default: 120)")
    parser.add_argument("--strict", action="store_true",
        help="Exit code 1 wenn Schwellwert überschritten")
    args = parser.parse_args()

    if not DATA_DIR.exists():
        print(f"ERROR: data dir not found: {DATA_DIR}", file=sys.stderr)
        sys.exit(2)

    today = date.today()
    rows = []
    for path in sorted(DATA_DIR.glob("*.json")):
        fetched_at, label = _scan_json(path)
        if fetched_at is None:
            rows.append((path.name, None, label, "—"))
            continue
        age = (today - fetched_at).days
        rows.append((path.name, fetched_at, label, age))

    print(f"=== Evidora Data-Freshness-Check ({today.isoformat()}, max-age {args.max_age_days} d) ===")
    print()
    fmt = "  {:32s}  {:12s}  {:>6s}  {:32s}"
    print(fmt.format("file", "fetched_at", "age", "source"))
    print(fmt.format("-" * 32, "-" * 12, "-" * 6, "-" * 32))
    stale_files = []
    for name, fetched_at, label, age in rows:
        fa = fetched_at.isoformat() if fetched_at else "—"
        age_s = f"{age}d" if isinstance(age, int) else "—"
        marker = "⚠" if isinstance(age, int) and age > args.max_age_days else " "
        print(f"  {marker} " + fmt.format(name, fa, age_s, (label or "")[:30])[2:])
        if isinstance(age, int) and age > args.max_age_days:
            stale_files.append((name, age))

    print()
    if stale_files:
        print(f"⚠ {len(stale_files)} files stale (> {args.max_age_days} d):")
        for name, age in stale_files:
            print(f"  - {name}: {age} d alt")
        if args.strict:
            sys.exit(1)
    else:
        print(f"OK — alle {len(rows)} files sind frisch (< {args.max_age_days} d).")


if __name__ == "__main__":
    main()
