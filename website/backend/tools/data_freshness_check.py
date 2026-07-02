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

# Generierte Caches: kein fetched_at_iso, werden separat auf Gesundheit
# geprüft (Existenz/Größe/Alter) statt json-geladen — cordis ist ~110 MB.
# Format: name -> (min_bytes, max_age_days | None = Alter egal)
GENERATED_CACHES = {
    "cordis_projects_slim.json": (50_000_000, 100),   # Quartals-Cron + Puffer
    "claimreview_index.json": (1_000_000, None),      # beim Backend-Start neu gebaut
}


def check_generated_caches(data_dir: Path) -> list[str]:
    """Gesundheits-Check der generierten Cache-Dateien.

    Fängt stille Refresh-Fehlschläge (Lehrgeld 2026-07-02: der CORDIS-
    Quartals-Cron lief nach einer Upstream-Format-Umstellung unbemerkt
    auf 0 Records). Returns: Liste menschenlesbarer Probleme (leer = ok).
    """
    problems = []
    for name, (min_bytes, max_age_days) in GENERATED_CACHES.items():
        p = data_dir / name
        if not p.exists():
            problems.append(
                f"{name}: FEHLT (Refresh nie gelaufen oder fehlgeschlagen?)"
            )
            continue
        size = p.stat().st_size
        if size < min_bytes:
            problems.append(
                f"{name}: nur {size/1e6:.1f} MB (< {min_bytes/1e6:.0f} MB "
                f"Minimum) — Refresh lieferte vermutlich leere/kaputte Daten"
            )
        if max_age_days is not None:
            age_days = (datetime.now().timestamp() - p.stat().st_mtime) / 86400
            if age_days > max_age_days:
                problems.append(
                    f"{name}: {age_days:.0f} d alt (> {max_age_days} d) — "
                    f"Refresh-Cron läuft nicht mehr durch"
                )
    return problems


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
        help="Exit code 1 wenn Schwellwert überschritten oder ein "
             "generierter Cache fehlt/zu klein/zu alt ist")
    parser.add_argument("--alert-webhook", default=os.getenv("EVIDORA_ALERT_WEBHOOK", ""),
        help="Optional URL für Alert-POST (JSON) bei Problemen — "
             "gleiche Mechanik wie weekly_phrasing_check; Default aus "
             "env EVIDORA_ALERT_WEBHOOK")
    args = parser.parse_args()

    if not DATA_DIR.exists():
        print(f"ERROR: data dir not found: {DATA_DIR}", file=sys.stderr)
        sys.exit(2)

    today = date.today()
    rows = []
    for path in sorted(DATA_DIR.glob("*.json")):
        if path.name in GENERATED_CACHES:
            continue  # separat geprüft; cordis (~110 MB) nicht json-laden
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
    cache_problems = check_generated_caches(DATA_DIR)
    if cache_problems:
        print(f"ALERT — {len(cache_problems)} generierte Caches ungesund:")
        for pr in cache_problems:
            print(f"  - {pr}")
    else:
        print(f"OK — {len(GENERATED_CACHES)} generierte Caches gesund "
              f"(Existenz/Größe/Alter).")

    print()
    if stale_files:
        print(f"⚠ {len(stale_files)} files stale (> {args.max_age_days} d):")
        for name, age in stale_files:
            print(f"  - {name}: {age} d alt")
    else:
        print(f"OK — alle {len(rows)} files sind frisch (< {args.max_age_days} d).")

    if (stale_files or cache_problems) and args.alert_webhook:
        try:
            import httpx
            httpx.post(args.alert_webhook, json={
                "source": "evidora data_freshness_check",
                "date": today.isoformat(),
                "stale_files": [f"{n}: {a} d" for n, a in stale_files],
                "cache_problems": cache_problems,
            }, timeout=15)
            print(f"  alert webhook posted to {args.alert_webhook}")
        except Exception as e:
            print(f"  alert webhook failed: {e}")

    if (stale_files or cache_problems) and args.strict:
        sys.exit(1)


if __name__ == "__main__":
    main()
