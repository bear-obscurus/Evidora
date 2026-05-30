"""Standalone-Refresh-Tool für CORDIS Slim-Cache.

Lädt cordis-HORIZONprojects-json.zip + cordis-h2020projects-json.zip von
cordis.europa.eu, slim Felder, schreibt /app/data/cordis_projects_slim.json.

Nutzung:
  docker compose exec backend python3 /app/tools/refresh_cordis.py [--force]

Optionen:
  --force   Erzwingt Refresh auch wenn Cache <90 Tage alt ist.

Cron (quartalsweise, 1. des Quartals 03:00):
  0 3 1 1,4,7,10 * /opt/Evidora/website/run_evidora_tool.sh refresh_cordis.py >> /home/burrito/evidora-logs/cordis_refresh.log 2>&1
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

# Backend-Modul-Import ermöglichen wenn Tool aus /app/tools/ ausgeführt
if "/app" not in sys.path:
    sys.path.insert(0, "/app")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="Erzwingt Refresh auch wenn Cache <90 Tage alt ist",
    )
    args = parser.parse_args()

    if args.force:
        from services.cordis import SLIM_CACHE_PATH
        if os.path.exists(SLIM_CACHE_PATH):
            os.remove(SLIM_CACHE_PATH)
            print(f"Cache {SLIM_CACHE_PATH} entfernt (--force).")

    from services.cordis import prefetch_cordis

    print("Starte CORDIS-Refresh (kann 30-60s dauern)…")
    t0 = time.time()
    n = asyncio.run(prefetch_cordis())
    elapsed = time.time() - t0
    print(f"\nFertig in {elapsed:.1f}s: {n} Projekte cached")
    if n <= 0:
        print("WARNUNG: 0 Records — Refresh fehlgeschlagen.")
        sys.exit(1)


if __name__ == "__main__":
    main()
