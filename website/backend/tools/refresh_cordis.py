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

    from services.cordis import prefetch_cordis

    print("Starte CORDIS-Refresh (kann 30-60s dauern)…")
    t0 = time.time()
    # --force überspringt nur den Frische-Skip in prefetch_cordis. Der
    # alte Cache wird NICHT vorab gelöscht (Lehrgeld 2026-07-02: das
    # frühere pre-delete hätte bei fehlgeschlagenem Download einen guten
    # Cache zerstört; bei 0 Records schreibt prefetch_cordis nicht).
    n = asyncio.run(prefetch_cordis(force=args.force))
    elapsed = time.time() - t0
    print(f"\nFertig in {elapsed:.1f}s: {n} Projekte cached")
    if n <= 0:
        print("ALERT: 0 Records — CORDIS-Refresh fehlgeschlagen "
              "(Upstream-Format? Download?). Cache unverändert.")
        sys.exit(1)


if __name__ == "__main__":
    main()
