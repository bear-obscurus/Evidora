"""Standalone-Refresh-Tool für CORDIS Slim-Cache.

Lädt cordis-HORIZONprojects-json.zip + cordis-h2020projects-json.zip von
cordis.europa.eu, slim Felder, schreibt /app/data/cordis_projects_slim.json.

Nutzung — als EIGENER Einmal-Container, nicht im servierenden:
  /opt/Evidora/website/run_evidora_tool.sh refresh_cordis.py --force

Der Wrapper startet seit 12.10.2026 fuer JEDES Werkzeug einen eigenen
Einmal-Container (`docker compose --profile wartung run --rm wartung …`).

Bis 2026-10-11 lief das als `docker compose exec backend python3 ...`, also
im Prozessraum des Dienstes. Der haelt ~2,5 GB von 3 GB Limit, der Refresh
brauchte gemessen 1.363 MiB — drei der fuenf Container-Neustarts im Oktober
fielen auf die Startminute eines solchen Wartungs-Tools, und der Lauf vom
01.10. starb mit dem Container (leeres Logfile, nur der ALERT). Seither:
eigener Container mit eigenem Limit (224m/512m Swap), und der Refresh
selbst ist sparsam (182 MiB statt 1.363, Ergebnis byteweise identisch).

Optionen:
  --force   Erzwingt Refresh auch wenn Cache <90 Tage alt ist. Der
            Compose-Dienst setzt das bereits.

Cron (quartalsweise, 1. des Quartals 03:00):
  siehe `crontab -l` — ruft den Compose-Dienst, nicht mehr run_evidora_tool.sh
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

#: Welche Exit-Codes einen BEFUND bedeuten — also: das Werkzeug ist
#: gelaufen, hat etwas gefunden und den ntfy-Push SELBST geschickt. Der
#: Cron darf dann keinen zweiten schicken. Alles andere ausser 0 heisst
#: "kam nicht zum Laufen", und DAS muss der Cron pushen, weil das
#: Werkzeug es nicht mehr kann.
#:
#: Maschinenlesbar und nicht nur im Docstring, weil ein Gate es prueft:
#: tests/test_cron_aufrufe.py las bis zum 18.10.2026 eine von Hand
#: gefuehrte Liste im Test selbst. Darin fehlte stichtag_check.py — und
#: genau dessen Cron-Zeile warf sechs Tage lang einen OOM-Kill (exit 137)
#: mit einem Befund zusammen. Eine Liste, die man pflegen muss, vergisst
#: man.
BEFUND_EXITS: tuple[int, ...] = ()

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
