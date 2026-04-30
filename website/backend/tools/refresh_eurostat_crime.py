#!/usr/bin/env python3
"""Refresh-Skript für eu_crime.json — holt aktuelle Eurostat-Werte
für den homicide_eu_compare-Topic via Live-API.

Showcase-Implementation für den Live-API-Refresh-Pfad. Pattern kann
auf weitere Eurostat-basierte Topics ausgerollt werden.

Aufruf:
  python3 tools/refresh_eurostat_crime.py [--dry-run]

Cron (alle 3 Monate, 1. Tag des Monats 02:00 UTC):
  0 2 1 */3 * /opt/Evidora/website/run_evidora_tool.sh \\
      refresh_eurostat_crime.py >> /home/burrito/evidora-logs/eurostat_refresh.log 2>&1
"""
import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, "/app")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services._eurostat_live import fetch_eurostat_dataset, filter_rows


JSON_PATH = Path(__file__).resolve().parent.parent / "data" / "eu_crime.json"


async def fetch_homicide_rates() -> dict:
    """Hole intentional-homicide-Inzidenz pro 100k Einwohner für AT/DE/EU.

    Eurostat-Dataset crim_hom_soff (homicide offences) — wir filtern strikt:
      - leg_stat=OFF (Anzahl Tatfälle, NICHT Verurteilungen)
      - unit=P_HTHAB (per 100k inhabitants)
      - sex=T (Total) — falls nicht verfügbar, alle Geschlechter aggregiert
    """
    rows = await fetch_eurostat_dataset(
        "crim_hom_soff",
        geo=["AT", "DE", "EU27_2020", "FR", "IT", "ES", "PT",
             "LV", "LT", "EE"],
        time=[2022, 2023, 2024],
        unit=["P_HTHAB"],
    )
    if not rows:
        return {}
    # WHO/UNODC-Standard für 'Mordrate pro 100k':
    #   leg_stat=PER_VICT (Opfer), sex=T (Total),
    #   iccs=ICCS0101 (intentional homicide laut International Classification
    #                  of Crime for Statistical Purposes)
    out: dict[str, dict[str, float]] = {}
    for r in rows:
        if r.get("leg_stat") != "PER_VICT":
            continue
        if r.get("sex") != "T":
            continue
        if r.get("iccs") != "ICCS0101":
            continue
        geo = r.get("geo")
        time = str(r.get("time"))
        val = r.get("value")
        if val is None or geo is None:
            continue
        out.setdefault(geo, {})[time] = float(val)
    return out


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
        help="Nur Werte ausgeben, nicht ins JSON schreiben")
    args = parser.parse_args()

    print(f"=== Eurostat-Crime-Refresh {date.today().isoformat()} ===")
    rates = await fetch_homicide_rates()
    if not rates:
        print("ERROR: keine Daten von Eurostat erhalten")
        sys.exit(1)

    # Bevorzugt 2024, sonst 2023
    def _latest(rates_for_geo: dict[str, float]) -> tuple[str, float] | None:
        for year in ("2024", "2023", "2022"):
            if year in rates_for_geo:
                return (year, rates_for_geo[year])
        return None

    summary = {}
    for geo, by_year in sorted(rates.items()):
        latest = _latest(by_year)
        if latest:
            print(f"  {geo:8s} {latest[0]}: {latest[1]:.2f} pro 100k")
            summary[geo] = latest

    if args.dry_run:
        print("\n--dry-run gesetzt — kein Schreibvorgang")
        return

    # Schreibe in JSON: nur den homicide_eu_compare-Topic aktualisieren
    if not JSON_PATH.exists():
        print(f"ERROR: {JSON_PATH} nicht gefunden")
        sys.exit(1)
    with JSON_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    target = next((f for f in data.get("facts", [])
                   if f.get("topic") == "homicide_eu_compare"), None)
    if not target:
        print("ERROR: homicide_eu_compare topic nicht gefunden")
        sys.exit(1)

    d = target.setdefault("data", {})
    if "AT" in summary:
        d["homicide_at_per_100k"] = round(summary["AT"][1], 2)
    if "DE" in summary:
        d["homicide_de_per_100k"] = round(summary["DE"][1], 2)
    if "EU27_2020" in summary:
        d["homicide_eu_avg_per_100k"] = round(summary["EU27_2020"][1], 2)
    target["fetched_at_iso"] = date.today().isoformat()
    data["fetched_at_iso"] = date.today().isoformat()

    with JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\nWrote {JSON_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
