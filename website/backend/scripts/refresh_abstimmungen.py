#!/usr/bin/env python3
"""Refresh the static Parlament Voting-Records snapshot.

Datenquelle: parlament.gv.at Open-Data Filter-API
(``Filter/api/filter/data/101``).  Pro Verhandlungsgegenstand liefert
die API zusätzlich zu Metadaten (Datum, DOKTYP, Betreff) auch das
Klub-Abstimmungsverhalten in den Feldern ``Dafür`` / ``Dagegen`` als
JSON-Array (z.B. ``["ÖVP","SPÖ","GRÜNE","NEOS"]``).

Wir filtern auf:
- NRBR = NR (Nationalrat — Bundesrat ist nicht abgedeckt in v1)
- DOKTYP IN (RV, A, BUA, BRA) — die vier Typen, in denen Voting-Daten
  systematisch vorhanden sind:
    * RV   = Regierungsvorlage
    * A    = Antrag (Initiativantrag)
    * BUA  = Bericht und Antrag aus Ausschuss
    * BRA  = Bericht
- Periode: seit GP XXVI (2017) — die letzten drei vollen
  Gesetzgebungsperioden plus die laufende.  Aelter ist seltener
  Faktencheck-relevant und blaeht den Snapshot auf.

Ausgabe-Schema (kompakt, pro Eintrag ~250 Bytes):

    {
      "datum": "18.10.2023",          # Tag der 3. Lesung im Plenum
      "betreff": "...",                # Kurzbeschreibung
      "doktyp": "RV",                  # Typ des Verhandlungsgegenstandes
      "gp": "XXVII",                   # roemische Periode
      "abstimmung_3l": 1,              # 1 = angenommen, 0 = abgelehnt
      "dafuer": ["OEVP","SPOe",...],   # Klubs die zugestimmt haben
      "dagegen": ["FPOe"],             # Klubs die abgelehnt haben
      "url": "https://...",            # Permalink zum Beschluss-PDF
      "abstimmungstext": null          # Optionaler Klartext-Kommentar
    }

Usage (from backend/):
    python3 scripts/refresh_abstimmungen.py
"""

import asyncio
import json
import os
import sys
import time

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND_ROOT = os.path.dirname(HERE)
DATA_PATH = os.path.join(BACKEND_ROOT, "data", "abstimmungen.json")

PARLAMENT_FILTER_URL = (
    "https://www.parlament.gv.at/Filter/api/filter/data/101"
    "?showAll=false&js=eval"
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Safari/605.1.15"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Gesetzgebungsperioden, die wir abdecken.  XXVIII ist laufend.
PERIODS = ["XXVI", "XXVII", "XXVIII"]
DOKTYPS = ["RV", "A", "BUA", "BRA"]


async def fetch_period(client: httpx.AsyncClient, gp: str) -> list[dict]:
    """Fetch all NR-Beschluesse with voting info for one GP."""
    body = {
        "NRBR": ["NR"],
        "DOKTYP": DOKTYPS,
        "GP_CODE": [gp],
    }
    r = await client.post(PARLAMENT_FILTER_URL, json=body)
    if r.status_code != 200:
        print(f"  GP {gp}: HTTP {r.status_code}", file=sys.stderr)
        return []
    payload = r.json()
    rows = payload.get("rows") or []
    header = [h.get("label") for h in (payload.get("header") or [])]

    # Header-Indizes auflösen
    def idx(name: str) -> int:
        try:
            return header.index(name)
        except ValueError:
            return -1

    i_datum = idx("Datum")
    i_betreff = idx("Betreff")
    i_doktyp = idx("DOKTYP")
    i_gp = idx("GP_CODE")
    i_abst = idx("Abstimmung 3. Lesung")
    i_dafuer = idx("Dafür")
    i_dagegen = idx("Dagegen")
    i_text = idx("Abstimmungstext")
    i_url = idx("HIS_URL")

    out: list[dict] = []
    for row in rows:
        # Nur Eintraege mit Voting-Info uebernehmen
        if not (row[i_abst] or row[i_dafuer] or row[i_dagegen]):
            continue

        def parse_clubs(cell):
            """Cell ist None, ein JSON-String oder bereits eine Liste."""
            if cell is None or cell == "":
                return []
            if isinstance(cell, list):
                return [str(c) for c in cell if c]
            if isinstance(cell, str):
                try:
                    parsed = json.loads(cell)
                    if isinstance(parsed, list):
                        return [str(c) for c in parsed if c]
                except json.JSONDecodeError:
                    pass
            return []

        dafuer = parse_clubs(row[i_dafuer])
        dagegen = parse_clubs(row[i_dagegen])
        if not dafuer and not dagegen:
            continue  # leere Voting-Cells -> uninteressant

        url_path = row[i_url] if i_url >= 0 else ""
        url = (
            f"https://www.parlament.gv.at{url_path}"
            if url_path and url_path.startswith("/")
            else url_path
        )

        out.append({
            "datum": row[i_datum],
            "betreff": row[i_betreff] or "",
            "doktyp": row[i_doktyp] or "",
            "gp": row[i_gp] or gp,
            "abstimmung_3l": row[i_abst],
            "dafuer": dafuer,
            "dagegen": dagegen,
            "abstimmungstext": (row[i_text] if i_text >= 0 else None) or None,
            "url": url,
        })
    return out


async def main() -> int:
    print(f"Refreshing Parlament Abstimmungen -> {DATA_PATH}")
    all_entries: list[dict] = []
    async with httpx.AsyncClient(timeout=60.0,
                                  headers=BROWSER_HEADERS) as client:
        for gp in PERIODS:
            entries = await fetch_period(client, gp)
            print(f"  GP {gp}: {len(entries)} voting records")
            all_entries.extend(entries)

    if not all_entries:
        print("ERROR: no entries fetched", file=sys.stderr)
        return 1

    # Sortieren: neueste zuerst
    def parse_date_key(d: str) -> tuple:
        # Datum kommt als "DD.MM.YYYY"
        try:
            day, month, year = d.split(".")
            return (int(year), int(month), int(day))
        except (ValueError, AttributeError):
            return (0, 0, 0)

    all_entries.sort(key=lambda e: parse_date_key(e.get("datum") or ""),
                     reverse=True)

    payload = {
        "entries": all_entries,
        "fetched_at_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "source_url": (
            "https://www.parlament.gv.at/recherchieren/open-data/"
            "daten-und-lizenz/beschluesse"
        ),
        "license": "CC BY 4.0 — Parlament Oesterreich",
        "note": (
            "Generated by scripts/refresh_abstimmungen.py.  Filter: NR-"
            "Beschluesse seit GP XXVI mit DOKTYP in (RV, A, BUA, BRA) "
            "und nicht-leeren Voting-Daten (Dafuer/Dagegen)."
        ),
    }
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\nWrote {len(all_entries)} entries to {DATA_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
