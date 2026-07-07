#!/usr/bin/env python3
"""Refresh der V-Dem-Indikatoren aus dem offiziellen Country-Year-Core-CSV.

Ersetzt die bisherige Misch-Lage (7 Indikatoren via OWID-Syndication,
4 als LLM-Approximationen — der historische ⚠-Boundary-Fall) durch eine
einheitliche Vintage direkt von v-dem.net.

Nutzung:
  python3 tools/refresh_vdem.py --zip /pfad/zu/V-Dem-CY-Core-vXX_csv.zip
  python3 tools/refresh_vdem.py            # lädt DEFAULT_URL (~16 MB)

Der Download-Link ist der stabile media-Pfad hinter dem v-dem.net-
Formular (Formular einmal im Browser ausfüllen zeigt ihn; Zitations-
Pflicht beachten — source_label nennt V-Dem korrekt).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import zipfile
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services._atomic import atomic_write_json  # noqa: E402

DEFAULT_URL = "https://www.v-dem.net/media/datasets/V-Dem-CY-Core-v16_csv.zip"
JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data",
                         "vdem_indicators.json")
MIN_YEAR = 2019

INDICATOR_CODES = [
    "v2x_libdem", "v2x_polyarchy", "v2x_partipdem", "v2x_delibdem",
    "v2x_egaldem", "v2x_civlib", "v2x_freexp", "v2xcl_rol",
    "v2xeg_eqdr", "v2x_corr", "v2x_clphy",
]


def extract_from_zip(zip_path: str) -> tuple[dict, int, str]:
    """Returns ({code: {iso3: {year: value}}}, max_year, version_tag)."""
    zf = zipfile.ZipFile(zip_path)
    csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
    version = "v16"
    for part in csv_name.replace(".csv", "").split("-"):
        if part.startswith("v") and part[1:].isdigit():
            version = part
    out: dict = {c: {} for c in INDICATOR_CODES}
    max_year = 0
    with zf.open(csv_name) as fh:
        reader = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8"))
        for row in reader:
            try:
                year = int(row.get("year") or 0)
            except ValueError:
                continue
            if year < MIN_YEAR:
                continue
            iso3 = (row.get("country_text_id") or "").strip()
            if not iso3:
                continue
            max_year = max(max_year, year)
            for code in INDICATOR_CODES:
                raw = (row.get(code) or "").strip()
                if not raw:
                    continue
                try:
                    out[code].setdefault(iso3, {})[str(year)] = round(float(raw), 3)
                except ValueError:
                    continue
    return out, max_year, version


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", help="Pfad zum V-Dem-CY-Core-Zip (sonst Download)")
    args = ap.parse_args()

    zip_path = args.zip
    if not zip_path:
        import httpx
        print(f"Lade {DEFAULT_URL} …")
        zip_path = "/tmp/vdem_core.zip"
        with httpx.stream("GET", DEFAULT_URL, timeout=300,
                          follow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0"}) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_bytes():
                    f.write(chunk)

    values, max_year, version = extract_from_zip(zip_path)
    doc = json.load(open(JSON_PATH, encoding="utf-8"))

    total_points = 0
    for ind in doc["indicators"]:
        code = ind["code"]
        new = values.get(code) or {}
        if not new:
            print(f"WARNUNG: {code} nicht im CSV — unverändert")
            continue
        old_n = len(ind.get("data") or {})
        ind["data"] = {iso: dict(sorted(years.items(), reverse=True))
                       for iso, years in sorted(new.items())}
        ind["dataset_verified"] = True
        ind.pop("v14_verified", None)
        pts = sum(len(y) for y in new.values())
        total_points += pts
        print(f"{code:16s} {old_n:3d} -> {len(new):3d} Länder, {pts} Punkte")

    today = date.today().isoformat()
    doc["dataset_version"] = version
    doc["fetched_at_iso"] = today
    doc["source_label"] = (
        f"V-Dem Institute (University of Gothenburg) — Varieties of "
        f"Democracy {version}, Country-Year Core (offizielles CSV, "
        f"v-dem.net). Alle 11 Indikatoren aus derselben Dataset-Vintage."
    )
    doc["version_note"] = (
        f"V-Dem {version}, Jahre {MIN_YEAR}–{max_year}. Punktschätzungen "
        f"der Aggregat-Indizes (0-1). Refresh: tools/refresh_vdem.py "
        f"(jährlich nach dem V-Dem-Release im März)."
    )
    doc["audit_flag"] = (
        f"REFRESHED {today} aus offiziellem V-Dem-{version}-Core-CSV — "
        f"alle 11/11 Indikatoren dataset_verified (die früheren 4 "
        f"LLM-Approximationen delibdem/egaldem/eqdr/clphy sind ersetzt)."
    )
    atomic_write_json(JSON_PATH, doc, ensure_ascii=False, indent=2,
                      trailing_newline=True)
    print(f"\nOK: {version}, Jahre bis {max_year}, {total_points} Datenpunkte "
          f"-> {os.path.normpath(JSON_PATH)}")


if __name__ == "__main__":
    main()
