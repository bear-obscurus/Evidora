#!/usr/bin/env python3
"""Refresh data/rsf.json from the RSF World Press Freedom Index CSV.

Run once a year (RSF publishes the index annually, typically in early
May). Reads the current-year CSV from rsf.org, falls back up to two
years if the new file isn't published yet, and writes the static-first
pack used by ``services/rsf.py``.

Usage:
  python3 tools/refresh_rsf.py

Why this is a manual one-shot tool, not a live API call:
  RSF blocks generic User-Agents on the public site. Live polling from
  the running pipeline used to set a crawler-style UA to bypass that —
  which was a TOS-grey-zone. Since 2026-05-01 we instead fetch once a
  year (TDM-Exception territory: a single, attributed download of an
  annually-published open-data file) and cache the result statically.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import sys
from datetime import date, datetime

import httpx

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("refresh_rsf")

CSV_URL_TEMPLATE = "https://rsf.org/sites/default/files/import_classement/{year}.csv"
USER_AGENT = "Evidora/1.0 (+https://evidora.eu; mailto:Evidora@proton.me; annual one-shot fetch)"

OUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "rsf.json",
)


def _parse_eu_decimal(value):
    if not value:
        return None
    s = value.strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(value):
    if not value:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


async def fetch_csv() -> tuple[dict, int]:
    current_year = datetime.now().year
    async with httpx.AsyncClient(timeout=30.0,
                                 headers={"User-Agent": USER_AGENT}) as client:
        for year in (current_year, current_year - 1, current_year - 2):
            url = CSV_URL_TEMPLATE.format(year=year)
            try:
                resp = await client.get(url)
                if resp.status_code == 404:
                    logger.info(f"{year}: 404, trying earlier year")
                    continue
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"{year}: fetch failed: {e}")
                continue

            text = resp.content.decode("utf-8-sig", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            score_col = next(
                (c for c in (reader.fieldnames or []) if c.startswith("Score ")),
                None,
            )
            if not score_col:
                logger.warning(f"{year}: no 'Score YYYY' column")
                continue

            data: dict = {}
            for row in reader:
                iso = (row.get("ISO") or "").strip().upper()
                if not iso:
                    continue
                data[iso] = {
                    "year": year,
                    "score": _parse_eu_decimal(row.get(score_col)),
                    "rank": _parse_int(row.get("Rank")),
                    "political": _parse_eu_decimal(row.get("Political Context")),
                    "economic": _parse_eu_decimal(row.get("Economic Context")),
                    "legal": _parse_eu_decimal(row.get("Legal Context")),
                    "social": _parse_eu_decimal(row.get("Social Context")),
                    "safety": _parse_eu_decimal(row.get("Safety")),
                    "country_en": (row.get("Country_EN") or "").strip(),
                    "score_prev": _parse_eu_decimal(row.get("Score N-1")),
                    "rank_prev": _parse_int(row.get("Rank N-1")),
                }
            logger.info(f"{year}: {len(data)} countries fetched")
            return data, year

    raise RuntimeError("RSF: no data could be fetched for the last 3 years")


def build_pack(by_country: dict, year: int) -> dict:
    return {
        "schema_version": 1,
        "fetched_at_iso": date.today().isoformat(),
        "source_label": (
            f"Reporter ohne Grenzen (RSF) — World Press Freedom Index {year}"
        ),
        "year": year,
        "facts": [
            {
                "id": f"rsf_index_{year}",
                "topic": "pressefreiheit_index",
                "year": year,
                "headline": (
                    f"RSF World Press Freedom Index {year} — 180 Länder, "
                    "Skala 0–100"
                ),
                "data": {
                    "year": year,
                    "scale_explanation": (
                        "Skala 0 (sehr ernst) bis 100 (gut). >85 Gut, "
                        "70–85 Zufriedenstellend, 55–70 Problematisch, "
                        "40–55 Schwierig, <40 Sehr ernst."
                    ),
                    "sub_indicators": [
                        "Political Context", "Economic Context",
                        "Legal Context", "Social Context", "Safety",
                    ],
                    "context": (
                        "Der RSF World Press Freedom Index wird jährlich von "
                        "Reporters Without Borders publiziert. Der Gesamtscore "
                        "setzt sich aus fünf Sub-Indikatoren zusammen: "
                        "Political, Economic, Legal, Social Context und "
                        "Safety. Methodenbruch 2022 — aktuelle Methodik ist "
                        "mit der vor 2022 nur bedingt vergleichbar. "
                        "Quantitativer Fragebogen an Medienexpert:innen + "
                        "dokumentierte Übergriffe/Tötungen an Journalist:innen "
                        "kombiniert."
                    ),
                    "by_country": by_country,
                },
                "context_notes": [
                    "Methodenbruch 2022 — neue Sub-Indikatoren, größerer "
                    "Expertenkreis. Vergleich vor/nach 2022 nur bedingt "
                    "möglich.",
                    "Index aggregiert auf Staatsebene; regionale Unterschiede "
                    "(z.B. Ungarn-Budapest vs. Land) gehen nicht ein.",
                    "Social Media + Plattform-Moderation + Desinformation nur "
                    "teilweise erfasst — Index fokussiert klassische Medien.",
                ],
                "claim_phrasings_handled": [
                    "Pressefreiheit in Österreich",
                    "Wie steht Deutschland beim RSF-Index?",
                    "Medienfreiheit in Ungarn",
                    "Pressefreiheitsindex Türkei",
                ],
                "trigger_keywords": [
                    "pressefreiheit", "press freedom",
                    "medienfreiheit", "media freedom",
                    "pressefreiheitsindex", "press freedom index",
                    "reporter ohne grenzen", "reporters without borders",
                    "reporters sans frontieres", "rsf",
                    "journalismus", "journalism",
                    "journalist:innen", "journalists", "journalisten",
                    "zensur", "censorship",
                    "medienzensur", "pressezensur",
                    "medienvielfalt", "media pluralism",
                ],
                "source_label": f"RSF World Press Freedom Index {year}",
                "source_url": "https://rsf.org/en/index",
                "secondary_url": (
                    "https://rsf.org/en/methodology-used-compiling-"
                    "world-press-freedom-index-2024"
                ),
            }
        ],
    }


async def main():
    by_country, year = await fetch_csv()
    if not by_country:
        sys.exit("no data")
    pack = build_pack(by_country, year)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(pack, f, ensure_ascii=False, indent=2)
    print(f"Wrote {OUT_PATH}: {len(by_country)} countries, year {year}")


if __name__ == "__main__":
    asyncio.run(main())
