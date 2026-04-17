"""Reporters Without Borders (RSF) — World Press Freedom Index.

Datenquelle: Reporters sans frontières / Reporters Without Borders, jährlicher
Index der Pressefreiheit in 180 Ländern (seit 2013, aktuelle Methodik seit 2022).

Datenformat: CSV, direkt von RSF, pro Jahr eine Datei.
URL: https://rsf.org/sites/default/files/import_classement/{YYYY}.csv

Skala: 0–100 Punkte (höher = freiere Presse).
Kategorien: >85 Gut | 70–85 Zufriedenstellend | 55–70 Problematisch |
40–55 Schwierig | <40 Sehr ernst.

Fünf Sub-Indikatoren: Political Context, Economic Context, Legal Context,
Social Context, Safety.

Lizenz: RSF publiziert den Index als Open Data; Attribution an "Reporters
Without Borders / RSF" erforderlich.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren RSF-Scores, keine eigene Bewertung der Pressefreiheit.
- Caveat zur Methodik (Mix aus Fragebogen + Sicherheitsvorfälle) ist Pflicht.
"""

import csv
import io
import logging
import time
from datetime import datetime

import httpx

logger = logging.getLogger("evidora")

RSF_CSV_URL_TEMPLATE = "https://rsf.org/sites/default/files/import_classement/{year}.csv"
# RSF blockt generische User-Agents — wir setzen einen Browser-UA
RSF_USER_AGENT = "Mozilla/5.0 (compatible; Evidora-FactCheck/1.0)"

RSF_CACHE_TTL = 86400  # 24h

# Cache structure: {iso3: {year, score, rank, political, economic, legal, social, safety, country_en}}
_rsf_cache: dict | None = None
_rsf_cache_time: float = 0.0
_rsf_data_year: int | None = None

# Trigger-Keywords (DE + EN)
RSF_KEYWORDS = [
    "pressefreiheit", "press freedom",
    "medienfreiheit", "media freedom",
    "pressefreiheitsindex", "press freedom index",
    "reporter ohne grenzen", "reporters without borders", "reporters sans frontieres",
    "rsf",
    "journalismus", "journalism",
    "journalist:innen", "journalists", "journalisten",
    "medien", "media",
    "zensur", "censorship",
    "medienzensur",
    "pressezensur",
    "pressefreiheit eingeschränkt",
    "medienvielfalt", "media pluralism",
]

# RSF verwendet in der CSV ISO3-Codes (z.B. FIN, EST, AUT, NLD)
COUNTRY_MAP = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "schweiz": "CHE", "switzerland": "CHE",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czech republic": "CZE", "czechia": "CZE",
    "ungarn": "HUN", "hungary": "HUN",
    "rumänien": "ROU", "romania": "ROU",
    "bulgarien": "BGR", "bulgaria": "BGR",
    "kroatien": "HRV", "croatia": "HRV",
    "slowenien": "SVN", "slovenia": "SVN",
    "slowakei": "SVK", "slovakia": "SVK",
    "dänemark": "DNK", "denmark": "DNK",
    "schweden": "SWE", "sweden": "SWE",
    "norwegen": "NOR", "norway": "NOR",
    "finnland": "FIN", "finland": "FIN",
    "portugal": "PRT",
    "griechenland": "GRC", "greece": "GRC",
    "irland": "IRL", "ireland": "IRL",
    "luxemburg": "LUX", "luxembourg": "LUX",
    "estland": "EST", "estonia": "EST",
    "lettland": "LVA", "latvia": "LVA",
    "litauen": "LTU", "lithuania": "LTU",
    "vereinigtes königreich": "GBR", "united kingdom": "GBR", "uk": "GBR", "großbritannien": "GBR",
    "türkei": "TUR", "turkey": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "belarus": "BLR", "weißrussland": "BLR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "südkorea": "KOR", "south korea": "KOR",
    "nordkorea": "PRK", "north korea": "PRK",
    "iran": "IRN",
    "israel": "ISR",
    "saudi-arabien": "SAU", "saudi arabia": "SAU",
    "ägypten": "EGY", "egypt": "EGY",
}


def _parse_eu_decimal(value: str | None) -> float | None:
    """Parse European decimal format (comma as separator)."""
    if not value:
        return None
    s = value.strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


async def fetch_rsf(client: httpx.AsyncClient | None = None) -> dict:
    """Download and parse the RSF Press Freedom Index CSV.

    Tries the current year first, falls back to previous years.
    """
    global _rsf_cache, _rsf_cache_time, _rsf_data_year

    now = time.time()
    if _rsf_cache is not None and (now - _rsf_cache_time) < RSF_CACHE_TTL:
        return _rsf_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(
            timeout=30.0, headers={"User-Agent": RSF_USER_AGENT}
        )
        close_client = True

    current_year = datetime.now().year
    data: dict = {}
    year_loaded: int | None = None

    try:
        # Versuche aktuelles Jahr → dann bis zu 2 Jahre zurück
        for candidate_year in [current_year, current_year - 1, current_year - 2]:
            url = RSF_CSV_URL_TEMPLATE.format(year=candidate_year)
            try:
                resp = await client.get(url, headers={"User-Agent": RSF_USER_AGENT})
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                # BOM-aware decode
                text = resp.content.decode("utf-8-sig", errors="replace")
                reader = csv.DictReader(io.StringIO(text), delimiter=";")

                # Spalten können je Jahr "Score 2025", "Score 2024" etc. heißen
                score_col = next(
                    (c for c in (reader.fieldnames or []) if c.startswith("Score ")),
                    None,
                )
                if not score_col:
                    logger.warning(f"RSF {candidate_year}: No 'Score YYYY' column found")
                    continue

                for row in reader:
                    iso = (row.get("ISO") or "").strip().upper()
                    if not iso:
                        continue
                    entry = {
                        "year": candidate_year,
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
                    data[iso] = entry

                year_loaded = candidate_year
                logger.info(f"RSF {candidate_year}: {len(data)} countries cached")
                break
            except Exception as e:
                logger.warning(f"RSF {candidate_year}: fetch failed: {e}")
                continue

        _rsf_cache = data
        _rsf_cache_time = now
        _rsf_data_year = year_loaded
        if not data:
            logger.warning("RSF: no data loaded for any candidate year")
        return data
    finally:
        if close_client:
            await client.aclose()


def _find_countries(analysis: dict, max_n: int = 3) -> list[str]:
    """Extract ISO3 country codes from claim."""
    ner_countries = analysis.get("ner_entities", {}).get("countries", [])
    claim = analysis.get("claim", "")
    search_terms = ner_countries + [claim]

    found: list[str] = []
    seen: set[str] = set()
    for term in search_terms:
        term_lower = term.lower()
        for name, code in COUNTRY_MAP.items():
            if name in term_lower and code not in seen:
                found.append(code)
                seen.add(code)
                if len(found) >= max_n:
                    return found
    return found


def _claim_mentions_rsf(claim: str) -> bool:
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in RSF_KEYWORDS)


def _category_label(score: float | None) -> str:
    if score is None:
        return "keine Daten"
    if score > 85:
        return "Gut"
    if score >= 70:
        return "Zufriedenstellend"
    if score >= 55:
        return "Problematisch"
    if score >= 40:
        return "Schwierig"
    return "Sehr ernst"


async def search_rsf(analysis: dict) -> dict:
    """Search RSF cache for press freedom data."""
    if not _claim_mentions_rsf(analysis.get("claim", "")):
        return {"source": "Reporter ohne Grenzen (RSF)", "type": "official_data", "results": []}

    data = await fetch_rsf()
    if not data:
        return {"source": "Reporter ohne Grenzen (RSF)", "type": "official_data", "results": []}

    countries = _find_countries(analysis)
    if not countries:
        countries = ["AUT", "DEU"]

    results: list[dict] = []
    for code in countries:
        entry = data.get(code)
        if not entry:
            continue
        score = entry.get("score")
        rank = entry.get("rank")
        year = entry.get("year")
        entity = entry.get("country_en") or code
        category = _category_label(score)

        # Vorjahresvergleich
        trend_note = ""
        score_prev = entry.get("score_prev")
        if score is not None and score_prev is not None:
            delta = score - score_prev
            if abs(delta) >= 0.5:
                arrow = "↑" if delta > 0 else "↓"
                trend_note = f" | Vorjahr: {arrow} ({delta:+.1f})"

        # Sub-Indikatoren (wenn verfügbar)
        sub_parts = []
        for key, label in [
            ("political", "Politisch"),
            ("economic", "Wirtschaft"),
            ("legal", "Recht"),
            ("social", "Sozial"),
            ("safety", "Sicherheit"),
        ]:
            v = entry.get(key)
            if v is not None:
                sub_parts.append(f"{label}: {v:.1f}")
        sub_line = " | ".join(sub_parts) if sub_parts else ""

        rank_text = f"Rang {rank}/180" if rank else ""
        score_text = f"{score:.1f}/100" if score is not None else "–"

        name = f"RSF Pressefreiheit {entity} ({year}): {score_text} — {category}"
        if rank_text:
            name += f" ({rank_text})"
        name += trend_note

        results.append({
            "indicator_name": name,
            "indicator": "rsf_press_freedom_score",
            "country": code,
            "country_name": entity,
            "year": str(year) if year else "",
            "value": score,
            "display_value": score_text,
            "url": "https://rsf.org/en/index",
            "description": sub_line,
        })

    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: RSF-Index kombiniert Umfrage und Vorfallsdaten",
            "indicator": "context",
            "country": "",
            "country_name": "",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://rsf.org/en/methodology-used-compiling-world-press-freedom-index-2024",
            "description": (
                "Der RSF World Press Freedom Index wird jährlich von Reporters Without Borders "
                "publiziert. Skala: 0 (sehr ernst) bis 100 (gut). Kategorien: >85 Gut, "
                "70–85 Zufriedenstellend, 55–70 Problematisch, 40–55 Schwierig, <40 Sehr ernst. "
                "Der Gesamtscore setzt sich aus fünf Sub-Indikatoren zusammen: "
                "Political Context, Economic Context, Legal Context, Social Context, Safety. "
                "Einschränkungen: "
                "(1) Zwei Methoden kombiniert — ein quantitativer Fragebogen an Medienexpert:innen "
                "sowie eine Zählung dokumentierter Übergriffe/Tötungen an Journalist:innen. "
                "(2) Methodenbruch 2022 — aktuelle Methodik ist mit der vor 2022 nur bedingt "
                "vergleichbar (neue Sub-Indikatoren, größerer Expertenkreis). "
                "(3) Nur öffentliche Medien — Social Media, Plattform-Moderation und Desinformation "
                "sind nur teilweise erfasst. "
                "(4) Länderfokus, nicht Regionen — innerhalb großer Länder kann die Pressefreiheit "
                "stark variieren; der Index aggregiert auf Staatsebene."
            ),
        })

    logger.info(f"RSF: {len(results) - (1 if results else 0)} country results, countries={countries}")
    return {"source": "Reporter ohne Grenzen (RSF)", "type": "official_data", "results": results}
