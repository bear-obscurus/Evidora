"""IRENA — International Renewable Energy Agency.

Datenquelle: IRENASTAT PxWeb-Query-API
  https://pxweb.irena.org/api/v1/en/IRENASTAT/

Lizenz: CC BY 4.0 (IRENA, Renewable Capacity Statistics)

Use-Case:
- "Österreich hat X MW Solar-PV-Kapazität"
- "Deutschland EE-Anteil an Stromerzeugung Y %"
- "Wasserkraft-Kapazität Schweiz / Norwegen ...""
- "Photovoltaik / Windkraft / Geothermie / Biomasse [Land]"
- "Renewable Capacity weltweit"

Politische Guardrails:
- Pure Statistik (installierte Kapazität in MW). KEINE Wertung der
  Klimapolitik, keine Forderungen, keine Bewertung von Energiewende-
  Tempo. Daten "as published by IRENA" — bei Diskrepanz zu nationalen
  Quellen (Energy-Charts/E-Control) Methodik-Hinweis ergaenzen.

Pattern:
- POST JSON-Query gegen die PxFile `Country_ELECCAP_*` (Kapazitaet)
  + `Country_ELECGEN_*` (Generation).
- Tech-Mapping: "solar pv" → Technology-Code 2, "wind onshore" → 5 usw.
- Country-Detection: ISO-3 (AUT, DEU, CHE, ...) per Stichwortliste.
- Cache 24 h pro (country, technology, table) — die PxFiles werden
  halbjaehrlich aktualisiert, also ist 24 h grosszuegig genug.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Config / Konstanten
# ---------------------------------------------------------------------------
BASE_URL = "https://pxweb.irena.org/api/v1/en/IRENASTAT"
CAPACITY_TABLE = (
    "Power Capacity and Generation/Country_ELECCAP_2026_H1_v-PX 1.px"
)

CACHE_TTL = 24 * 3600  # 24 h
TIMEOUT_S = 25.0

DATA_URL = "https://www.irena.org/Data/Downloads/IRENASTAT"
SOURCE_LABEL = "IRENA (International Renewable Energy Agency)"

# Cache: key=(table, country_iso3, tech_code) → (timestamp, list[(year, value_mw)])
_cache: dict[tuple[str, str, str], tuple[float, list[tuple[str, float]]]] = {}


# ---------------------------------------------------------------------------
# ISO-3 Country-Mapping
# ---------------------------------------------------------------------------
# Begrenzte Liste der wichtigsten Faktencheck-relevanten Laender. Bei
# Bedarf erweitern. Reihenfolge im Dict ist irrelevant — Match auf Term.
_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    # term-lowercase → (iso3, deutscher_name)
    "österreich": ("AUT", "Österreich"),
    "oesterreich": ("AUT", "Österreich"),
    "austria": ("AUT", "Österreich"),
    "deutschland": ("DEU", "Deutschland"),
    "germany": ("DEU", "Deutschland"),
    "schweiz": ("CHE", "Schweiz"),
    "switzerland": ("CHE", "Schweiz"),
    "frankreich": ("FRA", "Frankreich"),
    "france": ("FRA", "Frankreich"),
    "italien": ("ITA", "Italien"),
    "italy": ("ITA", "Italien"),
    "spanien": ("ESP", "Spanien"),
    "spain": ("ESP", "Spanien"),
    "niederlande": ("NLD", "Niederlande"),
    "netherlands": ("NLD", "Niederlande"),
    "polen": ("POL", "Polen"),
    "poland": ("POL", "Polen"),
    "ungarn": ("HUN", "Ungarn"),
    "hungary": ("HUN", "Ungarn"),
    "tschechien": ("CZE", "Tschechien"),
    "czechia": ("CZE", "Tschechien"),
    "slowakei": ("SVK", "Slowakei"),
    "slovakia": ("SVK", "Slowakei"),
    "slowenien": ("SVN", "Slowenien"),
    "slovenia": ("SVN", "Slowenien"),
    "norwegen": ("NOR", "Norwegen"),
    "norway": ("NOR", "Norwegen"),
    "schweden": ("SWE", "Schweden"),
    "sweden": ("SWE", "Schweden"),
    "dänemark": ("DNK", "Dänemark"),
    "daenemark": ("DNK", "Dänemark"),
    "denmark": ("DNK", "Dänemark"),
    "finnland": ("FIN", "Finnland"),
    "finland": ("FIN", "Finnland"),
    "großbritannien": ("GBR", "Vereinigtes Königreich"),
    "grossbritannien": ("GBR", "Vereinigtes Königreich"),
    "united kingdom": ("GBR", "Vereinigtes Königreich"),
    "vereinigtes königreich": ("GBR", "Vereinigtes Königreich"),
    "irland": ("IRL", "Irland"),
    "ireland": ("IRL", "Irland"),
    "portugal": ("PRT", "Portugal"),
    "griechenland": ("GRC", "Griechenland"),
    "greece": ("GRC", "Griechenland"),
    "rumänien": ("ROU", "Rumänien"),
    "romania": ("ROU", "Rumänien"),
    "bulgarien": ("BGR", "Bulgarien"),
    "bulgaria": ("BGR", "Bulgarien"),
    "estland": ("EST", "Estland"),
    "lettland": ("LVA", "Lettland"),
    "litauen": ("LTU", "Litauen"),
    "kroatien": ("HRV", "Kroatien"),
    "belgien": ("BEL", "Belgien"),
    "belgium": ("BEL", "Belgien"),
    "luxemburg": ("LUX", "Luxemburg"),
    "usa": ("USA", "USA"),
    "vereinigte staaten": ("USA", "USA"),
    "united states": ("USA", "USA"),
    "china": ("CHN", "China"),
    "indien": ("IND", "Indien"),
    "india": ("IND", "Indien"),
    "japan": ("JPN", "Japan"),
    "südkorea": ("KOR", "Südkorea"),
    "suedkorea": ("KOR", "Südkorea"),
    "south korea": ("KOR", "Südkorea"),
    "australien": ("AUS", "Australien"),
    "australia": ("AUS", "Australien"),
    "kanada": ("CAN", "Kanada"),
    "canada": ("CAN", "Kanada"),
    "brasilien": ("BRA", "Brasilien"),
    "brazil": ("BRA", "Brasilien"),
    "russland": ("RUS", "Russland"),
    "russia": ("RUS", "Russland"),
    "türkei": ("TUR", "Türkei"),
    "tuerkei": ("TUR", "Türkei"),
    "turkey": ("TUR", "Türkei"),
    "ukraine": ("UKR", "Ukraine"),
}


# ---------------------------------------------------------------------------
# Technology-Mapping
# ---------------------------------------------------------------------------
# Codes aus dem PxWeb-Variablen-Dump. Wir bilden "begriff → code"
# (mit alias-Liste) und "code → label_de/label_en" fuer Render.
#
# IRENA-Technology-Codes (Auszug):
#   0  Total renewable energy
#   1  Solar energy
#   2  Solar photovoltaic
#   3  Solar thermal energy
#   4  Wind energy
#   5  Onshore wind energy
#   6  Offshore wind energy
#   7  Renewable hydropower
#  10  Bioenergy
#  15  Geothermal energy
_TECH_INFO: dict[str, dict[str, str]] = {
    "0":  {"label_de": "Erneuerbare Energien gesamt", "label_en": "Total renewable energy"},
    "1":  {"label_de": "Solarenergie gesamt", "label_en": "Solar energy"},
    "2":  {"label_de": "Photovoltaik", "label_en": "Solar photovoltaic"},
    "3":  {"label_de": "Solarthermie", "label_en": "Solar thermal energy"},
    "4":  {"label_de": "Windenergie gesamt", "label_en": "Wind energy"},
    "5":  {"label_de": "Wind onshore", "label_en": "Onshore wind energy"},
    "6":  {"label_de": "Wind offshore", "label_en": "Offshore wind energy"},
    "7":  {"label_de": "Wasserkraft (erneuerbar)", "label_en": "Renewable hydropower"},
    "10": {"label_de": "Bioenergie", "label_en": "Bioenergy"},
    "15": {"label_de": "Geothermie", "label_en": "Geothermal energy"},
}

# Stichwoerter → Technology-Code(s). Reihenfolge irrelevant; bei mehreren
# Match nimmt der Caller den ersten Treffer, oder wir geben mehrere Codes
# zurueck und queren alle (mit Begrenzung in search_irena auf max 3 Tech).
_TECH_TRIGGERS: list[tuple[tuple[str, ...], str]] = [
    # spezifisch zuerst — Multi-Word vor Single-Word
    (("photovoltaik", "solar-pv", "solar pv", "pv-anlage", "pv anlage", "solaranlage"), "2"),
    (("solarthermie", "solar thermal", "solarwaerme", "solarwärme"), "3"),
    (("offshore-wind", "offshore wind", "wind-offshore", "offshore-windkraft"), "6"),
    (("onshore-wind", "onshore wind", "wind-onshore"), "5"),
    (("windkraft", "windenergie", "wind power", "wind energy", "windrad", "windräder", "windraeder"), "4"),
    (("wasserkraft", "hydropower", "hydro power", "hydroelektrisch"), "7"),
    (("biomasse", "bioenergie", "bioenergy", "biogas"), "10"),
    (("geothermie", "geothermal", "erdwärme", "erdwaerme"), "15"),
    (("solar", "sonnenenergie"), "1"),  # generisch, am Ende
    (("erneuerbare", "renewables", "renewable energy", "ee-anteil", "ee anteil", "ökostrom", "oekostrom"), "0"),
]


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_IRENA_DIRECT_TERMS = (
    "irena", "irenastat",
    "international renewable energy agency",
    "renewable capacity statistics",
    "renewable energy statistics",
)

_IRENA_TOPIC_TERMS = (
    # Tech-Begriffe (auch wenn IRENA nicht direkt genannt wird, koennen
    # wir bei klarer Themen-Bindung helfen)
    "erneuerbare energie", "erneuerbare energien",
    "renewable energy", "renewables",
    "photovoltaik", "solar pv", "solar-pv",
    "solaranlage", "solaranlagen",
    "windkraft", "windenergie", "wind energy", "wind power",
    "wasserkraft", "hydropower", "hydro power",
    "biomasse", "bioenergie", "geothermie", "geothermal",
    "solarthermie", "solarwärme", "solarwaerme",
    "ee-anteil", "ee anteil",
    "ökostrom", "oekostrom",
    "ee-kapazität", "ee-kapazitaet", "ee kapazität", "ee kapazitaet",
)


def _claim_mentions_irena(claim_lc: str) -> bool:
    """Trigger fuer IRENA-Service.

    1. Direkter IRENA-Mention → True.
    2. Renewable-Tech-Topic + Country-Mention → True.
    3. Renewable-Tech-Topic + globaler/EU-/weltweit-Mention → True.
    """
    if any(t in claim_lc for t in _IRENA_DIRECT_TERMS):
        return True

    has_topic = any(t in claim_lc for t in _IRENA_TOPIC_TERMS)
    if not has_topic:
        return False

    # Country-Bindung
    if _detect_country(claim_lc) is not None:
        return True

    # Globale/weltweite Bindung
    has_global = any(t in claim_lc for t in (
        "weltweit", "global", "global", "world",
        "international", "weltkapazität", "weltkapazitaet",
        "eu-weit", "europaweit", "europäisch", "europaeisch",
    ))
    if has_global:
        return True

    return False


def claim_mentions_irena_cached(claim: str) -> bool:
    return _claim_mentions_irena((claim or "").lower())


# ---------------------------------------------------------------------------
# Detection-Helpers
# ---------------------------------------------------------------------------
def _detect_country(claim_lc: str) -> tuple[str, str] | None:
    """Erkenne erstes (iso3, deutscher_name) im Claim."""
    # laengere Begriffe zuerst, sonst matched "Niederlande" auch auf "land"
    for term in sorted(_COUNTRY_MAP.keys(), key=len, reverse=True):
        if term in claim_lc:
            return _COUNTRY_MAP[term]
    return None


def _detect_technologies(claim_lc: str) -> list[str]:
    """Erkenne Technology-Codes. Max 3 Codes (Trigger-Spam vermeiden)."""
    out: list[str] = []
    for triggers, code in _TECH_TRIGGERS:
        if any(t in claim_lc for t in triggers):
            if code not in out:
                out.append(code)
        if len(out) >= 3:
            break
    # Default: wenn nichts erkannt aber das Triggern ueberhaupt feuerte
    # (z.B. "IRENA-Daten zu Österreich"), defaulten auf "Total renewable".
    if not out:
        out.append("0")
    return out


# ---------------------------------------------------------------------------
# PxWeb-Query
# ---------------------------------------------------------------------------
async def _pxweb_query(
    client: httpx.AsyncClient,
    country_iso3: str,
    tech_code: str,
) -> list[tuple[str, float]]:
    """POST-Query gegen Country_ELECCAP-PxFile.

    Returns: list[(year_str, value_mw)] sortiert nach Jahr aufsteigend.
    Bei Fehler → leer.
    """
    cache_key = (CAPACITY_TABLE, country_iso3, tech_code)
    now = time.time()
    cached = _cache.get(cache_key)
    if cached and (now - cached[0]) < CACHE_TTL:
        return cached[1]

    url = f"{BASE_URL}/{CAPACITY_TABLE}"
    body = {
        "query": [
            {
                "code": "Country/area",
                "selection": {"filter": "item", "values": [country_iso3]},
            },
            {
                "code": "Technology",
                "selection": {"filter": "item", "values": [tech_code]},
            },
            {
                "code": "Grid connection",
                "selection": {"filter": "item", "values": ["0"]},
            },
        ],
        "response": {"format": "json"},
    }

    try:
        resp = await client.post(
            url,
            json=body,
            headers={"Accept": "application/json"},
            timeout=TIMEOUT_S,
        )
    except (httpx.TimeoutException, httpx.RequestError) as e:
        logger.debug(f"IRENA: request failed {country_iso3}/{tech_code}: {e}")
        return cached[1] if cached else []

    if resp.status_code != 200:
        logger.debug(
            f"IRENA: {country_iso3}/{tech_code} HTTP {resp.status_code}"
        )
        return cached[1] if cached else []

    try:
        payload = resp.json()
    except Exception as e:
        logger.debug(f"IRENA: JSON parse failed: {e}")
        return cached[1] if cached else []

    rows: list[tuple[str, float]] = []
    # Variable-Index → Jahres-Mapping (Year-Code in PxWeb ist 0..25 → 2000..2025)
    for entry in payload.get("data", []) or []:
        key = entry.get("key", [])
        vals = entry.get("values", [])
        if len(key) < 4 or not vals:
            continue
        # key = [country, tech, grid, year_idx]
        try:
            year_idx = int(key[3])
        except (TypeError, ValueError):
            continue
        year_str = str(2000 + year_idx)
        raw = vals[0]
        if raw in (None, "-", "..", ""):
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        rows.append((year_str, value))

    rows.sort(key=lambda r: r[0])
    _cache[cache_key] = (now, rows)
    logger.info(
        f"IRENA: fetched {country_iso3}/{tech_code} → {len(rows)} year-rows"
    )
    return rows


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _format_mw(v: float) -> str:
    """Format MW with German thousands separator. >1000 MW → GW-Hinweis."""
    if v >= 1000:
        gw = v / 1000.0
        # 1234.5 MW → "1.235 MW (1,23 GW)"
        mw_int = int(round(v))
        mw_de = f"{mw_int:,}".replace(",", ".")
        gw_de = f"{gw:.2f}".replace(".", ",")
        return f"{mw_de} MW ({gw_de} GW)"
    if v >= 10:
        return f"{int(round(v)):,}".replace(",", ".") + " MW"
    # kleine Werte mit Nachkomma
    return f"{v:.2f}".replace(".", ",") + " MW"


def _build_result(
    country_iso3: str,
    country_name: str,
    tech_code: str,
    rows: list[tuple[str, float]],
) -> dict | None:
    if not rows:
        return None

    tech = _TECH_INFO.get(tech_code, {})
    tech_label = tech.get("label_de") or tech.get("label_en") or f"Tech-{tech_code}"
    tech_en = tech.get("label_en") or tech_label

    # Letzter Wert (aktuellstes Jahr) + Trend (vs vor 5 Jahren)
    latest_year, latest_val = rows[-1]
    display = (
        f"{country_name}: {tech_label} installierte Kapazitaet "
        f"{latest_year}: {_format_mw(latest_val)}"
    )

    # Beschreibung mit Trend
    description_parts: list[str] = []
    if len(rows) >= 6:
        # Wert vor 5 Jahren (oder erster Punkt, falls < 5 Jahre Historie)
        prev_year, prev_val = rows[-6]
        if prev_val > 0:
            change_pct = (latest_val - prev_val) / prev_val * 100.0
            arrow = "+" if change_pct >= 0 else ""
            change_str = f"{arrow}{change_pct:.1f}".replace(".", ",") + " %"
            description_parts.append(
                f"Trend {prev_year} → {latest_year}: "
                f"{_format_mw(prev_val)} → {_format_mw(latest_val)} "
                f"({change_str})."
            )
    if len(rows) >= 2:
        first_year, first_val = rows[0]
        if first_val > 0 and first_year != latest_year:
            factor = latest_val / first_val
            factor_str = f"{factor:.1f}".replace(".", ",")
            description_parts.append(
                f"Langfristig seit {first_year}: {_format_mw(first_val)} "
                f"→ {_format_mw(latest_val)} (Faktor {factor_str}x)."
            )

    description_parts.append(
        f"Quelle: IRENASTAT Renewable Capacity Statistics (CC BY 4.0). "
        f"Werte = installierte On-Grid-Netto-Kapazitaet in Megawatt, "
        f"erhoben von IRENA bei nationalen Energie-Behoerden."
    )
    if "/" not in tech_en:  # keine seltsamen Slash-Tech-Labels
        description_parts.append(f"Technology-Label: {tech_en}.")

    return {
        "indicator_name": f"{tech_label}, {country_name}",
        "indicator": f"irena_eleccap_{country_iso3.lower()}_{tech_code}",
        "country": country_iso3,
        "country_name": country_name,
        "year": latest_year,
        "value": latest_val,
        "display_value": display,
        "description": " ".join(description_parts),
        "url": DATA_URL,
        "source": SOURCE_LABEL,
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_irena(analysis: dict) -> dict:
    """IRENA-Live-Lookup fuer Renewable-Energy-Capacity-Claims."""
    empty = {
        "source": "IRENA",
        "type": "renewable_energy",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_irena(matchable):
        return empty

    country = _detect_country(matchable)
    techs = _detect_technologies(matchable)
    if not country:
        # Kein erkennbares Land — wir koennten "World" oder Region-Aggregate
        # versuchen, aber die Country_ELECCAP-Tabelle kennt keine
        # "World"-Zeile (dafuer waere Region_ELECCAP* noetig). Sauberer:
        # graceful empty.
        logger.debug("IRENA: no country detected, skipping")
        return empty

    country_iso3, country_name = country

    results: list[dict] = []
    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            # Parallel pro Tech
            tasks = [
                _pxweb_query(client, country_iso3, tc) for tc in techs
            ]
            tech_rows_list = await asyncio.gather(*tasks, return_exceptions=True)
        for tc, rows_or_err in zip(techs, tech_rows_list):
            if isinstance(rows_or_err, Exception):
                logger.debug(f"IRENA: tech {tc} errored: {rows_or_err}")
                continue
            rows = rows_or_err or []
            r = _build_result(country_iso3, country_name, tc, rows)
            if r is not None:
                results.append(r)
            if len(results) >= 3:
                break
    except Exception as e:
        logger.warning(f"IRENA: search failed: {e}")
        return empty

    return {
        "source": "IRENA",
        "type": "renewable_energy",
        "results": results,
    }


# WIRING für main.py:
# from services.irena import search_irena, claim_mentions_irena_cached
# if claim_mentions_irena_cached(claim):
#     tasks.append(cached("IRENA", search_irena, analysis))
#     queried_names.append("IRENA")
