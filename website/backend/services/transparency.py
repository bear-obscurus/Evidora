"""Transparency International — Corruption Perception Index (CPI).

Datenquelle: Transparency International (CPI, jährlich seit 1995).
Zugriff via Our World in Data (OWID) Grapher CSV, der täglich aus
den offiziellen TI-Veröffentlichungen aktualisiert wird.

Skala: 0 = höchst korrupt, 100 = keine wahrgenommene Korruption.
Abdeckung: ~180 Länder, Jahre 2012–heute (konsistente Methodik seit 2012).

Lizenz: CC BY-ND 4.0 (Transparency International).
Zitation: Transparency International, Corruption Perceptions Index {Jahr}.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren CPI-Scores, berechnen keine eigenen Korruptionsbewertungen.
- Caveat zur Methodik (Wahrnehmung, nicht gemessene Korruption) ist Pflicht.
"""

import csv
import io
import logging
import time

import httpx

logger = logging.getLogger("evidora")

CPI_CSV_URL = "https://ourworldindata.org/grapher/ti-corruption-perception-index.csv"

CPI_CACHE_TTL = 86400  # 24h

# Cache structure: {iso3: {year: {score, entity}}}
_cpi_cache: dict | None = None
_cpi_cache_time: float = 0.0

# Trigger-Keywords (DE + EN)
CPI_KEYWORDS = [
    "korruption", "korrupt", "korruptionsindex",
    "corruption", "corrupt", "corruption index",
    "transparency international",
    "cpi", "corruption perception",
    "bestechung", "bribery", "bribe",
    "vetternwirtschaft", "cronyism",
    "amtsmissbrauch", "abuse of office",
    "integrität", "integrity",
]

# Kleinere Country-Map (wir setzen auf die gleiche ISO3-Logik wie V-Dem)
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
    "vereinigtes königreich": "GBR", "united kingdom": "GBR",
    "türkei": "TUR", "turkey": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "belarus": "BLR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "südkorea": "KOR", "south korea": "KOR",
    "iran": "IRN",
}


async def fetch_cpi(client: httpx.AsyncClient | None = None) -> dict:
    """Download and parse the CPI CSV from OWID."""
    global _cpi_cache, _cpi_cache_time

    now = time.time()
    if _cpi_cache is not None and (now - _cpi_cache_time) < CPI_CACHE_TTL:
        return _cpi_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    data: dict = {}

    try:
        resp = await client.get(CPI_CSV_URL)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            code = (row.get("Code") or "").strip()
            entity = (row.get("Entity") or "").strip()
            year_raw = (row.get("Year") or "").strip()
            score_raw = (row.get("Corruption Perceptions Index") or "").strip()
            if not code or not year_raw or not score_raw:
                continue
            try:
                year = int(year_raw)
                score = float(score_raw)
            except ValueError:
                continue
            data.setdefault(code, {})[year] = {"score": score, "entity": entity}

        _cpi_cache = data
        _cpi_cache_time = now
        total_points = sum(len(y) for y in data.values())
        logger.info(f"CPI: {len(data)} countries, {total_points} country-years cached")
        return data
    finally:
        if close_client:
            await client.aclose()


def _find_countries(analysis: dict, max_n: int = 3) -> list[str]:
    """Extract ISO3 country codes from claim (NER-prioritized)."""
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


def _claim_mentions_cpi(claim: str) -> bool:
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in CPI_KEYWORDS)


async def search_transparency(analysis: dict) -> dict:
    """Search CPI cache for corruption scores."""
    if not _claim_mentions_cpi(analysis.get("claim", "")):
        return {"source": "Transparency International", "type": "official_data", "results": []}

    data = await fetch_cpi()
    if not data:
        return {"source": "Transparency International", "type": "official_data", "results": []}

    countries = _find_countries(analysis)
    if not countries:
        countries = ["AUT", "DEU"]  # Default-Kontext

    results: list[dict] = []
    for code in countries:
        country_data = data.get(code)
        if not country_data:
            continue
        latest_year = max(country_data.keys())
        entry = country_data[latest_year]
        score = entry.get("score")
        entity = entry.get("entity", code)

        # Trend über 10 Jahre (oder wie verfügbar)
        years = sorted(country_data.keys())
        trend_note = ""
        if len(years) >= 10:
            ten_years_ago_year = latest_year - 10
            if ten_years_ago_year in country_data:
                delta = score - country_data[ten_years_ago_year]["score"]
                trend_arrow = "↑" if delta > 1 else ("↓" if delta < -1 else "→")
                trend_note = f" | Trend 10J: {trend_arrow} ({delta:+.1f} Punkte)"

        # Einordnung in Cluster (nur zur Info, nicht als Urteil)
        if score >= 80:
            cluster = "sehr gering wahrgenommene Korruption"
        elif score >= 60:
            cluster = "gering wahrgenommene Korruption"
        elif score >= 40:
            cluster = "moderat wahrgenommene Korruption"
        elif score >= 20:
            cluster = "hoch wahrgenommene Korruption"
        else:
            cluster = "sehr hoch wahrgenommene Korruption"

        results.append({
            "indicator_name": f"CPI {entity} ({latest_year}): {score:.0f}/100 — {cluster}{trend_note}",
            "indicator": "cpi_score",
            "country": code,
            "country_name": entity,
            "year": str(latest_year),
            "value": score,
            "display_value": f"{score:.0f}/100",
            "url": "https://www.transparency.org/en/cpi",
        })

    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: CPI misst wahrgenommene, nicht gemessene Korruption",
            "indicator": "context",
            "country": "",
            "country_name": "",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://www.transparency.org/en/cpi/methodology",
            "description": (
                "Der Corruption Perceptions Index (CPI) von Transparency International aggregiert "
                "13 verschiedene Umfragen und Expertenbewertungen zur Wahrnehmung von Korruption im "
                "öffentlichen Sektor. Skala: 0 = höchst korrupt, 100 = keine wahrgenommene Korruption. "
                "Einschränkungen: "
                "(1) Wahrnehmung ≠ Realität — der Index misst, wie Expert:innen und Unternehmen "
                "Korruption einschätzen, nicht tatsächliche Korruptionsfälle. "
                "(2) Nur öffentlicher Sektor — private Korruption und Korruption zwischen Unternehmen "
                "sind nicht erfasst. "
                "(3) Jahresvergleiche mit Vorsicht — die Methodik seit 2012 ist stabil, aber kleine "
                "Punktänderungen (±2) liegen im Unschärfebereich. "
                "(4) Mindestzahl Quellen — pro Land müssen mindestens 3 unabhängige Quellen "
                "vorliegen; für kleinere Länder ist die Datenbasis dünner."
            ),
        })

    logger.info(f"CPI: {len(results) - (1 if results else 0)} country results, countries={countries}")
    return {"source": "Transparency International", "type": "official_data", "results": results}
