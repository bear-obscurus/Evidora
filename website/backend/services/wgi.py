"""WGI — World Bank Worldwide Governance Indicators (Live-API).

Komplementär zu services/worldbank.py (Development-Indicators) und
services/transparency.py (CPI). WGI liefert sechs Governance-Dimensionen,
zusammengefasst aus 35 Cross-Country-Quellen.

Quelle: World Bank — Worldwide Governance Indicators (WGI).
URL:    https://api.worldbank.org/v2/country/{iso}/indicator/{ind}?source=75&format=json
Skala:  Estimate −2,5 (schwach) bis +2,5 (stark).
Lizenz: CC BY 4.0.

Dimensionen (6):
  VA.EST  Voice and Accountability
  PV.EST  Political Stability and Absence of Violence/Terrorism
  GE.EST  Government Effectiveness
  RQ.EST  Regulatory Quality
  RL.EST  Rule of Law
  CC.EST  Control of Corruption

GUARDRAILS (project_political_guardrails.md):
  - Wir zitieren WGI-Scores, wir bewerten sie nicht.
  - Description nennt Methodik (Aggregat aus 35 Quellen) + Skala.
  - Keine eigene Bewertung der Regierung.

Wiring (NICHT in dieser Datei):
  # from services.wgi import search_wgi, claim_mentions_wgi_cached
  # if claim_mentions_wgi_cached(claim):
  #     tasks.append(cached("WGI", search_wgi, analysis))
  #     queried_names.append("WGI (World Bank Governance)")
"""

from __future__ import annotations

import logging
import time

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BASE_URL = "https://api.worldbank.org/v2"
SOURCE_ID = "75"  # WGI

# 24h cache TTL — WGI ist jährlich, intraday-Cache reicht völlig.
CACHE_TTL = 86400
# Cache: key = (country_str, indicator, year_range) -> (timestamp, results_list)
_cache: dict[tuple[str, str, str], tuple[float, list[dict]]] = {}

# ---------------------------------------------------------------------------
# Indicator-Map
# ---------------------------------------------------------------------------
# (id, display-name, keyword-list)
WGI_INDICATORS: dict[str, dict] = {
    "VA.EST": {
        "name": "Voice and Accountability (WGI)",
        "short": "Voice & Accountability",
        "keywords": (
            "voice and accountability", "voice & accountability",
            "voice accountability", "mitsprache", "rechenschaft",
            "meinungsfreiheit governance", "pressefreiheit governance",
            "bürgerbeteiligung", "buergerbeteiligung",
            "civil liberties", "politische rechte governance",
        ),
    },
    "PV.EST": {
        "name": "Political Stability and Absence of Violence/Terrorism (WGI)",
        "short": "Politische Stabilität",
        "keywords": (
            "political stability", "politische stabilität",
            "politische stabilitaet", "absence of violence",
            "abwesenheit von gewalt", "terrorismus governance",
            "staatliche stabilität", "staatliche stabilitaet",
            "instabilität staat", "instabilitaet staat",
        ),
    },
    "GE.EST": {
        "name": "Government Effectiveness (WGI)",
        "short": "Regierungs-Effektivität",
        "keywords": (
            "government effectiveness", "regierungseffektivität",
            "regierungseffektivitaet", "regierungsqualität",
            "regierungsqualitaet", "verwaltungsqualität",
            "verwaltungsqualitaet", "öffentliche dienstleistungen",
            "oeffentliche dienstleistungen",
            "public services quality", "bureaucracy quality",
            "qualität der verwaltung", "qualitaet der verwaltung",
        ),
    },
    "RQ.EST": {
        "name": "Regulatory Quality (WGI)",
        "short": "Regulierungsqualität",
        "keywords": (
            "regulatory quality", "regulierungsqualität",
            "regulierungsqualitaet", "regulierung qualität",
            "wirtschaftsregulierung", "regulatorische qualität",
            "regulatorische qualitaet", "marktregulierung qualität",
            "marktregulierung qualitaet",
        ),
    },
    "RL.EST": {
        "name": "Rule of Law (WGI)",
        "short": "Rule of Law",
        "keywords": (
            "rule of law", "rechtsstaatlichkeit", "rechtsstaat",
            "verfassungsstaat", "rechtsstaatsprinzip",
            "justiz unabhängigkeit", "justiz unabhaengigkeit",
            "gerichtsbarkeit qualität", "gerichtsbarkeit qualitaet",
            "rechtssicherheit governance",
        ),
    },
    "CC.EST": {
        "name": "Control of Corruption (WGI)",
        "short": "Korruptionskontrolle",
        "keywords": (
            "control of corruption", "korruptionskontrolle",
            "korruptions-kontrolle", "korruptions-bekämpfung",
            "korruptions bekaempfung", "korruptions-bekaempfung",
            "anti-korruption", "antikorruption", "anti korruption",
            "korruptions-prävention", "korruptionspraevention",
            "korruptionsbekämpfung", "korruptionsbekaempfung",
        ),
    },
}

# Cross-Cluster-Trigger: Generelle Governance-Begriffe → alle 6 Dimensionen
_GENERAL_TRIGGERS = (
    "wgi", "worldwide governance indicators", "worldbank governance",
    "world bank governance", "weltbank governance",
    "governance-index", "governance index",
    "governance qualität", "governance qualitaet",
    "regierungsführung", "regierungsfuehrung",
)

# Cross-Cluster: CPI/Transparency-Begriffe sollen auch WGI feuern (Komplement)
_CPI_CROSS_TRIGGERS = (
    "korruption", "corruption index", "cpi",
    "transparency international",
)

# ---------------------------------------------------------------------------
# Country-Map (EU-27 + erweitert)
# ---------------------------------------------------------------------------
COUNTRY_MAP: dict[str, str] = {
    # DACH
    "österreich": "AUT", "oesterreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "schweiz": "CHE", "switzerland": "CHE",
    # EU-27
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD", "holland": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czech": "CZE", "czechia": "CZE",
    "ungarn": "HUN", "hungary": "HUN",
    "rumänien": "ROU", "rumaenien": "ROU", "romania": "ROU",
    "bulgarien": "BGR", "bulgaria": "BGR",
    "kroatien": "HRV", "croatia": "HRV",
    "slowenien": "SVN", "slovenia": "SVN",
    "slowakei": "SVK", "slovakia": "SVK",
    "dänemark": "DNK", "daenemark": "DNK", "denmark": "DNK",
    "schweden": "SWE", "sweden": "SWE",
    "finnland": "FIN", "finland": "FIN",
    "portugal": "PRT",
    "griechenland": "GRC", "greece": "GRC",
    "irland": "IRL", "ireland": "IRL",
    "luxemburg": "LUX", "luxembourg": "LUX",
    "estland": "EST", "estonia": "EST",
    "lettland": "LVA", "latvia": "LVA",
    "litauen": "LTU", "lithuania": "LTU",
    "malta": "MLT",
    "zypern": "CYP", "cyprus": "CYP",
    # Erweiterung (Non-EU EU-Nachbar + globale Referenz)
    "norwegen": "NOR", "norway": "NOR",
    "island": "ISL", "iceland": "ISL",
    "vereinigtes königreich": "GBR", "vereinigtes koenigreich": "GBR",
    "großbritannien": "GBR", "grossbritannien": "GBR",
    "united kingdom": "GBR", "uk": "GBR",
    "türkei": "TUR", "tuerkei": "TUR", "turkey": "TUR", "türkiye": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "australien": "AUS", "australia": "AUS",
    "kanada": "CAN", "canada": "CAN",
    "südkorea": "KOR", "suedkorea": "KOR", "south korea": "KOR",
    "südafrika": "ZAF", "suedafrika": "ZAF", "south africa": "ZAF",
}

# EU-Aggregat für Vergleich
EU_AGGREGATE = "EUU"

# Default-Länder, wenn Claim Governance-Term aber kein Land nennt
_DEFAULT_COUNTRIES = ("AUT",)

# Default-Year-Range (5 Jahre Lookback — WGI ist 1-2 Jahre verzögert)
_DEFAULT_YEAR_RANGE = "2020:2024"

# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_wgi(claim_lc: str) -> bool:
    """Pure-string Trigger-Test gegen die WGI-Themenkeywords."""
    # 1) Generelle WGI-Begriffe
    if any(t in claim_lc for t in _GENERAL_TRIGGERS):
        return True
    # 2) Dimension-spezifische Keywords
    for spec in WGI_INDICATORS.values():
        if any(kw in claim_lc for kw in spec["keywords"]):
            return True
    # 3) Cross-Cluster: CPI/Transparency-Begriffe (Komplement zu Transparency-Service)
    if any(t in claim_lc for t in _CPI_CROSS_TRIGGERS):
        return True
    return False


def claim_mentions_wgi_cached(claim: str) -> bool:
    """Public-API: lowercase + Test."""
    return _claim_mentions_wgi((claim or "").lower())


# ---------------------------------------------------------------------------
# Indicator-Detection
# ---------------------------------------------------------------------------
def _find_indicators(analysis: dict) -> list[str]:
    """Findet die gefragten WGI-Dimensionen im Claim.

    Returns: Liste der Indicator-IDs in Match-Reihenfolge.
    Fallback: alle 6, wenn nur generelle Governance-Begriffe vorkommen.
    """
    claim = (analysis.get("claim") or "").lower()
    original = (analysis.get("original_claim") or "").lower()
    keywords = " ".join(analysis.get("spacy_keywords") or []).lower()
    factcheck_q = " ".join(analysis.get("factcheck_queries") or []).lower()
    search = f"{original} {claim} {keywords} {factcheck_q}"

    matched: list[str] = []
    for ind_id, spec in WGI_INDICATORS.items():
        if any(kw in search for kw in spec["keywords"]):
            matched.append(ind_id)

    if matched:
        return matched[:3]  # Top-3

    # Fallback: generelle Governance-/Korruptions-Erwähnung → alle 6 (Top-3 Default)
    if any(t in search for t in _GENERAL_TRIGGERS):
        return ["RL.EST", "CC.EST", "GE.EST"]
    if any(t in search for t in _CPI_CROSS_TRIGGERS):
        # CPI-Cross — nur Korruption (Komplement zu transparency.py)
        return ["CC.EST"]

    return []


def _find_countries(analysis: dict) -> list[str]:
    """Extract ISO-3 country codes from claim (NER + Claim-Text)."""
    ner_countries = (analysis.get("ner_entities") or {}).get("countries") or []
    claim = analysis.get("claim", "") or ""
    original = analysis.get("original_claim", "") or ""
    search_terms = list(ner_countries) + [claim, original]

    found: list[str] = []
    seen: set[str] = set()
    for term in search_terms:
        tl = term.lower()
        # längste Namen zuerst, damit "südkorea" nicht durch "korea" überschrieben wird
        for name in sorted(COUNTRY_MAP.keys(), key=len, reverse=True):
            if name in tl:
                code = COUNTRY_MAP[name]
                if code not in seen:
                    found.append(code)
                    seen.add(code)
                    if len(found) >= 3:
                        return found
    return found


# ---------------------------------------------------------------------------
# Value-Helpers
# ---------------------------------------------------------------------------
def _de_num(v: float | None, decimals: int = 2) -> str:
    if v is None:
        return "k. A."
    return f"{v:.{decimals}f}".replace(".", ",")


def _qualitative_band(val: float) -> str:
    """Qualitatives Label zur WGI-Estimate-Skala (−2,5 … +2,5).

    NICHT als eigene Bewertung, sondern als Lese-Hilfe zur Skala.
    """
    if val >= 1.5:
        return "Top-10 % weltweit"
    if val >= 1.0:
        return "obere 20 % weltweit"
    if val >= 0.5:
        return "oberes Mittelfeld"
    if val >= -0.5:
        return "Mittelfeld"
    if val >= -1.0:
        return "unteres Mittelfeld"
    if val >= -1.5:
        return "untere 20 % weltweit"
    return "untere 10 % weltweit"


def _build_description(
    ind_id: str,
    country_name: str,
    year: str,
    value: float,
    eu_value: float | None,
) -> str:
    short = WGI_INDICATORS[ind_id]["short"]
    parts = [
        f"WGI-Dimension '{short}' für {country_name} im Jahr {year}: "
        f"Estimate-Score {_de_num(value)} auf der Skala −2,5 (schwach) "
        f"bis +2,5 (stark). Einordnung: {_qualitative_band(value)}.",
    ]
    if eu_value is not None:
        diff = value - eu_value
        sign = "+" if diff >= 0 else "−"
        parts.append(
            f"EU-Aggregat-Vergleich (EUU) {year}: {_de_num(eu_value)} — "
            f"{country_name} liegt {sign}{_de_num(abs(diff))} Punkte über/unter "
            f"dem EU-Schnitt."
        )
    parts.append(
        "Methodik: Aggregat aus 35 Cross-Country-Quellen (Experten-Surveys, "
        "Unternehmens-Surveys, Bevölkerungsumfragen, NGO-Indizes) via "
        "Unobserved-Components-Modell. Konfidenzintervalle typisch ±0,15 "
        "auf der Estimate-Skala. WGI ist eine Wahrnehmungs-/Experten-"
        "Aggregation, kein direktes Verhaltensmaß."
    )
    return " ".join(parts)


# ---------------------------------------------------------------------------
# API-Call
# ---------------------------------------------------------------------------
async def _fetch_indicator(
    client: httpx.AsyncClient,
    country_str: str,
    indicator: str,
    year_range: str,
) -> list[dict]:
    """Hole WGI-Daten für (countries, indicator). Cache-bewusst."""
    key = (country_str, indicator, year_range)
    now = time.time()
    hit = _cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL:
        return hit[1]

    try:
        resp = await client.get(
            f"{BASE_URL}/country/{country_str}/indicator/{indicator}",
            params={
                "source": SOURCE_ID,
                "format": "json",
                "per_page": 200,
                "date": year_range,
            },
        )
        if resp.status_code == 429:
            logger.warning("WGI API rate limit (429) for %s/%s", country_str, indicator)
            return []
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as e:
        logger.warning("WGI HTTP %s for %s/%s", e.response.status_code, country_str, indicator)
        return []
    except Exception as e:  # noqa: BLE001
        logger.warning("WGI fetch failed for %s/%s: %s", country_str, indicator, e)
        return []

    if not isinstance(data, list) or len(data) < 2 or not data[1]:
        _cache[key] = (now, [])
        return []

    entries = data[1] or []
    _cache[key] = (now, entries)
    return entries


def _latest_per_country(entries: list[dict]) -> dict[str, dict]:
    """Letzter Nicht-Null-Wert je Land aus einer WGI-Antwort."""
    out: dict[str, dict] = {}
    # entries sind typisch neueste zuerst — wir prüfen value != None
    for e in entries:
        if e.get("value") is None:
            continue
        iso = e.get("countryiso3code") or ""
        if not iso:
            continue
        if iso not in out:
            out[iso] = e
    return out


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_wgi(analysis: dict) -> dict:
    """Live-API-Search gegen die WGI World-Bank-Quelle."""
    empty = {"source": "WGI", "type": "governance_data", "results": []}

    claim = analysis.get("claim", "") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_wgi(matchable):
        return empty

    indicators = _find_indicators(analysis)
    if not indicators:
        return empty

    countries = _find_countries(analysis)
    if not countries:
        countries = list(_DEFAULT_COUNTRIES)
    countries = countries[:3]

    # EU-Aggregat zur Einordnung mitabholen
    country_str = ";".join([*countries, EU_AGGREGATE])
    year_range = _DEFAULT_YEAR_RANGE

    results: list[dict] = []
    try:
        async with polite_client(timeout=30.0) as client:
            for ind_id in indicators:
                entries = await _fetch_indicator(client, country_str, ind_id, year_range)
                if not entries:
                    continue
                latest = _latest_per_country(entries)
                eu_entry = latest.get(EU_AGGREGATE)
                eu_value = eu_entry.get("value") if eu_entry else None

                for iso in countries:
                    entry = latest.get(iso)
                    if not entry:
                        continue
                    value = entry.get("value")
                    if value is None:
                        continue
                    country_name = (entry.get("country") or {}).get("value") or iso
                    year = entry.get("date") or ""
                    short = WGI_INDICATORS[ind_id]["short"]
                    de_val = _de_num(value)
                    band = _qualitative_band(value)
                    display_value = (
                        f"{country_name}-{short}-Score {year}: {de_val} "
                        f"(Skala −2,5 bis +2,5; {band})"
                    )
                    if eu_value is not None:
                        display_value += f" — EU-Schnitt {year}: {_de_num(eu_value)}"

                    results.append({
                        "indicator_name": WGI_INDICATORS[ind_id]["name"],
                        "indicator": (
                            f"wgi_{ind_id.lower().replace('.', '_')}_{iso.lower()}"
                        ),
                        "country": iso,
                        "country_name": country_name,
                        "year": year,
                        "value": value,
                        "display_value": display_value,
                        "description": _build_description(
                            ind_id, country_name, year, value, eu_value,
                        ),
                        "url": (
                            "https://databank.worldbank.org/source/"
                            "worldwide-governance-indicators"
                        ),
                        "source": "WGI (World Bank Worldwide Governance Indicators)",
                    })
    except Exception as e:  # noqa: BLE001
        logger.warning("WGI search failed: %s", e)
        return empty

    # Top-3 nach |value| (kräftigster Score zuerst), maximal 3
    results.sort(key=lambda r: abs(r.get("value") or 0.0), reverse=True)
    results = results[:3]

    logger.info(
        "WGI: %d results for indicators=%s countries=%s",
        len(results), indicators, countries,
    )
    return {"source": "WGI", "type": "governance_data", "results": results}


# ---------------------------------------------------------------------------
# Optional: Bulk-Prefetch für data_updater (NICHT verdrahtet)
# ---------------------------------------------------------------------------
async def fetch_wgi(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Optionaler Bulk-Refresh-Hook für späteres Prefetch.

    Aktuell: Pure-Live-API, daher leerer Stub mit Pre-Warm der DACH-Länder
    auf RL.EST + CC.EST (häufigste Trigger), damit der erste Live-Claim
    bereits Cache-Hits erzeugt.
    """
    countries = "AUT;DEU;CHE"
    if client is None:
        async with polite_client(timeout=30.0) as c:
            for ind in ("RL.EST", "CC.EST"):
                await _fetch_indicator(c, countries, ind, _DEFAULT_YEAR_RANGE)
    else:
        for ind in ("RL.EST", "CC.EST"):
            await _fetch_indicator(client, countries, ind, _DEFAULT_YEAR_RANGE)
    return []
