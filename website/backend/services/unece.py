"""UNECE — UN Economic Commission for Europe — Transport Statistics.

Datenquelle: UNECE PxWeb v1 (Statistical Database / Transport).
  Listing : https://w3.unece.org/PXWeb2015/api/v1/en/STAT/40-TRTRANS/
  Tables  : .../<sub-folder>/<table.px>

Lizenz: Public Domain / Open Data — UN-Standard. Quellen-Hinweis:
  "UNECE Statistical Database, Sustainable Transport Division" gehört
  in jede ausgegebene Beschreibung.

Use-Case (Mobilität-Cluster):
- "Schienen-Passagierkilometer Österreich"
- "PKW-Bestand Deutschland / Schweiz / ..."
- "Straßenbahn-Bestand AT" (Trams stehen in der Vehicle-Fleet-Tabelle)
- "Straßennetz / Autobahn-Länge [Land]"
- "Binnenschifffahrt / Inland waterways"
- "UNECE Transport Statistics"

Cross-Cluster: `transport_at.py` liefert tiefe AT-Details (Statistik Austria
+ ÖBB-Geschäftsbericht), UNECE liefert Welt-Vergleich zwischen den 56
UNECE-Mitgliedsstaaten (EU + EEA + Westbalkan + Türkei + Kaukasus +
Zentralasien + USA + Kanada + Russland).

Politische Guardrails:
- Pure Statistik. KEINE Wertung von Verkehrspolitik (Verbrenner-Aus,
  Bahn-Subventionen, Autobahn-Maut etc.), keine Prognosen, keine
  Bewertung "ist die Bahn besser als das Auto". Werte "as published by
  UNECE" — bei Diskrepanz zu nationalen Quellen (Statistik Austria /
  Eurostat) Methodik-Hinweis.

Pattern:
- POST JSON-Query gegen 3 PxWeb-Tabellen:
    * Rail Passenger Traffic (`05-TRRAIL/01_en_TRrailpassengers_r.px`)
    * Road Vehicle Fleet — Passenger (`03-TRRoadFleet/01_en_TRRoadTypVehR_r.px`)
    * Road Infrastructure — Total length (`11-TRINFRA/ZZZ_en_TRInfraRoad_r.px`)
- Country-Detection: UN-M49 numerisch (AT=040, DE=276, …) per Stichwortliste.
- Year-Codes sind Tabellen-spezifische Indizes (Listings via Metadata-GET).
- Cache 24 h pro (table, country, var-Tupel) — UNECE published Annual.

WIRING für main.py:
# from services.unece import search_unece, claim_mentions_unece_cached
# if claim_mentions_unece_cached(claim):
#     tasks.append(cached("UNECE", search_unece, analysis))
#     queried_names.append("UNECE")
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
BASE_URL = "https://w3.unece.org/PXWeb2015/api/v1/en/STAT/40-TRTRANS"
PIVOT_BASE = "https://w3.unece.org/PXWeb/en/Pivot/STAT/STAT__40-TRTRANS"

# Tabellen-Pfad-Suffixe relativ zu BASE_URL
TABLE_RAIL_PAX = "05-TRRAIL/01_en_TRrailpassengers_r.px"
TABLE_ROAD_FLEET = "03-TRRoadFleet/01_en_TRRoadTypVehR_r.px"
TABLE_ROAD_INFRA = "11-TRINFRA/ZZZ_en_TRInfraRoad_r.px"

CACHE_TTL = 24 * 3600  # 24 h
TIMEOUT_S = 25.0

SOURCE_LABEL = "UNECE Statistical Database"

# Cache: key=(table_suffix, country_m49, var_tuple) → (timestamp, list[(year, value)])
_cache: dict[tuple[str, str, tuple[str, ...]], tuple[float, list[tuple[str, float]]]] = {}

# Cache für Year-Index-Mapping pro Tabelle, gefüllt nach erstem Metadata-Pull
# key=table_suffix → (timestamp, list[year_str])  (Index = Position in der Liste)
_year_cache: dict[str, tuple[float, list[str]]] = {}


# ---------------------------------------------------------------------------
# UN-M49 Country-Mapping (UNECE-Mitgliedsstaaten)
# ---------------------------------------------------------------------------
# 56 UNECE-Mitgliedsstaaten, M49 numerisch (3 Zeichen). Lookup-Term lowercase.
# Nicht jede Tabelle hat alle 56 Codes — wir nehmen den größten gemeinsamen Nenner.
_COUNTRY_MAP: dict[str, tuple[str, str, str]] = {
    # term-lowercase → (m49_code, iso3, deutscher_name)
    "albanien": ("008", "ALB", "Albanien"),
    "albania": ("008", "ALB", "Albanien"),
    "andorra": ("020", "AND", "Andorra"),
    "armenien": ("051", "ARM", "Armenien"),
    "armenia": ("051", "ARM", "Armenien"),
    "österreich": ("040", "AUT", "Österreich"),
    "oesterreich": ("040", "AUT", "Österreich"),
    "austria": ("040", "AUT", "Österreich"),
    "aserbaidschan": ("031", "AZE", "Aserbaidschan"),
    "azerbaijan": ("031", "AZE", "Aserbaidschan"),
    "belarus": ("112", "BLR", "Belarus"),
    "weißrussland": ("112", "BLR", "Belarus"),
    "weissrussland": ("112", "BLR", "Belarus"),
    "belgien": ("056", "BEL", "Belgien"),
    "belgium": ("056", "BEL", "Belgien"),
    "bosnien": ("070", "BIH", "Bosnien-Herzegowina"),
    "bosnia": ("070", "BIH", "Bosnien-Herzegowina"),
    "bulgarien": ("100", "BGR", "Bulgarien"),
    "bulgaria": ("100", "BGR", "Bulgarien"),
    "kanada": ("124", "CAN", "Kanada"),
    "canada": ("124", "CAN", "Kanada"),
    "kroatien": ("191", "HRV", "Kroatien"),
    "croatia": ("191", "HRV", "Kroatien"),
    "zypern": ("196", "CYP", "Zypern"),
    "cyprus": ("196", "CYP", "Zypern"),
    "tschechien": ("203", "CZE", "Tschechien"),
    "czechia": ("203", "CZE", "Tschechien"),
    "czech republic": ("203", "CZE", "Tschechien"),
    "dänemark": ("208", "DNK", "Dänemark"),
    "daenemark": ("208", "DNK", "Dänemark"),
    "denmark": ("208", "DNK", "Dänemark"),
    "estland": ("233", "EST", "Estland"),
    "estonia": ("233", "EST", "Estland"),
    "finnland": ("246", "FIN", "Finnland"),
    "finland": ("246", "FIN", "Finnland"),
    "frankreich": ("250", "FRA", "Frankreich"),
    "france": ("250", "FRA", "Frankreich"),
    "georgien": ("268", "GEO", "Georgien"),
    "georgia": ("268", "GEO", "Georgien"),
    "deutschland": ("276", "DEU", "Deutschland"),
    "germany": ("276", "DEU", "Deutschland"),
    "griechenland": ("300", "GRC", "Griechenland"),
    "greece": ("300", "GRC", "Griechenland"),
    "ungarn": ("348", "HUN", "Ungarn"),
    "hungary": ("348", "HUN", "Ungarn"),
    "island": ("352", "ISL", "Island"),
    "iceland": ("352", "ISL", "Island"),
    "irland": ("372", "IRL", "Irland"),
    "ireland": ("372", "IRL", "Irland"),
    "israel": ("376", "ISR", "Israel"),
    "italien": ("380", "ITA", "Italien"),
    "italy": ("380", "ITA", "Italien"),
    "kasachstan": ("398", "KAZ", "Kasachstan"),
    "kazakhstan": ("398", "KAZ", "Kasachstan"),
    "kirgisistan": ("417", "KGZ", "Kirgisistan"),
    "kyrgyzstan": ("417", "KGZ", "Kirgisistan"),
    "lettland": ("428", "LVA", "Lettland"),
    "latvia": ("428", "LVA", "Lettland"),
    "liechtenstein": ("438", "LIE", "Liechtenstein"),
    "litauen": ("440", "LTU", "Litauen"),
    "lithuania": ("440", "LTU", "Litauen"),
    "luxemburg": ("442", "LUX", "Luxemburg"),
    "luxembourg": ("442", "LUX", "Luxemburg"),
    "malta": ("470", "MLT", "Malta"),
    "monaco": ("492", "MCO", "Monaco"),
    "montenegro": ("499", "MNE", "Montenegro"),
    "niederlande": ("528", "NLD", "Niederlande"),
    "netherlands": ("528", "NLD", "Niederlande"),
    "nordmazedonien": ("807", "MKD", "Nordmazedonien"),
    "north macedonia": ("807", "MKD", "Nordmazedonien"),
    "norwegen": ("578", "NOR", "Norwegen"),
    "norway": ("578", "NOR", "Norwegen"),
    "polen": ("616", "POL", "Polen"),
    "poland": ("616", "POL", "Polen"),
    "portugal": ("620", "PRT", "Portugal"),
    "moldau": ("498", "MDA", "Moldau"),
    "moldova": ("498", "MDA", "Moldau"),
    "rumänien": ("642", "ROU", "Rumänien"),
    "rumaenien": ("642", "ROU", "Rumänien"),
    "romania": ("642", "ROU", "Rumänien"),
    "russland": ("643", "RUS", "Russland"),
    "russia": ("643", "RUS", "Russland"),
    "russian federation": ("643", "RUS", "Russland"),
    "san marino": ("674", "SMR", "San Marino"),
    "serbien": ("688", "SRB", "Serbien"),
    "serbia": ("688", "SRB", "Serbien"),
    "slowakei": ("703", "SVK", "Slowakei"),
    "slovakia": ("703", "SVK", "Slowakei"),
    "slowenien": ("705", "SVN", "Slowenien"),
    "slovenia": ("705", "SVN", "Slowenien"),
    "spanien": ("724", "ESP", "Spanien"),
    "spain": ("724", "ESP", "Spanien"),
    "schweden": ("752", "SWE", "Schweden"),
    "sweden": ("752", "SWE", "Schweden"),
    "schweiz": ("756", "CHE", "Schweiz"),
    "switzerland": ("756", "CHE", "Schweiz"),
    "tadschikistan": ("762", "TJK", "Tadschikistan"),
    "tajikistan": ("762", "TJK", "Tadschikistan"),
    "türkei": ("792", "TUR", "Türkei"),
    "tuerkei": ("792", "TUR", "Türkei"),
    "turkey": ("792", "TUR", "Türkei"),
    "turkmenistan": ("795", "TKM", "Turkmenistan"),
    "ukraine": ("804", "UKR", "Ukraine"),
    "großbritannien": ("826", "GBR", "Vereinigtes Königreich"),
    "grossbritannien": ("826", "GBR", "Vereinigtes Königreich"),
    "united kingdom": ("826", "GBR", "Vereinigtes Königreich"),
    "vereinigtes königreich": ("826", "GBR", "Vereinigtes Königreich"),
    "usa": ("840", "USA", "USA"),
    "vereinigte staaten": ("840", "USA", "USA"),
    "united states": ("840", "USA", "USA"),
    "usbekistan": ("860", "UZB", "Usbekistan"),
    "uzbekistan": ("860", "UZB", "Usbekistan"),
}


# ---------------------------------------------------------------------------
# Tabellen-Konfiguration: Mapping von Indikator → PxWeb-Variablen
# ---------------------------------------------------------------------------
# Jeder Tabellen-Eintrag definiert:
#   table     : Suffix nach BASE_URL
#   vars      : Liste von (PxWeb-code, value, label_de, label_en)
#                  — beschreibt die GEFILTERTE Selektion (alle Vars außer
#                    Country + Year werden auf 1 Wert eingeschränkt).
#   unit_de   : Einheit für display_value
#   indicator : machine-kennung (slug)
#   ind_de    : menschenlesbarer Indikator-Name DE
#   pivot_path: Sub-Path für die PXWeb-Pivot-URL (Quellen-Link)
_TABLES: dict[str, dict[str, Any]] = {
    "rail_passenger_km": {
        "table": TABLE_RAIL_PAX,
        # Passengers=Total, Topic=Pkm (Mio.)
        "vars": [
            ("Passengers", "TR.119", "Gesamt", "Total"),
            ("Topic", "TR.8", "Passagier-Kilometer", "Passenger kilometres (millions)"),
        ],
        "unit_de": "Mio. Pkm",
        "indicator": "unece_rail_pax_km",
        "ind_de": "Schienenverkehr — Passagier-Kilometer",
        "pivot_path": "05-TRRAIL",
    },
    "road_fleet_cars": {
        "table": TABLE_ROAD_FLEET,
        # Measurement=absolut, Vehicle category=Passenger cars
        "vars": [
            ("Measurement", "TR.19", "absoluter Bestand", "absolute value"),
            ("Vehicle category", "TR.396", "PKW", "Passenger cars"),
        ],
        "unit_de": "PKW",
        "indicator": "unece_road_fleet_cars",
        "ind_de": "PKW-Bestand (31. Dez.)",
        "pivot_path": "03-TRRoadFleet",
    },
    "road_fleet_trams": {
        "table": TABLE_ROAD_FLEET,
        # Measurement=absolut, Vehicle category=Trams
        "vars": [
            ("Measurement", "TR.19", "absoluter Bestand", "absolute value"),
            ("Vehicle category", "TR.431", "Straßenbahn-Triebwagen", "Trams"),
        ],
        "unit_de": "Tram-Triebwagen",
        "indicator": "unece_road_fleet_trams",
        "ind_de": "Straßenbahn-Triebwagen-Bestand",
        "pivot_path": "03-TRRoadFleet",
    },
    "road_infra_motorways": {
        "table": TABLE_ROAD_INFRA,
        # Type of Road=Motorways
        "vars": [
            ("Type of Road", "TR.386", "Autobahnen", "Motorways"),
        ],
        "unit_de": "km",
        "indicator": "unece_road_infra_motorways",
        "ind_de": "Autobahn-Netzlänge",
        "pivot_path": "11-TRINFRA",
    },
}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_UNECE_DIRECT_TERMS = (
    "unece", "un economic commission for europe",
    "un-wirtschaftskommission europa",
    "unece transport", "unece statistics",
    "unece statistical database",
    "ece transport division",
)

# Themen-Begriffe für Mobilität / Verkehr
_UNECE_TOPIC_TERMS = (
    # Schiene
    "schienenverkehr", "schienen-passagier", "bahn-passagier",
    "bahn-statistik", "schienen-statistik", "rail passenger",
    "rail traffic", "eisenbahnverkehr", "personenverkehr bahn",
    "personenkilometer bahn", "bahn personenkilometer",
    "zug-passagier", "passagierkilometer bahn",
    # Straße
    "straßenverkehr", "strassenverkehr", "road traffic",
    "pkw-bestand", "pkw bestand", "kfz-bestand", "kfz bestand",
    "vehicle fleet", "fahrzeugbestand",
    "passenger cars", "autobestand",
    "autobahn-länge", "autobahn-laenge", "autobahn länge",
    "autobahn laenge", "autobahnen länge", "autobahnen laenge",
    "motorway length", "straßennetz", "strassennetz",
    "road network", "länge straßen", "laenge strassen",
    # Tram
    "tram-netz", "tram netz", "straßenbahn-statistik",
    "strassenbahn-statistik", "straßenbahn statistik",
    "strassenbahn statistik", "straßenbahn-bestand",
    "strassenbahn-bestand", "straßenbahn", "strassenbahn",
    "tramway", "tram",
    # Eisenbahn als Topic (zusätzlich zu rail-spezifischen Begriffen oben)
    "eisenbahn", "railway",
    # Binnenschiff
    "binnenwasserstraße", "binnenwasserstrasse",
    "binnenschifffahrt", "inland waterway",
    "binnenwasser",
    # Verkehr/Mobilität Allgemein mit Statistik-Bezug
    "verkehrsstatistik", "transport statistics",
    "mobilität-statistik", "mobilitaet-statistik",
    "modal-split",
)


def _claim_mentions_unece(claim_lc: str) -> bool:
    """Trigger für UNECE-Service.

    1. Direkter UNECE-Mention → True.
    2. Transport-Topic + Country-Mention → True.
    3. Transport-Topic + EU-/Europa-/weltweit-Mention → True.
    """
    if any(t in claim_lc for t in _UNECE_DIRECT_TERMS):
        return True

    has_topic = any(t in claim_lc for t in _UNECE_TOPIC_TERMS)
    if not has_topic:
        return False

    # Country-Bindung
    if _detect_country(claim_lc) is not None:
        return True

    # EU- / Europa-weite Bindung
    has_global = any(t in claim_lc for t in (
        "eu-weit", "eu weit", "europaweit", "europäisch", "europaeisch",
        "europa-vergleich", "europa vergleich", "länder-vergleich",
        "laender-vergleich", "länder vergleich", "international",
        "weltweit", "global",
    ))
    if has_global:
        return True

    return False


def claim_mentions_unece_cached(claim: str) -> bool:
    return _claim_mentions_unece((claim or "").lower())


# ---------------------------------------------------------------------------
# Detection-Helpers
# ---------------------------------------------------------------------------
def _detect_country(claim_lc: str) -> tuple[str, str, str] | None:
    """Erkenne ersten (m49, iso3, deutscher_name) im Claim.

    Strategie:
    1. Längere ausgeschriebene Namen zuerst (Niederlande vor Land etc.).
    2. Häufige 2-Letter-Suffixe (-AT, -DE, -CH, -FR, -IT, -ES, -GB, -US)
       mit Trennzeichen (Bindestrich oder Leerzeichen) erkennen — vermeidet
       false-positives wie "bewertet" für "at".
    """
    for term in sorted(_COUNTRY_MAP.keys(), key=len, reverse=True):
        if term in claim_lc:
            return _COUNTRY_MAP[term]
    # 2-Letter-Suffixe — nur mit Trennzeichen davor und am Wort-Ende
    iso2_map = {
        "at": ("040", "AUT", "Österreich"),
        "de": ("276", "DEU", "Deutschland"),
        "ch": ("756", "CHE", "Schweiz"),
        "fr": ("250", "FRA", "Frankreich"),
        "it": ("380", "ITA", "Italien"),
        "es": ("724", "ESP", "Spanien"),
        "gb": ("826", "GBR", "Vereinigtes Königreich"),
        "uk": ("826", "GBR", "Vereinigtes Königreich"),
        "us": ("840", "USA", "USA"),
        "nl": ("528", "NLD", "Niederlande"),
        "pl": ("616", "POL", "Polen"),
        "cz": ("203", "CZE", "Tschechien"),
        "sk": ("703", "SVK", "Slowakei"),
        "hu": ("348", "HUN", "Ungarn"),
        "si": ("705", "SVN", "Slowenien"),
        "hr": ("191", "HRV", "Kroatien"),
        "ro": ("642", "ROU", "Rumänien"),
        "bg": ("100", "BGR", "Bulgarien"),
        "se": ("752", "SWE", "Schweden"),
        "no": ("578", "NOR", "Norwegen"),
        "dk": ("208", "DNK", "Dänemark"),
        "fi": ("246", "FIN", "Finnland"),
        "ie": ("372", "IRL", "Irland"),
        "be": ("056", "BEL", "Belgien"),
        "pt": ("620", "PRT", "Portugal"),
        "gr": ("300", "GRC", "Griechenland"),
        "tr": ("792", "TUR", "Türkei"),
        "ua": ("804", "UKR", "Ukraine"),
        "ru": ("643", "RUS", "Russland"),
        "ca": ("124", "CAN", "Kanada"),
    }
    import re
    for iso2, info in iso2_map.items():
        # Bindestrich-Suffix oder isoliertes Token am Anfang/Ende/eingerahmt
        if re.search(rf"[\-\s,]({iso2})\b", claim_lc):
            return info
    return None


def _detect_indicators(claim_lc: str) -> list[str]:
    """Erkenne welche Tabellen-Indikatoren der Claim ansprich.

    Max 3 Indikatoren — Trigger-Spam vermeiden.
    """
    out: list[str] = []

    # Tram zuerst (bevor generisches "straßenverkehr" greift)
    if any(t in claim_lc for t in (
        "tram", "straßenbahn", "strassenbahn", "tramway",
    )):
        out.append("road_fleet_trams")

    # Autobahn-spezifisch
    if any(t in claim_lc for t in (
        "autobahn", "motorway", "straßennetz", "strassennetz",
        "länge straßen", "laenge strassen", "road network",
    )):
        out.append("road_infra_motorways")

    # Schiene
    if any(t in claim_lc for t in (
        "schiene", "bahn", "rail", "eisenbahn", "zug-passagier",
        "personenkilometer bahn", "passagierkilometer bahn",
        "personenverkehr bahn",
    )):
        if "rail_passenger_km" not in out:
            out.append("rail_passenger_km")

    # PKW / Fahrzeugbestand
    if any(t in claim_lc for t in (
        "pkw-bestand", "pkw bestand", "kfz-bestand", "kfz bestand",
        "fahrzeugbestand", "vehicle fleet", "passenger cars",
        "autobestand", "auto-bestand",
    )):
        if "road_fleet_cars" not in out:
            out.append("road_fleet_cars")

    # Default: wenn UNECE direkt erwähnt aber kein Indikator klar →
    # rail (häufigste Faktencheck-Anfrage)
    if not out:
        if any(t in claim_lc for t in _UNECE_DIRECT_TERMS):
            out.append("rail_passenger_km")

    # Max 3 — wir wollen nicht 4 parallele POSTs feuern
    return out[:3]


# ---------------------------------------------------------------------------
# PxWeb-Year-Lookup (Index → Jahr-String)
# ---------------------------------------------------------------------------
async def _fetch_year_codes(
    client: httpx.AsyncClient,
    table_suffix: str,
) -> list[str]:
    """Liefert die Year-Liste der Tabelle (Index N → Jahr-String).

    Cache 24h. Bei Fehler → leer (Caller filtert).
    """
    now = time.time()
    cached = _year_cache.get(table_suffix)
    if cached and (now - cached[0]) < CACHE_TTL:
        return cached[1]

    url = f"{BASE_URL}/{table_suffix}"
    try:
        resp = await client.get(
            url,
            headers={"Accept": "application/json"},
            timeout=TIMEOUT_S,
        )
    except (httpx.TimeoutException, httpx.RequestError) as e:
        logger.debug(f"UNECE: year-meta failed {table_suffix}: {e}")
        return cached[1] if cached else []

    if resp.status_code != 200:
        logger.debug(f"UNECE: year-meta {table_suffix} HTTP {resp.status_code}")
        return cached[1] if cached else []

    try:
        payload = resp.json()
    except Exception as e:
        logger.debug(f"UNECE: year-meta JSON failed: {e}")
        return cached[1] if cached else []

    years: list[str] = []
    for v in payload.get("variables", []) or []:
        if v.get("code") == "Year":
            years = list(v.get("valueTexts") or [])
            break

    if years:
        _year_cache[table_suffix] = (now, years)
        logger.debug(f"UNECE: cached {len(years)} years for {table_suffix}")

    return years


# ---------------------------------------------------------------------------
# PxWeb-Query
# ---------------------------------------------------------------------------
async def _pxweb_query(
    client: httpx.AsyncClient,
    indicator_key: str,
    country_m49: str,
) -> list[tuple[str, float]]:
    """POST-Query gegen UNECE-Tabelle für genau einen Indikator + Land.

    Returns: list[(year_str, value)] sortiert nach Jahr aufsteigend.
    """
    spec = _TABLES[indicator_key]
    table_suffix = spec["table"]
    var_tuple = tuple(f"{c}={v}" for c, v, *_ in spec["vars"])
    cache_key = (table_suffix, country_m49, var_tuple)

    now = time.time()
    cached = _cache.get(cache_key)
    if cached and (now - cached[0]) < CACHE_TTL:
        return cached[1]

    # Year-Index-Mapping holen (parallel zu Query — aber wir brauchen das
    # Ergebnis VOR der Verarbeitung; daher seriell, mit getrenntem Cache)
    years = await _fetch_year_codes(client, table_suffix)
    if not years:
        return cached[1] if cached else []

    # Body bauen
    query = []
    for code, value, *_ in spec["vars"]:
        query.append({
            "code": code,
            "selection": {"filter": "item", "values": [value]},
        })
    query.append({
        "code": "Country",
        "selection": {"filter": "item", "values": [country_m49]},
    })
    body = {"query": query, "response": {"format": "json"}}

    url = f"{BASE_URL}/{table_suffix}"
    try:
        resp = await client.post(
            url,
            json=body,
            headers={"Accept": "application/json"},
            timeout=TIMEOUT_S,
        )
    except (httpx.TimeoutException, httpx.RequestError) as e:
        logger.debug(f"UNECE: post failed {indicator_key}/{country_m49}: {e}")
        return cached[1] if cached else []

    if resp.status_code != 200:
        logger.debug(
            f"UNECE: {indicator_key}/{country_m49} HTTP {resp.status_code}"
        )
        return cached[1] if cached else []

    try:
        payload = resp.json()
    except Exception as e:
        logger.debug(f"UNECE: JSON parse failed: {e}")
        return cached[1] if cached else []

    rows: list[tuple[str, float]] = []
    n_years = len(years)
    for entry in payload.get("data", []) or []:
        key = entry.get("key", [])
        vals = entry.get("values", [])
        if not key or not vals:
            continue
        # Year ist immer der letzte Key-Eintrag (PxWeb-Konvention)
        try:
            year_idx = int(key[-1])
        except (TypeError, ValueError):
            continue
        if year_idx < 0 or year_idx >= n_years:
            continue
        year_str = years[year_idx]
        raw = vals[0]
        if raw in (None, "-", "..", "...", "", ":"):
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        rows.append((year_str, value))

    rows.sort(key=lambda r: r[0])
    _cache[cache_key] = (now, rows)
    logger.info(
        f"UNECE: fetched {indicator_key}/{country_m49} → {len(rows)} year-rows"
    )
    return rows


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _format_de_int(v: float) -> str:
    """Format integer with German thousands separator (10000 → '10.000')."""
    return f"{int(round(v)):,}".replace(",", ".")


def _format_value(v: float, unit_de: str) -> str:
    """Format value with unit. Treat counts as integer, km/Pkm same."""
    return f"{_format_de_int(v)} {unit_de}"


def _build_result(
    indicator_key: str,
    country_m49: str,
    country_iso3: str,
    country_name: str,
    rows: list[tuple[str, float]],
) -> dict | None:
    if not rows:
        return None

    spec = _TABLES[indicator_key]
    ind_de = spec["ind_de"]
    unit_de = spec["unit_de"]
    table_suffix = spec["table"]
    pivot_path = spec["pivot_path"]

    latest_year, latest_val = rows[-1]
    display = (
        f"{country_name}: {ind_de} "
        f"{latest_year}: {_format_value(latest_val, unit_de)}"
    )

    # Trend-Beschreibung
    description_parts: list[str] = []
    if len(rows) >= 6:
        prev_year, prev_val = rows[-6]
        if prev_val > 0:
            change_pct = (latest_val - prev_val) / prev_val * 100.0
            arrow = "+" if change_pct >= 0 else ""
            change_str = f"{arrow}{change_pct:.1f}".replace(".", ",") + " %"
            description_parts.append(
                f"Trend {prev_year} → {latest_year}: "
                f"{_format_value(prev_val, unit_de)} → "
                f"{_format_value(latest_val, unit_de)} ({change_str})."
            )
    if len(rows) >= 2:
        first_year, first_val = rows[0]
        if first_val > 0 and first_year != latest_year:
            factor = latest_val / first_val
            factor_str = f"{factor:.1f}".replace(".", ",")
            description_parts.append(
                f"Langfristig seit {first_year}: {_format_value(first_val, unit_de)} "
                f"→ {_format_value(latest_val, unit_de)} (Faktor {factor_str}x)."
            )

    description_parts.append(
        f"Quelle: {SOURCE_LABEL} — Sustainable Transport Division. "
        f"Daten Public Domain / UN-Open-Data. UNECE veröffentlicht jährlich; "
        f"erhoben bei nationalen Statistikämtern der 56 UNECE-Mitgliedsstaaten. "
        f"Bei Diskrepanzen zu nationalen Quellen (Statistik Austria / Eurostat) "
        f"Methodik-Definitionen beachten."
    )

    # Pivot-URL (UI-tauglicher Quellen-Link)
    pivot_url = f"{PIVOT_BASE}__{pivot_path}"

    return {
        "indicator_name": f"{ind_de}, {country_name}",
        "indicator": f"{spec['indicator']}_{country_iso3.lower()}",
        "country": country_iso3,
        "country_name": country_name,
        "year": latest_year,
        "value": latest_val,
        "display_value": display,
        "description": " ".join(description_parts),
        "url": pivot_url,
        "source": SOURCE_LABEL,
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_unece(analysis: dict) -> dict:
    """UNECE-Live-Lookup für Verkehrs-/Mobilitäts-Claims."""
    empty = {
        "source": "UNECE",
        "type": "transport_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_unece(matchable):
        return empty

    country = _detect_country(matchable)
    indicators = _detect_indicators(matchable)

    if not country:
        logger.debug("UNECE: no country detected, skipping")
        return empty
    if not indicators:
        logger.debug("UNECE: no indicator detected, skipping")
        return empty

    country_m49, country_iso3, country_name = country

    results: list[dict] = []
    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            tasks = [
                _pxweb_query(client, ind_key, country_m49)
                for ind_key in indicators
            ]
            rows_list = await asyncio.gather(*tasks, return_exceptions=True)
        for ind_key, rows_or_err in zip(indicators, rows_list):
            if isinstance(rows_or_err, Exception):
                logger.debug(f"UNECE: indicator {ind_key} errored: {rows_or_err}")
                continue
            rows = rows_or_err or []
            r = _build_result(
                ind_key, country_m49, country_iso3, country_name, rows
            )
            if r is not None:
                results.append(r)
            if len(results) >= 3:
                break
    except Exception as e:
        logger.warning(f"UNECE: search failed: {e}")
        return empty

    return {
        "source": "UNECE",
        "type": "transport_data",
        "results": results,
    }
