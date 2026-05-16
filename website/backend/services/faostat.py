"""FAOSTAT Live-API Connector — UN Food and Agriculture Organization.

Datenquelle: FAOSTAT REST-API der Food and Agriculture Organization of
the United Nations. 245+ Länder/Aggregate, Daten ab 1961, ~20.000
Indikatoren über mehrere Domains:

  - QCL  : Crops and Livestock Production (Mengen, Erträge, Flächen)
  - FBSH : Food Balance Sheets — Nahrungsverfügbarkeit pro Kopf
  - EM   : Emissions from Agriculture (CO2, CH4, N2O)
  - TCL  : Trade — Crops/Livestock (Ex-/Import)
  - RL   : Land Use (Ackerland, Weide, Wald)
  - RP   : Pesticides Use
  - RFN  : Fertilizers Nutrient

API-Endpoint:
  https://fenixservices.fao.org/faostat/api/v1/en/data/{domain_code}
  ?area={area_code}&item={item_code}&year={range}&format=json

Free, kein API-Key. Höflich = 1 req/s. Antwort: JSON mit "data"-Array.

Lizenz: CC BY-NC-SA 3.0 IGO — Evidora ist non-commercial → kompatibel.

Trigger: globale Lebensmittel-/Landwirtschafts-Claims (Welt-Hunger,
Fleisch-Konsum, Getreide-Produktion, Pestizid-/Dünger-Einsatz, FAOSTAT
explizit). Achtung: AT-spezifische Agrar-Claims werden NICHT von FAOSTAT
beantwortet — dafür Grüner Bericht / AMA.

Politische Guardrails: Bei Welt-Hunger/Ernährungs-Themen werden nur
Fakten ausgegeben; keine Politik-Kritik, keine Regierungs-Anklagen.
"""

# WIRING für main.py:
# from services.faostat import search_faostat, claim_mentions_faostat_cached
# if claim_mentions_faostat_cached(claim):
#     tasks.append(cached("FAOSTAT", search_faostat, analysis))
#     queried_names.append("FAOSTAT")

from __future__ import annotations

import logging
import time
from functools import lru_cache
from urllib.parse import urlencode

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------
FAOSTAT_BASE = "https://fenixservices.fao.org/faostat/api/v1/en/data"
FAOSTAT_PORTAL = "https://www.fao.org/faostat/en/#data"

CACHE_TTL = 24 * 3600  # 24 h
TIMEOUT_S = 25.0
MAX_RECORDS = 5  # Max Records pro search_faostat-Call

# Cache: key=(domain, area, item, year_range) → (timestamp, list[record])
_cache: dict[tuple, tuple[float, list[dict]]] = {}

# ---------------------------------------------------------------------------
# Domain-Mapping — FAOSTAT-Domain-Codes mit deutschen Trigger-Heuristiken
# ---------------------------------------------------------------------------
_DOMAIN_QCL = "QCL"      # Crops & Livestock Production
_DOMAIN_FBSH = "FBSH"    # Food Balance Sheets Historic
_DOMAIN_EM = "EM"        # Emissions
_DOMAIN_TCL = "TCL"      # Trade Crops & Livestock
_DOMAIN_RL = "RL"        # Land Use
_DOMAIN_RP = "RP"        # Pesticides Use
_DOMAIN_RFN = "RFN"      # Fertilizers by Nutrient

# Domain-Label für display_value-Anzeige
_DOMAIN_LABEL = {
    _DOMAIN_QCL: "Crops & Livestock Production",
    _DOMAIN_FBSH: "Food Balance",
    _DOMAIN_EM: "Emissionen Landwirtschaft",
    _DOMAIN_TCL: "Handel Agrarprodukte",
    _DOMAIN_RL: "Land Use",
    _DOMAIN_RP: "Pestizide",
    _DOMAIN_RFN: "Düngemittel",
}

# Domain-Begriff-Mapping → erstes Match gewinnt
_DOMAIN_TERMS: list[tuple[str, tuple[str, ...]]] = [
    (_DOMAIN_EM, (
        "emission landwirtschaft", "emissionen landwirtschaft",
        "treibhausgas landwirtschaft", "co2 landwirtschaft",
        "methan landwirtschaft", "lachgas landwirtschaft",
    )),
    (_DOMAIN_RP, (
        "pestizid", "pestizide", "herbizid", "fungizid",
        "pflanzenschutzmittel weltweit",
    )),
    (_DOMAIN_RFN, (
        "düngemittel", "dünger weltweit", "stickstoff-dünger",
        "phosphor-dünger", "kunstdünger",
    )),
    (_DOMAIN_RL, (
        "anbaufläche", "anbau-fläche", "ackerland weltweit",
        "weidefläche", "agrarfläche weltweit",
    )),
    (_DOMAIN_TCL, (
        "agrar-export", "agrar-import", "agrarhandel",
        "lebensmittel-export", "lebensmittel-import",
    )),
    (_DOMAIN_FBSH, (
        "lebensmittel-verschwendung", "food waste global",
        "kalorien pro kopf", "nahrungsverfügbarkeit",
        "welt-ernährung", "welt-hunger", "hunger weltweit",
        "ernährungslage global", "globale ernährung",
        "fleisch-konsum", "fleischkonsum",
        "fisch-konsum", "fischkonsum",
        "pro kopf konsum", "pro-kopf-konsum",
    )),
    (_DOMAIN_QCL, (
        "getreide-produktion", "weizen-produktion",
        "fleisch-produktion", "milch-produktion",
        "tierhaltung weltweit", "anbau weltweit",
    )),
]

# ---------------------------------------------------------------------------
# Area-Codes — FAOSTAT verwendet eigene numerische M49-/FAO-Codes
# ---------------------------------------------------------------------------
_AREA_CODES: dict[str, tuple[int, str]] = {
    # de_keyword → (FAOSTAT-area-code, country_name)
    "österreich": (11, "Österreich"),
    "austria": (11, "Österreich"),
    "deutschland": (79, "Deutschland"),
    "germany": (79, "Deutschland"),
    "schweiz": (211, "Schweiz"),
    "switzerland": (211, "Schweiz"),
    "italien": (106, "Italien"),
    "italy": (106, "Italien"),
    "frankreich": (68, "Frankreich"),
    "france": (68, "Frankreich"),
    "spanien": (203, "Spanien"),
    "spain": (203, "Spanien"),
    "polen": (173, "Polen"),
    "poland": (173, "Polen"),
    "usa": (231, "USA"),
    "vereinigte staaten": (231, "USA"),
    "united states": (231, "USA"),
    "china": (351, "China"),
    "indien": (100, "Indien"),
    "india": (100, "Indien"),
    "russland": (185, "Russland"),
    "russia": (185, "Russland"),
    "brasilien": (21, "Brasilien"),
    "brazil": (21, "Brasilien"),
    "ukraine": (230, "Ukraine"),
    "japan": (110, "Japan"),
    # Welt-Aggregate
    "weltweit": (5000, "Welt"),
    "global": (5000, "Welt"),
    "world": (5000, "Welt"),
    "europäische union": (5707, "EU-27"),
    "european union": (5707, "EU-27"),
    "eu-27": (5707, "EU-27"),
    "afrika": (5100, "Afrika"),
    "africa": (5100, "Afrika"),
}

_DEFAULT_AREA = (5000, "Welt")

# ---------------------------------------------------------------------------
# Item-Codes — wichtige Crops/Tiere (Mapping de-Keyword → FAOSTAT-Item)
# ---------------------------------------------------------------------------
_ITEM_CODES: dict[str, tuple[int, str]] = {
    # Getreide
    "weizen": (15, "Weizen"),
    "wheat": (15, "Weizen"),
    "mais": (56, "Mais"),
    "maize": (56, "Mais"),
    "corn": (56, "Mais"),
    "reis": (27, "Reis"),
    "rice": (27, "Reis"),
    "gerste": (44, "Gerste"),
    "barley": (44, "Gerste"),
    "roggen": (71, "Roggen"),
    "rye": (71, "Roggen"),
    "hafer": (75, "Hafer"),
    "oats": (75, "Hafer"),
    # Hülsenfrüchte / Öl
    "soja": (236, "Sojabohnen"),
    "soybean": (236, "Sojabohnen"),
    "sonnenblume": (267, "Sonnenblumen"),
    "raps": (270, "Raps"),
    "rapeseed": (270, "Raps"),
    # Knollen
    "kartoffel": (116, "Kartoffeln"),
    "potato": (116, "Kartoffeln"),
    "zuckerrübe": (157, "Zuckerrüben"),
    # Tiere/Fleisch (Bestände)
    "rind": (867, "Rinder"),
    "cattle": (867, "Rinder"),
    "schwein": (1035, "Schweine"),
    "pig": (1035, "Schweine"),
    "schaf": (976, "Schafe"),
    "sheep": (976, "Schafe"),
    "huhn": (1057, "Hühner"),
    "chicken": (1057, "Hühner"),
    "geflügel": (1057, "Hühner"),
    # Fleisch-Produkte (für FBS-Konsum)
    "rindfleisch": (867, "Rinder"),
    "schweinefleisch": (1035, "Schweine"),
    # Milch / Eier
    "milch": (882, "Kuhmilch"),
    "milk": (882, "Kuhmilch"),
    "ei": (1062, "Hühnereier"),
    "eier": (1062, "Hühnereier"),
    "eggs": (1062, "Hühnereier"),
    # Sonstige
    "fisch": (2960, "Fisch (Aggregat)"),
    "fish": (2960, "Fisch (Aggregat)"),
    "kaffee": (656, "Kaffee"),
    "coffee": (656, "Kaffee"),
    "zucker": (157, "Zuckerrüben"),  # Approx; Zuckerrohr wäre 156
}

# ---------------------------------------------------------------------------
# Trigger-Terms
# ---------------------------------------------------------------------------
_FAOSTAT_PRIMARY = (
    "fao", "faostat", "food and agriculture organization",
)

_FAOSTAT_TOPIC_TERMS = (
    "welt-hunger", "welthunger", "welt-ernährung", "welternährung",
    "globale lebensmittel", "globale ernährung", "global food",
    "globaler hunger", "hunger weltweit", "ernährung weltweit",
    "anbau-fläche", "anbaufläche", "anbau weltweit",
    "fleisch-konsum", "fleischkonsum",
    "fisch-konsum", "fischkonsum",
    "getreide-produktion", "getreideproduktion",
    "weizen-produktion", "fleisch-produktion",
    "milch-produktion",
    "pestizid-einsatz", "pestizide weltweit",
    "düngemittel weltweit", "dünger weltweit",
    "lebensmittel-verschwendung", "food waste",
    "tierhaltung weltweit", "agrar-export", "agrar-import",
    "agrarhandel", "ackerland weltweit",
)

# AT-Marker — wenn der Claim ausschließlich AT-fokussiert ist, NICHT
# trigger; dafür ist der Grüner-Bericht-/AMA-Service zuständig.
_AT_EXCLUSIVE_MARKERS = (
    "österreichische landwirtschaft", "austrias landwirtschaft",
    "grüner bericht", "ama gütesiegel", "ama-gütesiegel",
    "österreichische bauern", "österreichischer bauer",
)


def _claim_mentions_faostat(claim_lc: str) -> bool:
    """Trigger-Check: Claim ist global/welt-orientiert UND Agrar/
    Lebensmittel-Thema, oder nennt FAOSTAT explizit.

    NICHT triggern, wenn der Claim sich AT-spezifisch auf den Grünen
    Bericht oder AMA bezieht — dafür gibt es eigene Services.
    """
    if not claim_lc:
        return False

    # Explizite FAO/FAOSTAT-Nennung schlägt alle Filter
    if any(t in claim_lc for t in _FAOSTAT_PRIMARY):
        return True

    # AT-exklusiver Claim → andere Quelle ist besser geeignet
    if any(t in claim_lc for t in _AT_EXCLUSIVE_MARKERS):
        return False

    # Globaler/welter Agrar-Claim?
    if any(t in claim_lc for t in _FAOSTAT_TOPIC_TERMS):
        return True

    # Kombinierter Trigger: Produkt + globaler Marker
    has_product = any(t in claim_lc for t in _ITEM_CODES.keys())
    has_global = any(t in claim_lc for t in (
        "weltweit", "global", "world", "international",
        "alle länder", "länder-vergleich",
    ))
    if has_product and has_global:
        return True

    return False


@lru_cache(maxsize=512)
def claim_mentions_faostat_cached(claim: str) -> bool:
    """LRU-gecachter Wrapper für Trigger-Check."""
    return _claim_mentions_faostat((claim or "").lower())


# ---------------------------------------------------------------------------
# Resolver: Claim → (Domain, Area, Item)
# ---------------------------------------------------------------------------
def _resolve_domain(claim_lc: str) -> str:
    """Bestimme die wahrscheinlichste FAOSTAT-Domain aus dem Claim-Text.

    Fällt auf QCL (Crops/Livestock) zurück — das ist die nützlichste
    Default-Domain für allgemeine "wie viel produziert X von Y"-Claims.
    """
    for domain, terms in _DOMAIN_TERMS:
        if any(t in claim_lc for t in terms):
            return domain
    return _DOMAIN_QCL


def _resolve_area(claim_lc: str) -> tuple[int, str]:
    """Bestimme FAOSTAT-Area-Code aus dem Claim. Default = Welt (5000)."""
    for keyword, (code, name) in _AREA_CODES.items():
        if keyword in claim_lc:
            return code, name
    return _DEFAULT_AREA


def _resolve_item(claim_lc: str) -> tuple[int, str] | None:
    """Bestimme FAOSTAT-Item-Code aus dem Claim. None = kein spezifisches
    Item identifizierbar."""
    for keyword, (code, name) in _ITEM_CODES.items():
        # Word-Boundary-light: Keyword muss als Substring auftreten,
        # bei sehr kurzen Keywords (≤4 Zeichen) prüfen wir zusätzlich
        # auf umgebende Wortgrenzen, um Fehl-Matches ("ei" in "Schweden")
        # zu vermeiden.
        if len(keyword) <= 4:
            padded_claim = f" {claim_lc} "
            for delim in (" ", ",", ".", ";", ":", "-", "/"):
                if f"{delim}{keyword}{delim}" in padded_claim or \
                   f"{delim}{keyword}" in padded_claim or \
                   f"{keyword}{delim}" in padded_claim:
                    return code, name
            continue
        if keyword in claim_lc:
            return code, name
    return None


def _resolve_year_range() -> str:
    """Default-Jahres-Range: letzte 3 vollständig erhobene Jahre.

    FAOSTAT-Daten haben ~1-2 Jahre Verzug; 2023 ist meist neuestes.
    """
    return "2021-2023"


# ---------------------------------------------------------------------------
# API-Fetch
# ---------------------------------------------------------------------------
def _cache_key(domain: str, area: int, item: int | None, year_range: str) -> tuple:
    return (domain, area, item or 0, year_range)


def _build_url(
    domain: str,
    area: int,
    item: int | None,
    year_range: str,
) -> str:
    params: list[tuple[str, str]] = [
        ("area", str(area)),
        ("year", year_range),
        ("format", "json"),
    ]
    if item is not None:
        params.append(("item", str(item)))
    return f"{FAOSTAT_BASE}/{domain}?{urlencode(params)}"


async def _fetch_faostat(
    domain: str,
    area: int,
    item: int | None,
    year_range: str,
) -> list[dict]:
    """HTTP-Call gegen FAOSTAT-API mit 24h-TTL-Cache.

    Returns Liste der "data"-Records (raw) oder [].
    """
    key = _cache_key(domain, area, item, year_range)
    now = time.time()
    cached = _cache.get(key)
    if cached and (now - cached[0]) < CACHE_TTL:
        logger.debug(
            f"faostat: cache hit {domain}/area={area}/item={item}/y={year_range}"
        )
        return cached[1]

    url = _build_url(domain, area, item, year_range)
    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(url, follow_redirects=True)
            if resp.status_code != 200:
                logger.info(
                    f"faostat: HTTP {resp.status_code} for {url[:120]}"
                )
                return cached[1] if cached else []
            payload = resp.json()
    except Exception as e:
        logger.info(f"faostat: fetch failed for {url[:120]}: {e}")
        return cached[1] if cached else []

    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        logger.debug(f"faostat: unexpected payload shape for {domain}")
        _cache[key] = (now, [])
        return []

    _cache[key] = (now, data)
    logger.info(
        f"faostat: fetched {domain} area={area} item={item} "
        f"year={year_range} → {len(data)} records"
    )
    return data


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _format_value_de(value, unit: str) -> str:
    """Formatiere Zahl deutsch + Einheits-Pretty-Print + Mio./Mrd.-Abkürz."""
    if value is None or value == "":
        return "—"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)

    # Abkürz für große Mengen
    abs_v = abs(v)
    if abs_v >= 1_000_000_000:
        short = f"{v / 1_000_000_000:.2f} Mrd."
    elif abs_v >= 1_000_000:
        short = f"{v / 1_000_000:.2f} Mio."
    elif abs_v >= 1_000:
        short = f"{v / 1_000:.1f} Tsd."
    else:
        short = f"{v:.2f}"

    short = short.replace(".", ",")
    # Einheits-Pretty-Print
    unit_clean = (unit or "").strip()
    if unit_clean.lower() == "tonnes":
        unit_clean = "t"
    elif unit_clean.lower() == "head":
        unit_clean = "Tiere"
    elif unit_clean.lower() in ("kg", "kg/cap"):
        pass
    return f"{short} {unit_clean}".strip()


def _record_to_result(rec: dict, domain: str, area_name: str) -> dict | None:
    """Konvertiere einen FAOSTAT-Datensatz in das Evidora-Result-Schema."""
    if not isinstance(rec, dict):
        return None

    item_name = rec.get("Item") or rec.get("item") or "—"
    element = rec.get("Element") or rec.get("element") or "—"
    unit = rec.get("Unit") or rec.get("unit") or ""
    year = rec.get("Year") or rec.get("year") or ""
    value = rec.get("Value")
    if value is None:
        return None
    flag = rec.get("Flag") or rec.get("flag") or ""

    domain_label = _DOMAIN_LABEL.get(domain, domain)
    area_code = rec.get("Area Code") or rec.get("area_code")
    iso_hint = rec.get("Area Code (M49)") or ""

    display_value = (
        f"{area_name} · {item_name} · {element} ({year}): "
        f"{_format_value_de(value, unit)}"
    )
    if flag:
        display_value += f" [Flag: {flag}]"

    indicator_name = (
        f"FAOSTAT {domain}: {element} · {item_name} · {area_name} · {year}"
    )[:300]

    description = (
        f"FAOSTAT-Daten aus '{domain_label}' Domain ({domain}). "
        f"Quelle: UN FAO Food and Agriculture Organization. "
        f"Flag-Bedeutung: 'E' = Schätzung FAO, 'I' = Imputed, "
        f"'A' = offizielle Angabe. Lizenz CC BY-NC-SA 3.0 IGO."
    )

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        numeric_value = None

    return {
        "indicator_name": indicator_name,
        "indicator": f"faostat_{domain}_{rec.get('Item Code') or rec.get('item_code') or '?'}",
        "country": str(iso_hint or area_code or area_name),
        "country_name": area_name,
        "year": str(year),
        "value": numeric_value,
        "display_value": display_value,
        "description": description,
        "url": f"{FAOSTAT_PORTAL}/{domain}",
        "source": "FAOSTAT (FAO)",
    }


def _top_records(
    raw_records: list[dict],
    max_results: int = MAX_RECORDS,
) -> list[dict]:
    """Wähle die nützlichsten Records aus: aktuelles Jahr bevorzugt,
    danach plausible "Production"-/"Yield"-Elemente, dedupliziert.
    """
    if not raw_records:
        return []

    # Stelle Produktion vor Fläche/Ertrag (Faktencheck-Relevanz)
    priority_elements = (
        "Production", "Producing Animals/Slaughtered", "Stocks",
        "Food supply quantity (kg/capita/yr)",
        "Food supply (kcal/capita/day)",
    )

    def sort_key(r: dict) -> tuple:
        elem = (r.get("Element") or r.get("element") or "").strip()
        year_val = r.get("Year") or r.get("year") or 0
        try:
            year_int = int(year_val)
        except (TypeError, ValueError):
            year_int = 0
        prio = priority_elements.index(elem) if elem in priority_elements else 99
        # Höheres Jahr besser → -year_int; niedrigere prio besser
        return (prio, -year_int)

    return sorted(raw_records, key=sort_key)[:max_results]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_faostat(analysis: dict) -> dict:
    """Live-Lookup gegen FAOSTAT für globale Agrar-/Lebensmittel-Claims.

    Strategie:
      1. Trigger-Match → frühes Empty-Return bei AT-spezifischen Claims
      2. Domain aus Claim-Inhalt bestimmen (default: QCL)
      3. Area-Code aus Claim-Inhalt (default: Welt = 5000)
      4. Item-Code falls Produkt erkennbar (optional, sonst alle Items)
      5. API-Query → Top-Records ins Evidora-Format

    Returns Dict mit ≤MAX_RECORDS Treffern oder Empty.
    """
    empty = {
        "source": "FAOSTAT",
        "type": "agriculture_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_faostat(matchable):
        return empty

    domain = _resolve_domain(matchable)
    area_code, area_name = _resolve_area(matchable)
    item_resolved = _resolve_item(matchable)
    item_code = item_resolved[0] if item_resolved else None
    year_range = _resolve_year_range()

    raw = await _fetch_faostat(domain, area_code, item_code, year_range)
    if not raw:
        logger.info(
            f"faostat: 0 records for domain={domain} area={area_code} "
            f"item={item_code} year={year_range}"
        )
        return empty

    top = _top_records(raw)
    results: list[dict] = []
    for rec in top:
        r = _record_to_result(rec, domain, area_name)
        if r:
            results.append(r)

    if not results:
        return empty

    logger.info(
        f"faostat: {len(results)} Treffer (domain={domain}, area={area_name})"
    )
    return {
        "source": "FAOSTAT",
        "type": "agriculture_data",
        "results": results,
    }
