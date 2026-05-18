"""ESPON Database Portal — EU-Territorial-Indikatoren für NUTS-2-Regionen.

ESPON (European Observation Network for Territorial Development and Cohesion)
ist ein EU-Programm (Interreg) mit Sitz in Luxemburg. Es publiziert
vergleichende Daten zu territorialer Kohäsion, Stadt-Land-Disparitäten,
Demografie und Wirtschafts-Performance auf NUTS-2-/NUTS-3-Ebene für
EU-27 + EFTA + Kandidatenländer.

Quelle: https://database.espon.eu/

Pattern-Hintergrund:
- ESPON Database Portal liefert CSV/XLSX (Bulk-Downloads) und Tile-Maps,
  aber KEINEN klassischen JSON-REST-Endpoint. Ein Live-Crawl wäre fragil
  (Drupal-7-Frontend mit Session-Cookies). Daher: kuratierter JSON-Pack
  mit den hochfrequenten NUTS-2-Aggregaten (AT-9, DE-Vergleichsregionen,
  EU-27-Referenz) + 4 zentrale ESPON-Projekt-Highlights.
- Polite-Client wird trotzdem importiert: Falls zukünftig ein einzelner
  CSV-Indikator nachgeladen werden soll, ist der Stack konsistent zum
  Rest der Backend-Services.

Use-Cases (Trigger-Beispiele):
- "ESPON klassifiziert das Waldviertel als Inner Periphery"
- "NUTS-2 Wien hat höchstes BIP/Kopf in Österreich"
- "EU-Regional-Daten Oberbayern vs Wien"
- "Territoriale Kohäsion EU — Stadt-Land-Gefälle"

WICHTIG — Politische Guardrails (memory/project_political_guardrails.md):
- ESPON-Daten sind deskriptiv (BIP/Kopf-PPS, Bevölkerung, Cluster-
  Klassifikation). Keine Bewertung von Politiken oder Parteien.
- Caveat-Section liefert Mess-Hinweise (PPS-Index ≠ Wohlstand pro Person,
  Pendler-Effekte, NUTS-Grenzen ≠ funktionale Räume).

Lizenz: Frei nutzbar im Rahmen des ESPON-2030-Programms (EU-Förderung).
"""

# WIRING für main.py (nicht hier auto-applizieren — gemäß Auftrag):
# from services.espon import search_espon, claim_mentions_espon_cached
# if claim_mentions_espon_cached(claim):
#     tasks.append(cached("ESPON", search_espon, analysis))
#     queried_names.append("ESPON")
# + reranker-Whitelist: "ESPON" in services/reranker.py:_LIVE_API_SOURCES

from __future__ import annotations

import json
import logging
import os
import time

from services._http_polite import polite_client  # noqa: F401 — reserved for future CSV-bulk fetch

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "espon.json",
)

CACHE_TTL_S = 24 * 3600  # 24h

_static_cache: dict | None = None


# ---------------------------------------------------------------------------
# Trigger-Lexikon
# ---------------------------------------------------------------------------
# Direkt-Trigger: namentliche Erwähnung von ESPON oder seinen Projekten
_DIRECT_TERMS = (
    "espon",
    "espon-database", "espon database",
    "espon cohes", "espon escape", "espon demifer", "espon locate",
    "european observation network",
    # Territoriale-Kohäsion-Phrasen sind ESPON-Kernthemen → Direkt-Trigger
    "territoriale kohäsion", "territoriale kohaesion",
    "territoriale cohesion", "territorial cohesion",
)

# NUTS-Codes / Konzepte
_NUTS_TERMS = (
    "nuts-2", "nuts 2", "nuts2",
    "nuts-3", "nuts 3", "nuts3",
    "functional urban area", "fua",
    "regional cohesion eu", "regionale kohäsion eu",
    "inner periphery", "innere peripherie",
    "stadt-land-gefälle", "stadt-land-gefaelle", "rural-urban divide",
    "schrumpfende region", "shrinking region",
)

# AT-NUTS-2 Regionalnamen + Code-Aliasse (Triple-Pack-Erweiterung AT)
_AT_NUTS2_TERMS = (
    "burgenland", "at11",
    "niederösterreich", "niederoesterreich", "at12",
    "wien", "at13",
    "kärnten", "kaernten", "at21",
    "steiermark", "at22",
    "oberösterreich", "oberoesterreich", "at31",
    "salzburg", "at32",
    "tirol", "at33",
    "vorarlberg", "at34",
)

# DE-NUTS-2 Vergleichsregionen
_DE_NUTS2_TERMS = (
    "oberbayern", "de21",
    "düsseldorf", "duesseldorf", "dea1",
    "köln", "koeln", "dea2",
    "bayern", "nrw", "nordrhein-westfalen",
)

# EU-Vergleichs-Kontext
_EU_CONTEXT_TERMS = (
    "eu-vergleich", "eu vergleich",
    "eu-regional", "eu regional",
    "regional disparities eu", "regionale disparitäten eu",
    "bip pro kopf region", "gdp per capita region",
    "pps eu-27", "pps eu 27", "kaufkraftstandard region",
)


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_espon(claim_lc: str) -> bool:
    """Conservative Trigger:
    1. Direkter ESPON-Term → True
    2. NUTS-Term + (AT-/DE-Region ODER EU-Kontext) → True
    3. AT-/DE-Region + EU-Regional-Kontext → True
    """
    if not claim_lc:
        return False

    # 1. Direkt-Match
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    has_nuts = any(t in claim_lc for t in _NUTS_TERMS)
    has_at_region = any(t in claim_lc for t in _AT_NUTS2_TERMS)
    has_de_region = any(t in claim_lc for t in _DE_NUTS2_TERMS)
    has_eu_ctx = any(t in claim_lc for t in _EU_CONTEXT_TERMS)

    # 2. NUTS-Term + Region oder EU-Kontext
    if has_nuts and (has_at_region or has_de_region or has_eu_ctx):
        return True

    # 3. AT-/DE-Region + EU-Regional-Kontext (z.B. "Wien BIP/Kopf im EU-Vergleich")
    if (has_at_region or has_de_region) and has_eu_ctx:
        return True

    return False


# Modul-Level 24h-Trigger-Cache (cepii-style)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_espon_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_espon(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    # Cache-Hygiene
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Static load (24h cache via module-state)
# ---------------------------------------------------------------------------
_static_loaded_at: float = 0.0


def _load_static_json() -> dict | None:
    """Lädt espon.json einmalig (24h-TTL für Hot-Reload-Resilienz)."""
    global _static_cache, _static_loaded_at
    now = time.time()
    if _static_cache is not None and (now - _static_loaded_at) < CACHE_TTL_S:
        return _static_cache
    try:
        with open(STATIC_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "data" not in data:
            logger.warning("espon.json missing 'data' key")
            return None
        _static_cache = data
        _static_loaded_at = now
        nuts_count = len((data.get("data") or {}).get("nuts2_regions") or {})
        logger.info(f"ESPON data loaded: {nuts_count} NUTS-2-Regionen")
        return _static_cache
    except FileNotFoundError:
        logger.warning(f"espon.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"espon.json load failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Region detection
# ---------------------------------------------------------------------------
def _detect_regions(claim_lc: str, nuts_dict: dict) -> list[str]:
    """Findet NUTS-2-Codes, die im Claim erwähnt werden (Name oder Code).
    Reihenfolge: erstes Auftreten zuerst. Maximal 3 Treffer.
    """
    if not claim_lc:
        return []
    found: list[tuple[int, str]] = []
    for code, info in nuts_dict.items():
        if code == "EU27":
            continue  # Referenz wird separat angefügt
        name_lc = (info.get("name") or "").lower()
        # Versuche zuerst Name-Match (länger, spezifischer)
        candidates = [name_lc, code.lower()]
        # Umlaut-Aliasse hinzufügen
        if "ö" in name_lc:
            candidates.append(name_lc.replace("ö", "oe"))
        if "ü" in name_lc:
            candidates.append(name_lc.replace("ü", "ue"))
        if "ä" in name_lc:
            candidates.append(name_lc.replace("ä", "ae"))
        for cand in candidates:
            if not cand:
                continue
            idx = claim_lc.find(cand)
            if idx >= 0:
                found.append((idx, code))
                break
    found.sort(key=lambda t: t[0])
    # Duplikate raus, Reihenfolge beibehalten
    seen: set[str] = set()
    ordered: list[str] = []
    for _, code in found:
        if code not in seen:
            ordered.append(code)
            seen.add(code)
        if len(ordered) >= 3:
            break
    return ordered


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _build_region_result(code: str, info: dict, src_url: str, src_label: str) -> dict:
    pop = info.get("population_thousand")
    pop_str = f"{pop:.0f} Tsd. Einw." if isinstance(pop, (int, float)) else "k.A."
    gdp = info.get("gdp_per_capita_pps_eu27_100")
    gdp_str = f"{gdp} (EU-27 = 100)" if gdp is not None else "k.A."
    note = info.get("note") or ""

    display = (
        f"ESPON-NUTS-2 {code} {info.get('name', '')}: "
        f"Bevölkerung {pop_str}; BIP/Kopf-PPS-Index {gdp_str}. "
        f"{note}"
    )[:700]

    return {
        "indicator_name": (
            f"ESPON {code} {info.get('name', '')} — "
            f"Territoriale Indikatoren"
        ),
        "indicator": f"espon_nuts2_{code.lower()}",
        "country": info.get("country", ""),
        "country_name": info.get("name", ""),
        "year": "2022",
        "topic": "espon_territorial",
        "value": gdp,
        "display_value": display,
        "description": (
            "ESPON-Aggregat-Daten zur NUTS-2-Region. BIP/Kopf in "
            "Kaufkraftstandards (PPS) als Index relativ zu EU-27 = 100. "
            "Bevölkerungsstand letzter verfügbarer ESPON-COHES-Bericht "
            "(typisch 2-3 Jahre Lag). Deskriptive Territorialstatistik — "
            "ESPON bewertet keine Politiken oder Parteien."
        ),
        "url": src_url,
        "source": src_label,
    }


def _build_eu_reference(nuts_dict: dict, src_url: str, src_label: str) -> dict | None:
    """Anhängbare EU-27-Referenz-Zeile für Kontext."""
    eu = nuts_dict.get("EU27")
    if not eu:
        return None
    return {
        "indicator_name": "ESPON-Referenz: EU-27 Durchschnitt (PPS = 100)",
        "indicator": "espon_nuts2_eu27",
        "country": "EU",
        "country_name": "EU-27",
        "year": "2022",
        "topic": "espon_territorial",
        "value": 100,
        "display_value": (
            "EU-27 Durchschnitt = 100 (PPS-Referenz). "
            f"Bevölkerung EU-27 gesamt: ~{eu.get('population_thousand'):.0f} "
            f"Tsd. Einw. Kontext-Zeile für regionale ESPON-Vergleiche."
        ),
        "description": eu.get("note") or "",
        "url": src_url,
        "source": src_label,
    }


def _build_projects_entry(projects: list, src_url: str, src_label: str) -> dict:
    """Ein zusammenfassender Eintrag über die wichtigsten ESPON-Projekte."""
    lines = []
    for p in (projects or [])[:4]:
        lines.append(
            f"{p.get('code', '')} ({p.get('topic', '')}): "
            f"{p.get('headline', '')}"
        )
    display = " | ".join(lines)[:700] or "ESPON-Projekte: keine Daten."
    return {
        "indicator_name": "ESPON-Programm — Zentrale Projekte (Überblick)",
        "indicator": "espon_projects_overview",
        "country": "EU",
        "country_name": "EU-27",
        "year": "2021-2027",
        "topic": "espon_program",
        "value": None,
        "display_value": display,
        "description": (
            "ESPON-2030 ist ein EU-Interreg-Programm (Sitz Luxemburg) mit "
            "Fokus auf vergleichende Territorialstatistik für NUTS-2-/NUTS-3-"
            "Regionen. Hauptthemen: COHES (Kohäsion), ESCAPE (Shrinking "
            "Regions), DEMIFER (Bevölkerungsprojektion), LOCATE (Industrie-"
            "Standorte)."
        ),
        "url": src_url,
        "source": src_label,
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_espon(analysis: dict) -> dict:
    """Liefert ESPON-NUTS-2-Aggregate für den Claim.

    Strategy:
    1. Trigger-Check. Nein → empty.
    2. Statisches Pack laden. Fehlt → empty.
    3. Region-Detection: Welche NUTS-2 sind im Claim genannt?
       - 0 erkannt → Generischer Projekt-Überblick + EU-Referenz
       - 1-3 erkannt → Regions-Zeilen + EU-Referenz
    4. Bei Direkt-Trigger ohne Region: Projekte + EU-Referenz.
    """
    empty = {
        "source": "ESPON",
        "type": "territorial_indicators",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_espon(matchable):
        return empty

    data = _load_static_json()
    if not data:
        return empty

    payload = data.get("data") or {}
    nuts = payload.get("nuts2_regions") or {}
    projects = payload.get("espon_projects_brief") or []
    src_url = data.get("source_url") or "https://database.espon.eu/"
    src_label = data.get("source_label") or "ESPON Database Portal"

    detected = _detect_regions(matchable, nuts)

    results: list[dict] = []

    if detected:
        for code in detected:
            info = nuts.get(code)
            if not info:
                continue
            results.append(_build_region_result(code, info, src_url, src_label))
        # EU-27-Referenzlinie anhängen (Kontext)
        ref = _build_eu_reference(nuts, src_url, src_label)
        if ref:
            results.append(ref)
    else:
        # Direkt-Trigger ohne Region oder generischer Kohäsions-Claim
        results.append(_build_projects_entry(projects, src_url, src_label))
        ref = _build_eu_reference(nuts, src_url, src_label)
        if ref:
            results.append(ref)

    if not results:
        return empty

    logger.info(
        f"ESPON: {len(results)} Aggregate (regions={detected or '—'})"
    )
    return {
        "source": "ESPON",
        "type": "territorial_indicators",
        "results": results,
    }
