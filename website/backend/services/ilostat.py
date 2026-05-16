"""ILOSTAT — International Labour Organization Statistics.

ILOSTAT (https://ilostat.ilo.org/) ist die globale Referenz-Datenbank der
ILO für Arbeitsmarkt-Indikatoren: Arbeitslosigkeit, Erwerbsbeteiligung,
Lohnniveau, Working Poor, Kinderarbeit, Gender Pay Gap, Arbeitsschutz.

Für Evidora ergänzt der Service die DACH-Quellen (Statistik Austria,
destatis, WIFO/IHS) um die WELT-Sicht: globale Arbeitslosenquote,
Kinderarbeits-Anteile, Working Poor weltweit, ILO-Lohnstatistiken.

API-Strategie (träge-resistent):
1. PRIMÄR: DBnomics-Aggregator (api.db.nomics.world/v22/series/ILO/…)
   — schnell, identisch lizenziert, gleiche Roh-Daten via SDMX-Mirror
2. FALLBACK: Statische ILO-Indikator-Codes aus der eingebauten Mapping-
   Tabelle, falls DBnomics auch ausfällt.

ILO direkt (https://www.ilo.org/sdmx/rest/data/…) ist BEKANNTERMASSEN
träge (oft >10 s, manchmal 502/504). Wir versuchen es daher nur, wenn
DBnomics gar nichts liefert — und mit kurzem Timeout.

Lizenz: CC BY 4.0 (ILO) — Evidora-kompatibel.

Trigger-Strategie:
1. Direkt-Trigger: "ilo" / "ilostat" / "internationale arbeitsorganisation"
2. Composite (globale Arbeit): Arbeit-Indikator + Welt-Kontext
3. AT/DE-Hard-Skip wenn rein DACH-Kontext (Statistik Austria/destatis
   sind dafür zuständig). Wenn aber "Vergleich Österreich vs. Welt"
   → trotzdem triggern.

WICHTIG — Politische Guardrails (memory/project_political_guardrails.md):
- Nur deskriptive Werte, KEINE Bewertung
- Keine Schuldzuweisung an Regierungen/Branchen
- ILO-Schätzungen sind modelliert — kennzeichnen ("ILO-Schätzung")
"""

# WIRING für main.py:
# from services.ilostat import search_ilostat, claim_mentions_ilostat_cached
# if claim_mentions_ilostat_cached(claim):
#     tasks.append(cached("ILOSTAT", search_ilostat, analysis))
#     queried_names.append("ILOSTAT")

from __future__ import annotations

import json
import logging
import time
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------
DBNOMICS_API = "https://api.db.nomics.world/v22"
ILO_SDMX = "https://www.ilo.org/sdmx/rest/data"
CACHE_TTL_S = 24 * 3600          # 24h
TIMEOUT_DBNOMICS_S = 15.0        # DBnomics ist meist <3 s
TIMEOUT_ILO_DIRECT_S = 8.0       # ILO direkt: kurz halten, sonst Stall
MAX_RESULTS = 3
DEFAULT_DATA_URL = "https://ilostat.ilo.org/data/"

# ---------------------------------------------------------------------------
# Trigger-Begriffe
# ---------------------------------------------------------------------------
_DIRECT_TERMS = (
    "ilostat", "ilo-stat", "ilo stat",
    "internationale arbeitsorganisation",
    "international labour organization",
    "international labour organisation",
    "international labor organization",
    "ilo-bericht", "ilo bericht", "ilo report",
    "ilo-schätzung", "ilo schätzung", "ilo schaetzung",
)

# Arbeit-Indikator-Begriffe (DE/EN) — Composite-Trigger-Teil 1
_LABOR_INDICATOR_TERMS = (
    "arbeitslosigkeit", "arbeitslosenquote", "arbeitslosenrate",
    "unemployment", "unemployment rate",
    "jugendarbeitslosigkeit", "youth unemployment",
    "erwerbsquote", "erwerbsbeteiligung", "labour force participation",
    "labor force participation",
    "beschäftigung", "beschaeftigung", "employment rate",
    "working poor", "arbeitende arme", "arme erwerbstätige",
    "arme erwerbstaetige",
    "kinderarbeit", "child labour", "child labor",
    "gender pay gap", "lohnlücke", "lohnluecke", "lohngefälle",
    "lohngefaelle", "gender wage gap",
    "mindestlohn", "minimum wage",
    "lohnniveau", "wage level", "average wage", "lohnungleichheit",
    "wage inequality",
    "informelle wirtschaft", "informal economy", "informal employment",
    "arbeitsschutz", "occupational safety",
    "arbeitsunfälle", "arbeitsunfaelle", "occupational injuries",
    "zwangsarbeit", "forced labour", "forced labor",
    "moderne sklaverei", "modern slavery",
    "arbeitszeit", "working hours",
)

# Welt-Kontext — nur dann ILO triggern, wenn international gemeint ist
_GLOBAL_CONTEXT_TERMS = (
    "weltweit", "global", "international", "world",
    "globaler süden", "globaler sueden", "global south",
    "entwicklungsländer", "entwicklungslaender", "developing countries",
    "schwellenländer", "schwellenlaender", "emerging markets",
    "afrika", "africa",
    "asien", "asia",
    "lateinamerika", "latin america",
    "subsahara", "sub-sahara", "sub sahara",
    "g20", "g7", "oecd-länder", "oecd laender",
    "in jedem land", "weltbevölkerung", "weltbevoelkerung",
    "alle länder", "alle laender",
    # ILO-typische Länder (NICHT DACH)
    "indien", "india", "china", "brasilien", "brazil",
    "südafrika", "suedafrika", "south africa",
    "indonesien", "indonesia",
    "vereinte nationen", "uno", "un agency",
)

_DACH_TERMS = (
    "österreich", "oesterreich", "austria",
    "deutschland", "germany", "schweiz", "switzerland",
)

# ---------------------------------------------------------------------------
# ILO-Indikator-Codes (DBnomics-Dataset + Default-Series-Pattern für WORLD)
# Konservativ: nur Top-Indikatoren mit verifizierter DBnomics-Series.
# ---------------------------------------------------------------------------
# Format:
#   keyword: {
#       "dataset": "DBnomics-Dataset-Code",
#       "label_de": "Anzeige-Name (DE)",
#       "ref_areas": [(ISO/Code, Anzeige-Name)],
#       "dim_filters": {dim_name: code},  # weitere Dimensionen
#   }
_ILO_INDICATORS: dict[str, dict] = {
    "arbeitslosigkeit": {
        "dataset": "UNE_2EAP_SEX_AGE_GEO_RT",
        "label_de": "Arbeitslosenquote (ILO-Schätzung, 15+, %)",
        "indicator_id": "ilo_unemp_15plus",
        "dim_filters": {
            "sex": "SEX_T",
            "classif1": "AGE_YTHADULT_YGE15",
            "classif2": "GEO_COV_NAT",
        },
        "default_ref_areas": [("X01", "Welt")],
    },
    "jugendarbeitslosigkeit": {
        "dataset": "UNE_2EAP_SEX_AGE_GEO_RT",
        "label_de": "Jugend-Arbeitslosenquote (ILO-Schätzung, 15–24, %)",
        "indicator_id": "ilo_youth_unemp",
        "dim_filters": {
            "sex": "SEX_T",
            "classif1": "AGE_YTHADULT_Y15-24",
            "classif2": "GEO_COV_NAT",
        },
        "default_ref_areas": [("X01", "Welt")],
    },
    "kinderarbeit": {
        "dataset": "CLD_XCHL_SEX_AGE_STE_NB",
        "label_de": "Kinder in Kinderarbeit (ILO/UNICEF, in Tausend)",
        "indicator_id": "ilo_child_labour_count",
        "dim_filters": {
            "sex": "SEX_T",
        },
        "default_ref_areas": [("X01", "Welt")],
    },
}

# Welt-Aggregate-Codes (ILO/X-Prefix)
_WORLD_AREAS: dict[str, tuple[str, str]] = {
    "weltweit": ("X01", "Welt"),
    "global": ("X01", "Welt"),
    "world": ("X01", "Welt"),
    "afrika": ("X06", "Afrika"),
    "africa": ("X06", "Afrika"),
    "subsahara": ("X13", "Subsahara-Afrika"),
    "sub-sahara": ("X13", "Subsahara-Afrika"),
    "lateinamerika": ("X26", "Lateinamerika & Karibik"),
    "latin america": ("X26", "Lateinamerika & Karibik"),
}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _is_pure_dach_claim(claim_lc: str) -> bool:
    """True wenn Claim nur AT/DE/CH erwähnt UND keinen Welt-Kontext hat.

    Composite-Schutz: wenn 'österreich' + 'weltweit' / 'im Vergleich zu'
    → NICHT pure DACH (Vergleichs-Aussage, ILO darf feuern).
    """
    has_dach = any(t in claim_lc for t in _DACH_TERMS)
    if not has_dach:
        return False
    has_global = any(t in claim_lc for t in _GLOBAL_CONTEXT_TERMS)
    if has_global:
        return False  # Vergleichs-Aussage — kein Hard-Skip
    has_compare = any(t in claim_lc for t in (
        "im vergleich", "vergleich zu", "vergleich mit",
        "verglichen mit", "gegenüber", "gegenueber",
    ))
    return not has_compare


def _claim_mentions_ilostat(claim_lc: str) -> bool:
    """Conservative Trigger:
    1. Direkter Term ("ilostat" / "ILO") → True
    2. Arbeit-Indikator + Welt-Kontext → True
    3. Hard-Skip wenn rein DACH-Claim
    """
    if not claim_lc:
        return False

    # 1. Direkt
    if any(t in claim_lc for t in _DIRECT_TERMS):
        # auch hier: pure DACH-ILO-Erwähnung ohne Welt-Kontext NICHT
        # zwingend filtern — direkte ILO-Erwähnung ist ein klarer Wunsch.
        return True

    # Eigener "ilo"-Sonderfall: nur als isoliertes Wort triggern, damit
    # z.B. "Pilot" / "Trillonen" / "Insilico" nicht False-Positive werden.
    for needle in (" ilo ", " ilo.", " ilo,", " ilo:", " ilo;",
                   "(ilo)", " ilo)", "ilo-"):
        if needle in f" {claim_lc} ":
            return not _is_pure_dach_claim(claim_lc)

    has_indicator = any(t in claim_lc for t in _LABOR_INDICATOR_TERMS)
    has_global = any(t in claim_lc for t in _GLOBAL_CONTEXT_TERMS)

    # 2. Arbeit + Welt
    if has_indicator and has_global:
        # nicht reine DACH-Aussage (z.B. "Arbeitslosigkeit in Österreich
        # weltweit niedrig" → ist Vergleich, also OK)
        return True

    return False


# Modul-Level-Cache: (claim_lc) → (ts, result)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_ilostat_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_ilostat(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    # Cache-Hygiene
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Result-Cache (24h pro Query-Key)
# ---------------------------------------------------------------------------
_result_cache: dict[str, tuple[float, list[dict]]] = {}


def _cache_get(key: str) -> list[dict] | None:
    now = time.time()
    hit = _result_cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL_S:
        return hit[1]
    return None


def _cache_put(key: str, value: list[dict]) -> None:
    _result_cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _de_num(v) -> str:
    """1234.56 → '1.234,56' (DE-Format)."""
    if v is None:
        return "?"
    try:
        if isinstance(v, float):
            return f"{v:.2f}".replace(".", ",")
        return f"{v:,}".replace(",", ".")
    except Exception:
        return str(v)


def _pick_indicator(claim_lc: str) -> tuple[str, dict] | None:
    """Wähle den passendsten ILO-Indikator zum Claim.

    Longest-Match first, damit 'jugendarbeitslosigkeit' nicht von
    'arbeitslosigkeit' überstimmt wird. Synonyme werden via
    _INDICATOR_SYNONYMS auf den kanonischen Schlüssel gemappt.
    """
    sorted_keys = sorted(_ILO_INDICATORS.keys(), key=len, reverse=True)
    for k in sorted_keys:
        if k in claim_lc:
            return k, _ILO_INDICATORS[k]
    # Synonym-Map: oberflächliche Treffer (DE+EN) → kanonische Keys
    for syn, canonical in _INDICATOR_SYNONYMS.items():
        if syn in claim_lc and canonical in _ILO_INDICATORS:
            return canonical, _ILO_INDICATORS[canonical]
    return None


# Synonyme → kanonische _ILO_INDICATORS-Keys (zusätzlich zur Exakt-Match-Schleife)
_INDICATOR_SYNONYMS: dict[str, str] = {
    "arbeitslosenquote": "arbeitslosigkeit",
    "arbeitslosenrate": "arbeitslosigkeit",
    "unemployment rate": "arbeitslosigkeit",
    "unemployment": "arbeitslosigkeit",
    "youth unemployment": "jugendarbeitslosigkeit",
    "jugend-arbeitslosigkeit": "jugendarbeitslosigkeit",
    "child labour": "kinderarbeit",
    "child labor": "kinderarbeit",
    "child workers": "kinderarbeit",
}


def _pick_ref_areas(claim_lc: str, default_areas: list) -> list:
    """Welche Regions-Codes wollen wir? Welt ist default.

    Wenn 'afrika'/'subsahara'/'lateinamerika' im Claim, ergänze diese.
    Maximal 2 Regionen.
    """
    out: list[tuple[str, str]] = list(default_areas)
    seen = {a for a, _ in out}
    for hint, (code, name) in _WORLD_AREAS.items():
        if hint in claim_lc and code not in seen:
            out.append((code, name))
            seen.add(code)
            if len(out) >= 2:
                break
    return out[:2]


# ---------------------------------------------------------------------------
# DBnomics-Fetch (primär)
# ---------------------------------------------------------------------------
async def _fetch_dbnomics_series(
    client, dataset: str, ref_area: str, dim_filters: dict,
) -> dict | None:
    """Hole ILO-Series via DBnomics-Aggregator.

    Filter werden als JSON-Dict im ?dimensions=… Parameter URL-encoded
    übergeben (DBnomics-API).
    """
    cache_key = (
        f"ilodb::{dataset}::{ref_area}::"
        f"{json.dumps(dim_filters, sort_keys=True)}"
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached[0] if cached else None

    dims = dict(dim_filters or {})
    dims["ref_area"] = [ref_area]
    # DBnomics erwartet Listen-Werte je Dimension
    dims_norm = {k: (v if isinstance(v, list) else [v]) for k, v in dims.items()}
    dims_json = json.dumps(dims_norm, separators=(",", ":"))
    url = (
        f"{DBNOMICS_API}/series/ILO/{quote_plus(dataset)}"
        f"?dimensions={quote_plus(dims_json)}&observations=1&limit=5"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"ILOSTAT/DBnomics HTTP {resp.status_code} for "
                f"{dataset}/{ref_area}"
            )
            _cache_put(cache_key, [])
            return None
        data = resp.json()
    except Exception as e:
        logger.debug(
            f"ILOSTAT/DBnomics fetch failed {dataset}/{ref_area}: {e}"
        )
        return None

    series_docs = (data.get("series") or {}).get("docs") or []
    if not series_docs:
        _cache_put(cache_key, [])
        return None
    _cache_put(cache_key, [series_docs[0]])
    return series_docs[0]


def _latest_observation(doc: dict) -> tuple[str, float | int | None]:
    """Hole letzte nicht-NULL Beobachtung (Periode, Wert) aus DBnomics-Doc."""
    periods = doc.get("period") or []
    values = doc.get("value") or []
    if not periods or not values:
        return "—", None
    for p, v in zip(reversed(periods), reversed(values)):
        if v is not None:
            return str(p), v
    return "—", None


def _format_result(
    indicator_key: str,
    spec: dict,
    ref_area_code: str,
    ref_area_name: str,
    doc: dict,
) -> dict:
    """DBnomics-Series-Doc → Evidora-Schema."""
    period, value = _latest_observation(doc)
    label_de = spec.get("label_de") or indicator_key
    indicator_id = spec.get("indicator_id") or f"ilo_{indicator_key}"

    # Display je nach Indikator-Art
    if "_count" in indicator_id:  # Mengen-Zahl (Tausend Kinder)
        if isinstance(value, (int, float)):
            display = (
                f"ILO/UNICEF-Schätzung Kinderarbeit "
                f"{ref_area_name} {period}: "
                f"{_de_num(value)} Tausend Kinder."
            )
        else:
            display = (
                f"ILO/UNICEF-Schätzung Kinderarbeit {ref_area_name}: "
                "keine aktuellen Werte."
            )
    else:  # Quote/%-Wert
        if isinstance(value, (int, float)):
            display = (
                f"{label_de} {ref_area_name} {period}: "
                f"{_de_num(value)} % (ILO-Schätzung, "
                "modellierte Daten)."
            )
        else:
            display = (
                f"{label_de} {ref_area_name}: keine aktuellen Werte."
            )

    description = (
        f"ILO-Indikator '{label_de}', Region: {ref_area_name} "
        f"({ref_area_code}). "
        "Quelle: ILOSTAT via DBnomics-Aggregator. ILO-Werte sind "
        "modelliert (Imputation + Forecasting), tatsächliche "
        "Erhebungs-Stände in Einzelländern können abweichen. "
        "Lizenz: CC BY 4.0 (ILO). Nur deskriptive Werte — "
        "keine politische Bewertung."
    )

    url = DEFAULT_DATA_URL
    return {
        "indicator_name": f"{label_de} — {ref_area_name}",
        "indicator": indicator_id,
        "country": ref_area_code,
        "country_name": ref_area_name,
        "year": period[:4] if period and period != "—" else "—",
        "value": value,
        "display_value": display,
        "description": description,
        "url": url,
        "source": "ILOSTAT (International Labour Organization)",
    }


def _generic_search_result(
    claim_lc: str, indicator_key: str | None,
) -> dict:
    """Pflicht-Antwort, wenn weder DBnomics noch ILO direkt Werte liefert.

    Liefert einen Verweis auf ILOSTAT-Datenportal — kein Zahlenfake.
    """
    if indicator_key and indicator_key in _ILO_INDICATORS:
        spec = _ILO_INDICATORS[indicator_key]
        label = spec.get("label_de") or indicator_key
        ind_id = spec.get("indicator_id") or f"ilo_{indicator_key}"
    else:
        label = "ILOSTAT-Datenbank"
        ind_id = "ilostat_portal"
    return {
        "indicator_name": f"ILOSTAT-Verweis: {label}",
        "indicator": ind_id,
        "country": "WLD",
        "country_name": "Welt",
        "year": "—",
        "value": None,
        "display_value": (
            "Keine Live-Werte aus ILOSTAT/DBnomics verfügbar. "
            f"Recherche-Einstieg: {DEFAULT_DATA_URL} — globale "
            "Arbeitsmarkt-Indikatoren der ILO."
        ),
        "description": (
            "Die ILO erhebt Arbeitsmarkt-Daten in 180+ Ländern und "
            "veröffentlicht modellierte globale Schätzungen unter "
            "ILOSTAT. Lizenz: CC BY 4.0."
        ),
        "url": DEFAULT_DATA_URL,
        "source": "ILOSTAT (International Labour Organization)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_ilostat(analysis: dict) -> dict:
    """Live-Lookup gegen ILOSTAT (DBnomics-Aggregator → ILO direkt).

    Strategie:
    1. Trigger-Gate (claim_mentions_ilostat_cached)
    2. Hard-Skip pure DACH-Claims (außer Direkt-ILO-Erwähnung)
    3. Indikator + Region picken
    4. DBnomics-Fetch (timeout 15 s, cached 24h)
    5. Fallback: generischer ILOSTAT-Verweis (kein Zahlenfake)
    """
    empty = {
        "source": "ILOSTAT",
        "type": "labor_data",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_ilostat(matchable):
        return empty

    # AT/DE-Hard-Skip mit Composite-Check
    has_direct = any(t in matchable for t in _DIRECT_TERMS)
    if not has_direct and _is_pure_dach_claim(matchable):
        logger.debug(
            "ILOSTAT: skip — pure DACH-Claim ohne Welt-Kontext"
        )
        return empty

    picked = _pick_indicator(matchable)
    if not picked:
        # Direkt-Erwähnung ohne Indikator → generischer Verweis
        if has_direct:
            return {
                "source": "ILOSTAT",
                "type": "labor_data",
                "results": [_generic_search_result(matchable, None)],
            }
        return empty

    indicator_key, spec = picked
    ref_areas = _pick_ref_areas(matchable, spec.get("default_ref_areas") or [])

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_DBNOMICS_S) as client:
        for ref_code, ref_name in ref_areas:
            try:
                doc = await _fetch_dbnomics_series(
                    client,
                    dataset=spec["dataset"],
                    ref_area=ref_code,
                    dim_filters=spec.get("dim_filters") or {},
                )
            except Exception as e:
                logger.debug(
                    f"ILOSTAT: fetch error {ref_code}: {e}"
                )
                doc = None
            if doc:
                try:
                    results.append(
                        _format_result(
                            indicator_key, spec, ref_code, ref_name, doc,
                        )
                    )
                except Exception as e:
                    logger.debug(f"ILOSTAT: format-error {ref_code}: {e}")
            if len(results) >= MAX_RESULTS:
                break

    if not results:
        # Fallback: generischer Verweis (kein Zahlenfake)
        logger.info(
            f"ILOSTAT: 0 Live-Treffer für '{indicator_key}' — "
            "Fallback auf Portal-Verweis"
        )
        return {
            "source": "ILOSTAT",
            "type": "labor_data",
            "results": [_generic_search_result(matchable, indicator_key)],
        }

    logger.info(
        f"ILOSTAT: {len(results)} Live-Treffer für '{indicator_key}' "
        f"({', '.join(r['country'] for r in results)})"
    )
    return {
        "source": "ILOSTAT",
        "type": "labor_data",
        "results": results,
    }
