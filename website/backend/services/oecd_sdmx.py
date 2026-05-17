"""OECD SDMX Multi-Domain Live-API Connector — TALIS / SOCX / Family / Housing / PIAAC.

Ergänzt den bestehenden `services/oecd.py` (PISA + Wirtschaft + Gender + Health)
um OECD-Domains, die dort NICHT abgedeckt sind:

  * TALIS 2024 — Lehrkräfte-Befragung (Belastung, Schulleitung, Gehälter)
  * SOCX — Social Expenditure Database (Sozialausgaben in % BIP)
  * Family Database — Karenz, Childcare, Familien-Politik
  * Affordable Housing Database — Wohnungspreise, Affordability, Homelessness
  * PIAAC 2023 — Erwachsenenkompetenzen (Literacy/Numeracy)

API: https://sdmx.oecd.org/public/rest/data/{dataflow}/{key}?format=jsondata
  * SDMX-JSON-Format (NICHT JSON-stat-2.0 — andere Familie als Eurostat)
  * Kein Auth, höflich = 1 req/s
  * OECD-Lizenz: frei mit Citation, ähnlich CC-BY

Trigger-Strategie:
  1. Hard-Skip wenn rein Health/Wirtschaft (→ oecd.py zuständig)
  2. Hard-Skip wenn rein DACH-Bildung ohne OECD-Bezug (→ bildung_pack.py)
  3. Domain-Detection (TALIS/SOCX/Family/Housing/PIAAC) aus Claim
  4. Country-Detection (38 OECD-Länder)
  5. Multi-Domain-Routing: nur relevante Query abfeuern

Politische Guardrails (memory/project_political_guardrails.md):
  * Pure Statistik, keine Bewertung
  * Methodologie-Hinweis im `description`
  * Bei normativen Termen (z.B. "Wohnungsnot", "Karenz-Politik") nur
    deskriptive Zahlen, keine politische Empfehlung
"""

# WIRING für main.py:
# from services.oecd_sdmx import search_oecd_sdmx, claim_mentions_oecd_sdmx_cached
# if claim_mentions_oecd_sdmx_cached(claim):
#     tasks.append(cached("OECD SDMX", search_oecd_sdmx, analysis))
#     queried_names.append("OECD SDMX (TALIS+SOCX+Family+Housing+PIAAC)")

from __future__ import annotations

import logging
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

SDMX_BASE = "https://sdmx.oecd.org/public/rest/data"
TIMEOUT_S = 15.0
CACHE_TTL_S = 24 * 3600  # 24h
MAX_RESULTS_PER_DOMAIN = 3

# ---------------------------------------------------------------------------
# OECD Country Mapping (38 OECD-Mitglieder, ISO-3 wie in SDMX REF_AREA)
# Stand 2026: 38 Mitglieder + häufige Vergleichs-Länder
# ---------------------------------------------------------------------------
_OECD_COUNTRIES: dict[str, tuple[str, str]] = {
    # DACH + EU-Kern
    "österreich": ("AUT", "Österreich"),
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
    "portugal": ("PRT", "Portugal"),
    "niederlande": ("NLD", "Niederlande"),
    "netherlands": ("NLD", "Niederlande"),
    "belgien": ("BEL", "Belgien"),
    "belgium": ("BEL", "Belgien"),
    "luxemburg": ("LUX", "Luxemburg"),
    "luxembourg": ("LUX", "Luxemburg"),
    "irland": ("IRL", "Irland"),
    "ireland": ("IRL", "Irland"),
    # Nord-EU
    "schweden": ("SWE", "Schweden"),
    "sweden": ("SWE", "Schweden"),
    "norwegen": ("NOR", "Norwegen"),
    "norway": ("NOR", "Norwegen"),
    "dänemark": ("DNK", "Dänemark"),
    "denmark": ("DNK", "Dänemark"),
    "finnland": ("FIN", "Finnland"),
    "finland": ("FIN", "Finnland"),
    "island": ("ISL", "Island"),
    "iceland": ("ISL", "Island"),
    # CEE
    "polen": ("POL", "Polen"),
    "poland": ("POL", "Polen"),
    "tschechien": ("CZE", "Tschechien"),
    "czechia": ("CZE", "Tschechien"),
    "ungarn": ("HUN", "Ungarn"),
    "hungary": ("HUN", "Ungarn"),
    "slowakei": ("SVK", "Slowakei"),
    "slovakia": ("SVK", "Slowakei"),
    "slowenien": ("SVN", "Slowenien"),
    "slovenia": ("SVN", "Slowenien"),
    "estland": ("EST", "Estland"),
    "estonia": ("EST", "Estland"),
    "lettland": ("LVA", "Lettland"),
    "latvia": ("LVA", "Lettland"),
    "litauen": ("LTU", "Litauen"),
    "lithuania": ("LTU", "Litauen"),
    # Süd-EU
    "griechenland": ("GRC", "Griechenland"),
    "greece": ("GRC", "Griechenland"),
    # UK + Englische OECD
    "vereinigtes königreich": ("GBR", "Vereinigtes Königreich"),
    "united kingdom": ("GBR", "Vereinigtes Königreich"),
    "großbritannien": ("GBR", "Vereinigtes Königreich"),
    "uk": ("GBR", "Vereinigtes Königreich"),
    # Türkei (OECD-Mitglied)
    "türkei": ("TUR", "Türkei"),
    "türkiye": ("TUR", "Türkei"),
    "turkey": ("TUR", "Türkei"),
    # Außer-EU OECD
    "usa": ("USA", "USA"),
    "vereinigte staaten": ("USA", "USA"),
    "united states": ("USA", "USA"),
    "kanada": ("CAN", "Kanada"),
    "canada": ("CAN", "Kanada"),
    "japan": ("JPN", "Japan"),
    "südkorea": ("KOR", "Südkorea"),
    "south korea": ("KOR", "Südkorea"),
    "korea": ("KOR", "Südkorea"),
    "australien": ("AUS", "Australien"),
    "australia": ("AUS", "Australien"),
    "neuseeland": ("NZL", "Neuseeland"),
    "new zealand": ("NZL", "Neuseeland"),
    "mexiko": ("MEX", "Mexiko"),
    "mexico": ("MEX", "Mexiko"),
    "chile": ("CHL", "Chile"),
    "kolumbien": ("COL", "Kolumbien"),
    "colombia": ("COL", "Kolumbien"),
    "costa rica": ("CRI", "Costa Rica"),
    "israel": ("ISR", "Israel"),
}

# ---------------------------------------------------------------------------
# Domain-Definitions
# ---------------------------------------------------------------------------
# Dataflow-IDs aus OECD Data Explorer
_DOMAINS: dict[str, dict] = {
    "talis": {
        "flow": "OECD.EDU.ECS,DSD_TALIS@DF_TALIS,1.0",
        "label": "TALIS 2024 — Lehrkräfte",
        "label_short": "TALIS",
        "url": "https://www.oecd.org/en/about/programmes/talis.html",
        "description_methodology": (
            "OECD TALIS 2024 — Teaching and Learning International Survey. "
            "Selbstauskunft Lehrkräfte/Schulleitungen in 50+ Ländern."
        ),
        "keywords": (
            "talis", "lehrer-belastung", "lehrerbelastung",
            "schulleitungspraxis", "lehrer-gehälter international",
            "lehrkräfte international", "teacher survey",
            "lehrer arbeitsbelastung",
            "lehrkräfte oecd", "lehrer oecd",
        ),
    },
    "socx": {
        "flow": "OECD.ELS.SPD,DSD_SOCX_AGG@DF_PUB_PRV,1.0",
        "label": "SOCX — Public Social Expenditure",
        "label_short": "SOCX",
        "url": "https://www.oecd.org/social/expenditure.htm",
        "description_methodology": (
            "OECD SOCX — Social Expenditure Database. Sozialausgaben als "
            "% BIP nach Funktion (Alter, Gesundheit, Familie, Arbeitsmarkt). "
            "Komplementär zu Eurostat ESSPROS (anderes Klassifikations-Schema)."
        ),
        "keywords": (
            "sozialausgaben", "social spending", "social expenditure",
            "socx", "public social spending",
            "sozialquote", "sozialleistungen oecd",
        ),
    },
    "family": {
        "flow": "OECD.ELS.SPD,DSD_SOCX_AGG@DF_PUB_FAM,1.0",
        "label": "OECD Family Database",
        "label_short": "Family DB",
        "url": "https://www.oecd.org/els/family/database.htm",
        "description_methodology": (
            "OECD Family Database — Karenz, Childcare-Kosten, Geburtsraten, "
            "Erwerbsquoten Mütter. Daten aus nationalen Erhebungen, "
            "harmonisiert."
        ),
        "keywords": (
            "karenz", "elternzeit", "parental leave",
            "childcare", "kinderbetreuung",
            "kinderbetreuungskosten", "childcare costs",
            "geburtsrate", "geburtenrate", "fertility rate",
            "family policy", "familien-politik", "familienpolitik",
            "mütter erwerbsquote", "maternal employment",
        ),
    },
    "housing": {
        "flow": "OECD.ECO.MPD,DSD_AN_HOUSE_PRICES@DF_HOUSE_PRICES,1.0",
        "label": "OECD Affordable Housing Database",
        "label_short": "Affordable Housing DB",
        "url": "https://www.oecd.org/housing/data/affordable-housing-database/",
        "description_methodology": (
            "OECD Affordable Housing Database — Wohnungspreise, "
            "Affordability-Index, Homelessness. Daten aus nationalen "
            "Erhebungen, harmonisiert."
        ),
        "keywords": (
            "wohnungspreise oecd", "wohnungspreise vergleich",
            "wohnen oecd", "affordability", "affordability-index",
            "wohnkosten-belastung oecd",
            "homelessness", "obdachlosigkeit oecd",
            "house price oecd", "housing affordability",
        ),
    },
    "piaac": {
        "flow": "OECD.CFE.EDS,DSD_REG_EDU@DF_TRAINING,2.5",
        "label": "PIAAC 2023 — Adult Skills",
        "label_short": "PIAAC",
        "url": "https://www.oecd.org/en/about/programmes/piaac.html",
        "description_methodology": (
            "OECD PIAAC 2023 — Programme for the International Assessment of "
            "Adult Competencies. Erwachsenenkompetenzen (Literacy, Numeracy, "
            "Adaptive Problem Solving) in 31 Ländern."
        ),
        "keywords": (
            "piaac", "erwachsenenkompetenzen", "adult skills",
            "adult literacy", "erwachsenenbildung kompetenzen",
            "piaac-studie", "piaac studie",
            "lese-kompetenz erwachsene",
            "rechen-kompetenz erwachsene", "numeracy adults",
        ),
    },
}

# Allgemeine OECD-SDMX-Trigger (zusätzlich, falls Domain unklar)
_GENERIC_OECD_TERMS = (
    "oecd-studie", "oecd studie", "oecd-daten", "oecd daten",
    "oecd-bericht", "oecd bericht",
)

# Hard-Skip-Terme — Anti-Trigger für Health / Wirtschaft (gehört zu oecd.py)
_HEALTH_ECON_SKIP_TERMS = (
    "lebenserwartung", "sterblichkeit", "mortality",
    "spitalsbett", "krankenhaus", "hospital bed",
    "gesundheitsausgaben", "health expenditure",
    "bip oecd", "gdp oecd",
    "leitzins oecd",  # gehört zu ECB/oenb
    "arbeitslosenquote oecd",  # bereits in oecd.py SDMX abgedeckt
    "gender wage gap oecd",  # bereits in oecd.py SDMX abgedeckt
)

# Hard-Skip-Terme — DACH-Bildung ohne OECD-Bezug
_DACH_EDU_SKIP_TERMS = (
    "ahs",  # österr. spezifisch
    "nms ",  # österr. spezifisch
    "matura",  # österr. spezifisch
    "abitur ",  # dt. spezifisch
)


# ---------------------------------------------------------------------------
# Trigger-Logic
# ---------------------------------------------------------------------------
def _matches_any_domain(claim_lc: str) -> list[str]:
    """Liefert die Liste der gematchten Domain-Keys (talis/socx/family/housing/piaac)."""
    matches: list[str] = []
    for dom_id, info in _DOMAINS.items():
        if any(kw in claim_lc for kw in info["keywords"]):
            matches.append(dom_id)
    return matches


def _is_pure_health_econ(claim_lc: str) -> bool:
    """Hard-Skip: rein Health/Wirtschaft → oecd.py."""
    return any(t in claim_lc for t in _HEALTH_ECON_SKIP_TERMS)


def _is_pure_dach_edu_without_oecd(claim_lc: str) -> bool:
    """Hard-Skip: DACH-spezifische Bildungs-Begriffe ohne OECD-Bezug → bildung_pack.py."""
    if not any(t in claim_lc for t in _DACH_EDU_SKIP_TERMS):
        return False
    # Wenn explizit OECD/TALIS/PIAAC im Claim, NICHT skippen
    if any(t in claim_lc for t in ("oecd", "talis", "piaac", "international")):
        return False
    return True


def _claim_mentions_oecd_sdmx(claim_lc: str) -> bool:
    """Trigger-Check (ungecached).

    1. Hard-Skip wenn rein Health/Wirtschaft (oecd.py)
    2. Hard-Skip wenn rein DACH-Bildung (bildung_pack.py)
    3. True wenn mindestens eine Domain matcht
    4. True wenn allgemeiner OECD-Term + irgendein OECD-Country
    """
    if not claim_lc:
        return False

    if _is_pure_health_econ(claim_lc):
        return False
    if _is_pure_dach_edu_without_oecd(claim_lc):
        return False

    # Domain-Match → Trigger
    if _matches_any_domain(claim_lc):
        return True

    # Allgemeiner OECD-Term + Country → Trigger (vager Fall)
    has_generic = any(t in claim_lc for t in _GENERIC_OECD_TERMS)
    has_country = any(name in claim_lc for name in _OECD_COUNTRIES.keys())
    if has_generic and has_country:
        return True

    return False


# Trigger-Cache (24h)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_oecd_sdmx_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_oecd_sdmx(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
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
    if len(_result_cache) > 500:
        oldest = sorted(_result_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _result_cache.pop(k, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _extract_countries(analysis: dict, claim_lc: str) -> list[tuple[str, str]]:
    """Erkenne genannte OECD-Länder. Bevorzugt NER, fällt auf Substring.

    Returns: list[(ISO3, DisplayName)]. Default: [("AUT", "Österreich")].
    """
    ner_countries = (analysis or {}).get("ner_entities", {}).get("countries", [])
    text = " ".join(ner_countries).lower() + " " + claim_lc

    found: list[tuple[str, str]] = []
    seen = set()
    sorted_names = sorted(_OECD_COUNTRIES.keys(), key=len, reverse=True)
    for name in sorted_names:
        if name in text:
            iso, disp = _OECD_COUNTRIES[name]
            if iso not in seen:
                found.append((iso, disp))
                seen.add(iso)
    if not found:
        # Fallback: AT (BORG-Lehrer-Standard)
        found.append(("AUT", "Österreich"))
    return found[:3]  # max 3 Länder pro Query


def _parse_sdmx_json(payload: dict, dom_info: dict, target_iso: set[str],
                     dom_id: str) -> list[dict]:
    """Parse SDMX-JSON-Response.

    SDMX-JSON-Schema (vereinfacht):
      data.dataSets[0].observations: { "0:1:2:...": [value, ...] }
      data.structures[0].dimensions.observation: [
        {id, name, values: [{id, name}]}
      ]
    """
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") or {}
    datasets = data.get("dataSets") or []
    if not datasets:
        return []
    obs = datasets[0].get("observations") or {}
    if not obs:
        return []
    structures = data.get("structures") or []
    if not structures:
        return []
    dims = structures[0].get("dimensions", {}).get("observation") or []
    if not dims:
        return []

    # Dimension-Lookup: [dim_idx] → {id: dim_id, values: [{id, name}]}
    dim_meta: list[dict] = []
    ref_area_idx: int | None = None
    time_period_idx: int | None = None
    for d_idx, d in enumerate(dims):
        dim_id = d.get("id", "")
        vals = d.get("values") or []
        # Normalize value names (sometimes dict {en: "..."})
        norm_vals = []
        for v in vals:
            name = v.get("name", "")
            if isinstance(name, dict):
                name = name.get("en") or name.get("de") or str(name)
            norm_vals.append({"id": v.get("id", ""), "name": name or v.get("id", "")})
        dim_meta.append({"id": dim_id, "values": norm_vals})
        if dim_id == "REF_AREA":
            ref_area_idx = d_idx
        if dim_id == "TIME_PERIOD":
            time_period_idx = d_idx

    # Allowed REF_AREA indices (positions in dim values list)
    allowed_indices: set[int] = set()
    if ref_area_idx is not None and target_iso:
        for v_idx, v in enumerate(dim_meta[ref_area_idx]["values"]):
            if (v["id"] or "").upper() in target_iso:
                allowed_indices.add(v_idx)

    results: list[dict] = []
    # Sort obs-keys to get a stable, recent-first order
    # SDMX TIME_PERIOD-Dimension contains period — sort lexically (works for years/quarters)
    keys = list(obs.keys())

    def _sort_key(k: str) -> str:
        if time_period_idx is None:
            return k
        parts = k.split(":")
        if len(parts) > time_period_idx:
            try:
                v_idx = int(parts[time_period_idx])
                vals = dim_meta[time_period_idx]["values"]
                if 0 <= v_idx < len(vals):
                    return vals[v_idx]["id"] or ""
            except (ValueError, IndexError):
                pass
        return ""

    keys.sort(key=_sort_key, reverse=True)

    for key_str in keys:
        if len(results) >= MAX_RESULTS_PER_DOMAIN:
            break
        parts = key_str.split(":")
        val_list = obs[key_str]
        if not val_list or val_list[0] is None:
            continue
        value = val_list[0]

        # Country-Filter
        if ref_area_idx is not None and allowed_indices:
            try:
                if int(parts[ref_area_idx]) not in allowed_indices:
                    continue
            except (ValueError, IndexError):
                continue

        # Labels resolvieren
        labels: dict[str, str] = {}
        country_iso = ""
        time_period = ""
        for i, p in enumerate(parts):
            if i >= len(dim_meta):
                break
            try:
                v_idx = int(p)
            except ValueError:
                continue
            vals = dim_meta[i]["values"]
            if 0 <= v_idx < len(vals):
                labels[dim_meta[i]["id"]] = vals[v_idx]["name"]
                if i == ref_area_idx:
                    country_iso = vals[v_idx]["id"] or ""
                if i == time_period_idx:
                    time_period = vals[v_idx]["id"] or ""

        if isinstance(value, float):
            value = round(value, 2)

        country_name = _iso_to_display(country_iso) or labels.get("REF_AREA", country_iso)
        measure = labels.get("MEASURE") or labels.get("INDICATOR") or dom_info["label"]
        unit = labels.get("UNIT_MEASURE") or labels.get("UNIT") or ""

        unit_display = f" {unit}" if unit and unit.lower() not in ("dimensionless", "") else ""

        display_value = (
            f"{country_name} {time_period} ({dom_info['label_short']}): "
            f"{measure} = {value}{unit_display}".strip()
        )

        results.append({
            "indicator_name": f"{dom_info['label']} — {country_name}",
            "indicator": f"oecd_{dom_id}_{(country_iso or 'xxx').lower()}",
            "country": country_iso or "—",
            "country_name": country_name,
            "year": time_period or "latest",
            "value": value,
            "display_value": display_value,
            "description": dom_info["description_methodology"],
            "url": dom_info["url"],
            "source": "OECD SDMX (Data Explorer)",
        })

    return results


def _iso_to_display(iso: str) -> str:
    """Reverse-Lookup ISO-3 → Anzeige-Name."""
    if not iso:
        return ""
    for _, (code, disp) in _OECD_COUNTRIES.items():
        if code == iso:
            return disp
    return iso


# ---------------------------------------------------------------------------
# HTTP-Call pro Domain
# ---------------------------------------------------------------------------
async def _fetch_domain(client, dom_id: str, target_iso: list[str],
                        start_period: str = "2020") -> list[dict]:
    """Fetch SDMX-Daten für eine Domain + Country-Set.

    Strategie: nutzt 'all' für die Filter-Key (OECD-SDMX akzeptiert keine
    REF_AREA-Filter mehr im Path verlässlich), filtert client-side.
    """
    dom_info = _DOMAINS[dom_id]
    target_set = {c.upper() for c in target_iso}
    cache_key = f"{dom_id}::{'_'.join(sorted(target_set))}::{start_period}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    flow = dom_info["flow"]
    # 'all' key — wir filtern danach client-side per REF_AREA
    url = (
        f"{SDMX_BASE}/{flow}/all"
        f"?lastNObservations=1&dimensionAtObservation=AllDimensions"
        f"&startPeriod={start_period}"
    )
    try:
        resp = await client.get(url, headers={
            "Accept": "application/vnd.sdmx.data+json",
        })
    except Exception as e:
        logger.debug(f"oecd_sdmx: {dom_id} request failed: {e}")
        _cache_put(cache_key, [])
        return []

    if resp.status_code == 429:
        logger.warning(f"oecd_sdmx: {dom_id} rate-limited (429)")
        return []
    if resp.status_code == 404:
        logger.info(f"oecd_sdmx: {dom_id} dataflow not found (404)")
        _cache_put(cache_key, [])
        return []
    if resp.status_code != 200:
        logger.warning(f"oecd_sdmx: {dom_id} HTTP {resp.status_code}")
        return []

    try:
        payload = resp.json()
    except Exception as e:
        logger.debug(f"oecd_sdmx: {dom_id} JSON-parse failed: {e}")
        return []

    results = _parse_sdmx_json(payload, dom_info, target_set, dom_id)
    _cache_put(cache_key, results)
    logger.info(
        f"oecd_sdmx: {dom_id} → {len(results)} Treffer für "
        f"{','.join(sorted(target_set)) or '*'}"
    )
    return results


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_oecd_sdmx(analysis: dict) -> dict:
    """Live-Lookup gegen OECD SDMX (Multi-Domain).

    Strategie:
      1. Domain-Detection (TALIS/SOCX/Family/Housing/PIAAC)
      2. Country-Detection (38 OECD-Länder, Default AT)
      3. Pro Domain ein SDMX-Call (max 2 Domains parallel um Rate-Limit zu schonen)
      4. Top-3 Indikatoren je Domain
    """
    empty = {
        "source": "OECD SDMX",
        "type": "oecd_data",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_oecd_sdmx(matchable):
        return empty

    domains = _matches_any_domain(matchable)
    if not domains:
        # Generic-Trigger: nimm SOCX als Default-Statistik (häufigste OECD-Anfrage)
        domains = ["socx"]
    # Max 2 Domains, um OECD-Rate-Limit nicht zu reißen
    domains = domains[:2]

    countries = _extract_countries(analysis, matchable)
    target_iso = [c[0] for c in countries]

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for dom_id in domains:
            try:
                dom_results = await _fetch_domain(client, dom_id, target_iso)
            except Exception as e:
                logger.warning(f"oecd_sdmx: {dom_id} unexpected error: {e}")
                continue
            results.extend(dom_results)

    if not results:
        logger.info(
            f"oecd_sdmx: 0 Treffer (domains={domains}, "
            f"countries={','.join(target_iso)})"
        )
        return empty

    return {
        "source": "OECD SDMX",
        "type": "oecd_data",
        "results": results[: MAX_RESULTS_PER_DOMAIN * 2],  # cap insgesamt
    }
