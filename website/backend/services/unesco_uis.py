"""UNESCO UIS — Globale Bildungs-Statistik (SDG-4-Custodian).

Datenquelle: UNESCO Institute for Statistics (UIS) — Custodian-Status
fuer SDG-4 (Bildung weltweit). 5.000+ Indikatoren, 200+ Laender.
Alphabetisierung, Schulbesuchsquoten, Out-of-school-Children, ECCE,
Hochschul-Enrolment, Lehrer-Statistik, Bildungsausgaben.

API: https://api.uis.unesco.org/api/public/  (HTTP GET, JSON,
kein API-Key noetig). Endpunkte:
- /api/public/versions/default
- /api/public/data/indicators?indicator=<CODE>&geoUnit=<ISO3>&start=<YEAR>
- /api/public/definitions/indicators
- /api/public/definitions/geounits

Hinweis: Die alte SDMX-API (api.uis.unesco.org/sdmx/...) ist seit
September 2024 nicht mehr aktualisiert. Der neue UIS-Data-Browser
nutzt /api/public/. Dieser Service verwendet ausschliesslich den
neuen Endpunkt.

Lizenz: CC BY-SA 3.0 IGO — Evidora-tauglich.

Komplementaer zu existierenden Quellen:
- eric.py: US-Bildungsforschungs-Records (peer-reviewed Studien)
- bildung_pack.py: kuratierter DACH-Forschungsstand
- oecd.py: PISA / OECD-Wirtschafts-Indikatoren
- worldbank.py / dbnomics.py: makrooekonomische Vergleichswerte
- UNESCO UIS: globale SDG-4-Statistik (Alphabetisierung, OOSC, NER/GER)

Politische Guardrails (memory/project_political_guardrails.md):
- Bildungsdaten sind auch politisch — Service liefert NUR die Zahlen.
- KEINE Bewertung "gut/schlecht", KEINE Schuldzuweisungen.
- Bei Trend-Aussagen ("Alphabetisierung sinkt") werden Datenpunkte
  zitiert; das Verdict entscheidet der Synthesizer.

# WIRING fuer main.py:
# from services.unesco_uis import search_unesco_uis, claim_mentions_unesco_uis_cached
# if claim_mentions_unesco_uis_cached(claim):
#     tasks.append(cached("UNESCO UIS", search_unesco_uis, analysis))
#     queried_names.append("UNESCO UIS")
"""

from __future__ import annotations

import logging
import re
import time
from urllib.parse import urlencode

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

UIS_API = "https://api.uis.unesco.org/api/public"
TIMEOUT_S = 20.0
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 60 * 60  # 24 h

# ---------------------------------------------------------------------------
# Indikator-Mapping: claim-keywords → (UIS-Code, Anzeige-Name, Einheits-Suffix)
# Konservativ: vier Schluessel-Cluster, je 1-3 Top-Codes.
# Mehr Codes ueber https://api.uis.unesco.org/api/public/definitions/indicators
# ---------------------------------------------------------------------------
_INDICATOR_MAP: dict[str, list[tuple[str, str, str]]] = {
    # Alphabetisierung
    "literacy": [
        ("LR.AG15T24", "Youth literacy rate (15-24)", "%"),
        ("LR.AG15T99", "Adult literacy rate (15+)", "%"),
    ],
    # Schulbesuchsquote
    "school_enrollment": [
        ("NER.1.CP", "Net enrolment rate, primary", "%"),
        ("GER.1", "Gross enrolment ratio, primary", "%"),
        ("GER.2T3", "Gross enrolment ratio, secondary", "%"),
    ],
    # Out-of-school Children
    "out_of_school": [
        ("OFST.1.CP", "Out-of-school children, primary age", "Kinder"),
        ("OFST.1T2.CP", "Out-of-school children, primary + lower-sec", "Kinder"),
    ],
    # Hochschulbildung
    "higher_education": [
        ("GER.5T8", "Gross enrolment ratio, tertiary", "%"),
        ("CR.3", "Completion rate, upper-secondary", "%"),
    ],
    # ECCE
    "ecce": [
        ("NER.0.CP", "Net enrolment rate, ECCE (pre-primary)", "%"),
    ],
}

# Trigger-Wort → Indikator-Cluster
_KEYWORD_TO_CLUSTER: dict[str, str] = {
    # DE
    "alphabetisierung": "literacy",
    "alphabetisierungsrate": "literacy",
    "lesefaehigkeit weltweit": "literacy",
    "analphabetismus": "literacy",
    "schulbesuchsquote": "school_enrollment",
    "schulbesuch": "school_enrollment",
    "einschulungsrate": "school_enrollment",
    "einschulungsquote": "school_enrollment",
    "kein zugang zu schule": "out_of_school",
    "ohne schulbesuch": "out_of_school",
    "kinder ohne schule": "out_of_school",
    "out-of-school": "out_of_school",
    "out of school children": "out_of_school",
    "hochschulquote": "higher_education",
    "hochschul-quote": "higher_education",
    "studienquote": "higher_education",
    "tertiaere bildung": "higher_education",
    "tertiärbildung": "higher_education",
    "kindergartenquote": "ecce",
    "vorschulquote": "ecce",
    # EN
    "literacy rate": "literacy",
    "adult literacy": "literacy",
    "youth literacy": "literacy",
    "illiteracy": "literacy",
    "school enrollment": "school_enrollment",
    "school enrolment": "school_enrollment",
    "net enrolment": "school_enrollment",
    "gross enrolment": "school_enrollment",
    "primary education": "school_enrollment",
    "secondary education": "school_enrollment",
    "out of school": "out_of_school",
    "tertiary enrolment": "higher_education",
    "tertiary education": "higher_education",
    "higher education rate": "higher_education",
    "pre-primary": "ecce",
    "early childhood education": "ecce",
    "ecce": "ecce",
}

# Direkt-Trigger (UNESCO selbst genannt)
_DIRECT_TERMS = (
    "unesco uis", "unesco-uis", "unesco institute for statistics",
    "uis statistik", "uis-statistik",
    "unesco bildungsdaten", "unesco bildungs-daten",
    "unesco-bildungsbericht",
    "global education monitoring",
    "sdg 4", "sdg-4", "sdg4",
    "ziel 4 der nachhaltigkeitsziele",
    "bildung weltweit", "weltbildung",
    "globale bildung",
)

# ---------------------------------------------------------------------------
# Country-Mapping: claim-keyword → ISO-3 (subset wichtigster Laender,
# mit Fokus auf "klassische" SDG-4-Beispiele). Erweiterung jederzeit moeglich.
# ---------------------------------------------------------------------------
_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    # DACH (nur Kontext-Vergleiche — UIS hat fuer Hochlohnlaender oft Luecken)
    "österreich": ("AUT", "Österreich"),
    "austria": ("AUT", "Österreich"),
    "deutschland": ("DEU", "Deutschland"),
    "germany": ("DEU", "Deutschland"),
    "schweiz": ("CHE", "Schweiz"),
    "switzerland": ("CHE", "Schweiz"),
    # Sub-Sahara-Afrika (typische SDG-4-Hotspots)
    "nigeria": ("NGA", "Nigeria"),
    "kenia": ("KEN", "Kenia"),
    "kenya": ("KEN", "Kenia"),
    "äthiopien": ("ETH", "Äthiopien"),
    "ethiopia": ("ETH", "Äthiopien"),
    "südafrika": ("ZAF", "Südafrika"),
    "south africa": ("ZAF", "Südafrika"),
    "ghana": ("GHA", "Ghana"),
    "uganda": ("UGA", "Uganda"),
    "tansania": ("TZA", "Tansania"),
    "tanzania": ("TZA", "Tansania"),
    "senegal": ("SEN", "Senegal"),
    "mali": ("MLI", "Mali"),
    "niger": ("NER", "Niger"),
    "demokratische republik kongo": ("COD", "DR Kongo"),
    "dr kongo": ("COD", "DR Kongo"),
    "drc": ("COD", "DR Kongo"),
    # MENA / Asien
    "ägypten": ("EGY", "Ägypten"),
    "egypt": ("EGY", "Ägypten"),
    "indien": ("IND", "Indien"),
    "india": ("IND", "Indien"),
    "pakistan": ("PAK", "Pakistan"),
    "bangladesch": ("BGD", "Bangladesch"),
    "bangladesh": ("BGD", "Bangladesch"),
    "afghanistan": ("AFG", "Afghanistan"),
    "indonesien": ("IDN", "Indonesien"),
    "indonesia": ("IDN", "Indonesien"),
    "philippinen": ("PHL", "Philippinen"),
    "vietnam": ("VNM", "Vietnam"),
    "china": ("CHN", "China"),
    # Latam
    "brasilien": ("BRA", "Brasilien"),
    "brazil": ("BRA", "Brasilien"),
    "mexiko": ("MEX", "Mexiko"),
    "mexico": ("MEX", "Mexiko"),
    "argentinien": ("ARG", "Argentinien"),
    "kolumbien": ("COL", "Kolumbien"),
    "peru": ("PER", "Peru"),
    "chile": ("CHL", "Chile"),
    # Sonstige
    "ukraine": ("UKR", "Ukraine"),
    "türkei": ("TUR", "Türkei"),
    "turkey": ("TUR", "Türkei"),
    "iran": ("IRN", "Iran"),
    "syrien": ("SYR", "Syrien"),
    "syria": ("SYR", "Syrien"),
    "jemen": ("YEM", "Jemen"),
    "yemen": ("YEM", "Jemen"),
}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_unesco_uis(claim_lc: str) -> bool:
    """Pure Trigger-Funktion (lowercase claim erwartet).

    Drei Trigger-Wege:
    1. Direkt-Term (unesco/sdg 4/etc.)
    2. Indikator-Keyword + Bildungs-Kontext
    3. Indikator-Keyword + Land aus Country-Map
    """
    if not claim_lc:
        return False

    # 1. Direkt-Trigger
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    # 2. Indikator-Keyword (alleine reicht, wenn klar bildungs-spezifisch)
    has_indicator_kw = any(k in claim_lc for k in _KEYWORD_TO_CLUSTER.keys())
    if has_indicator_kw:
        return True

    return False


# Trigger-Resolve-Cache: { claim_lc: (ts, bool) }
_TRIGGER_CACHE: dict[str, tuple[float, bool]] = {}


def claim_mentions_unesco_uis_cached(claim: str) -> bool:
    """24h-Cache-Wrapper fuer den Trigger-Check."""
    if not claim:
        return False
    key = claim.lower().strip()
    if not key:
        return False
    now = time.time()
    cached = _TRIGGER_CACHE.get(key)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_unesco_uis(key)
    # Cache-Hygiene
    if len(_TRIGGER_CACHE) > 512:
        oldest = sorted(_TRIGGER_CACHE.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _TRIGGER_CACHE.pop(k, None)
    _TRIGGER_CACHE[key] = (now, result)
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _detect_clusters(claim_lc: str) -> list[str]:
    """Welche Indikator-Cluster werden im Claim erwaehnt? (max 2)."""
    found: list[str] = []
    seen: set[str] = set()
    for kw, cluster in _KEYWORD_TO_CLUSTER.items():
        if kw in claim_lc and cluster not in seen:
            found.append(cluster)
            seen.add(cluster)
            if len(found) >= 2:
                break
    # Wenn direkt UNESCO erwaehnt aber kein Cluster → Default literacy + OOSC
    if not found and any(t in claim_lc for t in _DIRECT_TERMS):
        found = ["literacy", "out_of_school"]
    return found


def _detect_countries(claim_lc: str) -> list[tuple[str, str]]:
    """Welche Laender werden erwaehnt? (max 2).

    Verwendet Wort-Grenzen via Regex, damit z. B. "niger" nicht in "nigeria"
    triggert. Reihenfolge: laengstes Keyword zuerst (greedy), damit
    "demokratische republik kongo" vor "kongo" / "drc" matched.
    """
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    sorted_kws = sorted(_COUNTRY_MAP.keys(), key=len, reverse=True)
    # Maske bereits gematchter Spans, um Substring-Kollisionen zu vermeiden.
    masked = claim_lc
    for kw in sorted_kws:
        iso, name = _COUNTRY_MAP[kw]
        if iso in seen:
            continue
        pattern = r"(?<![a-zäöüß])" + re.escape(kw) + r"(?![a-zäöüß])"
        if re.search(pattern, masked):
            found.append((iso, name))
            seen.add(iso)
            masked = re.sub(pattern, " " * len(kw), masked)
            if len(found) >= 2:
                break
    return found


# ---------------------------------------------------------------------------
# Result-Cache (24h pro indicator+country)
# ---------------------------------------------------------------------------
_RESULT_CACHE: dict[str, tuple[float, list[dict]]] = {}


def _cache_get(key: str) -> list[dict] | None:
    now = time.time()
    hit = _RESULT_CACHE.get(key)
    if hit and (now - hit[0]) < CACHE_TTL_S:
        return hit[1]
    return None


def _cache_put(key: str, value: list[dict]) -> None:
    if len(_RESULT_CACHE) > 256:
        _RESULT_CACHE.clear()
    _RESULT_CACHE[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# HTTP-Layer
# ---------------------------------------------------------------------------
async def _fetch_indicator(
    client, indicator: str, geo: str, start_year: int = 2015,
) -> list[dict]:
    """GET /data/indicators?indicator=<code>&geoUnit=<iso3>&start=<year>.

    Returns Liste von Records, je { indicatorId, geoUnit, year, value }.
    """
    cache_key = f"uis::{indicator}::{geo}::{start_year}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    params = {
        "indicator": indicator,
        "geoUnit": geo,
        "start": str(start_year),
    }
    url = f"{UIS_API}/data/indicators?{urlencode(params)}"

    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"UNESCO UIS HTTP {resp.status_code} for {indicator}/{geo}"
            )
            _cache_put(cache_key, [])
            return []
        data = resp.json()
    except Exception as e:
        logger.debug(f"UNESCO UIS fetch failed {indicator}/{geo}: {e}")
        return []

    records = data.get("records") or []
    if not isinstance(records, list):
        records = []
    _cache_put(cache_key, records)
    return records


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _format_value(val, unit_suffix: str) -> str:
    """Formatiert numerische Werte: % mit Komma, Counts mit Tausender-Punkt."""
    if val is None:
        return "—"
    try:
        f = float(val)
    except (TypeError, ValueError):
        return str(val)
    if unit_suffix == "%":
        # 1 Nachkommastelle, deutsches Komma
        return f"{f:.1f}".replace(".", ",") + " %"
    # Counts: Tausender-Punkte (deutsch)
    if f >= 1000:
        return f"{int(f):,}".replace(",", ".")
    return f"{f:.0f}" if f == int(f) else f"{f:.2f}".replace(".", ",")


def _pick_latest_record(records: list[dict]) -> dict | None:
    """Aus mehreren Jahres-Records den juengsten mit value != None waehlen."""
    if not records:
        return None
    valid = [r for r in records if r.get("value") is not None]
    if not valid:
        return None
    valid.sort(key=lambda r: r.get("year", 0), reverse=True)
    return valid[0]


def _build_result(
    record: dict, indicator_label: str, unit_suffix: str,
    country_name: str,
) -> dict | None:
    """Forme UIS-Record in das Evidora-Result-Schema."""
    if not isinstance(record, dict):
        return None

    code = str(record.get("indicatorId") or "")
    iso = str(record.get("geoUnit") or "")
    year = record.get("year")
    val = record.get("value")
    if not code or not iso or val is None:
        return None

    val_str = _format_value(val, unit_suffix)
    year_str = str(year) if year else "—"

    display = (
        f"UNESCO UIS / {country_name} {year_str}: "
        f"{indicator_label} = {val_str}."
    )

    description = (
        f"Quelle: UNESCO Institute for Statistics (UIS), "
        f"Indikator-Code {code}. "
        f"Datenstand letztes UIS-Release; Update halbjaehrlich. "
        f"Nur deskriptive Werte — keine Bewertung."
    )

    # Direkt-Link zum UIS-Data-Browser (filter by indicator+country)
    url = (
        f"https://databrowser.uis.unesco.org/view/{code}?"
        f"units={iso}"
    )

    return {
        "indicator_name": f"UNESCO UIS: {indicator_label} ({country_name})",
        "indicator": f"unesco_uis_{code.lower()}_{iso.lower()}",
        "country": iso,
        "country_name": country_name,
        "year": year_str,
        "value": val,
        "display_value": display,
        "description": description,
        "url": url,
        "source": "UNESCO Institute for Statistics (UIS)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_unesco_uis(analysis: dict) -> dict:
    """Live-Lookup gegen UNESCO UIS fuer globale Bildungs-Claims.

    Strategie:
    1. Cluster aus Claim ableiten (literacy / school_enrollment / OOSC / etc.)
    2. Land(er) aus Claim extrahieren
    3. Fuer jedes Cluster x Land: erste verfuegbare Code-Variante abrufen,
       juengsten Jahres-Record auswaehlen.
    4. Bis zu MAX_RESULTS zurueckgeben.

    Politische Guardrails: Service zitiert nur die offizielle UIS-Statistik;
    KEINE Verdict-Empfehlung. Synthesizer-Layer entscheidet.
    """
    empty = {
        "source": "UNESCO UIS",
        "type": "education_global",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")

    matchable = f"{original} {claim}".lower().strip()
    if not _claim_mentions_unesco_uis(matchable):
        return empty

    clusters = _detect_clusters(matchable)
    if not clusters:
        logger.debug("UNESCO UIS: kein Indikator-Cluster aus Claim ableitbar")
        return empty

    countries = _detect_countries(matchable)
    if not countries:
        # Default: globale Hotspot-Beispiele (NGA = bevoelkerungsreichstes
        # OOSC-Land; KEN = solide Datenlage fuer SDG-4-Vergleiche)
        countries = [("NGA", "Nigeria"), ("KEN", "Kenia")]

    results: list[dict] = []
    seen_indicators: set[str] = set()

    async with polite_client(timeout=TIMEOUT_S) as client:
        for cluster in clusters:
            indicator_specs = _INDICATOR_MAP.get(cluster) or []
            for code, label, unit in indicator_specs:
                if code in seen_indicators:
                    continue
                for iso, country_name in countries:
                    if len(results) >= MAX_RESULTS:
                        break
                    try:
                        records = await _fetch_indicator(client, code, iso)
                    except Exception as e:
                        logger.debug(
                            f"UNESCO UIS: fetch-error {code}/{iso}: {e}"
                        )
                        continue
                    latest = _pick_latest_record(records)
                    if not latest:
                        continue
                    try:
                        r = _build_result(latest, label, unit, country_name)
                    except Exception as e:
                        logger.debug(
                            f"UNESCO UIS: build-error {code}/{iso}: {e}"
                        )
                        continue
                    if r:
                        results.append(r)
                        seen_indicators.add(code)
                if len(results) >= MAX_RESULTS:
                    break
            if len(results) >= MAX_RESULTS:
                break

    if not results:
        logger.info(
            f"UNESCO UIS: 0 Treffer fuer clusters={clusters} "
            f"countries={[c[0] for c in countries]}"
        )
        return empty

    logger.info(
        f"UNESCO UIS: {len(results)} Treffer fuer clusters={clusters} "
        f"countries={[c[0] for c in countries]}"
    )
    return {
        "source": "UNESCO UIS",
        "type": "education_global",
        "results": results,
    }
