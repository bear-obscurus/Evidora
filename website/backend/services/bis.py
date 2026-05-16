"""BIS — Bank for International Settlements (Live-Statistik-Adapter).

Quelle: Bank for International Settlements (Basel, CH). Die BIS aggregiert
internationale Banken-Statistik, die nirgendwo sonst zentral verfügbar ist:

  - Locational Banking Statistics (LBS): grenzüberschreitende Forderungen
    und Verbindlichkeiten nach Banken-Standort
  - Consolidated Banking Statistics (CBS): konsolidiertes Auslandsengagement
  - International Debt Securities: Schuldverschreibungen-Statistik
  - Residential / Commercial Property Prices: Wohnimmobilien-Preisindex
    (Welt-Vergleich; ergänzt das AT-Wohnen-Pack mit internationalem Bezug)
  - Global Liquidity Indicators: globale USD-/EUR-/JPY-Liquidität
  - Central Bank Policy Rates: Leitzinsen (vergleichend international)

Komplementär zu:
  - OeNB (AT-Geldpolitik / EZB-Leitzins national)
  - IMF/DBnomics (BIP, Inflation, Schuldenquote als Aggregat)
  - housing_at / wohnen_pack (rein AT-fokussiert)

API-Pfade:
  1. PRIMÄR: DBnomics-Aggregator → https://api.db.nomics.world/v22/series/BIS/...
     stabil, JSON-Schema einheitlich, indexiert alle BIS-Datasets.
  2. FALLBACK: BIS direkt → https://stats.bis.org/api/v2/data/{flow}/{key}
     SDMX 2.1.0 REST. Schwierig (Accept-Header strikt, viele 406/500), wird
     daher nur als Sekundär-Versuch genutzt wenn DBnomics-Path leer bleibt.

Lizenz: BIS Open Data — Quellenangabe verpflichtend (siehe ``source``-Feld
in jedem Result). Daten frei für Forschung/Bildung/Faktencheck.

Politische Guardrails (memory/project_political_guardrails.md):
  - Pure Statistik-Wiedergabe, KEINE Bewertung
  - Keine Aussagen zur "Solidität" von Banken/Ländern
  - Keine politische Interpretation von Cross-Border-Flows

WIRING für main.py:
  from services.bis import search_bis, claim_mentions_bis_cached
  if claim_mentions_bis_cached(claim):
      tasks.append(cached("BIS", search_bis, analysis))
      queried_names.append("BIS")
"""

from __future__ import annotations

import json
import logging
import time
from urllib.parse import quote, quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Endpoints + Konfiguration
# ---------------------------------------------------------------------------
BIS_DIRECT_API = "https://stats.bis.org/api/v2/data"
DBNOMICS_API = "https://api.db.nomics.world/v22"
TIMEOUT_S = 20.0
MAX_RESULTS = 4
CACHE_TTL_S = 24 * 3600  # 24h

# SDMX-JSON Content-Negotiation (BIS akzeptiert nur sehr wenige Varianten;
# wir versuchen mehrere). Falls keine funktioniert → DBnomics-Fallback.
_BIS_ACCEPTS = (
    "application/vnd.sdmx.data+json;version=1.0.0",
    "application/vnd.sdmx.data+json",
    "application/json",
)

# ---------------------------------------------------------------------------
# Dataset-Mapping
# ---------------------------------------------------------------------------
# Pro Dataset:
#   - direct_flow:  Dataflow-Code für BIS-direct (stats.bis.org)
#   - dbnomics_ds:  Dataset-Code in DBnomics (api.db.nomics.world)
#   - title_de:     Anzeigetext deutsch
#   - frequency:    "quarterly" / "monthly" / "daily" — Methoden-Hinweis
#   - topic_url:    Deep-Link zur BIS-Topic-Seite
#   - description:  längerer Methoden-Hinweis
#   - country_dim:  Dimension-Key für Ländercode in DBnomics-Filter
#
# Auswahl: 5 Datasets, die die wichtigsten BIS-Trigger-Cluster abdecken.
_DATASETS: dict[str, dict] = {
    "LBS": {
        "direct_flow": "BIS,LBS_DSET,1.0",
        "dbnomics_ds": "WS_LBS_D_PUB",
        "title_de": "Locational Banking Statistics (grenzüberschreitend)",
        "frequency": "quarterly",
        "topic_url": "https://data.bis.org/topics/LBS",
        "description": (
            "BIS Locational Banking Statistics — quartalsweise Erhebung "
            "grenzüberschreitender Forderungen und Verbindlichkeiten von "
            "Banken nach Standortprinzip. Verfügbar in USD, EUR, JPY, CHF."
        ),
        "country_dim": "L_REP_CTY",
    },
    "CBS": {
        "direct_flow": "BIS,CBS_DSET,1.0",
        "dbnomics_ds": "WS_CBS_PUB",
        "title_de": "Consolidated Banking Statistics",
        "frequency": "quarterly",
        "topic_url": "https://data.bis.org/topics/CBS",
        "description": (
            "BIS Consolidated Banking Statistics — konsolidiertes "
            "Auslandsengagement nach Heimatland der Bank "
            "(immediate vs. ultimate-risk-basis), quartalsweise."
        ),
        "country_dim": "CBS_BANK_TYPE",
    },
    "DSS": {
        "direct_flow": "BIS,WS_DEBT_SEC2_PUB,1.0",
        "dbnomics_ds": "WS_DEBT_SEC2_PUB",
        "title_de": "International Debt Securities Statistics",
        "frequency": "quarterly",
        "topic_url": "https://data.bis.org/topics/DSS",
        "description": (
            "BIS International Debt Securities — quartalsweise Statistik "
            "internationaler Schuldverschreibungen nach Emittent, "
            "Währung und Laufzeit."
        ),
        "country_dim": "ISSUER_RES",
    },
    "RPP": {
        "direct_flow": "BIS,WS_DPP,1.0",
        "dbnomics_ds": "WS_DPP",
        "title_de": "Residential Property Prices (international)",
        "frequency": "quarterly",
        "topic_url": "https://data.bis.org/topics/PP/RPP",
        "description": (
            "BIS Detailed Residential Property Prices — Wohnimmobilien-"
            "Preisindex für ~60 Länder, quartalsweise. Methodische "
            "Harmonisierung erlaubt Ländervergleich (anders als rein "
            "nationale Statistiken)."
        ),
        "country_dim": "REF_AREA",
    },
    "CBPOL": {
        "direct_flow": "BIS,WS_CBPOL,1.0",
        "dbnomics_ds": "WS_CBPOL",
        "title_de": "Central Bank Policy Rates (international)",
        "frequency": "daily",
        "topic_url": "https://data.bis.org/topics/CBPOL",
        "description": (
            "BIS Central Bank Policy Rates — Leitzinsen aller "
            "Zentralbanken im internationalen Vergleich (täglich). "
            "Für EZB-Spezial siehe OeNB-Pack."
        ),
        "country_dim": "REF_AREA",
    },
}

# ---------------------------------------------------------------------------
# Country-ISO-3 + DBnomics-2-letter-Mapping
# ---------------------------------------------------------------------------
# DBnomics nutzt ISO-2 in den BIS-Dimensionen (AT, DE, US, …), wir geben aber
# nach außen ISO-3 zurück (Konvention im Evidora-Schema, vgl. dbnomics.py).
_COUNTRY_HINTS: dict[str, tuple[str, str, str]] = {
    # hint → (iso2_dbnomics, iso3, display_name)
    "österreich": ("AT", "AUT", "Österreich"),
    "austria": ("AT", "AUT", "Österreich"),
    "deutschland": ("DE", "DEU", "Deutschland"),
    "germany": ("DE", "DEU", "Deutschland"),
    "schweiz": ("CH", "CHE", "Schweiz"),
    "switzerland": ("CH", "CHE", "Schweiz"),
    "usa": ("US", "USA", "USA"),
    "vereinigte staaten": ("US", "USA", "USA"),
    "united states": ("US", "USA", "USA"),
    "großbritannien": ("GB", "GBR", "Vereinigtes Königreich"),
    "uk": ("GB", "GBR", "Vereinigtes Königreich"),
    "united kingdom": ("GB", "GBR", "Vereinigtes Königreich"),
    "frankreich": ("FR", "FRA", "Frankreich"),
    "france": ("FR", "FRA", "Frankreich"),
    "italien": ("IT", "ITA", "Italien"),
    "italy": ("IT", "ITA", "Italien"),
    "spanien": ("ES", "ESP", "Spanien"),
    "spain": ("ES", "ESP", "Spanien"),
    "japan": ("JP", "JPN", "Japan"),
    "china": ("CN", "CHN", "China"),
    "niederlande": ("NL", "NLD", "Niederlande"),
    "netherlands": ("NL", "NLD", "Niederlande"),
    "luxemburg": ("LU", "LUX", "Luxemburg"),
    "luxembourg": ("LU", "LUX", "Luxemburg"),
    "belgien": ("BE", "BEL", "Belgien"),
    "belgium": ("BE", "BEL", "Belgien"),
}

# ---------------------------------------------------------------------------
# Trigger-Terme
# ---------------------------------------------------------------------------
# Direkt-Trigger: Claim erwähnt BIS namentlich
_DIRECT_TERMS = (
    "bis ", " bis ", "bis-",
    "bank for international settlements",
    "bank für internationalen zahlungsausgleich",
    "bank fuer internationalen zahlungsausgleich",
    "bis-statistik", "bis statistik",
    "bis-banken", "bis banken",
)

# Themen-Trigger: BIS-spezifische Kern-Themen
_TOPIC_TERMS = (
    "cross-border-banking", "cross border banking",
    "grenzüberschreitende banken", "grenzueberschreitende banken",
    "internationale banken", "internationaler bankenverkehr",
    "globale liquidität", "global liquidity",
    "internationale liquidität",
    "wohnimmobilienpreise international",
    "residential property prices",
    "property price index", "international property prices",
    "international house prices",
    "weltimmobilienpreise",
    "banken-statistik", "banken statistik",
    "lbs", "locational banking",
    "cbs", "consolidated banking statistics",
    "debt securities statistics",
    "internationale schuldverschreibungen",
    "fx settlement", "fx-settlement",
    "internationale leitzinsen",
    "policy rates international",
    "leitzins-vergleich",
)

# Dataset-spezifische Trigger → forciert Auswahl eines bestimmten Datasets
_DATASET_HINTS: dict[str, str] = {
    "lbs": "LBS",
    "locational banking": "LBS",
    "cross-border-banking": "LBS",
    "grenzüberschreitende banken": "LBS",
    "internationale banken": "LBS",
    "cbs": "CBS",
    "consolidated banking": "CBS",
    "konsolidierte banken": "CBS",
    "debt securities": "DSS",
    "internationale schuldverschreibungen": "DSS",
    "international debt": "DSS",
    "property price": "RPP",
    "wohnimmobilienpreise": "RPP",
    "house prices international": "RPP",
    "weltimmobilienpreise": "RPP",
    "internationale leitzinsen": "CBPOL",
    "policy rates international": "CBPOL",
    "leitzins-vergleich": "CBPOL",
    "globale liquidität": "LBS",
    "global liquidity": "LBS",
}


# ---------------------------------------------------------------------------
# Trigger-Logik
# ---------------------------------------------------------------------------
def _claim_mentions_bis(claim_lc: str) -> bool:
    """Conservative Trigger:
    1. Direkter BIS-Term → True
    2. BIS-spezifisches Thema → True
    3. Cross-Cluster: generischer Banken-Krisen-Term + International →
       True (komplementär zu OeNB/IMF)
    """
    if not claim_lc:
        return False

    # 1. Direkter BIS-Mention.  Achtung: "bis" als deutsche Präposition
    # (z.B. "bis 2030") darf nicht triggern → wir verlangen entweder
    # einen längeren BIS-Term ODER einen Banken-Kontext im Claim.
    has_bis_word = False
    for t in _DIRECT_TERMS:
        if t in claim_lc:
            # nur "bis" allein ist mehrdeutig — andere Terme sind eindeutig
            if t.strip() == "bis":
                # Mehrdeutig: nur akzeptieren wenn parallel Banken-Kontext
                if any(b in claim_lc for b in (
                    "bank", "banken", "banking",
                    "statistik", "zahlungsausgleich",
                )):
                    has_bis_word = True
            else:
                has_bis_word = True
            if has_bis_word:
                break
    if has_bis_word:
        return True

    # 2. BIS-typische Themen
    if any(t in claim_lc for t in _TOPIC_TERMS):
        return True

    # 3. Cross-Cluster: Banken-Krise + International → BIS feuert parallel
    has_bank_crisis = any(t in claim_lc for t in (
        "bankenkrise", "banken-krise", "banking crisis",
        "finanzkrise international", "globale finanzkrise",
        "systemrisiko banken", "systemic risk banks",
    ))
    has_intl = any(t in claim_lc for t in (
        "international", "global", "weltweit", "world",
        "länder", "laender",
    ))
    if has_bank_crisis and has_intl:
        return True

    return False


# Modul-Level-Cache für Trigger-Check (24h)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_bis_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den BIS-Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_bis(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    # Cache-Hygiene: bei >500 Einträgen die ältesten 100 droppen
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
# Helpers — Country/Dataset-Auswahl
# ---------------------------------------------------------------------------
def _pick_country(claim_lc: str) -> tuple[str, str, str]:
    """Erstes erkanntes Land → (iso2, iso3, anzeige).
    Fallback AUT/Österreich (häufigster Evidora-Faktencheck-Bezug)."""
    for hint, (iso2, iso3, name) in _COUNTRY_HINTS.items():
        if hint in claim_lc:
            return iso2, iso3, name
    return "AT", "AUT", "Österreich"


def _pick_datasets(claim_lc: str) -> list[str]:
    """Liste der zu queryenden Dataset-Keys (max. 2).
    Wenn spezifischer Hinweis → diesen + LBS als Default.
    Sonst nur LBS (verlässlichster BIS-Indikator)."""
    picked: list[str] = []
    for hint, ds_key in _DATASET_HINTS.items():
        if hint in claim_lc and ds_key not in picked:
            picked.append(ds_key)
            if len(picked) >= 2:
                break
    if not picked:
        # Generisch — LBS (Cross-Border-Banking) als robuster Default
        picked = ["LBS"]
    return picked


# ---------------------------------------------------------------------------
# DBnomics-Fetch (primärer Pfad)
# ---------------------------------------------------------------------------
async def _fetch_dbnomics_series(
    client, dataset_key: str, iso2: str,
) -> list[dict]:
    """Hole 1 BIS-Zeitreihe via DBnomics für (Dataset, ISO-2-Land).

    Strategie: Dimension-Filter mit erstem Treffer pro Land. DBnomics
    liefert pro Aufruf bis zu 1000 Series; wir holen nur die ersten 5
    und nehmen davon die mit der jüngsten Beobachtung.
    """
    ds = _DATASETS.get(dataset_key)
    if not ds:
        return []
    dbnomics_ds = ds["dbnomics_ds"]
    country_dim = ds["country_dim"]

    cache_key = f"bis::dbnomics::{dbnomics_ds}::{iso2}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # Dimension-Filter: {"L_REP_CTY":["AT"]}
    dim_filter = json.dumps({country_dim: [iso2]})
    url = (
        f"{DBNOMICS_API}/series/BIS/{dbnomics_ds}"
        f"?limit=5&observations=1&dimensions={quote_plus(dim_filter)}"
    )

    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"BIS/DBnomics HTTP {resp.status_code} "
                f"for {dbnomics_ds}/{iso2}"
            )
            _cache_put(cache_key, [])
            return []
        data = resp.json()
    except Exception as e:
        logger.debug(
            f"BIS/DBnomics fetch failed {dbnomics_ds}/{iso2}: {e}"
        )
        return []

    docs = (data.get("series") or {}).get("docs") or []
    _cache_put(cache_key, docs)
    return docs


def _format_dbnomics_result(
    doc: dict, dataset_key: str, iso3: str, country_name: str,
) -> dict | None:
    """Wandle 1 DBnomics-Series-Doc in Evidora-Result-Schema um."""
    ds = _DATASETS.get(dataset_key)
    if not ds:
        return None
    periods = doc.get("period") or []
    values = doc.get("value") or []
    if not periods or not values:
        return None

    latest_period = "—"
    latest_value = None
    # DBnomics liefert chronologisch — letzte non-null-Beobachtung
    for p, v in zip(reversed(periods), reversed(values)):
        if v is not None:
            latest_period = str(p)
            latest_value = v
            break
    if latest_value is None:
        return None

    series_code = doc.get("series_code") or "?"
    series_name = (doc.get("series_name") or "").strip()

    # Year aus Period extrahieren (Formate: "2024-Q3", "2024-12", "2024-12-31")
    year = latest_period[:4] if len(latest_period) >= 4 else "—"

    # Display-String — Dataset-spezifisch leicht formuliert
    if dataset_key == "LBS":
        unit = "Indexpunkte/Mrd."  # je nach Series unterschiedlich
        head = f"BIS Cross-Border-Banken-Statistik {country_name}"
    elif dataset_key == "CBS":
        unit = "Mrd. USD"
        head = f"BIS Konsolidierte Banken-Statistik {country_name}"
    elif dataset_key == "DSS":
        unit = "Mrd. USD"
        head = f"BIS International Debt Securities {country_name}"
    elif dataset_key == "RPP":
        unit = "Indexpunkte (Basis = 100)"
        head = f"BIS Wohnimmobilien-Preisindex {country_name}"
    elif dataset_key == "CBPOL":
        unit = "% p.a."
        head = f"BIS-Leitzins-Statistik {country_name}"
    else:
        unit = ""
        head = f"BIS-{dataset_key} {country_name}"

    if isinstance(latest_value, float):
        val_str = f"{latest_value:.2f}"
    else:
        val_str = str(latest_value)

    display = (
        f"{head} — letzter Wert {val_str} {unit} ({latest_period}). "
        f"Serie: {series_code}."
    )

    return {
        "indicator_name": (
            f"BIS {ds['title_de']} — {country_name}"
        )[:300],
        "indicator": f"bis_{dataset_key.lower()}_{iso3.lower()}",
        "country": iso3,
        "country_name": country_name,
        "year": year,
        "value": latest_value,
        "display_value": display,
        "description": (
            f"{ds['description']} Frequenz: {ds['frequency']}. "
            f"Serie-Beschreibung: {series_name[:160]}. "
            "Reine Statistik-Wiedergabe — keine Bewertung."
        ),
        "url": ds["topic_url"],
        "source": "BIS (Bank for International Settlements)",
    }


# ---------------------------------------------------------------------------
# BIS-direct-Fetch (Fallback / Sekundär-Pfad)
# ---------------------------------------------------------------------------
async def _try_bis_direct(
    client, dataset_key: str, iso2: str,
) -> bool:
    """Sondiere ob BIS-direkt erreichbar ist (HEAD-artiger Probe-Call).

    Wir bauen eine sehr generische Series-Key-Anfrage (mit Wildcards
    soweit erlaubt) und prüfen nur den HTTP-Status. Bei 200 könnten wir
    parsen — in der Praxis ist BIS-direkt momentan instabil (406/500),
    deshalb dient diese Funktion primär als Indikator für Logs.

    Returns True wenn HTTP 200, sonst False.
    """
    ds = _DATASETS.get(dataset_key)
    if not ds:
        return False
    flow = ds["direct_flow"]
    # Wildcard-Key — BIS akzeptiert "all"-Slices sehr selten, das ist
    # bewusst ein Probe-Call.
    url = f"{BIS_DIRECT_API}/{quote(flow, safe=',')}/all"
    for accept in _BIS_ACCEPTS:
        try:
            resp = await client.get(
                url, headers={"Accept": accept},
                follow_redirects=True,
            )
            if resp.status_code == 200:
                logger.debug(
                    f"BIS-direct OK ({accept}) {dataset_key}/{iso2}"
                )
                return True
        except Exception as e:
            logger.debug(f"BIS-direct probe failed: {e}")
    return False


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_bis(analysis: dict) -> dict:
    """Live-Lookup gegen BIS via DBnomics-Aggregator (primärer Pfad).

    Fallback: bei leerer Antwort sondieren wir BIS-direkt; in der Praxis
    aktuell instabil, daher dient er als Telemetrie. Resultat wird nur
    auf Basis von DBnomics formatiert (zuverlässig + einheitliches Schema).
    """
    empty = {
        "source": "BIS",
        "type": "bis_statistics",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_bis(matchable):
        return empty

    iso2, iso3, country_name = _pick_country(matchable)
    dataset_keys = _pick_datasets(matchable)

    results: list[dict] = []
    fallback_attempted = False

    async with polite_client(timeout=TIMEOUT_S) as client:
        # PRIMÄRPFAD: DBnomics-Aggregator
        for dataset_key in dataset_keys:
            docs = await _fetch_dbnomics_series(client, dataset_key, iso2)
            for doc in docs:
                try:
                    r = _format_dbnomics_result(
                        doc, dataset_key, iso3, country_name,
                    )
                except Exception as e:
                    logger.debug(f"BIS doc-format-error: {e}")
                    continue
                if r:
                    results.append(r)
                    break  # 1 Result pro Dataset reicht
            if len(results) >= MAX_RESULTS:
                break

        # FALLBACK-SONDIERUNG: nur wenn DBnomics leer
        if not results:
            fallback_attempted = True
            ok = await _try_bis_direct(client, dataset_keys[0], iso2)
            if ok:
                logger.info(
                    "BIS-direct erreichbar, aber Parser nicht aktiv "
                    f"(dataset={dataset_keys[0]}, country={iso3})"
                )
            else:
                logger.info(
                    f"BIS: 0 Treffer + BIS-direct nicht erreichbar "
                    f"(datasets={dataset_keys}, country={iso3})"
                )

    if not results:
        logger.info(
            f"BIS: 0 Treffer für country={iso3} datasets={dataset_keys} "
            f"fallback_probe={fallback_attempted}"
        )
        return empty

    logger.info(
        f"BIS: {len(results)} Treffer für country={iso3} "
        f"datasets={dataset_keys}"
    )
    return {
        "source": "BIS",
        "type": "bis_statistics",
        "results": results,
    }
