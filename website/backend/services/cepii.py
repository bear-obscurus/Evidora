"""CEPII BACI / CHELEM-TRADE Live-Connector — Akademische Welthandels-DB.

CEPII (Centre d'études prospectives et d'informations internationales,
Paris) ist die führende akademische Quelle für harmonisierte bilaterale
Handelsdaten. Zwei Hauptprodukte:

- **BACI**: HS-6-Produktebene, ~200 Länder × Jahre 1995-2023; harmonisiert
  über Reporter-/Partner-Diskrepanzen via Mirror-Reconciliation. Granularer
  als UN Comtrade auf Produktebene, akademischer Trade-Forschungs-Standard.
- **CHELEM-TRADE**: Bilaterale Flüsse seit 1967 in 71 Warenklassen
  (CHELEM-Klassifikation), in Mio. USD. Datenquelle u.a. UN-COMTRADE +
  ergänzt + harmonisiert.

Komplementär zu:
- `uncomtrade.py` (UN Comtrade — offizielle reporter-self-deklarierte
  Daten, jährlich/monatlich, HS-Chapter-Aggregat)
- `dbnomics.py` (Aggregator-Layer — kann auch CEPII liefern, aber via
  /search-Endpoint mit Default-Heuristik; CEPII hier dedizierte
  Bilateral-Trade-Query mit ISO-3-Exporter×Importer-Dimensionen)

API: CEPII selbst stellt nur Bulk-CSV-Downloads via Web-Form bereit
(nicht API-tauglich). Daher: **DBnomics-Pfad als Aggregator-Proxy**,
analog zu IMF/BIS-Pattern.

Endpoint: https://api.db.nomics.world/v22/series/CEPII/CHELEM-TRADE-CHEL/
  {exporter}.{importer}.{secgroup}.{product}?observations=1
Series-Code-Format: ISO-3.ISO-3.{CAT|SEC}.{TT=Total | Produkt-Code}
Free, kein API-Key. Etalab Open Licence 2.0 (Code) + DBnomics ODbL (Daten).

Trigger-Strategie (conservative):
1. Direkt-Trigger: "cepii" / "baci" / "chelem" im Claim
2. Composite: Bilateral-Trade-Verb (Export/Import/Handel) + 2 Länder
   im Claim — hier ist CEPII-Granularität auf Produktebene Mehrwert
   gegenüber Comtrade-Top-Level.
3. Akademik-Composite: "akademisch"/"forschung"/"studie" + Trade-Term

WICHTIG — Politische Guardrails (memory/project_political_guardrails.md):
- Nur deskriptive Trade-Statistik, KEINE Bewertung
- KEINE Schuldzuweisung an Regierungen / Handelspartner
- Mirror-Reconciliation-Hinweis im display_value

Limitations:
- CEPII-Daten lag ~2-3 Jahre (BACI 2023 erst Mitte 2025 publiziert; in
  DBnomics typisch bis 2021/2022 indexiert)
- CHELEM nutzt eigene Country-Codes (DEU/AUT/USA passen, aber spezial
  wie BLX = Belgium+Luxembourg historisch — wir mappen konservativ
  auf Standard-ISO-3)
- Produkt = "TT" (Total Trade) als Default — Sub-Kategorien optional
"""

# WIRING für main.py:
# from services.cepii import search_cepii, claim_mentions_cepii_cached
# if claim_mentions_cepii_cached(claim):
#     tasks.append(cached("CEPII BACI", search_cepii, analysis))
#     queried_names.append("CEPII BACI")

from __future__ import annotations

import logging
import re
import time
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

DBNOMICS_API = "https://api.db.nomics.world/v22"
# CHELEM-TRADE-CHEL ist das pragmatische Default-Dataset für bilaterale
# Aggregate (71 Kategorien, ISO-3-kompatible Codes, seit 1967).
# CHELEM-TRADE-ISIC + CHELEM-TRADE-GTAP sind Geschwister-Datasets;
# BACI selbst (HS-6) ist über CEPII-Web nur als Bulk-CSV verfügbar.
CHELEM_DATASET = "CHELEM-TRADE-CHEL"
TIMEOUT_S = 15.0
MAX_RESULTS = 3
CACHE_TTL_S = 24 * 3600  # 24h

# ---------------------------------------------------------------------------
# Trigger-Lexikon
# ---------------------------------------------------------------------------
# Direkt-Trigger: namentliche Erwähnung von CEPII oder seinen Produkten
_DIRECT_TERMS = (
    "cepii", "baci", "chelem",
)

# Trade-Verben/Substantive (analog uncomtrade.py — wir nutzen einen
# kleineren Kern, weil CEPII NUR triggern soll wenn echt bilateral
# oder akademisch-Fokus vorhanden ist)
_TRADE_TERMS = (
    "bilateral", "bilateraler handel", "bilaterales handelsvolumen",
    "handel", "handelsstrom", "handelsströme",
    "handelsfluss", "handelsflüsse",
    "trade", "trade flow", "trade flows", "trade volume",
    "exportiert", "exportier", "export", "exports", "ausfuhr",
    "importiert", "importier", "import", "imports", "einfuhr",
    "handelsbilanz", "warenstrom", "warenströme",
)

# Akademik-Kontext (komplementär — wenn jemand explizit nach
# akademischer Forschungs-Quelle fragt, ist CEPII die bessere Wahl
# als Comtrade Public)
_ACADEMIC_TERMS = (
    "akademisch", "akademische", "wissenschaftlich",
    "forschung", "trade-forschung", "trade research",
    "studie", "studien", "harmonisiert", "harmonisierte",
    "hs-6", "hs6", "produktcode", "produktebene",
)

# Country-Mapping (DE/EN-Alias → ISO-3). CEPII-CHELEM nutzt ISO-3
# weitgehend identisch zum UN Comtrade-Stack — wir spiegeln daher
# das uncomtrade.py-Mapping, halten es aber lokal kopiert um keine
# zirkulären Imports / Layering-Probleme zu erzeugen.
_COUNTRY_ALIASES: dict[str, list[str]] = {
    "AUT": ["österreich", "austria"],
    "DEU": ["deutschland", "germany"],
    "CHE": ["schweiz", "switzerland"],
    "USA": ["usa", "vereinigte staaten", "united states", "amerika"],
    "CHN": ["china"],
    "RUS": ["russland", "russia"],
    "GBR": ["großbritannien", "united kingdom", "vereinigtes königreich", "uk"],
    "FRA": ["frankreich", "france"],
    "ITA": ["italien", "italy"],
    "ESP": ["spanien", "spain"],
    "POL": ["polen", "poland"],
    "TUR": ["türkei", "turkey", "türkiye"],
    "JPN": ["japan"],
    "IND": ["indien", "india"],
    "BRA": ["brasilien", "brazil"],
    "KOR": ["südkorea", "south korea", "korea"],
    "NLD": ["niederlande", "netherlands", "holland"],
    "BEL": ["belgien", "belgium"],
    "CZE": ["tschechien", "czech", "czechia"],
    "HUN": ["ungarn", "hungary"],
    "SWE": ["schweden", "sweden"],
    "DNK": ["dänemark", "denmark"],
    "NOR": ["norwegen", "norway"],
    "FIN": ["finnland", "finland"],
    "PRT": ["portugal"],
    "GRC": ["griechenland", "greece"],
    "IRL": ["irland", "ireland"],
    "ROU": ["rumänien", "romania"],
    "BGR": ["bulgarien", "bulgaria"],
    "HRV": ["kroatien", "croatia"],
    "SVN": ["slowenien", "slovenia"],
    "SVK": ["slowakei", "slovakia"],
    "UKR": ["ukraine"],
    "MEX": ["mexiko", "mexico"],
    "CAN": ["kanada", "canada"],
    "AUS": ["australien", "australia"],
    "ZAF": ["südafrika", "south africa"],
    "EGY": ["ägypten", "egypt"],
    "IDN": ["indonesien", "indonesia"],
    "VNM": ["vietnam"],
    "THA": ["thailand"],
    "MYS": ["malaysia"],
    "SGP": ["singapur", "singapore"],
    "ARG": ["argentinien", "argentina"],
}

# Anzeige-Namen für display_value (statt nackter ISO-3)
_DISPLAY_NAMES: dict[str, str] = {
    "AUT": "Österreich", "DEU": "Deutschland", "CHE": "Schweiz",
    "USA": "USA", "CHN": "China", "RUS": "Russland",
    "GBR": "Vereinigtes Königreich", "FRA": "Frankreich",
    "ITA": "Italien", "ESP": "Spanien", "POL": "Polen",
    "TUR": "Türkei", "JPN": "Japan", "IND": "Indien",
    "BRA": "Brasilien", "KOR": "Südkorea", "NLD": "Niederlande",
    "BEL": "Belgien", "CZE": "Tschechien", "HUN": "Ungarn",
    "SWE": "Schweden", "DNK": "Dänemark", "NOR": "Norwegen",
    "FIN": "Finnland", "PRT": "Portugal", "GRC": "Griechenland",
    "IRL": "Irland", "ROU": "Rumänien", "BGR": "Bulgarien",
    "HRV": "Kroatien", "SVN": "Slowenien", "SVK": "Slowakei",
    "UKR": "Ukraine", "MEX": "Mexiko", "CAN": "Kanada",
    "AUS": "Australien", "ZAF": "Südafrika", "EGY": "Ägypten",
    "IDN": "Indonesien", "VNM": "Vietnam", "THA": "Thailand",
    "MYS": "Malaysia", "SGP": "Singapur", "ARG": "Argentinien",
}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_cepii(claim_lc: str) -> bool:
    """Conservative Trigger:
    1. Direkter Term (cepii/baci/chelem) → True
    2. Bilateral-Term + ≥2 Länder → True
    3. Akademik-Term + Trade-Term → True
    """
    if not claim_lc:
        return False

    # 1. Direkt
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    countries = _detect_countries(claim_lc)
    has_trade = any(t in claim_lc for t in _TRADE_TERMS)
    has_academic = any(t in claim_lc for t in _ACADEMIC_TERMS)

    # 2. Bilateral + 2 Länder
    if has_trade and len(countries) >= 2:
        return True

    # 3. Akademisch + Trade-Kontext
    if has_academic and has_trade:
        return True

    return False


# Modul-Level-Cache: (claim_lc) → (ts, result)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_cepii_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_cepii(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    # Cache-Hygiene: bei >500 Einträgen ältere 100 droppen
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
# Country detection
# ---------------------------------------------------------------------------
def _detect_countries(claim_lc: str) -> list[str]:
    """Detect ISO-3-Codes mentioned in claim (longest-alias-first).
    Returns up to 3 ISO-3-Codes in order of first occurrence.

    Akzeptiert auch nackte ISO-3-Codes (DEU/FRA/AUT…) wenn als
    Wort-Token im Claim. Wortgrenze verhindert False-Positives
    wie "ind" in "individuell" oder "che" in "Recherche".
    """
    if not claim_lc:
        return []
    found: list[tuple[int, str]] = []
    seen: set[str] = set()
    for iso3, aliases in _COUNTRY_ALIASES.items():
        # Longest-alias-first verhindert dass kurzer Alias (z.B. "uk")
        # einen längeren Alias maskiert.
        for alias in sorted(aliases, key=len, reverse=True):
            idx = claim_lc.find(alias)
            if idx >= 0 and iso3 not in seen:
                found.append((idx, iso3))
                seen.add(iso3)
                break
        if iso3 in seen:
            continue
        # ISO-3-Code als nacktes Token mit Wortgrenze (\b…\b).
        # Beispiel: "DEU FRA" / "BACI DEU AUT" / "Export DEU→FRA".
        m = re.search(rf"\b{iso3.lower()}\b", claim_lc)
        if m is not None:
            found.append((m.start(), iso3))
            seen.add(iso3)
    found.sort(key=lambda t: t[0])
    return [iso for _, iso in found[:3]]


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------
def _format_value_musd(value: float | int | None) -> str:
    """CHELEM-Werte kommen in Mio USD. Wir formatieren auf Mrd/Mio USD."""
    if value is None:
        return "k. A."
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "k. A."
    # Wert ist bereits in Mio USD
    if abs(v) >= 1000:
        return f"{v / 1000:.2f} Mrd USD"
    if abs(v) >= 1:
        return f"{v:.1f} Mio USD"
    return f"{v * 1000:.0f} Tsd USD"


def _pick_latest_observation(
    periods: list, values: list,
) -> tuple[str | None, float | None]:
    """Letzte numerische Observation aus parallelen Listen."""
    if not periods or not values:
        return None, None
    for p, v in zip(reversed(periods), reversed(values)):
        if v is None:
            continue
        try:
            return str(p), float(v)
        except (TypeError, ValueError):
            continue
    return None, None


# ---------------------------------------------------------------------------
# HTTP-Calls
# ---------------------------------------------------------------------------
async def _fetch_bilateral_series(
    client, exporter_iso3: str, importer_iso3: str,
) -> dict | None:
    """Fetch DEU.AUT.CAT.TT-style series for bilateral total trade.

    Format: /v22/series/CEPII/CHELEM-TRADE-CHEL/{EXP}.{IMP}.CAT.TT
    secgroup = CAT (CHELEM-Categories, default-Tabelle)
    product = TT (Total Trade — summiert alle 71 Kategorien)
    """
    series_code = f"{exporter_iso3}.{importer_iso3}.CAT.TT"
    cache_key = f"series::{series_code}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached[0] if cached else None

    url = (
        f"{DBNOMICS_API}/series/CEPII/{CHELEM_DATASET}/"
        f"{quote_plus(series_code)}?observations=1"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"CEPII series HTTP {resp.status_code} für {series_code}"
            )
            _cache_put(cache_key, [])
            return None
        data = resp.json()
    except Exception as e:
        logger.debug(f"CEPII series-fetch failed für {series_code}: {e}")
        return None

    docs = (data.get("series") or {}).get("docs") or []
    if not docs:
        _cache_put(cache_key, [])
        return None
    _cache_put(cache_key, [docs[0]])
    return docs[0]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_cepii(analysis: dict) -> dict:
    """Live-Lookup gegen CEPII via DBnomics-Aggregator.

    Strategy:
    1. Trigger-Check (claim_mentions_cepii_cached). Nein → empty.
    2. Detect ≥1 Land aus Claim. Bei 2 Ländern → bilateral.
    3. Bei nur 1 Land + Direkt-Trigger → Total-Exports gegen
       Welt-Aggregat WLD ist NICHT verfügbar in CHELEM (eigene
       Aggregate-Codes wie ASO/AME etc., zu speziell für Auto-Lookup).
       Daher: Bei nur 1 Land Skip mit Hinweis im Log.
    4. Series-Query DEU.AUT.CAT.TT + Mirror AUT.DEU.CAT.TT für
       Symmetrie-Vergleich (max 2 Calls/Claim).
    5. Format display_value mit Mio→Mrd-USD-Konvertierung.
    """
    empty = {"source": "CEPII BACI", "type": "bilateral_trade", "results": []}

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_cepii(matchable):
        return empty

    countries = _detect_countries(matchable)
    if not countries:
        # NER-Fallback aus claim_analyzer
        ner = (analysis or {}).get("ner_entities", {}).get("countries", []) or []
        if ner:
            countries = _detect_countries(" ".join(ner).lower())

    if len(countries) < 2:
        # CHELEM kann ohne klares Reporter×Partner-Paar nicht sinnvoll
        # gequeried werden (kein einfaches "World"-Total wie in Comtrade).
        # Direkter Trigger ("cepii"/"baci") ohne Länder → kontextlos.
        logger.info(
            f"CEPII: Trigger ja, aber <2 Länder erkannt "
            f"(found={countries}) — skip Live-Call."
        )
        return empty

    exporter = countries[0]
    importer = countries[1]

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        # Query 1: Exports {exporter} → {importer}
        doc_x = await _fetch_bilateral_series(client, exporter, importer)
        # Query 2: Mirror Exports {importer} → {exporter}
        doc_m = await _fetch_bilateral_series(client, importer, exporter)

    if not doc_x and not doc_m:
        logger.info(
            f"CEPII: 0 Treffer für {exporter}↔{importer} "
            f"({CHELEM_DATASET}.CAT.TT)"
        )
        return empty

    # Extrahiere letzte Observation aus beiden Richtungen
    period_x = value_x = period_m = value_m = None
    if doc_x:
        period_x, value_x = _pick_latest_observation(
            doc_x.get("period") or [], doc_x.get("value") or [],
        )
    if doc_m:
        period_m, value_m = _pick_latest_observation(
            doc_m.get("period") or [], doc_m.get("value") or [],
        )

    if value_x is None and value_m is None:
        return empty

    exp_name = _DISPLAY_NAMES.get(exporter, exporter)
    imp_name = _DISPLAY_NAMES.get(importer, importer)

    # display_value: bilateral mit Mirror, plus harmonisierungs-Hinweis
    parts: list[str] = []
    if value_x is not None and period_x:
        parts.append(
            f"{exp_name} → {imp_name} ({period_x}): "
            f"{_format_value_musd(value_x)}"
        )
    if value_m is not None and period_m:
        parts.append(
            f"{imp_name} → {exp_name} ({period_m}): "
            f"{_format_value_musd(value_m)}"
        )
    bilateral_str = "; ".join(parts)

    # Konsolidierter Vergleichs-Period (für indicator-Year)
    year_str = period_x or period_m or "—"

    display = (
        f"Bilateraler Warenhandel laut CEPII-CHELEM (akademisch "
        f"harmonisiert, Mirror-Reconciliation): {bilateral_str}. "
        f"Quelle: CEPII CHELEM-TRADE, via DBnomics-Aggregator. "
        f"Hinweis: CEPII-Daten lag ~2-3 Jahre — letzter publizierter "
        f"Jahrgang typisch {year_str}; BACI HS-6-Detaildaten via Bulk-"
        f"Download verfügbar, hier Aggregat in 71 CHELEM-Kategorien."
    )[:700]

    secondary = (
        f"https://db.nomics.world/CEPII/{CHELEM_DATASET}/"
        f"{exporter}.{importer}.CAT.TT"
    )

    results.append({
        "indicator_name": (
            f"CEPII CHELEM: {exp_name}↔{imp_name} Bilateraler Handel"
            f" ({year_str})"
        ),
        "indicator": f"cepii_chelem_{exporter.lower()}_{importer.lower()}",
        "country": f"{exporter}/{importer}",
        "country_name": f"{exp_name} / {imp_name}",
        "year": year_str,
        "topic": "cepii_bilateral_trade",
        "value": value_x if value_x is not None else value_m,
        "display_value": display,
        "description": (
            "Bilaterale Warenströme via CEPII CHELEM-TRADE (akademisch "
            "harmonisiert seit 1967, 71 Warenklassen, Mio USD). Quellen-"
            "Basis: UN-COMTRADE + ergänzende Daten, mit Mirror-"
            "Reconciliation (Exports von A→B = Imports von B←A nach "
            "Harmonisierung). Nur deskriptive Trade-Statistik — keine "
            "politische Bewertung."
        ),
        "url": "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele.asp",
        "secondary_url": secondary,
        "source": (
            "CEPII BACI / CHELEM-TRADE (Centre d'études prospectives et "
            "d'informations internationales, via DBnomics; "
            "Etalab Open Licence 2.0)"
        ),
    })

    logger.info(
        f"CEPII: 1 Treffer für {exporter}↔{importer} "
        f"({year_str}, exp={value_x}, mirror={value_m})"
    )
    return {
        "source": "CEPII BACI",
        "type": "bilateral_trade",
        "results": results,
    }
