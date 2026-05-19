"""DBnomics — Meta-Aggregator für internationale Wirtschafts-Zeitreihen.

DBnomics (https://db.nomics.world/) aggregiert 50.000+ Datensets von
ECB, IMF, OECD, BIS, World Bank, FRED, OeNB, WIFO, CEPII, INSEE, Eurostat
etc. in EINEM einheitlichen JSON-Schema.

Für Evidora ergänzt der Service die bestehenden AT/DE-Quellen (OeNB,
Statistik Austria, destatis, Eurostat-Light) mit INTERNATIONALEN
Wirtschafts-Indikatoren:
- BIP/Inflation/Arbeitslosigkeit außerhalb AT-/DE-Bestand
- Leitzinsen Fed/BoE/BoJ/SNB (NICHT EZB — die kommt aus OeNB)
- Wechselkurse exotischer Währungen
- Provider-spezifische Lookups ("ECB-Daten X", "Worldbank Y")

API: https://api.db.nomics.world/v22/
- /series/{provider}/{dataset}/{series_code}?observations=1
- /search?q={query}&limit=N
- Free, kein API-Key. Rate-Limit nicht dokumentiert → 1 req/s polite.

Lizenz: AGPL-3.0 (Code) + ODbL (Daten) — Evidora-kompatibel.

Trigger-Strategie (conservative):
1. Direkt-Trigger: "dbnomics" / "db.nomics" im Claim
2. Provider-Trigger: ECB/IMF/Worldbank/Fed/BoE/etc. + Wirtschafts-Term
3. Composite: Indikator-Term (bip/inflation/arbeitslosigkeit/leitzins/
   wechselkurs) + INTERNATIONAL-Kontext (Land != AT/DE oder explizit
   "weltweit"/"international"/Land-Name)

WICHTIG — Politische Guardrails (memory/project_political_guardrails.md):
- Nur deskriptive Zahlen-Wiedergabe, KEINE Bewertung
- KEINE Schuldzuweisung an Parteien/Regierungen
- Bei Inflation/Arbeitslosigkeit etc. neutral berichten
"""

# WIRING für main.py:
# from services.dbnomics import search_dbnomics, claim_mentions_dbnomics_cached
# if claim_mentions_dbnomics_cached(claim):
#     tasks.append(cached("DBnomics", search_dbnomics, analysis))
#     queried_names.append("DBnomics")

from __future__ import annotations

import logging
import time
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

DBNOMICS_API = "https://api.db.nomics.world/v22"
TIMEOUT_S = 15.0
MAX_RESULTS = 3
CACHE_TTL_S = 24 * 3600  # 24h

# ---------------------------------------------------------------------------
# Provider-Code-Mapping (deutsche/englische Trigger-Wörter → Provider-Code)
# Konservativ: nur Top-Provider, die DBnomics tatsächlich indexiert.
# ---------------------------------------------------------------------------
_PROVIDER_HINTS: dict[str, str] = {
    "imf": "IMF",
    "internationaler währungsfonds": "IMF",
    "internationaler waehrungsfonds": "IMF",
    "world bank": "WB",
    "worldbank": "WB",
    "weltbank": "WB",
    "oecd": "OECD",
    "bis": "BIS",
    "bank für internationalen zahlungsausgleich": "BIS",
    "fred": "FED",
    "federal reserve": "FED",
    "bank of england": "BOE",
    "bank of japan": "BOJ",
    "swiss national bank": "SNB",
    "schweizerische nationalbank": "SNB",
    "ins ee": "INSEE",
    "insee": "INSEE",
    "cepii": "CEPII",
    "wifo": "WIFO",
}

# Direkt-Trigger: Claim erwähnt DBnomics namentlich
_DIRECT_TERMS = (
    "dbnomics", "db.nomics", "db nomics",
)

# Meta-Hub-Trigger (Folge-Sprint 2026-05-20): Wenn der Claim explizit
# Aggregator-/Meta-Hub-Sprache verwendet, ist DBnomics IMMER ein passender
# Cross-Validation-Layer — unabhängig von dedizierten Service-Tokens.
_META_HUB_TERMS = (
    "meta-hub", "meta hub", "metahub",
    "aggregator", "aggregierte zeitreihen",
    "zeitreihen", "time series", "time-series",
    "macroeconomic data", "macroeconomic indicators",
    "makrooekonomische daten", "makroökonomische daten",
    "cross-validation", "cross validation",
)

# Wirtschafts-Indikator-Begriffe (DE/EN) — Composite-Trigger-Teil 1
_INDICATOR_TERMS = (
    "bip", "bruttoinlandsprodukt", "gdp",
    "inflation", "inflationsrate", "verbraucherpreise", "cpi",
    "arbeitslosigkeit", "arbeitslosenquote", "unemployment",
    "leitzins", "policy rate", "interest rate",
    "wechselkurs", "exchange rate",
    "staatsverschuldung", "schuldenquote", "debt-to-gdp",
    "leistungsbilanz", "current account",
    "industrieproduktion", "industrial production",
    "einzelhandel", "retail sales",
    "exporte", "importe", "exports", "imports",
    # Folge-Sprint 2026-05-20: Fed-Bilanzsumme + WB-Tariff
    "bilanzsumme", "balance sheet", "total assets",
    "tariff", "tariffs", "zoll", "zölle",
)

# International-Kontext (NICHT AT/DE — sonst eigene Quellen)
_INTL_TERMS = (
    "weltweit", "international", "global", "world",
    "usa", "vereinigte staaten", "united states", "amerika",
    "großbritannien", "uk", "vereinigtes königreich", "britain",
    "japan", "china", "indien", "india",
    "frankreich", "france", "italien", "italy",
    "spanien", "spain", "schweiz", "switzerland",
    "türkei", "turkey", "russland", "russia",
    "brasilien", "brazil", "südkorea", "south korea",
    "australien", "australia", "kanada", "canada",
    "polen", "poland", "ungarn", "hungary",
    "tschechien", "czech", "slowakei", "slovakia",
    "g7", "g20", "emerging markets", "schwellenländer",
)

# Länder-Mapping (Trigger-Wort → ISO-3 + Anzeige-Name)
_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    "usa": ("USA", "USA"),
    "united states": ("USA", "USA"),
    "vereinigte staaten": ("USA", "USA"),
    "uk": ("GBR", "Vereinigtes Königreich"),
    "großbritannien": ("GBR", "Vereinigtes Königreich"),
    "britain": ("GBR", "Vereinigtes Königreich"),
    "japan": ("JPN", "Japan"),
    "china": ("CHN", "China"),
    "indien": ("IND", "Indien"),
    "india": ("IND", "Indien"),
    "frankreich": ("FRA", "Frankreich"),
    "france": ("FRA", "Frankreich"),
    "italien": ("ITA", "Italien"),
    "italy": ("ITA", "Italien"),
    "spanien": ("ESP", "Spanien"),
    "spain": ("ESP", "Spanien"),
    "schweiz": ("CHE", "Schweiz"),
    "switzerland": ("CHE", "Schweiz"),
    "türkei": ("TUR", "Türkei"),
    "russland": ("RUS", "Russland"),
    "brasilien": ("BRA", "Brasilien"),
}

# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_dbnomics(claim_lc: str) -> bool:
    """Conservative Trigger mit Meta-Hub-Routing (Folge-Sprint 2026-05-20):
    1. Direkter Term ("dbnomics") → True (IMMER)
    2. Meta-Hub-Sprache (aggregator/zeitreihen/macroeconomic) → True
    3. Provider + Indikator → True
    4. Provider-als-Meta-Hub (Cross-Validation parallel zu FRED/WB/etc.) →
       True — WENN dedizierter Provider (Federal Reserve, World Bank, FRED,
       IMF, OECD, BIS, …) mit thematischem Kontext erwähnt wird UND
       Claim nicht DACH-spezifisch ist.
    5. Indikator + International-Kontext (NICHT AT/DE) → True
    """
    if not claim_lc:
        return False

    # Hard-Skip: AT/DE-spezifische Wirtschafts-Claims gehören zu OeNB /
    # Statistik Austria / destatis — DBnomics ist hier nur Noise.
    has_at_de = any(t in claim_lc for t in (
        "österreich", "austria", "deutschland", "germany",
        "statistik austria", "destatis", "oenb",
    ))
    non_at_de_intl_present = any(
        t in claim_lc for t in _INTL_TERMS
        if t not in ("österreich", "austria", "deutschland", "germany")
    )

    # 1. Direkt — überstimmt sogar den DACH-Skip
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    # DACH-Hard-Skip (nach Direkt-Trigger): wenn AT/DE OHNE Drittland-Kontext,
    # bleibt der Claim bei den dedizierten DACH-Quellen.
    if has_at_de and not non_at_de_intl_present:
        return False

    # 2. Meta-Hub-Sprache → IMMER triggern (Aggregator-Cross-Validation)
    if any(t in claim_lc for t in _META_HUB_TERMS):
        return True

    has_indicator = any(t in claim_lc for t in _INDICATOR_TERMS)
    has_provider = any(t in claim_lc for t in _PROVIDER_HINTS.keys())
    has_intl = any(t in claim_lc for t in _INTL_TERMS)

    # 3. Provider + Indikator (z.B. "IMF Inflation")
    if has_provider and has_indicator:
        return True

    # 4. Provider-als-Meta-Hub: dedizierter Provider explizit genannt
    # (Federal Reserve / World Bank / FRED / IMF / OECD / BIS / …) →
    # DBnomics feuert PARALLEL zum dedizierten Service als Cross-
    # Validation. DACH-Hard-Skip oben filtert AT/DE-Claims bereits raus.
    if has_provider:
        return True

    # 5. Indikator + International — Land != AT/DE
    if has_indicator and has_intl:
        return True

    return False


# Modul-Level-Cache: (claim_lc) → (ts, result)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_dbnomics_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_dbnomics(claim_lc)
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
# Helpers
# ---------------------------------------------------------------------------
def _extract_provider(claim_lc: str) -> str | None:
    """Erkenne explizit genannten DBnomics-Provider-Code im Claim."""
    for hint, code in _PROVIDER_HINTS.items():
        if hint in claim_lc:
            return code
    return None


def _extract_country(claim_lc: str) -> tuple[str, str]:
    """Wähle erstes gefundenes Drittland → (ISO3, AnzeigeName).
    Fallback: ('—', '—')."""
    for hint, (iso, name) in _COUNTRY_MAP.items():
        if hint in claim_lc:
            return iso, name
    return "—", "—"


def _build_search_query(claim_lc: str) -> str:
    """Konstruiere eine kurze, fokussierte DBnomics-Search-Query.

    Heuristik: 1-2 Indikator-Begriffe + 1 Land/Provider-Hint.
    Englisch bevorzugt — DBnomics indexiert primär englisch.
    """
    indicator_en_map = {
        "bip": "gdp",
        "bruttoinlandsprodukt": "gdp",
        "inflation": "inflation",
        "inflationsrate": "inflation",
        "verbraucherpreise": "consumer prices",
        "arbeitslosigkeit": "unemployment",
        "arbeitslosenquote": "unemployment rate",
        "leitzins": "policy rate",
        "wechselkurs": "exchange rate",
        "staatsverschuldung": "government debt",
        "schuldenquote": "debt to gdp",
        "industrieproduktion": "industrial production",
        "leistungsbilanz": "current account",
    }
    country_en_map = {
        "usa": "united states",
        "vereinigte staaten": "united states",
        "großbritannien": "united kingdom",
        "uk": "united kingdom",
        "frankreich": "france",
        "italien": "italy",
        "spanien": "spain",
        "schweiz": "switzerland",
        "türkei": "turkey",
        "japan": "japan",
        "china": "china",
        "indien": "india",
    }
    parts: list[str] = []
    for de_term, en_term in indicator_en_map.items():
        if de_term in claim_lc:
            parts.append(en_term)
            break
    for de_country, en_country in country_en_map.items():
        if de_country in claim_lc:
            parts.append(en_country)
            break
    # Wenn nichts erkannt → erstes vorgefundenes englisches Wort raus
    for t in _INDICATOR_TERMS:
        if t in claim_lc and len(parts) == 0:
            parts.append(t)
            break
    return " ".join(parts)[:120]


def _format_dataset_doc(doc: dict, claim_lc: str) -> dict:
    """Formatiere ein /search-Result (Dataset-Doc) zum Evidora-Schema."""
    code = doc.get("code") or ""
    name = doc.get("name") or code
    provider_code = doc.get("provider_code") or "—"
    provider_name = doc.get("provider_name") or provider_code
    nb_series = doc.get("nb_series") or 0
    description = (doc.get("description") or "").strip()
    if len(description) > 400:
        description = description[:400] + "…"

    iso, country_name = _extract_country(claim_lc)
    year = (doc.get("updated_at") or doc.get("indexed_at") or "")[:4] or "—"

    url = f"https://db.nomics.world/{provider_code}/{code}"

    display = (
        f"DBnomics-Datenset '{name}' (Provider: {provider_name}, "
        f"Code: {provider_code}/{code}, {nb_series:,} Zeitreihen). "
        f"Letztes Update: {year}."
    )

    return {
        "indicator_name": f"DBnomics: {name} ({provider_code})",
        "indicator": f"dbnomics_{provider_code.lower()}_{code.lower()}",
        "country": iso,
        "country_name": country_name,
        "year": year,
        "value": nb_series,
        "display_value": display,
        "description": (
            description
            or "Aggregierte Wirtschafts-Zeitreihen via DBnomics. "
               "Nur deskriptive Werte — keine Bewertung."
        ),
        "url": url,
        "source": f"DBnomics (Aggregator) → {provider_name}",
    }


def _format_series_doc(doc: dict, claim_lc: str) -> dict:
    """Formatiere ein /series/{...}-Result mit Observations zum Schema."""
    series_code = doc.get("series_code") or ""
    series_name = doc.get("series_name") or series_code
    provider_code = doc.get("provider_code") or "—"
    dataset_code = doc.get("dataset_code") or ""
    dataset_name = doc.get("dataset_name") or dataset_code

    periods = doc.get("period") or []
    values = doc.get("value") or []
    latest_period = "—"
    latest_value = None
    if periods and values:
        # Letzte Beobachtung (DBnomics liefert chronologisch)
        for p, v in zip(reversed(periods), reversed(values)):
            if v is not None:
                latest_period = str(p)
                latest_value = v
                break

    iso, country_name = _extract_country(claim_lc)

    url = (
        f"https://db.nomics.world/{provider_code}/{dataset_code}/{series_code}"
    )

    if latest_value is not None:
        val_str = (
            f"{latest_value:.4f}" if isinstance(latest_value, float)
            else str(latest_value)
        )
        display = (
            f"DBnomics-Zeitreihe '{series_name}' "
            f"({provider_code}/{dataset_code}): "
            f"Letzter Wert {val_str} ({latest_period})."
        )
    else:
        display = (
            f"DBnomics-Zeitreihe '{series_name}' "
            f"({provider_code}/{dataset_code}) — keine numerischen "
            f"Beobachtungen verfügbar."
        )

    return {
        "indicator_name": (
            f"DBnomics: {dataset_name} — {series_name}"
        )[:300],
        "indicator": (
            f"dbnomics_{provider_code.lower()}_{dataset_code.lower()}"
        ),
        "country": iso,
        "country_name": country_name,
        "year": latest_period[:4] if latest_period != "—" else "—",
        "value": latest_value,
        "display_value": display,
        "description": (
            f"Aggregierte Wirtschafts-Zeitreihe via DBnomics, "
            f"Ursprung: {provider_code}. "
            "Nur deskriptive Werte — keine politische Bewertung."
        ),
        "url": url,
        "source": f"DBnomics (Aggregator) → {provider_code}",
    }


# ---------------------------------------------------------------------------
# HTTP-Calls
# ---------------------------------------------------------------------------
async def _search_api(client, query: str, provider: str | None) -> list[dict]:
    """GET /v22/search?q=…&limit=10 — gibt formatierte Dataset-Treffer."""
    if not query or len(query) < 3:
        return []
    cache_key = f"search::{provider or '*'}::{query.lower()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    url = (
        f"{DBNOMICS_API}/search?q={quote_plus(query)}&limit=10"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"DBnomics search HTTP {resp.status_code} for '{query[:40]}'"
            )
            return []
        data = resp.json()
    except Exception as e:
        logger.debug(f"DBnomics search failed for '{query[:40]}': {e}")
        return []

    docs = (data.get("results") or {}).get("docs") or []
    if provider:
        # Provider-Filter
        docs = [d for d in docs if (d.get("provider_code") or "") == provider]
    docs = docs[:MAX_RESULTS]
    _cache_put(cache_key, docs)
    return docs


async def _fetch_default_series(
    client, provider: str, dataset: str, series_code: str,
) -> dict | None:
    """GET /v22/series/{provider}/{dataset}/{series_code}?observations=1."""
    cache_key = f"series::{provider}::{dataset}::{series_code}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached[0] if cached else None

    url = (
        f"{DBNOMICS_API}/series/{quote_plus(provider)}/"
        f"{quote_plus(dataset)}/{quote_plus(series_code)}?observations=1"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"DBnomics series HTTP {resp.status_code} for "
                f"{provider}/{dataset}/{series_code}"
            )
            _cache_put(cache_key, [])
            return None
        data = resp.json()
    except Exception as e:
        logger.debug(
            f"DBnomics series fetch failed "
            f"{provider}/{dataset}/{series_code}: {e}"
        )
        return None

    series_docs = (data.get("series") or {}).get("docs") or []
    if not series_docs:
        _cache_put(cache_key, [])
        return None
    _cache_put(cache_key, [series_docs[0]])
    return series_docs[0]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_dbnomics(analysis: dict) -> dict:
    """Live-Lookup gegen DBnomics-API.

    Strategie:
    1. Provider erkannt → Search mit Provider-Filter
    2. Sonst → freie Search via Query-Builder
    Bei jedem Dataset-Treffer wird optional die meistgenutzte Zeitreihe
    nicht aufgelöst (kostet zu viele Calls) — wir geben das Dataset-
    Level-Result zurück, mit deep-link in db.nomics.world.
    """
    empty = {
        "source": "DBnomics",
        "type": "economic_data",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_dbnomics(matchable):
        return empty

    provider = _extract_provider(matchable)
    query = _build_search_query(matchable)
    if not query:
        # Fallback: nimm den Claim (gekürzt)
        query = claim[:80] if claim else ""
    if not query:
        return empty

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        docs = await _search_api(client, query, provider)
        for doc in docs:
            try:
                r = _format_dataset_doc(doc, matchable)
            except Exception as e:
                logger.debug(f"DBnomics doc-format-error: {e}")
                continue
            results.append(r)
            if len(results) >= MAX_RESULTS:
                break

    if not results:
        logger.info(
            f"DBnomics: 0 Treffer für query='{query[:40]}' "
            f"provider={provider or '*'}"
        )
        return empty

    logger.info(
        f"DBnomics: {len(results)} Treffer für query='{query[:40]}' "
        f"provider={provider or '*'}"
    )
    return {
        "source": "DBnomics",
        "type": "economic_data",
        "results": results,
    }
