"""FRED — Federal Reserve Bank of St. Louis (Live-API).

Quelle: FRED (Federal Reserve Economic Data, https://fred.stlouisfed.org/).
Die St.-Louis-Fed kuratiert 800.000+ US-Wirtschafts-Zeitreihen sowie
gespiegelte Aggregate von BIS/OECD/IMF/Worldbank. Für Evidora ist FRED
unsere primäre Live-Quelle für US-spezifische Indikatoren:

  - US-Inflation (CPI), US-Arbeitslosenquote (UNRATE), US-BIP (GDP)
  - Federal Funds Rate (Fed-Leitzins, DFF) und 10y-Treasury (DGS10)
  - Recession-Spread T10Y2Y (10-Year minus 2-Year)
  - Wechselkurs USD/EUR (DEXUSEU)
  - Housing Starts (HOUST), Nonfarm Payrolls (PAYEMS), M2 (M2SL)

Komplementär zu:
  - OeNB / dbnomics → EZB / DACH-Geldpolitik
  - IMF → Welt-BIP-Prognosen (WEO)
  - destatis / statistik_at → DACH-spezifische Indikatoren

API-Strategie (zwei Pfade, beide robust):
  1. PRIMARY: FRED Direct JSON-API mit ``FRED_API_KEY``-env
       https://api.stlouisfed.org/fred/series/observations?series_id={ID}
         &api_key={KEY}&file_type=json
     Voraussetzung: kostenloser Key via
       https://fredaccount.stlouisfed.org/apikeys
     Liefert vollständige JSON-Antwort inkl. Realtime-Bereich + Vintage-
     Information.
  2. FALLBACK (auch ohne Key): FRED fredgraph CSV-Mirror
       https://fred.stlouisfed.org/graph/fredgraph.csv?id={ID}
     Öffentlicher Endpunkt, keine Auth, gleiche Beobachtungs-Werte. Wird
     verwendet, wenn ``FRED_API_KEY`` fehlt oder die Primary-Antwort
     fehlerhaft / leer ist.

Lizenz: Public Domain (US-Bundes-Daten); Quellen-Attribution per
``source``-Feld + ``url`` zur FRED-Series-Detail-Seite.

Trigger-Strategie (komplementär zu OeNB / IMF / DBnomics):
  - Direkt-Trigger: "fred", "st. louis fed", "federal reserve bank of st.
    louis", "stlouisfed".
  - Composite-Trigger: US-Marker ("usa", "us-amerikanisch", "u.s.",
    "amerika", "vereinigte staaten") + Wirtschafts-Indikator-Term
    (inflation, bip, arbeitslosen…, fed-leitzins, federal funds rate,
    treasury, payroll, housing starts, m2).
  - Series-Direkt: erwähnt der Claim eine FRED-Series-ID wörtlich (z.B.
    "FRED UNRATE", "CPIAUCSL", "DGS10") → True.
  - HARD-SKIP: rein DACH/EU-Wirtschaft ohne US-Marker → False
    (überlassen wir OeNB / ECB / WIFO / IHS / destatis).

WICHTIG — Politische Guardrails (memory/project_political_guardrails.md):
  - NUR deskriptive Zahlen-Wiedergabe; KEINE Bewertung
  - KEINE Schuldzuweisung an Parteien/Regierungen/Notenbanker
  - Bei Recession-Spread (T10Y2Y < 0): nur Fakt nennen, KEINE Prognose
    ("Indikator historisch mit US-Rezessionen korreliert", nicht
    "es kommt eine Rezession")
  - Bei Fed-Leitzins-Werten KEINE Aussagen über künftige Pfade.

WIRING für main.py:
  from services.fred import search_fred, claim_mentions_fred_cached
  if claim_mentions_fred_cached(claim):
      tasks.append(cached("FRED", search_fred, analysis))
      queried_names.append("FRED (St. Louis Fed)")
"""

from __future__ import annotations

import csv
import io
import logging
import os
import time
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Endpoints + Konfiguration
# ---------------------------------------------------------------------------
FRED_API_BASE = "https://api.stlouisfed.org/fred"
FREDGRAPH_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_SERIES_PAGE = "https://fred.stlouisfed.org/series/{series_id}"

TIMEOUT_S = 12.0
MAX_RESULTS = 3
CACHE_TTL_S = 24 * 3600  # 24h
DEFAULT_OBS_LIMIT = 24    # genug für letzten Monatswert + 12-Monats-Vergleich


def _api_key() -> str | None:
    """Liest ``FRED_API_KEY`` aus der Umgebung. Leerstring → None."""
    key = (os.environ.get("FRED_API_KEY") or "").strip()
    return key or None


# ---------------------------------------------------------------------------
# Series-Mapping (Trigger-Wort → (Series-ID, Titel-DE, Einheit, Frequenz))
# 10 wichtigste US-Indikatoren laut Faktencheck-Praxis.
# ---------------------------------------------------------------------------
_SERIES_MAP: dict[str, tuple[str, str, str, str]] = {
    # US-Inflation (CPI All Items, Index 1982-84=100)
    "us-inflation": ("CPIAUCSL", "US CPI (alle Posten)", "Index", "monthly"),
    "us inflation": ("CPIAUCSL", "US CPI (alle Posten)", "Index", "monthly"),
    "amerikanische inflation": (
        "CPIAUCSL", "US CPI (alle Posten)", "Index", "monthly",
    ),
    "us-verbraucherpreise": (
        "CPIAUCSL", "US CPI (alle Posten)", "Index", "monthly",
    ),
    "us cpi": ("CPIAUCSL", "US CPI (alle Posten)", "Index", "monthly"),
    "cpiaucsl": ("CPIAUCSL", "US CPI (alle Posten)", "Index", "monthly"),

    # US-Arbeitslosenquote (UNRATE, Civilian Unemployment, %)
    "us-arbeitslosenquote": (
        "UNRATE", "US-Arbeitslosenquote", "%", "monthly",
    ),
    "us arbeitslosenquote": (
        "UNRATE", "US-Arbeitslosenquote", "%", "monthly",
    ),
    "us-arbeitslosigkeit": (
        "UNRATE", "US-Arbeitslosenquote", "%", "monthly",
    ),
    "us unemployment": (
        "UNRATE", "US-Arbeitslosenquote", "%", "monthly",
    ),
    "unrate": ("UNRATE", "US-Arbeitslosenquote", "%", "monthly"),

    # US-BIP (GDP, nominal, Mrd. USD)
    "us-bip": ("GDP", "US Nominal-BIP", "Mrd. USD", "quarterly"),
    "us bip": ("GDP", "US Nominal-BIP", "Mrd. USD", "quarterly"),
    "us-bruttoinlandsprodukt": (
        "GDP", "US Nominal-BIP", "Mrd. USD", "quarterly",
    ),
    "us gdp": ("GDP", "US Nominal-BIP", "Mrd. USD", "quarterly"),
    "amerikanisches bip": (
        "GDP", "US Nominal-BIP", "Mrd. USD", "quarterly",
    ),

    # Federal Funds Effective Rate (DFF, %)
    "federal funds rate": (
        "DFF", "Federal Funds Effective Rate", "%", "daily",
    ),
    "fed funds rate": (
        "DFF", "Federal Funds Effective Rate", "%", "daily",
    ),
    "fed-leitzins": (
        "DFF", "Federal Funds Effective Rate", "%", "daily",
    ),
    "fed leitzins": (
        "DFF", "Federal Funds Effective Rate", "%", "daily",
    ),
    "us-leitzins": (
        "DFF", "Federal Funds Effective Rate", "%", "daily",
    ),
    "us leitzins": (
        "DFF", "Federal Funds Effective Rate", "%", "daily",
    ),
    "dff": ("DFF", "Federal Funds Effective Rate", "%", "daily"),

    # 10-Year Treasury Constant Maturity Rate (DGS10, %)
    "10y treasury": (
        "DGS10", "10-Year US-Treasury (DGS10)", "%", "daily",
    ),
    "10-year treasury": (
        "DGS10", "10-Year US-Treasury (DGS10)", "%", "daily",
    ),
    "us-staatsanleihe 10 jahre": (
        "DGS10", "10-Year US-Treasury (DGS10)", "%", "daily",
    ),
    "us staatsanleihe 10 jahre": (
        "DGS10", "10-Year US-Treasury (DGS10)", "%", "daily",
    ),
    "dgs10": ("DGS10", "10-Year US-Treasury (DGS10)", "%", "daily"),

    # USD/EUR Exchange Rate (DEXUSEU, USD pro EUR)
    "usd/eur": ("DEXUSEU", "USD/EUR Wechselkurs", "USD/EUR", "daily"),
    "usd eur": ("DEXUSEU", "USD/EUR Wechselkurs", "USD/EUR", "daily"),
    "dollar euro wechselkurs": (
        "DEXUSEU", "USD/EUR Wechselkurs", "USD/EUR", "daily",
    ),
    "dexuseu": ("DEXUSEU", "USD/EUR Wechselkurs", "USD/EUR", "daily"),

    # Recession-Indicator (10y minus 2y Spread, %)
    "t10y2y": (
        "T10Y2Y", "Zinskurven-Spread 10y minus 2y", "% Punkte", "daily",
    ),
    "rezessions-indikator": (
        "T10Y2Y", "Zinskurven-Spread 10y minus 2y", "% Punkte", "daily",
    ),
    "rezessionsindikator usa": (
        "T10Y2Y", "Zinskurven-Spread 10y minus 2y", "% Punkte", "daily",
    ),
    "yield curve": (
        "T10Y2Y", "Zinskurven-Spread 10y minus 2y", "% Punkte", "daily",
    ),

    # Housing Starts (HOUST, Tsd. Einheiten annualisiert)
    "housing starts": (
        "HOUST", "US Housing Starts", "Tsd. (annualisiert)", "monthly",
    ),
    "us-wohnungsneubau": (
        "HOUST", "US Housing Starts", "Tsd. (annualisiert)", "monthly",
    ),
    "us wohnungsneubau": (
        "HOUST", "US Housing Starts", "Tsd. (annualisiert)", "monthly",
    ),
    "houst": (
        "HOUST", "US Housing Starts", "Tsd. (annualisiert)", "monthly",
    ),

    # Nonfarm Payrolls (PAYEMS, Tsd. Beschäftigte)
    "payems": ("PAYEMS", "US Nonfarm Payrolls", "Tsd.", "monthly"),
    "nonfarm payrolls": (
        "PAYEMS", "US Nonfarm Payrolls", "Tsd.", "monthly",
    ),
    "us-beschäftigung": (
        "PAYEMS", "US Nonfarm Payrolls", "Tsd.", "monthly",
    ),

    # M2 Money Supply (M2SL, Mrd. USD)
    "m2sl": ("M2SL", "US M2 Geldmenge", "Mrd. USD", "monthly"),
    "m2 geldmenge": ("M2SL", "US M2 Geldmenge", "Mrd. USD", "monthly"),
    "us-geldmenge": ("M2SL", "US M2 Geldmenge", "Mrd. USD", "monthly"),
}

# Direkt-Trigger: FRED namentlich
_DIRECT_TERMS = (
    "fred", "stlouisfed", "st. louis fed", "st louis fed",
    "federal reserve bank of st. louis",
    "federal reserve bank of st louis",
    "federal reserve",  # generisch "Federal Reserve" → klar US-Kontext
)

# US-Markierungen (Composite-Trigger Voraussetzung)
_US_TERMS = (
    "usa", "u.s.", "u. s.", "us-amerika", "amerika",
    "amerikanisch", "amerikanische", "amerikanischer",
    "vereinigte staaten", "united states",
    "us-", "us ",  # erlaubt "US-Inflation" / "US Inflation" generisch
    # Institutionen / Indikatoren mit klarem US-Kontext (DACH-Skip-Override)
    "federal reserve", "fed ", "fed-", "stlouisfed",
    "st louis", "st. louis",
)

# Allgemeine US-Wirtschafts-Indikator-Worte (für Composite ohne explizite
# Series-ID). Mit US-Marker → FRED-Trigger.
_US_ECON_INDICATOR_TERMS = (
    "inflation", "verbraucherpreise", "cpi",
    "arbeitslosenquote", "arbeitslosigkeit", "unemployment",
    "bip", "bruttoinlandsprodukt", "gdp",
    "leitzins", "fed funds", "federal funds",
    "treasury", "staatsanleihe",
    "wohnungsneubau", "housing",
    "nonfarm", "payroll",
    "geldmenge", "money supply", "m2",
    "yield curve", "zinskurve",
)

# DACH-Hard-Skip: wenn der Claim AT/DE/CH-zentriert ist und KEINEN US-
# Marker enthält, überlassen wir das OeNB/destatis/dbnomics — wir wollen
# kein irreführendes US-Ergebnis an einen DACH-Claim anhängen.
_DACH_TERMS = (
    "österreich", "austria",
    "deutschland", "germany",
    "schweiz", "switzerland",
)


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_fred(claim_lc: str) -> bool:
    """Conservative Trigger.

    Reihenfolge (early-return):
      1. Direkt-Term ("fred"/"stlouisfed"/…) → True
      2. Wörtlicher Series-Mapping-Hit (z.B. "Federal Funds Rate") → True
      3. US-Marker + US-Wirtschafts-Indikator-Term → True
      4. Sonst → False (insbesondere reine DACH-Claims).
    """
    if not claim_lc:
        return False

    # 1. Direkt
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    # 2. Series-Mapping-Hit (wörtliches Vorkommen einer Map-Phrase)
    if any(hint in claim_lc for hint in _SERIES_MAP.keys()):
        return True

    # 3. Composite US-Marker + Indikator
    has_us = any(t in claim_lc for t in _US_TERMS)
    has_indicator = any(t in claim_lc for t in _US_ECON_INDICATOR_TERMS)
    if has_us and has_indicator:
        return True

    # 4. Hard-Skip: rein DACH, kein US — überlassen wir OeNB/destatis/…
    return False


# Modul-Level-Trigger-Cache: claim_lc → (ts, bool)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_fred_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_fred(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    # Cache-Hygiene
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Result-Cache (24h pro Series-ID)
# ---------------------------------------------------------------------------
_result_cache: dict[str, tuple[float, dict | None]] = {}


def _cache_get(key: str) -> dict | None:
    now = time.time()
    hit = _result_cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL_S:
        return hit[1]
    return None


def _cache_put(key: str, value: dict | None) -> None:
    _result_cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
def _extract_series(claim_lc: str) -> list[tuple[str, str, str, str]]:
    """Extrahiert FRED-Series-Tupel aus dem Claim.

    Dedupliziert auf Series-ID. Reihenfolge: Mapping-Reihenfolge.
    """
    seen: set[str] = set()
    out: list[tuple[str, str, str, str]] = []
    for hint, tup in _SERIES_MAP.items():
        if hint in claim_lc and tup[0] not in seen:
            out.append(tup)
            seen.add(tup[0])
    return out


def _default_series_for_us_term(claim_lc: str) -> list[tuple[str, str, str, str]]:
    """Fallback-Indikator-Auswahl, wenn US-Marker + generischer Term, aber
    kein präziser Map-Hint. Mapped allgemeine Begriffe auf typische Series.
    """
    out: list[tuple[str, str, str, str]] = []
    if any(t in claim_lc for t in ("inflation", "cpi", "verbraucherpreise")):
        out.append(_SERIES_MAP["us cpi"])
    if any(t in claim_lc for t in (
        "arbeitslosenquote", "arbeitslosigkeit", "unemployment",
    )):
        out.append(_SERIES_MAP["unrate"])
    if any(t in claim_lc for t in ("bip", "gdp", "bruttoinlandsprodukt")):
        out.append(_SERIES_MAP["us bip"])
    if any(t in claim_lc for t in (
        "leitzins", "fed funds", "federal funds",
        "interest rate", "interest rates", "rate decision",
    )):
        out.append(_SERIES_MAP["fed-leitzins"])
    # Fallback: "Federal Reserve" alleine ohne weiteren Indikator-Hint → DFF
    if "federal reserve" in claim_lc and not out:
        out.append(_SERIES_MAP["fed-leitzins"])
    if any(t in claim_lc for t in ("treasury", "staatsanleihe")):
        out.append(_SERIES_MAP["10y treasury"])
    if any(t in claim_lc for t in ("yield curve", "zinskurve")):
        out.append(_SERIES_MAP["t10y2y"])
    if any(t in claim_lc for t in ("housing", "wohnungsneubau")):
        out.append(_SERIES_MAP["housing starts"])
    if any(t in claim_lc for t in ("nonfarm", "payroll")):
        out.append(_SERIES_MAP["payems"])
    if any(t in claim_lc for t in ("m2", "geldmenge", "money supply")):
        out.append(_SERIES_MAP["m2sl"])
    # Deduplizieren auf Series-ID
    seen: set[str] = set()
    deduped: list[tuple[str, str, str, str]] = []
    for tup in out:
        if tup[0] not in seen:
            deduped.append(tup)
            seen.add(tup[0])
    return deduped


# ---------------------------------------------------------------------------
# HTTP — PRIMARY (JSON-API mit API-Key)
# ---------------------------------------------------------------------------
async def _fetch_fred_api_json(
    client, *, series_id: str, api_key: str,
) -> list[tuple[str, float]] | None:
    """FRED Direct JSON-Endpoint. Liefert Liste (date, value), neueste zuletzt.

    Bei jedem Fehler (HTTP-Status / JSON-Decode / Schema): None.
    """
    url = (
        f"{FRED_API_BASE}/series/observations"
        f"?series_id={quote_plus(series_id)}"
        f"&api_key={quote_plus(api_key)}"
        f"&file_type=json"
        f"&sort_order=desc"
        f"&limit={DEFAULT_OBS_LIMIT}"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(f"FRED-API HTTP {resp.status_code} for {series_id}")
            return None
        data = resp.json()
    except Exception as e:
        logger.debug(f"FRED-API fetch failed for {series_id}: {e}")
        return None

    obs = data.get("observations") or []
    pairs: list[tuple[str, float]] = []
    for o in obs:
        date = o.get("date") or ""
        val_raw = o.get("value")
        if not date or val_raw in (None, "", "."):
            continue
        try:
            v = float(val_raw)
        except (TypeError, ValueError):
            continue
        pairs.append((date, v))
    if not pairs:
        return None
    # FRED API liefert desc → wir wollen asc (älteste zuerst, neueste hinten)
    pairs.sort(key=lambda kv: kv[0])
    return pairs


# ---------------------------------------------------------------------------
# HTTP — FALLBACK (CSV-Mirror, kein Auth)
# ---------------------------------------------------------------------------
async def _fetch_fred_csv(
    client, *, series_id: str,
) -> list[tuple[str, float]] | None:
    """Public fredgraph CSV-Mirror. Liefert (date, value), älteste zuerst."""
    url = f"{FREDGRAPH_CSV}?id={quote_plus(series_id)}"
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(f"FRED-CSV HTTP {resp.status_code} for {series_id}")
            return None
        text = resp.text
    except Exception as e:
        logger.debug(f"FRED-CSV fetch failed for {series_id}: {e}")
        return None

    pairs: list[tuple[str, float]] = []
    try:
        reader = csv.reader(io.StringIO(text))
        header = next(reader, None)
        if not header:
            return None
        for row in reader:
            if len(row) < 2:
                continue
            date, val = row[0].strip(), row[1].strip()
            if not date or val in ("", "."):
                continue
            try:
                v = float(val)
            except ValueError:
                continue
            pairs.append((date, v))
    except Exception as e:
        logger.debug(f"FRED-CSV parse failed for {series_id}: {e}")
        return None

    if not pairs:
        return None
    # CSV ist bereits asc nach Datum. Auf letzte DEFAULT_OBS_LIMIT eindampfen.
    return pairs[-DEFAULT_OBS_LIMIT:]


# ---------------------------------------------------------------------------
# Lookup-Kette: API → CSV
# ---------------------------------------------------------------------------
async def _lookup_series(
    client, *, series_id: str, title_de: str, unit: str, frequency: str,
) -> dict | None:
    """Per-Series-Lookup mit 24h-Cache und Fallback-Kette."""
    cache_key = f"fred::{series_id}"
    hit = _result_cache.get(cache_key)
    now = time.time()
    if hit and (now - hit[0]) < CACHE_TTL_S:
        return hit[1]

    pairs: list[tuple[str, float]] | None = None
    source_path = ""
    api_key = _api_key()
    if api_key:
        pairs = await _fetch_fred_api_json(
            client, series_id=series_id, api_key=api_key,
        )
        if pairs:
            source_path = "FRED Direct API (JSON)"

    if not pairs:
        # Graceful fallback ohne (oder bei Fehler trotz) Key
        pairs = await _fetch_fred_csv(client, series_id=series_id)
        if pairs:
            source_path = "FRED fredgraph (CSV-Mirror)"

    if not pairs:
        _cache_put(cache_key, None)
        return None

    result = _build_result(
        series_id=series_id,
        title_de=title_de,
        unit=unit,
        frequency=frequency,
        pairs=pairs,
        source_path=source_path,
    )
    _cache_put(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _format_value(v: float | None, unit: str) -> str:
    if v is None:
        return "—"
    # Entscheidung Nachkommastellen
    if unit == "%" or unit == "% Punkte":
        s = f"{v:.2f}"
    elif unit == "USD/EUR":
        s = f"{v:.4f}"
    elif unit.startswith("Mrd."):
        s = f"{v:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    elif unit.startswith("Tsd."):
        s = f"{v:,.0f}".replace(",", ".")
    else:
        s = f"{v:.2f}"
    return s.replace(".", ",") if unit in ("%", "% Punkte") else s


def _pct_change(latest: float, prior: float | None) -> str:
    if prior is None or prior == 0:
        return ""
    chg = (latest - prior) / prior * 100.0
    sign = "+" if chg >= 0 else ""
    return f"{sign}{chg:.2f}".replace(".", ",") + " %"


def _build_result(
    *,
    series_id: str,
    title_de: str,
    unit: str,
    frequency: str,
    pairs: list[tuple[str, float]],
    source_path: str,
) -> dict:
    """Baue ein Evidora-Schema-Result aus der Zeitreihe."""
    pairs_sorted = sorted(pairs, key=lambda kv: kv[0])
    latest_date, latest_val = pairs_sorted[-1]
    prior_val = pairs_sorted[-2][1] if len(pairs_sorted) >= 2 else None

    val_str = _format_value(latest_val, unit)
    prior_str = (
        f", vorheriger Wert {_format_value(prior_val, unit)}"
        if prior_val is not None else ""
    )

    pct_note = _pct_change(latest_val, prior_val)
    pct_str = f" (Änderung {pct_note})" if pct_note else ""

    # Wir wollen aus dem ISO-Datum (YYYY-MM-DD) Jahr als 'year'-Feld
    year = latest_date[:4] if len(latest_date) >= 4 else ""

    display = (
        f"FRED {title_de} ({series_id}) zum {latest_date}: "
        f"{val_str} {unit}{pct_str}{prior_str}. "
        f"Frequenz: {frequency}. Quelle: {source_path}."
    )

    description = (
        f"Federal Reserve Economic Data (FRED), Series-ID {series_id} — "
        f"{title_de}. Letzte Beobachtung {latest_date}: {val_str} {unit}. "
        f"Methodik / Definition siehe FRED-Series-Seite. "
        "FRED kuratiert 800.000+ US-Wirtschafts-Zeitreihen der Federal "
        "Reserve Bank of St. Louis (Public Domain). "
        "Hinweis: Werte können nachträglich revidiert werden; FRED "
        "dokumentiert Realtime-Bereiche pro Beobachtung. Keine politische "
        "Wertung — die Daten beschreiben den Stand zum Stichtag."
    )
    # Spezial-Caveat für T10Y2Y (Recession-Indicator)
    if series_id == "T10Y2Y" and latest_val is not None and latest_val < 0:
        description += (
            " Hinweis zum Spread: Eine inverse Zinskurve "
            "(10y minus 2y < 0) korrelierte historisch mit folgenden "
            "US-Rezessionen. Korrelation ist keine Prognose; FRED selbst "
            "macht keine Rezessions-Vorhersage."
        )

    return {
        "indicator_name": f"FRED {title_de} ({series_id})"[:300],
        "indicator": f"fred_{series_id.lower()}",
        "country": "USA",
        "country_name": "USA",
        "year": year,
        "value": latest_val,
        "display_value": display,
        "description": description,
        "url": FRED_SERIES_PAGE.format(series_id=series_id),
        "source": "FRED (Federal Reserve Bank of St. Louis)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_fred(analysis: dict) -> dict:
    """Live-Lookup gegen FRED (API mit Key → CSV-Mirror als Fallback).

    Strategie:
      1. Trigger-Check (claim_mentions_fred_cached)
      2. Series aus Claim extrahieren (Mapping-Hit ODER US-Marker+Indikator
         als Default-Mapping). Bei leerem Ergebnis: empty.
      3. Pro Series: Cache-Hit prüfen → sonst HTTP-Kette
      4. Max MAX_RESULTS Treffer; jeder Treffer mit Methoden-Caveat.

    Hard-Skip: rein DACH-Claims ohne US-Marker → empty.
    """
    empty = {
        "source": "FRED",
        "type": "us_economic_data",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_fred(matchable):
        return empty

    # Defensive Hard-Skip: pure DACH-Anker ohne US-Marker.
    has_us = any(t in matchable for t in _US_TERMS)
    has_fred_direct = any(t in matchable for t in _DIRECT_TERMS)
    has_series_phrase = any(hint in matchable for hint in _SERIES_MAP.keys())
    has_dach = any(t in matchable for t in _DACH_TERMS)
    if has_dach and not (has_us or has_fred_direct or has_series_phrase):
        logger.info("FRED: hard-skip — DACH-Claim ohne US-Marker.")
        return empty

    series_tuples = _extract_series(matchable)
    if not series_tuples:
        # Fallback: US-Marker + Indikator-Term ohne präzises Map-Hint
        series_tuples = _default_series_for_us_term(matchable)
    if not series_tuples:
        logger.info("FRED: Trigger fired, but no series resolvable → empty.")
        return empty

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for series_id, title_de, unit, frequency in series_tuples:
            try:
                r = await _lookup_series(
                    client,
                    series_id=series_id,
                    title_de=title_de,
                    unit=unit,
                    frequency=frequency,
                )
            except Exception as e:
                logger.debug(f"FRED lookup {series_id} crashed: {e}")
                continue
            if r:
                results.append(r)
            if len(results) >= MAX_RESULTS:
                break

    if not results:
        logger.info(
            f"FRED: 0 Treffer für series="
            f"{[s[0] for s in series_tuples]} (Key-Status: "
            f"{'set' if _api_key() else 'unset'})"
        )
        return empty

    logger.info(
        f"FRED: {len(results)} Treffer für series="
        f"{[s[0] for s in series_tuples]}"
    )
    return {
        "source": "FRED",
        "type": "us_economic_data",
        "results": results,
    }
