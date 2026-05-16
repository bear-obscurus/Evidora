"""IMF — International Monetary Fund (WEO / FSI / IFS).

Quelle: International Monetary Fund (https://www.imf.org/) — World Economic
Outlook (WEO, halbjährlich), Financial Soundness Indicators (FSI) und
International Financial Statistics (IFS).

API-Strategie:
1. PRIMARY: IMF Direct-SDMX-API
       https://api.imf.org/external/sdmx/2.1/data/{dataflow}/{key}?format=jsondata
   Hinweis: IMF hat 2024/2025 die Endpoint-Struktur und URNs umgebaut;
   intensive Nutzung verlangt seitdem API-Key. Für leichte Nutzung quota-
   frei, aber wir implementieren defensiv mit kurzem Timeout und silentem
   Failover — Dataflow-URNs ändern sich saisonal (z.B. WEO:2024-10 → 2025-04).
2. FALLBACK: DBnomics-IMF-Mirror (kein Auth, höhere Verfügbarkeit)
       https://api.db.nomics.world/v22/series/IMF/{dataset}/{key}
   DBnomics spiegelt WEO + IFS + FSI mit ~tagesaktuellem Stand und
   konsistentem JSON-Schema (siehe services/dbnomics.py).

Wir liefern semantisch IMF-Daten (Provenance bleibt IMF), egal über
welchen Pfad sie kamen — die source-Spalte zeigt das via Suffix an.

Lizenz: IMF-Daten CC-BY (Quellenangabe in source-Feld + URL).

Trigger-Strategie (komplementär zu dbnomics.py, das auch IMF kennt):
- Direkt: "imf", "iwf", "weo", "world economic outlook", "article iv"
- Composite: Prognose-Term + (Land ∨ International) ohne explizite EZB/OECD-
  Markierung. Beispiele:
    - "IMF-Prognose Österreich BIP 2025"
    - "IWF erwartet Inflation USA 2,1%"
    - "Schulden-Niveau Italien (laut IMF)"

WICHTIG — Politische Guardrails (memory/project_political_guardrails.md):
- Prognose-Werte IMMER mit Caveat ("Schätzung des IWF", nicht "wird sein")
- Keine Schuldzuweisung an Parteien/Regierungen
- Nur deskriptive Werte + Methodologie-Hinweis
"""

# WIRING für main.py:
# from services.imf import search_imf, claim_mentions_imf_cached
# if claim_mentions_imf_cached(claim):
#     tasks.append(cached("IMF", search_imf, analysis))
#     queried_names.append("IMF (World Economic Outlook)")

from __future__ import annotations

import logging
import time
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

IMF_SDMX_API = "https://api.imf.org/external/sdmx/2.1/data"
DBNOMICS_API = "https://api.db.nomics.world/v22"
TIMEOUT_S = 12.0
TIMEOUT_DIRECT_S = 8.0  # IMF-Direkt hat höhere 5xx-Quote → kürzer
MAX_RESULTS = 3
CACHE_TTL_S = 24 * 3600  # 24h

# Aktuelle WEO-Vintage in DBnomics. IMF veröffentlicht zweimal jährlich
# (April + Oktober). Hier konservativ auf Oktober 2024; falls neuere
# Vintage existiert, fällt der Code auf Trefferloses Result-Set zurück.
# Hot-fix: tools/refresh_imf_weo_vintage.py kann das saisonal aktualisieren.
_WEO_VINTAGE_CANDIDATES = ("WEO:2025-04", "WEO:2024-10", "WEO")

# ---------------------------------------------------------------------------
# Country-ISO-3-Mapping (Trigger-Wort → ISO3 + AnzeigeName-DE)
# Mind. 20 EU + Welt-Top
# ---------------------------------------------------------------------------
_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    # AT/DE/CH zuerst (höchste Treffer-Wahrscheinlichkeit im Faktencheck)
    "österreich": ("AUT", "Österreich"),
    "austria": ("AUT", "Österreich"),
    "deutschland": ("DEU", "Deutschland"),
    "germany": ("DEU", "Deutschland"),
    "schweiz": ("CHE", "Schweiz"),
    "switzerland": ("CHE", "Schweiz"),
    # EU-Top
    "frankreich": ("FRA", "Frankreich"),
    "france": ("FRA", "Frankreich"),
    "italien": ("ITA", "Italien"),
    "italy": ("ITA", "Italien"),
    "spanien": ("ESP", "Spanien"),
    "spain": ("ESP", "Spanien"),
    "niederlande": ("NLD", "Niederlande"),
    "netherlands": ("NLD", "Niederlande"),
    "belgien": ("BEL", "Belgien"),
    "belgium": ("BEL", "Belgien"),
    "polen": ("POL", "Polen"),
    "poland": ("POL", "Polen"),
    "tschechien": ("CZE", "Tschechien"),
    "czech": ("CZE", "Tschechien"),
    "ungarn": ("HUN", "Ungarn"),
    "hungary": ("HUN", "Ungarn"),
    "slowakei": ("SVK", "Slowakei"),
    "slovakia": ("SVK", "Slowakei"),
    "slowenien": ("SVN", "Slowenien"),
    "slovenia": ("SVN", "Slowenien"),
    "kroatien": ("HRV", "Kroatien"),
    "croatia": ("HRV", "Kroatien"),
    "griechenland": ("GRC", "Griechenland"),
    "greece": ("GRC", "Griechenland"),
    "portugal": ("PRT", "Portugal"),
    "irland": ("IRL", "Irland"),
    "ireland": ("IRL", "Irland"),
    "schweden": ("SWE", "Schweden"),
    "sweden": ("SWE", "Schweden"),
    "dänemark": ("DNK", "Dänemark"),
    "denmark": ("DNK", "Dänemark"),
    "finnland": ("FIN", "Finnland"),
    "finland": ("FIN", "Finnland"),
    "norwegen": ("NOR", "Norwegen"),
    "norway": ("NOR", "Norwegen"),
    "rumänien": ("ROU", "Rumänien"),
    "romania": ("ROU", "Rumänien"),
    "bulgarien": ("BGR", "Bulgarien"),
    "bulgaria": ("BGR", "Bulgarien"),
    # Welt-Top
    "usa": ("USA", "USA"),
    "united states": ("USA", "USA"),
    "vereinigte staaten": ("USA", "USA"),
    "großbritannien": ("GBR", "Vereinigtes Königreich"),
    "vereinigtes königreich": ("GBR", "Vereinigtes Königreich"),
    "uk": ("GBR", "Vereinigtes Königreich"),
    "britain": ("GBR", "Vereinigtes Königreich"),
    "china": ("CHN", "China"),
    "japan": ("JPN", "Japan"),
    "indien": ("IND", "Indien"),
    "india": ("IND", "Indien"),
    "brasilien": ("BRA", "Brasilien"),
    "brazil": ("BRA", "Brasilien"),
    "russland": ("RUS", "Russland"),
    "russia": ("RUS", "Russland"),
    "türkei": ("TUR", "Türkei"),
    "turkey": ("TUR", "Türkei"),
    "kanada": ("CAN", "Kanada"),
    "canada": ("CAN", "Kanada"),
    "australien": ("AUS", "Australien"),
    "australia": ("AUS", "Australien"),
    "südkorea": ("KOR", "Südkorea"),
    "south korea": ("KOR", "Südkorea"),
    "mexiko": ("MEX", "Mexiko"),
    "mexico": ("MEX", "Mexiko"),
    "ukraine": ("UKR", "Ukraine"),
}

# ---------------------------------------------------------------------------
# IMF-Indicator-Code-Mapping (Trigger-Wort → (WEO-Subject, Anzeige-DE, Einheit))
# ---------------------------------------------------------------------------
_INDICATOR_MAP: dict[str, tuple[str, str, str]] = {
    # BIP-Wachstum real (Hauptindikator für Prognosen)
    "bip-wachstum": ("NGDP_RPCH", "BIP-Wachstum (real)", "%"),
    "bip wachstum": ("NGDP_RPCH", "BIP-Wachstum (real)", "%"),
    "wirtschaftswachstum": ("NGDP_RPCH", "BIP-Wachstum (real)", "%"),
    "gdp growth": ("NGDP_RPCH", "BIP-Wachstum (real)", "%"),
    "gdp-wachstum": ("NGDP_RPCH", "BIP-Wachstum (real)", "%"),
    "real gdp": ("NGDP_RPCH", "BIP-Wachstum (real)", "%"),
    # Inflation
    "inflationsrate": ("PCPIPCH", "Inflation (VPI-Veränderung)", "%"),
    "inflation": ("PCPIPCH", "Inflation (VPI-Veränderung)", "%"),
    "verbraucherpreise": ("PCPIPCH", "Inflation (VPI-Veränderung)", "%"),
    "cpi": ("PCPIPCH", "Inflation (VPI-Veränderung)", "%"),
    # Arbeitslosenquote
    "arbeitslosenquote": ("LUR", "Arbeitslosenquote", "%"),
    "arbeitslosigkeit": ("LUR", "Arbeitslosenquote", "%"),
    "unemployment": ("LUR", "Arbeitslosenquote", "%"),
    # Government Debt-to-GDP
    "staatsverschuldung": ("GGXWDG_NGDP", "Staatsverschuldung (% BIP)", "% BIP"),
    "schuldenquote": ("GGXWDG_NGDP", "Staatsverschuldung (% BIP)", "% BIP"),
    "schuldenstand": ("GGXWDG_NGDP", "Staatsverschuldung (% BIP)", "% BIP"),
    "schulden-niveau": ("GGXWDG_NGDP", "Staatsverschuldung (% BIP)", "% BIP"),
    "schulden niveau": ("GGXWDG_NGDP", "Staatsverschuldung (% BIP)", "% BIP"),
    "debt-to-gdp": ("GGXWDG_NGDP", "Staatsverschuldung (% BIP)", "% BIP"),
    "debt to gdp": ("GGXWDG_NGDP", "Staatsverschuldung (% BIP)", "% BIP"),
    "government debt": ("GGXWDG_NGDP", "Staatsverschuldung (% BIP)", "% BIP"),
}

# Direkt-Trigger
_DIRECT_TERMS = (
    "imf", "iwf",
    "world economic outlook", "weo",
    "international monetary fund",
    "internationaler währungsfonds",
    "internationaler waehrungsfonds",
    "article iv",
    "financial soundness",
    "financial stability report",
)

# Prognose-Term-Trigger (für Composite)
_FORECAST_TERMS = (
    "prognose", "vorhersage", "forecast",
    "erwartet", "geschätzt", "schätzt",
    "voraussichtlich",
)


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_imf(claim_lc: str) -> bool:
    """Conservative Trigger:
    1. Direkter Term ("imf"/"iwf"/"weo"/"article iv"/…) → True
    2. Composite: Indikator + Land (Prognose-Kontext) → True
    """
    if not claim_lc:
        return False

    # 1. Direkt
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    # 2. Composite: Prognose-Indikator + Land
    has_indicator = any(t in claim_lc for t in _INDICATOR_MAP.keys())
    has_country = any(t in claim_lc for t in _COUNTRY_MAP.keys())
    has_forecast = any(t in claim_lc for t in _FORECAST_TERMS)

    if has_indicator and has_country and has_forecast:
        return True

    # 3. Spezifische Kombi: "Schulden-Niveau" + Land (auch ohne Forecast-Term)
    has_debt = any(t in claim_lc for t in (
        "schulden-niveau", "schulden niveau", "schuldenquote",
        "schuldenstand", "debt-to-gdp", "debt to gdp",
    ))
    if has_debt and has_country:
        return True

    return False


# Modul-Level-Cache: (claim_lc) → (ts, result)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_imf_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_imf(claim_lc)
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
# Extraction helpers
# ---------------------------------------------------------------------------
def _extract_country(claim_lc: str) -> tuple[str, str] | None:
    """Erstes gefundenes Land → (ISO3, AnzeigeName-DE). None falls keins."""
    # Längste Matches zuerst (z.B. "vereinigte staaten" vor "usa")
    sorted_hints = sorted(_COUNTRY_MAP.keys(), key=len, reverse=True)
    for hint in sorted_hints:
        if hint in claim_lc:
            return _COUNTRY_MAP[hint]
    return None


def _extract_indicators(claim_lc: str) -> list[tuple[str, str, str]]:
    """Liste der erkannten Indikatoren als (Code, AnzeigeName, Einheit).
    Reihenfolge: Reihenfolge im Mapping (BIP-Wachstum zuerst). Dedupliziert
    auf Code.
    """
    seen_codes: set[str] = set()
    out: list[tuple[str, str, str]] = []
    for hint, triple in _INDICATOR_MAP.items():
        if hint in claim_lc and triple[0] not in seen_codes:
            out.append(triple)
            seen_codes.add(triple[0])
    return out


def _format_value(val: float | int | None, unit: str) -> str:
    """Hübsche deutsche Zahl-Darstellung mit Vorzeichen."""
    if val is None:
        return "—"
    try:
        f = float(val)
    except (TypeError, ValueError):
        return str(val)
    sign = "+" if f >= 0 else ""
    # 2 Nachkommastellen für %, 1 für % BIP
    nd = 2 if unit == "%" else 1
    return f"{sign}{f:.{nd}f}".replace(".", ",")


def _imf_datamapper_url(indicator_code: str, iso3: str) -> str:
    """Stabiler IMF-Datamapper-Link (öffentlich, keine Auth)."""
    return f"https://www.imf.org/external/datamapper/{indicator_code}@WEO/{iso3}"


def _format_observation(
    *,
    iso3: str,
    country_name: str,
    indicator_code: str,
    indicator_name_de: str,
    unit: str,
    year: str,
    value: float | None,
    vintage: str | None,
    source_path: str,
) -> dict:
    """Baue ein Evidora-Schema-Result für eine WEO-Beobachtung."""
    val_str = _format_value(value, unit)
    is_forecast = False
    try:
        # IMF WEO publiziert Schätzungen für aktuelles + Zukunfts-Jahre.
        # Konservativ: alles ≥ aktuelles Jahr als Schätzung markieren.
        is_forecast = int(year) >= time.gmtime().tm_year
    except (TypeError, ValueError):
        pass

    vintage_str = f" (IMF WEO {vintage})" if vintage else " (IMF WEO)"
    forecast_caveat = (
        " — Schätzung des IWF, nicht garantierter Ist-Wert"
        if is_forecast else ""
    )

    display = (
        f"{country_name}: {indicator_name_de} {year}: "
        f"{val_str} {unit}{vintage_str}{forecast_caveat}"
    )

    description = (
        f"IMF World Economic Outlook (WEO) Datenbank — "
        f"{indicator_name_de} für {country_name} ({iso3}), Jahr {year}. "
        f"Wert: {val_str} {unit}. "
        f"Quellenpfad: {source_path}. "
        + (
            "WICHTIG: WEO-Werte für das laufende Jahr und Folgejahre sind "
            "Modell-Schätzungen des IWF; sie können von späteren amtlichen "
            "Statistiken abweichen. Keine Aussage über Verschulden."
            if is_forecast else
            "Wert basiert auf amtlichen Nationalkonten / IWF-Konsolidierung. "
            "Methodologie: IMF WEO Database (FAQ siehe imf.org)."
        )
    )

    return {
        "indicator_name": (
            f"WEO {indicator_name_de}, {country_name}"
        )[:300],
        "indicator": f"imf_weo_{indicator_code.lower()}_{iso3.lower()}",
        "country": iso3,
        "country_name": country_name,
        "year": year,
        "value": value,
        "display_value": display,
        "description": description,
        "url": _imf_datamapper_url(indicator_code, iso3),
        "source": "IMF WEO (International Monetary Fund)",
    }


# ---------------------------------------------------------------------------
# HTTP-Calls
# ---------------------------------------------------------------------------
async def _fetch_dbnomics_series(
    client,
    *,
    dataset: str,
    series_code: str,
) -> dict | None:
    """GET via DBnomics-Mirror. Robust gegen 404 → silentes None."""
    url = (
        f"{DBNOMICS_API}/series/IMF/{quote_plus(dataset)}/"
        f"{quote_plus(series_code)}?observations=1"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"IMF→DBnomics HTTP {resp.status_code} for "
                f"IMF/{dataset}/{series_code}"
            )
            return None
        data = resp.json()
    except Exception as e:
        logger.debug(
            f"IMF→DBnomics fetch failed IMF/{dataset}/{series_code}: {e}"
        )
        return None

    series_docs = (data.get("series") or {}).get("docs") or []
    if not series_docs:
        return None
    return series_docs[0]


async def _fetch_imf_direct_weo(
    client,
    *,
    iso3: str,
    indicator_code: str,
) -> dict | None:
    """Versuch via IMF-Direkt-SDMX. URN-Schema änderte sich 2024/2025;
    wir probieren das aktuell dokumentierte Format und brechen bei 404
    silent ab — der Caller fällt dann auf DBnomics zurück.

    Frequenz A=annual. Key-Format: A.{iso3}.{subject}.
    """
    # WEO-URN-Kandidaten (IMF wechselt zwischen "RES,WEO" und "STA,WEO")
    urn_candidates = (
        "IMF.RES,WEO,1.0",
        "IMF.STA,WEO,1.0",
    )
    for urn in urn_candidates:
        key = f"A.{iso3}.{indicator_code}"
        url = (
            f"{IMF_SDMX_API}/{quote_plus(urn, safe=',.')}/"
            f"{quote_plus(key, safe='.')}?format=jsondata"
        )
        try:
            resp = await client.get(
                url, follow_redirects=True, timeout=TIMEOUT_DIRECT_S,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception as e:
            logger.debug(f"IMF direct fetch failed for {urn}/{key}: {e}")
            continue

        # SDMX-JSON: data.dataSets[0].series → keys "0:0:0" → observations
        try:
            ds = (data.get("dataSets") or data.get("data", {}).get("dataSets"))
            if not ds:
                continue
            series = (ds[0].get("series") or {})
            if not series:
                continue
            # Es gibt typischerweise nur 1 Serie pro Single-Country-Single-Key
            _first_key = next(iter(series))
            obs_map = series[_first_key].get("observations") or {}
            if not obs_map:
                continue
            # Mapping idx→year via structure → observation → 0 → values
            structure = (
                data.get("structure")
                or data.get("structures", [{}])[0]
            )
            time_dim = None
            for dim in (
                structure.get("dimensions", {}).get("observation") or []
            ):
                if dim.get("id") == "TIME_PERIOD":
                    time_dim = dim
                    break
            if not time_dim:
                continue
            time_values = [
                v.get("id") for v in (time_dim.get("values") or [])
            ]
            # Sortierte (Jahr, Wert)
            pairs: list[tuple[str, float]] = []
            for idx_str, obs in obs_map.items():
                try:
                    idx = int(idx_str)
                except ValueError:
                    continue
                if idx >= len(time_values):
                    continue
                yr = time_values[idx]
                val = obs[0] if isinstance(obs, list) and obs else None
                if val is None:
                    continue
                pairs.append((yr, float(val)))
            if not pairs:
                continue
            pairs.sort(key=lambda kv: kv[0])
            return {
                "_path": f"imf_direct/{urn}",
                "period": [p for p, _ in pairs],
                "value": [v for _, v in pairs],
                "vintage": urn.replace("IMF.", "").split(",")[1] if "," in urn else "WEO",
            }
        except Exception as e:
            logger.debug(f"IMF direct parse failed for {urn}: {e}")
            continue
    return None


def _pick_latest_value(
    periods: list, values: list,
) -> tuple[str, float] | None:
    """Letzte nicht-None-Beobachtung."""
    if not periods or not values:
        return None
    for p, v in zip(reversed(periods), reversed(values)):
        if v is None:
            continue
        try:
            return str(p), float(v)
        except (TypeError, ValueError):
            continue
    return None


# ---------------------------------------------------------------------------
# Per-(iso3, indicator) Lookup mit Fallback-Kette
# ---------------------------------------------------------------------------
async def _lookup_one(
    client,
    *,
    iso3: str,
    country_name: str,
    indicator_code: str,
    indicator_name_de: str,
    unit: str,
) -> dict | None:
    """Versuch in Reihenfolge:
       (a) IMF-Direkt-SDMX (kurzes Timeout)
       (b) DBnomics-IMF-Mirror mit jeder Vintage-Variante
    Cached pro (iso3, indicator_code) für 24h.
    """
    cache_key = f"weo::{iso3}::{indicator_code}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached[0] if cached else None

    # (a) IMF Direct
    direct = await _fetch_imf_direct_weo(
        client, iso3=iso3, indicator_code=indicator_code,
    )
    if direct:
        latest = _pick_latest_value(direct.get("period", []),
                                    direct.get("value", []))
        if latest:
            year, value = latest
            result = _format_observation(
                iso3=iso3,
                country_name=country_name,
                indicator_code=indicator_code,
                indicator_name_de=indicator_name_de,
                unit=unit,
                year=year,
                value=value,
                vintage=direct.get("vintage"),
                source_path="IMF Direct SDMX",
            )
            _cache_put(cache_key, [result])
            return result

    # (b) DBnomics-Fallback (probiere Vintages der Reihe nach)
    for vintage in _WEO_VINTAGE_CANDIDATES:
        series_code = f"{iso3}.{indicator_code}"
        doc = await _fetch_dbnomics_series(
            client, dataset=vintage, series_code=series_code,
        )
        if not doc:
            continue
        latest = _pick_latest_value(
            doc.get("period", []), doc.get("value", []),
        )
        if not latest:
            continue
        year, value = latest
        result = _format_observation(
            iso3=iso3,
            country_name=country_name,
            indicator_code=indicator_code,
            indicator_name_de=indicator_name_de,
            unit=unit,
            year=year,
            value=value,
            vintage=vintage.replace("WEO:", "").replace("WEO", ""),
            source_path=f"DBnomics-Mirror IMF/{vintage}",
        )
        _cache_put(cache_key, [result])
        return result

    _cache_put(cache_key, [])
    return None


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_imf(analysis: dict) -> dict:
    """Live-Lookup gegen IMF (mit DBnomics-Fallback).

    Strategie:
    1. Trigger-Check (claim_mentions_imf_cached)
    2. Country + Indikator(en) aus Claim extrahieren
    3. Pro Kombi: IMF-Direkt versuchen → bei Fehler DBnomics-Mirror
    4. Max MAX_RESULTS, mit Prognose-Caveat-Disclaimer
    """
    empty = {
        "source": "IMF",
        "type": "imf_data",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_imf(matchable):
        return empty

    country = _extract_country(matchable)
    if not country:
        # Ohne Land kein WEO-Lookup möglich — IMF braucht Country-Key
        logger.info("IMF: trigger fired, but no country detected → empty")
        return empty
    iso3, country_name = country

    indicators = _extract_indicators(matchable)
    if not indicators:
        # Default: BIP-Wachstum, wenn Article-IV / WEO-Direkt-Term aber kein
        # Indikator-Term gefunden wurde.
        if any(t in matchable for t in (
            "article iv", "weo", "world economic outlook", "imf", "iwf",
        )):
            indicators = [("NGDP_RPCH", "BIP-Wachstum (real)", "%")]
        else:
            logger.info(
                f"IMF: trigger fired for {iso3}, but no indicator detected"
            )
            return empty

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for indicator_code, indicator_name_de, unit in indicators:
            try:
                r = await _lookup_one(
                    client,
                    iso3=iso3,
                    country_name=country_name,
                    indicator_code=indicator_code,
                    indicator_name_de=indicator_name_de,
                    unit=unit,
                )
            except Exception as e:
                logger.debug(
                    f"IMF lookup {iso3}/{indicator_code} crashed: {e}"
                )
                continue
            if r:
                results.append(r)
            if len(results) >= MAX_RESULTS:
                break

    if not results:
        logger.info(
            f"IMF: 0 Treffer für country={iso3} "
            f"indicators={[i[0] for i in indicators]}"
        )
        return empty

    logger.info(
        f"IMF: {len(results)} Treffer für country={iso3} "
        f"indicators={[i[0] for i in indicators]}"
    )
    return {
        "source": "IMF",
        "type": "imf_data",
        "results": results,
    }
