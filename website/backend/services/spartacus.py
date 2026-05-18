"""GeoSphere SPARTACUS — 1km-Klima-Gitterdaten für Österreich.

Datenquelle: GeoSphere Austria Data Hub, Dataset
`timeseries/historical/spartacus-v2-1y-1km` (jährlich aggregierte
Gitter-Reanalysedaten seit 1961).

Komplementär zu `services/geosphere.py` (Stationsdaten an ~9 Bundesland-
Hauptstädten): SPARTACUS liefert für jeden 1km²-Pixel in Österreich
flächendeckende Klima-Werte aus interpolierten Stationsmessungen
(„Spatiotemporal Reanalysis Dataset for Climate in Austria", v2).

Wichtig — Trennung Stationsdaten vs. Gitter:
- geosphere.py = Punkt-Messung an einzelner Wetterstation
- spartacus.py = Flächen-Mittel auf 1km² (interpoliert, urbaner
  Wärmeinsel-Effekt teilweise rausgemittelt)

Use-Case-Trigger:
- „Temperatur in [AT-Stadt/Region] seit [Jahr]"
- „Klimawandel [AT-Bezirk]" (Bezirk hat oft keine eigene Station)
- „SPARTACUS", „1km-Klima-Grid"
- AT-Stadt-/Region-Name + Klima-Begriff (Trend / Erwärmung / Niederschlag)

API:
  GET https://dataset.api.hub.geosphere.at/v1/timeseries/historical/
       spartacus-v2-1y-1km
       ?parameters=TM&lat_lon=48.21,16.37&start=2020-01-01&end=2023-12-31

Parameter:
  TM = yearly mean of air temperature (°C)
  RR = yearly precipitation sum (kg/m² = mm)
  SA = yearly duration of sunshine (s)

Lizenz: CC-BY 4.0 (GeoSphere Austria). Kein Auth, kein Rate-Limit-Header
dokumentiert; trotzdem mit ~0.4s zwischen Calls (vgl. geosphere.py).

WIRING-Snippet für main.py (NICHT in diesem PR; nur als Referenz):
------------------------------------------------------------------
  # imports
  from services.spartacus import (
      search_spartacus,
      claim_mentions_spartacus_cached as _claim_mentions_spartacus,
  )

  # in search-orchestration (parallel zu GeoSphere, etwa Z. 433+):
  # SPARTACUS: 1km-Gitter-Klimadaten für AT-Regionen/Bezirke
  if _claim_mentions_spartacus(claim):
      tasks.append(cached("SPARTACUS", search_spartacus, analysis))
      queried_names.append("GeoSphere SPARTACUS")

  # reranker.py whitelist (alphabetisch):
  #   "GeoSphere SPARTACUS",

  # data_updater.py prefetch (optional, AT-Klima ändert sich nicht stündlich):
  #   from services.spartacus import prefetch_spartacus  # falls implementiert
------------------------------------------------------------------
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

API_BASE = "https://dataset.api.hub.geosphere.at/v1"
DATASET = "timeseries/historical/spartacus-v2-1y-1km"

CACHE_TTL = 86400  # 24h
TREND_START_YEAR = 1961  # SPARTACUS-v2 reicht zurück bis 1961
DEFAULT_END_YEAR = 2024   # letzte komplette Jahresscheibe

# ---------------------------------------------------------------------------
# AT-Koordinaten-Mapping
# ---------------------------------------------------------------------------
# 15 Hauptorte für SPARTACUS-Punktabfragen (lat, lon). Bewusst breiter als
# `geosphere.py` (nur 9 Hauptstädte) — SPARTACUS deckt jeden 1km²-Pixel, also
# können wir auch kleinere Städte / Bezirke abfragen.
LOCATIONS: dict[str, dict] = {
    "wien":         {"lat": 48.210, "lon": 16.370, "state": "Wien",            "label": "Wien"},
    "salzburg":     {"lat": 47.810, "lon": 13.040, "state": "Salzburg",        "label": "Salzburg"},
    "innsbruck":    {"lat": 47.260, "lon": 11.390, "state": "Tirol",           "label": "Innsbruck"},
    "graz":         {"lat": 47.070, "lon": 15.440, "state": "Steiermark",      "label": "Graz"},
    "linz":         {"lat": 48.300, "lon": 14.290, "state": "Oberösterreich",  "label": "Linz"},
    "klagenfurt":   {"lat": 46.620, "lon": 14.310, "state": "Kärnten",         "label": "Klagenfurt"},
    "bregenz":      {"lat": 47.500, "lon":  9.740, "state": "Vorarlberg",      "label": "Bregenz"},
    "eisenstadt":   {"lat": 47.850, "lon": 16.520, "state": "Burgenland",      "label": "Eisenstadt"},
    "st. pölten":   {"lat": 48.200, "lon": 15.620, "state": "Niederösterreich","label": "St. Pölten"},
    "villach":      {"lat": 46.610, "lon": 13.850, "state": "Kärnten",         "label": "Villach"},
    "wels":         {"lat": 48.160, "lon": 14.030, "state": "Oberösterreich",  "label": "Wels"},
    "dornbirn":     {"lat": 47.410, "lon":  9.740, "state": "Vorarlberg",      "label": "Dornbirn"},
    "krems":        {"lat": 48.410, "lon": 15.610, "state": "Niederösterreich","label": "Krems an der Donau"},
    "leoben":       {"lat": 47.380, "lon": 15.090, "state": "Steiermark",      "label": "Leoben"},
    "kitzbühel":    {"lat": 47.450, "lon": 12.390, "state": "Tirol",           "label": "Kitzbühel"},
}

# Aliase → kanonischer Key
LOCATION_ALIASES: dict[str, str] = {
    "vienna": "wien",
    "wien": "wien",
    "salzburg": "salzburg",
    "innsbruck": "innsbruck",
    "graz": "graz",
    "linz": "linz",
    "klagenfurt": "klagenfurt",
    "bregenz": "bregenz",
    "eisenstadt": "eisenstadt",
    "st. pölten": "st. pölten",
    "st.pölten": "st. pölten",
    "sankt pölten": "st. pölten",
    "st pölten": "st. pölten",
    "villach": "villach",
    "wels": "wels",
    "dornbirn": "dornbirn",
    "krems": "krems",
    "leoben": "leoben",
    "kitzbühel": "kitzbühel",
    "kitzbuehel": "kitzbühel",
    # Bundesland-Mappings (nimm Landeshauptstadt als Repräsentanten)
    "burgenland": "eisenstadt",
    "kärnten": "klagenfurt",
    "niederösterreich": "st. pölten",
    "oberösterreich": "linz",
    "steiermark": "graz",
    "tirol": "innsbruck",
    "vorarlberg": "bregenz",
}

# ---------------------------------------------------------------------------
# Trigger-Logik
# ---------------------------------------------------------------------------
# SPARTACUS-spezifische Direkt-Trigger
_SPARTACUS_DIRECT_TERMS = (
    "spartacus",
    "1km-klima-grid", "1km klima grid", "1-km-klima-grid",
    "klima-gitter", "klimagitter", "klimagrid",
    "spatiotemporal reanalysis", "klima-reanalyse",
    "gitter-klimadaten", "gridded climate",
)

# Klima-Schlagworte (DE/EN). Stamm-Formen wo möglich, damit
# Beugung/Komposita matchen.
_CLIMATE_TERMS = (
    "klima", "klimawandel", "klimakrise", "climate", "climate change",
    "global warming", "temperatur", "temperature",
    "erwärm",  # erwärmt, erwärmung, klimaerwärmung
    "abkühl",
    "hitze", "heatwave", "hitzewelle", "hitzerekord", "heat record",
    "kälterekord",
    "niederschlag", "regen", "precipitation", "rainfall", "trocken",
    "sonnenschein", "sunshine duration",
    "jahresmittel", "jahresmitteltemperatur", "annual mean",
    "trend", "dekade", "decade",
    "wärmer", "kälter", "warmer", "colder",
)

# AT-Kontext-Marker (Land-Begriff oder bekannte AT-Region/Stadt)
_AT_CONTEXT_TERMS = (
    "österreich", "austria", "alpenraum", "ostalpen", "südösterreich",
    "westösterreich", "ostösterreich", "österr.",
)


def _claim_mentions_spartacus(claim_lc: str) -> bool:
    """Trigger SPARTACUS-Service.

    Zwei Trigger-Pfade:
    1. Direkter SPARTACUS-/Gitter-Begriff (immer triggern).
    2. Klima-Schlagwort + AT-Kontext (entweder Land-Begriff oder bekannte
       Stadt/Region im LOCATION_ALIASES-Wortschatz).
    """
    # Pfad 1: direkter Begriff
    if any(t in claim_lc for t in _SPARTACUS_DIRECT_TERMS):
        return True

    # Pfad 2: Klima-Wort + AT-Kontext
    has_climate = any(t in claim_lc for t in _CLIMATE_TERMS)
    if not has_climate:
        return False

    if any(t in claim_lc for t in _AT_CONTEXT_TERMS):
        return True

    # Klima + bekannte AT-Stadt/Region (Bundesland-Alias)
    # Längere Aliase zuerst, damit „st. pölten" vor „pölten" matcht.
    for alias in sorted(LOCATION_ALIASES.keys(), key=len, reverse=True):
        if alias in claim_lc:
            return True

    return False


def claim_mentions_spartacus_cached(claim: str) -> bool:
    """Cache-freundlicher Trigger (analog claim_mentions_oenb_cached)."""
    return _claim_mentions_spartacus((claim or "").lower())


# ---------------------------------------------------------------------------
# In-process Cache
# ---------------------------------------------------------------------------
# {location_key: {"TM": {year: val}, "RR": {year: val}, "fetched_at": ts}}
_data_cache: dict[str, dict] = {}


def _cache_fresh(entry: dict | None) -> bool:
    if not entry:
        return False
    return (time.time() - entry.get("fetched_at", 0.0)) < CACHE_TTL


# ---------------------------------------------------------------------------
# API-Fetch
# ---------------------------------------------------------------------------
async def _fetch_location(
    client: httpx.AsyncClient,
    loc_key: str,
    start_year: int = TREND_START_YEAR,
    end_year: int = DEFAULT_END_YEAR,
) -> dict[str, dict[int, float]]:
    """Fetch TM (Temperatur) + RR (Niederschlag) für einen Punkt."""
    info = LOCATIONS[loc_key]
    params = {
        "parameters": "TM,RR",
        "lat_lon": f"{info['lat']},{info['lon']}",
        "start": f"{start_year}-01-01",
        "end": f"{end_year}-12-31",
    }
    resp = await client.get(f"{API_BASE}/{DATASET}", params=params)
    resp.raise_for_status()
    payload = resp.json()

    timestamps: list[str] = payload.get("timestamps", []) or []
    features = payload.get("features", []) or []
    if not features:
        return {}
    params_dict = features[0].get("properties", {}).get("parameters", {}) or {}

    out: dict[str, dict[int, float]] = {}
    for pname in ("TM", "RR"):
        pblock = params_dict.get(pname) or {}
        values = pblock.get("data") or []
        series: dict[int, float] = {}
        for ts, v in zip(timestamps, values):
            if v is None:
                continue
            try:
                year = int(str(ts)[:4])
                series[year] = float(v)
            except (ValueError, TypeError):
                continue
        if series:
            out[pname] = series
    return out


async def fetch_spartacus_point(
    loc_key: str,
    client: httpx.AsyncClient | None = None,
) -> dict[str, dict[int, float]]:
    """Public fetch — cached pro Location (24h TTL)."""
    entry = _data_cache.get(loc_key)
    if _cache_fresh(entry):
        return {k: v for k, v in entry.items() if k != "fetched_at"}

    close = False
    if client is None:
        client = polite_client(timeout=30.0)
        close = True

    try:
        try:
            data = await _fetch_location(client, loc_key)
        except Exception as e:
            logger.warning(f"SPARTACUS: fetch failed for {loc_key}: {e}")
            return {}
        if not data:
            logger.warning(f"SPARTACUS: empty response for {loc_key}")
            return {}
        _data_cache[loc_key] = {**data, "fetched_at": time.time()}
        return data
    finally:
        if close:
            await client.aclose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _detect_locations(claim_lc: str) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for alias in sorted(LOCATION_ALIASES.keys(), key=len, reverse=True):
        if alias in claim_lc:
            canonical = LOCATION_ALIASES[alias]
            if canonical not in seen:
                found.append(canonical)
                seen.add(canonical)
    return found


def _linear_trend(series: dict[int, float]) -> tuple[float, int] | None:
    """Slope per decade (least squares) über die volle Reihe.

    Returns ``(slope_per_decade, n_used)`` oder None bei <20 Punkten.
    """
    if not series:
        return None
    points = sorted(series.items())
    n = len(points)
    if n < 20:
        return None
    mx = sum(p[0] for p in points) / n
    my = sum(p[1] for p in points) / n
    num = sum((x - mx) * (y - my) for x, y in points)
    den = sum((x - mx) ** 2 for x, y in points)
    if den == 0:
        return None
    return (num / den) * 10, n


def _build_temperature_result(loc_key: str, info: dict, tm: dict[int, float]) -> dict:
    latest_year = max(tm)
    latest_val = tm[latest_year]
    earliest_year = min(tm)
    earliest_val = tm[earliest_year]
    diff = latest_val - earliest_val
    trend = _linear_trend(tm)

    desc_parts = [
        f"Jahresmittel-Lufttemperatur ({info['label']}, "
        f"1km-Gitterzelle, SPARTACUS v2): {latest_val:.1f}°C in {latest_year}, "
        f"{earliest_val:.1f}°C in {earliest_year} — "
        f"Differenz {diff:+.1f}°C über {latest_year - earliest_year} Jahre.",
    ]
    if trend:
        slope, n = trend
        arrow = "↑" if slope > 0.01 else ("↓" if slope < -0.01 else "→")
        desc_parts.append(
            f"Linearer Trend (n={n} Jahre): {arrow} {slope:+.2f}°C / Dekade."
        )
    warmest = max(tm, key=lambda y: tm[y])
    coldest = min(tm, key=lambda y: tm[y])
    desc_parts.append(
        f"Wärmstes Jahr in der Reihe: {warmest} ({tm[warmest]:.1f}°C); "
        f"kältestes Jahr: {coldest} ({tm[coldest]:.1f}°C)."
    )

    title = (
        f"SPARTACUS Jahresmitteltemperatur — {info['label']} ({info['state']}) "
        f"{latest_year}: {latest_val:.1f}°C"
    )

    return {
        "indicator_name": title,
        "indicator": "spartacus_TM",
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(latest_year),
        "value": latest_val,
        "display_value": f"{latest_val:.1f}°C",
        "url": (
            "https://data.hub.geosphere.at/dataset/spartacus-v2-1y-1km"
        ),
        "source": "GeoSphere SPARTACUS",
        "description": " ".join(desc_parts),
    }


def _build_precip_result(loc_key: str, info: dict, rr: dict[int, float]) -> dict:
    latest_year = max(rr)
    latest_val = rr[latest_year]
    mean_val = sum(rr.values()) / len(rr)

    desc_parts = [
        f"Jahres-Niederschlagssumme ({info['label']}, 1km-Gitterzelle, "
        f"SPARTACUS v2): {latest_val:.0f} mm in {latest_year}; "
        f"langjähriges Mittel der Reihe ({min(rr)}–{max(rr)}): "
        f"{mean_val:.0f} mm.",
    ]
    trend = _linear_trend(rr)
    if trend:
        slope, n = trend
        arrow = "↑" if slope > 0.5 else ("↓" if slope < -0.5 else "→")
        desc_parts.append(
            f"Linearer Trend (n={n} Jahre): {arrow} {slope:+.1f} mm / Dekade."
        )
    wettest = max(rr, key=lambda y: rr[y])
    driest = min(rr, key=lambda y: rr[y])
    desc_parts.append(
        f"Nassestes Jahr: {wettest} ({rr[wettest]:.0f} mm); "
        f"trockenstes Jahr: {driest} ({rr[driest]:.0f} mm)."
    )

    title = (
        f"SPARTACUS Jahresniederschlag — {info['label']} ({info['state']}) "
        f"{latest_year}: {latest_val:.0f} mm"
    )

    return {
        "indicator_name": title,
        "indicator": "spartacus_RR",
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(latest_year),
        "value": latest_val,
        "display_value": f"{latest_val:.0f} mm",
        "url": (
            "https://data.hub.geosphere.at/dataset/spartacus-v2-1y-1km"
        ),
        "source": "GeoSphere SPARTACUS",
        "description": " ".join(desc_parts),
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_spartacus(analysis: dict) -> dict:
    """Search SPARTACUS 1km grid for Austrian regions/cities/districts.

    Trigger:
    - SPARTACUS-Direktbegriff, ODER
    - Klima-Keyword + (AT-Kontext ODER bekannte AT-Stadt/Region)

    Returns gridded (interpolated) climate values; komplementär zu
    `services/geosphere.py` (Stationspunkt-Messungen).
    """
    empty = {"source": "GeoSphere SPARTACUS", "type": "climate_grid", "results": []}

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_spartacus(matchable):
        return empty

    locations = _detect_locations(matchable)
    if not locations:
        # Kein konkreter Ort genannt, aber Trigger angesprungen — falls
        # AT-Kontext oder SPARTACUS-Direktbegriff: Wien als
        # Default-Repräsentanten anzeigen, damit der Synthesizer mindestens
        # eine konkrete Reihe sieht.
        locations = ["wien"]

    # Maximal 3 Orte abfragen — analog geosphere.py — damit der Synthesizer
    # nicht unter zu vielen Quellen erstickt.
    locations = locations[:3]

    client = polite_client(timeout=30.0)
    try:
        results: list[dict] = []
        for i, loc_key in enumerate(locations):
            info = LOCATIONS.get(loc_key)
            if not info:
                continue
            data = await fetch_spartacus_point(loc_key, client=client)
            if not data:
                continue
            tm = data.get("TM") or {}
            rr = data.get("RR") or {}
            if tm:
                results.append(_build_temperature_result(loc_key, info, tm))
            if rr:
                results.append(_build_precip_result(loc_key, info, rr))
            # Politeness-Delay zwischen Calls (analog geosphere.py: 0.4s)
            if i < len(locations) - 1:
                await asyncio.sleep(0.4)
    finally:
        await client.aclose()

    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: SPARTACUS-Gitterdaten",
            "indicator": "context",
            "country": "AUT",
            "country_name": "Österreich",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://data.hub.geosphere.at/dataset/spartacus-v2-1y-1km",
            "source": "GeoSphere SPARTACUS",
            "description": (
                "SPARTACUS v2 (Spatiotemporal Reanalysis Dataset for Climate "
                "in Austria) liefert flächendeckende 1km²-Gitterdaten ab 1961, "
                "interpoliert aus ~250 GeoSphere-Stationen. Unterschied zu "
                "Stations-Messungen (vgl. geosphere.py): "
                "(1) Gitter ≠ Station — der hier ausgegebene Wert ist der "
                "Mittelwert des 1km²-Pixels, in dem die genannte Koordinate "
                "liegt; urbaner Wärmeinsel-Effekt teilweise rausgemittelt. "
                "(2) Reanalyse — Werte stammen aus interpolierter Modellrechnung, "
                "nicht aus direkter Punkt-Messung am Ort. "
                "(3) Trends über ≥30 Jahre belastbar; einzelne Jahre schwanken "
                "natürlich um ±1°C. "
                "(4) Referenzperiode WMO: 1991–2020. Diese Baseline liegt "
                "selbst bereits ~+0.7°C über vorindustriellem Niveau "
                "(1850–1900), Anomalien gegen sie unterschätzen daher die "
                "Gesamterwärmung. "
                "Lizenz: CC-BY 4.0, GeoSphere Austria."
            ),
        })

    logger.info(
        f"SPARTACUS: locations={locations}, results={len(results)}"
    )
    return {
        "source": "GeoSphere SPARTACUS",
        "type": "climate_grid",
        "results": results,
    }
