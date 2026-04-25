"""GeoSphere Austria — regionale Klimadaten für österreichische Städte.

Datenquelle: GeoSphere Austria Data Hub (data.hub.geosphere.at), Dataset
`klima-v2-1y` (jährliche Stationsdaten ab 1775 für die ältesten Reihen).

Ergänzt Berkeley Earth (Land-Ebene) und NASA GISS (global) um Stationsdaten
für die neun Bundesland-Hauptstädte. Schließt damit die City-Level-Lücke,
die Berkeley nicht abdeckt — relevant für Claims wie:
- „Wien wird wärmer"
- „Innsbruck Hitzerekord 2024"
- „In Graz hat sich das Klima in den letzten 50 Jahren um X°C erwärmt"

Lizenz: CC BY 4.0 (GeoSphere Austria, Bundesanstalt für Meteorologie).
Attribution-Pflicht via API-Terms.

Konsumiertes Indikatorenfeld: `tl_mittel` (Lufttemperatur 2m Jahresmittelwert).

Caveat:
- Stationsdaten ≠ Flächenmittel (Berkeley/Spartacus liefern flächendeckende Felder).
- Stadt-Stationen können urbanen Wärmeinsel-Effekt zeigen, der über die globale
  Erwärmung hinausgeht.
- Stationen wurden teils umverlegt (z.B. Wien Hohe Warte 1775→1872→aktuell).
- Trends erst über ≥30 Jahre belastbar.
"""

import asyncio
import logging
import time

import httpx

logger = logging.getLogger("evidora")

API_BASE = "https://dataset.api.hub.geosphere.at/v1"
DATASET = "station/historical/klima-v2-1y"

CACHE_TTL = 86400  # 24h

# {station_id: {year: value, ...}}
_data_cache: dict[int, dict[int, float]] = {}
_cache_time: float = 0.0

# Canonische Stationen pro Stadt/Bundesland.
# IDs gemäß `klima-v2-1y` metadata (Stand 2026-04). Bevorzugt die "modernen"
# Stationen mit zuverlässiger Abdeckung der letzten ~30 Jahre.
STATIONS: dict[str, dict] = {
    "wien": {
        "id": 5904,
        "name": "Wien Hohe Warte",
        "state": "Wien",
        "lat": 48.249,
        "lon": 16.356,
        "alt": 198.0,
    },
    "salzburg": {
        "id": 6300,
        "name": "Salzburg Flughafen",
        "state": "Salzburg",
        "lat": 47.793,
        "lon": 13.000,
        "alt": 430.0,
    },
    "innsbruck": {
        "id": 11803,
        "name": "Innsbruck Universität",
        "state": "Tirol",
        "lat": 47.260,
        "lon": 11.385,
        "alt": 578.0,
    },
    "graz": {
        "id": 16412,
        "name": "Graz Universität",
        "state": "Steiermark",
        "lat": 47.078,
        "lon": 15.450,
        "alt": 366.7,
    },
    "linz": {
        "id": 3202,
        "name": "Linz Stadt",
        "state": "Oberösterreich",
        "lat": 48.300,
        "lon": 14.286,
        "alt": 262.0,
    },
    "klagenfurt": {
        "id": 20212,
        "name": "Klagenfurt Flughafen",
        "state": "Kärnten",
        "lat": 46.650,
        "lon": 14.324,
        "alt": 450.0,
    },
    "bregenz": {
        "id": 11102,
        "name": "Bregenz",
        "state": "Vorarlberg",
        "lat": 47.500,
        "lon": 9.735,
        "alt": 424.0,
    },
    "eisenstadt": {
        "id": 7704,
        "name": "Eisenstadt Nordost",
        "state": "Burgenland",
        "lat": 47.853,
        "lon": 16.547,
        "alt": 184.0,
    },
    "st. pölten": {
        "id": 5609,
        "name": "St. Pölten Landhaus",
        "state": "Niederösterreich",
        "lat": 48.205,
        "lon": 15.620,
        "alt": 273.6,
    },
}

# Aliase / Schreibvarianten → kanonisches Stadt-Keyword
CITY_ALIASES = {
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
    # Bundesland-Mappings (wenn Bundesland statt Stadt erwähnt)
    "burgenland": "eisenstadt",
    "kärnten": "klagenfurt",
    "niederösterreich": "st. pölten",
    "oberösterreich": "linz",
    "steiermark": "graz",
    "tirol": "innsbruck",
    "vorarlberg": "bregenz",
}

# Climate-relevant keywords (DE + EN)
CLIMATE_KEYWORDS = [
    "klima", "klimawandel", "klimaerwärmung", "erwärmung", "klimakrise",
    "climate", "climate change", "global warming",
    "temperatur", "temperature", "temperaturen",
    "hitzerekord", "hitze", "heat record", "heatwave", "hitzewelle",
    "wärmste", "wärmster", "wärmstes", "warmest year",
    "kälteste", "kältester", "kältestes", "coldest year",
    "jahresmittel", "jahresmitteltemperatur", "annual mean temperature",
    "wärmer", "kälter", "warmer", "colder",
    "rekordjahr", "record year",
]

# Reference period: 1991-2020 (WMO-Standard für Klimanormalperiode)
REF_START = 1991
REF_END = 2020
TREND_YEARS = 50  # 50-year trend (analog Berkeley)


async def _fetch_station(
    client: httpx.AsyncClient, station_id: int, start_year: int = 1970
) -> dict[int, float]:
    """Fetch annual mean temperature (tl_mittel) for a single station."""
    params = {
        "parameters": "tl_mittel",
        "station_ids": str(station_id),
        # Annual data with offset; klima-v2-1y entries are timestamped Jan 1 UTC
        "start": f"{start_year}-01-01T00:00",
        "end": "2025-12-31T23:59",
    }
    resp = await client.get(f"{API_BASE}/{DATASET}", params=params)
    resp.raise_for_status()
    payload = resp.json()
    timestamps: list[str] = payload.get("timestamps", [])
    features = payload.get("features", [])
    if not features:
        return {}
    params_dict = features[0].get("properties", {}).get("parameters", {})
    tl = params_dict.get("tl_mittel", {})
    values = tl.get("data", [])
    out: dict[int, float] = {}
    for ts, v in zip(timestamps, values):
        if v is None:
            continue
        try:
            year = int(ts[:4])
            out[year] = float(v)
        except (ValueError, TypeError):
            continue
    return out


async def fetch_geosphere(client: httpx.AsyncClient | None = None) -> dict:
    """Fetch annual temperature series for all canonical stations.

    Returns the populated cache (`{station_id: {year: value}}`) for callers
    that want to inspect cardinality.
    """
    global _data_cache, _cache_time

    now = time.time()
    if _data_cache and (now - _cache_time) < CACHE_TTL:
        return _data_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    try:
        # Sequential fetch with small delay to respect GeoSphere rate limits
        # (parallel requests trigger 429 Too Many Requests).
        new_cache: dict[int, dict[int, float]] = {}
        for city_key, info in STATIONS.items():
            try:
                series = await _fetch_station(client, info["id"])
            except Exception as e:
                logger.warning(
                    f"GeoSphere: failed to fetch {info['name']} "
                    f"(id={info['id']}): {e}"
                )
                continue
            if not series:
                logger.warning(
                    f"GeoSphere: empty data for {info['name']} (id={info['id']})"
                )
                continue
            new_cache[info["id"]] = series
            await asyncio.sleep(0.4)  # ~2.5 req/s — well below typical limits

        _data_cache = new_cache
        _cache_time = now
        total_points = sum(len(s) for s in new_cache.values())
        logger.info(
            f"GeoSphere: {len(new_cache)} stations cached, {total_points} station-years"
        )
        return new_cache
    finally:
        if close_client:
            await client.aclose()


def _claim_mentions_climate(claim: str) -> bool:
    """Check if the claim mentions climate-related terms."""
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in CLIMATE_KEYWORDS)


def _detect_cities(claim: str) -> list[str]:
    """Return list of canonical city keys mentioned in the claim."""
    claim_lower = claim.lower()
    found: list[str] = []
    seen: set[str] = set()
    # Längere Aliase zuerst, damit „st. pölten" vor „pölten" matcht
    for alias in sorted(CITY_ALIASES.keys(), key=len, reverse=True):
        if alias in claim_lower:
            canonical = CITY_ALIASES[alias]
            if canonical not in seen:
                found.append(canonical)
                seen.add(canonical)
    return found


def _claim_mentions_austria(analysis: dict) -> bool:
    """Heuristic: claim has Austria context (country mention or AT entity)."""
    claim_lower = analysis.get("claim", "").lower()
    if "österreich" in claim_lower or "austria" in claim_lower:
        return True
    countries = analysis.get("ner_entities", {}).get("countries", [])
    return any(("österreich" in c.lower() or "austria" in c.lower()) for c in countries)


def _compute_baseline(series: dict[int, float]) -> float | None:
    """Compute mean temperature over the WMO 1991-2020 reference period."""
    vals = [v for y, v in series.items() if REF_START <= y <= REF_END]
    if len(vals) < 15:  # need at least half the reference period
        return None
    return sum(vals) / len(vals)


def _compute_trend(series: dict[int, float], lookback: int = TREND_YEARS) -> tuple[float, int] | None:
    """Linear least-squares trend over the last `lookback` years.

    Returns (slope_per_decade, n_used) or None if insufficient data.
    """
    if not series:
        return None
    latest_year = max(series.keys())
    cutoff = latest_year - lookback + 1
    points = [(y, v) for y, v in sorted(series.items()) if y >= cutoff]
    n = len(points)
    if n < 20:
        return None
    mean_x = sum(p[0] for p in points) / n
    mean_y = sum(p[1] for p in points) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in points)
    den = sum((x - mean_x) ** 2 for x, y in points)
    if den == 0:
        return None
    slope_per_year = num / den
    return slope_per_year * 10, n  # slope per decade


def _format_station_entry(city_key: str, info: dict, series: dict[int, float]) -> dict:
    """Build a synthesizer-compatible result dict for one station."""
    if not series:
        return {}
    latest_year = max(series.keys())
    latest_val = series[latest_year]
    baseline = _compute_baseline(series)
    trend = _compute_trend(series)

    # Build description with anomaly + trend (LLM reads this for record-claims)
    desc_parts: list[str] = []
    if baseline is not None:
        anomaly = latest_val - baseline
        sign = "+" if anomaly >= 0 else ""
        desc_parts.append(
            f"Anomalie {latest_year}: {sign}{anomaly:.2f}°C vs. WMO-Referenzperiode "
            f"1991–2020 ({baseline:.1f}°C Mittel)."
        )
    if trend:
        slope, n = trend
        arrow = "↑" if slope > 0.01 else ("↓" if slope < -0.01 else "→")
        desc_parts.append(
            f"Trend (linearer Fit, letzte {n} Jahre): {arrow} {slope:+.2f}°C/Dekade."
        )
    # Add warmest/coldest year on record
    warmest_year = max(series, key=lambda y: series[y])
    coldest_year = min(series, key=lambda y: series[y])
    desc_parts.append(
        f"Wärmstes Jahr in der Reihe: {warmest_year} ({series[warmest_year]:.1f}°C). "
        f"Kältestes Jahr: {coldest_year} ({series[coldest_year]:.1f}°C)."
    )

    title = (
        f"{info['name']} — Jahresmitteltemperatur {latest_year}: {latest_val:.1f}°C "
        f"(Station {info['state']}, {info['alt']:.0f} m)"
    )

    return {
        "indicator_name": title,
        "indicator": "tl_mittel",
        "country": "AUT",
        "country_name": "Austria",
        "year": str(latest_year),
        "value": latest_val,
        "display_value": f"{latest_val:.1f}°C",
        "url": (
            "https://data.hub.geosphere.at/dataset/klima-v2-1y"
        ),
        "description": " ".join(desc_parts),
    }


async def search_geosphere(analysis: dict) -> dict:
    """Search GeoSphere stations for Austrian city/state climate data.

    Triggers when:
    - claim mentions a climate keyword AND
    - claim mentions either an Austrian city/state OR an Austrian country reference
    """
    claim = analysis.get("claim", "")
    if not _claim_mentions_climate(claim):
        return {"source": "GeoSphere Austria", "type": "official_data", "results": []}

    cities = _detect_cities(claim)
    if not cities and not _claim_mentions_austria(analysis):
        return {"source": "GeoSphere Austria", "type": "official_data", "results": []}

    # Default selection if no city is named but Austria is mentioned: Wien (capital)
    if not cities:
        cities = ["wien"]

    data = await fetch_geosphere()
    if not data:
        return {"source": "GeoSphere Austria", "type": "official_data", "results": []}

    results: list[dict] = []
    for city_key in cities[:3]:  # cap at 3 cities to stay under synthesizer limit
        info = STATIONS.get(city_key)
        if not info:
            continue
        series = data.get(info["id"])
        if not series:
            continue
        entry = _format_station_entry(city_key, info, series)
        if entry:
            results.append(entry)

    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: GeoSphere Stationsdaten",
            "indicator": "context",
            "country": "AUT",
            "country_name": "Austria",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://data.hub.geosphere.at/",
            "description": (
                "GeoSphere Austria (ehem. ZAMG) liefert Stationsmessungen einzelner "
                "Wetterstationen. Einschränkungen: "
                "(1) Punktmessung — nicht zu verwechseln mit flächendeckenden "
                "Mittelwerten (Berkeley Earth, Spartacus). Eine einzelne Station "
                "kann lokal abweichen (z. B. urbaner Wärmeinsel-Effekt in der "
                "Innenstadt vs. Umland). "
                "(2) Stationsumlegungen — historische Reihen wurden teils relokiert "
                "(z. B. Wien Hohe Warte 1775→1872→aktuell), was kleine Sprünge "
                "in der Reihe erzeugen kann. "
                "(3) Referenzperiode — die WMO-Klimanormalperiode 1991–2020 ist "
                "selbst bereits ca. +0.7°C über dem vorindustriellen Mittel "
                "(1850–1900). Anomalien gegen diese Baseline unterschätzen daher die "
                "Gesamterwärmung. "
                "(4) Jahresvariabilität — einzelne Jahre schwanken um ±1°C; "
                "belastbare Trends erst über ≥30 Jahre."
            ),
        })

    logger.info(
        f"GeoSphere: {len(results) - (1 if results else 0)} city results, cities={cities}"
    )
    return {"source": "GeoSphere Austria", "type": "official_data", "results": results}
