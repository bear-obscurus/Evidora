"""OpenAQ Live-Connector — Air-Quality-Sensor-Daten via OpenAQ v3 REST API.

OpenAQ aggregiert globale Luftqualitäts-Sensor-Daten von staatlichen
Mess-Stationen (EPA, EEA, Umweltbundesamt-AT/DE) PLUS zivilen IoT-Sensoren
(Sensor.Community u. ä.). Für Faktencheck-Zwecke besonders wertvoll für:

- „Wien hat schlechteste Luft" → AT-Stationen-Vergleich PM2.5/NO2
- „NO2 in Stuttgart über Grenzwert" → Stuttgart NO2 latest readings
- „Luftqualität Berlin schlechter als Madrid" → cross-city compare
- Komplementär zu UBA-Daten in mobilitaet_pack mit globalen Sensor-Daten

Komplementär zu existierenden Quellen:
- mobilitaet_pack: kuratierte Konsens-NO2/PM2.5-Aussagen für DACH-Hotspots
- Wikipedia: enzyklopädische Schadstoff-Definitionen
- GDELT: aktuelle News-Coverage zu Luftqualitäts-Vorfällen
- OPENAQ: direkte Mess-Werte einzelner Sensor-Stationen, jung (~1-2h Delay)

API: https://api.openaq.org/v3/
- /v3/locations?coordinates=lat,lon&radius=50000&parameters_id=2,5
- /v3/locations/{id}/measurements?parameters_id=5&limit=100
- Parameter-IDs: PM2.5=2, PM10=1, NO2=5, O3=3, SO2=4, CO=8

Wichtig: OpenAQ v3 verlangt API-Key (Free Tier 60 req/min nach Registrierung).
Wenn `OPENAQ_API_KEY` env-var fehlt → graceful skip mit Log-Warnung.

Trigger: claim hat Luftqualitäts-Keyword (PM2.5, NO2, Feinstaub, Smog,
Luftverschmutzung, …) UND erkennbare Stadt aus CITY_COORDS-Whitelist
(über Regex am Claim-Text ODER über analysis.entities).

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle, keine kuratierte Konsens-DB).

OpenAQ-Limitations:
- API-Key ist seit v3 zwingend (Schmerzpunkt 2024 von OpenAQ neu eingeführt)
- Sensor-Coverage variiert: AT/DE/CH gut, Süd-/Ost-Europa lückenhaft
- Sensoren ungleichmäßig kalibriert (billige IoT neben EPA-Stationen)
- Realtime-Daten haben ~1-2h Delay (nicht für Live-Sub-Hour-Vergleiche)
- WHO-Limits (PM2.5: 5 µg/m³ Jahres-Mittel) sind STRENGER als EU-Limits
  (25 µg/m³ Jahres-Mittel) — Synthesizer muss beim Display beide nennen.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Any

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

OPENAQ_BASE = "https://api.openaq.org/v3"
OPENAQ_LOCATIONS = OPENAQ_BASE + "/locations"
OPENAQ_LOCATION_LATEST = OPENAQ_BASE + "/locations/{location_id}/latest"

# Parameter-IDs (OpenAQ v3 Schema, Stand 2026)
# https://docs.openaq.org/about/parameters
PARAM_PM25 = 2
PARAM_PM10 = 1
PARAM_NO2 = 5
PARAM_O3 = 3
PARAM_SO2 = 4
PARAM_CO = 8

# Default-Set für Luftqualitäts-Look-Up: PM2.5 + NO2 (Haupt-Indikatoren EU)
DEFAULT_PARAMS = [PARAM_PM25, PARAM_NO2]

# Reverse-Map für Display-Format
PARAM_NAMES = {
    PARAM_PM25: "PM2.5",
    PARAM_PM10: "PM10",
    PARAM_NO2: "NO2",
    PARAM_O3: "O3",
    PARAM_SO2: "SO2",
    PARAM_CO: "CO",
}

# WHO + EU Jahres-Mittel-Grenzwerte in µg/m³ (zur Einordnung im Display).
# Quellen: WHO Air Quality Guidelines 2021 + EU-RL 2008/50/EG.
LIMITS_INFO = {
    PARAM_PM25: {"who": 5, "eu": 25, "unit": "µg/m³"},
    PARAM_PM10: {"who": 15, "eu": 40, "unit": "µg/m³"},
    PARAM_NO2: {"who": 10, "eu": 40, "unit": "µg/m³"},
    PARAM_O3: {"who": 100, "eu": 120, "unit": "µg/m³"},  # 8h-Spitze
    PARAM_SO2: {"who": 40, "eu": 125, "unit": "µg/m³"},
    PARAM_CO: {"who": 4, "eu": 10, "unit": "mg/m³"},
}

# Stadt-Koordinaten-Whitelist für DACH + ausgewählte EU-Vergleichs-Städte.
# (lat, lon, country_code) — country_code für Display-Wahrheit.
CITY_COORDS: dict[str, tuple[float, float, str]] = {
    # AT
    "wien": (48.2082, 16.3738, "AT"),
    "vienna": (48.2082, 16.3738, "AT"),
    "graz": (47.0707, 15.4395, "AT"),
    "linz": (48.3069, 14.2858, "AT"),
    "salzburg": (47.8095, 13.0550, "AT"),
    "innsbruck": (47.2692, 11.4041, "AT"),
    "klagenfurt": (46.6247, 14.3050, "AT"),
    # DE
    "berlin": (52.5200, 13.4050, "DE"),
    "münchen": (48.1351, 11.5820, "DE"),
    "munchen": (48.1351, 11.5820, "DE"),
    "munich": (48.1351, 11.5820, "DE"),
    "hamburg": (53.5511, 9.9937, "DE"),
    "köln": (50.9375, 6.9603, "DE"),
    "koeln": (50.9375, 6.9603, "DE"),
    "cologne": (50.9375, 6.9603, "DE"),
    "frankfurt": (50.1109, 8.6821, "DE"),
    "stuttgart": (48.7758, 9.1829, "DE"),
    "düsseldorf": (51.2277, 6.7735, "DE"),
    "duesseldorf": (51.2277, 6.7735, "DE"),
    "dusseldorf": (51.2277, 6.7735, "DE"),
    "leipzig": (51.3397, 12.3731, "DE"),
    "dresden": (51.0504, 13.7373, "DE"),
    # CH
    "zürich": (47.3769, 8.5417, "CH"),
    "zurich": (47.3769, 8.5417, "CH"),
    "bern": (46.9480, 7.4474, "CH"),
    "basel": (47.5596, 7.5886, "CH"),
    "genf": (46.2044, 6.1432, "CH"),
    "geneva": (46.2044, 6.1432, "CH"),
    # Weitere EU-Vergleichs-Städte
    "london": (51.5074, -0.1278, "GB"),
    "paris": (48.8566, 2.3522, "FR"),
    "madrid": (40.4168, -3.7038, "ES"),
    "barcelona": (41.3851, 2.1734, "ES"),
    "rom": (41.9028, 12.4964, "IT"),
    "rome": (41.9028, 12.4964, "IT"),
    "mailand": (45.4642, 9.1900, "IT"),
    "milan": (45.4642, 9.1900, "IT"),
    "milano": (45.4642, 9.1900, "IT"),
    "warschau": (52.2297, 21.0122, "PL"),
    "warsaw": (52.2297, 21.0122, "PL"),
    "prag": (50.0755, 14.4378, "CZ"),
    "prague": (50.0755, 14.4378, "CZ"),
    "budapest": (47.4979, 19.0402, "HU"),
}

# Regex für Luftqualitäts-Keywords (DE + EN)
AIR_QUALITY_KEYWORDS = [
    "pm2.5", "pm 2.5", "pm25", "pm10", "pm 10",
    "no2", "nox", "stickoxid", "stickstoffdioxid", "stickstoff-dioxid",
    "ozon", "o3", "ozone",
    "so2", "schwefel", "sulfur",
    "feinstaub", "fine particulate", "particulate matter",
    "luftqualität", "luftguete", "luftgüte", "air quality",
    "smog", "schadstoff", "pollutant",
    "luftverschmutzung", "air pollution", "air pollutants",
    "abgase", "exhaust",
    "co2",  # CO2 wird NICHT von OpenAQ gemessen, aber Erwähnung triggert
]

# Vor-kompiliertes Regex (Wort-Grenzen-tolerant für Sub-Strings wie "PM2.5")
_AIRQ_RE = re.compile(
    r"(?i)(" + "|".join(re.escape(k) for k in AIR_QUALITY_KEYWORDS) + r")"
)

# Stadt-Detector: einfaches Wort-Boundary-Matching (case-insensitive)
_CITY_RE = re.compile(
    r"(?i)\b("
    + "|".join(re.escape(c) for c in CITY_COORDS.keys())
    + r")\b"
)


def claim_mentions_air_quality(claim: str) -> bool:
    """Prüfe, ob der Claim ein Luftqualitäts-Keyword enthält."""
    if not claim:
        return False
    return bool(_AIRQ_RE.search(claim))


def _detect_cities_in_claim(
    claim: str, entities: list[str]
) -> list[tuple[str, float, float, str]]:
    """Identifiziere bis zu 3 Städte aus Claim-Text + Entities.

    Returns Liste mit (city_label_lower, lat, lon, country_code).
    Doppel-Matches (z. B. „München" + „Munich") werden über lat/lon dedupliziert.
    """
    found: list[tuple[str, float, float, str]] = []
    seen_coords: set[tuple[float, float]] = set()

    candidates: list[str] = []
    if claim:
        candidates.extend(m.group(1) for m in _CITY_RE.finditer(claim))
    for ent in entities or []:
        if not ent:
            continue
        # Entity könnte mehrere Wörter enthalten — match alle Stadt-Hits
        candidates.extend(m.group(1) for m in _CITY_RE.finditer(ent))

    for cand in candidates:
        key = cand.lower()
        if key not in CITY_COORDS:
            continue
        lat, lon, cc = CITY_COORDS[key]
        coord_key = (round(lat, 3), round(lon, 3))
        if coord_key in seen_coords:
            continue
        seen_coords.add(coord_key)
        found.append((key, lat, lon, cc))
        if len(found) >= 3:
            break
    return found


async def _fetch_locations_near(
    client, api_key: str, lat: float, lon: float,
    radius_m: int = 25000, params: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Hole OpenAQ-Locations im Umkreis von (lat, lon).

    Returns Liste von Location-Dicts (max 5, sortiert nach Nähe).
    Bei API-Fehler/Empty-Response: leere Liste.
    """
    params = params or DEFAULT_PARAMS
    query_params = {
        "coordinates": f"{lat},{lon}",
        "radius": str(radius_m),
        "parameters_id": ",".join(str(p) for p in params),
        "limit": "5",
        "order_by": "distance",
        "sort_order": "asc",
    }
    headers = {"X-API-Key": api_key}
    try:
        resp = await client.get(
            OPENAQ_LOCATIONS, params=query_params, headers=headers,
        )
        if resp.status_code != 200:
            logger.debug(
                f"OpenAQ locations near ({lat:.3f},{lon:.3f}) "
                f"returned {resp.status_code}"
            )
            return []
        data = resp.json()
        results = data.get("results", []) or []
        return results[:5]
    except Exception as e:
        logger.debug(
            f"OpenAQ locations fetch failed near ({lat:.3f},{lon:.3f}): {e}"
        )
        return []


async def _fetch_latest_for_location(
    client, api_key: str, location_id: int,
) -> list[dict[str, Any]]:
    """Hole jüngste Mess-Werte für eine Location (alle Sensoren)."""
    headers = {"X-API-Key": api_key}
    url = OPENAQ_LOCATION_LATEST.format(location_id=location_id)
    try:
        resp = await client.get(url, headers=headers, params={"limit": "20"})
        if resp.status_code != 200:
            logger.debug(
                f"OpenAQ latest for loc={location_id} "
                f"returned {resp.status_code}"
            )
            return []
        data = resp.json()
        return data.get("results", []) or []
    except Exception as e:
        logger.debug(f"OpenAQ latest fetch failed loc={location_id}: {e}")
        return []


def _format_limit_hint(parameter_id: int) -> str:
    """Erzeuge kompakten WHO/EU-Limit-Hinweis für ein Schadstoff-Parameter."""
    info = LIMITS_INFO.get(parameter_id)
    if not info:
        return ""
    return (
        f"WHO-Limit {info['who']} {info['unit']}, "
        f"EU-Limit {info['eu']} {info['unit']} (Jahresschnitt)"
    )


def _build_display_value(
    city_label: str, country: str, sensors_summary: list[dict[str, Any]],
    measure_date: str,
) -> str:
    """Baue display_value-String mit Schadstoff-Werten + Limit-Einordnung."""
    if not sensors_summary:
        return f"{city_label.title()} ({country}): keine aktuellen OpenAQ-Mess-Werte verfügbar"

    parts = []
    for s in sensors_summary[:3]:
        param_id = s.get("parameter_id")
        param_name = PARAM_NAMES.get(param_id, str(param_id or "?"))
        value = s.get("value")
        unit = s.get("unit", "µg/m³")
        if value is None:
            continue
        limit_hint = _format_limit_hint(param_id) if param_id else ""
        sensor_name = s.get("sensor_name", "")
        bracket = f" '{sensor_name}'" if sensor_name else ""
        if limit_hint:
            parts.append(
                f"{param_name}{bracket} {value} {unit} ({limit_hint})"
            )
        else:
            parts.append(f"{param_name}{bracket} {value} {unit}")

    if not parts:
        return f"{city_label.title()} ({country}): keine aktuellen OpenAQ-Mess-Werte verfügbar"

    head = f"{city_label.title()} ({country})"
    if measure_date:
        head += f", gemessen {measure_date}"
    return head + " — " + "; ".join(parts)


def _summarize_latest(
    latest: list[dict[str, Any]], location: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    """Aggregiere bis zu 3 Schadstoff-Werte aus Latest-Response.

    Returns (sensors_summary_list, latest_iso_date).
    Pro Parameter wird ein Mess-Wert genommen (der erste Treffer).
    """
    # Sensor-ID → Parameter-Info aus Location-Sensoren-Liste
    sensor_param_map: dict[int, tuple[int, str, str]] = {}
    for sensor in (location.get("sensors") or []):
        sid = sensor.get("id")
        param = sensor.get("parameter") or {}
        if sid is None or not param:
            continue
        sensor_param_map[sid] = (
            param.get("id", 0),
            param.get("name", ""),
            param.get("units", "µg/m³"),
        )

    seen_params: set[int] = set()
    summary: list[dict[str, Any]] = []
    latest_date = ""

    for entry in latest:
        sid = entry.get("sensorsId")
        value = entry.get("value")
        if sid is None or value is None:
            continue
        param_info = sensor_param_map.get(sid)
        if not param_info:
            continue
        param_id, _param_name, unit = param_info
        if param_id in seen_params:
            continue
        # Nur PM2.5 + NO2 + PM10 + O3 zur Anzeige (Hauptindikatoren)
        if param_id not in (PARAM_PM25, PARAM_PM10, PARAM_NO2, PARAM_O3):
            continue
        seen_params.add(param_id)

        # Sensor-Name aus location.sensors finden
        sensor_name = ""
        for sensor in (location.get("sensors") or []):
            if sensor.get("id") == sid:
                sensor_name = sensor.get("name", "") or ""
                # Sensor-Name endet oft auf "/parameter" — kürzen
                if " " in sensor_name:
                    sensor_name = sensor_name.split(" ")[0]
                break

        # datetime extrahieren (utc.datetime ist häufiges OpenAQ-Format)
        dt_obj = (entry.get("datetime") or {}).get("utc")
        if dt_obj and not latest_date:
            try:
                # ISO-8601 → YYYY-MM-DD
                latest_date = dt_obj.split("T")[0]
            except Exception:
                latest_date = ""

        summary.append({
            "parameter_id": param_id,
            "value": round(float(value), 1) if isinstance(value, (int, float)) else value,
            "unit": unit,
            "sensor_name": sensor_name,
        })
        if len(summary) >= 3:
            break

    return summary, latest_date


def _build_indicator_name(
    city_label: str, country: str, sensors_summary: list[dict[str, Any]],
    measure_date: str,
) -> str:
    """Erzeuge knackigen indicator_name für Reranker."""
    bits = []
    for s in sensors_summary[:2]:
        pid = s.get("parameter_id")
        pname = PARAM_NAMES.get(pid, str(pid or "?"))
        v = s.get("value")
        u = s.get("unit", "")
        if v is None:
            continue
        bits.append(f"{pname} {v} {u}")
    metric_str = " + ".join(bits) if bits else "Sensor-Daten"
    date_part = f" ({measure_date})" if measure_date else ""
    return (
        f"{city_label.title()} Luftqualität: {metric_str}{date_part}"
    )[:200]


async def _process_city(
    client, api_key: str,
    city_label: str, lat: float, lon: float, country: str,
) -> list[dict[str, Any]]:
    """Hole OpenAQ-Mess-Werte für eine Stadt + formatiere als result-Dicts.

    Returns Liste von max 1 result-Dict pro Stadt (top-Sensor),
    leer bei keinen Treffern.
    """
    # 25 km Radius reicht in der Regel für Stadt-Sensoren, aber Wien
    # + andere AT-Städte haben schwächere Sensor-Dichte → 50 km
    # Fallback wenn 25-km leer.
    locations = await _fetch_locations_near(
        client, api_key, lat, lon, radius_m=25000, params=DEFAULT_PARAMS,
    )
    if not locations:
        logger.debug(
            f"OpenAQ: 0 locations für {city_label} bei 25 km — versuche 50 km"
        )
        locations = await _fetch_locations_near(
            client, api_key, lat, lon, radius_m=50000, params=DEFAULT_PARAMS,
        )
    if not locations:
        logger.debug(f"OpenAQ: 0 locations für {city_label} auch bei 50 km")
        return []

    # Wähle erste Location mit ≥1 PM2.5-/NO2-Sensor
    chosen = locations[0]
    chosen_id = chosen.get("id")
    if not chosen_id:
        return []

    latest = await _fetch_latest_for_location(client, api_key, chosen_id)
    if not latest:
        return []

    summary, measure_date = _summarize_latest(latest, chosen)
    if not summary:
        return []

    explore_url = f"https://explore.openaq.org/locations/{chosen_id}"
    indicator_name = _build_indicator_name(
        city_label, country, summary, measure_date,
    )
    display_value = _build_display_value(
        city_label, country, summary, measure_date,
    )
    year = measure_date.split("-")[0] if measure_date else str(datetime.utcnow().year)

    return [{
        "indicator_name": indicator_name,
        "indicator": "openaq_measurement",
        "country": country,
        "year": year,
        "topic": "openaq_air_quality",
        "display_value": display_value[:500],
        "description": (
            "OpenAQ-Sensor-Daten — direkte Messwerte staatlicher + "
            "ziviler Stationen. Skala µg/m³. WHO-Limits sind strenger "
            "als EU-Limits (PM2.5: WHO 5 vs. EU 25 µg/m³)."
        ),
        "url": explore_url,
        "secondary_url": "",
        "source": "OpenAQ v3 (CC-BY 4.0)",
    }]


async def search_openaq(analysis: dict) -> dict:
    """Live-Lookup gegen OpenAQ v3 für Luftqualitäts-Sensor-Daten.

    Trigger-Bedingungen:
    - claim hat ≥1 Luftqualitäts-Keyword (PM2.5, NO2, Feinstaub, …)
    - claim ODER analysis.entities enthält ≥1 Stadt aus CITY_COORDS

    Wenn `OPENAQ_API_KEY` env-var fehlt → graceful skip mit Log-Warnung
    und leere Result-Liste (kein Crash der Pipeline).
    """
    empty = {"source": "OpenAQ", "type": "live_air_quality", "results": []}

    claim = (analysis or {}).get("claim", "") or ""
    entities = (analysis or {}).get("entities", []) or []

    if not claim_mentions_air_quality(claim):
        return empty

    cities = _detect_cities_in_claim(claim, entities)
    if not cities:
        logger.debug(
            f"OpenAQ: Luftqualitäts-Keyword erkannt, aber keine bekannte "
            f"Stadt im Claim — skip"
        )
        return empty

    api_key = os.getenv("OPENAQ_API_KEY", "").strip()
    if not api_key:
        logger.warning(
            "OpenAQ: OPENAQ_API_KEY env-var nicht gesetzt — "
            "Luftqualitäts-Connector wird übersprungen "
            "(siehe https://docs.openaq.org/ für kostenlose Registrierung)."
        )
        return empty

    async with polite_client(timeout=15.0) as client:
        tasks = [
            _process_city(client, api_key, label, lat, lon, cc)
            for (label, lat, lon, cc) in cities
        ]
        per_city = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict[str, Any]] = []
    for chunk in per_city:
        if isinstance(chunk, Exception) or not chunk:
            continue
        results.extend(chunk)

    if not results:
        logger.info(
            f"OpenAQ: 0 Treffer für Städte "
            f"{[c[0] for c in cities]}"
        )
        return empty

    logger.info(
        f"OpenAQ: {len(results)} Treffer für Städte "
        f"{[c[0] for c in cities]}"
    )
    return {
        "source": "OpenAQ",
        "type": "live_air_quality",
        "results": results,
    }
