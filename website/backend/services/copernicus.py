import csv
import io
import time

import httpx
import logging

logger = logging.getLogger("evidora")

CDS_CATALOGUE_URL = "https://cds.climate.copernicus.eu/api/catalogue/v1/collections"
NASA_GISS_URL = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"

# Pre-industrial baseline offset: NASA GISS uses 1951-1980 baseline,
# pre-industrial level is ~0.3°C below that baseline
PREINDUSTRIAL_OFFSET = 0.3

# In-memory cache for NASA GISS data
_giss_cache: list[dict] | None = None
_giss_cache_ts: float = 0
GISS_CACHE_TTL = 604800  # 7 days

# Map climate keywords to relevant CDS dataset IDs + readable descriptions
CLIMATE_DATASET_MAP = {
    # Temperatur
    "temperatur": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "2m_temperature",
        "label": "ERA5 Monatliche Durchschnittstemperatur",
    },
    "temperature": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "2m_temperature",
        "label": "ERA5 Monthly Mean Temperature",
    },
    "hitze": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "2m_temperature",
        "label": "ERA5 Monatliche Durchschnittstemperatur",
    },
    "erwärmung": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "2m_temperature",
        "label": "ERA5 Monatliche Durchschnittstemperatur",
    },
    "warming": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "2m_temperature",
        "label": "ERA5 Monthly Mean Temperature",
    },
    # Niederschlag
    "niederschlag": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "total_precipitation",
        "label": "ERA5 Monatlicher Niederschlag",
    },
    "precipitation": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "total_precipitation",
        "label": "ERA5 Monthly Precipitation",
    },
    "regen": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "total_precipitation",
        "label": "ERA5 Monatlicher Niederschlag",
    },
    "flood": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "total_precipitation",
        "label": "ERA5 Monthly Precipitation",
    },
    "hochwasser": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "total_precipitation",
        "label": "ERA5 Monatlicher Niederschlag",
    },
    # Meereis
    "eis": {
        "id": "satellite-sea-ice-concentration",
        "variable": "sea_ice_concentration",
        "label": "Satellitengestützte Meereiskonzentration",
    },
    "ice": {
        "id": "satellite-sea-ice-concentration",
        "variable": "sea_ice_concentration",
        "label": "Satellite Sea Ice Concentration",
    },
    "arktis": {
        "id": "satellite-sea-ice-concentration",
        "variable": "sea_ice_concentration",
        "label": "Satellitengestützte Meereiskonzentration",
    },
    "arctic": {
        "id": "satellite-sea-ice-concentration",
        "variable": "sea_ice_concentration",
        "label": "Satellite Sea Ice Concentration",
    },
    "antarktis": {
        "id": "satellite-sea-ice-concentration",
        "variable": "sea_ice_concentration",
        "label": "Satellitengestützte Meereiskonzentration",
    },
    # Meeresspiegel
    "meeresspiegel": {
        "id": "satellite-sea-level-global",
        "variable": "sea_level",
        "label": "Satellitengestützter globaler Meeresspiegel",
    },
    "sea_level": {
        "id": "satellite-sea-level-global",
        "variable": "sea_level",
        "label": "Satellite Global Sea Level",
    },
    # Treibhausgase / CO2
    "co2": {
        "id": "cams-global-greenhouse-gas-reanalysis",
        "variable": "carbon_dioxide",
        "label": "CAMS Globale Treibhausgas-Reanalyse (CO₂)",
    },
    "treibhausgas": {
        "id": "cams-global-greenhouse-gas-reanalysis",
        "variable": "carbon_dioxide",
        "label": "CAMS Globale Treibhausgas-Reanalyse",
    },
    "greenhouse": {
        "id": "cams-global-greenhouse-gas-reanalysis",
        "variable": "carbon_dioxide",
        "label": "CAMS Global Greenhouse Gas Reanalysis",
    },
    "emission": {
        "id": "cams-global-greenhouse-gas-reanalysis",
        "variable": "carbon_dioxide",
        "label": "CAMS Globale Treibhausgas-Reanalyse",
    },
    "methan": {
        "id": "cams-global-greenhouse-gas-reanalysis",
        "variable": "methane",
        "label": "CAMS Globale Treibhausgas-Reanalyse (Methan)",
    },
    "methane": {
        "id": "cams-global-greenhouse-gas-reanalysis",
        "variable": "methane",
        "label": "CAMS Global Greenhouse Gas Reanalysis (Methane)",
    },
    # Dürre / Bodenfeuchte
    "dürre": {
        "id": "reanalysis-era5-land-monthly-means",
        "variable": "soil_moisture",
        "label": "ERA5-Land Bodenfeuchte",
    },
    "drought": {
        "id": "reanalysis-era5-land-monthly-means",
        "variable": "soil_moisture",
        "label": "ERA5-Land Soil Moisture",
    },
    # Wind / Stürme
    "wind": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "10m_wind_speed",
        "label": "ERA5 Monatliche Windgeschwindigkeit",
    },
    "sturm": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "10m_wind_speed",
        "label": "ERA5 Monatliche Windgeschwindigkeit",
    },
    "storm": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "10m_wind_speed",
        "label": "ERA5 Monthly Wind Speed",
    },
    # Klima allgemein
    "klima": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "2m_temperature",
        "label": "ERA5 Globale Klimadaten",
    },
    "climate": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "2m_temperature",
        "label": "ERA5 Global Climate Data",
    },
    "klimawandel": {
        "id": "reanalysis-era5-single-levels-monthly-means",
        "variable": "2m_temperature",
        "label": "ERA5 Globale Klimadaten",
    },
}


def _find_datasets(analysis: dict) -> list[dict]:
    """Find matching CDS datasets based on claim analysis."""
    entities = analysis.get("entities", [])
    subcategory = analysis.get("subcategory", "")
    search_terms = entities + [subcategory]

    matched = {}
    for term in search_terms:
        for keyword, dataset in CLIMATE_DATASET_MAP.items():
            if keyword in term.lower():
                # Deduplicate by dataset ID + variable
                key = f"{dataset['id']}_{dataset['variable']}"
                if key not in matched:
                    matched[key] = dataset
    return list(matched.values())


async def _fetch_nasa_giss(client: httpx.AsyncClient) -> list[dict]:
    """Fetch global temperature anomaly data from NASA GISS (cached for 7 days)."""
    global _giss_cache, _giss_cache_ts

    now = time.time()
    if _giss_cache is not None and now - _giss_cache_ts < GISS_CACHE_TTL:
        return _giss_cache

    results = []
    try:
        resp = await client.get(NASA_GISS_URL)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")

        reader = csv.reader(lines[1:])  # skip description line
        header = next(reader)  # column names

        rows = list(reader)
        # Last 10 years of data
        recent = [r for r in rows[-12:] if len(r) > 13 and r[13] not in ("***", "")]

        # Find all-time max
        max_anomaly = -999
        max_year = ""
        for row in rows:
            try:
                val = float(row[13])  # J-D = annual mean
                if val > max_anomaly:
                    max_anomaly = val
                    max_year = row[0]
            except (ValueError, IndexError):
                pass

        for row in recent:
            try:
                year = row[0]
                anomaly = float(row[13])  # J-D column = annual mean
                vs_preindustrial = anomaly + PREINDUSTRIAL_OFFSET

                title = (
                    f"Globale Durchschnittstemperatur {year}: "
                    f"{anomaly:+.2f}°C vs. 1951-1980 Mittel "
                    f"(ca. {vs_preindustrial:+.2f}°C vs. vorindustriell)"
                )

                # Add historical context to most recent entry
                if row == recent[-1] and max_year:
                    title += (
                        f" — Wärmstes Jahr: {max_year} "
                        f"({max_anomaly:+.2f}°C / ca. {max_anomaly + PREINDUSTRIAL_OFFSET:+.2f}°C vs. vorindustriell)"
                    )

                results.append({
                    "title": title,
                    "year": year,
                    "value": f"{vs_preindustrial:+.2f}°C vs. vorindustriell",
                    "source": "NASA GISS / GISTEMP v4",
                    "url": "https://data.giss.nasa.gov/gistemp/",
                })
            except (ValueError, IndexError):
                continue

        if results:
            _giss_cache = results
            _giss_cache_ts = now
            logger.info(f"NASA GISS: {len(results)} years of temperature data loaded")

    except Exception as e:
        logger.warning(f"NASA GISS fetch failed: {e}")
        if _giss_cache is not None:
            return _giss_cache

    return results


# Keywords that indicate temperature-related claims
TEMPERATURE_KEYWORDS = [
    "temperatur", "temperature", "erwärmung", "warming", "hitze", "heat",
    "grad", "degree", "celsius", "klima", "climate", "klimawandel",
    "climate change", "global warming", "erderwärmung",
]


async def search_copernicus(analysis: dict) -> dict:
    """Search Copernicus Climate Data Store + NASA GISS for climate data."""
    datasets = _find_datasets(analysis)

    # Check if claim is temperature-related
    entities = analysis.get("entities", [])
    claim = analysis.get("original_claim", "")
    search_text = " ".join([t.lower() for t in entities + [claim]])
    is_temperature = any(kw in search_text for kw in TEMPERATURE_KEYWORDS)

    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Fetch real temperature data from NASA GISS if claim is temperature-related
        if is_temperature:
            giss_data = await _fetch_nasa_giss(client)
            results.extend(giss_data)

        # Also fetch Copernicus dataset metadata as reference
        for ds in datasets[:2]:
            try:
                resp = await client.get(f"{CDS_CATALOGUE_URL}/{ds['id']}")
                resp.raise_for_status()
                metadata = resp.json()

                title = metadata.get("title", ds["label"])
                description = metadata.get("description", "")
                if description:
                    description = description[:200].rsplit(" ", 1)[0] + "…"

                temporal = metadata.get("extent", {}).get("temporal", {})
                interval = temporal.get("interval", [[]])
                time_range = ""
                if interval and len(interval[0]) >= 2:
                    time_range = f"{interval[0][0][:4]}–{interval[0][1][:4] if interval[0][1] else 'heute'}"

                results.append({
                    "title": title,
                    "dataset_id": ds["id"],
                    "variable": ds["variable"],
                    "description": description,
                    "time_range": time_range,
                    "source": "Copernicus Climate Data Store (ECMWF/EU)",
                    "url": f"https://cds.climate.copernicus.eu/datasets/{ds['id']}",
                })

            except Exception as e:
                logger.warning(f"Copernicus catalogue request failed for {ds['id']}: {e}")
                results.append({
                    "title": ds["label"],
                    "dataset_id": ds["id"],
                    "variable": ds["variable"],
                    "description": "",
                    "time_range": "",
                    "source": "Copernicus Climate Data Store (ECMWF/EU)",
                    "url": f"https://cds.climate.copernicus.eu/datasets/{ds['id']}",
                })

    if not results:
        results.append({
            "title": "ERA5 Globale Klimadaten (Reanalyse)",
            "dataset_id": "reanalysis-era5-single-levels-monthly-means",
            "variable": "multiple",
            "description": "ERA5 ist der umfassendste globale Klimadatensatz, produziert vom ECMWF.",
            "time_range": "1940–heute",
            "source": "Copernicus Climate Data Store (ECMWF/EU)",
            "url": "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means",
        })

    return {
        "source": "Copernicus Climate Data Store",
        "type": "official_data",
        "results": results,
        "attribution": "Contains modified Copernicus Climate Change Service information (2024). Neither the European Commission nor ECMWF is responsible for any use of this information. Global temperature data: NASA GISS GISTEMP v4.",
    }
