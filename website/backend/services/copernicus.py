import httpx
import logging

logger = logging.getLogger("evidora")

CDS_CATALOGUE_URL = "https://cds.climate.copernicus.eu/api/catalogue/v1/collections"

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


async def search_copernicus(analysis: dict) -> dict:
    """Search Copernicus Climate Data Store for relevant datasets."""
    datasets = _find_datasets(analysis)

    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for ds in datasets[:3]:
            try:
                resp = await client.get(f"{CDS_CATALOGUE_URL}/{ds['id']}")
                resp.raise_for_status()
                metadata = resp.json()

                title = metadata.get("title", ds["label"])
                description = metadata.get("description", "")
                # Extract first 200 chars of description for context
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
                # Still include the dataset as a reference even if metadata fetch fails
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
        # Fallback: general ERA5 reference
        results.append({
            "title": "ERA5 Globale Klimadaten (Reanalyse)",
            "dataset_id": "reanalysis-era5-single-levels-monthly-means",
            "variable": "multiple",
            "description": "ERA5 ist der umfassendste globale Klimadatensatz, produziert vom ECMWF. Er deckt Atmosphäre, Ozean und Land seit 1940 ab.",
            "time_range": "1940–heute",
            "source": "Copernicus Climate Data Store (ECMWF/EU)",
            "url": "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means",
        })

    return {
        "source": "Copernicus Climate Data Store",
        "type": "official_data",
        "results": results,
        "attribution": "Contains modified Copernicus Climate Change Service information (2024). Neither the European Commission nor ECMWF is responsible for any use of this information.",
    }
