import httpx
import logging
from urllib.parse import quote

logger = logging.getLogger("evidora")

BASE_URL = "https://discodata.eea.europa.eu/sql"

# Map country names to ISO codes used by EEA
COUNTRY_CODES = {
    "österreich": "AT", "austria": "AT",
    "deutschland": "DE", "germany": "DE",
    "frankreich": "FR", "france": "FR",
    "italien": "IT", "italy": "IT",
    "spanien": "ES", "spain": "ES",
    "niederlande": "NL", "netherlands": "NL",
    "belgien": "BE", "belgium": "BE",
    "polen": "PL", "poland": "PL",
    "schweden": "SE", "sweden": "SE",
    "dänemark": "DK", "denmark": "DK",
    "finnland": "FI", "finland": "FI",
    "irland": "IE", "ireland": "IE",
    "portugal": "PT",
    "griechenland": "GR", "greece": "GR",
    "tschechien": "CZ", "czechia": "CZ",
    "rumänien": "RO", "romania": "RO",
    "ungarn": "HU", "hungary": "HU",
    "kroatien": "HR", "croatia": "HR",
    "bulgarien": "BG", "bulgaria": "BG",
    "slowakei": "SK", "slovakia": "SK",
    "slowenien": "SI", "slovenia": "SI",
    "luxemburg": "LU", "luxembourg": "LU",
    "estland": "EE", "estonia": "EE",
    "lettland": "LV", "latvia": "LV",
    "litauen": "LT", "lithuania": "LT",
    "malta": "MT", "zypern": "CY", "cyprus": "CY",
    "norwegen": "NO", "norway": "NO",
    "schweiz": "CH", "switzerland": "CH",
}

# Map keywords to SQL queries + metadata
# Each entry: keyword -> {query_template, label, format_fn, eea_url}
# {country} placeholder is replaced with the detected country code
QUERY_MAP = {
    # CO2-Emissionen Autos
    "co2_cars": {
        "keywords": ["auto", "car", "pkw", "fahrzeug", "vehicle", "kfz"],
        "label": "CO₂-Emissionen von PKW",
        "label_en": "CO₂ Emissions from Cars",
        "query": """
            SELECT TOP 10
                Year AS year,
                AVG(CAST(Ewltp AS FLOAT)) AS avg_co2_gkm,
                COUNT(*) AS vehicle_count
            FROM [CO2Emission].[latest].[co2cars]
            WHERE Ewltp IS NOT NULL AND Ewltp > 0
            {country_filter}
            GROUP BY Year
            ORDER BY Year DESC
        """,
        "country_field": "Ms",
        "format": lambda r: {
            "indicator": "Durchschnittliche CO₂-Emissionen (WLTP)",
            "year": str(r.get("year", "")),
            "value": f"{r.get('avg_co2_gkm', 0):.1f} g/km ({r.get('vehicle_count', 0):,} Fahrzeuge)",
            "source": "EEA / CO2 Emission Database",
            "url": "https://www.eea.europa.eu/en/datahub/datahubitem-view/fa8b1229-3db6-495d-b18e-9c9b3267c02b",
        },
    },
    # CO2 / Treibhausgase allgemein
    "ghg": {
        "keywords": ["treibhausgas", "greenhouse", "emission", "co2", "kohlendioxid", "carbon"],
        "label": "Treibhausgasemissionen",
        "label_en": "Greenhouse Gas Emissions",
        "query": """
            SELECT TOP 10
                Year AS year,
                CountryName AS country,
                Gas AS gas,
                SUM(CAST(Value AS FLOAT)) AS total_emissions
            FROM [Greenhouse_Gas_Emissions].[latest].[v_Total_GHG_Emissions]
            WHERE Gas = 'Total GHG'
            {country_filter}
            GROUP BY Year, CountryName, Gas
            ORDER BY Year DESC
        """,
        "country_field": "CountryCode",
        "format": lambda r: {
            "indicator": "Treibhausgasemissionen (gesamt)",
            "country": r.get("country", ""),
            "year": str(r.get("year", "")),
            "value": f"{r.get('total_emissions', 0):,.0f} Mt CO₂-Äquivalent",
            "source": "EEA / Greenhouse Gas Emissions",
            "url": "https://www.eea.europa.eu/en/datahub/datahubitem-view/d20a183e-b642-4edc-8a9c-30528c0b8e9c",
        },
    },
    # Luftqualität
    "air": {
        "keywords": ["luft", "air", "feinstaub", "pm10", "pm2.5", "stickstoff", "no2",
                      "ozon", "ozone", "luftverschmutzung", "air_pollution", "smog"],
        "label": "Luftqualitätsdaten",
        "label_en": "Air Quality Data",
        "query": """
            SELECT TOP 10
                ReportingYear AS year,
                CountryOrTerritory AS country,
                Pollutant AS pollutant,
                AggType AS aggregation,
                AVG(CAST(Value_numeric AS FLOAT)) AS avg_value,
                Unit AS unit
            FROM [AirQualityStatistics].[latest].[c_statistics]
            WHERE Pollutant IN ('PM10', 'PM2.5', 'NO2', 'O3')
            {country_filter}
            GROUP BY ReportingYear, CountryOrTerritory, Pollutant, AggType, Unit
            ORDER BY ReportingYear DESC
        """,
        "country_field": "CountryOrTerritory",
        "format": lambda r: {
            "indicator": f"Luftqualität — {r.get('pollutant', '')} ({r.get('aggregation', '')})",
            "country": r.get("country", ""),
            "year": str(r.get("year", "")),
            "value": f"{r.get('avg_value', 0):.1f} {r.get('unit', 'µg/m³')}",
            "source": "EEA / Air Quality Statistics",
            "url": "https://www.eea.europa.eu/en/datahub/datahubitem-view/0e1d28fc-bef0-4498-891a-81570c8e4bc5",
        },
    },
    # Biodiversität / Arten
    "biodiversity": {
        "keywords": ["arten", "species", "biodiversität", "biodiversity", "naturschutz",
                      "tierschutz", "fauna", "flora", "artensterben", "extinction"],
        "label": "Biodiversität & Artenschutz",
        "label_en": "Biodiversity & Species Protection",
        "query": """
            SELECT TOP 10
                species_group_name AS species_group,
                COUNT(*) AS species_count
            FROM [EUNIS].[v1].[Site_Species]
            WHERE code_site LIKE '{country_prefix}%'
            GROUP BY species_group_name
            ORDER BY species_count DESC
        """,
        "country_field": None,
        "format": lambda r: {
            "indicator": f"Geschützte Arten — {r.get('species_group', '')}",
            "value": f"{r.get('species_count', 0):,} Arten in Schutzgebieten",
            "source": "EEA / EUNIS",
            "url": "https://eunis.eea.europa.eu/",
        },
    },
}


def _find_country(analysis: dict) -> str | None:
    """Extract country code from entities."""
    entities = analysis.get("entities", [])
    for entity in entities:
        for name, code in COUNTRY_CODES.items():
            if name in entity.lower():
                return code
    return None


def _find_matching_queries(analysis: dict) -> list[dict]:
    """Find matching EEA query sets based on claim analysis."""
    entities = analysis.get("entities", [])
    subcategory = analysis.get("subcategory", "")
    category = analysis.get("category", "")
    search_terms = [t.lower() for t in entities + [subcategory, category]]
    search_text = " ".join(search_terms)

    matched = {}
    for key, query_set in QUERY_MAP.items():
        for kw in query_set["keywords"]:
            if kw in search_text:
                if key not in matched:
                    matched[key] = query_set
                break
    return list(matched.values())


async def search_eea(analysis: dict) -> dict:
    """Search EEA Discodata for relevant environmental data."""
    query_sets = _find_matching_queries(analysis)
    country = _find_country(analysis)

    # Default to GHG if no specific match but climate-related
    if not query_sets:
        query_sets = [QUERY_MAP["ghg"]]

    all_results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for qs in query_sets[:2]:  # Max 2 queries to keep it fast
            try:
                sql = qs["query"]

                # Apply country filter
                country_field = qs.get("country_field")
                if country and country_field:
                    sql = sql.replace("{country_filter}", f"AND {country_field} = '{country}'")
                else:
                    sql = sql.replace("{country_filter}", "")

                # Handle EUNIS special case (uses code_site prefix)
                if "{country_prefix}" in sql:
                    sql = sql.replace("{country_prefix}", country or "AT")

                # Clean up whitespace
                sql = " ".join(sql.split())

                resp = await client.get(
                    BASE_URL,
                    params={"query": sql, "p": 1, "nrOfHits": 10},
                )
                resp.raise_for_status()
                data = resp.json()

                records = data.get("results", [])
                format_fn = qs["format"]

                for record in records[:5]:
                    try:
                        formatted = format_fn(record)
                        all_results.append(formatted)
                    except Exception as e:
                        logger.warning(f"EEA format error: {e}")

            except Exception as e:
                logger.warning(f"EEA query failed for {qs['label']}: {e}")
                # Add reference link even on failure
                all_results.append({
                    "indicator": qs["label"],
                    "value": "Daten nicht verfügbar",
                    "source": "EEA",
                    "url": "https://www.eea.europa.eu/en/datahub",
                })

    return {
        "source": "European Environment Agency (EEA)",
        "type": "official_data",
        "results": all_results,
    }
