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

# --- Berkeley Earth (via OWID mirror) ---
# Annual land temperature anomalies (°C vs. 1951-1980 baseline).
# Covers World, continents (as "<Name> (NIAID)" entities), ~240 countries, 1940-2024.
# Licence: CC BY 4.0 (Berkeley Earth + OWID redistribution).
BERKELEY_TEMP_URL = "https://ourworldindata.org/grapher/annual-temperature-anomalies.csv"
BERKELEY_CACHE_TTL = 86400  # 24h — Berkeley updates annually, but cheap to refresh

# cache shape: {key: {"entity": str, "years": {year: anomaly}}}
#   key = ISO3 for countries, "OWID_WRL" for World, "OWID_<REGION>" for continents
_berkeley_cache: dict | None = None
_berkeley_cache_ts: float = 0

# Country name (DE + EN) → ISO3 (mirrors vdem.py COUNTRY_MAP for consistency)
BERKELEY_COUNTRY_MAP: dict[str, str] = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "schweiz": "CHE", "switzerland": "CHE",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czech republic": "CZE", "czechia": "CZE",
    "ungarn": "HUN", "hungary": "HUN",
    "rumänien": "ROU", "romania": "ROU",
    "bulgarien": "BGR", "bulgaria": "BGR",
    "kroatien": "HRV", "croatia": "HRV",
    "slowenien": "SVN", "slovenia": "SVN",
    "slowakei": "SVK", "slovakia": "SVK",
    "dänemark": "DNK", "denmark": "DNK",
    "schweden": "SWE", "sweden": "SWE",
    "norwegen": "NOR", "norway": "NOR",
    "finnland": "FIN", "finland": "FIN",
    "portugal": "PRT",
    "griechenland": "GRC", "greece": "GRC",
    "irland": "IRL", "ireland": "IRL",
    "luxemburg": "LUX", "luxembourg": "LUX",
    "estland": "EST", "estonia": "EST",
    "lettland": "LVA", "latvia": "LVA",
    "litauen": "LTU", "lithuania": "LTU",
    "vereinigtes königreich": "GBR", "united kingdom": "GBR", "großbritannien": "GBR",
    "türkei": "TUR", "turkey": "TUR", "türkiye": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "belarus": "BLR", "weißrussland": "BLR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "südkorea": "KOR", "south korea": "KOR",
    "nordkorea": "PRK", "north korea": "PRK",
    "iran": "IRN",
    "israel": "ISR",
    "ägypten": "EGY", "egypt": "EGY",
    "saudi-arabien": "SAU", "saudi arabia": "SAU",
    "venezuela": "VEN",
    "kuba": "CUB", "cuba": "CUB",
    "australien": "AUS", "australia": "AUS",
    "neuseeland": "NZL", "new zealand": "NZL",
    "kanada": "CAN", "canada": "CAN",
    "mexiko": "MEX", "mexico": "MEX",
    "südafrika": "ZAF", "south africa": "ZAF",
}

# Continent keyword → internal cache key (DE + EN)
BERKELEY_CONTINENT_MAP: dict[str, str] = {
    "europa": "OWID_EUR", "europe": "OWID_EUR", "europäisch": "OWID_EUR", "european": "OWID_EUR",
    "asien": "OWID_ASI", "asia": "OWID_ASI", "asiatisch": "OWID_ASI", "asian": "OWID_ASI",
    "afrika": "OWID_AFR", "africa": "OWID_AFR", "african": "OWID_AFR",
    "nordamerika": "OWID_NAM", "north america": "OWID_NAM",
    "südamerika": "OWID_SAM", "south america": "OWID_SAM",
    "ozeanien": "OWID_OCE", "oceania": "OWID_OCE",
    "antarktis": "OWID_ANT", "antarctica": "OWID_ANT",
    "weltweit": "OWID_WRL", "global": "OWID_WRL", "globus": "OWID_WRL", "world": "OWID_WRL",
}

# OWID entity name ("Europe (NIAID)") → internal cache key (map above)
_NIAID_CONTINENT_TO_KEY: dict[str, str] = {
    "Europe (NIAID)": "OWID_EUR",
    "Asia (NIAID)": "OWID_ASI",
    "Africa (NIAID)": "OWID_AFR",
    "North America (NIAID)": "OWID_NAM",
    "South America (NIAID)": "OWID_SAM",
    "Oceania (NIAID)": "OWID_OCE",
    "Antarctica (NIAID)": "OWID_ANT",
}

# Display names for internal keys (used in result titles)
_KEY_TO_DISPLAY: dict[str, str] = {
    "OWID_WRL": "Globus",
    "OWID_EUR": "Europa",
    "OWID_ASI": "Asien",
    "OWID_AFR": "Afrika",
    "OWID_NAM": "Nordamerika",
    "OWID_SAM": "Südamerika",
    "OWID_OCE": "Ozeanien",
    "OWID_ANT": "Antarktis",
}

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


async def _fetch_berkeley(client: httpx.AsyncClient | None = None) -> dict:
    """Download Berkeley Earth annual temperature anomalies via OWID mirror.

    Returns {key: {"entity": str, "years": {year: anomaly}}}
    where key = ISO3 for countries, "OWID_WRL"/"OWID_EUR"/... for global & continents.
    """
    global _berkeley_cache, _berkeley_cache_ts

    now = time.time()
    if _berkeley_cache is not None and (now - _berkeley_cache_ts) < BERKELEY_CACHE_TTL:
        return _berkeley_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=60.0)
        close_client = True

    merged: dict = {}
    try:
        resp = await client.get(BERKELEY_TEMP_URL)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            entity = (row.get("Entity") or "").strip()
            code = (row.get("Code") or "").strip()
            year_raw = (row.get("Year") or "").strip()
            val_raw = (row.get("Temperature anomaly") or "").strip()

            if not entity or not year_raw or not val_raw:
                continue

            # Determine cache key:
            # - Countries with ISO3 code → use code
            # - "World" (code = OWID_WRL) → OWID_WRL
            # - Continent entities ("Europe (NIAID)") → map to OWID_EUR etc.
            # - Ocean entities → skipped (not needed for fact-checking)
            if code:
                key = code
                display_entity = entity
            elif entity in _NIAID_CONTINENT_TO_KEY:
                key = _NIAID_CONTINENT_TO_KEY[entity]
                display_entity = entity[:-len(" (NIAID)")]
            else:
                continue

            try:
                year = int(year_raw)
                val = float(val_raw)
            except ValueError:
                continue

            merged.setdefault(key, {"entity": display_entity, "years": {}})
            merged[key]["years"][year] = val

        _berkeley_cache = merged
        _berkeley_cache_ts = now
        logger.info(
            f"Berkeley Earth: {len(merged)} entities (countries+continents+world) cached"
        )
        return merged
    except Exception as e:
        logger.warning(f"Berkeley Earth fetch failed: {e}")
        return _berkeley_cache or {}
    finally:
        if close_client:
            await client.aclose()


def _find_berkeley_entities(analysis: dict) -> list[str]:
    """Extract Berkeley cache keys (ISO3 or OWID_*) from the claim.

    Looks at NER countries first, then raw claim + entities for continents.
    Returns at most 3 keys. Returns [] if no specific region is mentioned
    (global claims are handled by NASA GISS).
    """
    ner_countries = analysis.get("ner_entities", {}).get("countries", [])
    claim = (analysis.get("claim") or "").lower()
    entities = [e.lower() for e in analysis.get("entities", [])]
    search_text = " ".join(ner_countries + entities + [claim]).lower()

    found: list[str] = []
    seen: set[str] = set()

    # Countries first
    for name, code in BERKELEY_COUNTRY_MAP.items():
        if name in search_text and code not in seen:
            found.append(code)
            seen.add(code)
            if len(found) >= 3:
                return found

    # Continents: collect candidates separately so we can filter "globe-only" out
    continent_keys: list[str] = []
    for name, key in BERKELEY_CONTINENT_MAP.items():
        if name in search_text and key not in seen:
            continent_keys.append(key)
            seen.add(key)

    # Purely global claims → leave Berkeley out (NASA GISS is the primary global source).
    # Globus is kept only when it appears alongside a country or continent (comparison).
    if not found and continent_keys == ["OWID_WRL"]:
        return []

    for key in continent_keys:
        found.append(key)
        if len(found) >= 3:
            break

    return found


def _format_berkeley_entry(cache: dict, key: str) -> dict | None:
    """Format a single Berkeley Earth country/continent entry into a result row.

    The title holds a single unambiguous primary fact (latest year + anomaly).
    Auxiliary facts (warmest year, 50-year trend, data range) go into
    ``description`` as separate sentences — both the reranker and the LLM
    synthesizer read ``description``, so each sentence becomes an indexable
    fact rather than noise crammed into a long title (Bug C).
    """
    entry = cache.get(key)
    if not entry:
        return None
    years_data = entry.get("years", {})
    if not years_data:
        return None

    latest_year = max(years_data.keys())
    latest_val = years_data[latest_year]
    vs_preindustrial = latest_val + PREINDUSTRIAL_OFFSET
    display_name = _KEY_TO_DISPLAY.get(key, entry.get("entity", key))

    # Warmest year on record
    max_year = max(years_data, key=years_data.get)
    max_val = years_data[max_year]
    max_preindustrial = max_val + PREINDUSTRIAL_OFFSET

    # 50-year trend
    trend_sentence = ""
    fifty_ago = latest_year - 50
    if fifty_ago in years_data:
        delta_50 = latest_val - years_data[fifty_ago]
        arrow = "↑" if delta_50 > 0.1 else ("↓" if delta_50 < -0.1 else "→")
        trend_sentence = (
            f"50-Jahres-Trend: {arrow} {delta_50:+.2f}°C seit {fifty_ago}."
        )

    earliest_year = min(years_data.keys())

    # --- Title: one primary fact only ---
    # Short enough that the LLM cannot miss the year or the value.
    title = (
        f"{display_name} Jahrestemperatur-Anomalie {latest_year}: "
        f"{latest_val:+.2f}°C vs. 1951–1980 Mittel "
        f"(ca. {vs_preindustrial:+.2f}°C vs. vorindustriell)"
    )

    # --- Description: auxiliary facts as separate sentences ---
    # The synthesizer compacts rows to a known field list; ``description`` is
    # included, so putting the warmest-year fact here guarantees the LLM sees
    # it as its own statement instead of as a suffix on the title.
    desc_parts = [
        f"Wärmstes Jahr seit Messbeginn für {display_name}: {max_year} "
        f"({max_val:+.2f}°C vs. 1951–1980 / ca. {max_preindustrial:+.2f}°C vs. vorindustriell)."
    ]
    if trend_sentence:
        desc_parts.append(trend_sentence)
    desc_parts.append(
        f"Datenbasis: Berkeley Earth jährliche Land-Temperatur-Anomalien "
        f"{earliest_year}–{latest_year} ({len(years_data)} Jahre)."
    )
    description = " ".join(desc_parts)

    return {
        "title": title,
        "indicator_name": f"Jahres-Temperaturanomalie {display_name}",
        "indicator": "berkeley_temperature_anomaly",
        "country": key,
        "country_name": display_name,
        "year": str(latest_year),
        "value": f"{vs_preindustrial:+.2f}°C vs. vorindustriell",
        "display_value": f"{latest_val:+.2f}°C",
        "description": description,
        "source": "Berkeley Earth (via Our World in Data)",
        "url": "https://berkeleyearth.org/data/",
    }


def _berkeley_caveat_row() -> dict:
    """Methodology caveat appended after Berkeley Earth results (V-Dem/RSF pattern)."""
    return {
        "title": "Methodik: Berkeley Earth Temperatur-Anomalien",
        "indicator_name": "WICHTIGER KONTEXT: Berkeley Earth misst Temperatur-Anomalien",
        "indicator": "Hinweis",
        "country": "",
        "country_name": "",
        "year": "",
        "value": "",
        "display_value": "",
        "source": "Berkeley Earth",
        "url": "https://berkeleyearth.org/about/",
        "description": (
            "Berkeley Earth misst Landtemperatur-Anomalien ggü. der Referenzperiode 1951–1980. "
            "Positive Werte = wärmer als die Baseline. Die Daten basieren auf ~1.6 Mio. "
            "Wetterstationen weltweit, statistisch kombiniert (Kriging) zu flächendeckenden Feldern. "
            "Einschränkungen: "
            "(1) Raumauflösung — Länderwerte sind räumliche Mittel; einzelne Regionen (Alpen, "
            "Küsten) können deutlich abweichen. "
            "(2) Vorindustrielles Niveau — die Baseline 1951–1980 liegt bereits ca. +0.3°C über "
            "dem vorindustriellen Mittel (1850–1900), das für IPCC-Ziele (1.5°C / 2°C) herangezogen "
            "wird. Die im Titel genannten Werte sind entsprechend umgerechnet. "
            "(3) Jahresvariabilität — einzelne Jahre schwanken natürlich um mehrere Zehntelgrad; "
            "Trends sind erst über ≥10 Jahre belastbar."
        ),
    }


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

    async with httpx.AsyncClient(timeout=20.0) as client:
        # Fetch real temperature data from NASA GISS if claim is temperature-related
        if is_temperature:
            giss_data = await _fetch_nasa_giss(client)
            results.extend(giss_data)

            # Berkeley Earth: add regional (country/continent) anomalies if applicable.
            # Trigger only when the claim mentions a specific country or continent —
            # purely global claims stay with NASA GISS (the proven default).
            berkeley_keys = _find_berkeley_entities(analysis)
            if berkeley_keys:
                berkeley_cache = await _fetch_berkeley(client)
                berkeley_rows = []
                for key in berkeley_keys:
                    row = _format_berkeley_entry(berkeley_cache, key)
                    if row:
                        berkeley_rows.append(row)
                if berkeley_rows:
                    results.extend(berkeley_rows)
                    results.append(_berkeley_caveat_row())

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
