import httpx
import logging
from datetime import datetime

logger = logging.getLogger("evidora")

ATLAS_BASE = "https://atlas.ecdc.europa.eu/public/AtlasService/rest"

# Map disease keywords (DE + EN) to ECDC health topic codes
DISEASE_MAP = {
    # Masern
    "masern": {"code": "MEAS", "label": "Masern", "label_en": "Measles"},
    "measles": {"code": "MEAS", "label": "Masern", "label_en": "Measles"},
    # Influenza / Grippe
    "grippe": {"code": "INFL", "label": "Influenza", "label_en": "Influenza"},
    "influenza": {"code": "INFL", "label": "Influenza", "label_en": "Influenza"},
    "flu": {"code": "INFL", "label": "Influenza", "label_en": "Influenza"},
    # COVID-19 — not available as separate topic in ECDC Surveillance Atlas
    # "covid" / "corona" → no ECDC mapping
    # Tuberkulose
    "tuberkulose": {"code": "TUBE", "label": "Tuberkulose", "label_en": "Tuberculosis"},
    "tuberculosis": {"code": "TUBE", "label": "Tuberkulose", "label_en": "Tuberculosis"},
    "tb": {"code": "TUBE", "label": "Tuberkulose", "label_en": "Tuberculosis"},
    # HIV/AIDS
    "hiv": {"code": "HIV", "label": "HIV/AIDS", "label_en": "HIV/AIDS"},
    "aids": {"code": "HIV", "label": "HIV/AIDS", "label_en": "HIV/AIDS"},
    # Keuchhusten
    "keuchhusten": {"code": "PERT", "label": "Keuchhusten", "label_en": "Pertussis"},
    "pertussis": {"code": "PERT", "label": "Keuchhusten", "label_en": "Pertussis"},
    "whooping cough": {"code": "PERT", "label": "Keuchhusten", "label_en": "Pertussis"},
    # Hepatitis
    "hepatitis a": {"code": "HEPA", "label": "Hepatitis A", "label_en": "Hepatitis A"},
    "hepatitis b": {"code": "HEPB", "label": "Hepatitis B", "label_en": "Hepatitis B"},
    "hepatitis c": {"code": "HEPC", "label": "Hepatitis C", "label_en": "Hepatitis C"},
    "hepatitis": {"code": "HEPA", "label": "Hepatitis A", "label_en": "Hepatitis A"},
    # Salmonellose
    "salmonellen": {"code": "SALM", "label": "Salmonellose", "label_en": "Salmonellosis"},
    "salmonellose": {"code": "SALM", "label": "Salmonellose", "label_en": "Salmonellosis"},
    "salmonellosis": {"code": "SALM", "label": "Salmonellose", "label_en": "Salmonellosis"},
    "salmonella": {"code": "SALM", "label": "Salmonellose", "label_en": "Salmonellosis"},
    # Dengue
    "dengue": {"code": "DENGUE", "label": "Dengue-Fieber", "label_en": "Dengue"},
    # Malaria
    "malaria": {"code": "MALA", "label": "Malaria", "label_en": "Malaria"},
    # West-Nil-Virus
    "west-nil": {"code": "WNF", "label": "West-Nil-Fieber", "label_en": "West Nile Fever"},
    "west nile": {"code": "WNF", "label": "West-Nil-Fieber", "label_en": "West Nile Fever"},
    # Polio
    "polio": {"code": "POLI", "label": "Poliomyelitis", "label_en": "Poliomyelitis"},
    "kinderlähmung": {"code": "POLI", "label": "Poliomyelitis", "label_en": "Poliomyelitis"},
    # Diphtherie
    "diphtherie": {"code": "DIPH", "label": "Diphtherie", "label_en": "Diphtheria"},
    "diphtheria": {"code": "DIPH", "label": "Diphtherie", "label_en": "Diphtheria"},
    # Röteln
    "röteln": {"code": "RUBE", "label": "Röteln", "label_en": "Rubella"},
    "rubella": {"code": "RUBE", "label": "Röteln", "label_en": "Rubella"},
    # Mumps
    "mumps": {"code": "MUMP", "label": "Mumps", "label_en": "Mumps"},
    # Cholera
    "cholera": {"code": "CHOL", "label": "Cholera", "label_en": "Cholera"},
    # Legionellen
    "legionellen": {"code": "LEGI", "label": "Legionärskrankheit", "label_en": "Legionnaires' Disease"},
    "legionella": {"code": "LEGI", "label": "Legionärskrankheit", "label_en": "Legionnaires' Disease"},
    "legionnaires": {"code": "LEGI", "label": "Legionärskrankheit", "label_en": "Legionnaires' Disease"},
    # FSME / Zecken
    "fsme": {"code": "TBE", "label": "FSME (Zeckenenzephalitis)", "label_en": "Tick-borne Encephalitis"},
    "zecken": {"code": "TBE", "label": "FSME (Zeckenenzephalitis)", "label_en": "Tick-borne Encephalitis"},
    "tick-borne encephalitis": {"code": "TBE", "label": "FSME (Zeckenenzephalitis)", "label_en": "Tick-borne Encephalitis"},
    # Antibiotikaresistenz
    "antibiotikaresistenz": {"code": "AMR", "label": "Antibiotikaresistenz", "label_en": "Antimicrobial Resistance"},
    "antimicrobial resistance": {"code": "AMR", "label": "Antibiotikaresistenz", "label_en": "Antimicrobial Resistance"},
    "amr": {"code": "AMR", "label": "Antibiotikaresistenz", "label_en": "Antimicrobial Resistance"},
    "mrsa": {"code": "AMR", "label": "Antibiotikaresistenz", "label_en": "Antimicrobial Resistance"},
    # Ebola
    "ebola": {"code": "FILO", "label": "Ebola", "label_en": "Ebola/Marburg"},
    # Mpox (Affenpocken)
    # Mpox — not available as separate topic in ECDC Surveillance Atlas
}

# Country name to ISO 2-letter code (reuse Eurostat mapping)
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
    "griechenland": "EL", "greece": "EL",
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
    "island": "IS", "iceland": "IS",
}

# Cache for health topic IDs (populated on first request)
_topic_cache: dict[str, int] = {}


def _find_disease(analysis: dict) -> dict | None:
    """Match claim analysis to an ECDC disease topic."""
    entities = analysis.get("entities", [])
    subcategory = analysis.get("subcategory", "")
    search_terms = entities + [subcategory]

    for term in search_terms:
        term_lower = term.lower()
        for keyword, disease in DISEASE_MAP.items():
            if keyword in term_lower:
                return disease
    return None


def _find_country(analysis: dict) -> str | None:
    """Extract country code from entities. Returns None for EU-wide search."""
    entities = analysis.get("entities", [])
    for entity in entities:
        entity_lower = entity.lower()
        for name, code in COUNTRY_CODES.items():
            if name in entity_lower:
                return code
    return None


async def _get_topic_id(client: httpx.AsyncClient, topic_code: str) -> int | None:
    """Get the health topic ID for a given code, using cache."""
    if topic_code in _topic_cache:
        return _topic_cache[topic_code]

    try:
        resp = await client.get(f"{ATLAS_BASE}/GetHealthTopics")
        resp.raise_for_status()
        data = resp.json()

        for topic in data.get("HealthTopics", []):
            _topic_cache[topic["Code"]] = topic["Id"]

        return _topic_cache.get(topic_code)
    except Exception as e:
        logger.warning(f"ECDC: Failed to fetch health topics: {e}")
        return None


async def _get_dataset_and_measure(client: httpx.AsyncClient, topic_id: int) -> tuple[int | None, int | None]:
    """Get the current dataset ID and 'reported cases' measure ID for a topic."""
    try:
        resp = await client.get(
            f"{ATLAS_BASE}/GetDatasetsForHealthTopic",
            params={"healthTopicId": topic_id},
        )
        resp.raise_for_status()
        raw = resp.json()
        datasets = raw.get("Datasets", []) if isinstance(raw, dict) else raw

        # Find the CURRENT dataset
        dataset_id = None
        for ds in datasets:
            code = ds.get("Code", "")
            if code.startswith("CURRENT."):
                dataset_id = ds["Id"]
                break

        if dataset_id is None and datasets:
            dataset_id = datasets[0]["Id"]

        if dataset_id is None:
            return None, None

        # Get measures for this topic + dataset
        resp = await client.get(
            f"{ATLAS_BASE}/GetIndicatorMeasuresForHealthTopicAndDataset",
            params={"healthTopicId": topic_id, "datasetId": dataset_id},
        )
        resp.raise_for_status()
        raw_measures = resp.json()
        measures = raw_measures.get("Measures", []) if isinstance(raw_measures, dict) else raw_measures

        # Prefer "reported cases" (ALL.COUNT), fallback to first measure
        measure_id = None
        for m in measures:
            code = m.get("Code", "")
            if code == "ALL.COUNT":
                measure_id = m["Id"]
                break

        # Fallback: any COUNT measure
        if measure_id is None:
            for m in measures:
                code = m.get("Code", "")
                if "COUNT" in code and "FATAL" not in code:
                    measure_id = m["Id"]
                    break

        # Last fallback: first measure
        if measure_id is None and measures:
            measure_id = measures[0]["Id"]

        return dataset_id, measure_id

    except Exception as e:
        logger.warning(f"ECDC: Failed to get dataset/measure for topic {topic_id}: {e}")
        return None, None


async def search_ecdc(analysis: dict) -> dict:
    """Search ECDC Surveillance Atlas for infectious disease data."""
    disease = _find_disease(analysis)

    if not disease:
        return {
            "source": "ECDC",
            "type": "official_data",
            "results": [],
        }

    country = _find_country(analysis)
    now = datetime.now()
    # Query last 5 years
    start_year = now.year - 5
    end_year = now.year + 1

    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        topic_id = await _get_topic_id(client, disease["code"])
        if topic_id is None:
            logger.warning(f"ECDC: Health topic not found: {disease['code']}")
            return {"source": "ECDC", "type": "official_data", "results": []}

        dataset_id, measure_id = await _get_dataset_and_measure(client, topic_id)
        if measure_id is None:
            logger.warning(f"ECDC: No measure found for topic {disease['code']}")
            return {"source": "ECDC", "type": "official_data", "results": []}

        try:
            if country:
                # Query specific country
                resp = await client.get(
                    f"{ATLAS_BASE}/GetMeasureResultsForTimePeriodAndGeoRegion",
                    params={
                        "measureId": measure_id,
                        "timeCodes": "",
                        "startTimeCode": str(start_year),
                        "endTimeCodeExcl": str(end_year),
                        "geoCode": country,
                    },
                )
            else:
                # Query all countries (EU level)
                resp = await client.get(
                    f"{ATLAS_BASE}/GetMeasureResultsForTimePeriodAndGeoLevel",
                    params={
                        "measureIds": measure_id,
                        "timeCodes": "",
                        "startTimeCode": str(start_year),
                        "endTimeCodeExcl": str(end_year),
                        "geoLevel": 2,
                    },
                )

            resp.raise_for_status()
            raw_data = resp.json()
            data = raw_data.get("MeasureResults", []) if isinstance(raw_data, dict) else raw_data

            if not isinstance(data, list):
                data = []

            # Filter to yearly data only and aggregate by year (or country+year)
            yearly_data = {}
            for entry in data:
                time_code = entry.get("TimeCode", "")
                geo = entry.get("GeoCountry", "")
                value = entry.get("YValue")

                if value is None:
                    continue

                # Only yearly data (format "YYYY", not "YYYY-MM")
                if len(time_code) == 4 and time_code.isdigit():
                    if country:
                        # Single country: aggregate by year
                        key = time_code
                        if key not in yearly_data or value > yearly_data[key]["value"]:
                            yearly_data[key] = {"year": time_code, "geo": geo, "value": value}
                    else:
                        # All countries: sum per year
                        key = time_code
                        if key not in yearly_data:
                            yearly_data[key] = {"year": time_code, "geo": "EU/EEA", "value": 0, "countries": 0}
                        yearly_data[key]["value"] += value
                        yearly_data[key]["countries"] += 1

            # Sort by year descending
            sorted_years = sorted(yearly_data.values(), key=lambda x: x["year"], reverse=True)

            country_label = country if country else "EU/EEA"
            # Resolve country name
            if country:
                for name, code in COUNTRY_CODES.items():
                    if code == country and name[0].isupper():
                        country_label = name.title()
                        break

            for entry in sorted_years[:5]:
                value = int(entry["value"]) if entry["value"] == int(entry["value"]) else entry["value"]
                geo_label = country_label if country else f"EU/EEA ({entry.get('countries', '')} Länder)"

                results.append({
                    "title": f"ECDC: {disease['label']} — {geo_label} {entry['year']}: {value:,} gemeldete Fälle".replace(",", "."),
                    "indicator": disease["label"],
                    "country": geo_label,
                    "year": entry["year"],
                    "value": f"{value:,} Fälle".replace(",", "."),
                    "source": "ECDC",
                    "url": f"https://atlas.ecdc.europa.eu/public/index.aspx?Dataset=27&HealthTopic={topic_id}",
                })

        except Exception as e:
            logger.warning(f"ECDC: Data request failed for {disease['code']}: {e}")

    return {
        "source": "ECDC",
        "type": "official_data",
        "results": results,
    }
