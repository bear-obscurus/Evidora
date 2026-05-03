"""WHO Europe Gateway API v5 — europäische Gesundheitsdaten.

Ergänzt die globale WHO GHO API mit europaweiten Indikatoren aus dem
Health for All (HFA) Explorer der WHO/Europe.

API-Basis: https://dw.euro.who.int/api/v5/
Lizenz: Open data, CC BY-NC-SA 3.0 IGO
"""

import logging

import httpx
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BASE_URL = "https://dw.euro.who.int/api/v5"

# Map keywords (DE + EN) to HFA indicator codes
INDICATOR_MAP = {
    # Lebenserwartung
    "lebenserwartung": "HFA_43",
    "life_expectancy": "HFA_43",
    "life expectancy": "HFA_43",
    # Adipositas / Übergewicht
    "adipositas": "HFA_630",
    "obesity": "HFA_630",
    "übergewicht": "HFA_630",
    "bmi": "HFA_630",
    # Suizid
    "suizid": "HFA_173",
    "suicide": "HFA_173",
    "selbstmord": "HFA_173",
    # Alkohol
    "alkohol": "HFA_293",
    "alcohol": "HFA_293",
    # Tabak / Rauchen
    "tabak": "HFA_622",
    "tobacco": "HFA_622",
    "rauchen": "HFA_622",
    "smoking": "HFA_622",
    # Krankenhausbetten
    "krankenhausbetten": "HFA_476",
    "hospital_beds": "HFA_476",
    "hospital beds": "HFA_476",
    "spitalsbetten": "HFA_476",
    # Ärzte
    "ärzte": "HFA_494",
    "physicians": "HFA_494",
    "doctors": "HFA_494",
    # Säuglingssterblichkeit
    "säuglingssterblichkeit": "HFA_10",
    "infant_mortality": "HFA_10",
    "infant mortality": "HFA_10",
    # Müttersterblichkeit
    "müttersterblichkeit": "HFA_85",
    "maternal_mortality": "HFA_85",
    "maternal mortality": "HFA_85",
    # Tuberkulose
    "tuberkulose": "HFA_130",
    "tuberculosis": "HFA_130",
    # Masern
    "masern": "HFA_141",
    "measles": "HFA_141",
    # Impfung (Masern als Proxy)
    "impfung": "HFA_588",
    "vaccination": "HFA_588",
    "immunization": "HFA_588",
    # Gesundheitsausgaben
    "gesundheitsausgaben": "HFA_566",
    "health_expenditure": "HFA_566",
    "health expenditure": "HFA_566",
}

# Country name → ISO 3-letter code (European focus)
COUNTRY_MAP = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "schweiz": "CHE", "switzerland": "CHE",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD", "holland": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czech": "CZE", "czechia": "CZE",
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
    "vereinigtes königreich": "GBR", "united kingdom": "GBR",
    "türkei": "TUR", "turkey": "TUR", "türkiye": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
}


def _find_indicator(analysis: dict) -> str | None:
    """Find matching HFA indicator code from claim analysis."""
    entities = analysis.get("entities", [])
    subcategory = analysis.get("subcategory", "")
    keywords = analysis.get("spacy_keywords", [])
    search_terms = entities + keywords + [subcategory]

    for term in search_terms:
        term_lower = term.lower()
        for keyword, code in INDICATOR_MAP.items():
            if keyword in term_lower:
                return code

    return None


def _find_country(analysis: dict) -> str | None:
    """Extract European country code from claim text.

    Prioritizes SpaCy NER countries (from actual claim text) over
    LLM-extracted entities to avoid hallucinated country references.
    """
    # 1. SpaCy NER countries — guaranteed from actual claim text
    for country in analysis.get("ner_entities", {}).get("countries", []):
        for name, code in COUNTRY_MAP.items():
            if name in country.lower():
                return code

    # 2. Check claim text directly (catches adjective forms)
    claim_lower = analysis.get("claim", "").lower()
    for name, code in COUNTRY_MAP.items():
        if name in claim_lower:
            return code

    return None


async def search_who_europe(analysis: dict) -> dict:
    """Search the WHO Europe Gateway API for health indicators."""
    indicator = _find_indicator(analysis)

    if not indicator:
        return {"source": "WHO Europe (HFA)", "type": "official_data", "results": []}

    country = _find_country(analysis)

    # Build filter
    filters = []
    if country:
        filters.append(f"COUNTRY:{country}")
    else:
        # Default to Austria, Germany, EU average
        filters.append("COUNTRY:AUT,DEU,EU")
    filters.append("SEX:ALL")

    filter_str = ";".join(filters)

    results = []
    try:
        async with polite_client(timeout=30.0) as client:
            resp = await client.get(
                f"{BASE_URL}/measures/{indicator}",
                params={"filter": filter_str, "output": "data"},
            )
            resp.raise_for_status()
            data = resp.json()

            # Get measure metadata for display name
            measure_name = indicator
            meta_resp = await client.get(f"{BASE_URL}/measures/{indicator}")
            if meta_resp.status_code == 200:
                meta = meta_resp.json()
                measure_name = meta.get("label", indicator)

            rows = data.get("data", [])
            if not rows:
                return {"source": "WHO Europe (HFA)", "type": "official_data", "results": []}

            # Sort by year descending, take latest entries
            rows.sort(
                key=lambda r: r.get("dimensions", {}).get("YEAR", "0"),
                reverse=True,
            )

            seen_countries = set()
            for row in rows:
                dims = row.get("dimensions", {})
                val = row.get("value", {})
                country_code = dims.get("COUNTRY", "")
                year = dims.get("YEAR", "")
                numeric = val.get("numeric")

                if numeric is None:
                    continue

                # One result per country (latest year)
                if country_code in seen_countries:
                    continue
                seen_countries.add(country_code)

                results.append({
                    "indicator_name": f"{measure_name} ({country_code} {year})",
                    "indicator": indicator,
                    "country": country_code,
                    "year": year,
                    "value": numeric,
                    "url": f"https://dw.euro.who.int/api/v5/measures/{indicator}",
                })

                if len(results) >= 5:
                    break

    except httpx.HTTPStatusError as e:
        logger.warning(f"WHO Europe API error: {e.response.status_code}")
    except Exception as e:
        logger.warning(f"WHO Europe search failed: {e}")

    return {
        "source": "WHO Europe (HFA)",
        "type": "official_data",
        "results": results,
    }
