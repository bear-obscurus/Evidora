import re

import httpx
from services._http_polite import polite_client

BASE_URL = "https://ghoapi.azureedge.net/api"


def _sanitize_odata(value: str) -> str:
    """Escape user-derived values for safe OData filter injection prevention."""
    # Remove anything that isn't alphanumeric, space, or basic punctuation
    sanitized = re.sub(r"[^a-zA-Z0-9äöüÄÖÜß \-_]", "", value)
    # OData single-quote escaping (double the quote)
    sanitized = sanitized.replace("'", "''")
    # Limit length to prevent abuse
    return sanitized[:100]

# Common health indicators relevant for fact-checking
INDICATOR_MAP = {
    # Lebenserwartung & Sterblichkeit
    "life_expectancy": "WHOSIS_000001",
    "lebenserwartung": "WHOSIS_000001",
    "mortality": "NCDMORT3070",
    "sterblichkeit": "NCDMORT3070",
    "death": "NCDMORT3070",
    "infant_mortality": "MDG_0000000001",
    "säuglingssterblichkeit": "MDG_0000000001",
    "child_mortality": "MDG_0000000007",
    "kindersterblichkeit": "MDG_0000000007",
    # Impfungen
    "vaccination": "WHS4_100",
    "vaccine": "WHS4_100",
    "impfung": "WHS4_100",
    "immunization": "WHS4_100",
    "immunisierung": "WHS4_100",
    # Infektionskrankheiten
    "tuberculosis": "MDG_0000000020",
    "tuberkulose": "MDG_0000000020",
    "malaria": "MALARIA_EST_DEATHS",
    "hiv": "HIV_0000000001",
    "aids": "HIV_0000000001",
    "hepatitis": "WHS4_117",
    "measles": "WHS4_100",
    "masern": "WHS4_100",
    # Nicht-übertragbare Krankheiten
    "diabetes": "NCD_GLUC_04",
    "obesity": "NCD_BMI_30A",
    "adipositas": "NCD_BMI_30A",
    "übergewicht": "NCD_BMI_25A",
    "blood_pressure": "NCD_HYP_PREVALENCE_A",
    "blutdruck": "NCD_HYP_PREVALENCE_A",
    "hypertension": "NCD_HYP_PREVALENCE_A",
    "cancer": "NCDMORT3070",
    "krebs": "NCDMORT3070",
    # Umwelt & Gesundheit
    "air_pollution": "AIR_41",
    "luftverschmutzung": "AIR_41",
    "water": "WSH_SANITATION_SAFELY_MANAGED",
    "wasser": "WSH_SANITATION_SAFELY_MANAGED",
    "sanitation": "WSH_SANITATION_SAFELY_MANAGED",
    # Gesundheitssystem
    "hospital_beds": "HWF_0006",
    "krankenhausbetten": "HWF_0006",
    "physicians": "HWF_0001",
    "ärzte": "HWF_0001",
    # Mental Health
    "suicide": "MH_12",
    "suizid": "MH_12",
    "mental_health": "MH_12",
    # Rauchen & Alkohol
    "tobacco": "M_Est_tob_curr_std",
    "rauchen": "M_Est_tob_curr_std",
    "smoking": "M_Est_tob_curr_std",
    "alcohol": "SA_0000001688",
    "alkohol": "SA_0000001688",
}


async def search_who(analysis: dict) -> dict:
    entities = analysis.get("entities", [])
    subcategory = analysis.get("subcategory", "")

    # Find matching indicator
    indicator = None
    search_terms = entities + [subcategory]
    for term in search_terms:
        for keyword, code in INDICATOR_MAP.items():
            if keyword in term.lower():
                indicator = code
                break
        if indicator:
            break

    async with polite_client(timeout=30.0) as client:
        if indicator:
            # Fetch specific indicator data
            resp = await client.get(
                f"{BASE_URL}/{indicator}",
                params={"$filter": "SpatialDim eq 'EUR'", "$top": 10},
            )
            resp.raise_for_status()
            data = resp.json().get("value", [])

            # Resolve indicator name for display
            indicator_name = indicator
            for keyword, code in INDICATOR_MAP.items():
                if code == indicator:
                    indicator_name = keyword.replace("_", " ").title()
                    break

            results = []
            for entry in data[:5]:
                results.append(
                    {
                        "indicator": indicator,
                        "indicator_name": f"WHO: {indicator_name} ({entry.get('SpatialDim', '')} {entry.get('TimeDim', '')})",
                        "country": entry.get("SpatialDim", ""),
                        "year": entry.get("TimeDim", ""),
                        "value": entry.get("NumericValue", ""),
                        "url": f"https://www.who.int/data/gho/data/indicators/indicator-details/GHO/{indicator}",
                    }
                )

            return {
                "source": "WHO Global Health Observatory",
                "type": "official_data",
                "results": results,
            }
        else:
            # Search for relevant indicators
            safe_subcategory = _sanitize_odata(subcategory)
            if not safe_subcategory:
                return {
                    "source": "WHO Global Health Observatory",
                    "type": "official_data",
                    "results": [],
                }
            resp = await client.get(
                f"{BASE_URL}/Indicator",
                params={"$filter": f"contains(IndicatorName,'{safe_subcategory}')"},
            )
            resp.raise_for_status()
            indicators = resp.json().get("value", [])

            results = []
            for ind in indicators[:3]:
                results.append(
                    {
                        "indicator_code": ind.get("IndicatorCode", ""),
                        "indicator_name": ind.get("IndicatorName", ""),
                        "url": f"https://www.who.int/data/gho/data/indicators/indicator-details/GHO/{ind.get('IndicatorCode', '')}",
                    }
                )

            return {
                "source": "WHO Global Health Observatory",
                "type": "official_data",
                "results": results,
            }
