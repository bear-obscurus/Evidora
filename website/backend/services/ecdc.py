"""ECDC / infectious disease data via Our World in Data (OWID).

The original ECDC Surveillance Atlas API returned empty results for most
queries. This module fetches reliable data from OWID's GitHub CSV files
(COVID-19) and falls back to the ECDC Atlas for other infectious diseases.
"""

import csv
import io
import logging
from datetime import datetime

import httpx

logger = logging.getLogger("evidora")

# OWID COVID latest data — one row per country, ~30 KB
OWID_LATEST_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv"

# In-memory cache for OWID data
_owid_cache: dict | None = None
_owid_cache_ts: float = 0
OWID_CACHE_TTL = 86400  # 24 hours

# Disease keywords → type mapping
COVID_KEYWORDS = [
    "covid", "corona", "sars-cov-2", "coronavirus", "pandemie", "pandemic",
    "impfquote", "vaccination rate", "impfung", "geimpft",
]

DISEASE_MAP = {
    "masern": {"label": "Masern", "label_en": "Measles"},
    "measles": {"label": "Masern", "label_en": "Measles"},
    "grippe": {"label": "Influenza", "label_en": "Influenza"},
    "influenza": {"label": "Influenza", "label_en": "Influenza"},
    "flu": {"label": "Influenza", "label_en": "Influenza"},
    "tuberkulose": {"label": "Tuberkulose", "label_en": "Tuberculosis"},
    "tuberculosis": {"label": "Tuberkulose", "label_en": "Tuberculosis"},
    "tb": {"label": "Tuberkulose", "label_en": "Tuberculosis"},
    "hiv": {"label": "HIV/AIDS", "label_en": "HIV/AIDS"},
    "aids": {"label": "HIV/AIDS", "label_en": "HIV/AIDS"},
    "keuchhusten": {"label": "Keuchhusten", "label_en": "Pertussis"},
    "pertussis": {"label": "Keuchhusten", "label_en": "Pertussis"},
    "hepatitis": {"label": "Hepatitis", "label_en": "Hepatitis"},
    "malaria": {"label": "Malaria", "label_en": "Malaria"},
    "ebola": {"label": "Ebola", "label_en": "Ebola"},
    "polio": {"label": "Poliomyelitis", "label_en": "Poliomyelitis"},
    "kinderlähmung": {"label": "Poliomyelitis", "label_en": "Poliomyelitis"},
    "dengue": {"label": "Dengue-Fieber", "label_en": "Dengue"},
    "cholera": {"label": "Cholera", "label_en": "Cholera"},
    "mpox": {"label": "Mpox", "label_en": "Mpox"},
    "affenpocken": {"label": "Mpox", "label_en": "Mpox"},
}

# Country name → ISO code (for OWID matching)
COUNTRY_CODES = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "schweden": "SWE", "sweden": "SWE",
    "dänemark": "DNK", "denmark": "DNK",
    "finnland": "FIN", "finland": "FIN",
    "irland": "IRL", "ireland": "IRL",
    "portugal": "PRT",
    "griechenland": "GRC", "greece": "GRC",
    "tschechien": "CZE", "czechia": "CZE",
    "rumänien": "ROU", "romania": "ROU",
    "ungarn": "HUN", "hungary": "HUN",
    "kroatien": "HRV", "croatia": "HRV",
    "bulgarien": "BGR", "bulgaria": "BGR",
    "slowakei": "SVK", "slovakia": "SVK",
    "slowenien": "SVN", "slovenia": "SVN",
    "luxemburg": "LUX", "luxembourg": "LUX",
    "estland": "EST", "estonia": "EST",
    "lettland": "LVA", "latvia": "LVA",
    "litauen": "LTU", "lithuania": "LTU",
    "malta": "MLT", "zypern": "CYP", "cyprus": "CYP",
    "europa": "OWID_EUR", "europe": "OWID_EUR",
    "eu": "OWID_EUR",
    "welt": "OWID_WRL", "world": "OWID_WRL",
}


def _is_covid_claim(analysis: dict) -> bool:
    """Check if the claim is about COVID-19."""
    text = " ".join([
        analysis.get("claim", ""),
        analysis.get("subcategory", ""),
        " ".join(analysis.get("entities", [])),
    ]).lower()
    return any(kw in text for kw in COVID_KEYWORDS)


def _find_country_iso3(analysis: dict) -> str | None:
    """Extract ISO-3 country code from entities."""
    for entity in analysis.get("entities", []):
        for name, code in COUNTRY_CODES.items():
            if name in entity.lower():
                return code
    return None


def _find_disease(analysis: dict) -> dict | None:
    """Match claim to a non-COVID disease."""
    text = " ".join([
        analysis.get("subcategory", ""),
        " ".join(analysis.get("entities", [])),
    ]).lower()
    for keyword, disease in DISEASE_MAP.items():
        if keyword in text:
            return disease
    return None


async def _fetch_owid_latest(client: httpx.AsyncClient) -> list[dict]:
    """Fetch OWID COVID latest data (cached for 24h)."""
    global _owid_cache, _owid_cache_ts
    import time

    now = time.time()
    if _owid_cache is not None and now - _owid_cache_ts < OWID_CACHE_TTL:
        return _owid_cache

    try:
        resp = await client.get(OWID_LATEST_URL, timeout=15.0)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        rows = list(reader)
        _owid_cache = rows
        _owid_cache_ts = now
        logger.info(f"OWID COVID latest: {len(rows)} countries loaded")
        return rows
    except Exception as e:
        logger.warning(f"OWID COVID fetch failed: {e}")
        return _owid_cache or []


def _safe_int(val: str) -> int | None:
    """Parse a string to int, handling floats and empty strings."""
    if not val or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _safe_float(val: str) -> float | None:
    """Parse a string to float, handling empty strings."""
    if not val or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _format_number(n: int | float | None) -> str:
    """Format a number with thousands separator."""
    if n is None:
        return "k.A."
    if isinstance(n, float):
        return f"{n:,.1f}".replace(",", ".")
    return f"{n:,}".replace(",", ".")


async def _search_covid(analysis: dict, client: httpx.AsyncClient) -> list[dict]:
    """Search OWID for COVID-19 data."""
    rows = await _fetch_owid_latest(client)
    if not rows:
        return []

    country_code = _find_country_iso3(analysis)
    results = []

    if country_code:
        # Single country
        for row in rows:
            if row.get("iso_code") == country_code:
                location = row.get("location", country_code)
                last_updated = row.get("last_updated_date", "")
                total_cases = _safe_int(row.get("total_cases"))
                total_deaths = _safe_int(row.get("total_deaths"))
                total_vaccinations = _safe_int(row.get("total_vaccinations"))
                people_vaccinated = _safe_int(row.get("people_vaccinated"))
                population = _safe_int(row.get("population"))
                cases_per_million = _safe_float(row.get("total_cases_per_million"))
                deaths_per_million = _safe_float(row.get("total_deaths_per_million"))

                results.append({
                    "title": f"COVID-19 {location}: {_format_number(total_cases)} Fälle, {_format_number(total_deaths)} Todesfälle (Stand: {last_updated})",
                    "indicator": "COVID-19",
                    "country": location,
                    "date": last_updated,
                    "value": f"{_format_number(total_cases)} Fälle",
                    "source": "OWID/ECDC",
                    "url": "https://ourworldindata.org/covid-cases",
                })

                if total_vaccinations:
                    vacc_pct = ""
                    if people_vaccinated and population:
                        vacc_pct = f" ({people_vaccinated / population * 100:.1f}% mind. 1 Dosis)"
                    results.append({
                        "title": f"COVID-19 Impfungen {location}: {_format_number(total_vaccinations)} Dosen verabreicht{vacc_pct}",
                        "indicator": "COVID-19 Impfungen",
                        "country": location,
                        "date": last_updated,
                        "value": f"{_format_number(total_vaccinations)} Impfdosen",
                        "source": "OWID/ECDC",
                        "url": "https://ourworldindata.org/covid-vaccinations",
                    })

                if cases_per_million:
                    results.append({
                        "title": f"COVID-19 {location}: {_format_number(cases_per_million)} Fälle pro Million, {_format_number(deaths_per_million)} Todesfälle pro Million",
                        "indicator": "COVID-19 pro Kopf",
                        "country": location,
                        "date": last_updated,
                        "value": f"{_format_number(cases_per_million)} Fälle/Mio.",
                        "source": "OWID/ECDC",
                        "url": "https://ourworldindata.org/covid-cases",
                    })
                break
    else:
        # Top 10 countries by total cases for global overview
        valid = [r for r in rows if r.get("iso_code", "").startswith("OWID") is False
                 and _safe_int(r.get("total_cases")) is not None
                 and not r.get("iso_code", "").startswith("OWID")]
        valid.sort(key=lambda r: _safe_int(r.get("total_cases")) or 0, reverse=True)

        # Add world total
        for row in rows:
            if row.get("iso_code") == "OWID_WRL":
                results.append({
                    "title": f"COVID-19 weltweit: {_format_number(_safe_int(row.get('total_cases')))} Fälle, {_format_number(_safe_int(row.get('total_deaths')))} Todesfälle",
                    "indicator": "COVID-19 Weltweit",
                    "country": "Welt",
                    "date": row.get("last_updated_date", ""),
                    "value": f"{_format_number(_safe_int(row.get('total_cases')))} Fälle",
                    "source": "OWID/ECDC",
                    "url": "https://ourworldindata.org/covid-cases",
                })
                break

        # Add Europe total
        for row in rows:
            if row.get("iso_code") == "OWID_EUR":
                results.append({
                    "title": f"COVID-19 Europa: {_format_number(_safe_int(row.get('total_cases')))} Fälle, {_format_number(_safe_int(row.get('total_deaths')))} Todesfälle",
                    "indicator": "COVID-19 Europa",
                    "country": "Europa",
                    "date": row.get("last_updated_date", ""),
                    "value": f"{_format_number(_safe_int(row.get('total_cases')))} Fälle",
                    "source": "OWID/ECDC",
                    "url": "https://ourworldindata.org/covid-cases",
                })
                break

    return results


async def search_ecdc(analysis: dict) -> dict:
    """Search for infectious disease data (COVID via OWID, others via WHO fallback)."""
    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        if _is_covid_claim(analysis):
            results = await _search_covid(analysis, client)
        else:
            # For non-COVID diseases, provide OWID/WHO reference
            disease = _find_disease(analysis)
            if disease:
                results.append({
                    "title": f"{disease['label']}: Aktuelle Daten über WHO/ECDC verfügbar",
                    "indicator": disease["label"],
                    "country": "",
                    "value": "Siehe WHO Global Health Observatory",
                    "source": "ECDC/WHO",
                    "url": f"https://ourworldindata.org/search?q={disease['label_en'].replace(' ', '+')}",
                })

    return {
        "source": "ECDC (Infektionskrankheiten)",
        "type": "official_data",
        "results": results,
    }
