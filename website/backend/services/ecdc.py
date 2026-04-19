"""ECDC / infectious disease data via Our World in Data (OWID).

The ECDC Surveillance Atlas API requires authentication and doesn't
publish bulk CSV. We therefore query OWID mirrors:

- COVID-19 cases + vaccinations: OWID GitHub (permissive)
- Reported measles cases (annual, by country): OWID grapher (WHO-sourced,
  explicitly redistributable)
- Global vaccination coverage (MCV1, DTP3, Polio, HepB, Rubella, Rota,
  Pneumococcal, Hib): OWID grapher — WHO/UNICEF Estimates of National
  Immunization Coverage (WUENIC), redistributable

Notes on what's *not* available via OWID:
- HIV / TB / Malaria incidence are WHO GHO data and flagged
  non-redistributable (OWID returns 403). We therefore surface these as
  reference links only (with the canonical WHO URL) — same as before.
- Flu/RSV surveillance data (ECDC ERVISS) is not published as open CSV
  as of 2026-04.  We surface an ECDC link instead.
"""

import csv
import io
import logging
import time
from datetime import datetime

import httpx

logger = logging.getLogger("evidora")

# OWID COVID latest data — one row per country, ~30 KB
OWID_LATEST_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv"

# Additional OWID grapher CSVs (redistributable, WHO/UNICEF upstream)
OWID_MEASLES_URL = "https://ourworldindata.org/grapher/reported-cases-of-measles.csv"
OWID_VACCINATION_URL = "https://ourworldindata.org/grapher/global-vaccination-coverage.csv"

# In-memory caches
_owid_cache: dict | None = None
_owid_cache_ts: float = 0
OWID_CACHE_TTL = 86400  # 24 hours

# Measles cache: {iso3: {year: cases}}
_measles_cache: dict | None = None
_measles_cache_ts: float = 0

# Vaccination coverage cache: {iso3: {year: {MCV1, DTP3, Pol3, HepB3, ...}}}
_vacc_cache: dict | None = None
_vacc_cache_ts: float = 0

# Mapping Disease-Keyword → Vaccine column in OWID global-vaccination-coverage.csv
DISEASE_TO_VACCINE = {
    "masern": ("MCV1", "Masern (MCV1, 1. Dosis)"),
    "measles": ("MCV1", "Measles (MCV1, 1st dose)"),
    "röteln": ("RCV1", "Röteln (RCV1)"),
    "rubella": ("RCV1", "Rubella (RCV1)"),
    "polio": ("Pol3", "Polio (Pol3)"),
    "kinderlähmung": ("Pol3", "Polio (Pol3)"),
    "keuchhusten": ("DTP3", "Diphtherie/Tetanus/Pertussis (DTP3)"),
    "pertussis": ("DTP3", "Diphtheria/Tetanus/Pertussis (DTP3)"),
    "diphtherie": ("DTP3", "Diphtherie/Tetanus/Pertussis (DTP3)"),
    "tetanus": ("DTP3", "Diphtherie/Tetanus/Pertussis (DTP3)"),
    "hepatitis b": ("HepB3", "Hepatitis B (HepB3)"),
    "hepatitis-b": ("HepB3", "Hepatitis B (HepB3)"),
    "haemophilus": ("Hib3", "Haemophilus influenzae Typ b (Hib3)"),
    "rotavirus": ("RotaC", "Rotavirus (RotaC)"),
    "pneumokokken": ("PCV3", "Pneumokokken (PCV3)"),
    "pneumococcal": ("PCV3", "Pneumococcal (PCV3)"),
}

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
    """Extract ISO-3 country code from claim text.

    Prioritizes SpaCy NER countries (from actual claim text) over
    LLM-extracted entities to avoid hallucinated country references.
    """
    # 1. SpaCy NER countries — guaranteed from actual claim text
    for country in analysis.get("ner_entities", {}).get("countries", []):
        for name, code in COUNTRY_CODES.items():
            if name in country.lower():
                return code

    # 2. Check claim text directly (catches adjective forms)
    claim_lower = analysis.get("claim", "").lower()
    for name, code in COUNTRY_CODES.items():
        if name in claim_lower:
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


async def _fetch_measles(client: httpx.AsyncClient | None = None) -> dict:
    """Fetch reported measles cases per country/year (cached 24h).

    Returns ``{iso3: {year: cases, "entity": name}}`` — annual counts from
    WHO/UNICEF, mirrored via OWID.
    """
    global _measles_cache, _measles_cache_ts
    now = time.time()
    if _measles_cache is not None and now - _measles_cache_ts < OWID_CACHE_TTL:
        return _measles_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    merged: dict = {}
    try:
        resp = await client.get(OWID_MEASLES_URL)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        col = "Measles - number of reported cases"
        for row in reader:
            code = (row.get("Code") or "").strip()
            entity = (row.get("Entity") or "").strip()
            year_raw = (row.get("Year") or "").strip()
            val_raw = (row.get(col) or "").strip()
            if not code or not year_raw or not val_raw:
                continue
            try:
                year = int(year_raw)
                cases = int(float(val_raw))
            except ValueError:
                continue
            bucket = merged.setdefault(code, {"entity": entity})
            bucket[year] = cases
        _measles_cache = merged
        _measles_cache_ts = now
        logger.info(f"OWID measles: {len(merged)} countries cached")
        return merged
    except Exception as e:
        logger.warning(f"OWID measles fetch failed: {e}")
        return _measles_cache or {}
    finally:
        if close_client:
            await client.aclose()


async def _fetch_vaccination(client: httpx.AsyncClient | None = None) -> dict:
    """Fetch global vaccination coverage per country/year (cached 24h).

    Source: WUENIC (WHO/UNICEF Estimates of National Immunization Coverage)
    via OWID grapher.  Columns include MCV1 (Masern), DTP3
    (Diphtherie/Tetanus/Pertussis), Pol3 (Polio), HepB3, RCV1 (Röteln),
    Hib3, PCV3, RotaC.

    Returns ``{iso3: {year: {MCV1: %, DTP3: %, ..., "entity": name}}}``.
    """
    global _vacc_cache, _vacc_cache_ts
    now = time.time()
    if _vacc_cache is not None and now - _vacc_cache_ts < OWID_CACHE_TTL:
        return _vacc_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    merged: dict = {}
    try:
        resp = await client.get(OWID_VACCINATION_URL)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        # Header: Entity, Code, Year, HepB3, Hib3, IPV1, MCV1, PCV3, Pol3,
        # RCV1, RotaC, "Diptheria/tetanus/pertussis (DTP3)"
        vacc_cols = {
            "HepB3": "Hepatitis B (HepB3)",
            "Hib3": "H. influenza type b (Hib3)",
            "IPV1": "Inactivated polio vaccine (IPV1)",
            "MCV1": "Measles, first dose (MCV1)",
            "PCV3": "Pneumococcal vaccine (PCV3)",
            "Pol3": "Polio (Pol3)",
            "RCV1": "Rubella (RCV1)",
            "RotaC": "Rotavirus (RotaC)",
            "DTP3": "Diptheria/tetanus/pertussis (DTP3)",
        }
        for row in reader:
            code = (row.get("Code") or "").strip()
            entity = (row.get("Entity") or "").strip()
            year_raw = (row.get("Year") or "").strip()
            if not code or not year_raw:
                continue
            try:
                year = int(year_raw)
            except ValueError:
                continue
            country = merged.setdefault(code, {})
            year_bucket = country.setdefault(year, {"entity": entity})
            for short, full in vacc_cols.items():
                val_raw = (row.get(full) or "").strip()
                if not val_raw:
                    continue
                try:
                    year_bucket[short] = float(val_raw)
                except ValueError:
                    continue
        _vacc_cache = merged
        _vacc_cache_ts = now
        total_years = sum(len(v) for v in merged.values())
        logger.info(
            f"OWID vaccination coverage: {len(merged)} countries, "
            f"{total_years} country-years cached"
        )
        return merged
    except Exception as e:
        logger.warning(f"OWID vaccination fetch failed: {e}")
        return _vacc_cache or {}
    finally:
        if close_client:
            await client.aclose()


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


def _is_vaccination_context(analysis: dict) -> bool:
    """True when the *user's raw claim* is about vaccination coverage.

    We intentionally look only at ``analysis["claim"]`` (the user's input),
    not at LLM-derived ``subcategory``/``entities``. The LLM routinely tags
    a Masern-"Fälle" claim with "vaccination" concepts, which previously
    hijacked the cases path and returned MCV1 coverage instead.
    """
    text = (analysis.get("claim") or "").lower()

    # Explicit *cases* signals override vaccination keywords. If the user asks
    # about Fälle/outbreak/incidence, that's an incidence question even if
    # vaccination is somehow mentioned alongside.
    case_kws = (
        "fälle", "fallzahl", "ausbruch", "ausbrüche",
        "cases", "outbreak", "outbreaks",
        "incidence", "inzidenz", "neuinfektion", "neuinfektionen",
    )
    if any(kw in text for kw in case_kws):
        return False

    vacc_kws = (
        "impfquote", "impfrate", "impfung", "geimpft", "durchimpfung",
        "vaccination", "vaccine", "coverage", "vaccinated", "immunization",
        "immunisierung", "immunisation",
    )
    return any(kw in text for kw in vacc_kws)


def _find_vaccine_code(analysis: dict) -> tuple[str, str] | None:
    """Map claim keywords to a (vaccine_code, display_label) pair."""
    text = " ".join([
        analysis.get("claim", ""),
        analysis.get("subcategory", ""),
        " ".join(analysis.get("entities", [])),
    ]).lower()
    for keyword, (code, label) in DISEASE_TO_VACCINE.items():
        if keyword in text:
            return code, label
    return None


async def _search_measles_cases(analysis: dict, client: httpx.AsyncClient) -> list[dict]:
    """Return reported-measles-cases rows for the claim's country (or fallback)."""
    data = await _fetch_measles(client)
    if not data:
        return []

    results: list[dict] = []
    country_code = _find_country_iso3(analysis)
    codes = [country_code] if country_code else ["AUT", "DEU", "OWID_WRL"]

    for code in codes:
        country = data.get(code)
        if not country:
            continue
        entity = country.get("entity", code)
        years = sorted(k for k in country.keys() if isinstance(k, int))
        if not years:
            continue
        latest_year = years[-1]
        latest_cases = country[latest_year]

        # Reference point ~5 years prior for a trend arrow
        ref_year: int | None = None
        for y in years:
            if y <= latest_year - 5:
                ref_year = y
        if ref_year is None and len(years) >= 2:
            ref_year = years[0]

        trend = ""
        if ref_year is not None and ref_year != latest_year:
            ref_cases = country[ref_year]
            if ref_cases > 0:
                delta_pct = (latest_cases - ref_cases) / ref_cases * 100
                arrow = "↑" if delta_pct > 10 else ("↓" if delta_pct < -10 else "→")
                trend = f" — {arrow} seit {ref_year}: {delta_pct:+.0f}%"
            elif latest_cases > 0:
                trend = f" — Anstieg gegenüber 0 Fällen in {ref_year}"

        results.append({
            "title": f"Masern {entity}: {_format_number(latest_cases)} gemeldete Fälle in {latest_year}{trend}",
            "indicator": "Masern-Fälle (jährlich gemeldet)",
            "country": entity,
            "date": str(latest_year),
            "value": f"{_format_number(latest_cases)} Fälle",
            "source": "WHO via Our World in Data",
            "url": "https://ourworldindata.org/grapher/reported-cases-of-measles",
        })

    if results:
        results.append({
            "title": "Methodik: Gemeldete Masern-Fälle — Untererfassung möglich",
            "indicator": "Hinweis",
            "country": "",
            "value": "WHO-Daten auf Basis nationaler Meldesysteme. Labor­bestätigungs­raten und Melde­praxis variieren zwischen Ländern.",
            "source": "WHO/UNICEF",
            "url": "https://immunizationdata.who.int/",
        })

    return results


async def _search_vaccination(
    analysis: dict,
    client: httpx.AsyncClient,
    vaccine_code: str,
    vaccine_label: str,
) -> list[dict]:
    """Return WUENIC vaccination-coverage rows for the claim's country (or fallback)."""
    data = await _fetch_vaccination(client)
    if not data:
        return []

    results: list[dict] = []
    country_code = _find_country_iso3(analysis)
    codes = [country_code] if country_code else ["AUT", "DEU", "OWID_WRL"]

    for code in codes:
        country = data.get(code)
        if not country:
            continue
        years = sorted(k for k in country.keys() if isinstance(k, int))

        latest_year: int | None = None
        latest_val: float | None = None
        for y in reversed(years):
            val = country[y].get(vaccine_code)
            if val is not None:
                latest_year = y
                latest_val = val
                break
        if latest_year is None or latest_val is None:
            continue

        entity = country[latest_year].get("entity", code)

        # Reference point ~5 years prior for a trend arrow
        ref_year: int | None = None
        ref_val: float | None = None
        for y in years:
            if y <= latest_year - 5:
                v = country[y].get(vaccine_code)
                if v is not None:
                    ref_year = y
                    ref_val = v

        trend = ""
        if ref_val is not None and ref_year is not None:
            delta = latest_val - ref_val
            arrow = "↑" if delta > 1 else ("↓" if delta < -1 else "→")
            trend = f" — {arrow} seit {ref_year}: {delta:+.1f} pp"

        results.append({
            "title": f"Impfquote {vaccine_label} {entity}: {latest_val:.0f}% ({latest_year}){trend}",
            "indicator": f"Impfquote {vaccine_label}",
            "country": entity,
            "date": str(latest_year),
            "value": f"{latest_val:.0f}%",
            "source": "WHO/UNICEF WUENIC via Our World in Data",
            "url": "https://ourworldindata.org/grapher/global-vaccination-coverage",
        })

    if results:
        results.append({
            "title": "Methodik: WUENIC — nationale Schätzungen, jährlich revidiert",
            "indicator": "Hinweis",
            "country": "",
            "value": "WHO/UNICEF Estimates of National Immunization Coverage. Basiert auf Verwaltungsdaten und Surveys; rück­wirkende Revisionen üblich.",
            "source": "WHO/UNICEF WUENIC",
            "url": "https://immunizationdata.who.int/",
        })

    return results


async def search_ecdc(analysis: dict) -> dict:
    """Search for infectious disease data.

    Routing:
    - COVID-19 claim (and no other disease mentioned) → OWID COVID latest
    - Measles claim → either MCV1 coverage (if vaccination context) or reported cases
    - Other vaccine-preventable disease (Polio/Pertussis/Diphtherie/Tetanus/HepB/
      Hib/Rota/Pneumokokken/Röteln) → WUENIC coverage for the matching vaccine
    - HIV / TB / Malaria / Influenza / RSV / Ebola / Dengue / Cholera / Mpox →
      reference link to WHO (these datasets are not redistributable via OWID)
    """
    results: list[dict] = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        disease = _find_disease(analysis)
        covid_kw = _is_covid_claim(analysis)

        # COVID path only if the claim doesn't explicitly name a non-COVID disease.
        # (Avoids "Impfquote Masern" getting routed to COVID.)
        if covid_kw and disease is None:
            results = await _search_covid(analysis, client)
        elif disease:
            label_en = disease["label_en"]
            vacc = _find_vaccine_code(analysis)
            vacc_context = _is_vaccination_context(analysis)

            if label_en == "Measles":
                # Cases by default, coverage if the claim is explicitly about vaccination
                if vacc_context:
                    results = await _search_vaccination(
                        analysis, client, "MCV1", "Masern (MCV1, 1. Dosis)"
                    )
                else:
                    results = await _search_measles_cases(analysis, client)
            elif vacc:
                # Polio / DTP / HepB / Hib / Rota / Pneumo / Rubella — coverage only
                code, label = vacc
                results = await _search_vaccination(analysis, client, code, label)
            else:
                # HIV / TB / Malaria / Influenza / RSV / Ebola / Dengue / Cholera / Mpox
                results.append({
                    "title": f"{disease['label']}: Daten via WHO Global Health Observatory",
                    "indicator": disease["label"],
                    "country": "",
                    "value": "WHO GHO ist die primäre Quelle. Die Daten dürfen von OWID nicht gespiegelt werden — bitte direkt dort abfragen.",
                    "source": "WHO/ECDC (Referenz)",
                    "url": "https://www.who.int/data/gho",
                })

    return {
        "source": "ECDC (Infektionskrankheiten)",
        "type": "official_data",
        "results": results,
    }
