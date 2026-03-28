"""Energy safety data — deaths per TWh by energy source.

Static dataset based on Our World in Data / Markandya & Wilkinson (2007, Lancet),
Sovacool et al. (2016, Journal of Cleaner Production), and OWID estimates.

Triggered for claims comparing energy source safety, risk, or mortality.
Source: https://ourworldindata.org/safest-sources-of-energy
License: CC BY 4.0
"""

import logging

logger = logging.getLogger("evidora")

# Deaths per TWh of electricity production (OWID 2021)
ENERGY_DEATHS = {
    "braunkohle":    {"name_de": "Braunkohle",    "name_en": "Brown coal",  "deaths_per_twh": 32.72},
    "kohle":         {"name_de": "Kohle",         "name_en": "Coal",        "deaths_per_twh": 24.62},
    "öl":            {"name_de": "Öl",            "name_en": "Oil",         "deaths_per_twh": 18.43},
    "biomasse":      {"name_de": "Biomasse",      "name_en": "Biomass",     "deaths_per_twh": 4.63},
    "gas":           {"name_de": "Erdgas",         "name_en": "Gas",         "deaths_per_twh": 2.82},
    "erdgas":        {"name_de": "Erdgas",         "name_en": "Gas",         "deaths_per_twh": 2.82},
    "wasserkraft":   {"name_de": "Wasserkraft",   "name_en": "Hydropower",  "deaths_per_twh": 1.30},
    "wind":          {"name_de": "Windenergie",   "name_en": "Wind",        "deaths_per_twh": 0.035},
    "windenergie":   {"name_de": "Windenergie",   "name_en": "Wind",        "deaths_per_twh": 0.035},
    "windkraft":     {"name_de": "Windkraft",     "name_en": "Wind",        "deaths_per_twh": 0.035},
    "atom":          {"name_de": "Atomkraft",     "name_en": "Nuclear",     "deaths_per_twh": 0.03},
    "atomkraft":     {"name_de": "Atomkraft",     "name_en": "Nuclear",     "deaths_per_twh": 0.03},
    "kernkraft":     {"name_de": "Kernkraft",     "name_en": "Nuclear",     "deaths_per_twh": 0.03},
    "kernenergie":   {"name_de": "Kernenergie",   "name_en": "Nuclear",     "deaths_per_twh": 0.03},
    "nuklear":       {"name_de": "Nuklearenergie", "name_en": "Nuclear",    "deaths_per_twh": 0.03},
    "solar":         {"name_de": "Solarenergie",  "name_en": "Solar",       "deaths_per_twh": 0.019},
    "solarenergie":  {"name_de": "Solarenergie",  "name_en": "Solar",       "deaths_per_twh": 0.019},
    "photovoltaik":  {"name_de": "Photovoltaik",  "name_en": "Solar",       "deaths_per_twh": 0.019},
    # English aliases
    "coal":          {"name_de": "Kohle",         "name_en": "Coal",        "deaths_per_twh": 24.62},
    "brown coal":    {"name_de": "Braunkohle",    "name_en": "Brown coal",  "deaths_per_twh": 32.72},
    "oil":           {"name_de": "Öl",            "name_en": "Oil",         "deaths_per_twh": 18.43},
    "biomass":       {"name_de": "Biomasse",      "name_en": "Biomass",     "deaths_per_twh": 4.63},
    "natural gas":   {"name_de": "Erdgas",         "name_en": "Gas",         "deaths_per_twh": 2.82},
    "hydropower":    {"name_de": "Wasserkraft",   "name_en": "Hydropower",  "deaths_per_twh": 1.30},
    "hydro":         {"name_de": "Wasserkraft",   "name_en": "Hydropower",  "deaths_per_twh": 1.30},
    "nuclear":       {"name_de": "Atomkraft",     "name_en": "Nuclear",     "deaths_per_twh": 0.03},
    "wind energy":   {"name_de": "Windenergie",   "name_en": "Wind",        "deaths_per_twh": 0.035},
    "solar energy":  {"name_de": "Solarenergie",  "name_en": "Solar",       "deaths_per_twh": 0.019},
}

# Keywords that indicate a safety/risk comparison claim
SAFETY_KEYWORDS = {
    "sicher", "gefährlich", "risiko", "tödlich", "todesfälle", "sterben",
    "unfall", "unfälle", "safe", "dangerous", "risk", "deadly", "deaths",
    "mortality", "accident", "fatalities", "sicherer", "gefährlicher",
}

# Full ranking for context (deduplicated, sorted by deaths/TWh desc)
FULL_RANKING = [
    ("Braunkohle",    32.72),
    ("Kohle",         24.62),
    ("Öl",            18.43),
    ("Biomasse",      4.63),
    ("Erdgas",        2.82),
    ("Wasserkraft",   1.30),
    ("Windenergie",   0.035),
    ("Atomkraft",     0.03),
    ("Solarenergie",  0.019),
]


def _is_energy_safety_claim(analysis: dict) -> bool:
    """Check if the claim is about energy safety or risk comparison."""
    claim = analysis.get("claim", "").lower()
    category = analysis.get("category", "")

    # Must mention at least one energy source
    has_energy = any(kw in claim for kw in ENERGY_DEATHS)
    if not has_energy:
        return False

    # Must mention safety/risk OR be a comparison
    has_safety = any(kw in claim for kw in SAFETY_KEYWORDS)
    has_comparison = any(w in claim for w in ("als", "versus", "vs", "compared", "safer", "sicherer"))

    return has_safety or has_comparison or category == "energy"


def _find_mentioned_sources(claim: str) -> list[dict]:
    """Find energy sources mentioned in the claim, deduplicated."""
    claim_lower = claim.lower()
    found = {}
    # Sort keywords longest-first to prefer specific matches
    for keyword in sorted(ENERGY_DEATHS.keys(), key=len, reverse=True):
        if keyword in claim_lower:
            entry = ENERGY_DEATHS[keyword]
            name = entry["name_en"]
            if name not in found:
                found[name] = entry
    return list(found.values())


async def search_energy_safety(analysis: dict) -> dict:
    """Return energy safety data for mentioned energy sources."""
    if not _is_energy_safety_claim(analysis):
        return {"source": "OWID Energy Safety", "results": []}

    claim = analysis.get("claim", "")
    mentioned = _find_mentioned_sources(claim)

    results = []

    # Add data for each mentioned energy source
    for entry in mentioned:
        results.append({
            "indicator_name": f"Todesfälle pro TWh: {entry['name_de']}",
            "description": f"{entry['name_de']} ({entry['name_en']}): {entry['deaths_per_twh']} Todesfälle pro TWh",
            "value": entry["deaths_per_twh"],
            "unit": "Todesfälle/TWh",
            "url": "https://ourworldindata.org/safest-sources-of-energy",
            "source_citation": "Markandya & Wilkinson (2007), Sovacool et al. (2016), OWID",
        })

    # Always add the full ranking for context
    ranking_text = " | ".join(f"{name}: {val}" for name, val in FULL_RANKING)
    results.append({
        "indicator_name": "Vollständiges Ranking: Todesfälle pro TWh",
        "description": ranking_text,
        "value": None,
        "unit": "Todesfälle/TWh",
        "url": "https://ourworldindata.org/grapher/death-rates-from-energy-production-per-twh",
        "source_citation": "Our World in Data (CC BY 4.0)",
    })

    logger.info(f"Energy Safety: {len(mentioned)} sources found in claim")
    return {
        "source": "OWID Energy Safety",
        "type": "official_data",
        "results": results,
    }
