"""Multi-dimensional energy safety data — comprehensive comparison of energy sources.

Static dataset combining multiple established sources:
- Deaths per TWh: OWID / Markandya & Wilkinson (2007, Lancet), Sovacool et al. (2016)
- CO2 lifecycle emissions: IPCC AR5 (2014), median values
- Land use: UNECE (2022) lifecycle assessment
- Radioactive waste: IAEA, World Nuclear Association
- Catastrophe potential & decommissioning: historical data, IAEA

Triggered for claims comparing energy source safety, risk, or environmental impact.
Source: https://ourworldindata.org/safest-sources-of-energy
License: CC BY 4.0 (OWID data), public domain (IPCC/UNECE/IAEA)
"""

import logging

logger = logging.getLogger("evidora")

# Comprehensive energy source profiles
ENERGY_PROFILES = {
    "Brown coal": {
        "name_de": "Braunkohle",
        "deaths_per_twh": 32.72,
        "co2_g_per_kwh": 1054,
        "land_km2_per_twh": 11.0,
        "radioactive_waste": "Nein",
        "catastrophe_potential": "Niedrig (lokal: Grubenunglücke, Luftverschmutzung)",
        "decommission_years": "2–5 (Tagebau-Rekultivierung: Jahrzehnte)",
        "capacity_factor_pct": 50,
    },
    "Coal": {
        "name_de": "Kohle",
        "deaths_per_twh": 24.62,
        "co2_g_per_kwh": 820,
        "land_km2_per_twh": 7.0,
        "radioactive_waste": "Nein",
        "catastrophe_potential": "Niedrig (lokal: Grubenunglücke, Luftverschmutzung)",
        "decommission_years": "2–5",
        "capacity_factor_pct": 55,
    },
    "Oil": {
        "name_de": "Öl",
        "deaths_per_twh": 18.43,
        "co2_g_per_kwh": 720,
        "land_km2_per_twh": 5.0,
        "radioactive_waste": "Nein",
        "catastrophe_potential": "Mittel (Ölpest, Raffineriebrände, z.B. Deepwater Horizon)",
        "decommission_years": "1–3",
        "capacity_factor_pct": 50,
    },
    "Biomass": {
        "name_de": "Biomasse",
        "deaths_per_twh": 4.63,
        "co2_g_per_kwh": 230,
        "land_km2_per_twh": 460.0,
        "radioactive_waste": "Nein",
        "catastrophe_potential": "Sehr niedrig",
        "decommission_years": "1–2",
        "capacity_factor_pct": 60,
    },
    "Gas": {
        "name_de": "Erdgas",
        "deaths_per_twh": 2.82,
        "co2_g_per_kwh": 490,
        "land_km2_per_twh": 2.4,
        "radioactive_waste": "Nein",
        "catastrophe_potential": "Mittel (Gasexplosionen, Methan-Leckagen)",
        "decommission_years": "1–3",
        "capacity_factor_pct": 55,
    },
    "Hydropower": {
        "name_de": "Wasserkraft",
        "deaths_per_twh": 1.30,
        "co2_g_per_kwh": 24,
        "land_km2_per_twh": 18.0,
        "radioactive_waste": "Nein",
        "catastrophe_potential": "Hoch (Dammbruch: Banqiao 1975 — ca. 170.000 Tote)",
        "decommission_years": "5–10 (Damm-Rückbau komplex)",
        "capacity_factor_pct": 45,
    },
    "Wind": {
        "name_de": "Windenergie",
        "deaths_per_twh": 0.035,
        "co2_g_per_kwh": 11,
        "land_km2_per_twh": 72.0,
        "radioactive_waste": "Nein",
        "catastrophe_potential": "Sehr niedrig (einzelne Arbeitsunfälle)",
        "decommission_years": "Wochen bis Monate, vollständig recyclebar",
        "capacity_factor_pct": 30,
    },
    "Nuclear": {
        "name_de": "Atomkraft",
        "deaths_per_twh": 0.03,
        "co2_g_per_kwh": 12,
        "land_km2_per_twh": 0.3,
        "radioactive_waste": "Ja — ca. 3 m³ hochaktiver Abfall/TWh, Halbwertszeit bis 24.000 Jahre (Plutonium-239), Endlagerung ungelöst",
        "catastrophe_potential": (
            "Sehr hoch — Historische Katastrophen: "
            "(1) Tschernobyl 1986: 31 akute Strahlentote, WHO schätzt 4.000–9.000 Langzeit-Krebstote, "
            "IPPNW/Greenpeace bis 93.000; 350.000 Evakuierte, 2.600 km² Sperrzone bis heute unbewohnbar. "
            "(2) Fukushima 2011: 1 bestätigter Strahlentod, ca. 2.300 evakuierungsbedingte Todesfälle "
            "(Stress, Suizid, unterbrochene medizinische Versorgung), 154.000 Evakuierte, "
            "Dekontamination dauert Jahrzehnte, Kosten >200 Mrd. USD (Japan Center for Economic Research). "
            "(3) Kyshtym/Majak 1957: 200+ akute Tote (geschätzt), 10.000 Evakuierte, Kontamination bis heute. "
            "Ein einzelner GAU kann ganze Regionen über Jahrzehnte unbewohnbar machen."
        ),
        "decommission_years": "15–20 Jahre, Kosten: 500 Mio.–1 Mrd. € pro Reaktor",
        "capacity_factor_pct": 90,
    },
    "Solar": {
        "name_de": "Solarenergie",
        "deaths_per_twh": 0.019,
        "co2_g_per_kwh": 41,
        "land_km2_per_twh": 37.0,
        "radioactive_waste": "Nein",
        "catastrophe_potential": "Sehr niedrig (Einzelunfälle bei Installation)",
        "decommission_years": "Wochen, Recycling-Quote >90%",
        "capacity_factor_pct": 15,
    },
}

# Keyword → profile name mapping (DE + EN)
KEYWORD_MAP = {
    "braunkohle": "Brown coal", "brown coal": "Brown coal",
    "kohle": "Coal", "coal": "Coal",
    "öl": "Oil", "oil": "Oil", "erdöl": "Oil",
    "biomasse": "Biomass", "biomass": "Biomass",
    "gas": "Gas", "erdgas": "Gas", "natural gas": "Gas",
    "wasserkraft": "Hydropower", "hydropower": "Hydropower", "hydro": "Hydropower",
    "staudamm": "Hydropower",
    "wind": "Wind", "windenergie": "Wind", "windkraft": "Wind",
    "wind energy": "Wind",
    "atom": "Nuclear", "atomkraft": "Nuclear", "kernkraft": "Nuclear",
    "kernenergie": "Nuclear", "nuklear": "Nuclear", "nuclear": "Nuclear",
    "akw": "Nuclear",
    "solar": "Solar", "solarenergie": "Solar", "photovoltaik": "Solar",
    "solar energy": "Solar", "pv": "Solar",
}

# Keywords that indicate a safety/risk/comparison claim
SAFETY_KEYWORDS = {
    "sicher", "gefährlich", "risiko", "tödlich", "todesfälle", "sterben",
    "unfall", "unfälle", "safe", "dangerous", "risk", "deadly", "deaths",
    "mortality", "accident", "fatalities", "sicherer", "gefährlicher",
    "umwelt", "klima", "emissionen", "co2", "abfall", "müll",
    "environment", "climate", "emissions", "waste",
}


def _is_energy_safety_claim(analysis: dict) -> bool:
    """Check if the claim is about energy safety, risk, or environmental comparison."""
    claim = analysis.get("claim", "").lower()
    category = analysis.get("category", "")

    has_energy = any(kw in claim for kw in KEYWORD_MAP)
    if not has_energy:
        return False

    has_safety = any(kw in claim for kw in SAFETY_KEYWORDS)
    has_comparison = any(w in claim for w in ("als", "versus", "vs", "compared", "safer", "sicherer"))

    return has_safety or has_comparison or category == "energy"


def _find_mentioned_sources(claim: str) -> list[dict]:
    """Find energy sources mentioned in the claim, deduplicated."""
    claim_lower = claim.lower()
    found = {}
    for keyword in sorted(KEYWORD_MAP.keys(), key=len, reverse=True):
        if keyword in claim_lower:
            profile_name = KEYWORD_MAP[keyword]
            if profile_name not in found:
                found[profile_name] = ENERGY_PROFILES[profile_name]
    return list(found.items())


def _format_profile(name_en: str, profile: dict) -> str:
    """Format a full energy source profile as readable text."""
    return (
        f"{profile['name_de']} ({name_en}): "
        f"Todesfälle: {profile['deaths_per_twh']}/TWh, "
        f"CO₂-Lifecycle: {profile['co2_g_per_kwh']} g/kWh, "
        f"Landverbrauch: {profile['land_km2_per_twh']} km²/TWh, "
        f"Radioaktiver Abfall: {profile['radioactive_waste']}, "
        f"Katastrophenpotential: {profile['catastrophe_potential']}, "
        f"Rückbau: {profile['decommission_years']}"
    )


async def search_energy_safety(analysis: dict) -> dict:
    """Return multi-dimensional energy safety data for mentioned energy sources."""
    if not _is_energy_safety_claim(analysis):
        return {"source": "OWID Energy Safety", "results": []}

    claim = analysis.get("claim", "")
    mentioned = _find_mentioned_sources(claim)

    results = []

    # Add full profile for each mentioned energy source
    for name_en, profile in mentioned:
        results.append({
            "indicator_name": f"Energieprofil: {profile['name_de']}",
            "description": _format_profile(name_en, profile),
            "deaths_per_twh": profile["deaths_per_twh"],
            "co2_g_per_kwh": profile["co2_g_per_kwh"],
            "land_km2_per_twh": profile["land_km2_per_twh"],
            "radioactive_waste": profile["radioactive_waste"],
            "catastrophe_potential": profile["catastrophe_potential"],
            "decommission": profile["decommission_years"],
            "url": "https://ourworldindata.org/safest-sources-of-energy",
            "source_citation": "OWID, IPCC AR5, UNECE 2022, IAEA",
        })

    # Add comparative context
    ranking_deaths = " > ".join(
        f"{p['name_de']} ({p['deaths_per_twh']})"
        for _, p in sorted(ENERGY_PROFILES.items(), key=lambda x: -x[1]["deaths_per_twh"])
    )
    ranking_co2 = " > ".join(
        f"{p['name_de']} ({p['co2_g_per_kwh']}g)"
        for _, p in sorted(ENERGY_PROFILES.items(), key=lambda x: -x[1]["co2_g_per_kwh"])
    )

    results.append({
        "indicator_name": "Ranking: Todesfälle pro TWh (höchste zuerst)",
        "description": ranking_deaths,
        "url": "https://ourworldindata.org/grapher/death-rates-from-energy-production-per-twh",
        "source_citation": "OWID (CC BY 4.0), Markandya & Wilkinson (2007, Lancet)",
    })
    results.append({
        "indicator_name": "Ranking: CO₂-Lifecycle-Emissionen g/kWh (höchste zuerst)",
        "description": ranking_co2,
        "url": "https://www.ipcc.ch/report/ar5/wg3/",
        "source_citation": "IPCC AR5 (2014), Median-Werte",
    })
    results.append({
        "indicator_name": "WICHTIGER KONTEXT: 'Sicherheit' ist mehrdimensional",
        "description": (
            "Todesfälle pro TWh messen nur die direkte Mortalität im Normal- und Störbetrieb. "
            "Sie erfassen NICHT: "
            "(1) Katastrophenpotential — Tschernobyl 1986: 31 akute Tote + 4.000–93.000 geschätzte "
            "Langzeit-Krebstote (WHO vs. IPPNW), 2.600 km² Sperrzone bis heute; Fukushima 2011: "
            "1 Strahlentod + 2.300 evakuierungsbedingte Tode, Kosten >200 Mrd. USD; "
            "Kyshtym 1957: 200+ Tote, 10.000 Evakuierte. "
            "(2) Radioaktiven Abfall — hochaktiver Atommüll muss über Hunderttausende Jahre sicher "
            "gelagert werden, ein Endlager existiert weltweit noch nicht (Ausnahme: Onkalo/Finnland im Bau). "
            "(3) Proliferationsrisiko — Dual-Use-Potential für Kernwaffen. "
            "(4) Rückbaukosten — 500 Mio. bis 1 Mrd. € pro Reaktor, 15–20 Jahre Dauer. "
            "Ein vollständiger Sicherheitsvergleich muss alle Dimensionen berücksichtigen."
        ),
        "url": "https://unece.org/sed/documents/2021/10/reports/life-cycle-assessment-electricity-generation-options",
        "source_citation": "UNECE 2022 LCA, IAEA, World Nuclear Association",
    })

    logger.info(f"Energy Safety: {len(mentioned)} sources found, multi-dimensional profile")
    return {
        "source": "OWID Energy Safety",
        "type": "official_data",
        "results": results,
    }
