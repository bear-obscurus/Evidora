import httpx
import logging

logger = logging.getLogger("evidora")

# Eurostat JSON API (reliable source for EU environmental data)
EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

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
    "schweiz": "CH", "switzerland": "CH",
    "europa": "EU27_2020", "eu": "EU27_2020", "europe": "EU27_2020",
}

# Datasets grouped by topic
DATASETS = {
    "ghg": {
        "keywords": ["treibhausgas", "greenhouse", "emission", "co2", "kohlendioxid",
                      "carbon", "klimawandel", "climate change", "erwärmung", "warming"],
        "dataset": "sdg_13_10",
        "label": "Treibhausgasemissionen (Index 1990=100)",
        "params": {},
        "unit": "Index (1990=100)",
        "url": "https://ec.europa.eu/eurostat/databrowser/view/sdg_13_10/default/table",
    },
    "air_emissions": {
        "keywords": ["luft", "air", "feinstaub", "pm10", "pm2.5", "stickstoff", "no2",
                      "stickoxid", "nox", "schwefeldioxid", "sox", "ammoniak",
                      "luftverschmutzung", "air pollution", "smog", "luftqualität", "air quality"],
        "dataset": "env_air_emis",
        "label": "Luftschadstoff-Emissionen",
        "params": {
            "airpol": ["NOX", "SOX", "PM2_5", "PM10", "NH3"],
            "src_nfr": ["NFR_TOT_NAT"],
            "unit": ["T"],
        },
        "unit": "Tonnen",
        "url": "https://ec.europa.eu/eurostat/databrowser/view/env_air_emis/default/table",
    },
    "pm25_deaths": {
        "keywords": ["feinstaub", "pm2.5", "luftverschmutzung", "air pollution",
                      "todesfälle", "deaths", "gesundheit", "health", "atemweg", "lunge"],
        "dataset": "sdg_11_52",
        "label": "Vorzeitige Todesfälle durch Feinstaub (PM2.5)",
        "params": {},
        "unit": "pro 100.000 Einwohner",
        "url": "https://ec.europa.eu/eurostat/databrowser/view/sdg_11_52/default/table",
    },
    "renewable": {
        "keywords": ["erneuerbar", "renewable", "solar", "wind", "photovoltaik",
                      "wasserkraft", "hydropower", "energiewende", "green energy",
                      "grüne energie", "ökostrom"],
        "dataset": "nrg_ind_ren",
        "label": "Anteil erneuerbarer Energien",
        "params": {"nrg_bal": ["REN"], "unit": ["PC"]},
        "unit": "%",
        "url": "https://ec.europa.eu/eurostat/databrowser/view/nrg_ind_ren/default/table",
    },
    "waste": {
        "keywords": ["müll", "abfall", "waste", "recycling", "plastik", "plastic",
                      "verpackung", "packaging", "deponie", "landfill"],
        "dataset": "env_wasgen",
        "label": "Abfallaufkommen",
        "params": {"waste": ["TOTAL"], "nace_r2": ["TOTAL_HH"], "unit": ["T"]},
        "unit": "Tonnen",
        "url": "https://ec.europa.eu/eurostat/databrowser/view/env_wasgen/default/table",
    },
}

# Pollutant display names
POLLUTANT_NAMES = {
    "NOX": "Stickoxide (NOₓ)",
    "SOX": "Schwefeldioxid (SOₓ)",
    "PM2_5": "Feinstaub (PM2.5)",
    "PM10": "Feinstaub (PM10)",
    "NH3": "Ammoniak (NH₃)",
    "CO": "Kohlenmonoxid (CO)",
}


def _find_country(analysis: dict) -> str | None:
    """Extract country code from entities."""
    entities = analysis.get("entities", [])
    # Also check the original claim
    claim = analysis.get("original_claim", "")
    all_text = " ".join(entities) + " " + claim

    for name, code in COUNTRY_CODES.items():
        if name in all_text.lower():
            return code
    return None


def _find_matching_datasets(analysis: dict) -> list[dict]:
    """Find matching datasets based on claim keywords."""
    entities = analysis.get("entities", [])
    subcategory = analysis.get("subcategory", "")
    category = analysis.get("category", "")
    claim = analysis.get("original_claim", "")
    search_text = " ".join([t.lower() for t in entities + [subcategory, category, claim]])

    matched = {}
    for key, ds in DATASETS.items():
        for kw in ds["keywords"]:
            if kw in search_text:
                if key not in matched:
                    matched[key] = ds
                break
    return list(matched.values())


def _parse_eurostat_response(data: dict, dataset_info: dict, country: str | None) -> list[dict]:
    """Parse Eurostat JSON-stat 2.0 response into result entries."""
    results = []

    dimensions = data.get("id", [])
    dim_data = data.get("dimension", {})
    sizes = data.get("size", [])
    values = data.get("value", {})

    if not values or not dimensions:
        return results

    # Build dimension label maps
    dim_labels = {}
    dim_indices = {}
    for dim_name in dimensions:
        dim = dim_data.get(dim_name, {})
        cat = dim.get("category", {})
        labels = cat.get("label", {})
        index = cat.get("index", {})
        dim_labels[dim_name] = labels
        dim_indices[dim_name] = {v: k for k, v in index.items()} if isinstance(index, dict) else {}

    # Calculate strides
    strides = []
    stride = 1
    for s in reversed(sizes):
        strides.insert(0, stride)
        stride *= s

    # Decode values
    for flat_idx_str, value in values.items():
        if value is None:
            continue
        flat_idx = int(flat_idx_str)

        # Decode each dimension
        entry = {}
        for i, dim_name in enumerate(dimensions):
            dim_idx = (flat_idx // strides[i]) % sizes[i]
            code = dim_indices.get(dim_name, {}).get(dim_idx, str(dim_idx))
            label = dim_labels.get(dim_name, {}).get(code, code)
            entry[dim_name] = {"code": code, "label": label}

        # Build display title
        geo_label = entry.get("geo", {}).get("label", "")
        time_label = entry.get("time", {}).get("label", "")
        airpol_code = entry.get("airpol", {}).get("code", "")
        airpol_label = POLLUTANT_NAMES.get(airpol_code, entry.get("airpol", {}).get("label", ""))

        if airpol_label:
            title = f"{dataset_info['label']}: {airpol_label} — {geo_label} {time_label} — {value:,.0f} {dataset_info['unit']}"
        else:
            title = f"{dataset_info['label']}: {geo_label} {time_label} — {value:,.1f} {dataset_info['unit']}"

        results.append({
            "title": title,
            "indicator": dataset_info["label"],
            "geo": geo_label,
            "time": time_label,
            "value": value,
            "unit": dataset_info["unit"],
            "source": "EEA / Eurostat",
            "url": dataset_info["url"],
        })

    # Sort by time descending, limit to most recent
    results.sort(key=lambda r: r.get("time", ""), reverse=True)
    return results[:10]


async def search_eea(analysis: dict) -> dict:
    """Search EEA environmental data via Eurostat API."""
    datasets = _find_matching_datasets(analysis)
    country = _find_country(analysis)

    # Default to GHG if no match but environment-related
    if not datasets:
        datasets = [DATASETS["ghg"]]

    all_results = []
    geo = country or "EU27_2020"

    async with httpx.AsyncClient(timeout=30.0) as client:
        for ds in datasets[:2]:
            try:
                params = {"geo": geo, "sinceTimePeriod": "2015"}

                # Add dataset-specific params
                for key, vals in ds.get("params", {}).items():
                    params[key] = vals

                # Also fetch EU average for comparison if country-specific
                geos = [geo]
                if geo != "EU27_2020":
                    geos.append("EU27_2020")
                params["geo"] = geos

                url = f"{EUROSTAT_BASE}/{ds['dataset']}"
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

                parsed = _parse_eurostat_response(data, ds, country)
                all_results.extend(parsed)

            except Exception as e:
                logger.warning(f"EEA/Eurostat query failed for {ds['label']}: {e}")
                all_results.append({
                    "title": f"{ds['label']}: Daten nicht verfügbar",
                    "indicator": ds["label"],
                    "value": "nicht verfügbar",
                    "source": "EEA / Eurostat",
                    "url": ds["url"],
                })

    # Add CO₂ multi-dimensional context caveat if GHG data was returned
    ghg_datasets = {"sdg_13_10"}
    if any(ds["dataset"] in ghg_datasets for ds in datasets[:2]) and all_results:
        all_results.append({
            "title": "WICHTIGER KONTEXT: Treibhausgasemissionen sind mehrdimensional",
            "indicator": "Methodische Einordnung",
            "geo": "",
            "time": "",
            "value": "",
            "unit": "",
            "source": "EEA / Eurostat",
            "url": "https://ec.europa.eu/eurostat/databrowser/view/sdg_13_10/default/table",
            "description": (
                "Der SDG-Indikator 13.10 zeigt Treibhausgasemissionen als Index (1990=100) — er misst "
                "den TREND relativ zum Basisjahr, nicht das absolute Niveau. Ein niedriger Indexwert "
                "bedeutet starke Reduktion seit 1990, sagt aber nichts über die Gesamtmenge aus. "
                "Weitere Einschränkungen: "
                "(1) Nur territoriale Emissionen — konsumbasierte Emissionen (importierte Güter) fehlen. "
                "(2) Pro-Kopf-Unterschiede — Länder mit hohem Basisjahr-Niveau und starker Reduktion "
                "können trotzdem höhere Pro-Kopf-Emissionen haben als Länder mit wenig Reduktion. "
                "(3) Wirtschaftsstruktur — Deindustrialisierung (z.B. Osteuropa nach 1990) senkt den "
                "Index ohne aktive Klimapolitik. "
                "(4) Historische Verantwortung — kumulierte Emissionen seit Industrialisierung werden "
                "nicht abgebildet."
            ),
        })

    return {
        "source": "European Environment Agency (EEA)",
        "type": "official_data",
        "results": all_results,
    }
