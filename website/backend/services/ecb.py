import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://data-api.ecb.europa.eu/service/data"

# Map keywords (DE + EN) to ECB series keys
SERIES_MAP = {
    # Key interest rates
    "leitzins": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "key interest rate": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "zinsen": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "interest rate": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "einlagezins": {
        "series": "FM/B.U2.EUR.4F.KR.DFR.LEV",
        "label": "EZB-Einlagefazilität",
        "label_en": "ECB Deposit Facility Rate",
        "unit": "%",
    },
    "deposit rate": {
        "series": "FM/B.U2.EUR.4F.KR.DFR.LEV",
        "label": "EZB-Einlagefazilität",
        "label_en": "ECB Deposit Facility Rate",
        "unit": "%",
    },
    # Exchange rates
    "wechselkurs": {
        "series": "EXR/D.USD.EUR.SP00.A",
        "label": "EUR/USD Wechselkurs",
        "label_en": "EUR/USD Exchange Rate",
        "unit": "USD",
    },
    "exchange rate": {
        "series": "EXR/D.USD.EUR.SP00.A",
        "label": "EUR/USD Wechselkurs",
        "label_en": "EUR/USD Exchange Rate",
        "unit": "USD",
    },
    "dollar": {
        "series": "EXR/D.USD.EUR.SP00.A",
        "label": "EUR/USD Wechselkurs",
        "label_en": "EUR/USD Exchange Rate",
        "unit": "USD",
    },
    "euro": {
        "series": "EXR/D.USD.EUR.SP00.A",
        "label": "EUR/USD Wechselkurs",
        "label_en": "EUR/USD Exchange Rate",
        "unit": "USD",
    },
    "franken": {
        "series": "EXR/D.CHF.EUR.SP00.A",
        "label": "EUR/CHF Wechselkurs",
        "label_en": "EUR/CHF Exchange Rate",
        "unit": "CHF",
    },
    "pfund": {
        "series": "EXR/D.GBP.EUR.SP00.A",
        "label": "EUR/GBP Wechselkurs",
        "label_en": "EUR/GBP Exchange Rate",
        "unit": "GBP",
    },
    "yen": {
        "series": "EXR/D.JPY.EUR.SP00.A",
        "label": "EUR/JPY Wechselkurs",
        "label_en": "EUR/JPY Exchange Rate",
        "unit": "JPY",
    },
    # Money supply
    "geldmenge": {
        "series": "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "label": "Geldmenge M3 (Euroraum)",
        "label_en": "Money Supply M3 (Euro Area)",
        "unit": "EUR Mio.",
    },
    "money supply": {
        "series": "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "label": "Geldmenge M3 (Euroraum)",
        "label_en": "Money Supply M3 (Euro Area)",
        "unit": "EUR Mio.",
    },
    "geld drucken": {
        "series": "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "label": "Geldmenge M3 (Euroraum)",
        "label_en": "Money Supply M3 (Euro Area)",
        "unit": "EUR Mio.",
    },
    "money printing": {
        "series": "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "label": "Geldmenge M3 (Euroraum)",
        "label_en": "Money Supply M3 (Euro Area)",
        "unit": "EUR Mio.",
    },
    # HICP Inflation (ECB perspective)
    "ezb inflation": {
        "series": "ICP/M.U2.N.000000.4.ANR",
        "label": "HVPI-Inflationsrate (Euroraum)",
        "label_en": "HICP Inflation Rate (Euro Area)",
        "unit": "%",
    },
    "ecb inflation": {
        "series": "ICP/M.U2.N.000000.4.ANR",
        "label": "HVPI-Inflationsrate (Euroraum)",
        "label_en": "HICP Inflation Rate (Euro Area)",
        "unit": "%",
    },
    "geldpolitik": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "monetary policy": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
}


def _find_series(claim: str) -> list[dict]:
    """Find matching ECB series based on keywords in the claim."""
    claim_lower = claim.lower()
    found = {}
    for keyword, series_info in SERIES_MAP.items():
        if keyword in claim_lower:
            series_key = series_info["series"]
            if series_key not in found:
                found[series_key] = series_info
    return list(found.values())


def _parse_sdmx_json(data: dict, series_info: dict) -> list[dict]:
    """Parse SDMX-JSON response and extract observations."""
    results = []
    try:
        datasets = data.get("dataSets", [])
        if not datasets:
            return results

        # Get time dimension values
        dimensions = data.get("structure", {}).get("dimensions", {})
        obs_dimensions = dimensions.get("observation", [])
        time_dim = None
        for dim in obs_dimensions:
            if dim.get("id") == "TIME_PERIOD":
                time_dim = dim.get("values", [])
                break

        if not time_dim:
            return results

        # Get series observations
        series_data = datasets[0].get("series", {})
        for series_key, series_obj in series_data.items():
            observations = series_obj.get("observations", {})
            # Get last 6 observations
            obs_indices = sorted(observations.keys(), key=lambda x: int(x))
            recent = obs_indices[-6:] if len(obs_indices) > 6 else obs_indices

            for idx in recent:
                obs_values = observations[idx]
                value = obs_values[0] if obs_values else None
                if value is None:
                    continue

                time_idx = int(idx)
                time_val = time_dim[time_idx]["id"] if time_idx < len(time_dim) else "?"

                # Format value based on unit
                if series_info["unit"] in ("%",):
                    formatted = f"{value:.2f} {series_info['unit']}"
                elif series_info["unit"] == "EUR Mio.":
                    formatted = f"{value:,.0f} {series_info['unit']}"
                else:
                    formatted = f"{value:.4f} {series_info['unit']}"

                results.append({
                    "title": f"{series_info['label']}: {time_val} — {formatted}",
                    "indicator": series_info["label"],
                    "period": time_val,
                    "value": value,
                    "unit": series_info["unit"],
                    "url": f"https://data.ecb.europa.eu/data/datasets/{series_info['series'].split('/')[0]}",
                })
    except Exception as e:
        logger.error(f"ECB JSON parse error: {e}")

    return results


async def search_ecb(analysis: dict) -> dict:
    """Search ECB Statistical Data Warehouse for relevant economic data."""
    claim = analysis.get("claim", "")
    keywords = analysis.get("entities", [])
    search_text = f"{claim} {' '.join(keywords)}".lower()

    # Find matching series from keywords
    matching = _find_series(search_text)

    if not matching:
        # Default: try key interest rate for general economy claims
        matching = [SERIES_MAP["leitzins"]]

    results = []

    async with httpx.AsyncClient(timeout=20.0) as client:
        for series_info in matching[:3]:  # Max 3 series per request
            series_path = series_info["series"]
            url = f"{BASE_URL}/{series_path}"
            params = {
                "lastNObservations": "6",
                "format": "jsondata",
                "detail": "dataonly",
            }

            try:
                resp = await client.get(url, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    parsed = _parse_sdmx_json(data, series_info)
                    results.extend(parsed)
                else:
                    logger.warning(f"ECB API error {resp.status_code} for {series_path}")
            except Exception as e:
                logger.error(f"ECB request failed for {series_path}: {e}")

    return {
        "source": "EZB (Europäische Zentralbank)",
        "type": "official_data",
        "results": results,
    }
