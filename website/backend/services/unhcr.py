import httpx
import logging
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BASE_URL = "https://api.unhcr.org/population/v1"

# Map country keywords to ISO3 codes
COUNTRY_MAP = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "griechenland": "GRC", "greece": "GRC",
    "ungarn": "HUN", "hungary": "HUN",
    "polen": "POL", "poland": "POL",
    "schweden": "SWE", "sweden": "SWE",
    "schweiz": "CHE", "switzerland": "CHE",
    "türkei": "TUR", "turkey": "TUR", "türkiye": "TUR",
    "syrien": "SYR", "syria": "SYR",
    "ukraine": "UKR",
    "afghanistan": "AFG",
    "irak": "IRQ", "iraq": "IRQ",
    "iran": "IRN",
    "somalia": "SOM",
    "eritrea": "ERI",
    "sudan": "SDN",
    "nigeria": "NGA",
    "venezuela": "VEN",
    "myanmar": "MMR",
}

# Keywords that indicate origin country (not asylum country)
ORIGIN_KEYWORDS = [
    "aus ", "from ", "herkunft", "origin", "fliehen", "flüchten",
    "kommen aus", "stammen aus", "coming from",
]


def _detect_countries(text: str) -> tuple[str | None, str | None]:
    """Detect country of asylum (coa) and country of origin (coo) from text."""
    text_lower = text.lower()
    coa = None
    coo = None

    # Check if text mentions origin context
    is_origin_context = any(kw in text_lower for kw in ORIGIN_KEYWORDS)

    for keyword, iso3 in COUNTRY_MAP.items():
        if keyword in text_lower:
            # European countries are likely asylum countries
            # Non-European countries are likely origin countries
            european = iso3 in ("AUT", "DEU", "FRA", "ITA", "ESP", "GRC", "HUN",
                                "POL", "SWE", "CHE", "TUR")
            if european and not is_origin_context:
                if not coa:
                    coa = iso3
            else:
                if not coo:
                    coo = iso3

    return coa, coo


async def search_unhcr(analysis: dict) -> dict:
    """Search UNHCR Refugee Data for population and asylum statistics."""
    claim = analysis.get("claim", "")
    # Use NER countries + claim text (NOT flat entities — may contain LLM hallucinations)
    ner_countries = analysis.get("ner_entities", {}).get("countries", [])
    search_text = f"{claim} {' '.join(ner_countries)}".lower()

    coa, coo = _detect_countries(search_text)

    results = []

    async with polite_client(timeout=20.0) as client:
        # 1. Population data (refugees, asylum seekers)
        pop_params = {
            "yearFrom": "2019",
            "yearTo": "2024",
            "cf_type": "ISO",
            "limit": "100",
        }
        if coa:
            pop_params["coa"] = coa
        if coo:
            pop_params["coo"] = coo

        # If no specific country detected, get global totals
        if not coa and not coo:
            pop_params["limit"] = "20"

        try:
            resp = await client.get(f"{BASE_URL}/population/", params=pop_params)
            if resp.status_code == 200:
                data = resp.json()

                # Aggregate by year
                yearly = {}
                for item in data.get("items", []):
                    year = item.get("year")
                    if year not in yearly:
                        yearly[year] = {
                            "refugees": 0,
                            "asylum_seekers": 0,
                            "idps": 0,
                            "coa": item.get("coa_name", ""),
                            "coo": item.get("coo_name", ""),
                        }
                    yearly[year]["refugees"] += item.get("refugees", 0) or 0
                    yearly[year]["asylum_seekers"] += item.get("asylum_seekers", 0) or 0
                    yearly[year]["idps"] += item.get("idps", 0) or 0

                for year in sorted(yearly.keys(), reverse=True)[:5]:
                    y = yearly[year]
                    total = y["refugees"] + y["asylum_seekers"]

                    location = ""
                    if coa:
                        location = f" in {y['coa']}" if y["coa"] else ""
                    if coo:
                        location = f" aus {y['coo']}" if y["coo"] else ""

                    parts = []
                    if y["refugees"]:
                        parts.append(f"{y['refugees']:,} Flüchtlinge")
                    if y["asylum_seekers"]:
                        parts.append(f"{y['asylum_seekers']:,} Asylsuchende")
                    if y["idps"]:
                        parts.append(f"{y['idps']:,} Binnenvertriebene")

                    detail = ", ".join(parts) if parts else f"{total:,} Personen"

                    results.append({
                        "title": f"UNHCR {year}{location}: {detail}",
                        "indicator": "UNHCR Refugee Population",
                        "year": year,
                        "refugees": y["refugees"],
                        "asylum_seekers": y["asylum_seekers"],
                        "url": "https://www.unhcr.org/refugee-statistics/",
                    })

        except Exception as e:
            logger.error(f"UNHCR population request failed: {e}")

        # 2. Asylum applications (if asylum country detected)
        if coa:
            try:
                app_params = {
                    "yearFrom": "2019",
                    "yearTo": "2024",
                    "coa": coa,
                    "cf_type": "ISO",
                    "limit": "100",
                }
                resp = await client.get(f"{BASE_URL}/asylum-applications/", params=app_params)
                if resp.status_code == 200:
                    data = resp.json()

                    # Aggregate applications by year
                    yearly_apps = {}
                    for item in data.get("items", []):
                        year = item.get("year")
                        if year not in yearly_apps:
                            yearly_apps[year] = 0
                        yearly_apps[year] += item.get("applied", 0) or 0

                    coa_name = ""
                    if data.get("items"):
                        coa_name = data["items"][0].get("coa_name", "")

                    for year in sorted(yearly_apps.keys(), reverse=True)[:5]:
                        count = yearly_apps[year]
                        if count > 0:
                            results.append({
                                "title": f"UNHCR Asylanträge {coa_name} {year}: {count:,}",
                                "indicator": "UNHCR Asylum Applications",
                                "year": year,
                                "applications": count,
                                "url": "https://www.unhcr.org/refugee-statistics/",
                            })

            except Exception as e:
                logger.error(f"UNHCR asylum applications request failed: {e}")

    # Add multi-dimensional context caveat for migration/refugee data
    if results:
        results.append({
            "title": "WICHTIGER KONTEXT: Flüchtlings- und Migrationsdaten sind mehrdimensional",
            "indicator": "Methodische Einordnung",
            "year": "",
            "url": "https://www.unhcr.org/refugee-statistics/methodology/",
            "description": (
                "UNHCR-Flüchtlingsstatistiken erfassen anerkannte Flüchtlinge, Asylsuchende und "
                "Binnenvertriebene (IDPs). Sie bilden aber nur einen Ausschnitt des Migrationsgeschehens ab. "
                "Einschränkungen: "
                "(1) Absolute vs. Pro-Kopf-Zahlen — große Länder (Türkei, Deutschland) haben hohe "
                "Absolutzahlen, aber gemessen an der Bevölkerung liegt z.B. Libanon (1 Flüchtling pro "
                "4 Einwohner) weit vorne. "
                "(2) Nur Fluchtmigration — Arbeitsmigration, Familiennachzug und EU-Binnenmobilität "
                "(~80 % der Zuwanderung in viele EU-Länder) sind nicht enthalten. "
                "(3) Anerkennungsquoten — hohe Antragszahlen sagen nichts über die Schutzquote aus; "
                "diese variiert stark nach Herkunftsland und Aufnahmeland. "
                "(4) Irreguläre Migration — nicht registrierte Personen fehlen in den Statistiken. "
                "(5) Wirtschaftliche und soziale Integration — Beschäftigungsquoten, Bildungsteilhabe "
                "und fiskalische Effekte erfordern andere Datenquellen (z.B. OECD, Eurostat LFS). "
                "(6) Push- vs. Pull-Faktoren — Zahlen erklären nicht die Ursachen (Krieg, Klima, "
                "wirtschaftliche Not) und sind ohne diesen Kontext leicht politisch instrumentalisierbar."
            ),
        })

    return {
        "source": "UNHCR (UN-Flüchtlingshilfswerk)",
        "type": "official_data",
        "results": results,
    }
