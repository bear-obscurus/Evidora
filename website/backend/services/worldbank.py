"""World Bank Open Data — global development indicators.

Covers GDP, poverty, unemployment, inflation, CO2 emissions, education,
health expenditure, inequality, and more. Free API, no key required.

API: https://api.worldbank.org/v2/
License: CC BY 4.0
"""

import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://api.worldbank.org/v2"

# Map keywords (DE + EN) to World Bank indicator codes
INDICATOR_MAP = {
    # GDP
    "bip": "NY.GDP.MKTP.CD",
    "bruttoinlandsprodukt": "NY.GDP.MKTP.CD",
    "gdp": "NY.GDP.MKTP.CD",
    "bip pro kopf": "NY.GDP.PCAP.CD",
    "gdp per capita": "NY.GDP.PCAP.CD",
    "wirtschaftswachstum": "NY.GDP.MKTP.KD.ZG",
    "gdp growth": "NY.GDP.MKTP.KD.ZG",
    # Population
    "bevölkerung": "SP.POP.TOTL",
    "einwohner": "SP.POP.TOTL",
    "population": "SP.POP.TOTL",
    "bevölkerungswachstum": "SP.POP.GROW",
    "population growth": "SP.POP.GROW",
    # Life expectancy
    "lebenserwartung": "SP.DYN.LE00.IN",
    "life expectancy": "SP.DYN.LE00.IN",
    # Unemployment
    "arbeitslosigkeit": "SL.UEM.TOTL.ZS",
    "arbeitslosenquote": "SL.UEM.TOTL.ZS",
    "arbeitslosenrate": "SL.UEM.TOTL.ZS",
    "unemployment": "SL.UEM.TOTL.ZS",
    "jugendarbeitslosigkeit": "SL.UEM.1524.ZS",
    "youth unemployment": "SL.UEM.1524.ZS",
    # Inflation
    "inflation": "FP.CPI.TOTL.ZG",
    "teuerung": "FP.CPI.TOTL.ZG",
    "verbraucherpreise": "FP.CPI.TOTL.ZG",
    # CO2
    "co2": "EN.ATM.CO2E.PC",
    "kohlendioxid": "EN.ATM.CO2E.PC",
    "carbon emissions": "EN.ATM.CO2E.PC",
    "co2 emissions": "EN.ATM.CO2E.PC",
    # Renewable energy
    "erneuerbare energie": "EG.FEC.RNEW.ZS",
    "renewable energy": "EG.FEC.RNEW.ZS",
    # Inequality
    "gini": "SI.POV.GINI",
    "ungleichheit": "SI.POV.GINI",
    "inequality": "SI.POV.GINI",
    # Poverty
    "armut": "SI.POV.DDAY",
    "poverty": "SI.POV.DDAY",
    # Education spending
    "bildungsausgaben": "SE.XPD.TOTL.GD.ZS",
    "education spending": "SE.XPD.TOTL.GD.ZS",
    "education expenditure": "SE.XPD.TOTL.GD.ZS",
    # Health expenditure
    "gesundheitsausgaben": "SH.XPD.CHEX.GD.ZS",
    "health expenditure": "SH.XPD.CHEX.GD.ZS",
    # Infant mortality
    "säuglingssterblichkeit": "SP.DYN.IMRT.IN",
    "kindersterblichkeit": "SP.DYN.IMRT.IN",
    "infant mortality": "SP.DYN.IMRT.IN",
    # Military spending
    "militärausgaben": "MS.MIL.XPND.GD.ZS",
    "verteidigungsausgaben": "MS.MIL.XPND.GD.ZS",
    "military spending": "MS.MIL.XPND.GD.ZS",
    "military expenditure": "MS.MIL.XPND.GD.ZS",
    # Government debt
    "staatsschulden": "GC.DOD.TOTL.GD.ZS",
    "staatsverschuldung": "GC.DOD.TOTL.GD.ZS",
    "government debt": "GC.DOD.TOTL.GD.ZS",
    # Internet
    "internetnutzung": "IT.NET.USER.ZS",
    "internet users": "IT.NET.USER.ZS",
    # Trade
    "handel": "NE.TRD.GNFS.ZS",
    "trade": "NE.TRD.GNFS.ZS",
    "außenhandel": "NE.TRD.GNFS.ZS",
    # Refugees
    "flüchtlinge": "SM.POP.REFG",
    "refugees": "SM.POP.REFG",
}

# Country name → ISO 3-letter code
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
    "vereinigtes königreich": "GBR", "uk": "GBR", "united kingdom": "GBR",
    "türkei": "TUR", "turkey": "TUR", "türkiye": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "australien": "AUS", "australia": "AUS",
    "kanada": "CAN", "canada": "CAN",
    "mexiko": "MEX", "mexico": "MEX",
    "südkorea": "KOR", "south korea": "KOR",
    "argentinien": "ARG", "argentina": "ARG",
    "südafrika": "ZAF", "south africa": "ZAF",
    "nigeria": "NGA",
    "ägypten": "EGY", "egypt": "EGY",
    "indonesien": "IDN", "indonesia": "IDN",
}

# Aggregate codes for comparison
EU_AGGREGATE = "EUU"


def _find_indicator(analysis: dict) -> str | None:
    """Find matching World Bank indicator from claim analysis.

    Sorts keywords longest-first so specific matches (e.g. "jugendarbeitslosigkeit")
    win over generic substrings (e.g. "arbeitslosigkeit").
    """
    keywords = analysis.get("spacy_keywords", [])
    entities = analysis.get("entities", [])
    claim = analysis.get("claim", "")
    factcheck_queries = analysis.get("factcheck_queries", [])
    subcategory = analysis.get("subcategory", "")
    search_terms = keywords + entities + [claim] + factcheck_queries + [subcategory]

    sorted_keywords = sorted(INDICATOR_MAP.keys(), key=len, reverse=True)
    for term in search_terms:
        term_lower = term.lower()
        for keyword in sorted_keywords:
            if keyword in term_lower:
                return INDICATOR_MAP[keyword]

    return None


def _find_countries(analysis: dict) -> list[str]:
    """Extract country codes from claim entities. Returns up to 3."""
    entities = analysis.get("entities", [])
    claim = analysis.get("claim", "")
    search_terms = entities + [claim]

    found = []
    seen = set()
    for term in search_terms:
        term_lower = term.lower()
        for name, code in COUNTRY_MAP.items():
            if name in term_lower and code not in seen:
                found.append(code)
                seen.add(code)
                if len(found) >= 3:
                    return found

    return found


async def search_worldbank(analysis: dict) -> dict:
    """Search the World Bank API for development indicators."""
    indicator = _find_indicator(analysis)
    if not indicator:
        return {"source": "World Bank", "type": "official_data", "results": []}

    countries = _find_countries(analysis)
    if not countries:
        # Default: Austria, Germany, EU aggregate
        countries = ["AUT", "DEU"]
        include_eu = True
    else:
        include_eu = False

    # Build country string (semicolon-separated)
    country_str = ";".join(countries)
    if include_eu:
        country_str += f";{EU_AGGREGATE}"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{BASE_URL}/country/{country_str}/indicator/{indicator}",
                params={
                    "format": "json",
                    "per_page": 100,
                    "date": "2015:2025",
                },
            )
            if resp.status_code == 429:
                logger.warning("World Bank API rate limit reached")
                return {"source": "World Bank", "results": []}
            resp.raise_for_status()
            data = resp.json()

        # Response is [metadata, entries] — handle empty or error responses
        if not isinstance(data, list) or len(data) < 2 or not data[1]:
            return {"source": "World Bank", "type": "official_data", "results": []}

        entries = data[1]
        indicator_name = entries[0].get("indicator", {}).get("value", indicator) if entries else indicator

        # Get latest non-null value per country
        results = []
        seen_countries = set()
        for entry in entries:
            if entry.get("value") is None:
                continue
            country_code = entry.get("countryiso3code", "")
            if country_code in seen_countries:
                continue
            seen_countries.add(country_code)

            country_name = entry.get("country", {}).get("value", country_code)
            year = entry.get("date", "")
            value = entry.get("value")

            # Format large numbers
            if isinstance(value, (int, float)):
                if abs(value) >= 1_000_000_000:
                    display_value = f"{value / 1_000_000_000:.1f} Mrd."
                elif abs(value) >= 1_000_000:
                    display_value = f"{value / 1_000_000:.1f} Mio."
                elif abs(value) >= 1000 and value == int(value):
                    display_value = f"{int(value):,}".replace(",", ".")
                else:
                    display_value = f"{value:.2f}"
            else:
                display_value = str(value)

            results.append({
                "indicator_name": f"{indicator_name}: {country_name} ({year})",
                "indicator": indicator,
                "country": country_code,
                "country_name": country_name,
                "year": year,
                "value": value,
                "display_value": display_value,
                "url": f"https://data.worldbank.org/indicator/{indicator}?locations={country_code}",
            })

            if len(results) >= 5:
                break

        # Add GDP/economy multi-dimensional context caveat
        gdp_indicators = {"NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "NY.GDP.MKTP.KD.ZG"}
        if indicator in gdp_indicators and results:
            results.append({
                "indicator_name": "WICHTIGER KONTEXT: BIP ist kein umfassendes Wohlstandsmaß",
                "indicator": "context",
                "country": "",
                "country_name": "",
                "year": "",
                "value": "",
                "display_value": "",
                "url": "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD",
                "description": (
                    "Das BIP misst die Wirtschaftsleistung, nicht den Wohlstand. "
                    "Einschränkungen: "
                    "(1) Nominell vs. KKP — nominale US-Dollar-Werte ignorieren Preisniveauunterschiede; "
                    "Kaufkraftparität (KKP) ist für Lebensstandard-Vergleiche aussagekräftiger. "
                    "(2) Verteilung — hohes BIP pro Kopf bei hoher Ungleichheit (Gini) bedeutet, dass "
                    "der Wohlstand bei wenigen konzentriert ist. "
                    "(3) Informelle Wirtschaft — in Entwicklungsländern macht der informelle Sektor "
                    "bis zu 60 % der Wirtschaftsleistung aus und wird im BIP nur geschätzt. "
                    "(4) Bevölkerungsgröße — Gesamtwirtschaft (MKTP.CD) und Pro-Kopf-Leistung (PCAP.CD) "
                    "erzählen sehr unterschiedliche Geschichten. China hat das zweitgrößte BIP, "
                    "liegt aber pro Kopf auf Platz ~70. "
                    "(5) Nachhaltigkeit — BIP-Wachstum auf Kosten von Umwelt oder Staatsverschuldung "
                    "ist langfristig kein Wohlstandsgewinn."
                ),
            })

        # Add migration multi-dimensional context caveat
        migration_indicators = {"SM.POP.REFG"}
        if indicator in migration_indicators and results:
            results.append({
                "indicator_name": "WICHTIGER KONTEXT: Flüchtlingszahlen sind mehrdimensional",
                "indicator": "context",
                "country": "",
                "country_name": "",
                "year": "",
                "value": "",
                "display_value": "",
                "url": "https://data.worldbank.org/indicator/SM.POP.REFG",
                "description": (
                    "Der World-Bank-Indikator SM.POP.REFG zählt die Gesamtzahl der Flüchtlinge "
                    "nach Herkunftsland. Einschränkungen: "
                    "(1) Nur anerkannte Flüchtlinge — Asylsuchende, Binnenvertriebene (IDPs) und "
                    "irreguläre Migration fehlen. "
                    "(2) Absolut vs. Pro-Kopf — kleine Aufnahmeländer (Libanon, Jordanien) tragen "
                    "relativ zur Bevölkerung eine viel höhere Last als große Länder. "
                    "(3) Herkunft vs. Aufnahme — dieser Indikator zählt nach Herkunftsland; für "
                    "Aufnahmezahlen ist SM.POP.REFG.OR aussagekräftiger. "
                    "(4) Keine Integration — Beschäftigung, Bildungszugang und soziale Teilhabe "
                    "erfordern andere Datenquellen."
                ),
            })

        # Add CO₂ multi-dimensional context caveat
        co2_indicators = {"EN.ATM.CO2E.PC"}
        if indicator in co2_indicators and results:
            results.append({
                "indicator_name": "WICHTIGER KONTEXT: CO₂ pro Kopf ist nur eine Dimension",
                "indicator": "context",
                "country": "",
                "country_name": "",
                "year": "",
                "value": "",
                "display_value": "",
                "url": "https://data.worldbank.org/indicator/EN.ATM.CO2E.PC",
                "description": (
                    "CO₂-Emissionen pro Kopf (World Bank EN.ATM.CO2E.PC) messen nur die "
                    "territorialen, produktionsbasierten Emissionen geteilt durch die Bevölkerung. "
                    "Sie erfassen NICHT: "
                    "(1) Konsumbasierte Emissionen — in importierten Gütern enthaltenes CO₂ wird dem "
                    "Produktionsland zugerechnet, nicht dem Konsumland. "
                    "(2) Absolute Emissionen — bevölkerungsreiche Länder (China, Indien) haben niedrige "
                    "Pro-Kopf-Werte, aber die weltweit höchsten Absolutemissionen. "
                    "(3) Historische Kumulativ-Emissionen — die USA und EU tragen die größte historische "
                    "Verantwortung, auch wenn aktuelle Pro-Kopf-Werte sinken. "
                    "(4) Methan & andere Treibhausgase — dieser Indikator erfasst nur CO₂, nicht CH₄, "
                    "N₂O oder F-Gase. Für Gesamtvergleiche sind CO₂-Äquivalente aussagekräftiger. "
                    "(5) Datenverzögerung — World-Bank-Emissionsdaten haben typisch 2–3 Jahre Verzug."
                ),
            })

        logger.info(f"World Bank: {len(results)} results for {indicator} ({country_str})")
        return {"source": "World Bank", "type": "official_data", "results": results}

    except httpx.HTTPStatusError as e:
        logger.warning(f"World Bank API error: {e.response.status_code}")
    except Exception as e:
        logger.warning(f"World Bank search failed: {e}")

    return {"source": "World Bank", "type": "official_data", "results": []}
