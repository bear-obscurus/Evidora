"""SIPRI — Military Expenditure Database.

Datenquelle: Stockholm International Peace Research Institute (SIPRI), jährlich.
Zugriff via Our World in Data (OWID) Grapher-CSVs, die die offiziellen
SIPRI-Veröffentlichungen mirrorn (jährliches Update ~Ende April).

Drei Kennzahlen werden kombiniert:
1. Absolute Militärausgaben (konstante US-Dollar, Basisjahr 2023)
2. Anteil am Bruttoinlandsprodukt (% BIP)
3. Anteil an den Staatsausgaben (% öffentlicher Haushalt)

Abdeckung: ~170 Länder, 1949–aktuelles Jahr. Update April 2026.
Lizenz: SIPRI stellt die Daten kostenlos zur Verfügung (Attribution Pflicht).
Zitation: SIPRI Military Expenditure Database {Jahr}.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren SIPRI-Werte, bewerten keine Verteidigungspolitik.
- Der NATO-2%-Richtwert wird als Kontext genannt, nicht als Urteil.
- Caveat zur Methodik (was zählt zu Militärausgaben?) ist Pflicht.
"""

import csv
import io
import logging
import time

import httpx

logger = logging.getLogger("evidora")

SIPRI_TOTAL_URL = "https://ourworldindata.org/grapher/military-spending-sipri.csv"
SIPRI_GDP_URL = "https://ourworldindata.org/grapher/military-spending-as-a-share-of-gdp-sipri.csv"
SIPRI_GOVT_URL = "https://ourworldindata.org/grapher/military-expenditure-as-a-share-of-government-spending.csv"

SIPRI_CACHE_TTL = 86400  # 24h

# Cache structure: {iso3: {year: {expenditure_usd, gdp_share, govt_share, entity}}}
_sipri_cache: dict | None = None
_sipri_cache_time: float = 0.0

# Trigger-Keywords (DE + EN)
SIPRI_KEYWORDS = [
    "militär", "militaer", "militärausgaben", "militaerausgaben",
    "military", "military spending", "military expenditure", "military budget",
    "verteidigung", "verteidigungsausgaben", "verteidigungshaushalt", "verteidigungsbudget",
    "defence", "defense", "defence spending", "defense spending",
    "defence budget", "defense budget",
    "rüstung", "ruestung", "rüstungsausgaben", "ruestungsausgaben",
    "armament", "armaments", "arms budget",
    "nato", "nato-ziel", "nato ziel", "nato target",
    "2-prozent-ziel", "2%-ziel", "2% ziel", "zwei-prozent-ziel",
    "2 prozent vom bip", "2% vom bip", "2% of gdp", "two percent of gdp",
    "sipri",
    "streitkräfte", "streitkraefte", "armed forces",
    "bundeswehr", "bundesheer",
]

# Country-Map (ISO3, gleiche Konvention wie V-Dem/CPI/RSF)
COUNTRY_MAP = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "schweiz": "CHE", "switzerland": "CHE",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czech republic": "CZE", "czechia": "CZE",
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
    "türkei": "TUR", "turkey": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "belarus": "BLR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA", "amerika": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "südkorea": "KOR", "south korea": "KOR",
    "nordkorea": "PRK", "north korea": "PRK",
    "iran": "IRN",
    "israel": "ISR",
    "saudi-arabien": "SAU", "saudi arabia": "SAU",
    "kanada": "CAN", "canada": "CAN",
    "australien": "AUS", "australia": "AUS",
}

# NATO-Mitgliedsstaaten (für 2%-Ziel-Kontext)
NATO_MEMBERS = {
    "USA", "GBR", "FRA", "DEU", "ITA", "CAN", "ESP", "NLD", "POL", "TUR",
    "BEL", "DNK", "NOR", "PRT", "GRC", "LUX", "ISL", "CZE", "HUN", "SVK",
    "SVN", "BGR", "ROU", "HRV", "ALB", "MNE", "MKD", "EST", "LVA", "LTU",
    "FIN", "SWE",
}


async def _fetch_one(client: httpx.AsyncClient, url: str, value_column_prefix: str) -> dict:
    """Fetch one SIPRI CSV and return {iso3: {year: (value, entity)}}."""
    out: dict = {}
    resp = await client.get(url)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    # Value column name varies per file; find it by prefix
    fieldnames = reader.fieldnames or []
    value_col = next((c for c in fieldnames if c.startswith(value_column_prefix)), None)
    if not value_col:
        logger.warning(f"SIPRI: no column starting with {value_column_prefix!r} in {url}")
        return out
    for row in reader:
        code = (row.get("Code") or "").strip()
        entity = (row.get("Entity") or "").strip()
        year_raw = (row.get("Year") or "").strip()
        val_raw = (row.get(value_col) or "").strip()
        if not code or not year_raw or not val_raw:
            continue
        try:
            year = int(year_raw)
            value = float(val_raw)
        except ValueError:
            continue
        out.setdefault(code, {})[year] = (value, entity)
    return out


async def fetch_sipri(client: httpx.AsyncClient | None = None) -> dict:
    """Download and merge SIPRI military expenditure data (3 indicators)."""
    global _sipri_cache, _sipri_cache_time

    now = time.time()
    if _sipri_cache is not None and (now - _sipri_cache_time) < SIPRI_CACHE_TTL:
        return _sipri_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    try:
        totals = await _fetch_one(client, SIPRI_TOTAL_URL, "Military expenditure")
        gdp_shares = await _fetch_one(client, SIPRI_GDP_URL, "Military expenditure (% of GDP)")
        govt_shares = await _fetch_one(client, SIPRI_GOVT_URL, "Military expenditure (% of government spending)")

        merged: dict = {}
        all_codes = set(totals) | set(gdp_shares) | set(govt_shares)
        for code in all_codes:
            years = set(totals.get(code, {})) | set(gdp_shares.get(code, {})) | set(govt_shares.get(code, {}))
            country_entries: dict = {}
            for year in years:
                t_entry = totals.get(code, {}).get(year)
                g_entry = gdp_shares.get(code, {}).get(year)
                s_entry = govt_shares.get(code, {}).get(year)
                entity = (t_entry or g_entry or s_entry or (None, ""))[1]
                country_entries[year] = {
                    "expenditure_usd": t_entry[0] if t_entry else None,
                    "gdp_share": g_entry[0] if g_entry else None,
                    "govt_share": s_entry[0] if s_entry else None,
                    "entity": entity,
                }
            if country_entries:
                merged[code] = country_entries

        _sipri_cache = merged
        _sipri_cache_time = now
        total_points = sum(len(y) for y in merged.values())
        logger.info(f"SIPRI: {len(merged)} countries, {total_points} country-years cached")
        return merged
    finally:
        if close_client:
            await client.aclose()


def _find_countries(analysis: dict, max_n: int = 3) -> list[str]:
    """Extract ISO3 country codes from claim (NER-prioritized)."""
    ner_countries = analysis.get("ner_entities", {}).get("countries", [])
    claim = analysis.get("claim", "")
    search_terms = ner_countries + [claim]

    found: list[str] = []
    seen: set[str] = set()
    for term in search_terms:
        term_lower = term.lower()
        for name, code in COUNTRY_MAP.items():
            if name in term_lower and code not in seen:
                found.append(code)
                seen.add(code)
                if len(found) >= max_n:
                    return found
    return found


def _claim_mentions_sipri(claim: str) -> bool:
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in SIPRI_KEYWORDS)


def _format_usd(value: float) -> str:
    """Format absolute USD value (e.g. 86_301_290_000 → '86,3 Mrd. USD')."""
    if value >= 1e12:
        return f"{value / 1e12:.2f}".replace(".", ",") + " Bio. USD"
    if value >= 1e9:
        return f"{value / 1e9:.1f}".replace(".", ",") + " Mrd. USD"
    if value >= 1e6:
        return f"{value / 1e6:.0f} Mio. USD"
    return f"{value:.0f} USD"


async def search_sipri(analysis: dict) -> dict:
    """Search SIPRI cache for military expenditure data."""
    if not _claim_mentions_sipri(analysis.get("claim", "")):
        return {"source": "SIPRI", "type": "official_data", "results": []}

    data = await fetch_sipri()
    if not data:
        return {"source": "SIPRI", "type": "official_data", "results": []}

    countries = _find_countries(analysis)
    if not countries:
        countries = ["AUT", "DEU"]  # Default-Kontext

    results: list[dict] = []
    for code in countries:
        country_data = data.get(code)
        if not country_data:
            continue

        # Neuestes Jahr mit mindestens einem Wert
        latest_year = max(country_data.keys())
        entry = country_data[latest_year]
        entity = entry.get("entity") or code

        exp_usd = entry.get("expenditure_usd")
        gdp_share = entry.get("gdp_share")
        govt_share = entry.get("govt_share")

        # Trend absolute Ausgaben 10 Jahre
        trend_abs = ""
        ten_years_ago_year = latest_year - 10
        prev = country_data.get(ten_years_ago_year)
        if prev and exp_usd is not None and prev.get("expenditure_usd") is not None:
            ratio = exp_usd / prev["expenditure_usd"]
            if ratio > 1.05:
                trend_abs = f" | 10J: ↑ ×{ratio:.2f}"
            elif ratio < 0.95:
                trend_abs = f" | 10J: ↓ ×{ratio:.2f}"
            else:
                trend_abs = f" | 10J: → ×{ratio:.2f}"

        # Primärzeile: BIP-Anteil (meistens relevanteste Kennzahl)
        parts = []
        if exp_usd is not None:
            parts.append(_format_usd(exp_usd))
        if gdp_share is not None:
            parts.append(f"{gdp_share:.2f}% BIP".replace(".", ","))
        if govt_share is not None:
            parts.append(f"{govt_share:.1f}% Staatsausgaben".replace(".", ","))

        # NATO-2%-Ziel-Kontext (neutral als Referenzwert, kein Urteil)
        nato_note = ""
        if code in NATO_MEMBERS and gdp_share is not None:
            if gdp_share >= 2.0:
                nato_note = f" | NATO-2%-Richtwert: erreicht ({gdp_share:.2f}%)".replace(".", ",")
            else:
                nato_note = f" | NATO-2%-Richtwert: nicht erreicht ({gdp_share:.2f}%)".replace(".", ",")

        summary = f"SIPRI {entity} ({latest_year}): " + " | ".join(parts) + trend_abs + nato_note

        results.append({
            "indicator_name": summary,
            "indicator": "sipri_military",
            "country": code,
            "country_name": entity,
            "year": str(latest_year),
            "value": exp_usd if exp_usd is not None else 0,
            "display_value": _format_usd(exp_usd) if exp_usd is not None else "",
            "url": "https://www.sipri.org/databases/milex",
        })

    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: SIPRI-Methodik und NATO-2%-Ziel",
            "indicator": "context",
            "country": "",
            "country_name": "",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://www.sipri.org/databases/milex/sources-and-methods",
            "description": (
                "Die SIPRI Military Expenditure Database erfasst jährliche Militärausgaben von ~170 "
                "Staaten seit 1949. Absolutwerte sind in konstanten US-Dollar (Basisjahr 2023) angegeben — "
                "damit inflations- und wechselkursbereinigt, aber nicht kaufkraftparitätisch. "
                "Einschränkungen: "
                "(1) Was als 'Militärausgabe' zählt, variiert national — SIPRI harmonisiert, kann aber "
                "Posten wie Veteranen, paramilitärische Einheiten oder Forschung je nach Land unterschiedlich "
                "erfassen. "
                "(2) Für Länder mit intransparenten Haushalten (z. B. China, Russland, Nordkorea) arbeitet "
                "SIPRI mit Schätzungen, die von offiziellen Zahlen abweichen können. "
                "(3) Der NATO-2%-Richtwert ist eine politische Zielvorgabe für Mitgliedsstaaten, keine "
                "rechtliche Verpflichtung — das Erreichen oder Verfehlen ist kein Faktencheck-Urteil, "
                "sondern eine Referenzgröße. "
                "(4) Kaufkraft-Vergleiche zwischen Ländern sind mit Vorsicht zu interpretieren — 1 Mrd. USD "
                "kauft in unterschiedlichen Volkswirtschaften unterschiedlich viel Militärkapazität."
            ),
        })

    logger.info(f"SIPRI: {len(results) - (1 if results else 0)} country results, countries={countries}")
    return {"source": "SIPRI", "type": "official_data", "results": results}
