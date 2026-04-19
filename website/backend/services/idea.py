"""IDEA Voter Turnout — Wahlbeteiligung (International IDEA).

Datenquelle: International Institute for Democracy and Electoral Assistance
(International IDEA, Stockholm), Voter Turnout Database.
Zugriff via Our World in Data (OWID) Grapher CSV-Endpunkte, die auf dem
offiziellen IDEA-Datensatz basieren und regelmäßig aktualisiert werden.

Zwei Kennzahlen pro Parlamentswahl:
- Voter turnout of registered voters (% der registrierten Wähler:innen)
- Voter turnout of voting-age population (% der wahlberechtigten Bevölkerung)

Die beiden Metriken unterscheiden sich erheblich, insbesondere dort, wo die
Wahlregistrierung unvollständig ist (z.B. USA): "Registered voters" misst
die Mobilisierung registrierter Personen, "voting-age population" die
Gesamtmobilisierung der Stimmberechtigten.

Lizenz: CC BY 4.0 (International IDEA), via OWID Attribution.
Zitation: International IDEA, Voter Turnout Database (abgerufen via OWID).

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren Wahlbeteiligungs-Daten, bewerten sie nicht politisch.
- Keine Prognose zukünftiger Wahlbeteiligungen.
- Caveat zur Methodik (VAP vs. registrierte Wähler) ist Pflicht.
"""

import csv
import io
import logging
import time

import httpx

logger = logging.getLogger("evidora")

# --- OWID Grapher CSV URLs (gespeist aus IDEA Voter Turnout Database) ---
IDEA_REGISTERED_URL = (
    "https://ourworldindata.org/grapher/voter-turnout-of-registered-voters.csv"
)
IDEA_VAP_URL = (
    "https://ourworldindata.org/grapher/voter-turnout-of-voting-age-population.csv"
)

IDEA_CACHE_TTL = 86400  # 24h

# Cache structure: {iso3: {year: {reg, vap, entity}}}
_idea_cache: dict | None = None
_idea_cache_time: float = 0.0

# Keywords die IDEA-Lookup auslösen (DE + EN)
IDEA_KEYWORDS = [
    # Wahlbeteiligung
    "wahlbeteiligung", "wahlbeteiligungen",
    "voter turnout", "voter turn-out", "turnout", "electoral turnout",
    "abstimmungsbeteiligung",
    # Nichtwähler
    "nichtwähler", "nichtwählerinnen", "nicht-wähler",
    "non-voter", "non-voters", "nonvoter",
    # Mobilisierung
    "wähler mobilisierung", "wählermobilisierung",
    "mobilisation of voters", "voter mobilization", "voter mobilisation",
    # Spezifisch
    "urnengang", "gang zur urne", "gang an die urne",
    # IDEA selbst
    "international idea", "voter turnout database",
    # Parlamentswahlen (Kontext)
    "nationalratswahl", "bundestagswahl", "parlamentswahl",
    "nationalratswahlen", "bundestagswahlen", "parlamentswahlen",
    "general election", "general elections",
    "parliamentary election", "parliamentary elections",
    "legislative election", "legislative elections",
]

# Country name → ISO 3-letter code (fokussiert auf EU + wichtigste Länder)
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
    "malta": "MLT",
    "zypern": "CYP", "cyprus": "CYP",
    "vereinigtes königreich": "GBR", "united kingdom": "GBR", "großbritannien": "GBR",
    "türkei": "TUR", "turkey": "TUR", "türkiye": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "kanada": "CAN", "canada": "CAN",
    "australien": "AUS", "australia": "AUS",
    "neuseeland": "NZL", "new zealand": "NZL",
    "japan": "JPN",
    "südkorea": "KOR", "south korea": "KOR",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "mexiko": "MEX", "mexico": "MEX",
    "argentinien": "ARG", "argentina": "ARG",
    "südafrika": "ZAF", "south africa": "ZAF",
}


async def fetch_idea(client: httpx.AsyncClient | None = None) -> dict:
    """Download both IDEA voter-turnout datasets and merge into a unified cache.

    Returns {iso3: {year: {reg, vap, entity}}} where ``reg`` is turnout of
    registered voters in % and ``vap`` is turnout of voting-age population in %.
    Years are **election years** only (sparse — typical democracies hold a
    parliamentary election every 3–5 years).
    """
    global _idea_cache, _idea_cache_time

    now = time.time()
    if _idea_cache is not None and (now - _idea_cache_time) < IDEA_CACHE_TTL:
        return _idea_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=60.0)
        close_client = True

    merged: dict = {}

    try:
        for url, field, col_name in [
            (IDEA_REGISTERED_URL, "reg", "Voter turnout of registered voters"),
            (IDEA_VAP_URL, "vap", "Voter turnout of voting-age population"),
        ]:
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                reader = csv.DictReader(io.StringIO(resp.text))
                for row in reader:
                    code = (row.get("Code") or "").strip()
                    entity = (row.get("Entity") or "").strip()
                    year_raw = (row.get("Year") or "").strip()
                    val_raw = (row.get(col_name) or "").strip()
                    if not code or not year_raw or not val_raw:
                        continue
                    try:
                        year = int(year_raw)
                        val = float(val_raw)
                    except ValueError:
                        continue
                    merged.setdefault(code, {}).setdefault(year, {"entity": entity})[field] = val
            except Exception as e:
                logger.warning(f"IDEA: failed to fetch {col_name}: {e}")
                # Weiter mit dem anderen Dataset

        _idea_cache = merged
        _idea_cache_time = now
        total_points = sum(len(y) for y in merged.values())
        logger.info(f"IDEA: {len(merged)} countries, {total_points} election-years cached")
        return merged
    finally:
        if close_client:
            await client.aclose()


def _find_countries(analysis: dict, max_n: int = 3) -> list[str]:
    """Extract ISO3 country codes from the claim. Defaults to Austria+Germany."""
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


def _claim_mentions_idea(claim: str) -> bool:
    """Check if claim mentions voter-turnout-relevant keywords."""
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in IDEA_KEYWORDS)


def _trend_arrow(delta: float) -> str:
    if delta > 2.0:
        return "↑"
    if delta < -2.0:
        return "↓"
    return "→"


async def search_idea(analysis: dict) -> dict:
    """Search IDEA cache for voter-turnout data for countries in the claim."""
    if not _claim_mentions_idea(analysis.get("claim", "")):
        return {"source": "IDEA Voter Turnout", "type": "official_data", "results": []}

    data = await fetch_idea()
    if not data:
        return {"source": "IDEA Voter Turnout", "type": "official_data", "results": []}

    countries = _find_countries(analysis)
    if not countries:
        # Generischer Wahlbeteiligungs-Claim ohne Länderbezug: AT + DE als Default
        countries = ["AUT", "DEU"]

    results: list[dict] = []
    for code in countries:
        country_data = data.get(code)
        if not country_data:
            continue

        # Election years sortiert (aufsteigend)
        years = sorted(country_data.keys())
        if not years:
            continue

        latest_year = years[-1]
        entry = country_data[latest_year]
        entity = entry.get("entity", code)
        reg = entry.get("reg")
        vap = entry.get("vap")

        # Baue Anzeige-Text
        parts = []
        if reg is not None:
            parts.append(f"registrierte Wähler: {reg:.1f}%")
        if vap is not None:
            parts.append(f"wahlberechtigte Bev.: {vap:.1f}%")
        if not parts:
            continue

        # Trend über die letzten 4 Wahlen (falls verfügbar) — wir nehmen reg
        # bevorzugt, sonst vap. So zeigen wir die Richtung über ca. 15–20 Jahre.
        primary_field = "reg" if reg is not None else "vap"
        series = [
            (y, country_data[y].get(primary_field))
            for y in years
            if country_data[y].get(primary_field) is not None
        ]
        trend_note = ""
        if len(series) >= 4:
            current = series[-1][1]
            baseline = series[-4][1]
            delta = current - baseline
            arrow = _trend_arrow(delta)
            baseline_year = series[-4][0]
            trend_note = (
                f" | Trend ({baseline_year}→{latest_year}): {arrow} ({delta:+.1f} pp)"
            )

        # NATO-style-Referenz: falls EU-Kontext, Delta zum EU-Median des
        # entsprechenden Jahres wäre nett — bewusst weggelassen in v1,
        # da Wahlbeteiligung pro Land andere Wahltermine hat (kein
        # sinnvoller EU-Ø pro Jahr).

        display_value = f"{reg:.1f}%" if reg is not None else f"{vap:.1f}%"
        results.append({
            "indicator_name": (
                f"IDEA Wahlbeteiligung {entity} ({latest_year}): "
                + " | ".join(parts)
                + trend_note
            ),
            "indicator": "idea_voter_turnout",
            "country": code,
            "country_name": entity,
            "year": str(latest_year),
            "value": reg if reg is not None else vap,
            "display_value": display_value,
            "url": "https://www.idea.int/data-tools/data/voter-turnout-database",
        })

    # Methodik-Caveat anfügen
    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: IDEA misst Parlamentswahl-Beteiligung",
            "indicator": "context",
            "country": "",
            "country_name": "",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://www.idea.int/data-tools/data/voter-turnout-database",
            "description": (
                "International IDEA (Stockholm) sammelt Wahlbeteiligungs-Daten für Parlamentswahlen "
                "weltweit und publiziert zwei Metriken pro Wahl: "
                "(1) Anteil der registrierten Wähler:innen, die gewählt haben (üblicher Wert in "
                "Medien und Wahlkampagnen), und "
                "(2) Anteil der wahlberechtigten Bevölkerung (\"voting-age population\", VAP) — "
                "ein internationaler Vergleichsmaßstab. "
                "Einschränkungen: "
                "(a) Die beiden Werte können stark abweichen, wo die Wahlregistrierung unvollständig "
                "ist — in den USA etwa liegt der VAP-Wert meist 10–20 Punkte unter dem "
                "Registered-Wert. In Ländern mit automatischer Wahlregistrierung (AT, DE, FR) ist "
                "der Unterschied gering. "
                "(b) Daten betreffen nur Parlamentswahlen (Unterhaus). Präsidentschafts- oder "
                "Regionalwahlen sind nicht enthalten. "
                "(c) Referenzjahr = Wahljahr. Jahre ohne Wahl haben keinen Datenpunkt; die IDEA-"
                "Zeitreihe ist naturgemäß lückenhaft."
            ),
        })

    logger.info(
        f"IDEA: {len(results) - (1 if results else 0)} country results, countries={countries}"
    )
    return {"source": "IDEA Voter Turnout", "type": "official_data", "results": results}
