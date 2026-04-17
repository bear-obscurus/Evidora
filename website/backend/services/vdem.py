"""V-Dem (Varieties of Democracy) — Demokratie-Qualität weltweit.

Datenquelle: V-Dem Institute (Universität Göteborg), Version 16 (2026).
Zugriff via Our World in Data (OWID) Grapher CSV-Endpunkte, die
täglich aus dem offiziellen V-Dem R-Paket aktualisiert werden.

Drei Hauptindizes (Skala 0.0–1.0, 0 = keine Demokratie, 1 = volle Demokratie):
- Liberal Democracy Index (v2x_libdem) — Freiheitsrechte, Rechtsstaat, Gewaltenteilung
- Electoral Democracy Index (v2x_polyarchy) — freie, faire Wahlen
- Participatory Democracy Index (v2x_partipdem) — aktive Bürgerbeteiligung

Lizenz: CC BY 4.0 (V-Dem Institute) + Attribution via OWID.
Zitation: Coppedge et al. "V-Dem Dataset v16", V-Dem Institute, 2026.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren V-Dem-Scores, wir berechnen sie nicht selbst.
- Wir nehmen keine eigene Partei- oder Politiker-Bewertung vor.
- Caveat zur Methodik (Experten-Befragung) ist Pflicht.
"""

import csv
import io
import logging
import time

import httpx

logger = logging.getLogger("evidora")

# --- OWID Grapher CSV URLs (auto-updated from V-Dem v16) ---
VDEM_LIBDEM_URL = "https://ourworldindata.org/grapher/liberal-democracy-index.csv"
VDEM_ELECDEM_URL = "https://ourworldindata.org/grapher/electoral-democracy-index.csv"
VDEM_PARTIPDEM_URL = "https://ourworldindata.org/grapher/participatory-democracy-index.csv"

VDEM_CACHE_TTL = 86400  # 24h

# Cache structure: {iso3: {year: {libdem, elecdem, partipdem, entity}}}
_vdem_cache: dict | None = None
_vdem_cache_time: float = 0.0

# Keywords that trigger V-Dem lookup (DE + EN)
VDEM_KEYWORDS = [
    # Demokratie/Autokratie
    "demokratie", "demokratisch", "demokratieabbau", "demokratieindex",
    "democracy", "democratic", "democracy index",
    "autokratie", "autokratisch", "autocracy", "autocratic",
    "diktatur", "dictatorship",
    "autoritär", "autoritarismus", "authoritarian", "authoritarianism",
    "illiberal", "illiberal democracy",
    # Rechtsstaat/Gewaltenteilung
    "rechtsstaat", "rechtsstaatlichkeit", "rule of law",
    "gewaltenteilung", "separation of powers",
    "checks and balances",
    # Freiheit
    "meinungsfreiheit", "freedom of expression",
    "versammlungsfreiheit", "freedom of assembly",
    "freie wahlen", "free elections", "fair elections",
    "wahlintegrität", "electoral integrity",
    "freiheitsrechte", "civil liberties",
    "bürgerrechte",
    # Spezifisch
    "v-dem", "vdem", "varieties of democracy",
]

# Country name → ISO 3-letter code (bewusst fokussiert auf EU + wichtigste Länder)
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
    "vereinigtes königreich": "GBR", "united kingdom": "GBR", "uk": "GBR", "großbritannien": "GBR",
    "türkei": "TUR", "turkey": "TUR", "türkiye": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "belarus": "BLR", "weißrussland": "BLR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "südkorea": "KOR", "south korea": "KOR",
    "nordkorea": "PRK", "north korea": "PRK",
    "iran": "IRN",
    "israel": "ISR",
    "ägypten": "EGY", "egypt": "EGY",
    "saudi-arabien": "SAU", "saudi arabia": "SAU",
    "venezuela": "VEN",
    "kuba": "CUB", "cuba": "CUB",
}


async def fetch_vdem(client: httpx.AsyncClient | None = None) -> dict:
    """Download and merge the three V-Dem indices into a unified cache.

    Returns {iso3: {year: {libdem, elecdem, partipdem, entity}}}
    """
    global _vdem_cache, _vdem_cache_time

    now = time.time()
    if _vdem_cache is not None and (now - _vdem_cache_time) < VDEM_CACHE_TTL:
        return _vdem_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=60.0)
        close_client = True

    merged: dict = {}

    try:
        for url, field, col_name in [
            (VDEM_LIBDEM_URL, "libdem", "Liberal Democracy Index"),
            (VDEM_ELECDEM_URL, "elecdem", "Electoral Democracy Index"),
            (VDEM_PARTIPDEM_URL, "partipdem", "Participatory Democracy Index"),
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
                logger.warning(f"V-Dem: failed to fetch {col_name}: {e}")
                # Continue with other indices

        _vdem_cache = merged
        _vdem_cache_time = now
        total_points = sum(len(y) for y in merged.values())
        logger.info(f"V-Dem: {len(merged)} countries, {total_points} country-years cached")
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


def _claim_mentions_vdem(claim: str) -> bool:
    """Check if claim mentions V-Dem-relevant keywords."""
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in VDEM_KEYWORDS)


async def search_vdem(analysis: dict) -> dict:
    """Search V-Dem cache for democracy indices for countries in the claim."""
    if not _claim_mentions_vdem(analysis.get("claim", "")):
        return {"source": "V-Dem", "type": "official_data", "results": []}

    data = await fetch_vdem()
    if not data:
        return {"source": "V-Dem", "type": "official_data", "results": []}

    countries = _find_countries(analysis)
    if not countries:
        # Default context for generic democracy claims: Austria + Germany
        countries = ["AUT", "DEU"]

    results: list[dict] = []
    for code in countries:
        country_data = data.get(code)
        if not country_data:
            continue
        # Latest year available
        latest_year = max(country_data.keys())
        entry = country_data[latest_year]
        entity = entry.get("entity", code)

        # Format scores (show all 3 if available)
        parts = []
        if "libdem" in entry:
            parts.append(f"Liberale Demokratie: {entry['libdem']:.3f}")
        if "elecdem" in entry:
            parts.append(f"Elektorale Demokratie: {entry['elecdem']:.3f}")
        if "partipdem" in entry:
            parts.append(f"Partizipative Demokratie: {entry['partipdem']:.3f}")

        if not parts:
            continue

        # Trend over last 10 years (libdem) — only if we have 10+ data points
        trend_note = ""
        libdem_series = [
            (y, country_data[y].get("libdem"))
            for y in sorted(country_data.keys())
            if country_data[y].get("libdem") is not None
        ]
        if len(libdem_series) >= 10:
            recent = libdem_series[-1][1]
            ten_years_ago = next(
                (v for y, v in libdem_series if y == latest_year - 10),
                libdem_series[-10][1] if len(libdem_series) >= 10 else None,
            )
            if ten_years_ago is not None:
                delta = recent - ten_years_ago
                trend_arrow = "↑" if delta > 0.02 else ("↓" if delta < -0.02 else "→")
                trend_note = f" | Trend 10J: {trend_arrow} ({delta:+.3f})"

        results.append({
            "indicator_name": f"V-Dem {entity} ({latest_year}): " + " | ".join(parts) + trend_note,
            "indicator": "vdem_democracy_indices",
            "country": code,
            "country_name": entity,
            "year": str(latest_year),
            "value": entry.get("libdem"),
            "display_value": f"{entry.get('libdem', 0):.3f}" if "libdem" in entry else "",
            "url": f"https://v-dem.net/data/the-v-dem-dataset/",
        })

    # Add methodology caveat when we have results
    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: V-Dem misst Demokratiequalität",
            "indicator": "context",
            "country": "",
            "country_name": "",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://v-dem.net/about/what-is-v-dem/",
            "description": (
                "Das V-Dem-Institut (Universität Göteborg) misst Demokratiequalität auf einer "
                "Skala von 0 (keine Demokratie) bis 1 (volle Demokratie). Die Scores basieren auf "
                "strukturierten Befragungen von ~4.000 Länderexpert:innen weltweit und werden "
                "statistisch modelliert (Bayesian IRT), um subjektive Verzerrungen zu reduzieren. "
                "Einschränkungen: "
                "(1) Expert:innen-Urteile — kein objektiver Messwert wie BIP, sondern "
                "aggregiertes Expertenurteil, das leicht verzögert auf Veränderungen reagiert. "
                "(2) Indexaufbau — die drei Hauptindizes gewichten unterschiedliche Aspekte: "
                "Liberale Demokratie (Freiheits- und Minderheitenrechte), Elektorale Demokratie "
                "(nur Wahlintegrität), Partizipative Demokratie (aktive Bürgerbeteiligung). "
                "Ein Land kann elektoral demokratisch, aber liberal schwächer sein. "
                "(3) Referenzjahr — Daten beziehen sich auf das Ende des berichteten Jahres. "
                "Jüngste Ereignisse (Regierungswechsel, Verfassungsänderungen) fließen erst in "
                "die Folgeversion ein."
            ),
        })

    logger.info(f"V-Dem: {len(results) - (1 if results else 0)} country results, countries={countries}")
    return {"source": "V-Dem", "type": "official_data", "results": results}
