"""CDC Open Data API — strukturierte US-Public-Health-Datasets via Socrata-Catalog.

Anders als CDC Newsroom (RSS-Feed mit aktuellen Statements) bietet CDC
Open Data Zugriff auf ~3000 strukturierte Datasets:
- Vaccination rates (state-level, age-stratified)
- Disease surveillance (NNDSS — National Notifiable Diseases Surveillance)
- Mortality data (CDC WONDER, leading causes of death)
- BRFSS (Behavioral Risk Factor Surveillance — chronic disease, prevention)
- Wastewater surveillance (COVID, influenza, RSV)
- ARDI (Alcohol-Related Disease Impact)

Connector durchsucht den Socrata-Catalog (https://api.us.socrata.com/api/catalog/v1
mit domains=data.cdc.gov) nach Claim-Entities/Queries und gibt die top
Dataset-Beschreibungen + Link zurück. Komplementär zu CDC Newsroom: wo
Newsroom News-Items liefert, liefert Open Data verifizierbare Datenpunkte.

Kein API-Key nötig (Public Catalog).

Trigger: pubmed_queries oder factcheck_queries oder Entities (>=1
englisches medizinisches/Public-Health-Term). Helper-Funktion
_relevant_query_for_cdc filtert auf englische Public-Health-Queries.

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle, kein Konsens-Pack).
"""

import logging
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

CATALOG_URL = "https://api.us.socrata.com/api/catalog/v1"
DOMAIN = "data.cdc.gov"

# Public-Health-Begriffe, bei denen die Suche relevante Datasets findet.
# Wenn eine Query (factcheck/pubmed) eines dieser Tokens enthält, wird
# CDC Open Data konsultiert. Andere Queries (z.B. zu Recht/Wirtschaft)
# werden übersprungen, um Catalog-API-Last zu reduzieren.
_PUBLIC_HEALTH_MARKERS = (
    "disease", "vaccine", "vaccination", "outbreak", "epidemic", "pandemic",
    "mortality", "death rate", "morbidity", "infection", "infectious",
    "cancer", "diabetes", "obesity", "smoking", "tobacco", "alcohol",
    "drug overdose", "opioid", "suicide", "injury", "homicide",
    "asthma", "hypertension", "stroke", "heart disease", "cardiovascular",
    "hiv", "aids", "tuberculosis", "malaria", "measles", "influenza",
    "covid", "sars-cov", "long covid", "mpox", "h5n1", "rsv",
    "salmonella", "norovirus", "ebola", "polio", "rabies",
    "wastewater", "surveillance", "brfss", "nndss", "cdc wonder",
    "preventive", "screening", "mammography", "colonoscopy",
    "maternal", "infant mortality", "low birth weight",
    "lead exposure", "asbestos", "air quality", "water quality",
    "physical activity", "exercise", "nutrition",
    "mental health", "depression", "anxiety",
    "antibiotic resistance", "amr", "hospital-acquired",
    "vision", "hearing", "dental",
)


def _relevant_query_for_cdc(query: str) -> bool:
    """Heuristik: ist diese Query (englisch) Public-Health-relevant?"""
    if not query:
        return False
    ql = query.lower()
    return any(marker in ql for marker in _PUBLIC_HEALTH_MARKERS)


def _pick_search_query(analysis: dict) -> str | None:
    """Wählt die beste 1-Query für die Catalog-Suche.

    Bevorzugt englische pubmed_queries (sind explizit englisch formuliert,
    matchen besser gegen englischen Catalog), dann factcheck_queries die
    Public-Health-Marker enthalten.
    """
    # 1. pubmed_queries — sind in services/analyzer.py auf Englisch
    for q in (analysis or {}).get("pubmed_queries") or []:
        if _relevant_query_for_cdc(q):
            return q

    # 2. factcheck_queries (vorrangig englische)
    for q in (analysis or {}).get("factcheck_queries") or []:
        if _relevant_query_for_cdc(q):
            return q

    # 3. Fallback: Erste pubmed_query falls überhaupt vorhanden
    pubmed = (analysis or {}).get("pubmed_queries") or []
    if pubmed:
        return pubmed[0]

    return None


async def search_cdc_open_data(analysis: dict) -> dict:
    """Catalog-Suche auf data.cdc.gov.

    Returns dict mit top 5 Dataset-Treffern. Jeder Treffer enthält
    Dataset-Name, Beschreibung (gekürzt), Link auf data.cdc.gov, und
    last-updated-Datum als Stand-der-Daten-Indikator.
    """
    empty = {"source": "CDC Open Data", "type": "public_health_dataset", "results": []}

    query = _pick_search_query(analysis)
    if not query:
        return empty

    # Catalog-API: q (Query), domains (Filter auf CDC), limit (max 5)
    url = (
        f"{CATALOG_URL}?q={quote_plus(query)}&domains={DOMAIN}&limit=5"
    )

    try:
        async with polite_client(timeout=15.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning(f"CDC Open Data fetch failed: {e}")
        return empty

    catalog_results = data.get("results") or []
    if not catalog_results:
        logger.info(f"CDC Open Data: 0 datasets for query '{query[:60]}'")
        return empty

    results: list[dict] = []
    for r in catalog_results[:5]:
        res = r.get("resource") or {}
        name = res.get("name") or ""
        desc = res.get("description") or ""
        dataset_id = res.get("id") or ""
        updated = res.get("updatedAt") or ""

        if not name or not dataset_id:
            continue

        # Construct user-facing link from the catalog response
        link = (r.get("link") or "").strip()
        if not link:
            link = f"https://{DOMAIN}/d/{dataset_id}"

        # Year extraction from ISO updatedAt
        year = updated[:4] if len(updated) >= 4 and updated[:4].isdigit() else "—"

        # Truncate description for the synthesizer payload
        desc_short = (desc[:400] + "…") if len(desc) > 400 else desc

        results.append({
            "indicator_name": name[:200],
            "indicator": "cdc_open_data_dataset",
            "country": "USA",
            "year": year,
            "topic": "cdc_public_health_dataset",
            "display_value": desc_short,
            "description": f"Last updated: {updated[:10]}" if updated else "",
            "url": link,
            "secondary_url": "",
            "source": "CDC Open Data (Centers for Disease Control and Prevention, USA — Socrata-Catalog data.cdc.gov)",
        })

    if not results:
        return empty

    logger.info(
        f"CDC Open Data: {len(results)} datasets matched query '{query[:60]}'"
    )
    return {
        "source": "CDC Open Data",
        "type": "public_health_dataset",
        "results": results,
    }
