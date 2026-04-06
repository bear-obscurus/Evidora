"""Europe PMC — 40M+ life science articles with European focus.

Free REST API, no key required. Complements PubMed with:
- European research emphasis
- Open Access full texts
- Preprints from bioRxiv/medRxiv
- Grant-linked research (EU funding)
"""

import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest"


# Generic scientific words that match too broadly when used alone as filter terms
_STOPWORDS = {
    "risk", "study", "health", "effect", "effects", "review", "analysis",
    "association", "associations", "evidence", "exposure", "impact", "role",
    "safety", "human", "clinical", "disease", "treatment", "research",
    "data", "case", "cases", "report", "system", "systems", "model",
    "results", "outcomes", "factors", "update", "population", "general",
}


def _has_entity_overlap(title: str, entities: list[str], query_terms: list[str] | None = None) -> bool:
    """Check if any entity or query keyword appears in the result title."""
    all_terms = [e for e in entities if len(e) >= 3]
    if query_terms:
        for q in query_terms:
            all_terms.extend(
                w for w in q.split()
                if len(w) >= 4 and w.lower() not in _STOPWORDS
            )
    if not all_terms:
        return True
    text = title.lower()
    return any(t.lower() in text for t in all_terms)


async def search_europe_pmc(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    entities = analysis.get("entities", [])
    if not queries:
        return {"source": "Europe PMC", "results": []}

    # Use first query as keywords (no exact phrase — EPMC needs loose matching)
    search_term = queries[0]

    params = {
        "query": search_term,
        "format": "json",
        "pageSize": 5,
        "sort": "CITED desc",
        "resultType": "lite",
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{BASE_URL}/search", params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning(f"Europe PMC request failed: {e}")
        return {"source": "Europe PMC", "results": []}

    articles = data.get("resultList", {}).get("result", [])
    if not articles:
        logger.info(f"Europe PMC: 0 results for '{search_term[:80]}'")
        return {"source": "Europe PMC", "results": []}

    results = []
    for article in articles:
        title = article.get("title", "")
        if not title:
            continue

        # Build URL: prefer DOI, fall back to Europe PMC page
        doi = article.get("doi", "")
        pmid = article.get("pmid", "")
        epmc_id = article.get("id", "")
        source_type = article.get("source", "MED")

        if doi:
            url = f"https://doi.org/{doi}"
        elif pmid:
            url = f"https://europepmc.org/article/MED/{pmid}"
        else:
            url = f"https://europepmc.org/article/{source_type}/{epmc_id}"

        # Authors (lite returns authorString, not authorList)
        author_str = article.get("authorString", "")
        # Truncate to first 3 authors
        parts = [a.strip() for a in author_str.split(",") if a.strip()]
        if len(parts) > 6:  # ~3 authors × 2 parts (last, first)
            author_names = ", ".join(parts[:6]) + " et al."
        else:
            author_names = author_str.rstrip(".")

        journal = article.get("journalTitle", "")
        year = article.get("pubYear", "")
        is_open_access = article.get("isOpenAccess", "N") == "Y"
        cited_by = article.get("citedByCount", 0)

        results.append({
            "title": title,
            "authors": author_names,
            "journal": journal,
            "date": str(year),
            "url": url,
            "cited_by_count": cited_by,
            "open_access": is_open_access,
        })

    # Filter by entity overlap to remove off-topic results
    if entities:
        filtered = [r for r in results if _has_entity_overlap(r["title"], entities, queries)]
        logger.info(f"Europe PMC: {len(results)} raw, {len(filtered)} after entity filter for '{search_term[:80]}'")
        results = filtered
    else:
        logger.info(f"Europe PMC: {len(results)} results for '{search_term[:80]}'")
    return {"source": "Europe PMC", "type": "study", "results": results}
