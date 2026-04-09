"""Europe PMC — 40M+ life science articles with European focus.

Free REST API, no key required. Complements PubMed with:
- European research emphasis
- Open Access full texts
- Preprints from bioRxiv/medRxiv
- Grant-linked research (EU funding)
"""

import asyncio

import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest"


# Generic scientific words that match too broadly when used alone as filter terms
# Generic academic words that are too broad for single-term title matching
_STOPWORDS = {
    # Generic academic terms
    "study", "effect", "effects", "review", "analysis", "role",
    "human", "clinical", "report", "system", "systems", "model",
    "results", "outcomes", "factors", "update", "general", "based",
    "using", "novel", "approach", "method", "high", "long", "term",
    "data", "case", "cases",
    # Population descriptors (too broad on their own)
    "children", "child", "adolescent", "adolescents", "adult", "adults",
    "young", "youth", "patients", "women", "infants",
    # Broad domain terms (match across unrelated fields)
    "health", "brain", "cognitive", "development", "developmental",
    "mental", "behavioral", "behaviour", "disorder", "disorders",
    "disease", "treatment", "risk", "social", "intervention",
    "associated", "association", "impact", "among",
}


def _has_entity_overlap(title: str, entities: list[str], query_terms: list[str] | None = None) -> bool:
    """Check if any entity or query keyword appears in the result title.

    For query terms, requires at least 2 matches to prevent overly broad
    single-word matches (e.g. 'cancer' alone matching 'prostate cancer').
    Entity matches (from claim analysis) count as a direct hit.
    """
    entity_terms = [e for e in entities if len(e) >= 3]
    query_words = []
    if query_terms:
        for q in query_terms:
            query_words.extend(
                w for w in q.split()
                if len(w) >= 4 and w.lower() not in _STOPWORDS
            )
    if not entity_terms and not query_words:
        return True
    text = title.lower()
    # Direct entity match (DE terms) — single hit is enough
    if any(e.lower() in text for e in entity_terms):
        return True
    # Query term match (EN terms) — require at least 2 hits to be specific
    if query_words:
        hits = sum(1 for w in query_words if w.lower() in text)
        return hits >= 2
    return False


async def _epmc_single_query(client: httpx.AsyncClient, query: str) -> list[dict]:
    """Run a single Europe PMC query."""
    params = {
        "query": query,
        "format": "json",
        "pageSize": 10,
        "sort": "CITED desc",
        "resultType": "lite",
    }
    try:
        resp = await client.get(f"{BASE_URL}/search", params=params)
        resp.raise_for_status()
        return resp.json().get("resultList", {}).get("result", [])
    except Exception as e:
        logger.warning(f"Europe PMC query failed: {e}")
        return []


async def search_europe_pmc(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    entities = analysis.get("entities", [])
    if not queries:
        return {"source": "Europe PMC", "results": []}

    # Run up to 3 queries in parallel, merge and deduplicate
    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = [_epmc_single_query(client, q) for q in queries[:3]]
        all_articles = await asyncio.gather(*tasks)

    # Flatten and deduplicate by DOI or pmid
    seen = set()
    articles = []
    for article_list in all_articles:
        for article in article_list:
            key = article.get("doi") or article.get("pmid") or article.get("id", "")
            if key and key not in seen:
                seen.add(key)
                articles.append(article)

    if not articles:
        logger.info(f"Europe PMC: 0 results from {len(queries[:3])} queries")
        return {"source": "Europe PMC", "results": []}

    logger.info(f"Europe PMC: {len(articles)} unique articles from {len(queries[:3])} queries")

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
        logger.info(f"Europe PMC: {len(results)} raw, {len(filtered)} after entity filter")
        results = filtered
    else:
        logger.info(f"Europe PMC: {len(results)} results")
    return {"source": "Europe PMC", "type": "study", "results": results}
