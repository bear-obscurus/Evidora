"""OpenAlex — open catalog of 250M+ scholarly works across all disciplines.

Uses the free API (no key required). Complements PubMed by covering
non-biomedical fields: physics, social science, economics, engineering, etc.
"""

import httpx
import logging
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BASE_URL = "https://api.openalex.org/works"
MAILTO = "evidora@evidora.eu"


# Generic scientific words that match too broadly when used alone as filter terms
# Generic academic words that are too broad for single-term title matching
_STOPWORDS = {
    "study", "effect", "effects", "review", "analysis", "role",
    "human", "clinical", "report", "system", "systems", "model",
    "results", "outcomes", "factors", "update", "general", "based",
    "using", "novel", "approach", "method", "high", "long", "term",
    "data", "case", "cases", "associated", "association", "among",
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
    if any(e.lower() in text for e in entity_terms):
        return True
    if query_words:
        hits = sum(1 for w in query_words if w.lower() in text)
        return hits >= 3
    return False


async def search_openalex(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    entities = analysis.get("entities", [])
    if not queries:
        return {"source": "OpenAlex", "results": []}

    # Combine all queries into one search string — OpenAlex relevance scoring
    # works best with a single rich query rather than separate narrow ones
    search_term = " ".join(queries[:3])

    params = {
        "search": search_term,
        "filter": "is_retracted:false,type:article",
        "per_page": 15,
        "sort": "relevance_score:desc",
        "select": "id,doi,title,display_name,relevance_score,publication_year,"
                  "cited_by_count,language,authorships,primary_location,is_retracted",
        "mailto": MAILTO,
    }

    async with polite_client(timeout=30.0) as client:
        resp = await client.get(BASE_URL, params=params)
        if resp.status_code == 429:
            logger.warning("OpenAlex daily rate limit reached")
            return {"source": "OpenAlex", "results": []}
        resp.raise_for_status()
        data = resp.json()

    works = data.get("results", [])
    if not works:
        return {"source": "OpenAlex", "results": []}

    logger.info(f"OpenAlex: {len(works)} unique works from {len(queries[:3])} queries")

    results = []
    for work in works:
        # Build URL: prefer DOI, fall back to OpenAlex page
        doi = work.get("doi") or ""
        url = doi if doi.startswith("http") else f"https://doi.org/{doi}" if doi else work.get("id", "")

        # Authors (max 3 + et al.)
        authorships = work.get("authorships", [])
        author_names = ", ".join(
            a.get("author", {}).get("display_name", "")
            for a in authorships[:3]
        )
        if len(authorships) > 3:
            author_names += " et al."

        # Journal from primary location
        location = work.get("primary_location") or {}
        source = location.get("source") or {}
        journal = source.get("display_name", "")

        title = work.get("display_name") or work.get("title") or ""
        if not title:
            continue

        results.append({
            "title": title,
            "authors": author_names,
            "journal": journal,
            "date": str(work.get("publication_year", "")),
            "url": url,
            "cited_by_count": work.get("cited_by_count", 0),
        })

    # Filter by entity overlap to remove off-topic results
    if entities:
        filtered = [r for r in results if _has_entity_overlap(r["title"], entities, queries)]
        logger.info(f"OpenAlex: {len(results)} raw, {len(filtered)} after entity filter for '{search_term[:80]}'")
        results = filtered
    else:
        logger.info(f"OpenAlex: {len(results)} results for '{search_term[:80]}'")
    return {"source": "OpenAlex", "type": "study", "results": results}
