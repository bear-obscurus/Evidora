"""OpenAlex — open catalog of 250M+ scholarly works across all disciplines.

Uses the free API (no key required). Complements PubMed by covering
non-biomedical fields: physics, social science, economics, engineering, etc.
"""

import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://api.openalex.org/works"
MAILTO = "evidora@evidora.eu"


def _has_entity_overlap(title: str, entities: list[str]) -> bool:
    """Check if any entity appears in the result title."""
    if not entities:
        return True
    text = title.lower()
    return any(e.lower() in text for e in entities if len(e) >= 3)


async def search_openalex(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    entities = analysis.get("entities", [])
    if not queries:
        return {"source": "OpenAlex", "results": []}

    search_term = " ".join(queries[:3])

    params = {
        "search": search_term,
        "filter": "is_retracted:false,type:article",
        "per_page": 5,
        "sort": "relevance_score:desc",
        "select": "id,doi,title,display_name,relevance_score,publication_year,"
                  "cited_by_count,language,authorships,primary_location,is_retracted",
        "mailto": MAILTO,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(BASE_URL, params=params)
        if resp.status_code == 429:
            logger.warning("OpenAlex daily rate limit reached (1,000 searches/day)")
            return {"source": "OpenAlex", "results": []}
        resp.raise_for_status()
        data = resp.json()

    works = data.get("results", [])
    if not works:
        return {"source": "OpenAlex", "results": []}

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
        filtered = [r for r in results if _has_entity_overlap(r["title"], entities)]
        logger.info(f"OpenAlex: {len(results)} raw, {len(filtered)} after entity filter for '{search_term[:80]}'")
        results = filtered
    else:
        logger.info(f"OpenAlex: {len(results)} results for '{search_term[:80]}'")
    return {"source": "OpenAlex", "type": "study", "results": results}
