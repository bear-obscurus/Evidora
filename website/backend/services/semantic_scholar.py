"""Semantic Scholar — AI-powered academic search by Allen AI.

200M+ papers with AI-generated TLDR summaries and influence scoring.
Free API (100 requests per 5 minutes without key).
Complements OpenAlex/PubMed with unique TLDR summaries.
"""

import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
FIELDS = "title,authors,year,citationCount,tldr,url,externalIds,journal"


async def search_semantic_scholar(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    if not queries:
        return {"source": "Semantic Scholar", "results": []}

    search_term = queries[0]

    params = {
        "query": search_term,
        "limit": 5,
        "fields": FIELDS,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(BASE_URL, params=params)
            if resp.status_code == 429:
                logger.warning("Semantic Scholar rate limit reached (100 req/5min)")
                return {"source": "Semantic Scholar", "results": []}
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning(f"Semantic Scholar request failed: {e}")
        return {"source": "Semantic Scholar", "results": []}

    papers = data.get("data", [])
    if not papers:
        return {"source": "Semantic Scholar", "results": []}

    results = []
    for paper in papers:
        title = paper.get("title", "")
        if not title:
            continue

        # Build URL: prefer DOI, fall back to Semantic Scholar page
        ext_ids = paper.get("externalIds") or {}
        doi = ext_ids.get("DOI", "")
        if doi:
            url = f"https://doi.org/{doi}"
        else:
            paper_id = paper.get("paperId", "")
            url = f"https://www.semanticscholar.org/paper/{paper_id}"

        # Authors (max 3)
        authors_list = paper.get("authors") or []
        author_names = ", ".join(
            a.get("name", "") for a in authors_list[:3]
        )
        if len(authors_list) > 3:
            author_names += " et al."

        # TLDR summary — unique to Semantic Scholar
        tldr = paper.get("tldr") or {}
        tldr_text = tldr.get("text", "")

        journal = paper.get("journal") or {}
        journal_name = journal.get("name", "")

        results.append({
            "title": title,
            "authors": author_names,
            "journal": journal_name,
            "date": str(paper.get("year", "")),
            "url": url,
            "cited_by_count": paper.get("citationCount", 0),
            "tldr": tldr_text,
        })

    logger.info(f"Semantic Scholar: {len(results)} results for '{search_term[:80]}'")
    return {"source": "Semantic Scholar", "type": "study", "results": results}
