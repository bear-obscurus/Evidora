"""Semantic Scholar — AI-powered academic search by Allen AI.

200M+ papers with AI-generated TLDR summaries and influence scoring.
Free API (100 requests per 5 minutes without key).
Complements OpenAlex/PubMed with unique TLDR summaries.
"""

import asyncio
import os

import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
FIELDS = "title,authors,year,citationCount,tldr,url,externalIds,journal"

# Optional API key (free tier: request at https://www.semanticscholar.org/product/api#api-key)
S2_API_KEY = os.getenv("S2_API_KEY", "")

MAX_RETRIES = 2
RETRY_DELAY = 1.5  # seconds


# Generic scientific words that match too broadly when used alone as filter terms
# Generic academic words that are too broad for single-term title matching
_STOPWORDS = {
    "study", "effect", "effects", "review", "analysis", "role",
    "human", "clinical", "report", "system", "systems", "model",
    "results", "outcomes", "factors", "update", "general", "based",
    "using", "novel", "approach", "method", "high", "long", "term",
    "data", "case", "cases",
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
        return hits >= 2
    return False


async def _s2_single_query(client: httpx.AsyncClient, query: str, headers: dict) -> list[dict]:
    """Run a single Semantic Scholar query with retry logic."""
    params = {"query": query, "limit": 10, "fields": FIELDS}
    try:
        resp = None
        for attempt in range(MAX_RETRIES + 1):
            resp = await client.get(BASE_URL, params=params, headers=headers)
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                logger.info(f"Semantic Scholar rate limited, retry {attempt + 1}/{MAX_RETRIES} after {RETRY_DELAY}s")
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            break
        if resp.status_code == 429:
            logger.warning(f"Semantic Scholar rate limit for query '{query[:60]}'")
            return []
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        logger.warning(f"Semantic Scholar query failed: {e}")
        return []


async def search_semantic_scholar(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    entities = analysis.get("entities", [])
    if not queries:
        return {"source": "Semantic Scholar", "results": []}

    headers = {}
    if S2_API_KEY:
        headers["x-api-key"] = S2_API_KEY

    # Run up to 3 queries in parallel, merge and deduplicate
    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = [_s2_single_query(client, q, headers) for q in queries[:3]]
        all_papers = await asyncio.gather(*tasks)

    # Flatten and deduplicate by paperId
    seen = set()
    papers = []
    for paper_list in all_papers:
        for paper in paper_list:
            pid = paper.get("paperId", "")
            if pid and pid not in seen:
                seen.add(pid)
                papers.append(paper)

    if not papers:
        return {"source": "Semantic Scholar", "results": []}

    logger.info(f"Semantic Scholar: {len(papers)} unique papers from {len(queries[:3])} queries")

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

    # Filter by entity overlap to remove off-topic results
    if entities:
        filtered = [r for r in results if _has_entity_overlap(r["title"], entities, queries)]
        logger.info(f"Semantic Scholar: {len(results)} raw, {len(filtered)} after entity filter")
        results = filtered
    else:
        logger.info(f"Semantic Scholar: {len(results)} results")
    return {"source": "Semantic Scholar", "type": "study", "results": results}
