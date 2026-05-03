"""EFSA — European Food Safety Authority scientific opinions.

Uses the CrossRef API to search the EFSA Journal (ISSN 1831-4732).
Free, no API key required. ~7,500 peer-reviewed scientific opinions
on food safety, pesticides, additives, contaminants, nutrition, GMOs, etc.
"""

import httpx
import logging
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BASE_URL = "https://api.crossref.org/journals/1831-4732/works"


async def search_efsa(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    if not queries:
        return {"source": "EFSA (European Food Safety Authority)", "type": "study", "results": []}

    search_term = queries[0]

    params = {
        "query": search_term,
        "rows": 5,
        "sort": "relevance",
        "select": "DOI,title,abstract,published-print,URL,author,is-referenced-by-count",
    }

    headers = {
        "User-Agent": "Evidora/1.0 (https://evidora.eu; mailto:Evidora@proton.me)",
    }

    try:
        async with polite_client(timeout=30.0) as client:
            resp = await client.get(BASE_URL, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning(f"EFSA/CrossRef request failed: {e}")
        return {"source": "EFSA (European Food Safety Authority)", "type": "study", "results": []}

    items = data.get("message", {}).get("items", [])
    if not items:
        logger.info(f"EFSA: 0 results for '{search_term[:80]}'")
        return {"source": "EFSA (European Food Safety Authority)", "type": "study", "results": []}

    results = []
    for item in items:
        title_list = item.get("title", [])
        title = title_list[0] if title_list else ""
        if not title:
            continue

        doi = item.get("DOI", "")
        url = f"https://doi.org/{doi}" if doi else item.get("URL", "")

        # Authors
        authors_raw = item.get("author", [])
        author_names = []
        for a in authors_raw[:3]:
            given = a.get("given", "")
            family = a.get("family", "")
            if family:
                author_names.append(f"{given} {family}".strip())
        authors = ", ".join(author_names)
        if len(authors_raw) > 3:
            authors += " et al."

        # Date
        date_parts = item.get("published-print", {}).get("date-parts", [[]])
        year = str(date_parts[0][0]) if date_parts and date_parts[0] else ""

        cited_by = item.get("is-referenced-by-count", 0)

        results.append({
            "title": title,
            "authors": authors if authors else "EFSA Panel",
            "journal": "EFSA Journal",
            "date": year,
            "url": url,
            "cited_by_count": cited_by,
        })

    logger.info(f"EFSA: {len(results)} results for '{search_term[:80]}'")
    return {"source": "EFSA (European Food Safety Authority)", "type": "study", "results": results}
