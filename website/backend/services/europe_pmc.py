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


async def search_europe_pmc(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    if not queries:
        return {"source": "Europe PMC", "results": []}

    # Combine top queries for broader search
    search_term = " OR ".join(f'"{q}"' for q in queries[:2])

    params = {
        "query": search_term,
        "format": "json",
        "pageSize": 5,
        "sort": "RELEVANCE",
        "resultType": "core",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{BASE_URL}/search", params=params)
        resp.raise_for_status()
        data = resp.json()

    articles = data.get("resultList", {}).get("result", [])
    if not articles:
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

        # Authors
        author_list = article.get("authorList", {}).get("author", [])
        author_names = ", ".join(
            a.get("fullName", a.get("lastName", ""))
            for a in author_list[:3]
        )
        if len(author_list) > 3:
            author_names += " et al."

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

    logger.info(f"Europe PMC: {len(results)} results for '{search_term[:80]}'")
    return {"source": "Europe PMC", "type": "study", "results": results}
