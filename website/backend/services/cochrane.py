import httpx
import os

PUBMED_API_KEY = os.getenv("PUBMED_API_KEY", "")
PUBMED_EMAIL = os.getenv("PUBMED_EMAIL", "")
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


async def search_cochrane(analysis: dict) -> dict:
    """Search PubMed for Cochrane systematic reviews on the claim topic."""
    queries = analysis.get("pubmed_queries", [])
    if not queries:
        return {"source": "Cochrane Reviews", "type": "systematic_review", "results": []}

    # Use the broadest query + Cochrane journal filter
    query = f'({" OR ".join(queries[:2])}) AND "Cochrane Database Syst Rev"[Journal]'

    async with httpx.AsyncClient(timeout=30.0) as client:
        params = {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retmax": 3,
            "sort": "relevance",
        }
        if PUBMED_API_KEY:
            params["api_key"] = PUBMED_API_KEY
        if PUBMED_EMAIL:
            params["email"] = PUBMED_EMAIL
            params["tool"] = "evidora"

        search_resp = await client.get(f"{BASE_URL}/esearch.fcgi", params=params)
        search_resp.raise_for_status()
        ids = search_resp.json().get("esearchresult", {}).get("idlist", [])

        if not ids:
            return {"source": "Cochrane Reviews", "type": "systematic_review", "results": []}

        summary_params = {
            "db": "pubmed",
            "id": ",".join(ids),
            "retmode": "json",
        }
        if PUBMED_API_KEY:
            summary_params["api_key"] = PUBMED_API_KEY
        if PUBMED_EMAIL:
            summary_params["email"] = PUBMED_EMAIL
            summary_params["tool"] = "evidora"

        summary_resp = await client.get(f"{BASE_URL}/esummary.fcgi", params=summary_params)
        summary_resp.raise_for_status()
        summary_data = summary_resp.json()

        results = []
        for pmid in ids:
            article = summary_data.get("result", {}).get(pmid, {})
            if not isinstance(article, dict):
                continue
            authors = article.get("authors", [])
            author_names = ", ".join(a.get("name", "") for a in authors[:3])
            if len(authors) > 3:
                author_names += " et al."
            results.append({
                "title": article.get("title", ""),
                "authors": author_names,
                "journal": "Cochrane Database of Systematic Reviews",
                "date": article.get("pubdate", ""),
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            })

        return {"source": "Cochrane Reviews", "type": "systematic_review", "results": results}
