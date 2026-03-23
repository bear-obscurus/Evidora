import httpx
import os

PUBMED_API_KEY = os.getenv("PUBMED_API_KEY", "")
PUBMED_EMAIL = os.getenv("PUBMED_EMAIL", "")
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


async def search_pubmed(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    if not queries:
        return {"source": "PubMed", "results": []}

    query = " OR ".join(queries[:2])

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Step 1: Search for article IDs
        params = {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retmax": 5,
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
            return {"source": "PubMed", "results": []}

        # Step 2: Fetch article summaries
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

        summary_resp = await client.get(
            f"{BASE_URL}/esummary.fcgi", params=summary_params
        )
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
            results.append(
                {
                    "title": article.get("title", ""),
                    "authors": author_names,
                    "journal": article.get("source", ""),
                    "date": article.get("pubdate", ""),
                    "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                }
            )

        return {"source": "PubMed", "type": "study", "results": results}
