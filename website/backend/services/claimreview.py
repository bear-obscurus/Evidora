"""
ClaimReview-Suche über die Google Fact Check Tools API.

Nutzt die offizielle API statt Web-Scraping. Enthält Ergebnisse von EFCSN-Mitgliedern
wie Correctiv, AFP, dpa und vielen weiteren europäischen Faktencheckern.

API-Key beantragen (kostenlos): https://console.cloud.google.com/apis/library/factchecktools.googleapis.com
"""

import os
import logging
import httpx

logger = logging.getLogger("evidora")

API_URL = "https://factchecktools.googleapis.com/v1alpha1/claims:search"
API_KEY = os.getenv("GOOGLE_FACTCHECK_API_KEY", "")


async def search_claimreview(analysis: dict) -> dict:
    queries = analysis.get("factcheck_queries", [])
    if not queries:
        return {"source": "Europäische Faktenchecker", "type": "factcheck", "results": []}

    if not API_KEY:
        logger.warning("GOOGLE_FACTCHECK_API_KEY not set — skipping ClaimReview search")
        return {"source": "Europäische Faktenchecker", "type": "factcheck", "results": []}

    all_results = []
    seen_urls = set()

    async with httpx.AsyncClient(timeout=15.0) as client:
        for query in queries[:2]:  # Max 2 queries
            try:
                resp = await client.get(
                    API_URL,
                    params={
                        "query": query,
                        "languageCode": "de",
                        "pageSize": 5,
                        "key": API_KEY,
                    },
                )
                resp.raise_for_status()
                data = resp.json()

                for claim in data.get("claims", []):
                    claim_text = claim.get("text", "")

                    for review in claim.get("claimReview", []):
                        url = review.get("url", "")
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)

                        publisher = review.get("publisher", {})
                        all_results.append({
                            "title": review.get("title", claim_text),
                            "url": url,
                            "source": publisher.get("name", publisher.get("site", "Faktenchecker")),
                            "rating": review.get("textualRating", ""),
                            "date": review.get("reviewDate", ""),
                        })

            except Exception as e:
                logger.warning(f"Google Fact Check API query failed: {e}")

    return {
        "source": "Europäische Faktenchecker",
        "type": "factcheck",
        "results": all_results[:10],
    }
