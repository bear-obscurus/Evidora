"""
ClaimReview-Suche über die Google Fact Check Tools API.

Nutzt die offizielle API statt Web-Scraping. Enthält Ergebnisse von EFCSN-Mitgliedern
wie Correctiv, AFP, dpa und vielen weiteren europäischen Faktencheckern.

Ergebnisse werden nach dem Abruf per Sentence Transformers (MiniLM) semantisch
gefiltert, um irrelevante Treffer zu entfernen.

API-Key beantragen (kostenlos): https://console.cloud.google.com/apis/library/factchecktools.googleapis.com
"""

import os
import logging
import httpx

logger = logging.getLogger("evidora")

API_URL = "https://factchecktools.googleapis.com/v1alpha1/claims:search"
API_KEY = os.getenv("GOOGLE_FACTCHECK_API_KEY", "")

# Minimum semantic similarity to keep a ClaimReview result
SEMANTIC_THRESHOLD = 0.20


def _filter_by_similarity(claim: str, results: list[dict]) -> list[dict]:
    """Filter ClaimReview results by semantic similarity to the original claim."""
    if not results or len(results) <= 1:
        return results

    try:
        from services.reranker import _load_model, _model
        if not _load_model() or _model is None:
            return results

        from sentence_transformers import util

        claim_embedding = _model.encode(claim, convert_to_tensor=True)
        texts = [f"{r.get('title', '')} {r.get('rating', '')}" for r in results]
        result_embeddings = _model.encode(texts, convert_to_tensor=True)
        scores = util.cos_sim(claim_embedding, result_embeddings)[0]

        scored = sorted(zip(results, scores.tolist()), key=lambda x: x[1], reverse=True)
        filtered = [r for r, score in scored if score > SEMANTIC_THRESHOLD]

        if filtered:
            logger.debug(f"ClaimReview semantic filter: {len(results)} → {len(filtered)} results (top: {scored[0][1]:.3f})")
            return filtered

        # If everything would be filtered out, return top 3 by score
        return [r for r, _ in scored[:3]]

    except Exception as e:
        logger.debug(f"ClaimReview semantic filter failed: {e}")
        return results


async def search_claimreview(analysis: dict) -> dict:
    queries = analysis.get("factcheck_queries", [])
    claim = analysis.get("claim", "")
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

                for claim_data in data.get("claims", []):
                    claim_text = claim_data.get("text", "")

                    for review in claim_data.get("claimReview", []):
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

    # Semantic filtering: remove irrelevant results, re-order by similarity
    if claim and all_results:
        all_results = _filter_by_similarity(claim, all_results)

    return {
        "source": "Europäische Faktenchecker",
        "type": "factcheck",
        "results": all_results[:10],
    }
