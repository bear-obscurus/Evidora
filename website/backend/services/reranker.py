"""Semantic re-ranking of source results using Sentence Transformers (MiniLM).

Loads model lazily on first use (~30 MB download, ~100 MB RAM).
If sentence-transformers is not installed, falls back to no-op (results unchanged).
"""

import logging

logger = logging.getLogger("evidora")

_model = None
_available = None


def _load_model():
    """Lazy-load the sentence transformer model."""
    global _model, _available
    if _available is not None:
        return _available

    try:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer("all-MiniLM-L6-v2")
        _available = True
        logger.info("Sentence Transformer model loaded (all-MiniLM-L6-v2)")
        return True
    except ImportError:
        _available = False
        logger.info("sentence-transformers not installed — semantic re-ranking disabled")
        return False
    except Exception as e:
        _available = False
        logger.warning(f"Failed to load Sentence Transformer: {e}")
        return False


def _result_text(result: dict) -> str:
    """Extract searchable text from a result entry."""
    parts = []
    for key in ("title", "name", "indicator_name", "description", "journal"):
        val = result.get(key)
        if val:
            parts.append(str(val))
    return " ".join(parts)


def rerank_results(claim: str, source_results: list) -> list:
    """Re-rank results within each source by semantic similarity to the claim.

    Args:
        claim: The original claim text.
        source_results: List of source dicts with "results" lists.

    Returns:
        The same list with results re-ordered by relevance (most similar first).
    """
    if not _load_model():
        return source_results

    try:
        from sentence_transformers import util

        claim_embedding = _model.encode(claim, convert_to_tensor=True)

        for source_data in source_results:
            if not isinstance(source_data, dict):
                continue
            results = source_data.get("results", [])
            if len(results) <= 1:
                continue

            texts = [_result_text(r) for r in results]
            result_embeddings = _model.encode(texts, convert_to_tensor=True)
            scores = util.cos_sim(claim_embedding, result_embeddings)[0]

            # Sort results by similarity score (descending)
            scored = sorted(zip(results, scores.tolist()), key=lambda x: x[1], reverse=True)
            source_data["results"] = [r for r, _ in scored]

            source_name = source_data.get("source", "Unknown")
            top_score = scored[0][1] if scored else 0
            logger.debug(f"Reranked {len(results)} results for {source_name} (top score: {top_score:.3f})")

        return source_results

    except Exception as e:
        logger.warning(f"Semantic re-ranking failed: {e}")
        return source_results
