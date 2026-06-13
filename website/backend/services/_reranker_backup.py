"""Reranker-Backup-Trigger — Cosine-Similarity-Fallback wenn Substring +
Composite-Trigger eines Services NICHT zünden, der Claim aber semantisch
klar zu einem Topic des Services gehört.

Pattern in einem Service:

    from services._reranker_backup import claim_might_be_about

    _TOPIC_DESCRIPTIONS = [
        "EuGH-Urteil C-411/10 N.S. zur Dublin-Verordnung und Asyl-Rückführungen",
        "Schrems II C-311/18 zu Datenschutz und EU-US-Datenexport",
        ...
    ]

    def claim_mentions_X_cached(claim: str) -> bool:
        cl = claim.lower()
        if _substring_match(cl):
            return True
        if _composite_match(cl):
            return True
        # Backup: Cosine ≥ threshold zu einem Topic-Descriptor
        return claim_might_be_about(claim, _TOPIC_DESCRIPTIONS, threshold=0.65)

Performance: Topic-Embeddings werden pro Tupel `tuple(descriptions)` einmal
berechnet und in `_topic_embedding_cache` gehalten — so dass nur das
Claim-Embedding pro Aufruf neu berechnet wird (~30 ms pro Claim mit
multilingual-MiniLM-L12-v2). Modell-Load ist lazy beim ersten Aufruf.

Wenn sentence-transformers nicht installiert ist, gibt der Helper immer
False zurück (no-op fallback) — der Substring/Composite-Pfad bleibt
intakt.
"""

import logging
from collections import OrderedDict

logger = logging.getLogger("evidora")

_topic_embedding_cache: dict[tuple, "torch.Tensor"] = {}  # noqa: F821 — lazy import

# Claim-Embedding-Cache (Key = exakter Claim-String).
# Der Trigger-Gate-Sweep in main.py ruft best_matches() pro Request bis zu
# ~60× mit demselben Claim auf (jedes Static-First-Pack, dessen Substring-/
# Composite-Trigger nicht zündet, fällt auf den Cosine-Backup zurück). Ohne
# diesen Cache wird DERSELBE Claim dutzendfach neu encodet (~30 ms/Encode,
# synchron im Event-Loop → bis ~1,8 s blockierende CPU pro Request). Mit
# Cache kollabiert das auf 1 Encode pro Claim-String. Bounded LRU, damit
# der Store über viele verschiedene Claims nicht unbegrenzt wächst.
_CLAIM_EMB_CACHE_MAX = 128
_claim_emb_cache: "OrderedDict[str, object]" = OrderedDict()


def _get_model():
    """Return the shared SentenceTransformer instance (or None if
    unavailable). Delegates to ``services._st_model`` so reranker.py and
    this module share ONE model instance instead of loading two."""
    from services._st_model import get_model
    return get_model()


def _encode_claim_cached(model, claim: str):
    """Encode ``claim`` once und cache das Embedding (bounded LRU).

    Wiederholte best_matches()-Aufrufe mit demselben Claim-String (der
    Trigger-Gate-Sweep tut das ~60×/Request) liefern danach das gecachte
    Embedding statt neu zu encodieren. Key ist der exakte Claim-String —
    das Embedding ist für ein festes Modell deterministisch, also kein
    Staleness-Risiko.
    """
    cached = _claim_emb_cache.get(claim)
    if cached is not None:
        _claim_emb_cache.move_to_end(claim)  # LRU-Touch
        return cached
    emb = model.encode(claim, convert_to_tensor=True)
    _claim_emb_cache[claim] = emb
    if len(_claim_emb_cache) > _CLAIM_EMB_CACHE_MAX:
        _claim_emb_cache.popitem(last=False)  # ältesten Eintrag verdrängen
    return emb


def claim_might_be_about(
    claim: str,
    topic_descriptions: list[str] | tuple[str, ...],
    threshold: float = 0.65,
) -> bool:
    """Cosine-Similarity-Check: Has claim a similarity ≥ threshold to ANY
    topic description?

    Convenience wrapper für reine Boolean-Verwendung; verwendet intern
    `best_matches` mit top_n=1.
    """
    return bool(best_matches(
        claim, list(zip(topic_descriptions, topic_descriptions)),
        threshold=threshold, top_n=1,
    ))


def best_matches(
    claim: str,
    items_with_descriptions: list,
    threshold: float = 0.65,
    top_n: int = 3,
) -> list:
    """Return up to ``top_n`` items whose description has cosine ≥ threshold.

    ``items_with_descriptions`` is a list of (item, description) tuples;
    only items whose description embedding is similar enough to the claim
    are returned, sorted descending by score.

    Returns the list of `item` objects (not tuples). Empty list if model
    unavailable or no item clears the threshold.
    """
    if not claim or not items_with_descriptions:
        return []
    model = _get_model()
    if model is None:
        return []

    try:
        from sentence_transformers import util
        descriptions = tuple(d for _, d in items_with_descriptions)
        topic_emb = _topic_embedding_cache.get(descriptions)
        if topic_emb is None:
            topic_emb = model.encode(list(descriptions), convert_to_tensor=True)
            _topic_embedding_cache[descriptions] = topic_emb

        claim_emb = _encode_claim_cached(model, claim)
        scores = util.cos_sim(claim_emb, topic_emb)[0].tolist()

        scored = [
            (items_with_descriptions[i][0], float(scores[i]))
            for i in range(len(items_with_descriptions))
        ]
        scored.sort(key=lambda x: x[1], reverse=True)
        winners = [item for item, score in scored[:top_n] if score >= threshold]
        if winners:
            top_score = scored[0][1]
            logger.debug(
                f"reranker_backup: matched {len(winners)}/{top_n} items "
                f"(top score={top_score:.3f}, threshold={threshold})"
            )
        return winners
    except Exception as e:
        logger.warning(f"reranker_backup: scoring failed: {e}")
        return []
