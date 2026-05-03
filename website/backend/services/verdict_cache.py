"""Verdict-Cache mit semantischer Ähnlichkeit (Hebel #4 der Latenz-
Optimierung).

Während `services/cache.py` einzelne Datenquellen-Resultate cached
(per source × analysis), cached `verdict_cache.py` das KOMPLETTE
synthetische Verdict-Dict (verdict, confidence, summary, evidence,
source_coverage, ...) auf Claim-Ebene — und zusätzlich semantisch:
selbst leichte Umformulierungen ('Ist Spinat eisenreich?' vs 'Hat
Spinat viel Eisen?') treffen denselben Cache-Eintrag, sofern Cosine-
Ähnlichkeit ≥ Threshold ist.

Latenz-Effekt: bei Cache-Hit kann die gesamte Pipeline (Analyzer +
30+ Datenquellen + Synthesizer) übersprungen werden — Antwort
in <100 ms statt 8-15 s.

Konservative Defaults:
  - Exact-Match-Cache: TTL 30 Min, identisch zu services/cache.py
  - Semantic-Cache: Threshold 0.92 (sehr hoch — fast identische Claims)
  - Nur Verdicts mit Confidence ≥ 0.8 werden in den Semantic-Cache
    gepackt (vermeidet, dass schwache 'unverifiable'-Verdicts
    semantische Treffer kontaminieren)
  - Hot-Reload-aware: data_version-Bump invalidiert alle Cache-Einträge

Sicherheit gegen False-Positives:
  - Threshold 0.92 ist konservativ. Studienlage zu Sentence-BERT-
    Embeddings: bei 0.95+ praktisch identisch, 0.90+ Bedeutungs-Kern
    gleich, <0.85 Themen-ähnlich aber unterschiedlich.
  - Cache wird per (claim_text, data_version) verschlüsselt — bei
    Datendaten-Änderung automatisch invalidiert.
  - Cache-Hit wird im Log markiert mit Cosine + Original-Claim, damit
    bei Nutzer-Beschwerden nachvollziehbar ist, welcher Claim den Hit
    geliefert hat.
"""

import logging
import time
from typing import Optional

import numpy as np

from services._reranker_backup import _get_model as _get_st_model
from services._static_cache import get_data_version

logger = logging.getLogger("evidora")

# In-Memory-Stores: claim_lc -> (timestamp, embedding, result_dict, data_version)
_verdict_store: dict[str, tuple[float, "np.ndarray | None", dict, str]] = {}

DEFAULT_TTL = 1800  # 30 Min — identisch zu services/cache.py
SEMANTIC_THRESHOLD = 0.92  # Cosine-Ähnlichkeit für Cache-Hit
MIN_CONFIDENCE_FOR_CACHE = 0.8  # nur sicher genug Verdicts cachen
MAX_STORE_SIZE = 500  # FIFO-Limit, damit Memory nicht unbegrenzt wächst


def _normalize(claim: str) -> str:
    """Trim + lowercase claim für Exact-Match-Lookup."""
    return claim.strip().lower()


def _purge_expired() -> None:
    """Entfernt abgelaufene Einträge."""
    now = time.time()
    expired = [k for k, (ts, *_) in _verdict_store.items()
               if now - ts > DEFAULT_TTL]
    for k in expired:
        del _verdict_store[k]


def _enforce_size_limit() -> None:
    """FIFO-Eviction wenn Store über MAX_STORE_SIZE."""
    if len(_verdict_store) <= MAX_STORE_SIZE:
        return
    # Älteste Einträge zuerst rausschmeißen
    items = sorted(_verdict_store.items(), key=lambda kv: kv[1][0])
    overflow = len(items) - MAX_STORE_SIZE
    for k, _ in items[:overflow]:
        del _verdict_store[k]


def _embed(text: str) -> Optional["np.ndarray"]:
    """Berechnet Sentence-BERT-Embedding (multilingual MiniLM-L12-v2).
    Returns None wenn Modell nicht verfügbar."""
    model = _get_st_model()
    if model is None:
        return None
    try:
        emb = model.encode(text, convert_to_numpy=True, normalize_embeddings=True)
        return emb
    except Exception as e:
        logger.warning(f"verdict_cache: embedding failed: {e}")
        return None


def get(claim: str) -> Optional[dict]:
    """Liefert ein gecachtes Verdict für ``claim``, falls verfügbar.

    Probiert in Reihenfolge:
      1. Exact-Match (Trim + lowercase) — sehr schnell, ohne Embedding
      2. Semantic-Match (Cosine ≥ SEMANTIC_THRESHOLD) — ~30 ms

    Returns das gecachte result-Dict oder None.
    """
    if not claim:
        return None
    _purge_expired()
    current_dv = get_data_version()

    # 1) Exact match
    norm = _normalize(claim)
    entry = _verdict_store.get(norm)
    if entry is not None:
        ts, emb, result, dv = entry
        if dv == current_dv and time.time() - ts <= DEFAULT_TTL:
            logger.info(f"verdict_cache: EXACT HIT for {claim[:60]!r}")
            return _annotate_hit(result, "exact", 1.0, claim)

    # 2) Semantic match
    if not _verdict_store:
        return None
    query_emb = _embed(claim)
    if query_emb is None:
        return None  # st-Modell nicht verfügbar — semantic-Pfad deaktiviert

    best_score = 0.0
    best_key = None
    best_result = None
    for key, (ts, emb, result, dv) in _verdict_store.items():
        if emb is None or dv != current_dv:
            continue
        if time.time() - ts > DEFAULT_TTL:
            continue
        # Cosine: beide normalisiert -> Skalarprodukt
        score = float(np.dot(query_emb, emb))
        if score > best_score:
            best_score = score
            best_key = key
            best_result = result

    if best_score >= SEMANTIC_THRESHOLD and best_result is not None:
        logger.info(
            f"verdict_cache: SEMANTIC HIT cos={best_score:.3f} "
            f"for {claim[:60]!r} -> matched {best_key[:60]!r}"
        )
        return _annotate_hit(best_result, "semantic", best_score, claim,
                             matched_claim=best_key)
    return None


def _annotate_hit(result: dict, hit_type: str, score: float,
                   claim: str, matched_claim: str | None = None) -> dict:
    """Annotiert das Cache-Hit-Result mit Metadata für Debugging.
    Mutiert das Dict NICHT — gibt eine Kopie zurück."""
    annotated = dict(result)
    annotated["_cache_hit"] = {
        "type": hit_type,
        "score": round(score, 4),
        "matched_claim": matched_claim,
    }
    return annotated


def put(claim: str, result: dict) -> None:
    """Speichert ein Verdict-Result für späteren Cache-Lookup.

    Cache-Filter:
      - Confidence muss ≥ MIN_CONFIDENCE_FOR_CACHE sein
      - Verdict darf NICHT 'unverifiable' sein (Stream-Loss-Artefakte
        + low-info Verdicts werden nicht gecached)
      - Result muss gültiges JSON-Dict mit 'verdict' + 'confidence' sein
    """
    if not claim or not result:
        return
    verdict = result.get("verdict")
    confidence = result.get("confidence")
    if not verdict or verdict == "unverifiable":
        return
    if confidence is None or confidence < MIN_CONFIDENCE_FOR_CACHE:
        return

    norm = _normalize(claim)
    emb = _embed(claim)
    dv = get_data_version()
    _verdict_store[norm] = (time.time(), emb, result, dv)
    _enforce_size_limit()
    logger.info(
        f"verdict_cache: STORED {claim[:60]!r} "
        f"(verdict={verdict}, conf={confidence}, store_size={len(_verdict_store)})"
    )


def clear() -> None:
    """Vollständige Cache-Leerung (z.B. für Tests)."""
    _verdict_store.clear()


def stats() -> dict:
    """Cache-Stats für Diagnose."""
    now = time.time()
    valid = sum(1 for ts, *_ in _verdict_store.values()
                if now - ts <= DEFAULT_TTL)
    with_emb = sum(1 for _, emb, *_ in _verdict_store.values()
                   if emb is not None)
    return {
        "total": len(_verdict_store),
        "valid_unexpired": valid,
        "with_embedding": with_emb,
        "max_size": MAX_STORE_SIZE,
        "ttl_seconds": DEFAULT_TTL,
        "semantic_threshold": SEMANTIC_THRESHOLD,
    }
