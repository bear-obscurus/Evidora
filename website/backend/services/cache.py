import hashlib
import json
import logging
import time

logger = logging.getLogger("evidora")

# In-memory cache: key -> (timestamp, data)
_cache: dict[str, tuple[float, dict]] = {}

# Default TTL: 30 minutes
DEFAULT_TTL = 1800


def _make_key(source: str, analysis: dict) -> str:
    """Create a deterministic cache key from source name and analysis."""
    relevant = {
        "claim": analysis.get("claim", ""),
        "category": analysis.get("category", ""),
        "entities": sorted(analysis.get("entities", [])),
        "pubmed_queries": analysis.get("pubmed_queries", []),
    }
    raw = f"{source}:{json.dumps(relevant, sort_keys=True)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def get(source: str, analysis: dict, ttl: int = DEFAULT_TTL) -> dict | None:
    """Return cached result if available and not expired."""
    key = _make_key(source, analysis)
    entry = _cache.get(key)
    if entry is None:
        return None
    timestamp, data = entry
    if time.time() - timestamp > ttl:
        del _cache[key]
        return None
    logger.info(f"Cache hit: {source}")
    return data


def put(source: str, analysis: dict, data: dict) -> None:
    """Store result in cache."""
    key = _make_key(source, analysis)
    _cache[key] = (time.time(), data)


def clear() -> None:
    """Clear all cached entries."""
    _cache.clear()


def stats() -> dict:
    """Return cache statistics."""
    now = time.time()
    valid = sum(1 for ts, _ in _cache.values() if now - ts < DEFAULT_TTL)
    return {"total": len(_cache), "valid": valid, "expired": len(_cache) - valid}
