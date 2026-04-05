import httpx
import logging
import re
import time
from xml.etree import ElementTree

logger = logging.getLogger("evidora")

# Cached feed items + pre-computed embeddings
_feed_cache: list[dict] | None = None
_feed_embeddings = None  # Tensor of shape (N, 384)
_feed_cache_ts: float = 0
FEED_CACHE_TTL = 3600  # 1 hour

# GADMO member RSS feeds (German-language fact-checks)
FEEDS = [
    {
        "name": "APA Faktencheck",
        "url": "https://apa.at/faktencheck/feed/",
        "lang": "de",
    },
    {
        "name": "Correctiv Faktencheck",
        "url": "https://correctiv.org/faktencheck/feed/",
        "lang": "de",
    },
    {
        "name": "dpa Faktencheck",
        "url": "https://www.dpa.com/de/faktencheck.rss",
        "lang": "de",
    },
    {
        "name": "Mimikama",
        "url": "https://www.mimikama.org/feed/",
        "lang": "de",
    },
    {
        "name": "AFP Faktencheck",
        "url": "https://faktencheck.afp.com/list/all/all/all/38970/rss",
        "lang": "de",
    },
]


def _extract_items(xml_text: str) -> list[dict]:
    """Parse RSS feed items, with regex fallback for malformed XML."""
    items = []

    # Try proper XML parsing first
    try:
        root = ElementTree.fromstring(xml_text)
        for item in root.findall(".//item"):
            title_el = item.find("title")
            link_el = item.find("link")
            desc_el = item.find("description")
            if title_el is not None and link_el is not None:
                items.append({
                    "title": title_el.text or "",
                    "url": link_el.text or "",
                    "description": (desc_el.text or "")[:300] if desc_el is not None else "",
                })
        return items
    except ElementTree.ParseError:
        pass

    # Regex fallback for CDATA-heavy feeds
    raw_items = re.findall(r"<item>(.*?)</item>", xml_text, re.DOTALL)
    for raw in raw_items:
        title = re.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", raw, re.DOTALL)
        link = re.search(r"<link>(.*?)</link>", raw)
        desc = re.search(r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>", raw, re.DOTALL)
        if title and link:
            items.append({
                "title": title.group(1).strip(),
                "url": link.group(1).strip(),
                "description": (desc.group(1).strip()[:300] if desc else ""),
            })
    return items


def _matches_keywords(item: dict, keywords: list[str]) -> bool:
    """Check if an item matches any keyword (fallback without sentence-transformers)."""
    text = f"{item['title']} {item['description']}".lower()
    return any(kw.lower() in text for kw in keywords)


async def _fetch_all_feeds() -> list[dict]:
    """Fetch all GADMO RSS feeds and return combined items."""
    all_items = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        for feed in FEEDS:
            try:
                resp = await client.get(feed["url"])
                resp.raise_for_status()
                items = _extract_items(resp.text)
                for item in items:
                    item["feed_name"] = feed["name"]
                all_items.extend(items)
            except Exception as e:
                logger.warning(f"GADMO: Failed to fetch {feed['name']}: {e}")
    return all_items


async def prefetch_feeds():
    """Fetch all feeds and pre-compute embeddings (called at startup + periodically)."""
    global _feed_cache, _feed_embeddings, _feed_cache_ts

    items = await _fetch_all_feeds()
    if not items:
        logger.warning("GADMO prefetch: no items from any feed")
        return

    _feed_cache = items
    _feed_cache_ts = time.time()

    # Pre-compute embeddings if model is available
    try:
        from services.reranker import _load_model, _model
        if _load_model() and _model is not None:
            texts = [f"{item['title']} {re.sub(r'<[^>]+>', '', item['description'])}" for item in items]
            _feed_embeddings = _model.encode(texts, convert_to_tensor=True)
            logger.info(f"GADMO prefetch: {len(items)} items cached, embeddings computed")
        else:
            _feed_embeddings = None
            logger.info(f"GADMO prefetch: {len(items)} items cached (no embeddings)")
    except Exception as e:
        _feed_embeddings = None
        logger.warning(f"GADMO prefetch embedding failed: {e}")


def _has_entity_overlap(item: dict, entities: list[str]) -> bool:
    """Check if any entity appears in the item's title or description."""
    if not entities:
        return True  # No entities to check — accept all
    text = f"{item['title']} {item['description']}".lower()
    return any(e.lower() in text for e in entities if len(e) >= 3)


def _semantic_match(claim: str, items: list[dict], entities: list[str] | None = None, top_k: int = 5) -> list[dict]:
    """Rank items by semantic similarity with entity overlap requirement.

    Requires both sufficient cosine similarity AND at least one entity
    from the claim analysis to prevent generic word matches.
    """
    try:
        from services.reranker import _load_model, _model
        if not _load_model() or _model is None:
            return None  # Signal to use keyword fallback

        from sentence_transformers import util

        claim_embedding = _model.encode(claim, convert_to_tensor=True)

        # Use pre-computed embeddings if items match the cache
        if _feed_embeddings is not None and _feed_cache is not None and items is _feed_cache:
            item_embeddings = _feed_embeddings
        else:
            texts = [f"{item['title']} {re.sub(r'<[^>]+>', '', item['description'])}" for item in items]
            item_embeddings = _model.encode(texts, convert_to_tensor=True)

        scores = util.cos_sim(claim_embedding, item_embeddings)[0]

        scored = sorted(zip(items, scores.tolist()), key=lambda x: x[1], reverse=True)
        # Require both semantic similarity AND entity overlap
        kept = [
            item for item, score in scored[:top_k * 3]
            if score > 0.35 and _has_entity_overlap(item, entities or [])
        ][:top_k]
        if scored:
            top_score = scored[0][1]
            logger.info(
                f"GADMO semantic: top score {top_score:.3f}, "
                f"kept {len(kept)}/{min(top_k * 3, len(scored))} "
                f"(entities: {entities})"
            )
        return kept
    except Exception as e:
        logger.debug(f"GADMO semantic matching failed: {e}")
        return None


async def search_gadmo(analysis: dict) -> dict:
    """Search GADMO member fact-check feeds for relevant articles."""
    global _feed_cache, _feed_cache_ts

    claim = analysis.get("claim", "")
    entities = analysis.get("entities", [])
    factcheck_queries = analysis.get("factcheck_queries", [])
    keywords = entities + factcheck_queries

    if not keywords and not claim:
        return {"source": "GADMO Faktenchecks", "type": "factcheck", "results": []}

    # Use cached items if fresh, otherwise fetch
    now = time.time()
    if _feed_cache is not None and now - _feed_cache_ts < FEED_CACHE_TTL:
        all_items = _feed_cache
    else:
        all_items = await _fetch_all_feeds()
        if all_items:
            _feed_cache = all_items
            _feed_cache_ts = now

    if not all_items:
        return {"source": "GADMO Faktenchecks", "type": "factcheck", "results": []}

    # Try semantic matching first, fall back to keywords
    matched = None
    if claim:
        matched = _semantic_match(claim, all_items, entities=entities)

    if matched is None:
        # Keyword fallback
        matched = [item for item in all_items if _matches_keywords(item, keywords)]

    results = []
    for item in matched[:5]:
        clean_desc = re.sub(r"<[^>]+>", "", item["description"]).strip()
        feed_name = item.get("feed_name", "GADMO")
        results.append({
            "title": f"{feed_name}: {item['title']}",
            "url": item["url"],
            "description": clean_desc[:200],
            "source": feed_name,
        })

    return {
        "source": "GADMO Faktenchecks",
        "type": "factcheck",
        "results": results,
    }
