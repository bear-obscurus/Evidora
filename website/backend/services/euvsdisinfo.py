"""EUvsDisinfo — geopolitische Desinformations-Erkennung via RSS-Feed.

EUvsDisinfo (EU East StratCom Task Force) dokumentiert pro-Kreml Desinformation
und FIMI-Kampagnen (Foreign Information Manipulation and Interference).

Der RSS-Feed enthält redaktionelle Analysen, Disinformation Reviews und Threat
Reports. Die Artikel referenzieren konkrete Desinformations-Fälle aus der
EUvsDisinfo-Datenbank (16.000+ Fälle).

Artikel werden beim Start gecacht und per Sentence Transformers semantisch
gegen Claims gematcht.
"""

import logging
import re
import time
from xml.etree import ElementTree

import httpx

logger = logging.getLogger("evidora")

FEED_URL = "https://euvsdisinfo.eu/feed/"

# In-memory cache
_feed_cache: list[dict] | None = None
_feed_embeddings = None
_feed_cache_ts: float = 0
FEED_CACHE_TTL = 3600  # 1 hour

# Keywords that indicate a claim might be related to geopolitical disinformation
DISINFO_KEYWORDS = [
    # German
    "desinformation", "propaganda", "fake news", "kreml", "russland", "russisch",
    "ukraine", "nato", "eu destabil", "manipulation", "troll", "bot",
    "verschwörung", "conspiracy", "geopolitik", "geopolitisch",
    "china", "peking", "beijing", "fimi",
    "einmischung", "interference", "beeinflussung", "unterwanderung",
    "informationskrieg", "information war", "hybrid", "krieg",
    "sanktionen", "sanctions", "annexion", "krim", "crimea",
    "separatist", "donbas", "donezk", "luhansk",
    # English
    "disinformation", "kremlin", "russia", "russian", "putin",
    "geopolitic", "destabilis", "destabiliz", "influence operation",
    "state media", "staatsmedien", "rt news", "sputnik",
]


def _is_disinfo_claim(analysis: dict) -> bool:
    """Check if a claim is related to geopolitical disinformation."""
    text = " ".join([
        analysis.get("claim", ""),
        analysis.get("subcategory", ""),
        " ".join(analysis.get("entities", [])),
    ]).lower()
    return any(kw in text for kw in DISINFO_KEYWORDS)


def _extract_items(xml_text: str) -> list[dict]:
    """Parse RSS feed items from EUvsDisinfo."""
    items = []
    try:
        root = ElementTree.fromstring(xml_text)
        # Namespace for content:encoded
        ns = {"content": "http://purl.org/rss/1.0/modules/content/"}

        for item in root.findall(".//item"):
            title_el = item.find("title")
            link_el = item.find("link")
            desc_el = item.find("description")
            content_el = item.find("content:encoded", ns)
            pub_date_el = item.find("pubDate")

            if title_el is None or link_el is None:
                continue

            # Extract categories/tags
            categories = [cat.text for cat in item.findall("category") if cat.text]

            # Get description — prefer short description, fall back to content excerpt
            description = ""
            if desc_el is not None and desc_el.text:
                description = re.sub(r"<[^>]+>", "", desc_el.text).strip()[:500]

            # Get full content for deeper matching
            full_text = ""
            if content_el is not None and content_el.text:
                full_text = re.sub(r"<[^>]+>", "", content_el.text).strip()[:2000]

            items.append({
                "title": title_el.text or "",
                "url": link_el.text or "",
                "description": description,
                "full_text": full_text,
                "date": pub_date_el.text if pub_date_el is not None else "",
                "categories": categories,
            })

    except ElementTree.ParseError as e:
        logger.warning(f"EUvsDisinfo RSS parse error: {e}")

    return items


async def _fetch_feed() -> list[dict]:
    """Fetch the EUvsDisinfo RSS feed."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(FEED_URL)
            resp.raise_for_status()
            items = _extract_items(resp.text)
            if items:
                logger.info(f"EUvsDisinfo: {len(items)} articles loaded from RSS")
            return items
    except Exception as e:
        logger.warning(f"EUvsDisinfo RSS fetch failed: {e}")
        return []


async def prefetch_feed():
    """Fetch feed and pre-compute embeddings (called at startup)."""
    global _feed_cache, _feed_embeddings, _feed_cache_ts

    items = await _fetch_feed()
    if not items:
        return

    _feed_cache = items
    _feed_cache_ts = time.time()

    try:
        from services.reranker import _load_model, _model
        if _load_model() and _model is not None:
            texts = [f"{item['title']} {item['description']}" for item in items]
            _feed_embeddings = _model.encode(texts, convert_to_tensor=True)
            logger.info(f"EUvsDisinfo: embeddings computed for {len(items)} articles")
        else:
            _feed_embeddings = None
    except Exception as e:
        _feed_embeddings = None
        logger.warning(f"EUvsDisinfo embedding failed: {e}")


def _semantic_match(claim: str, items: list[dict], top_k: int = 5) -> list[dict]:
    """Rank items by semantic similarity to the claim."""
    try:
        from services.reranker import _load_model, _model
        if not _load_model() or _model is None:
            return None

        from sentence_transformers import util

        claim_embedding = _model.encode(claim, convert_to_tensor=True)

        # Use pre-computed embeddings if available
        if _feed_embeddings is not None and _feed_cache is not None and items is _feed_cache:
            item_embeddings = _feed_embeddings
        else:
            texts = [f"{item['title']} {item['description']}" for item in items]
            item_embeddings = _model.encode(texts, convert_to_tensor=True)

        scores = util.cos_sim(claim_embedding, item_embeddings)[0]
        scored = sorted(zip(items, scores.tolist()), key=lambda x: x[1], reverse=True)
        return [item for item, score in scored[:top_k] if score > 0.25]

    except Exception as e:
        logger.debug(f"EUvsDisinfo semantic matching failed: {e}")
        return None


def _keyword_match(items: list[dict], keywords: list[str]) -> list[dict]:
    """Fallback keyword matching."""
    matched = []
    for item in items:
        text = f"{item['title']} {item['description']} {item['full_text']}".lower()
        if any(kw.lower() in text for kw in keywords):
            matched.append(item)
    return matched


async def search_euvsdisinfo(analysis: dict) -> dict:
    """Search EUvsDisinfo for geopolitical disinformation articles."""
    global _feed_cache, _feed_cache_ts

    claim = analysis.get("claim", "")
    entities = analysis.get("entities", [])
    factcheck_queries = analysis.get("factcheck_queries", [])
    keywords = entities + factcheck_queries

    if not claim and not keywords:
        return {"source": "EUvsDisinfo", "type": "factcheck", "results": []}

    # Use cached items if fresh
    now = time.time()
    if _feed_cache is not None and now - _feed_cache_ts < FEED_CACHE_TTL:
        all_items = _feed_cache
    else:
        all_items = await _fetch_feed()
        if all_items:
            _feed_cache = all_items
            _feed_cache_ts = now

    if not all_items:
        return {"source": "EUvsDisinfo", "type": "factcheck", "results": []}

    # Semantic matching first, keyword fallback
    matched = None
    if claim:
        matched = _semantic_match(claim, all_items)

    if matched is None:
        matched = _keyword_match(all_items, keywords)

    results = []
    for item in matched[:5]:
        categories = ", ".join(item["categories"][:3]) if item["categories"] else ""
        results.append({
            "title": item["title"],
            "url": item["url"],
            "description": item["description"][:200],
            "source": "EUvsDisinfo",
            "date": item["date"],
            "categories": categories,
        })

    return {
        "source": "EUvsDisinfo",
        "type": "factcheck",
        "results": results,
    }
