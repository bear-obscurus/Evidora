import httpx
import logging
import re
from xml.etree import ElementTree

logger = logging.getLogger("evidora")

# GADMO member RSS feeds (German-language fact-checks)
FEEDS = [
    {
        "name": "APA Faktencheck",
        "url": "https://apa.at/faktencheck/feed/",
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


def _semantic_match(claim: str, items: list[dict], top_k: int = 5) -> list[dict]:
    """Rank items by semantic similarity to the claim."""
    try:
        from services.reranker import _load_model, _model
        if not _load_model() or _model is None:
            return None  # Signal to use keyword fallback

        from sentence_transformers import util

        claim_embedding = _model.encode(claim, convert_to_tensor=True)
        texts = [f"{item['title']} {re.sub(r'<[^>]+>', '', item['description'])}" for item in items]
        item_embeddings = _model.encode(texts, convert_to_tensor=True)
        scores = util.cos_sim(claim_embedding, item_embeddings)[0]

        scored = sorted(zip(items, scores.tolist()), key=lambda x: x[1], reverse=True)
        # Only return items with similarity > 0.25 (threshold for relevance)
        return [item for item, score in scored[:top_k] if score > 0.25]
    except Exception as e:
        logger.debug(f"GADMO semantic matching failed: {e}")
        return None


async def search_gadmo(analysis: dict) -> dict:
    """Search GADMO member fact-check feeds for relevant articles."""
    claim = analysis.get("claim", "")
    entities = analysis.get("entities", [])
    factcheck_queries = analysis.get("factcheck_queries", [])
    keywords = entities + factcheck_queries

    if not keywords and not claim:
        return {"source": "GADMO Faktenchecks", "type": "factcheck", "results": []}

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

    if not all_items:
        return {"source": "GADMO Faktenchecks", "type": "factcheck", "results": []}

    # Try semantic matching first, fall back to keywords
    matched = None
    if claim:
        matched = _semantic_match(claim, all_items)

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
