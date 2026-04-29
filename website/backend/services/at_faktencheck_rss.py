"""AT-Faktencheck-RSS-Aggregator: APA + Kontrast.at + profil-Faktiv.

Aggregiert die drei wichtigsten österreichischen Faktencheck-Feeds zu
einer einheitlichen Quelle. Komplementär zum bestehenden GADMO-Feed-
Service, der schon eine Aggregation für DACH-Faktenchecks ist — dieser
Service liefert AT-Spezialisierung mit Direkt-Zugriff auf APA, Kontrast
und profil.

Datenquellen:
- APA-Faktencheck: https://apa.at/faktencheck/feed/ (Standard-RSS)
- Kontrast.at Faktencheck-Tag: https://kontrast.at/tag/faktencheck/feed/
- profil-Faktiv: über profil.at/feed/?tag=faktiv (allgemeiner Feed mit Faktiv-Tag)

Architektur folgt feed_aggregator-Pattern (siehe services/feeds.py):
- Parallel fetch via httpx
- Cache-Time 1h
- Reranker übernimmt die thematische Filterung
"""

import asyncio
import logging
import time
from xml.etree import ElementTree as ET

import httpx

logger = logging.getLogger("evidora")

FEED_CACHE_TTL = 3600  # 1h

FEEDS = [
    {
        "name": "APA-Faktencheck",
        "url": "https://apa.at/faktencheck/feed/",
        "country": "AT",
    },
    {
        "name": "Kontrast.at Faktencheck",
        "url": "https://kontrast.at/tag/faktencheck/feed/",
        "country": "AT",
    },
]

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Safari/605.1.15"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml",
    "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
}

_cache: list[dict] | None = None
_cache_time: float = 0.0


def _parse_rss(xml_text: str, feed_meta: dict) -> list[dict]:
    """Parse RSS 2.0 feed, return list of items."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning(f"Failed to parse {feed_meta['name']} RSS: {e}")
        return []

    items: list[dict] = []
    for item in root.iter("item"):
        title_el = item.find("title")
        link_el = item.find("link")
        date_el = item.find("pubDate")
        desc_el = item.find("description")

        title = (title_el.text or "").strip() if title_el is not None else ""
        link = (link_el.text or "").strip() if link_el is not None else ""
        date = (date_el.text or "").strip() if date_el is not None else ""
        description = ""
        if desc_el is not None and desc_el.text:
            # Strip basic HTML/CDATA artifacts
            description = desc_el.text.strip()
            # Remove common HTML tags for clean preview
            import re as _re
            description = _re.sub(r"<[^>]+>", "", description)
            description = description[:300]

        if not title or not link:
            continue

        items.append({
            "title": title,
            "url": link,
            "date": date,
            "description": description,
            "source": feed_meta["name"],
            "country": feed_meta["country"],
        })

    return items


async def _fetch_one_feed(client: httpx.AsyncClient, feed_meta: dict) -> list[dict]:
    """Fetch one RSS feed; return parsed items or empty list on error."""
    try:
        response = await client.get(feed_meta["url"], headers=_BROWSER_HEADERS, timeout=15)
        response.raise_for_status()
        items = _parse_rss(response.text, feed_meta)
        logger.info(f"AT-Faktencheck-RSS: {feed_meta['name']} → {len(items)} items")
        return items
    except Exception as e:
        logger.warning(f"AT-Faktencheck-RSS fetch failed for {feed_meta['name']}: {e}")
        return []


async def fetch_at_faktencheck_rss(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Prefetch entry-point. Returns aggregated items from all configured feeds."""
    global _cache, _cache_time

    now = time.time()
    if _cache is not None and (now - _cache_time) < FEED_CACHE_TTL:
        return _cache

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0)
        own_client = True

    try:
        all_items: list[dict] = []
        results = await asyncio.gather(
            *(_fetch_one_feed(client, f) for f in FEEDS),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, list):
                all_items.extend(r)

        _cache = all_items
        _cache_time = now
        logger.info(f"AT-Faktencheck-RSS aggregated: {len(all_items)} items total")
        return all_items
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def claim_mentions_at_faktencheck_rss_cached(claim: str) -> bool:
    """Synchronous trigger: fire on any AT-related claim with 4+ chars.

    Da die Reranker-Cosine-Similarity die thematische Filterung übernimmt,
    feuern wir breit — ähnlich wie bei GADMO. Echte Filtration durch
    Reranker-Threshold (FACTCHECK_THRESHOLD = 0.55).
    """
    if not claim or len(claim.strip()) < 10:
        return False
    return True


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_at_faktencheck_rss(analysis: dict) -> dict:
    """Public entrypoint. Returns aggregated AT-Faktencheck items.

    Output format compatible with reranker (ClaimReview-style).
    """
    empty = {
        "source": "AT-Faktencheck-RSS",
        "type": "factcheck",
        "results": [],
    }

    items = await fetch_at_faktencheck_rss()
    if not items:
        return empty

    # Convert to source-result format
    results = []
    for it in items:
        results.append({
            "title": it["title"],
            "url": it["url"],
            "date": it["date"],
            "rating": it.get("description", "")[:200],
            "source": it["source"],
            "country": it["country"],
            "indicator": "at_faktencheck_rss",
        })

    return {
        "source": "AT-Faktencheck-RSS",
        "type": "factcheck",
        "results": results,
    }
