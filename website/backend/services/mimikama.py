"""Mimikama — DACH Hoax-Faktencheck-Plattform.

Spezialisiert auf Social-Media-Falschmeldungen, Verschwörungstheorien,
KI-generierte Bilder, WhatsApp-/Facebook-Hoaxes. Komplementär zu APA-
Faktencheck (das primär klassische Boulevard-Themen abdeckt) und GADMO
(generischer Aggregator).

Datenquelle: RSS-Feed https://www.mimikama.org/feed/ (stündliches Update)
Output-Format: ClaimReview-Style, kompatibel zu reranker.

Use-Case:
- Social-Media-Hoax-Erkennung
- WhatsApp-Kettenbriefe
- KI-generierte Bilder/Videos
- Verschwörungs-Klassiker (Erde flach, Mondlandung, Chemtrails)
"""

import asyncio
import logging
import time
from xml.etree import ElementTree as ET

import httpx

logger = logging.getLogger("evidora")

FEED_CACHE_TTL = 3600  # 1h
FEED_URL = "https://www.mimikama.org/feed/"
FEED_NAME = "Mimikama"

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


def _parse_rss(xml_text: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning(f"Failed to parse Mimikama RSS: {e}")
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
            import re as _re
            description = _re.sub(r"<[^>]+>", "", desc_el.text)
            description = description.strip()[:300]

        if not title or not link:
            continue
        items.append({
            "title": title,
            "url": link,
            "date": date,
            "description": description,
            "source": FEED_NAME,
            "country": "DACH",
        })
    return items


async def fetch_mimikama(client: httpx.AsyncClient | None = None) -> list[dict]:
    global _cache, _cache_time

    now = time.time()
    if _cache is not None and (now - _cache_time) < FEED_CACHE_TTL:
        return _cache

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=15.0)
        own_client = True

    try:
        try:
            response = await client.get(FEED_URL, headers=_BROWSER_HEADERS, timeout=15)
            response.raise_for_status()
            items = _parse_rss(response.text)
            logger.info(f"Mimikama fetched: {len(items)} items")
        except Exception as e:
            logger.warning(f"Mimikama fetch failed: {e}")
            items = []

        _cache = items
        _cache_time = now
        return items
    finally:
        if own_client:
            await client.aclose()


def claim_mentions_mimikama_cached(claim: str) -> bool:
    """Synchronous trigger: fire on any claim with 10+ chars.

    Reranker filtert thematisch via FACTCHECK_THRESHOLD = 0.55.
    """
    if not claim or len(claim.strip()) < 10:
        return False
    return True


async def search_mimikama(analysis: dict) -> dict:
    empty = {
        "source": "Mimikama",
        "type": "factcheck",
        "results": [],
    }

    items = await fetch_mimikama()
    if not items:
        return empty

    results = []
    for it in items:
        results.append({
            "title": it["title"],
            "url": it["url"],
            "date": it["date"],
            "rating": it.get("description", "")[:200],
            "source": it["source"],
            "country": it["country"],
            "indicator": "mimikama_hoax_check",
        })

    return {
        "source": "Mimikama",
        "type": "factcheck",
        "results": results,
    }
