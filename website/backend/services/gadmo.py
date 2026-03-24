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


def _matches(item: dict, keywords: list[str]) -> bool:
    """Check if an item matches any keyword."""
    text = f"{item['title']} {item['description']}".lower()
    return any(kw.lower() in text for kw in keywords)


async def search_gadmo(analysis: dict) -> dict:
    """Search GADMO member fact-check feeds for relevant articles."""
    entities = analysis.get("entities", [])
    factcheck_queries = analysis.get("factcheck_queries", [])
    keywords = entities + factcheck_queries

    if not keywords:
        return {"source": "GADMO Faktenchecks", "type": "factcheck", "results": []}

    results = []

    async with httpx.AsyncClient(timeout=15.0) as client:
        for feed in FEEDS:
            try:
                resp = await client.get(feed["url"])
                resp.raise_for_status()
                items = _extract_items(resp.text)

                for item in items:
                    if _matches(item, keywords):
                        # Strip HTML tags from description
                        clean_desc = re.sub(r"<[^>]+>", "", item["description"]).strip()
                        results.append({
                            "title": f"{feed['name']}: {item['title']}",
                            "url": item["url"],
                            "description": clean_desc[:200],
                            "source": feed["name"],
                        })
            except Exception as e:
                logger.warning(f"GADMO: Failed to fetch {feed['name']}: {e}")

    return {
        "source": "GADMO Faktenchecks",
        "type": "factcheck",
        "results": results[:5],
    }
