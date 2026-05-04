"""CDC (Centers for Disease Control and Prevention) Newsroom RSS —
aktuelle Public-Health-Meldungen, Outbreak-Berichte, FDA/CDC Joint
Statements.

Free RSS-Feed ohne API-Key. Komplementär zu ECDC (Europe), WHO (Global)
+ MedlinePlus (Patientenaufklärung):
- US-Behörden-Sicht (CDC) auf aktuelle Public-Health-Themen
- Outbreak-Updates (Salmonella, Influenza, COVID, etc.)
- Tick-borne Diseases, vaccine-preventable Diseases, etc.

Trigger: pubmed_queries non-empty ODER who_relevant ODER ecdc_relevant
ODER outbreak/seuche-Stichworte im Claim.

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle).

RSS-Feed: https://tools.cdc.gov/api/v2/resources/media/132608.rss
"""

import logging
import re
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

CDC_NEWSROOM_RSS = "https://tools.cdc.gov/api/v2/resources/media/132608.rss"

# Strip generic HTML tags from descriptions
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", _HTML_TAG_RE.sub(" ", text)).strip()


def _claim_indicates_outbreak_topic(claim: str) -> bool:
    """Light-weight outbreak/public-health keyword match on raw claim."""
    if not claim:
        return False
    cl = claim.lower()
    keywords = (
        "outbreak", "ausbruch", "seuche", "epidemie", "pandemie",
        "salmonell", "norovir", "influenza", "grippe", "ebola",
        "tollwut", "rabies", "tick", "zecke", "fsme", "borrelio",
        "h5n1", "vogelgrippe", "mpox", "monkeypox", "polio",
        "masern", "measles", "covid", "sars-cov", "long covid",
        "impfquote", "vaccination rate", "cdc", "nih",
    )
    return any(kw in cl for kw in keywords)


async def search_cdc_newsroom(analysis: dict) -> dict:
    """Fetch latest CDC Newsroom items, filter by entity/keyword overlap.

    Returns most-recent matching items (up to 5).
    """
    empty = {"source": "CDC Newsroom", "type": "public_health_news", "results": []}

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    entities = (analysis or {}).get("entities", []) or []
    queries = (analysis or {}).get("pubmed_queries", []) or []

    try:
        async with polite_client(timeout=15.0) as client:
            resp = await client.get(CDC_NEWSROOM_RSS)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
    except Exception as e:
        logger.warning(f"CDC Newsroom RSS fetch failed: {e}")
        return empty

    items: list[dict] = []
    for item in root.findall(".//item"):
        title = _strip_html((item.findtext("title") or "").strip())
        desc = _strip_html((item.findtext("description") or "").strip())
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()

        if not title:
            continue
        items.append({
            "title": title,
            "description": desc,
            "link": link,
            "pubDate": pub_date,
        })

    if not items:
        logger.info("CDC Newsroom: 0 RSS items parsed")
        return empty

    # Filter by entity match OR query-word match OR outbreak keywords in claim
    haystack_keywords: list[str] = []
    haystack_keywords.extend(e.lower() for e in entities if len(e) >= 3)
    for q in queries[:3]:
        haystack_keywords.extend(
            w.lower() for w in q.split() if len(w) >= 4
        )

    matched: list[dict] = []
    for it in items:
        text = f"{it['title']} {it['description']}".lower()
        # Direct entity/query-word match
        if any(kw in text for kw in haystack_keywords):
            matched.append(it)
            continue

    # If no direct matches but claim itself indicates outbreak topic,
    # surface the 3 most recent items as "general context"
    if not matched and _claim_indicates_outbreak_topic(claim):
        matched = items[:3]

    if not matched:
        logger.info(
            f"CDC Newsroom: 0/{len(items)} matched on entities/queries "
            f"({len(haystack_keywords)} keywords tried)"
        )
        return empty

    # Limit to top 5
    matched = matched[:5]
    logger.info(f"CDC Newsroom: {len(matched)}/{len(items)} items matched")

    results: list[dict] = []
    for it in matched:
        # Parse pubDate to year if possible
        year = "—"
        pd = it.get("pubDate", "")
        if pd:
            try:
                # RSS pubDate: "Thu, 23 Apr 2026 18:00:00 GMT"
                dt = datetime.strptime(pd[:25].strip(), "%a, %d %b %Y %H:%M:%S")
                year = str(dt.year)
            except Exception:
                pass

        desc = it.get("description") or ""
        # Truncate description for synthesizer
        desc_short = (desc[:400] + "…") if len(desc) > 400 else desc

        results.append({
            "indicator_name": it["title"],
            "indicator": "cdc_newsroom_item",
            "country": "USA",
            "year": year,
            "topic": "cdc_public_health_news",
            "display_value": desc_short,
            "description": pd,
            "url": it["link"],
            "secondary_url": "",
            "source": "CDC Newsroom (Centers for Disease Control and Prevention, USA)",
        })

    return {
        "source": "CDC Newsroom",
        "type": "public_health_news",
        "results": results,
    }
