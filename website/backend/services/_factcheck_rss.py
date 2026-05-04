"""Gemeinsamer Helper für internationale Fact-Check-RSS-Connectoren.

Die folgenden Services delegieren an diesen Helper:
- services/snopes.py (US, EN — größte EN-Faktencheck-DB)
- services/correctiv.py (DE — investigative + Faktenchecks)
- services/full_fact.py (UK, EN)
- services/bellingcat.py (UK/global, EN — OSINT)
- services/factcheck_org.py (US, EN — Annenberg)

Pattern stammt aus services/cdc_newsroom.py und services/mimikama.py.

Ohne API-Key, alle nutzen polite_client(timeout=15s) mit korrektem
User-Agent. XML-Parsing via stdlib xml.etree (kein feedparser nötig).
"""

import logging
import re
from datetime import datetime
from xml.etree import ElementTree as ET

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", _HTML_TAG_RE.sub(" ", text)).strip()


def _parse_rss_year(pub_date: str) -> str:
    """RFC-822 pubDate → year string. Returns '—' on failure."""
    if not pub_date:
        return "—"
    try:
        # RSS pubDate: "Mon, 04 May 2026 02:00:00 +0000"
        dt = datetime.strptime(pub_date[:25].strip(), "%a, %d %b %Y %H:%M:%S")
        return str(dt.year)
    except Exception:
        return "—"


def _entity_or_query_match(text: str, entities: list[str], queries: list[str]) -> bool:
    """Stricter match (Pattern aus europe_pmc.py):

    EITHER:
      (a) mindestens eine Entity (>=3 chars, multi-word also möglich)
          appears in text — single hit reicht, weil Entities aus NER
          spezifisch sind ('Mozart', 'Salmonella', 'Vasektomie')
      OR:
      (b) mindestens 2 verschiedene Query-Wörter (>=4 chars) aus
          pubmed/factcheck_queries appears in text — verhindert
          Single-Word-False-Positives bei generischen Begriffen wie
          'document', 'study', 'effect'

    Falls beide Listen leer → False.
    """
    text_lc = text.lower()

    # (a) Entity-Match — single hit reicht
    entity_terms = [e.lower() for e in entities if len(e) >= 3]
    if any(e in text_lc for e in entity_terms):
        return True

    # (b) Query-Word-Match — mindestens 2 verschiedene Wörter
    query_words: set[str] = set()
    for q in queries[:4]:
        for w in q.split():
            if len(w) >= 4:
                query_words.add(w.lower())
    if not query_words:
        return False
    hits = sum(1 for w in query_words if w in text_lc)
    return hits >= 2


async def search_factcheck_rss(
    *,
    feed_url: str,
    name: str,
    source_label: str,
    indicator: str,
    country: str,
    analysis: dict,
    max_results: int = 5,
) -> dict:
    """Generic factcheck-RSS search.

    Args:
        feed_url: RSS-Feed-URL (https://...)
        name: kurzer Service-Name (z.B. "Snopes", "Correctiv")
        source_label: vollständiger Quellen-Label für UI/Synthesizer
        indicator: reranker-Indicator-Name (z.B. "snopes_factcheck_item")
        country: ISO-/Sigel-Country-Tag (z.B. "USA", "DE", "UK")
        analysis: Standard analysis dict
        max_results: Top-N Treffer

    Returns:
        Standard {source, type, results} dict.
    """
    empty = {"source": name, "type": "factcheck", "results": []}

    entities = (analysis or {}).get("entities", []) or []
    queries: list[str] = []
    queries.extend((analysis or {}).get("pubmed_queries", []) or [])
    queries.extend((analysis or {}).get("factcheck_queries", []) or [])

    if not entities and not queries:
        return empty

    try:
        async with polite_client(timeout=15.0) as client:
            resp = await client.get(feed_url, follow_redirects=True)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
    except Exception as e:
        logger.warning(f"{name} RSS fetch failed: {e}")
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
        logger.info(f"{name}: 0 RSS items parsed")
        return empty

    matched: list[dict] = []
    for it in items:
        text = f"{it['title']} {it['description']}".lower()
        if _entity_or_query_match(text, entities, queries):
            matched.append(it)

    if not matched:
        logger.info(
            f"{name}: 0/{len(items)} matched on entities/queries"
        )
        return empty

    matched = matched[:max_results]
    logger.info(f"{name}: {len(matched)}/{len(items)} items matched")

    results: list[dict] = []
    for it in matched:
        year = _parse_rss_year(it.get("pubDate", ""))
        desc = it.get("description") or ""
        desc_short = (desc[:400] + "…") if len(desc) > 400 else desc

        results.append({
            "indicator_name": it["title"],
            "indicator": indicator,
            "country": country,
            "year": year,
            "topic": f"{name.lower()}_factcheck_item",
            "display_value": desc_short,
            "description": it.get("pubDate", ""),
            "url": it["link"],
            "secondary_url": "",
            "source": source_label,
        })

    return {
        "source": name,
        "type": "factcheck",
        "results": results,
    }
