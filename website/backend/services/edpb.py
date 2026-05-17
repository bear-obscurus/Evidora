"""EDPB — European Data Protection Board.

Verbindliche GDPR-Guidelines, Opinions und Empfehlungen des EDPB für alle
27 EU-Mitgliedstaaten. Höchste Datenschutz-Autorität in der EU.

Datenquelle: zwei HTML-Listings (kein offizielles RSS verfügbar):
  - News:         https://www.edpb.europa.eu/news/news_en
  - Publications: https://www.edpb.europa.eu/our-work-tools/our-documents_en

Hinweis zu fehlendem RSS: Die in der Aufgabenstellung genannten Endpunkte
``/news/rss_en.xml`` und ``/publications/rss_en.xml`` liefern HTTP 404
(Stand 2026-05-17, Drupal-10-Migration). Da der Aufgaben-Brief explizit
einen Fallback zu "HTML-Listing" erlaubt, parsen wir die EDPB-Listing-
Seiten direkt via Regex/ElementTree-Fragmente. Die HTML-Struktur ist
konsistent (CSS-Klasse ``node__title h6`` + ``news-date``).

Lizenz: EU-Reuse-Decision (2011/833/EU) / CC BY 4.0.

Politische Guardrails:
- Nur Behörden-Information; keine Wertung der EDPB-Position
- Reine Quellen-Wiedergabe (Titel + Direct-Link + Datum)
"""

import asyncio
import logging
import re
import time
from html import unescape

import httpx

from services._http_polite import USER_AGENT
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

CACHE_TTL_SECONDS = 24 * 3600  # 24h
PREFETCH_LIMIT = 30  # neueste 30 Items pro Listing

_BASE = "https://www.edpb.europa.eu"
SOURCES = (
    {
        "name": "EDPB News",
        "url": f"{_BASE}/news/news_en",
        "kind": "news",
    },
    {
        "name": "EDPB Publications",
        "url": f"{_BASE}/our-work-tools/our-documents_en",
        "kind": "publication",
    },
)

_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en;q=0.9,de;q=0.8",
}

_cache: list[dict] | None = None
_cache_time: float = 0.0


# ---------------------------------------------------------------------------
# HTML-Parsing
# ---------------------------------------------------------------------------
# Drupal-10-Listings auf edpb.europa.eu nutzen für jedes Item ein konstantes
# ``<h4 class="node__title h6 m-0">`` mit einem inneren ``<a href="…">Titel</a>``.
# Direkt darunter sitzt ``<span class="news-date …">DATUM</span>``.

_RE_NODE = re.compile(
    r'<h4 class="node__title h6 m-0">\s*'
    r'<a href="(?P<href>[^"]+)"[^>]*title="(?P<title>[^"]*)"',
    re.IGNORECASE,
)
_RE_DATE = re.compile(
    r'<span class="news-date[^"]*">\s*(?P<date>[^<]+?)\s*</span>',
    re.IGNORECASE,
)
_RE_PUB_TYPE = re.compile(
    r'publication-type/[^_]+_en"[^>]*>(?P<type>[^<]+)</a>',
    re.IGNORECASE,
)
_RE_TOPIC = re.compile(
    r'/topic/[^_]+_en"[^>]*>(?P<topic>[^<]+)</a>',
    re.IGNORECASE,
)
_RE_YEAR = re.compile(r'\b(19|20)\d{2}\b')


def _slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")[:80]


def _parse_listing(html: str, source_meta: dict) -> list[dict]:
    """Extract teasers from one EDPB listing page.

    Wir splitten den HTML-Body an jeder ``node__title``-Marke und ziehen
    aus jedem Chunk Titel + Date + (optional) Publication-Type + Topic.
    """
    items: list[dict] = []
    # Split jeweils am Anker; das erste Stück ist der Listing-Header
    chunks = re.split(r'(?=<h4 class="node__title h6 m-0">)', html)
    for chunk in chunks[1:PREFETCH_LIMIT * 3 + 1]:  # Sicherheits-Cap
        m_node = _RE_NODE.search(chunk)
        if not m_node:
            continue
        href = m_node.group("href").strip()
        title = unescape(m_node.group("title").strip())
        if not href or not title:
            continue
        if href.startswith("/"):
            url = f"{_BASE}{href}"
        else:
            url = href

        m_date = _RE_DATE.search(chunk)
        date_str = unescape(m_date.group("date").strip()) if m_date else ""

        m_year = _RE_YEAR.search(date_str)
        year = m_year.group(0) if m_year else ""

        m_type = _RE_PUB_TYPE.search(chunk)
        pub_type = unescape(m_type.group("type").strip()) if m_type else ""

        topics = [unescape(t).strip() for t in _RE_TOPIC.findall(chunk)]

        items.append({
            "title": title,
            "url": url,
            "date": date_str,
            "year": year,
            "publication_type": pub_type,
            "topics": topics[:5],
            "kind": source_meta["kind"],
            "source": source_meta["name"],
        })
        if len(items) >= PREFETCH_LIMIT:
            break

    return items


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
async def _fetch_one(client: httpx.AsyncClient, source: dict) -> list[dict]:
    try:
        r = await client.get(source["url"], headers=_HEADERS, timeout=20.0,
                             follow_redirects=True)
        r.raise_for_status()
        items = _parse_listing(r.text, source)
        logger.info(f"EDPB {source['name']}: {len(items)} items")
        return items
    except Exception as e:
        logger.warning(f"EDPB fetch failed for {source['name']}: {e}")
        return []


async def fetch_edpb(client=None) -> list:
    """Prefetch beider Listings (News + Publications). Cache 24h.

    Wird auch im data_updater als Hook benutzt; nicht-blockierend, schlägt
    fehl-tolerant in []-Result-Set zurück.
    """
    global _cache, _cache_time
    now = time.time()
    if _cache is not None and (now - _cache_time) < CACHE_TTL_SECONDS:
        return _cache

    own_client = False
    if client is None:
        client = polite_client(timeout=20.0)
        own_client = True

    try:
        results = await asyncio.gather(
            *(_fetch_one(client, s) for s in SOURCES),
            return_exceptions=True,
        )
        merged: list[dict] = []
        for r in results:
            if isinstance(r, list):
                merged.extend(r)

        _cache = merged
        _cache_time = now
        logger.info(f"EDPB prefetch: {len(merged)} items total")
        return merged
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_EDPB_TERMS = (
    "edpb",
    "european data protection board",
    "europäischer datenschutzausschuss",
    "europaeischer datenschutzausschuss",
    "datenschutzausschuss eu",
    "datenschutzausschuss",  # standalone — EU-Behörde, semantisch eindeutig
    "dsgvo-leitlinie", "dsgvo leitlinie",
    "gdpr-guideline", "gdpr guideline",
    "datenschutz-beschluss eu",
    "datenschutz-empfehlung",
    "privacy-by-design leitlinie", "privacy by design leitlinie",
    "auftragsverarbeitung leitlinie",
    "drittlandsübermittlung leitlinie", "drittlandsuebermittlung leitlinie",
    "drittlandsübermittlung empfehlung", "drittlandsuebermittlung empfehlung",
    "drittlandsübermittlung", "drittlandsuebermittlung",
    "article 29 working party", "art-29-gruppe", "art. 29 working party",
    "wp29",
)

_EDPB_COMPOSITE_DP_TERMS = (
    "dsgvo", "gdpr", "datenschutz", "privacy",
)
_EDPB_COMPOSITE_EU_TERMS = (
    "eu-leitlinie", "eu leitlinie", "eu-guideline", "eu guideline",
    "eu-aufsicht", "eu-empfehlung", "eu-beschluss",
    "europäische datenschutz", "european data protection",
)


def _claim_mentions_edpb(claim_lc: str) -> bool:
    if any(t in claim_lc for t in _EDPB_TERMS):
        return True
    has_dp = any(t in claim_lc for t in _EDPB_COMPOSITE_DP_TERMS)
    has_eu = any(t in claim_lc for t in _EDPB_COMPOSITE_EU_TERMS)
    if has_dp and has_eu:
        return True
    return False


def claim_mentions_edpb_cached(claim: str) -> bool:
    return _claim_mentions_edpb((claim or "").lower())


# ---------------------------------------------------------------------------
# Keyword-Scoring
# ---------------------------------------------------------------------------
_TOKEN_RE = re.compile(r"[a-zäöüßéèêíóúñ0-9]+", re.IGNORECASE)
_STOPWORDS = {
    "die", "der", "das", "ein", "eine", "und", "oder", "ist", "sind",
    "with", "from", "this", "that", "have", "has", "are", "for", "the",
    "and", "but", "not", "auf", "von", "über", "im", "in", "an",
    "zu", "zur", "zum", "edpb", "gdpr", "dsgvo",
}


def _extract_keywords(claim: str) -> list[str]:
    toks = [t.lower() for t in _TOKEN_RE.findall(claim or "")]
    return [t for t in toks if len(t) >= 4 and t not in _STOPWORDS]


def _score_item(item: dict, keywords: list[str]) -> tuple[int, int, str]:
    """Return (score, recency_year, date_str) for sorting."""
    title_lc = item["title"].lower()
    topics_lc = " ".join(item.get("topics") or []).lower()
    desc_lc = (item.get("publication_type") or "").lower()
    haystack_title = f"{title_lc} {topics_lc}"

    title_hits = sum(1 for k in keywords if k in haystack_title)
    desc_hits = sum(1 for k in keywords if k in desc_lc)
    # Title-Hits zählen doppelt, Topic-Hits sind in haystack_title bereits drin
    score = title_hits * 2 + desc_hits

    year = 0
    try:
        year = int(item.get("year") or 0)
    except (TypeError, ValueError):
        year = 0
    return (score, year, item.get("date", ""))


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _to_result(item: dict) -> dict:
    slug_src = item.get("title") or item.get("url") or "edpb"
    indicator = f"edpb_{_slugify(slug_src)}"

    pub_type = item.get("publication_type") or ""
    topics = item.get("topics") or []
    kind = item.get("kind") or "news"

    if kind == "publication":
        prefix = pub_type or "EDPB-Publikation"
    else:
        prefix = "EDPB-Mitteilung"

    indicator_name = f"{prefix}: {item['title']}"[:240]

    display_value = item["title"]
    if pub_type:
        display_value = f"{pub_type}: {item['title']}"

    desc_parts = []
    if item.get("date"):
        desc_parts.append(f"Veröffentlicht: {item['date']}")
    if pub_type:
        desc_parts.append(f"Typ: {pub_type}")
    if topics:
        desc_parts.append("Themen: " + ", ".join(topics))
    desc_parts.append(
        "Status: offizielle EDPB-Veröffentlichung "
        "(verbindlich für 27 EU-Mitgliedstaaten unter GDPR Art. 70)."
    )
    description = " — ".join(desc_parts)

    return {
        "indicator_name": indicator_name,
        "indicator": indicator,
        "country": "EU",
        "country_name": "Europäische Union",
        "year": item.get("year") or "",
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": item["url"],
        "source": "EDPB (European Data Protection Board)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_edpb(analysis: dict) -> dict:
    """Keyword-Match auf gecachte EDPB-News + Publications, Top-5 zurück."""
    empty = {
        "source": "EDPB",
        "type": "data_protection_guidance",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_edpb(matchable):
        return empty

    items = await fetch_edpb()
    if not items:
        return empty

    keywords = _extract_keywords(f"{original} {claim}")
    if not keywords:
        # Trigger-only — return die letzten News
        scored = [(0, _safe_year(it), it.get("date", ""), it) for it in items]
    else:
        scored = []
        for it in items:
            score, year, date_str = _score_item(it, keywords)
            scored.append((score, year, date_str, it))
        scored = [s for s in scored if s[0] > 0]

    if not scored:
        return empty

    # Sortierung: Score-DESC, Jahr-DESC
    scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)

    top = [s[3] for s in scored[:5]]
    results = [_to_result(it) for it in top]

    return {
        "source": "EDPB",
        "type": "data_protection_guidance",
        "results": results,
    }


def _safe_year(it: dict) -> int:
    try:
        return int(it.get("year") or 0)
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# WIRING für main.py:
# from services.edpb import search_edpb, claim_mentions_edpb_cached
# if claim_mentions_edpb_cached(claim):
#     tasks.append(cached("EDPB", search_edpb, analysis))
#     queried_names.append("EDPB")
#
# data_updater.py:
# from services.edpb import fetch_edpb
# in prefetch_all(): await fetch_edpb(client)
# ---------------------------------------------------------------------------
