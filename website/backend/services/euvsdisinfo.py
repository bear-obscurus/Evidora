"""EUvsDisinfo — geopolitische Desinformations-Erkennung.

Zwei Datenquellen werden kombiniert:

1. **RSS-Feed** (aktuell): Redaktionelle Analysen, Disinformation Reviews,
   Threat Reports von euvsdisinfo.eu.  ~30 aktuelle Artikel, stündlich aktualisiert.
   Semantische Suche via MiniLM Embeddings.

2. **Falldatenbank** (historisch): 14.495 dokumentierte pro-Kreml
   Desinformations-Fälle (Jan 2015 – Nov 2022) mit dem Fake-Claim.
   Quelle: erosalie/euvsdisinfo (GitHub), CC BY-SA 4.0.
   Statische JSON-Datei, Keyword-basierte Suche (kein Embedding nötig).

Nur Claims mit geopolitischen Keywords werden gegen EUvsDisinfo geprüft.
"""

import json
import logging
import re
import time
from pathlib import Path
from xml.etree import ElementTree

import httpx
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# === RSS Feed ===
FEED_URL = "https://euvsdisinfo.eu/feed/"

_feed_cache: list[dict] | None = None
_feed_embeddings = None
_feed_cache_ts: float = 0
FEED_CACHE_TTL = 3600  # 1 hour

# === Case Database ===
DATA_DIR = Path(__file__).parent.parent / "data"
LOCAL_DB = DATA_DIR / "euvsdisinfo_db.json"

_db_index: list[dict] | None = None

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


# DE → EN translation for common geopolitical terms (DB is English)
_DE_EN_MAP = {
    "russland": "russia", "russisch": "russian", "kreml": "kremlin",
    "krim": "crimea", "ukraine": "ukraine", "nato": "nato",
    "weißrussland": "belarus", "moldau": "moldova",
    "tschechien": "czech", "ungarn": "hungary", "polen": "poland",
    "schweden": "sweden", "litauen": "lithuania", "lettland": "latvia",
    "estland": "estonia", "georgien": "georgia", "türkei": "turkey",
    "serbien": "serbia", "rumänien": "romania", "bulgarien": "bulgaria",
    "kroatien": "croatia", "slowakei": "slovakia", "slowenien": "slovenia",
    "griechenland": "greece", "niederlande": "netherlands",
    "frankreich": "france", "spanien": "spain", "italien": "italy",
    "vereinigtes königreich": "united kingdom",
    "vereinigte staaten": "united states",
    "deutschland": "germany", "österreich": "austria", "schweiz": "switzerland",
    "annexion": "annexation", "hungersnot": "famine", "hunger": "hunger",
    "krieg": "war", "frieden": "peace", "wahl": "election",
    "sanktionen": "sanctions", "propaganda": "propaganda",
    "flüchtlinge": "refugees", "atomwaffen": "nuclear",
    "biologische waffen": "biological weapons", "chemische waffen": "chemical weapons",
    "westen": "west", "osten": "east",
}


def _expand_keywords(keywords: list[str]) -> list[str]:
    """Expand German keywords with English translations for cross-language DB search."""
    expanded = list(keywords)
    for kw in keywords:
        en = _DE_EN_MAP.get(kw.lower())
        if en and en not in [k.lower() for k in expanded]:
            expanded.append(en)
    return expanded


def _is_disinfo_claim(analysis: dict) -> bool:
    """Check if a claim is related to geopolitical disinformation."""
    raw_entities = analysis.get("entities", [])
    flat_entities = []
    for e in raw_entities:
        if isinstance(e, str):
            flat_entities.append(e)
        elif isinstance(e, list):
            flat_entities.extend(str(x) for x in e)
        else:
            flat_entities.append(str(e))
    text = " ".join([
        analysis.get("claim", ""),
        analysis.get("subcategory", ""),
        " ".join(flat_entities),
    ]).lower()
    return any(kw in text for kw in DISINFO_KEYWORDS)


# ─── RSS Feed ────────────────────────────────────────────────────────


def _extract_items(xml_text: str) -> list[dict]:
    """Parse RSS feed items from EUvsDisinfo."""
    items = []
    try:
        root = ElementTree.fromstring(xml_text)
        ns = {"content": "http://purl.org/rss/1.0/modules/content/"}

        for item in root.findall(".//item"):
            title_el = item.find("title")
            link_el = item.find("link")
            desc_el = item.find("description")
            content_el = item.find("content:encoded", ns)
            pub_date_el = item.find("pubDate")

            if title_el is None or link_el is None:
                continue

            categories = [cat.text for cat in item.findall("category") if cat.text]

            description = ""
            if desc_el is not None and desc_el.text:
                description = re.sub(r"<[^>]+>", "", desc_el.text).strip()[:500]

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
        async with polite_client(timeout=15.0) as client:
            resp = await client.get(FEED_URL)
            resp.raise_for_status()
            items = _extract_items(resp.text)
            if items:
                logger.info(f"EUvsDisinfo RSS: {len(items)} articles loaded")
            return items
    except Exception as e:
        logger.warning(f"EUvsDisinfo RSS fetch failed: {e}")
        return []


async def prefetch_feed():
    """Fetch RSS feed and pre-compute embeddings (called at startup)."""
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
            logger.info(f"EUvsDisinfo RSS: embeddings computed for {len(items)} articles")
        else:
            _feed_embeddings = None
    except Exception as e:
        _feed_embeddings = None
        logger.warning(f"EUvsDisinfo RSS embedding failed: {e}")


# ─── Case Database ───────────────────────────────────────────────────


def _load_db():
    """Load the local database from the shipped JSON file."""
    global _db_index
    if _db_index is not None:
        return
    if LOCAL_DB.exists():
        try:
            with open(LOCAL_DB, encoding="utf-8") as f:
                _db_index = json.load(f)
            logger.info(f"EUvsDisinfo DB: loaded {len(_db_index)} cases")
        except Exception as e:
            logger.warning(f"EUvsDisinfo DB: failed to load: {e}")
    else:
        logger.warning(f"EUvsDisinfo DB: file not found at {LOCAL_DB}")


# ─── Search ──────────────────────────────────────────────────────────


def _semantic_match_feed(claim: str, items: list[dict], entities: list[str] | None = None, top_k: int = 5) -> list[dict]:
    """Rank RSS feed items by semantic similarity to the claim."""
    try:
        from services.reranker import _load_model, _model
        if not _load_model() or _model is None:
            return None

        from sentence_transformers import util

        claim_embedding = _model.encode(claim, convert_to_tensor=True)

        if _feed_embeddings is not None and _feed_cache is not None and items is _feed_cache:
            item_embeddings = _feed_embeddings
        else:
            texts = [f"{item['title']} {item['description']}" for item in items]
            item_embeddings = _model.encode(texts, convert_to_tensor=True)

        scores = util.cos_sim(claim_embedding, item_embeddings)[0]
        scored = sorted(zip(items, scores.tolist()), key=lambda x: x[1], reverse=True)

        # Entity overlap for RSS (same language, so it works)
        def has_overlap(item):
            if not entities:
                return True
            text = f"{item['title']} {item['description']}".lower()
            return any(e.lower() in text for e in entities if len(e) >= 3)

        kept = [
            item for item, score in scored[:top_k * 3]
            if score > 0.35 and has_overlap(item)
        ][:top_k]

        if scored:
            logger.info(f"EUvsDisinfo RSS semantic: top score {scored[0][1]:.3f}, kept {len(kept)}")
        return kept

    except Exception as e:
        logger.debug(f"EUvsDisinfo RSS semantic matching failed: {e}")
        return None


def _keyword_match_feed(items: list[dict], keywords: list[str]) -> list[dict]:
    """Fallback keyword matching for RSS items."""
    matched = []
    for item in items:
        text = f"{item['title']} {item['description']} {item['full_text']}".lower()
        if any(kw.lower() in text for kw in keywords):
            matched.append(item)
    return matched


def _keyword_match_db(keywords: list[str], top_k: int = 5) -> list[dict]:
    """Search the case database by keyword matching.

    Scores entries by number of keyword hits for better ranking.
    """
    if _db_index is None:
        return []

    scored = []
    for entry in _db_index:
        text = f"{entry['title']} {entry['claim']}".lower()
        hits = sum(1 for kw in keywords if kw.lower() in text and len(kw) >= 3)
        if hits > 0:
            scored.append((hits, entry))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [e for _, e in scored[:top_k]]


async def search_euvsdisinfo(analysis: dict) -> dict:
    """Search EUvsDisinfo RSS + case database for disinformation matches."""
    global _feed_cache, _feed_cache_ts

    claim = analysis.get("claim", "")
    entities = analysis.get("entities", [])
    factcheck_queries = analysis.get("factcheck_queries", [])
    keywords = _expand_keywords(entities + factcheck_queries)

    if not claim and not keywords:
        return {"source": "EUvsDisinfo", "type": "factcheck", "results": []}

    # === RSS Feed ===
    now = time.time()
    if _feed_cache is not None and now - _feed_cache_ts < FEED_CACHE_TTL:
        all_items = _feed_cache
    else:
        all_items = await _fetch_feed()
        if all_items:
            _feed_cache = all_items
            _feed_cache_ts = now

    rss_matched = []
    if all_items:
        rss_matched = _semantic_match_feed(claim, all_items, entities=entities) if claim else None
        if rss_matched is None:
            rss_matched = _keyword_match_feed(all_items, keywords)

    # === Case Database (keyword search) ===
    if _db_index is None:
        _load_db()

    db_matched = _keyword_match_db(keywords)
    logger.info(f"EUvsDisinfo DB: {len(db_matched)} keyword matches (index={len(_db_index) if _db_index else 0})")

    # === Merge results (RSS first, then DB) ===
    results = []
    seen_urls = set()

    # RSS results
    for item in (rss_matched or [])[:3]:
        if item["url"] in seen_urls:
            continue
        seen_urls.add(item["url"])
        categories = ", ".join(item["categories"][:3]) if item["categories"] else ""
        results.append({
            "title": item["title"],
            "url": item["url"],
            "description": item["description"][:200],
            "source": "EUvsDisinfo",
            "date": item["date"],
            "categories": categories,
        })

    # Database results
    for entry in db_matched:
        if entry["url"] in seen_urls:
            continue
        seen_urls.add(entry["url"])
        results.append({
            "title": entry["title"],
            "url": entry["url"],
            "description": entry["claim"][:200],
            "claim": entry["claim"],
            "source": "EUvsDisinfo",
            "date": entry["date"],
            "countries": entry["countries"],
        })

        if len(results) >= 5:
            break

    return {
        "source": "EUvsDisinfo",
        "type": "factcheck",
        "results": results,
    }
