"""DataCommons ClaimReview — lokaler Index historischer Faktenchecks.

Lädt den DataCommons ClaimReview Feed (CC BY 4.0, ~93.000 Einträge, täglich
aktualisiert) und filtert deutschsprachige/europäische Einträge heraus.
Die gefilterten Einträge werden lokal gespeichert und per MiniLM semantisch
durchsuchbar gemacht.

Quelle: https://datacommons.org/factcheck/download
Feed: https://storage.googleapis.com/datacommons-feeds/claimreview/latest/data.json
"""

import json
import logging
import re
import time
from pathlib import Path

import httpx

logger = logging.getLogger("evidora")

FEED_URL = "https://storage.googleapis.com/datacommons-feeds/claimreview/latest/data.json"
DATA_DIR = Path(__file__).parent.parent / "data"
LOCAL_INDEX = DATA_DIR / "claimreview_index.json"

# In-memory cache
_index: list[dict] | None = None
_embeddings = None
_index_ts: float = 0
INDEX_TTL = 86400  # 24 hours

# German-language fact-checkers and European orgs to keep
DE_EU_CHECKERS = {
    "correctiv", "dpa", "mimikama", "apa.at", "faktencheck", "faktenfinder",
    "br.de", "tagesschau", "spiegel", "volksverpetzer", "futurezone",
    "derstandard", "kurier.at", "orf.at", "dw.com", "zdf.de", "ndr.de",
    "mdr.de", "swr.de", "wdr.de", "sueddeutsche", "faz.net",
    "afp.com", "reuters.com", "france24", "liberation", "lemonde",
    "elpais", "repubblica", "nieuwscheckers", "logically", "fullfact",
    "polygraph", "eufactcheck", "euractiv",
}

# Languages to keep
KEEP_LANGUAGES = {"de", "german", "deutsch"}

# Characters that indicate German text
_DE_CHARS = re.compile(r'[äöüßÄÖÜ]')
_DE_WORDS = re.compile(r'\b(und|der|die|das|ist|nicht|ein|eine|dass|wird|hat|sind|für|mit|auch|nach|wie|nur|noch|aber)\b', re.I)


def _is_german_text(text: str) -> bool:
    """Heuristic check if text is German."""
    if not text or len(text) < 10:
        return False
    if _DE_CHARS.search(text):
        return True
    # Check for German words (need at least 2 matches in short text)
    matches = _DE_WORDS.findall(text)
    return len(matches) >= 2


def _is_relevant_entry(entry: dict) -> bool:
    """Check if a ClaimReview entry is from a German/European source."""
    # Check author URL/name
    author = entry.get("author", {})
    author_url = (author.get("url") or "").lower()
    author_name = (author.get("name") or "").lower()

    for checker in DE_EU_CHECKERS:
        if checker in author_url or checker in author_name:
            return True

    # Check language field
    lang = (entry.get("inLanguage") or "").lower()
    if lang in KEEP_LANGUAGES:
        return True

    # Check if claim text is German
    claim = entry.get("claimReviewed", "")
    if _is_german_text(claim):
        return True

    # Check rating text for German
    rating = entry.get("reviewRating", {})
    rating_text = rating.get("alternateName", "")
    if _is_german_text(rating_text):
        return True

    return False


def _extract_entry(raw: dict) -> dict | None:
    """Extract relevant fields from a raw ClaimReview JSON-LD object."""
    claim = raw.get("claimReviewed", "").strip()
    if not claim or len(claim) < 10:
        return None

    author = raw.get("author", {})
    rating = raw.get("reviewRating", {})
    item_reviewed = raw.get("itemReviewed", {})

    # Get the review URL (fact-check article)
    url = raw.get("url", "")

    # Get rating text
    rating_text = rating.get("alternateName", "") or rating.get("bestRating", "")

    return {
        "claim": claim[:500],
        "url": url,
        "source": author.get("name", ""),
        "source_url": author.get("url", ""),
        "rating": rating_text[:200],
        "date": raw.get("datePublished", ""),
    }


async def _download_and_filter() -> list[dict]:
    """Stream-download the DataCommons feed, filter for DE/EU entries."""
    entries = []

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            # Stream the large JSON file
            async with client.stream("GET", FEED_URL) as resp:
                resp.raise_for_status()

                chunks = []
                async for chunk in resp.aiter_bytes(chunk_size=1_000_000):
                    chunks.append(chunk)

                raw = b"".join(chunks).decode("utf-8", errors="replace")

        logger.info(f"DataCommons: downloaded {len(raw) // 1_000_000} MB")

        # Parse the DataFeed JSON — handle encoding errors gracefully
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Fix common issues: invalid unicode escapes
            raw_fixed = re.sub(r'\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u)', '', raw)
            data = json.loads(raw_fixed)

        elements = data.get("dataFeedElement", [])

        for element in elements:
            items = element.get("item") or []
            if isinstance(items, dict):
                items = [items]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") != "ClaimReview":
                    continue
                if not _is_relevant_entry(item):
                    continue
                entry = _extract_entry(item)
                if entry:
                    entries.append(entry)

        # Deduplicate by claim text
        seen = set()
        unique = []
        for e in entries:
            key = e["claim"].lower()[:100]
            if key not in seen:
                seen.add(key)
                unique.append(e)
        entries = unique

        logger.info(f"DataCommons: {len(entries)} DE/EU entries extracted from {len(elements)} feed elements")

    except Exception as e:
        logger.error(f"DataCommons download failed: {e}")

    return entries


async def update_index():
    """Download feed, filter, save locally, and compute embeddings."""
    global _index, _embeddings, _index_ts

    # Skip if index is still fresh
    now = time.time()
    if _index is not None and now - _index_ts < INDEX_TTL:
        return

    entries = await _download_and_filter()

    if not entries:
        # Try loading from local file
        if LOCAL_INDEX.exists():
            try:
                with open(LOCAL_INDEX) as f:
                    entries = json.load(f)
                logger.info(f"DataCommons: loaded {len(entries)} entries from local cache")
            except Exception:
                pass

    if not entries:
        logger.warning("DataCommons: no entries available")
        return

    # Save locally
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOCAL_INDEX, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=None)
        logger.info(f"DataCommons: saved {len(entries)} entries to {LOCAL_INDEX}")
    except Exception as e:
        logger.warning(f"DataCommons: failed to save local index: {e}")

    _index = entries
    _index_ts = time.time()

    # Compute embeddings
    try:
        from services.reranker import _load_model, _model
        if _load_model() and _model is not None:
            texts = [f"{e['claim']} {e['rating']}" for e in entries]
            _embeddings = _model.encode(texts, convert_to_tensor=True)
            logger.info(f"DataCommons: embeddings computed for {len(entries)} entries")
        else:
            _embeddings = None
    except Exception as e:
        _embeddings = None
        logger.warning(f"DataCommons embedding failed: {e}")


def _load_local_index():
    """Load the local index file if available (cold start fallback)."""
    global _index, _index_ts
    if _index is not None:
        return
    if LOCAL_INDEX.exists():
        try:
            with open(LOCAL_INDEX) as f:
                _index = json.load(f)
            _index_ts = LOCAL_INDEX.stat().st_mtime
            logger.info(f"DataCommons: loaded {len(_index)} entries from local cache")
        except Exception as e:
            logger.warning(f"DataCommons: failed to load local index: {e}")


def _semantic_search(claim: str, entries: list[dict], top_k: int = 5) -> list[dict]:
    """Search entries by semantic similarity."""
    try:
        from services.reranker import _load_model, _model
        if not _load_model() or _model is None:
            return None

        from sentence_transformers import util

        claim_embedding = _model.encode(claim, convert_to_tensor=True)

        if _embeddings is not None and _index is not None and entries is _index:
            entry_embeddings = _embeddings
        else:
            texts = [f"{e['claim']} {e['rating']}" for e in entries]
            entry_embeddings = _model.encode(texts, convert_to_tensor=True)

        scores = util.cos_sim(claim_embedding, entry_embeddings)[0]
        scored = sorted(zip(entries, scores.tolist()), key=lambda x: x[1], reverse=True)
        kept = [e for e, score in scored[:top_k] if score > 0.35]
        if scored:
            top_score = scored[0][1]
            logger.info(f"DataCommons semantic: top score {top_score:.3f}, kept {len(kept)}/{min(top_k, len(scored))}")
        return kept

    except Exception as e:
        logger.debug(f"DataCommons semantic search failed: {e}")
        return None


def _keyword_search(entries: list[dict], keywords: list[str]) -> list[dict]:
    """Fallback keyword search."""
    matched = []
    for entry in entries:
        text = f"{entry['claim']} {entry['rating']}".lower()
        if any(kw.lower() in text for kw in keywords):
            matched.append(entry)
    return matched[:5]


async def search_datacommons(analysis: dict) -> dict:
    """Search the local DataCommons ClaimReview index."""
    global _index, _index_ts

    claim = analysis.get("claim", "")
    entities = analysis.get("entities", [])
    factcheck_queries = analysis.get("factcheck_queries", [])
    keywords = entities + factcheck_queries

    if not claim and not keywords:
        return {"source": "DataCommons ClaimReview", "type": "factcheck", "results": []}

    # Ensure index is loaded
    if _index is None:
        _load_local_index()

    if not _index:
        return {"source": "DataCommons ClaimReview", "type": "factcheck", "results": []}

    # Semantic search first, keyword fallback
    matched = None
    if claim:
        matched = _semantic_search(claim, _index)

    if matched is None:
        matched = _keyword_search(_index, keywords)

    results = []
    for entry in matched[:5]:
        title = entry["claim"][:150]
        if entry["rating"]:
            title = f"{entry['source']}: {title}"

        results.append({
            "title": title,
            "url": entry["url"],
            "source": entry["source"] or "DataCommons",
            "rating": entry["rating"],
            "date": entry["date"],
        })

    return {
        "source": "DataCommons ClaimReview",
        "type": "factcheck",
        "results": results,
    }
