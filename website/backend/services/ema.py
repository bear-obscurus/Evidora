"""EMA — European Medicines Agency authorised medicines database.

Downloads the official XLSX export from ema.europa.eu and caches it in memory.
Contains ~2,700 authorised medicines with active substance, therapeutic area,
indication, and approval status.
"""

import io
import re
import time
import logging

import httpx
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

MEDICINES_URL = "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"

# In-memory cache
_cache: list[dict] | None = None
_cache_ts: float = 0
CACHE_TTL = 86400  # 24 hours

# Header row index (0-based) in the XLSX
HEADER_ROW = 8


async def _load_medicines(client: httpx.AsyncClient) -> list[dict]:
    """Download and parse the EMA medicines XLSX."""
    global _cache, _cache_ts

    now = time.time()
    if _cache is not None and (now - _cache_ts) < CACHE_TTL:
        return _cache

    try:
        resp = await client.get(MEDICINES_URL, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"EMA XLSX download failed: {e}")
        if _cache is not None:
            return _cache
        return []

    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
    except Exception as e:
        logger.warning(f"EMA XLSX parse failed: {e}")
        if _cache is not None:
            return _cache
        return []

    if len(rows) <= HEADER_ROW + 1:
        logger.warning("EMA XLSX has no data rows")
        return []

    # Parse header and data
    header = rows[HEADER_ROW]
    medicines = []

    # Map header names to indices
    col_map = {}
    for i, h in enumerate(header):
        if h:
            col_map[h.strip()] = i

    name_idx = col_map.get("Name of medicine", 1)
    status_idx = col_map.get("Medicine status", 3)
    inn_idx = col_map.get("International non-proprietary name (INN) / common name", 6)
    active_idx = col_map.get("Active substance", 7)
    area_idx = col_map.get("Therapeutic area (MeSH)", 8)
    indication_idx = col_map.get("Therapeutic indication", 15)
    category_idx = col_map.get("Category", 0)
    url_idx = col_map.get("Medicine URL", len(header) - 1)

    for row in rows[HEADER_ROW + 1:]:
        if not row or not row[name_idx]:
            continue
        medicines.append({
            "name": str(row[name_idx] or ""),
            "active_substance": str(row[active_idx] or ""),
            "inn": str(row[inn_idx] or ""),
            "status": str(row[status_idx] or ""),
            "therapeutic_area": str(row[area_idx] or ""),
            "indication": str(row[indication_idx] or "")[:300],
            "category": str(row[category_idx] or ""),
            "url": str(row[url_idx] or ""),
        })

    _cache = medicines
    _cache_ts = now
    logger.info(f"EMA: loaded {len(medicines)} medicines from XLSX")
    return medicines


def _word_match(term: str, text: str) -> bool:
    """Check if term appears as a whole word in text (case-insensitive)."""
    return bool(re.search(r'\b' + re.escape(term) + r'\b', text, re.IGNORECASE))


def _score_match(term: str, med: dict) -> int:
    """Score how well a search term matches a medicine entry.

    Returns 0 (no match) or a positive score.  Higher = better match.
    - 3: name or active substance match (most specific)
    - 2: INN match
    - 1: indication match (broader, but still relevant)
    - 0: therapeutic area alone is too broad — skip it
    """
    if _word_match(term, med["name"]) or _word_match(term, med["active_substance"]):
        return 3
    if _word_match(term, med["inn"]):
        return 2
    if _word_match(term, med["indication"]):
        return 1
    return 0


async def search_ema(analysis: dict) -> dict:
    """Search EMA medicines database by entity names.

    Uses word-boundary matching and scored ranking to avoid false positives
    from substring matches or overly broad therapeutic area hits.
    """
    entities = analysis.get("entities", [])
    if not entities:
        return {"source": "EMA (European Medicines Agency)", "type": "official_data", "results": []}

    async with polite_client(timeout=60.0) as client:
        medicines = await _load_medicines(client)

    if not medicines:
        return {"source": "EMA (European Medicines Agency)", "type": "official_data", "results": []}

    # Filter entities: skip very short terms (< 4 chars) to avoid spurious matches
    search_terms = [e for e in entities if len(e) >= 4]
    if not search_terms:
        return {"source": "EMA (European Medicines Agency)", "type": "official_data", "results": []}

    scored_results = []

    for med in medicines:
        best_score = 0
        for term in search_terms:
            score = _score_match(term, med)
            best_score = max(best_score, score)

        if best_score > 0:
            result = {
                "title": f"{med['name']} ({med['inn']})" if med['inn'] and med['inn'] != med['name'] else med['name'],
                "active_substance": med["active_substance"],
                "status": med["status"],
                "therapeutic_area": med["therapeutic_area"],
                "indication": med["indication"],
                "url": med["url"] if med["url"] else f"https://www.ema.europa.eu/en/search?search_api_fulltext={med['name']}",
            }
            scored_results.append((best_score, result))

    # Sort by score descending, take top 5
    scored_results.sort(key=lambda x: x[0], reverse=True)
    results = [r for _, r in scored_results[:5]]

    logger.info(f"EMA: {len(results)} matches for entities {search_terms}")
    return {"source": "EMA (European Medicines Agency)", "type": "official_data", "results": results}
