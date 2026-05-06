"""Wikipedia Live-Connector — Encyclopedia-Look-Up via REST API.

Wikipedia ist die mit Abstand größte enzyklopädische Wissens-Aggregation
weltweit. Für Faktencheck-Zwecke liefert sie:
- Personen-/Organisations-/Orts-Definitionen mit kompaktem Lead-Extract
- Wissenschaftliche Konsens-Zusammenfassungen (besonders DE-WP für DACH-
  Themen, EN-WP für globale Themen)
- Historische Ereignis-Übersichten
- Verlinkungen zu Primärquellen

Komplementär zu existierenden Quellen:
- PubMed/Cochrane: peer-reviewed Forschung
- Faktencheck-RSS (Snopes/Correctiv/...): redaktionelle Bewertungen
- GDELT: aktuelle News-Coverage
- Static-First-Packs: kuratierte Konsens-Daten
- WIKIPEDIA: enzyklopädische Definitionen + Mehrheits-Konsens

API: https://de.wikipedia.org/api/rest_v1/page/summary/{title} (DE first)
+ Fallback https://en.wikipedia.org/api/rest_v1/page/summary/{title}
+ Search-Fallback opensearch API für partial matches.

Free, kein Auth, Rate-Limit ~200 req/sec mit User-Agent.

Trigger: claim hat ≥1 Entity (Persons/Organizations/Locations/Events).

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle, keine kuratierte Konsens-DB).

Wikipedia-Limitation: Inhalte sind Crowdsourced, können falsch oder
veraltet sein. Pack-Pipeline-Synthesizer behandelt Wikipedia als
ergänzende Information, nicht als alleinige Verifikations-Basis.
"""

import asyncio
import logging
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

WIKIPEDIA_DE_API = "https://de.wikipedia.org/api/rest_v1/page/summary/{title}"
WIKIPEDIA_EN_API = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
WIKIPEDIA_DE_SEARCH = (
    "https://de.wikipedia.org/w/api.php"
    "?action=opensearch&search={query}&limit=3&namespace=0&format=json"
)
WIKIPEDIA_EN_SEARCH = (
    "https://en.wikipedia.org/w/api.php"
    "?action=opensearch&search={query}&limit=3&namespace=0&format=json"
)


async def _fetch_summary(client, lang: str, title: str) -> dict | None:
    """Hole Page-Summary für direkten Title-Match.

    Returns dict mit title/description/extract/url ODER None bei 404.
    """
    api = WIKIPEDIA_DE_API if lang == "de" else WIKIPEDIA_EN_API
    url = api.format(title=quote(title, safe=""))
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data.get("title"):
            return None
        # Filter Disambiguation-Pages out (sind selten hilfreich)
        if data.get("type") == "disambiguation":
            return None
        return {
            "title": data.get("title", ""),
            "description": data.get("description", "") or "",
            "extract": data.get("extract", "") or "",
            "url": data.get("content_urls", {}).get(
                "desktop", {}).get("page", ""),
            "lang": lang,
        }
    except Exception as e:
        logger.debug(f"Wikipedia {lang} summary fetch failed for '{title[:30]}': {e}")
        return None


async def _search_first_match(client, lang: str, query: str) -> str | None:
    """Wikipedia opensearch — gibt ersten plausiblen Title zurück.

    Returns Title-String ODER None.
    """
    api = WIKIPEDIA_DE_SEARCH if lang == "de" else WIKIPEDIA_EN_SEARCH
    url = api.format(query=quote(query, safe=""))
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            return None
        data = resp.json()
        # opensearch-Format: [query, [titles], [descriptions], [urls]]
        if not isinstance(data, list) or len(data) < 2:
            return None
        titles = data[1]
        if not titles:
            return None
        return titles[0]
    except Exception as e:
        logger.debug(f"Wikipedia {lang} search failed for '{query[:30]}': {e}")
        return None


async def _try_entity(client, entity: str) -> dict | None:
    """Versuche, für eine Entity eine Wikipedia-Zusammenfassung zu finden.

    Strategy:
    1. DE direct title-match
    2. DE opensearch fallback
    3. EN direct title-match (falls DE nichts liefert)
    4. EN opensearch fallback
    """
    if not entity or len(entity) < 3:
        return None

    # 1. DE direct
    result = await _fetch_summary(client, "de", entity)
    if result:
        return result

    # 2. DE search-fallback
    de_title = await _search_first_match(client, "de", entity)
    if de_title and de_title != entity:
        result = await _fetch_summary(client, "de", de_title)
        if result:
            return result

    # 3. EN direct
    result = await _fetch_summary(client, "en", entity)
    if result:
        return result

    # 4. EN search-fallback
    en_title = await _search_first_match(client, "en", entity)
    if en_title and en_title != entity:
        result = await _fetch_summary(client, "en", en_title)
        if result:
            return result

    return None


async def search_wikipedia(analysis: dict) -> dict:
    """Live-Lookup gegen Wikipedia (DE-first, EN-Fallback) für Claim-Entities.

    Returns Dict mit ≤3 Wikipedia-Article-Treffern. Wenn DE-WP-Article
    existiert, wird er bevorzugt; sonst EN-WP. Bei Disambiguation-Pages
    wird der Treffer übersprungen.
    """
    empty = {"source": "Wikipedia", "type": "encyclopedia", "results": []}

    entities = (analysis or {}).get("entities", []) or []
    if not entities:
        return empty

    # Limit auf top 3 Entities (vermeidet API-Spam, ohnehin meist redundant)
    entities = [e for e in entities if e and len(e) >= 3][:3]
    if not entities:
        return empty

    async with polite_client(timeout=15.0) as client:
        # Parallel-Fetches für Speed
        tasks = [_try_entity(client, e) for e in entities]
        results_raw = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = []
    seen_urls: set[str] = set()
    for r in results_raw:
        if isinstance(r, Exception) or not r:
            continue
        if r["url"] in seen_urls:
            continue
        seen_urls.add(r["url"])

        # Format display_value with extract preview
        extract_short = (r["extract"][:380] + "…") if len(r["extract"]) > 380 else r["extract"]
        display = (
            f"{r['title']} ({r['lang'].upper()}-Wikipedia) — "
            f"{r['description']}: {extract_short}"
        )[:500]

        results.append({
            "indicator_name": f"{r['title']} ({r['lang'].upper()}-Wikipedia)",
            "indicator": "wikipedia_article",
            "country": "—",
            "year": "—",  # Wikipedia API gibt last-modified, aber Article-Topic-Datum unklar
            "topic": "wikipedia_encyclopedia",
            "display_value": display,
            "description": r["description"][:200],
            "url": r["url"],
            "secondary_url": "",
            "source": (
                f"Wikipedia ({r['lang'].upper()}, "
                "Crowdsourced Encyclopedia)"
            ),
        })

    if not results:
        logger.info(
            f"Wikipedia: 0 Treffer für Entities "
            f"{[e[:30] for e in entities[:3]]}..."
        )
        return empty

    logger.info(f"Wikipedia: {len(results)} Treffer geliefert")
    return {
        "source": "Wikipedia",
        "type": "encyclopedia",
        "results": results,
    }
