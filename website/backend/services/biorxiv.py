"""bioRxiv + medRxiv — Preprint-Server für Lebenswissenschaften.

Datenquelle: Cold Spring Harbor Laboratory API
- bioRxiv: https://api.biorxiv.org/details/biorxiv/{from}/{to}/{cursor}
- medRxiv: https://api.medrxiv.org/details/medrxiv/{from}/{to}/{cursor}

Limitation: Die API erlaubt KEINE direkte Keyword-Suche, nur Datum-Range
oder DOI-Lookup. Wir holen die letzten 14 Tage und filtern lokal nach
Reranker-Cosine-Similarity.

Caveat — Preprints sind NICHT peer-reviewed:
Der Synthesizer muss das ausweisen. Output-Format hat 'preprint' im
indicator-Namen, damit das Synthesizer-Prompt es erkennen kann.

Use-Case:
- "Eine neue Studie zeigt..."
- "Forscher haben festgestellt..."
- Aktuelle COVID-/Variantsforschung
- Pharmakologische Schnellbefunde

Komplementär zu Europe PMC (das bereits Preprints einschließt) — bioRxiv
liefert FRISCHE (Tage-alte) Preprints mit Volltext-Verlinkung.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger("evidora")

CACHE_TTL = 86400  # 24h — Preprints kommen täglich, aber Tag-Granularität reicht
LOOKBACK_DAYS = 14  # Letzte 2 Wochen
MAX_RESULTS_PER_SERVER = 100
SERVERS = [
    {"name": "bioRxiv", "api_root": "https://api.biorxiv.org/details/biorxiv"},
    {"name": "medRxiv", "api_root": "https://api.medrxiv.org/details/medrxiv"},
]

_cache: list[dict] | None = None
_cache_time: float = 0.0


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_PREPRINT_TERMS = (
    "preprint", "preprints", "biorxiv", "medrxiv",
    "neue studie", "neue studien",
    "aktuelle studie", "aktuelle studien",
    "forscher haben festgestellt", "forschungsergebnis",
    "frische daten", "frische studie",
    "vor peer-review",
    "neueste forschung",
)
_HEALTH_TERMS = (
    "covid", "sars-cov-2", "impfung", "vakzin",
    "pharmakolog", "medikament", "wirkstoff", "therapie",
    "krankheit", "krebs", "diabetes", "alzheimer", "parkinson",
    "hiv", "tuberkulose", "malaria",
    "klinische studie", "rct", "metaanalyse",
    "drug", "vaccine", "treatment", "clinical trial",
)


def _claim_mentions_biorxiv(claim_lc: str) -> bool:
    has_preprint = any(t in claim_lc for t in _PREPRINT_TERMS)
    if has_preprint:
        return True
    # Composite: 'neue studie' / 'aktuelle forschung' + Health-Topic
    has_research = any(t in claim_lc for t in (
        "studie", "studien", "forschung", "untersuchung",
        "research", "study",
    ))
    has_health = any(t in claim_lc for t in _HEALTH_TERMS)
    if has_research and has_health:
        return True
    return False


def claim_mentions_biorxiv_cached(claim: str) -> bool:
    return _claim_mentions_biorxiv((claim or "").lower())


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
async def _fetch_one_server(client: httpx.AsyncClient, server: dict,
                              from_date: str, to_date: str) -> list[dict]:
    """Fetch one server's recent preprints."""
    url = f"{server['api_root']}/{from_date}/{to_date}/0"
    try:
        response = await client.get(url, timeout=20)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("collection") or []
        # Limit per server
        items = items[:MAX_RESULTS_PER_SERVER]
        # Annotate with server name
        for it in items:
            it["_server"] = server["name"]
        logger.info(f"{server['name']} fetched: {len(items)} preprints "
                    f"({from_date} to {to_date})")
        return items
    except Exception as e:
        logger.warning(f"{server['name']} fetch failed: {e}")
        return []


async def fetch_biorxiv(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Prefetch entry-point. Returns combined preprint list from
    bioRxiv + medRxiv for the last LOOKBACK_DAYS days.
    """
    global _cache, _cache_time

    now = time.time()
    if _cache is not None and (now - _cache_time) < CACHE_TTL:
        return _cache

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        own_client = True

    try:
        to_date = datetime.utcnow().strftime("%Y-%m-%d")
        from_date = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        results = await asyncio.gather(
            *(_fetch_one_server(client, s, from_date, to_date) for s in SERVERS),
            return_exceptions=True,
        )

        all_items: list[dict] = []
        for r in results:
            if isinstance(r, list):
                all_items.extend(r)

        _cache = all_items
        _cache_time = now
        logger.info(f"bioRxiv/medRxiv aggregated: {len(all_items)} preprints "
                    f"(last {LOOKBACK_DAYS} days)")
        return all_items
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_biorxiv(analysis: dict) -> dict:
    empty = {
        "source": "bioRxiv/medRxiv (Preprints)",
        "type": "study",
        "results": [],
    }

    items = await fetch_biorxiv()
    if not items:
        return empty

    # Output ähnlich zu Europe PMC. Wir markieren als 'preprint' damit
    # der Synthesizer das Caveat ausweisen kann.
    results: list[dict] = []
    for it in items:
        title = it.get("title", "")
        abstract = it.get("abstract", "")
        doi = it.get("doi", "")
        authors = it.get("authors", "")
        date = it.get("date", "")
        server = it.get("_server", "biorxiv")

        if not title or not doi:
            continue

        url = f"https://www.{server.lower()}.org/content/10.1101/{doi}v1"
        results.append({
            "title": f"[PREPRINT] {title}",
            "url": url,
            "authors": authors,
            "journal": server,
            "date": date,
            "indicator": "biorxiv_preprint",
            "description": abstract[:400] if abstract else "",
        })

    return {
        "source": "bioRxiv/medRxiv (Preprints)",
        "type": "study",
        "results": results,
    }
