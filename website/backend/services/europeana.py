"""Europeana Live-Connector — Digitales europäisches Kulturerbe via Search-API.

Die Europeana Foundation aggregiert ~50 Mio. digitalisierte Objekte aus
4.000+ EU-Kulturinstitutionen (Museen, Bibliotheken, Archive). Für AT-
Faktencheck-Zwecke besonders nützlich: Österreichische Nationalbibliothek
(ÖNB), Kunsthistorisches Museum (KHM), Albertina, Wien Museum sind direkt
im Pool — Provenienz, Datierung, Materialnachweise zu konkreten Werken/
Objekten lassen sich verifizieren.

Komplementär zu existierenden Quellen:
- Wikipedia (#21): unstrukturierte Lead-Extracts.
- Wikidata: strukturierte Personen-/Politik-Triples.
- Getty Vocabularies: Kunst-Terminologie (Stile, Materialien).
- EUROPEANA: konkrete Objekt-Nachweise (Digitalisate, Provenienz,
  Sammlung). Kann parallel zu Wikidata/Getty feuern.

API: https://api.europeana.eu/record/v2/search.json
- Free Key via https://pro.europeana.eu/get-api (env: EUROPEANA_API_KEY)
- Demo-Key 'apidemo' nur zum lokalen Testen — Production muss Key haben
- JSON-Response, EDM-Metadata-Format
- Lizenz Metadata: EUPL v1.2 (Code) / CC0 (Daten); Objektrechte variieren
- 30 s default Timeout — wir limitieren auf 20 s

Strategie: Query-Param-Search (kein SPARQL). Wir extrahieren einen
Top-Query-Term (Trigger-Substring oder erste Entity ≥3 Zeichen), holen
Top-5 Treffer und stellen Titel/Provider/Land/Jahr/Rechte als strukturierte
Treffer dar.

Politische Guardrails: Reine Kulturgüter-Metadaten. Keine politische
Bewertung. Trigger enthält keine politisch-normativen Terme.

# WIRING für main.py:
# from services.europeana import (
#     search_europeana, claim_mentions_europeana_cached,
# )
# if claim_mentions_europeana_cached(claim):
#     tasks.append(cached("Europeana", search_europeana, analysis))
#     queried_names.append("Europeana")
#
# WIRING für reranker.py (Indicator-Whitelist):
#   "europeana_" als Whitelist-Prefix in INDICATOR_WHITELIST_PREFIXES
#
# data_updater.py: KEIN Prefetch (Live-Only, kein Static-Pack)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from functools import lru_cache

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

EUROPEANA_SEARCH_URL = "https://api.europeana.eu/record/v2/search.json"

HTTP_TIMEOUT_S = 20.0
RESULT_LIMIT = 5

# In-Memory-Cache: term → (timestamp, result-dict)
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 24 * 3600.0  # 24 h


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Explizite Quelle-Mentions
_EXPLICIT_TERMS = (
    "europeana",
    "digitalisat", "digitalisate",
    "digital heritage", "digitales kulturerbe", "digitales erbe",
    "cultural heritage online",
)

# AT-Institutionen direkt im Europeana-Pool
_AT_INSTITUTIONS = (
    "önb", "oenb",
    "österreichische nationalbibliothek", "oesterreichische nationalbibliothek",
    "khm",
    "kunsthistorisches museum",
    "albertina",
    "wien museum", "wienmuseum",
    "mak wien", "museum für angewandte kunst",
    "belvedere museum", "österreichische galerie belvedere",
    "leopold museum",
    "naturhistorisches museum wien",
    "technisches museum wien",
)

# AT-Künstler (komplementär zu Wikidata-Personen-Triples)
_AT_ARTISTS = (
    "klimt", "gustav klimt",
    "schiele", "egon schiele",
    "kokoschka", "oskar kokoschka",
    "hundertwasser", "friedensreich hundertwasser",
    "waldmüller", "ferdinand georg waldmüller",
    "makart", "hans makart",
    "moser", "koloman moser",
    "hoffmann", "josef hoffmann",
    "loos", "adolf loos",
    "wagner", "otto wagner",
    "mozart", "wolfgang amadeus mozart",
    "haydn", "joseph haydn",
    "schubert", "franz schubert",
    "mahler", "gustav mahler",
    "bruckner", "anton bruckner",
    "strauss", "johann strauss",
    "beethoven",
)

# Generische Werk-/Objekt-Trigger (nur in Kombination mit Entity-Hint)
_WORK_OBJECT_TRIGGERS = (
    "gemälde", "ölgemälde", "gemalt",  # 2026-05-23: Verb-Form "gemalt"
    "kunstwerk", "kunstwerke", "artwork",
    "skulptur",
    "manuskript", "handschrift",
    "inkunabel", "wiegendruck",
    "stich", "kupferstich",
    "fotosammlung", "fotoarchiv",
    "museumssammlung", "sammlungsbestand",
)


def _has_any(claim_lc: str, terms: tuple[str, ...]) -> bool:
    return any(t in claim_lc for t in terms)


def _claim_mentions_europeana(claim_lc: str) -> bool:
    """Trigger-Check für Europeana-Lookup.

    True bei:
    - Expliziter Mention ("Europeana", "Digitalisat", …)
    - AT-Institution (ÖNB, KHM, Albertina, Wien Museum …)
    - AT-Künstler-Name (Klimt, Schiele, Mozart, Schubert …)
    - Generischer Werk-/Objekt-Trigger + AT-/Wien-Kontext
    """
    if not claim_lc:
        return False

    if _has_any(claim_lc, _EXPLICIT_TERMS):
        return True
    if _has_any(claim_lc, _AT_INSTITUTIONS):
        return True
    if _has_any(claim_lc, _AT_ARTISTS):
        return True

    # Composite: Werk-/Objekt-Trigger + AT-/Wien-/EU-Kontext
    has_work = _has_any(claim_lc, _WORK_OBJECT_TRIGGERS)
    has_context = any(t in claim_lc for t in (
        "österreich", "austria",
        "wien", "vienna",
        "europa", "europäisch",
        "salzburg", "graz", "linz", "innsbruck",
    ))
    if has_work and has_context:
        return True

    return False


@lru_cache(maxsize=2048)
def claim_mentions_europeana_cached(claim: str) -> bool:
    """LRU-gecachter Trigger-Check (Hot-Path-friendly)."""
    return _claim_mentions_europeana((claim or "").lower())


# ---------------------------------------------------------------------------
# Query-Term-Extraktion
# ---------------------------------------------------------------------------
# Pool aller lookup-würdigen Begriffe — diese haben sicher Europeana-Treffer.
_LOOKUP_TERMS: tuple[str, ...] = _AT_ARTISTS + _AT_INSTITUTIONS


def _extract_query_term(claim_lc: str, analysis: dict) -> str | None:
    """Wähle den am besten passenden Begriff für den Europeana-Lookup.

    Priorität:
    1. Substring-Match auf bekannten Künstler-/Institutions-Begriffen
       (höchste Trefferchance im Europeana-Pool).
    2. Erste Entity aus analysis.entities mit ≥3 Zeichen.
    """
    if not claim_lc:
        return None
    for term in _LOOKUP_TERMS:
        if term in claim_lc:
            return term

    entities = (analysis or {}).get("entities") or []
    for e in entities:
        if isinstance(e, str) and len(e.strip()) >= 3:
            return e.strip()
    return None


# ---------------------------------------------------------------------------
# HTTP-Lookup
# ---------------------------------------------------------------------------
async def _run_search(client, api_key: str, query: str) -> list[dict] | None:
    """Führt Europeana-Search aus. Returns Items-Liste oder None bei Fehler."""
    try:
        resp = await client.get(
            EUROPEANA_SEARCH_URL,
            params={
                "wskey": api_key,
                "query": query,
                "rows": RESULT_LIMIT,
                "profile": "minimal",
            },
            follow_redirects=True,
        )
        if resp.status_code != 200:
            logger.debug(
                f"Europeana HTTP {resp.status_code} "
                f"(body: {resp.text[:120]!r})"
            )
            return None
        data = resp.json()
        if not data.get("success"):
            logger.debug(f"Europeana success=false: {data.get('error', '?')[:120]}")
            return None
        return data.get("items") or []
    except Exception as e:
        logger.debug(f"Europeana fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _first_str(val) -> str:
    """Europeana-Felder sind oft Listen — wir nehmen den ersten String."""
    if isinstance(val, list):
        for x in val:
            if isinstance(x, str) and x.strip():
                return x.strip()
        return ""
    if isinstance(val, str):
        return val.strip()
    return ""


def _trim(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


def _safe_id_suffix(europeana_id: str) -> str:
    """Europeana-IDs wie '/15503/FS_PA84283alt' → 'eur_15503_FS_PA84283alt'."""
    cleaned = (europeana_id or "").strip().lstrip("/").replace("/", "_")
    # Schutz vor Sonderzeichen im Indicator-Key
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in cleaned)
    return safe[:80] or "unknown"


def _build_result_row(item: dict) -> dict | None:
    title = _first_str(item.get("title"))
    europeana_id = item.get("id") or ""
    guid = item.get("guid") or ""
    if not (title and europeana_id):
        return None

    provider = _first_str(item.get("dataProvider")) or _first_str(item.get("provider"))
    country = _first_str(item.get("country"))
    year = _first_str(item.get("year"))
    obj_type = _first_str(item.get("type"))
    rights = _first_str(item.get("rights"))

    # Display-Headline: "Titel — Provider (Land, Jahr)"
    bits = [title]
    meta_bits = [b for b in (provider, country, year) if b]
    if meta_bits:
        bits.append("— " + ", ".join(meta_bits))
    if obj_type:
        bits.append(f"[{obj_type}]")
    display_value = _trim(" ".join(bits), 280)

    description_parts: list[str] = []
    if provider:
        description_parts.append(f"Institution: {provider}.")
    if country:
        description_parts.append(f"Land: {country}.")
    if year:
        description_parts.append(f"Datierung: {year}.")
    if obj_type:
        description_parts.append(f"Objekttyp: {obj_type}.")
    if rights:
        description_parts.append(f"Rechte: {rights}.")
    description = _trim(" ".join(description_parts) or
                        "Europeana-Treffer (kein zusätzliches Metadatum).",
                        500)

    return {
        "indicator_name": f"Europeana: {title}",
        "indicator": f"europeana_{_safe_id_suffix(europeana_id)}",
        "country": "—",
        "country_name": country or "—",
        "year": year or "",
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": guid or f"https://www.europeana.eu/item{europeana_id}",
        "source": "Europeana (EUPL v1.2 Metadata)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_europeana(analysis: dict) -> dict:
    """Live-Lookup gegen Europeana Search-API.

    Returns Dict mit ≤5 Kulturerbe-Treffern. Bei fehlendem API-Key /
    Trigger-Miss / 0 Treffern / API-Fehler: leere results-Liste
    (graceful fail).
    """
    empty = {
        "source": "Europeana",
        "type": "cultural_heritage",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_europeana(matchable):
        return empty

    api_key = os.getenv("EUROPEANA_API_KEY", "").strip()
    if not api_key:
        logger.info("Europeana: kein EUROPEANA_API_KEY gesetzt — skip")
        return empty

    term = _extract_query_term(matchable, analysis)
    if not term:
        return empty

    cache_key = f"eur::{term.lower()}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0] < _CACHE_TTL_S):
        logger.info(f"Europeana: Cache-Hit für '{term[:40]}'")
        return cached[1]

    async with polite_client(timeout=HTTP_TIMEOUT_S) as client:
        try:
            items = await asyncio.wait_for(
                _run_search(client, api_key, term),
                timeout=HTTP_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.info(f"Europeana: Timeout für '{term[:40]}'")
            return empty

    if not items:
        logger.info(f"Europeana: 0 Treffer für '{term[:40]}'")
        _CACHE[cache_key] = (now, empty)
        return empty

    results: list[dict] = []
    seen_ids: set[str] = set()
    for item in items[:RESULT_LIMIT]:
        try:
            built = _build_result_row(item)
        except Exception as e:
            logger.debug(f"Europeana: Format-Fehler bei item: {e}")
            continue
        if not built:
            continue
        ind = built.get("indicator", "")
        if ind in seen_ids:
            continue
        seen_ids.add(ind)
        results.append(built)

    out = {
        "source": "Europeana",
        "type": "cultural_heritage",
        "results": results,
    }
    _CACHE[cache_key] = (now, out)
    if results:
        logger.info(
            f"Europeana: {len(results)} Treffer für '{term[:40]}'"
        )
    return out
