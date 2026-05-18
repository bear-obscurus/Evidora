"""DPLA Live-Connector — Digital Public Library of America.

Die DPLA (https://dp.la) aggregiert ~50 Mio. digitalisierte Objekte aus
US-Bibliotheken, -Museen, -Archiven und -Universitaeten (u.a. Library of
Congress, Smithsonian, HathiTrust, Internet Archive, NYPL). Pendant zu
Europeana, aber mit US-Fokus — die beiden duerfen parallel feuern und
ergaenzen sich (EU vs. US-Quellen-Pool).

Komplementaer zu existierenden Quellen:
- Europeana (services/europeana.py): EU-Kulturerbe.
- DOAB (services/doab.py): Open-Access-Buecher.
- Wikipedia / Wikidata: Strukturiertes Wissen.
- DPLA: Konkrete US-Objekt-Nachweise (Manuskripte, Fotos, Audio,
  Karten, Briefe) — Provenienz aus US-Sammlungen.

API: https://api.dp.la/v2/items
- Free Key via https://pro.dp.la/developers/api-codex (env: DPLA_API_KEY)
- JSON-Response, JSON-LD-Schema (sourceResource-Wrapper)
- Lizenz Metadata: CC0; Objektrechte variieren per Item ("rights")

Strategie: Query-Param-Search ueber ?q= Top-Term (Trigger-Substring oder
erste Entity >=3 Zeichen). Wir holen Top-5 Treffer und stellen Titel/
Provider/Datum/Typ/Rechte als strukturierte Treffer dar.

Politische Guardrails: Reine Kulturgueter-Metadaten aus US-Pool. Keine
politische Bewertung, keine eigene Klassifikation. Trigger enthaelt
keine politisch-normativen Terme.

# WIRING fuer main.py:
# from services.dpla import (
#     search_dpla, claim_mentions_dpla_cached,
# )
# if claim_mentions_dpla_cached(claim):
#     tasks.append(cached("DPLA", search_dpla, analysis))
#     queried_names.append("DPLA")
#
# WIRING fuer reranker.py (Indicator-Whitelist):
#   "dpla_" als Whitelist-Prefix in INDICATOR_WHITELIST_PREFIXES
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

DPLA_SEARCH_URL = "https://api.dp.la/v2/items"

HTTP_TIMEOUT_S = 20.0
RESULT_LIMIT = 5

# In-Memory-Cache: term -> (timestamp, result-dict)
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 24 * 3600.0  # 24 h


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Explizite Quelle-Mentions
_EXPLICIT_TERMS = (
    "dpla",
    "digital public library",
    "digital public library of america",
)

# US-Institutionen direkt im DPLA-Pool
_US_INSTITUTIONS = (
    "library of congress", "loc archive", "loc collection",
    "smithsonian",
    "hathitrust", "hathi trust",
    "internet archive",
    "nypl", "new york public library",
    "boston public library",
    "national archives", "us national archives", "nara",
    "ushmm", "united states holocaust memorial museum",
    "holocaust museum",
    "mountain west digital library",
    "digital commonwealth",
    "minnesota digital library",
    "north carolina digital heritage",
    "south carolina digital library",
    "mountain west",
    "harvard library", "harvard digital",
    "stanford libraries",
    "yale library",
    "university of california libraries",
)

# US-Historische Ereignisse / Personen mit hoher DPLA-Trefferchance
_US_HISTORY_TERMS = (
    # Buergerkrieg / Civil War
    "civil war", "us civil war", "american civil war",
    "us-buergerkrieg", "us-bürgerkrieg", "us buergerkrieg", "us bürgerkrieg",
    "amerikanischer buergerkrieg", "amerikanischer bürgerkrieg",
    "konfoederierte", "konföderierte", "confederate", "confederacy", "union army",
    # Praesidenten / Persoenlichkeiten
    "abraham lincoln", "lincoln",
    "george washington", "founding fathers",
    "thomas jefferson", "jefferson papers",
    "frederick douglass",
    "martin luther king", "mlk", "martin luther king jr",
    "rosa parks",
    "harriet tubman",
    "malcolm x",
    "john f. kennedy", "jfk", "kennedy papers",
    "franklin delano roosevelt", "franklin d roosevelt", "fdr",
    "theodore roosevelt",
    # Bewegungen / Epochen
    "civil rights movement", "civil-rights movement",
    "buergerrechtsbewegung", "bürgerrechtsbewegung",
    "abolition", "abolitionism", "anti-slavery", "abolitionist",
    "reconstruction era", "reconstruction-era",
    "great depression",
    "new deal", "new-deal",
    "harlem renaissance",
    "manhattan project",
    "wpa", "works progress administration",
    "dust bowl",
    "underground railroad",
    "japanese internment", "japanese-american internment",
    # Native American
    "native american", "native-american", "american indian",
    "trail of tears", "indian removal",
    "treaty of fort laramie",
    # Geographie / Symbole
    "ellis island",
    "mount rushmore",
    "westward expansion",
    "manifest destiny",
    # USHMM-Komplement (Holocaust)
    "ushmm materials", "ushmm holocaust", "holocaust materials usa",
    "holocaust-materialien usa",
)

# Generische Werk-/Objekt-Trigger (nur in Kombination mit US-Kontext)
_WORK_OBJECT_TRIGGERS = (
    "manuskript", "manuscript", "handschrift",
    "fotografie", "foto", "photograph", "photo",
    "tonaufnahme", "audio recording", "oral history",
    "filmaufnahme", "moving image",
    "tagebuch", "diary", "letter", "correspondence",
    "brief", "briefe",
    "karte", "map", "historische karte",
    "zeitungsausschnitt", "newspaper clipping",
    "primary source", "primary-source", "primaerquelle", "primärquelle",
    "archive collection", "archivbestand", "archivmaterial",
)


def _has_any(claim_lc: str, terms: tuple[str, ...]) -> bool:
    return any(t in claim_lc for t in terms)


def _claim_mentions_dpla(claim_lc: str) -> bool:
    """Trigger-Check fuer DPLA-Lookup.

    True bei:
    - Expliziter Mention ("DPLA", "Digital Public Library", ...)
    - US-Institution (Library of Congress, Smithsonian, NYPL, ...)
    - US-Historischer Person / Ereignis (Lincoln, Civil War, MLK, ...)
    - Generischer Werk-Trigger + US-Kontext
    """
    if not claim_lc:
        return False

    if _has_any(claim_lc, _EXPLICIT_TERMS):
        return True
    if _has_any(claim_lc, _US_INSTITUTIONS):
        return True
    if _has_any(claim_lc, _US_HISTORY_TERMS):
        return True

    # Composite: Werk-/Objekt-Trigger + US-Kontext
    has_work = _has_any(claim_lc, _WORK_OBJECT_TRIGGERS)
    has_context = any(t in claim_lc for t in (
        "usa", "u.s.a.", "u.s.", " us ", "vereinigte staaten",
        "amerikanisch", "american",
        "washington d.c.", "washington dc",
        "new york", "boston", "philadelphia", "virginia",
        "california", "kalifornien",
        "texas", "chicago",
    ))
    if has_work and has_context:
        return True

    return False


@lru_cache(maxsize=2048)
def claim_mentions_dpla_cached(claim: str) -> bool:
    """LRU-gecachter Trigger-Check (Hot-Path-friendly)."""
    return _claim_mentions_dpla((claim or "").lower())


# ---------------------------------------------------------------------------
# Query-Term-Extraktion
# ---------------------------------------------------------------------------
# Pool aller lookup-wuerdigen Begriffe — diese haben sicher DPLA-Treffer.
_LOOKUP_TERMS: tuple[str, ...] = _US_HISTORY_TERMS + _US_INSTITUTIONS


def _extract_query_term(claim_lc: str, analysis: dict) -> str | None:
    """Waehle den am besten passenden Begriff fuer den DPLA-Lookup.

    Prioritaet:
    1. Substring-Match auf bekannten US-Historik-/Institutions-Begriffen.
    2. Erste Entity aus analysis.entities mit >=3 Zeichen.
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
    """Fuehrt DPLA-Search aus. Returns docs-Liste oder None bei Fehler."""
    try:
        params = {
            "q": query,
            "page_size": RESULT_LIMIT,
        }
        if api_key:
            params["api_key"] = api_key
        resp = await client.get(
            DPLA_SEARCH_URL,
            params=params,
            follow_redirects=True,
        )
        if resp.status_code == 401 or resp.status_code == 403:
            logger.debug(
                f"DPLA HTTP {resp.status_code} — API-Key invalid/missing "
                f"(body: {resp.text[:120]!r})"
            )
            return None
        if resp.status_code != 200:
            logger.debug(
                f"DPLA HTTP {resp.status_code} "
                f"(body: {resp.text[:120]!r})"
            )
            return None
        data = resp.json()
        return data.get("docs") or []
    except Exception as e:
        logger.debug(f"DPLA fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _first_str(val) -> str:
    """DPLA-Felder sind oft Listen — wir nehmen den ersten String."""
    if isinstance(val, list):
        for x in val:
            if isinstance(x, str) and x.strip():
                return x.strip()
            if isinstance(x, dict):
                for cand_key in ("name", "displayDate", "@id", "title"):
                    cand = x.get(cand_key)
                    if isinstance(cand, str) and cand.strip():
                        return cand.strip()
        return ""
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, dict):
        for cand_key in ("name", "displayDate", "@id", "title"):
            cand = val.get(cand_key)
            if isinstance(cand, str) and cand.strip():
                return cand.strip()
    return ""


def _trim(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


def _safe_id_suffix(dpla_id: str) -> str:
    """DPLA-IDs (Hash) absichern fuer Indicator-Key."""
    cleaned = (dpla_id or "").strip().lstrip("/").replace("/", "_")
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in cleaned)
    return safe[:80] or "unknown"


def _build_result_row(item: dict) -> dict | None:
    src = item.get("sourceResource") or {}
    title = _first_str(src.get("title"))
    dpla_id = item.get("id") or ""
    if not (title and dpla_id):
        return None

    # Provider / Institution
    provider_obj = item.get("provider") or {}
    provider = _first_str(provider_obj.get("name")) if isinstance(provider_obj, dict) else _first_str(provider_obj)
    data_provider = _first_str(item.get("dataProvider"))
    institution = data_provider or provider or ""

    # Datum
    date_obj = src.get("date")
    if isinstance(date_obj, list) and date_obj:
        date_obj = date_obj[0]
    if isinstance(date_obj, dict):
        date_str = _first_str(date_obj.get("displayDate")) or _first_str(date_obj.get("begin"))
    else:
        date_str = _first_str(date_obj)

    # Typ
    obj_type = _first_str(src.get("type"))
    # Creator
    creator = _first_str(src.get("creator"))
    # Rechte
    rights = _first_str(src.get("rights")) or _first_str(item.get("rights"))
    # URL: isShownAt = Direkt-Link zum Original-Host
    is_shown_at = _first_str(item.get("isShownAt"))
    # Description (kurz)
    description_src = _first_str(src.get("description"))

    # Display-Headline: "Titel - Creator (Institution, Datum)"
    bits = [title]
    meta_bits = [b for b in (creator, institution, date_str) if b]
    if meta_bits:
        bits.append("— " + ", ".join(meta_bits))
    if obj_type:
        bits.append(f"[{obj_type}]")
    display_value = _trim(" ".join(bits), 280)

    description_parts: list[str] = []
    if creator:
        description_parts.append(f"Urheber: {creator}.")
    if institution:
        description_parts.append(f"Institution: {institution}.")
    if date_str:
        description_parts.append(f"Datierung: {date_str}.")
    if obj_type:
        description_parts.append(f"Objekttyp: {obj_type}.")
    if description_src:
        description_parts.append(_trim(description_src, 200))
    if rights:
        description_parts.append(f"Rechte: {_trim(rights, 100)}.")
    description = _trim(
        " ".join(description_parts) or
        "DPLA-Treffer (kein zusaetzliches Metadatum).",
        500,
    )

    url = is_shown_at or f"https://dp.la/item/{_safe_id_suffix(dpla_id)}"

    return {
        "indicator_name": f"DPLA: {title}",
        "indicator": f"dpla_{_safe_id_suffix(dpla_id)}",
        "country": "US",
        "country_name": "USA",
        "year": date_str or "",
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": url,
        "source": "DPLA (CC0 Metadata)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_dpla(analysis: dict) -> dict:
    """Live-Lookup gegen DPLA Search-API.

    Returns Dict mit <=5 US-Kulturerbe-Treffern. Bei fehlendem API-Key /
    Trigger-Miss / 0 Treffern / API-Fehler: leere results-Liste
    (graceful fail).
    """
    empty = {
        "source": "DPLA",
        "type": "cultural_heritage_us",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_dpla(matchable):
        return empty

    api_key = os.getenv("DPLA_API_KEY", "").strip()
    if not api_key:
        logger.info(
            "DPLA: kein DPLA_API_KEY gesetzt — skip "
            "(free key: https://pro.dp.la/developers/api-codex)"
        )
        return empty

    term = _extract_query_term(matchable, analysis)
    if not term:
        return empty

    cache_key = f"dpla::{term.lower()}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0] < _CACHE_TTL_S):
        logger.info(f"DPLA: Cache-Hit fuer '{term[:40]}'")
        return cached[1]

    async with polite_client(timeout=HTTP_TIMEOUT_S) as client:
        try:
            items = await asyncio.wait_for(
                _run_search(client, api_key, term),
                timeout=HTTP_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.info(f"DPLA: Timeout fuer '{term[:40]}'")
            return empty

    if not items:
        logger.info(f"DPLA: 0 Treffer fuer '{term[:40]}'")
        _CACHE[cache_key] = (now, empty)
        return empty

    results: list[dict] = []
    seen_ids: set[str] = set()
    for item in items[:RESULT_LIMIT]:
        try:
            built = _build_result_row(item)
        except Exception as e:
            logger.debug(f"DPLA: Format-Fehler bei item: {e}")
            continue
        if not built:
            continue
        ind = built.get("indicator", "")
        if ind in seen_ids:
            continue
        seen_ids.add(ind)
        results.append(built)

    out = {
        "source": "DPLA",
        "type": "cultural_heritage_us",
        "results": results,
    }
    _CACHE[cache_key] = (now, out)
    if results:
        logger.info(
            f"DPLA: {len(results)} Treffer fuer '{term[:40]}'"
        )
    return out
