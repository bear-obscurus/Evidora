"""NBER — National Bureau of Economic Research Working Papers.

Datenquelle: https://www.nber.org/ — Goldstandard der US-Wirtschafts-
forschung. ~1.500 neue Working Papers pro Jahr, alle wichtigen
US-Volkswirte (inkl. ~30 spätere Nobelpreisträger:innen) publizieren
hier Vorab-Manuskripte BEVOR die Arbeiten in QJE/AER/JPE peer-reviewed
erscheinen — typischer Vorlauf 1–2 Jahre.

Themen-Cluster (NBER-Programmes): Macro, Labor, Trade, Finance, Public
Economics, Industrial Organization, Health, Education, Development,
Productivity, Children, Aging, Environmental & Energy Economics.

Komplementär zu existierenden Wirtschafts-Quellen:
- IMF/Worldbank/OECD/DBnomics: aggregierte Statistik, KEINE Forschung
- ECB/BIS/OeNB: Geldpolitik + Banken-Statistik, KEINE NBER-Themen
- WIFO/IHS: AT-spezifische Wirtschaftsforschung (DE)
- arXiv (econ): überschneidet sich nur teilweise (econ.GN/econ.EM)
- OpenAlex/Crossref: peer-reviewed Endprodukt (Lag 1–2 Jahre vs NBER)
- NBER hier: bleeding-edge US-Wirtschaftsforschung als Working Paper

API-Architektur (Hybrid Pack+Live):
  1. RSS-Feed (https://www.nber.org/rss/new.xml): letzte ~30 frische
     Working Papers, vom Modul beim ersten Aufruf gecacht (24h-TTL).
     Liefert Title + Abstract + Link + Authors.
  2. JSON-Search-API (https://www.nber.org/api/v1/working_page_listing/
     contentType/working_paper/_/_/search?q=…): keyword-getriebene
     Live-Suche mit Volltext-Match, sortiert nach Relevance/Date.
     Wird nur ausgelöst, wenn Trigger UND nicht-leere Queries.

Strategie:
  - Bei Trigger: zuerst RSS-Cache (30-Tage-Window) lokal filtern auf
    Entity-/Query-Overlap; bei < 2 RSS-Treffern zusätzlich Live-Search-
    API anstoßen, um ältere passende Papers zu finden.

Lizenz: Metadata frei zugänglich, Volltexte (PDF) teilweise paywalled
(NBER-Subscriber / akademische Lizenzierung). Wir verlinken auf die
Abstract-Seite — Anwender:innen mit institutionellem Zugang sehen
das PDF, andere erhalten Abstract + Methoden-Kurzfassung.

Disclaimer: NBER Working Papers sind NICHT peer-reviewed im Sinne
eines Journals — sie durchlaufen interne NBER-Konferenz-Discussions
und Author-Network-Review, formales Journal-Review erfolgt später.
Das ist im Wirtschaftswissenschafts-Kontext der etablierte
Veröffentlichungs-Standard für Forschungsfront-Befunde, muss aber
im Synthesizer-Output ausgewiesen werden.

Politische Guardrails (memory/project_political_guardrails.md):
  - Reine Studien-Wiedergabe (Titel/Abstract/Autor:innen/Datum)
  - KEINE Bewertung der politischen Implikationen einzelner Papers
  - KEINE Aussagen über Methoden-Qualität einzelner Working Papers
  - Disclaimer "Working Paper ≠ peer-reviewed" im description-Feld

Trigger-Keywords: "NBER", "NBER Working Paper", "US-Wirtschafts-
forschung", "Macro/Labor/Trade Working Paper" + Hybrid-Composite
(Working-Paper-Word UND Econ-Topic-Hinweis im Claim).

# ---------------------------------------------------------------------------
# WIRING-SNIPPET (NICHT in dieser Datei applizieren, nur Anleitung!)
# ---------------------------------------------------------------------------
# In main.py (nach den anderen Wissenschafts-Live-Quellen, neben arxiv):
#   from services.nber import search_nber, claim_mentions_nber_cached
#   ...
#   if claim_mentions_nber_cached(claim):
#       tasks.append(cached("NBER", search_nber, analysis))
#       queried_names.append("NBER Working Papers")
#
# In services/reranker.py: NICHT in AUTHORITATIVE_INDICATORS aufnehmen
# (Working Papers sind keine peer-reviewed Endprodukte, sollen den
# normalen Cosine-Re-Rank durchlaufen).
#
# In services/data_updater.py (OPTIONAL): kein Prefetch erforderlich.
# Das Modul holt den RSS-Feed lazy beim ersten Trigger-Hit und cached
# 24h. Wer einen Cold-Start-Boost will, kann hinzufügen:
#   from services.nber import fetch_nber_rss
#   await fetch_nber_rss(client)  # in der Prefetch-gather-Liste
# ---------------------------------------------------------------------------
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import datetime
from urllib.parse import quote_plus
from xml.etree import ElementTree as ET

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

NBER_RSS_URL = "https://www.nber.org/rss/new.xml"
NBER_SEARCH_API = (
    "https://www.nber.org/api/v1/working_page_listing/"
    "contentType/working_paper/_/_/search"
)
NBER_BASE = "https://www.nber.org"

TIMEOUT_S = 15.0
RSS_CACHE_TTL_S = 24 * 60 * 60  # 24h
SEARCH_CACHE_TTL_S = 24 * 60 * 60  # 24h
MAX_RSS_RESULTS = 5
MAX_SEARCH_RESULTS = 5
MAX_TOTAL_RESULTS = 6  # Cap RSS + Search aggregiert

# Modul-Level RSS-Cache (lazy, 24h)
_rss_cache: list[dict] | None = None
_rss_cache_time: float = 0.0

# Modul-Level Search-Cache: query-key → (ts, results)
_search_cache: dict[str, tuple[float, list[dict]]] = {}

# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Direkte NBER-Marker (single-word/-phrase ausreichend)
_NBER_DIRECT_TRIGGERS = (
    "nber",
    "national bureau of economic research",
    "nber working paper",
    "nber-working-paper",
    "nber-studie",
    "nber studie",
    "us-wirtschaftsforschung",
    "us wirtschaftsforschung",
    "us-economic research",
    "us economic research",
    "working paper macro",
    "working paper labor",
    "working paper trade",
    "working-paper macro",
    "working-paper labor",
    "working-paper trade",
)

# Composite-Trigger: Working-Paper-Marker UND Econ-Topic
_WORKING_PAPER_TERMS = (
    "working paper", "working-paper",
    "diskussionspapier", "discussion paper", "discussion-paper",
    "preprint econ", "econ preprint",
)
_ECON_TOPIC_TERMS = (
    "makroökonom", "makrooekonom", "macro economics", "macroeconomic",
    "geldpolitik", "monetary policy", "fed policy", "federal reserve",
    "fiskalpolitik", "fiscal policy", "fiscal stimulus",
    "arbeitsmarkt", "labor market", "labour market",
    "lohnentwicklung", "wage growth", "wage dynamics",
    "handelspolitik", "trade policy", "tariff", "trade war",
    "trade deficit", "handelsbilanz",
    "produktivität", "produktivitaet", "productivity growth",
    "inflation", "deflation", "stagflation",
    "rezession", "recession",
    "ungleichheit", "inequality", "income inequality",
    "konjunktur", "business cycle",
    "humankapital", "human capital",
    "innovation policy", "r&d policy",
    "industrial organization", "industrieökonomik", "industrieoekonomik",
    "public economics", "public finance",
    "gesundheitsökonomik", "gesundheitsoekonomik", "health economics",
    "education economics", "bildungsökonomik", "bildungsoekonomik",
    "development economics", "entwicklungsökonomik",
    "environmental economics", "umweltökonomik",
)


def _claim_mentions_nber(claim_lc: str) -> bool:
    """Reine Substring-Heuristik auf bereits lowercased Claim."""
    if not claim_lc:
        return False
    # 1. Direkter NBER-Marker reicht
    if any(t in claim_lc for t in _NBER_DIRECT_TRIGGERS):
        return True
    # 2. Composite: Working-Paper UND Econ-Topic
    has_wp = any(t in claim_lc for t in _WORKING_PAPER_TERMS)
    if not has_wp:
        return False
    has_econ = any(t in claim_lc for t in _ECON_TOPIC_TERMS)
    return has_econ


def claim_mentions_nber_cached(claim: str) -> bool:
    """Public Trigger-Predicate (Erwartung von main.py: lowercase-tolerant).

    Idempotent + ohne I/O — wird in der dispatch-Schleife mehrfach
    aufgerufen, daher keine HTTP-Calls hier.
    """
    cl = (claim or "").lower()
    return _claim_mentions_nber(cl)


# ---------------------------------------------------------------------------
# RSS-Cache (lazy 24h)
# ---------------------------------------------------------------------------
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", _HTML_TAG_RE.sub(" ", text)).strip()


def _parse_rfc822_year(pub_date: str) -> str:
    if not pub_date:
        return "—"
    try:
        dt = datetime.strptime(pub_date[:25].strip(), "%a, %d %b %Y %H:%M:%S")
        return str(dt.year)
    except Exception:
        return "—"


def _split_title_authors(rss_title: str) -> tuple[str, str]:
    """RSS-Titel kommt im Format 'Paper Title -- by Author A, Author B'.

    Returns (title, authors_str). Authors '—' wenn Format nicht passt.
    """
    if not rss_title:
        return "", "—"
    parts = re.split(r"\s+--\s+by\s+", rss_title, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return rss_title.strip(), "—"


def _extract_paper_id_from_url(url: str) -> str:
    """Extrahiere NBER-WP-ID (z.B. 'w35197') aus URL."""
    if not url:
        return ""
    m = re.search(r"/papers/(w\d+)", url)
    return m.group(1) if m else ""


async def fetch_nber_rss(
    client: httpx.AsyncClient | None = None,
) -> list[dict]:
    """Holt + cached den NBER-RSS-Feed (24h TTL).

    Returns Liste normalisierter Paper-Dicts. Bei Fetch-Fehler wird der
    bestehende Cache zurückgegeben (auch wenn abgelaufen). Bei Cold-
    Start-Fehler: leere Liste.

    Kann von data_updater.py via gather() prefetched werden (optional).
    """
    global _rss_cache, _rss_cache_time

    now = time.time()
    if _rss_cache is not None and (now - _rss_cache_time) < RSS_CACHE_TTL_S:
        return _rss_cache

    own_client = False
    if client is None:
        client = polite_client(timeout=TIMEOUT_S)
        own_client = True

    try:
        resp = await client.get(NBER_RSS_URL, follow_redirects=True)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        logger.warning(f"NBER RSS fetch failed: {e}")
        if own_client:
            await client.aclose()
        # Bei Fehler: bestehender Cache (auch stale) oder leere Liste
        return _rss_cache or []

    try:
        items: list[dict] = []
        for item in root.findall(".//item"):
            raw_title = (item.findtext("title") or "").strip()
            raw_desc = (item.findtext("description") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            if not raw_title or not link:
                continue
            title, authors = _split_title_authors(raw_title)
            abstract = _strip_html(raw_desc)
            paper_id = _extract_paper_id_from_url(link)
            year = _parse_rfc822_year(pub_date)
            # RSS-Feed liefert oft KEINE pubDate — Fallback aufs aktuelle
            # Jahr, weil der RSS-Feed per Konstruktion „letzte ~30 Tage"
            # ausgibt. So bleibt der year-Slot informativ statt '—'.
            if year == "—":
                year = str(datetime.utcnow().year)
            items.append({
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "url": link.split("#")[0],  # strip #fromrss-Anchor
                "paper_id": paper_id,
                "year": year,
                "pub_date": pub_date,
            })

        _rss_cache = items
        _rss_cache_time = now
        logger.info(f"NBER RSS prefetched: {len(items)} working papers")
        return items
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# JSON-Search-API (Live-Lookup, on-demand)
# ---------------------------------------------------------------------------
def _clean_html_authors(authors_raw: list) -> str:
    """JSON-Authors-Feld enthält HTML-Anchors — strip + join."""
    if not authors_raw or not isinstance(authors_raw, list):
        return "—"
    cleaned: list[str] = []
    for a in authors_raw:
        if not isinstance(a, str):
            continue
        name = _strip_html(a)
        if name:
            cleaned.append(name)
    if not cleaned:
        return "—"
    if len(cleaned) <= 5:
        return " / ".join(cleaned)
    return f"{' / '.join(cleaned[:5])} et al."


async def _search_nber_api(
    client: httpx.AsyncClient,
    query: str,
) -> list[dict]:
    """Live-Search gegen NBER JSON-API (sortiert nach Relevance/Date)."""
    if not query or len(query.strip()) < 3:
        return []

    cache_key = query.strip().lower()
    now = time.time()
    cached = _search_cache.get(cache_key)
    if cached and (now - cached[0]) < SEARCH_CACHE_TTL_S:
        return cached[1]

    url = (
        f"{NBER_SEARCH_API}"
        f"?page=1&perPage={MAX_SEARCH_RESULTS}"
        f"&q={quote_plus(query.strip())}"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(f"NBER search HTTP {resp.status_code} for '{query[:40]}'")
            return []
        payload = resp.json()
    except Exception as e:
        logger.debug(f"NBER search failed for '{query[:30]}': {e}")
        return []

    raw_results = payload.get("results") or []
    items: list[dict] = []
    for r in raw_results[:MAX_SEARCH_RESULTS]:
        title = _strip_html((r.get("title") or "").strip())
        if not title:
            continue
        authors = _clean_html_authors(r.get("authors") or [])
        abstract = _strip_html((r.get("abstract") or "").strip())
        display_date = (r.get("displaydate") or "").strip()
        rel_url = (r.get("url") or "").strip()
        full_url = (
            f"{NBER_BASE}{rel_url}"
            if rel_url.startswith("/")
            else rel_url
        )
        paper_id = _extract_paper_id_from_url(rel_url)
        # Year aus displaydate: "May 2026" → "2026"
        year = "—"
        if display_date:
            m = re.search(r"(\d{4})", display_date)
            if m:
                year = m.group(1)
        items.append({
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "url": full_url,
            "paper_id": paper_id,
            "year": year,
            "pub_date": display_date,
        })

    _search_cache[cache_key] = (now, items)
    return items


# ---------------------------------------------------------------------------
# Lokales Filter (RSS-Cache → entity/query-Overlap)
# ---------------------------------------------------------------------------
def _rank_rss_items(
    items: list[dict],
    entities: list[str],
    queries: list[str],
) -> list[dict]:
    """Score RSS-Items nach Entity-/Query-Overlap. Sortiert + cuttet.

    Score: Anzahl matchender Entity-Strings + Anzahl matchender
    Query-Wörter (>=4 chars). Items mit Score 0 fallen weg.
    """
    entity_terms = [e.lower() for e in entities if len(e) >= 3]
    query_words: set[str] = set()
    for q in queries[:4]:
        for w in q.split():
            if len(w) >= 4:
                query_words.add(w.lower())

    if not entity_terms and not query_words:
        return []

    scored: list[tuple[int, dict]] = []
    for it in items:
        haystack = f"{it.get('title', '')} {it.get('abstract', '')}".lower()
        score = 0
        for e in entity_terms:
            if e in haystack:
                score += 2  # Entities sind spezifischer → höherer Score
        for w in query_words:
            if w in haystack:
                score += 1
        if score > 0:
            scored.append((score, it))

    scored.sort(key=lambda t: t[0], reverse=True)
    return [it for _, it in scored[:MAX_RSS_RESULTS]]


# ---------------------------------------------------------------------------
# Result-Formatierung
# ---------------------------------------------------------------------------
_NBER_DISCLAIMER = (
    "Working Paper — NICHT peer-reviewed. "
    "NBER-interne Diskussion + Author-Network-Review, formales "
    "Journal-Review erfolgt typischerweise 1–2 Jahre später."
)


def _format_paper(p: dict, *, source_tag: str) -> dict:
    """Bringe Paper-Dict ins Evidora-Result-Schema."""
    title = p.get("title", "")
    paper_id = p.get("paper_id", "")
    year = p.get("year") or "—"
    authors = p.get("authors") or "—"
    abstract = p.get("abstract") or ""
    url = p.get("url", "")
    pub_date = p.get("pub_date", "")

    # display_value: Author/Date/Title/ID + Abstract-Preview
    abstract_short = abstract[:280] + "…" if len(abstract) > 280 else abstract
    id_bit = f"NBER {paper_id}" if paper_id else "NBER WP"
    display_value = (
        f"{authors} ({year}). '{title}'. "
        f"{id_bit}. Abstract: {abstract_short}"
    )[:500]

    # indicator_name: kompakt, mit ID + Year
    indicator_name = (
        f"{id_bit} ({year}): {title}"
        if year != "—"
        else f"{id_bit}: {title}"
    )[:300]

    pdf_url = ""
    if paper_id:
        pdf_url = f"{NBER_BASE}/papers/{paper_id}.pdf"

    return {
        "indicator_name": indicator_name,
        "indicator": "nber_working_paper",
        "country": "USA",  # NBER-Forschung, primär US-Fokus
        "year": year,
        "topic": "economic_research",
        "display_value": display_value,
        "description": f"{_NBER_DISCLAIMER} [Quelle: {source_tag}]",
        "url": url or f"{NBER_BASE}/papers/{paper_id}" if paper_id else NBER_BASE,
        "secondary_url": pdf_url,
        "source": (
            "NBER Working Papers (National Bureau of Economic Research, "
            "Cambridge MA, USA — Metadata frei, Volltexte teils paywall)"
        ),
    }


# ---------------------------------------------------------------------------
# Public Search-Entry
# ---------------------------------------------------------------------------
async def search_nber(analysis: dict) -> dict:
    """Live + Cache Hybrid-Lookup gegen NBER (RSS + JSON-Search).

    Pfad:
      1. RSS-Cache (letzte ~30 frische Papers, 24h-Cache) lokal nach
         Entity-/Query-Overlap filtern.
      2. Wenn weniger als 2 RSS-Treffer ODER offene Slots, zusätzlich
         JSON-Search-API mit der besten verfügbaren Query anstoßen.
      3. De-duplizieren über paper_id, auf MAX_TOTAL_RESULTS cappen.

    Returns leeres Result, wenn weder RSS noch Search etwas Brauchbares
    liefern (Synthesizer fällt dann auf andere Quellen zurück).

    Disclaimer: Alle Treffer sind WORKING PAPERS — NICHT peer-reviewed
    (description-Feld trägt entsprechenden Hinweis).
    """
    empty = {"source": "NBER", "type": "economic_research", "results": []}

    analysis = analysis or {}
    claim = (
        analysis.get("original_claim")
        or analysis.get("claim")
        or analysis.get("original")
        or ""
    )
    if not isinstance(claim, str):
        claim = str(claim or "")
    entities = analysis.get("entities") or []
    if not isinstance(entities, list):
        entities = []
    # Wir nutzen factcheck_queries primär (econ-spezifischer als
    # pubmed_queries); pubmed_queries als Fallback weil manche econ-
    # health-cross-over Claims dort landen.
    fc_queries = analysis.get("factcheck_queries") or []
    pm_queries = analysis.get("pubmed_queries") or []
    queries: list[str] = []
    if isinstance(fc_queries, list):
        queries.extend(q for q in fc_queries if isinstance(q, str) and q.strip())
    if isinstance(pm_queries, list):
        queries.extend(q for q in pm_queries if isinstance(q, str) and q.strip())

    # ---- Pfad 1: RSS-Cache filtern ----
    rss_items = await fetch_nber_rss()
    rss_ranked = _rank_rss_items(rss_items, entities, queries)

    # ---- Pfad 2: Live-Search bei zu wenigen RSS-Treffern ----
    search_items: list[dict] = []
    remaining = MAX_TOTAL_RESULTS - len(rss_ranked)
    if remaining > 0 and queries:
        # Wähle die längste nicht-leere Query (mehr Information für die
        # Full-Text-Suche), fallback auf erste Query.
        best_q = max(queries[:3], key=lambda q: len(q.strip()), default="")
        if best_q.strip():
            async with polite_client(timeout=TIMEOUT_S) as client:
                search_items = await _search_nber_api(client, best_q)

    # ---- Merge + De-dup ----
    seen_ids: set[str] = set()
    seen_urls: set[str] = set()
    combined: list[dict] = []

    def _key(p: dict) -> tuple[str, str]:
        return (p.get("paper_id") or "").lower(), (p.get("url") or "").lower()

    # RSS zuerst — die sind frisch (≤ 30 Tage)
    for p in rss_ranked:
        pid, url = _key(p)
        if pid and pid in seen_ids:
            continue
        if url and url in seen_urls:
            continue
        if pid:
            seen_ids.add(pid)
        if url:
            seen_urls.add(url)
        combined.append((p, "RSS letzte 30 Tage"))
        if len(combined) >= MAX_TOTAL_RESULTS:
            break

    # Dann Live-Search-Ergänzungen
    for p in search_items:
        if len(combined) >= MAX_TOTAL_RESULTS:
            break
        pid, url = _key(p)
        if pid and pid in seen_ids:
            continue
        if url and url in seen_urls:
            continue
        if pid:
            seen_ids.add(pid)
        if url:
            seen_urls.add(url)
        combined.append((p, "Live-Search"))

    if not combined:
        logger.info(
            f"NBER: 0/{len(rss_items)} RSS, 0/{len(search_items)} Search "
            f"für Claim '{claim[:60]}'"
        )
        return empty

    results = [_format_paper(p, source_tag=tag) for p, tag in combined]
    logger.info(
        f"NBER: {len(results)} Working-Paper-Treffer "
        f"({len(rss_ranked)} RSS + {len(search_items)} Search-Candidates)"
    )
    return {
        "source": "NBER",
        "type": "economic_research",
        "results": results,
    }
