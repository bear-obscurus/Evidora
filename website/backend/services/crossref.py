"""Crossref Live-Connector — DOI-Resolution & Paper-Search via REST API.

Crossref ist die größte DOI-Registry weltweit (~150 Mio Records) und
liefert für Faktencheck-Zwecke:
- Direkte DOI-Resolution → autoritative Metadata (Titel, Autoren, Journal,
  Jahr, Citation-Count) bei Claims, die eine DOI explizit nennen.
- Paper-Suche über generische Queries → ergänzt OpenAlex bei
  DOI-Resolution (schneller) bzw. wenn OpenAlex-Index kein Match findet.

Komplementär zu existierenden Quellen:
- PubMed/Cochrane: peer-reviewed biomed. Forschung (Volltext-Abstracts)
- OpenAlex: alternative Paper-Metadata-DB (Author-Disambiguation,
  Citation-Graph)
- CROSSREF: DOI-first, Verlag-registriert, sehr robust für direkte
  DOI-Resolution

API: https://api.crossref.org/works/{doi}  (DOI-Lookup)
   + https://api.crossref.org/works?query={text}&rows=5  (Suche)

Free, kein API-Key nötig. Polite-Pool via mailto-Parameter empfohlen.

Trigger: Claim enthält eine DOI (Regex `\\b10\\.\\d{4,9}/[^\\s]+\\b`)
ODER Claim hat Paper-Keywords (Studie, Paper, Forschung, ...) UND
analysis.factcheck_queries oder analysis.pubmed_queries non-empty.

Wiring: main.py imports + tasks.append. NICHT in
AUTHORITATIVE_INDICATORS (ist Live-Quelle, kein kuratierter
Konsens-Pack).

Limitationen:
- Crossref hat ~150 Mio Records — sehr robust für DOI-Resolution.
- Search-Quality variiert (manchmal off-topic-Treffer für sehr
  generische Queries).
- Citation-Count ist manchmal 0 für sehr junge Papers (Indexer-Lag).
- Abstract-Coverage liegt nur bei ca. 30 % (Lizenz-Embargo der Verlage).
- Manche Verlage publizieren OA-Papers, registrieren aber keinen
  Abstract bei Crossref.
"""

import asyncio
import logging
import re
from html import unescape
from urllib.parse import quote, quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

CROSSREF_DOI_API = "https://api.crossref.org/works/{doi}"
CROSSREF_SEARCH_API = (
    "https://api.crossref.org/works"
    "?query={query}&rows=5&mailto=contact@evidora.eu"
)
# DOI-Regex: 10. + 4-9 Ziffern + / + 1+ Non-whitespace.
# Trailing-Punctuation (. , ) ;) wird unten gestrippt, da DOI selten
# direkt mit Satzzeichen endet.
DOI_REGEX = re.compile(r"\b10\.\d{4,9}/[^\s]+\b", re.IGNORECASE)

# Paper-Keywords (DE + EN) — Trigger für Search-Modus, falls keine DOI.
_PAPER_KEYWORDS = (
    "studie", "studien", "paper", "papers", "forschung", "journal",
    "peer-reviewed", "peer reviewed", "peerreview", "publikation",
    "veröffentlichung", "fachzeitschrift",
    "study", "research", "scholarly", "publication", "preprint",
    "review article", "meta-analysis", "metaanalyse", "metaanalysis",
    "systematic review", "systematische übersichtsarbeit",
)

MAX_DOIS = 3
MAX_SEARCH_RESULTS = 3
TIMEOUT_S = 15.0


def _extract_dois(claim: str) -> list[str]:
    """Extrahiere DOIs aus Claim-Text.

    Strippt typische Trailing-Punctuation (.,;:)]}>) und de-dupliziert.
    Limitiert auf MAX_DOIS Treffer.
    """
    if not claim:
        return []
    raw = DOI_REGEX.findall(claim)
    out: list[str] = []
    seen: set[str] = set()
    for doi in raw:
        # Trailing-Punctuation entfernen (typisch: "...10.1234/abc.")
        cleaned = doi.rstrip(".,;:)]}>'\"")
        if not cleaned or cleaned.lower() in seen:
            continue
        seen.add(cleaned.lower())
        out.append(cleaned)
        if len(out) >= MAX_DOIS:
            break
    return out


def _claim_mentions_paper(claim: str, analysis: dict) -> bool:
    """Trigger-Pre-Check: Claim erwähnt Paper/Studie/Forschung UND
    Analysis hat brauchbare Such-Queries.

    Wird nur ausgewertet, wenn KEINE DOI im Claim gefunden wurde.
    """
    if not claim:
        return False
    text = claim.lower()
    has_keyword = any(k in text for k in _PAPER_KEYWORDS)
    if not has_keyword:
        return False
    fc_queries = (analysis or {}).get("factcheck_queries") or []
    pm_queries = (analysis or {}).get("pubmed_queries") or []
    return bool(fc_queries) or bool(pm_queries)


def _format_authors(authors: list[dict]) -> str:
    """Formatiere Autorenliste — 'Smith et al.' bei 3+ Autoren."""
    if not authors:
        return ""
    names: list[str] = []
    for a in authors[:3]:
        family = (a or {}).get("family") or ""
        given = (a or {}).get("given") or ""
        if family and given:
            names.append(f"{family}, {given[0]}.")
        elif family:
            names.append(family)
    if not names:
        return ""
    if len(authors) > 3:
        return f"{names[0]} et al."
    return " / ".join(names)


def _extract_year(item: dict) -> str:
    """Crossref-Datum: issued.date-parts[0][0] oder published.date-parts."""
    for key in ("issued", "published-print", "published-online", "created"):
        block = item.get(key) or {}
        parts = block.get("date-parts") or []
        if parts and isinstance(parts, list) and parts[0]:
            year = parts[0][0]
            if year:
                return str(year)
    return "—"


def _extract_journal(item: dict) -> str:
    """Container-Title (Journal-Name) — Crossref-Liste, erstes Element."""
    titles = item.get("container-title") or []
    if titles and isinstance(titles, list):
        return titles[0] or ""
    return ""


def _clean_abstract(abstract: str) -> str:
    """Entferne JATS-XML-Tags + HTML-Entities aus Crossref-Abstract."""
    if not abstract:
        return ""
    # Strip <jats:p>, <p>, <i>, etc.
    cleaned = re.sub(r"<[^>]+>", "", abstract)
    cleaned = unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _format_result(item: dict) -> dict | None:
    """Formatiere ein Crossref-Item zum Evidora-Result-Schema.

    Skips Items ohne Titel oder ohne DOI.
    """
    if not item or not isinstance(item, dict):
        return None
    titles = item.get("title") or []
    title = titles[0] if titles and isinstance(titles, list) else ""
    title = title.strip() if title else ""
    doi = (item.get("DOI") or "").strip()
    if not title or not doi:
        return None

    authors_list = item.get("author") or []
    authors_str = _format_authors(authors_list)
    year = _extract_year(item)
    journal = _extract_journal(item)
    volume = item.get("volume") or ""
    issue = item.get("issue") or ""
    pages = item.get("page") or ""
    cited_by = item.get("is-referenced-by-count")
    if cited_by is None:
        cited_by_str = "n/a"
    else:
        cited_by_str = str(cited_by)

    # indicator_name: "Author et al. (2023): Paper Title (Journal, vol/issue)"
    name_author = authors_str or "Unbekannt"
    name_loc = ""
    if journal:
        loc_bits = [journal]
        if volume:
            loc_bits.append(f"vol. {volume}")
        if issue:
            loc_bits.append(f"issue {issue}")
        name_loc = f" ({', '.join(loc_bits)})"
    indicator_name = (
        f"{name_author} ({year}): {title}{name_loc}"
    )[:300]

    # display_value: kompakte Zitations-Zeile
    disp_loc = ""
    if journal:
        disp_loc = f" {journal}"
        if volume:
            disp_loc += f", {volume}"
            if issue:
                disp_loc += f"({issue})"
        if pages:
            disp_loc += f":{pages}"
        disp_loc += "."
    display_value = (
        f"{name_author} ({year}). '{title}'.{disp_loc} "
        f"DOI: {doi}. Cited by {cited_by_str} times."
    )[:500]

    # description: Abstract-Preview (max 280 chars) oder Fallback
    abstract_clean = _clean_abstract(item.get("abstract") or "")
    if abstract_clean:
        description = (
            abstract_clean[:280] + "…"
            if len(abstract_clean) > 280 else abstract_clean
        )
    else:
        description = "No abstract available"

    return {
        "indicator_name": indicator_name,
        "indicator": "crossref_paper",
        "country": "—",
        "year": year,
        "topic": "crossref_metadata",
        "display_value": display_value,
        "description": description,
        "url": f"https://doi.org/{doi}",
        "secondary_url": f"https://api.crossref.org/works/{doi}",
        "source": "Crossref REST API (frei, mailto-polite-pool)",
    }


async def _fetch_doi(client, doi: str) -> dict | None:
    """Direkte DOI-Resolution via /works/{doi}.

    Returns formatiertes Result-Dict ODER None.
    """
    # Crossref erwartet den DOI als Path-Segment mit erhaltenem '/' —
    # nur die übrigen reservierten Chars werden percent-encoded.
    url = CROSSREF_DOI_API.format(doi=quote(doi, safe="/"))
    # Polite-Pool: mailto als Query-Parameter.
    url = f"{url}?mailto=contact@evidora.eu"
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code == 404:
            logger.debug(f"Crossref DOI not found: {doi}")
            return None
        if resp.status_code != 200:
            logger.debug(
                f"Crossref DOI fetch HTTP {resp.status_code} for {doi}"
            )
            return None
        data = resp.json()
        if data.get("status") != "ok":
            return None
        item = data.get("message") or {}
        return _format_result(item)
    except Exception as e:
        logger.debug(f"Crossref DOI fetch failed for '{doi}': {e}")
        return None


async def _search_query(client, query: str) -> list[dict]:
    """Suche Crossref via /works?query={text}.

    Returns Liste formatierter Result-Dicts (bis zu MAX_SEARCH_RESULTS).
    """
    if not query or len(query.strip()) < 3:
        return []
    url = CROSSREF_SEARCH_API.format(query=quote_plus(query.strip()))
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"Crossref search HTTP {resp.status_code} for "
                f"'{query[:40]}'"
            )
            return []
        data = resp.json()
        if data.get("status") != "ok":
            return []
        items = (data.get("message") or {}).get("items") or []
        out: list[dict] = []
        for item in items[:MAX_SEARCH_RESULTS]:
            r = _format_result(item)
            if r:
                out.append(r)
        return out
    except Exception as e:
        logger.debug(f"Crossref search failed for '{query[:30]}': {e}")
        return []


async def search_crossref(analysis: dict) -> dict:
    """Live-Lookup gegen Crossref REST API für DOI-Resolution + Paper-Suche.

    Returns Dict mit ≤3 Crossref-Treffern. DOI-direkt-Resolution wird
    bevorzugt; falls keine DOI im Claim, fällt der Service auf
    Search-Modus zurück (nur wenn Paper-Keywords + non-empty Queries
    vorhanden).
    """
    empty = {"source": "Crossref", "type": "scholarly_metadata", "results": []}

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original") or ""
    if not isinstance(claim, str):
        claim = str(claim or "")

    dois = _extract_dois(claim)
    use_search = (not dois) and _claim_mentions_paper(claim, analysis)

    if not dois and not use_search:
        return empty

    results: list[dict] = []
    seen_dois: set[str] = set()

    async with polite_client(timeout=TIMEOUT_S) as client:
        if dois:
            tasks = [_fetch_doi(client, d) for d in dois]
            doi_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in doi_results:
                if isinstance(r, Exception) or not r:
                    continue
                doi_key = (r.get("url") or "").lower()
                if doi_key in seen_dois:
                    continue
                seen_dois.add(doi_key)
                results.append(r)
        elif use_search:
            # Wähle bestmögliche Query (factcheck_queries bevorzugt;
            # Fallback pubmed_queries). Erste Query reicht — Crossref-
            # Search liefert sowieso bereits Top-N-Matches.
            fc = analysis.get("factcheck_queries") or []
            pm = analysis.get("pubmed_queries") or []
            query = ""
            if fc and isinstance(fc, list) and fc[0]:
                query = str(fc[0])
            elif pm and isinstance(pm, list) and pm[0]:
                query = str(pm[0])
            if query:
                search_results = await _search_query(client, query)
                for r in search_results:
                    doi_key = (r.get("url") or "").lower()
                    if doi_key in seen_dois:
                        continue
                    seen_dois.add(doi_key)
                    results.append(r)
                    if len(results) >= MAX_SEARCH_RESULTS:
                        break

    if not results:
        if dois:
            logger.info(
                f"Crossref: 0 Treffer für DOIs "
                f"{[d[:40] for d in dois[:3]]}"
            )
        else:
            logger.info("Crossref: 0 Treffer für Paper-Search")
        return empty

    logger.info(f"Crossref: {len(results)} Treffer geliefert")
    return {
        "source": "Crossref",
        "type": "scholarly_metadata",
        "results": results,
    }
