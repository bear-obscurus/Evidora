"""arXiv Live-Connector — Preprint-Search via arXiv Query API.

arXiv (https://arxiv.org) ist der globale Preprint-Server (~2 Mio Papers
seit 1991), gehostet von der Cornell University. Kategorien: physics,
math, cs, q-bio, stat, econ, eess.

Für Faktencheck-Zwecke liefert der Connector:
- Direkte arXiv-ID-Resolution → autoritative Metadata bei Claims, die
  eine arXiv-ID explizit nennen (z. B. "2401.12345").
- Preprint-Suche bei Claims mit STEM-Research-Charakter, wenn
  peer-reviewed Papers (PubMed/Crossref/OpenAlex) noch nicht
  existieren — bleeding-edge AI/Klima/Physik-Forschung erscheint hier
  oft 6-12 Monate vor formalem Journal-Review.

Komplementär zu existierenden Quellen:
- PubMed/Cochrane: peer-reviewed biomed. Forschung
- Crossref/OpenAlex: peer-reviewed Paper-Metadata (DOI-Registry)
- bioRxiv/medRxiv: Lebenswissenschafts-Preprints
- ARXIV: STEM-/AI-/Klima-Preprints (NICHT peer-reviewed)

API: http://export.arxiv.org/api/query
- search_query=ti:"…"+AND+cat:cs.AI (Title + Kategorie)
- search_query=all:"…" (Volltext-Suche)
- id_list=2401.12345,2403.99999 (direkte ID-Auflösung)
- start, max_results, sortBy=submittedDate, sortOrder=descending

Format: Atom XML (NICHT JSON) — Parsing via xml.etree.ElementTree.

Free, kein API-Key. Polite User-Agent stark empfohlen. Rate-Limit:
1 Query / 3 s; Bursts können 503 auslösen.

Trigger: Claim hat arXiv-ID ODER (Paper-/Preprint-Keywords UND
analysis.pubmed_queries non-empty).

Wiring: main.py imports + tasks.append. NICHT in
AUTHORITATIVE_INDICATORS (Live-Quelle, Preprint, NICHT peer-reviewed).

Limitationen:
- Preprints sind NICHT peer-reviewed — Synthesizer muss das ausweisen.
- arXiv-Coverage primär Physik/Math/CS/Biology — kaum
  Geistes-/Sozialwissenschaft.
- ~6-12 Monate Vorsprung gegenüber peer-reviewed Journals üblich.
- Manchmal werden Preprints später zurückgezogen oder substantiell
  überarbeitet.
- Atom-XML statt JSON — Parsing erfordert mehr Code.
"""

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

ARXIV_API = "http://export.arxiv.org/api/query"

# arXiv-ID-Regex, post-2007 Format: YYMM.NNNNN (4 oder 5 Ziffern), opt. v-Suffix
_ARXIV_ID_NEW_REGEX = re.compile(
    r"\b(\d{4}\.\d{4,5})(?:v\d+)?\b"
)
# arXiv-ID-Regex, pre-2007 Format: archive/NNNNNNN (z. B. "hep-th/9501001")
_ARXIV_ID_OLD_REGEX = re.compile(
    r"\b([a-z][a-z\-]+/\d{7})\b",
    re.IGNORECASE,
)

# Trigger-Keywords (DE + EN) — Preprint/STEM-Research-Indikatoren
_PREPRINT_KEYWORDS = (
    "arxiv", "preprint", "preprints", "working paper", "working-paper",
    "vorabdruck", "vorab-druck",
    "stem research", "stem-research", "stem-forschung",
    "ai research", "ki-forschung", "ki research", "ai-research",
    "machine learning", "deep learning", "neural network",
    "maschinelles lernen", "deep-learning", "neural-network",
    "physik-paper", "physics paper", "math paper",
    "computer science", "computerwissenschaft", "informatik-paper",
    "neueste forschung", "bleeding edge", "cutting-edge research",
    "neue studie", "neue arbeit", "frische forschung",
)

# XML-Namespaces — arXiv Atom-Feed verwendet Standard-Atom + arxiv-Schema
_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
}

MAX_IDS = 3
MAX_SEARCH_RESULTS = 3
TIMEOUT_S = 15.0
RATE_LIMIT_SLEEP_S = 3.0


def _extract_arxiv_ids(claim: str) -> list[str]:
    """Extrahiere arXiv-IDs aus Claim-Text.

    Erkennt sowohl post-2007 (`2401.12345`) als auch pre-2007
    (`hep-th/9501001`) Formate. De-dupliziert und limitiert auf MAX_IDS.
    """
    if not claim:
        return []
    out: list[str] = []
    seen: set[str] = set()

    # Post-2007 zuerst (häufiger)
    for m in _ARXIV_ID_NEW_REGEX.findall(claim):
        key = m.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(m)
        if len(out) >= MAX_IDS:
            return out

    # Pre-2007
    for m in _ARXIV_ID_OLD_REGEX.findall(claim):
        key = m.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(m)
        if len(out) >= MAX_IDS:
            return out

    return out


def _claim_mentions_preprint_research(claim: str, analysis: dict) -> bool:
    """Trigger-Pre-Check: Claim erwähnt Preprint-/STEM-Research UND
    Analysis hat brauchbare Such-Queries.

    Wird nur ausgewertet, wenn KEINE arXiv-ID im Claim gefunden wurde.
    """
    if not claim:
        return False
    text = claim.lower()
    has_keyword = any(k in text for k in _PREPRINT_KEYWORDS)
    if not has_keyword:
        return False
    pm_queries = (analysis or {}).get("pubmed_queries") or []
    fc_queries = (analysis or {}).get("factcheck_queries") or []
    return bool(pm_queries) or bool(fc_queries)


def _strip_namespace(tag: str) -> str:
    """Entferne Namespace-Prefix aus XML-Tag-Name."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _normalize_text(s: str | None) -> str:
    """Normalisiere XML-Text: collapse whitespace, strip."""
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def _format_authors(authors: list[str]) -> str:
    """Formatiere Autorenliste — bis 5 Autoren, dann 'et al.'."""
    if not authors:
        return "Unbekannt"
    if len(authors) <= 5:
        return " / ".join(authors)
    return f"{' / '.join(authors[:5])} et al."


def _extract_short_id(arxiv_id_raw: str) -> str:
    """Extrahiere die Kurz-ID (ohne URL-Prefix, ohne Version-Suffix).

    Beispiele:
        "http://arxiv.org/abs/2401.12345v2" → "2401.12345"
        "2401.12345v1" → "2401.12345"
        "hep-th/9501001" → "hep-th/9501001"
    """
    if not arxiv_id_raw:
        return ""
    raw = arxiv_id_raw.strip()
    # Strip URL-Prefix
    for prefix in (
        "http://arxiv.org/abs/", "https://arxiv.org/abs/",
        "http://arxiv.org/pdf/", "https://arxiv.org/pdf/",
    ):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
            break
    # Strip Version-Suffix (v1, v2, …)
    raw = re.sub(r"v\d+$", "", raw)
    # Strip eventuell trailing ".pdf"
    if raw.endswith(".pdf"):
        raw = raw[:-4]
    return raw


def _format_entry(entry: ET.Element) -> dict | None:
    """Formatiere einen Atom-<entry>-Knoten zum Evidora-Result-Schema.

    Skips Withdrawn-Papers und Entries ohne brauchbare Felder.
    """
    if entry is None:
        return None

    # Title
    title_el = entry.find("atom:title", _NS)
    title = _normalize_text(title_el.text if title_el is not None else "")
    if not title:
        return None

    # ID (URL) → Short-ID
    id_el = entry.find("atom:id", _NS)
    id_url = _normalize_text(id_el.text if id_el is not None else "")
    short_id = _extract_short_id(id_url)
    if not short_id:
        return None

    # Published
    published_el = entry.find("atom:published", _NS)
    published = _normalize_text(
        published_el.text if published_el is not None else ""
    )
    year = published[:4] if len(published) >= 4 and published[:4].isdigit() else "—"
    pub_date_short = published[:10] if len(published) >= 10 else year

    # Summary (Abstract)
    summary_el = entry.find("atom:summary", _NS)
    summary = _normalize_text(
        summary_el.text if summary_el is not None else ""
    )

    # Authors
    authors: list[str] = []
    for author_el in entry.findall("atom:author", _NS):
        name_el = author_el.find("atom:name", _NS)
        name = _normalize_text(name_el.text if name_el is not None else "")
        if name:
            authors.append(name)
    authors_str = _format_authors(authors)

    # Primary Category (arxiv-Schema)
    primary_cat = ""
    primary_cat_el = entry.find("arxiv:primary_category", _NS)
    if primary_cat_el is not None:
        primary_cat = primary_cat_el.attrib.get("term", "") or ""
    if not primary_cat:
        # Fallback: erste atom:category
        cat_el = entry.find("atom:category", _NS)
        if cat_el is not None:
            primary_cat = cat_el.attrib.get("term", "") or ""
    primary_cat = primary_cat.strip()

    # Withdrawn-Check via arxiv:comment
    comment_el = entry.find("arxiv:comment", _NS)
    if comment_el is not None:
        comment_text = (comment_el.text or "").lower()
        if "withdrawn" in comment_text:
            logger.debug(
                f"arXiv: skip withdrawn paper {short_id}"
            )
            return None

    # DOI (arxiv:doi) — optional
    doi_el = entry.find("arxiv:doi", _NS)
    doi = _normalize_text(doi_el.text if doi_el is not None else "")

    # Month-Tag für indicator_name (Monat in englischem Kurzformat)
    month_short = ""
    if len(published) >= 7 and published[4] == "-" and published[5:7].isdigit():
        month_num = int(published[5:7])
        month_names = (
            "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        )
        if 1 <= month_num <= 12:
            month_short = month_names[month_num]

    # indicator_name: "arXiv 2401.12345 (cs.AI, Jan 2024): Title"
    cat_year_bits = []
    if primary_cat:
        cat_year_bits.append(primary_cat)
    if month_short and year != "—":
        cat_year_bits.append(f"{month_short} {year}")
    elif year != "—":
        cat_year_bits.append(year)
    cat_year = ", ".join(cat_year_bits) if cat_year_bits else ""
    if cat_year:
        indicator_name = f"arXiv {short_id} ({cat_year}): {title}"
    else:
        indicator_name = f"arXiv {short_id}: {title}"
    indicator_name = indicator_name[:300]

    # display_value: Author/Date/Title/ID + Abstract-Preview (max 280 chars)
    abstract_short = (
        summary[:280] + "…" if len(summary) > 280 else summary
    )
    cat_bracket = f" [{primary_cat}]" if primary_cat else ""
    display_value = (
        f"{authors_str} ({pub_date_short}). '{title}'. "
        f"arXiv:{short_id}{cat_bracket}. "
        f"Abstract: {abstract_short}"
    )[:500]

    description = (
        "Preprint — NICHT peer-reviewed. "
        "arXiv-Self-Submitted-Forschung mit Moderation aber "
        "ohne formelles Review."
    )

    abs_url = f"http://arxiv.org/abs/{short_id}"
    pdf_url = f"http://arxiv.org/pdf/{short_id}.pdf"

    out = {
        "indicator_name": indicator_name,
        "indicator": "arxiv_preprint",
        "country": "—",
        "year": year,
        "topic": "arxiv_preprint",
        "display_value": display_value,
        "description": description,
        "url": abs_url,
        "secondary_url": pdf_url,
        "source": "arXiv (frei, Cornell-University-Hosting)",
    }
    if doi:
        # Optional: DOI-Hinweis im display_value (peer-reviewed Version
        # kann existieren, wenn DOI gesetzt ist)
        out["display_value"] = (
            out["display_value"][:480] + f" DOI: {doi}"
        )[:500]
    return out


def _parse_arxiv_atom(xml_text: str) -> list[dict]:
    """Parse arXiv-Atom-Feed → Liste formatierter Result-Dicts.

    Robust gegen XML-Parse-Fehler und fehlende Felder.
    """
    if not xml_text or not xml_text.strip():
        return []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.debug(f"arXiv: XML-Parse-Error: {e}")
        return []

    # Stelle sicher, dass wir den Atom-Feed-Root haben
    if _strip_namespace(root.tag) != "feed":
        logger.debug(
            f"arXiv: unexpected root tag {root.tag!r}, expected 'feed'"
        )
        return []

    results: list[dict] = []
    for entry in root.findall("atom:entry", _NS):
        try:
            r = _format_entry(entry)
        except Exception as e:
            logger.debug(f"arXiv: entry-format-error: {e}")
            continue
        if r:
            results.append(r)
    return results


async def _fetch_by_ids(client, ids: list[str]) -> list[dict]:
    """Direkte ID-Auflösung via ?id_list=… (komma-separiert)."""
    if not ids:
        return []
    id_list_param = ",".join(ids)
    url = (
        f"{ARXIV_API}?id_list={quote_plus(id_list_param)}"
        f"&max_results={MAX_IDS}"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"arXiv id_list HTTP {resp.status_code} "
                f"for {id_list_param[:60]}"
            )
            return []
        return _parse_arxiv_atom(resp.text)
    except Exception as e:
        logger.debug(
            f"arXiv id_list fetch failed for '{id_list_param[:40]}': {e}"
        )
        return []


async def _search_query(client, query: str) -> list[dict]:
    """Suche arXiv via ?search_query=all:"…"&sortBy=submittedDate&...

    Returns Liste formatierter Result-Dicts (bis MAX_SEARCH_RESULTS).
    Sortiert nach Einreichungsdatum absteigend (neueste zuerst).
    """
    if not query or len(query.strip()) < 3:
        return []
    # arXiv-Search: all:"<phrase>" — Phrase-Quoting via "+" und URL-Encode.
    # quote_plus encodet Leerzeichen als "+" — passt zur Beispiel-Doku.
    q_phrase = f'all:"{query.strip()}"'
    url = (
        f"{ARXIV_API}"
        f"?search_query={quote_plus(q_phrase)}"
        f"&start=0&max_results={MAX_SEARCH_RESULTS}"
        f"&sortBy=submittedDate&sortOrder=descending"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"arXiv search HTTP {resp.status_code} for '{query[:40]}'"
            )
            return []
        return _parse_arxiv_atom(resp.text)
    except Exception as e:
        logger.debug(f"arXiv search failed for '{query[:30]}': {e}")
        return []


async def search_arxiv(analysis: dict) -> dict:
    """Live-Lookup gegen arXiv Query API für Preprint-Resolution + Suche.

    Returns Dict mit ≤3 arXiv-Treffern. ID-direkt-Resolution wird
    bevorzugt; falls keine arXiv-ID im Claim, fällt der Service auf
    Search-Modus zurück (nur wenn Preprint-/STEM-Keywords + non-empty
    Queries vorhanden).

    Disclaimer: Alle Treffer sind PREPRINTS — NICHT peer-reviewed.
    Synthesizer muss das in der Ergebnis-Darstellung berücksichtigen
    (description-Feld trägt einen entsprechenden Hinweis).
    """
    empty = {"source": "arXiv", "type": "preprint_metadata", "results": []}

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original") or ""
    if not isinstance(claim, str):
        claim = str(claim or "")

    ids = _extract_arxiv_ids(claim)
    use_search = (not ids) and _claim_mentions_preprint_research(
        claim, analysis
    )

    if not ids and not use_search:
        return empty

    results: list[dict] = []
    seen_urls: set[str] = set()

    async with polite_client(timeout=TIMEOUT_S) as client:
        if ids:
            id_results = await _fetch_by_ids(client, ids)
            for r in id_results:
                key = (r.get("url") or "").lower()
                if key in seen_urls:
                    continue
                seen_urls.add(key)
                results.append(r)
                if len(results) >= MAX_IDS:
                    break
        elif use_search:
            # Wähle bestmögliche Query (pubmed_queries bevorzugt für
            # STEM-Topics; Fallback factcheck_queries).
            pm = analysis.get("pubmed_queries") or []
            fc = analysis.get("factcheck_queries") or []
            query = ""
            if pm and isinstance(pm, list) and pm[0]:
                query = str(pm[0])
            elif fc and isinstance(fc, list) and fc[0]:
                query = str(fc[0])
            if query:
                # Rate-Limit-Respect: arXiv empfiehlt 1 Query / 3 s.
                # Bei einer einzelnen Query reicht ein vorgelagerter
                # Sleep nicht — wir machen ohnehin nur einen Call, also
                # keine Notwendigkeit zu warten. Den Sleep gibt es nur,
                # wenn ein Service mehrere arXiv-Calls in Serie macht.
                # Hier bleibt es bei einem Search-Call.
                search_results = await _search_query(client, query)
                for r in search_results:
                    key = (r.get("url") or "").lower()
                    if key in seen_urls:
                        continue
                    seen_urls.add(key)
                    results.append(r)
                    if len(results) >= MAX_SEARCH_RESULTS:
                        break

    if not results:
        if ids:
            logger.info(
                f"arXiv: 0 Treffer für IDs {[i[:40] for i in ids[:3]]}"
            )
        else:
            logger.info("arXiv: 0 Treffer für Preprint-Search")
        return empty

    logger.info(f"arXiv: {len(results)} Preprint-Treffer geliefert")
    return {
        "source": "arXiv",
        "type": "preprint_metadata",
        "results": results,
    }
