"""DOAB Live-Connector — Directory of Open Access Books.

DOAB (https://directory.doabooks.org) ist das zentrale Verzeichnis fuer
peer-reviewed Open-Access-Buecher: ~70k Titel von 600+ Verlagen, mit
Schwerpunkt Geistes- und Sozialwissenschaften (Geschichte, Philosophie,
Recht, Soziologie, Politikwissenschaft, Bildungs- und Literatur-
wissenschaften). Komplementaer zu Crossref (Artikel-Ebene), arXiv/bioRxiv
(Preprints) und ERIC (US-Bildungsforschung) — DOAB liefert die Buch-Ebene.

API: https://directory.doabooks.org/rest/search  (GET, JSON, kein Auth).
Beispiel:
  ?expand=metadata&query=democracy+europe&limit=5
  ?expand=metadata&query=dc.title:climate&limit=5

Response-Schema: Top-Level-Liste von Items; jedes Item hat:
  * uuid, handle, name (Titel)
  * metadata (Liste von {key, value} mit DC- + OAPEN-Feldern)

Wichtige metadata.key-Werte:
  * dc.title                  — Titel
  * dc.contributor.author     — Autor (mehrfach moeglich)
  * dc.date.issued            — Erscheinungsjahr
  * dc.description.abstract   — Abstract
  * dc.subject / dc.subject.other / dc.subject.classification — Themen
  * dc.language               — Sprache
  * dc.identifier.uri         — DOAB-Permalink
  * oapen.identifier.doi      — DOI (falls vorhanden)
  * publisher.name            — Verlag
  * publisher.oalicense       — CC-Lizenz-Hinweis
  * oapen.pages               — Seitenzahl

Lizenz: Metadata CC0 (Public Domain); Buecher selbst unter CC-Varianten.

Politische Guardrails (siehe project_political_guardrails.md):
Geistes- und Sozialwissenschafts-Claims sind oft normativ aufgeladen.
Der Service liefert ausschliesslich Buch-Metadaten + Abstract — KEINE
eigene Bewertung, KEINE Parteinahme. Synthesizer entscheidet Verdict.

# WIRING fuer main.py:
# from services.doab import search_doab, claim_mentions_doab_cached
# if claim_mentions_doab_cached(claim):
#     tasks.append(cached("DOAB", search_doab, analysis))
#     queried_names.append("DOAB")
"""

from __future__ import annotations

import logging
import re
import time
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

DOAB_API = "https://directory.doabooks.org/rest/search"

MAX_RESULTS = 5
TIMEOUT_S = 15.0
CACHE_TTL_S = 24 * 60 * 60  # 24 h

# Module-Level Query-Cache: { query_key: (timestamp, results_list) }
_QUERY_CACHE: dict[str, tuple[float, list[dict]]] = {}

# Trigger-Resolve-Cache: { claim_lc: bool }
_TRIGGER_CACHE: dict[str, bool] = {}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Direkte Buch-/OA-Begriffe — Match-on-substring.
_DOAB_TERMS = (
    "doab", "oapen", "openedition",
    # Buch-Begriffe DE
    "open-access-buch", "open access buch", "oa-buch", "oa buch",
    "open-access-buecher", "open-access-bücher",
    "open-access-verlag", "open access verlag",
    "monographie", "monografie",
    "sammelband", "tagungsband",
    "fachbuch", "fachbuecher", "fachbücher",
    "forschungsbuch", "wissenschaftliches buch", "wissenschaftliches werk",
    "lehrbuch", "lehrbücher", "lehrbuecher",
    "habilitationsschrift", "dissertationsschrift",
    # Buch-Begriffe EN
    "open access book", "open-access book",
    "oa book", "oa monograph",
    "academic book", "scholarly book", "scientific book",
    "edited volume", "edited collection",
    "monograph", "textbook",
    # Phrase-Trigger
    "hat ein buch geschrieben", "hat ein buch veroeffentlicht",
    "hat ein buch veröffentlicht", "schrieb ein buch", "schreibt ein buch",
    "wrote a book", "published a book", "writes a book",
    "in seinem buch", "in ihrem buch", "in his book", "in her book",
    "buch ueber", "buch über", "buch zum thema",
    "book about", "book on",
)

# DOI-Pattern fuer direkte DOI-Resolution (falls Claim einen DOI enthaelt).
_DOI_RE = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)

# Geistes-/Sozialwiss-Kontext fuer Cross-Cluster-Erkennung.
_HUMSOC_CONTEXT = (
    "geschichte", "historisch", "history", "historical",
    "philosophie", "philosophy", "philosophisch",
    "soziologie", "sociology", "soziologisch",
    "politikwissenschaft", "political science",
    "rechtswissenschaft", "jurisprudenz",
    "literaturwissenschaft", "literary studies",
    "kulturwissenschaft", "cultural studies",
    "religionswissenschaft", "religious studies",
    "paedagogik", "pädagogik", "pedagogy",
    "ethik", "ethics", "ethisch",
    "anthropologie", "anthropology",
    "archaeologie", "archäologie", "archaeology",
)

# "Studie/Forschung/Werk" + Geistes-Kontext → DOAB triggern.
_RESEARCH_NOUNS = (
    "buch", "buecher", "bücher", "werk", "werke",
    "book", "books", "volume",
)


def _claim_mentions_doab(claim_lc: str) -> bool:
    """Pure Trigger-Funktion (lowercase claim erwartet)."""
    if not claim_lc:
        return False
    # Direkte Begriffe.
    if any(t in claim_lc for t in _DOAB_TERMS):
        return True
    # DOI-Pattern → potenziell Buch-DOI.
    if _DOI_RE.search(claim_lc):
        return True
    # "Buch/Werk" + Geistes-/Sozialwiss-Kontext (Cross-Cluster).
    if any(n in claim_lc for n in _RESEARCH_NOUNS):
        if any(c in claim_lc for c in _HUMSOC_CONTEXT):
            return True
    return False


def claim_mentions_doab_cached(claim: str) -> bool:
    """Cached Wrapper — Trigger-Resolve cached pro Claim-String."""
    if not claim:
        return False
    key = claim.lower()
    cached = _TRIGGER_CACHE.get(key)
    if cached is not None:
        return cached
    result = _claim_mentions_doab(key)
    # Schutz gegen unbounded Wachstum.
    if len(_TRIGGER_CACHE) > 2048:
        _TRIGGER_CACHE.clear()
    _TRIGGER_CACHE[key] = result
    return result


# ---------------------------------------------------------------------------
# Query-Builder
# ---------------------------------------------------------------------------
# Stop-Words die wir bei der Keyword-Extraktion entfernen.
_STOP_WORDS = frozenset((
    # DE Artikel/Pronomen/Hilfsverben
    "die", "der", "das", "den", "dem", "des",
    "ein", "eine", "einer", "eines", "einem", "einen",
    "und", "oder", "aber", "doch", "denn", "weil", "dass", "ob",
    "ist", "sind", "war", "waren", "wird", "werden", "wurde", "wurden",
    "hat", "haben", "habe", "hatte", "hatten",
    "kann", "koennen", "können", "konnte", "konnten",
    "muss", "muessen", "müssen", "musste", "mussten",
    "soll", "sollen", "sollte", "sollten",
    "darf", "duerfen", "dürfen", "durfte", "durften",
    "mit", "ohne", "fuer", "für", "auf", "an", "am", "von", "vom",
    "zu", "zur", "zum", "bei", "nach", "vor", "ueber", "über", "unter",
    "nicht", "kein", "keine", "keinen", "keiner",
    "auch", "noch", "nur", "schon", "sehr", "mehr", "weniger",
    "ja", "nein", "wie", "was", "wer", "wann", "wo", "warum", "wieso",
    "im", "in",
    # EN
    "the", "a", "an", "of", "on", "at", "for", "with", "without",
    "is", "are", "was", "were", "be", "been", "being",
    "has", "have", "had", "having",
    "do", "does", "did", "doing",
    "and", "or", "but", "if", "then", "than",
    "this", "that", "these", "those",
    # Buch-Begriffe selbst (zu generisch fuer Boolean)
    "buch", "buecher", "bücher", "werk", "werke",
    "book", "books", "volume", "monograph",
    "monographie", "monografie", "sammelband",
    "fachbuch", "lehrbuch",
    "studie", "studien", "study", "studies",
    "forschung", "research", "untersuchung",
    "geschrieben", "veroeffentlicht", "veröffentlicht",
    "wrote", "written", "published",
    "his", "her", "their", "seinem", "ihrem", "seinen", "ihren",
)
)


def _build_doab_query(claim: str, analysis: dict | None = None) -> str:
    """Baue eine DOAB-Suche aus Claim + optionalen Analysis-Queries.

    Strategie:
    1. Wenn analysis.factcheck_queries non-empty, die erste nehmen (clean).
    2. Sonst: Claim normalisieren, Stop-Words filtern,
       Top-3-5 Keywords als Plus-getrennte Query bauen (impliziert AND).

    DOAB nutzt Apache-Lucene-Syntax — Plus-getrennte Tokens funktionieren
    als implizites AND. Wir verzichten auf field-qualifizierte Queries
    (`keywords:` lieferte 0 Treffer im Live-Test), nutzen die default
    Multi-Field-Suche ueber alle DC-Felder.
    """
    analysis = analysis or {}

    # Bevorzuge bereits aufbereitete Such-Queries aus dem ClaimAnalyzer.
    fc_queries = analysis.get("factcheck_queries") or []
    if isinstance(fc_queries, list) and fc_queries:
        q = str(fc_queries[0] or "").strip()
        if len(q) >= 3:
            return q

    if not claim:
        return ""

    text = claim.lower()

    # DOI-Direkt-Resolution: wenn ein DOI im Claim ist, daraus die Query bauen.
    doi_match = _DOI_RE.search(text)
    if doi_match:
        return doi_match.group(0)

    # Tokenisierung: alphanumerisch + Bindestrich (min. 3 Zeichen).
    tokens = re.findall(r"[a-zA-ZäöüÄÖÜß][a-zA-ZäöüÄÖÜß\-]{2,}", text)

    # Filter Stop-Words + Dedup.
    keywords: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        tl = tok.lower()
        if tl in _STOP_WORDS:
            continue
        if tl in seen:
            continue
        seen.add(tl)
        keywords.append(tl)
        if len(keywords) >= 4:
            break

    if not keywords:
        return ""

    return " ".join(keywords)


# ---------------------------------------------------------------------------
# HTTP-Layer
# ---------------------------------------------------------------------------
def _cache_get(query: str) -> list[dict] | None:
    key = query.lower()
    entry = _QUERY_CACHE.get(key)
    if not entry:
        return None
    ts, data = entry
    if (time.time() - ts) > CACHE_TTL_S:
        _QUERY_CACHE.pop(key, None)
        return None
    return data


def _cache_put(query: str, data: list[dict]) -> None:
    key = query.lower()
    if len(_QUERY_CACHE) > 512:
        _QUERY_CACHE.clear()
    _QUERY_CACHE[key] = (time.time(), data)


async def _fetch_doab(query: str) -> list[dict]:
    """Hole rohe Items von DOAB API. Returns [] bei jedem Fehler."""
    if not query or len(query.strip()) < 3:
        return []

    cached = _cache_get(query)
    if cached is not None:
        logger.debug(f"DOAB: cache-hit fuer '{query[:40]}'")
        return cached

    url = (
        f"{DOAB_API}?expand=metadata"
        f"&query={quote_plus(query)}"
        f"&limit={MAX_RESULTS}"
    )

    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"DOAB HTTP {resp.status_code} fuer '{query[:40]}'"
            )
            return []
        try:
            payload = resp.json()
        except Exception as e:
            logger.debug(f"DOAB JSON-Parse-Error: {e}")
            return []
    except Exception as e:
        logger.debug(f"DOAB fetch failed fuer '{query[:40]}': {e}")
        return []

    # Top-Level ist eine Liste, kein Dict.
    if not isinstance(payload, list):
        return []

    _cache_put(query, payload)
    return payload


# ---------------------------------------------------------------------------
# Metadata-Helpers
# ---------------------------------------------------------------------------
_HTML_ENTITIES = {
    "&quot;": '"', "&amp;": "&", "&lt;": "<", "&gt;": ">",
    "&apos;": "'", "&#39;": "'", "&nbsp;": " ",
}


def _clean(s) -> str:
    """Normalisiere DOAB-Text: HTML-Entities, Whitespace, strip."""
    if s is None:
        return ""
    text = str(s)
    for ent, repl in _HTML_ENTITIES.items():
        text = text.replace(ent, repl)
    return re.sub(r"\s+", " ", text).strip()


def _md_get_first(metadata: list, key: str) -> str:
    """Hole den ersten Wert fuer einen metadata-Key."""
    if not isinstance(metadata, list):
        return ""
    for entry in metadata:
        if isinstance(entry, dict) and entry.get("key") == key:
            return _clean(entry.get("value"))
    return ""


def _md_get_all(metadata: list, key: str) -> list[str]:
    """Hole alle Werte fuer einen metadata-Key (mehrfach moeglich)."""
    if not isinstance(metadata, list):
        return []
    out: list[str] = []
    for entry in metadata:
        if isinstance(entry, dict) and entry.get("key") == key:
            v = _clean(entry.get("value"))
            if v:
                out.append(v)
    return out


def _format_authors(authors: list[str]) -> str:
    """Bis 5 Autoren joinen, sonst et al."""
    if not authors:
        return "Unbekannt"
    if len(authors) <= 5:
        return " / ".join(authors)
    return f"{' / '.join(authors[:5])} et al."


def _format_subjects(metadata: list) -> str:
    """Sammle Subjects/Classifications/dc.subject.other (kurz)."""
    subjects: list[str] = []
    seen: set[str] = set()
    for key in ("dc.subject.other", "dc.subject", "dc.subject.classification"):
        for v in _md_get_all(metadata, key):
            # Classification-Strings wie "thema EDItEUR::N History..." abkuerzen
            short = v.split("::")[-1] if "::" in v else v
            short = short.strip()
            if not short:
                continue
            sl = short.lower()
            if sl in seen:
                continue
            seen.add(sl)
            subjects.append(short)
            if len(subjects) >= 5:
                break
        if len(subjects) >= 5:
            break
    return ", ".join(subjects)


def _format_item(item: dict) -> dict | None:
    """Forme ein DOAB-Item in das Evidora-Result-Schema um."""
    if not isinstance(item, dict):
        return None

    handle = _clean(item.get("handle"))
    name = _clean(item.get("name"))
    metadata = item.get("metadata") or []

    title = _md_get_first(metadata, "dc.title") or name
    if not handle or not title:
        return None

    authors_list = _md_get_all(metadata, "dc.contributor.author")
    authors_str = _format_authors(authors_list)

    year_raw = _md_get_first(metadata, "dc.date.issued")
    # DOAB liefert teils "2013", teils "2013-11-18 ..." — nur das Jahr extrahieren.
    year_match = re.match(r"(\d{4})", year_raw)
    year = year_match.group(1) if year_match else (year_raw or "—")

    publisher = _md_get_first(metadata, "publisher.name")
    doi = _md_get_first(metadata, "oapen.identifier.doi")
    abstract = _md_get_first(metadata, "dc.description.abstract")
    language = _md_get_first(metadata, "dc.language")
    pages = _md_get_first(metadata, "oapen.pages")
    license_raw = _md_get_first(metadata, "publisher.oalicense")
    subjects = _format_subjects(metadata)

    # URL: DOAB-Permalink (immer vorhanden).
    doab_url = f"https://directory.doabooks.org/handle/{handle}"

    # display_value: kompaktes Buch-Zitat.
    cite_parts = [title]
    if authors_str and authors_str != "Unbekannt":
        cite_parts.append(f"— {authors_str}")
    pubyear_bits = []
    if publisher:
        pubyear_bits.append(publisher)
    if year and year != "—":
        pubyear_bits.append(year)
    if pubyear_bits:
        cite_parts.append(f"({', '.join(pubyear_bits)})")
    display_value = " ".join(cite_parts)
    if doi:
        display_value += f". DOI: {doi}"
    if len(display_value) > 280:
        display_value = display_value[:277] + "..."

    # description: Abstract (gekuerzt) + Subjects + Sprache + Pages + Lizenz.
    desc_parts: list[str] = []
    if abstract:
        abs_short = (
            abstract[:400] + "..." if len(abstract) > 400 else abstract
        )
        desc_parts.append(abs_short)
    if subjects:
        desc_parts.append(f"Themen: {subjects}")
    meta_bits: list[str] = []
    if language:
        meta_bits.append(f"Sprache: {language}")
    if pages:
        meta_bits.append(f"{pages} S.")
    if meta_bits:
        desc_parts.append(" · ".join(meta_bits))
    # Lizenz-Kurzform extrahieren (CC BY / CC BY-NC / CC BY-NC-ND etc.).
    if license_raw:
        cc_match = re.search(r"\bCC[\s-]?BY(?:[\s-]?NC)?(?:[\s-]?ND)?(?:[\s-]?SA)?\b",
                             license_raw, re.IGNORECASE)
        cc_short = cc_match.group(0).upper() if cc_match else "Open Access"
        desc_parts.append(f"Lizenz: {cc_short}")
    description = " — ".join(desc_parts) if desc_parts else "Open-Access-Buch (DOAB)."

    # indicator-id aus handle: "20.500.12854/47072" → "doab_20_500_12854_47072"
    indicator_id = "doab_" + re.sub(r"[^a-z0-9]+", "_", handle.lower()).strip("_")

    return {
        "indicator_name": title[:300],
        "indicator": indicator_id,
        "country": "INT",
        "country_name": "International (OA-Wissenschafts-DB)",
        "year": year,
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": doab_url,
        "source": "DOAB (Directory of Open Access Books)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_doab(analysis: dict) -> dict:
    """Live-Lookup gegen DOAB fuer Buch-Claims (Geistes-/Sozialwiss).

    Returns Dict im Standard-Evidora-Format mit bis zu MAX_RESULTS Treffern.
    Liefert leeres Result-Set, wenn Trigger nicht greift oder API leer.

    Politische Guardrails: Service zitiert nur Buch-Metadaten + Abstract,
    keine eigene Verdict-Bewertung. Synthesizer entscheidet.
    """
    empty = {
        "source": "DOAB",
        "type": "academic_books",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")

    matchable = f"{original} {claim}".lower().strip()
    if not _claim_mentions_doab(matchable):
        return empty

    query = _build_doab_query(claim or original, analysis)
    if not query:
        logger.debug("DOAB: keine brauchbare Query aus Claim ableitbar")
        return empty

    items = await _fetch_doab(query)
    if not items:
        logger.info(f"DOAB: 0 Treffer fuer '{query[:40]}'")
        return empty

    results: list[dict] = []
    for item in items[:MAX_RESULTS]:
        try:
            r = _format_item(item)
        except Exception as e:
            logger.debug(f"DOAB: item-format-error: {e}")
            continue
        if r:
            results.append(r)

    if not results:
        return empty

    logger.info(f"DOAB: {len(results)} Treffer fuer '{query[:40]}'")
    return {
        "source": "DOAB",
        "type": "academic_books",
        "results": results,
    }
