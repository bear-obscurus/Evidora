"""OEAW EPUB.OEAW Live-Connector — OAI-PMH Institutional Repository.

EPUB.OEAW (https://epub.oeaw.ac.at/) ist das elektronische Publikations-
portal der Oesterreichischen Akademie der Wissenschaften. Seit 2006
werden Monographien, Sammelbaende, Zeitschriften und Lexika der OEAW
Open-Access publiziert — Schwerpunkt Geistes-, Sozial- und Kultur-
wissenschaften mit starkem AT-Bezug:

  * Oesterreichisches Biographisches Lexikon (OEBL)
  * Oesterreichisches Musiklexikon (ML)
  * Archaeologia Austriaca, Archaeologische Forschungen
  * Fontes Rerum Austriacarum, Archiv fuer Oesterreichische Geschichte
  * Forschungen zur Geschichte des Mittelalters
  * Wiener Studien, Wiener Slavistisches Jahrbuch
  * Mitteilungen der Praehistorischen Kommission, etc.

Komplementaer zu existierenden Quellen:
  * DOAB: globale OA-Buecher (Verlags-uebergreifend).
  * Crossref: Artikel-DOI-Lookup.
  * Europeana: Kulturobjekt-Digitalisate.
  * EPUB.OEAW: AT-akademische Primaerquellen + Biographien.

API: OAI-PMH 2.0 (https://www.openarchives.org/OAI/openarchivesprotocol.html)
Endpoint: https://epub.oeaw.ac.at/oai  (kein API-Key, kein Auth)
  ?verb=ListRecords&metadataPrefix=oai_dc&set=<setSpec>

Wichtige Sets (siehe verb=ListSets):
  * buecher           — Elektronische Publikationen (Gesamt-Hub)
  * oebl              — Oesterreichisches Biographisches Lexikon
  * ml                — Oesterreichisches Musiklexikon
  * kl                — Lexikon zur keltischen Archaeologie

Hinweis: OAI-PMH ist ein Harvest-Protokoll, kein Such-Protokoll. Wir
holen pro Trigger-Match die fuer den Claim wahrscheinlichste Set-
Sammlung und filtern client-seitig per Keyword-Match auf dc:title /
dc:description. Fuer haeufige Lexikon-Themen (OEBL, ML) ist das
ausreichend; die Sets sind <1 MB XML und werden 24 h gecached.

Dublin-Core-Felder pro Record:
  * dc:title          — Eintragstitel (Person, Werk, Begriff)
  * dc:subject        — Klassifikation (Musicology, Biography, ...)
  * dc:description    — Lebensdaten / Beschreibung / Abstract
  * dc:date           — Erstellungsdatum
  * dc:type           — info:eu-repo/semantics/article (oder book)
  * dc:rights         — info:eu-repo/semantics/openAccess (immer)
  * dc:identifier     — XML-Permalink + DOI (10.1553/...)
  * dc:language       — de / en / la / ...

Lizenz: Open Access (CC-BY-Varianten der OEAW-Verlagsabteilung).
Metadata via OAI-PMH ist gemeinfrei.

Politische Guardrails (siehe project_political_guardrails.md):
Geistes-/Biographie-Eintraege koennen normative Aspekte enthalten
(z. B. NS-Vergangenheit, politische Rollen). Wir zitieren ausschliesslich
die OEBL/ML-Beschreibung — KEINE eigene Bewertung, KEINE Parteinahme.
Synthesizer entscheidet das Verdict.

# WIRING fuer main.py:
# from services.oeaw_epub import search_oeaw_epub, claim_mentions_oeaw_cached
# if claim_mentions_oeaw_cached(claim):
#     tasks.append(cached("OEAW EPUB.OEAW", search_oeaw_epub, analysis))
#     queried_names.append("OEAW EPUB.OEAW")
#
# WIRING fuer reranker.py (Indicator-Whitelist-Prefix):
#   "oeaw_epub_" in INDICATOR_WHITELIST_PREFIXES
#
# data_updater.py: KEIN Prefetch noetig (Live-Only, Set-Cache reicht).
"""

from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

OEAW_OAI_URL = "https://epub.oeaw.ac.at/oai"

MAX_RESULTS = 5
TIMEOUT_S = 25.0
SET_CACHE_TTL_S = 24 * 60 * 60  # 24 h pro Set-XML
QUERY_CACHE_TTL_S = 24 * 60 * 60  # 24 h pro Claim-Query

# XML-Namespaces fuer ElementTree-Parsing.
_NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc": "http://purl.org/dc/elements/1.1/",
}

# Module-Level Set-XML-Cache: { set_spec: (timestamp, parsed_records) }
_SET_CACHE: dict[str, tuple[float, list[dict]]] = {}

# Query-Cache: { (set_spec, query_key): (timestamp, results) }
_QUERY_CACHE: dict[tuple[str, str], tuple[float, list[dict]]] = {}

# Trigger-Resolve-Cache: { claim_lc: bool }
_TRIGGER_CACHE: dict[str, bool] = {}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Direkte Quellen-Mentions.
_EXPLICIT_TERMS = (
    "epub.oeaw", "epub oeaw",
    "oeaw publikation", "öaw publikation", "öaw-publikation",
    "oeaw forschung", "öaw forschung", "öaw-forschung",
    "österreichische akademie der wissenschaften",
    "oesterreichische akademie der wissenschaften",
    "akademie der wissenschaften wien",
    "verlag der oeaw", "verlag der öaw",
    "oeaw-verlag", "öaw-verlag",
    "austrian academy of sciences",
)

# OEAW-Akronyme (Vorsicht bei Mehrdeutigkeit — wir matchen exakt mit Wortgrenze).
_OEAW_ACRONYMS = (
    "öaw", "oeaw",
)

# OEAW-Lexika und -Reihen (Namens-Trigger).
_OEAW_PUBLICATIONS = (
    # Lexika
    "österreichisches biographisches lexikon", "oesterreichisches biographisches lexikon",
    "oebl",
    "österreichisches musiklexikon", "oesterreichisches musiklexikon",
    "oeml", "oe-ml",
    "lexikon zur keltischen archäologie",
    # Zeitschriften / Reihen (haeufig zitiert)
    "archaeologia austriaca",
    "fontes rerum austriacarum",
    "archiv für österreichische geschichte",
    "wiener studien",
    "wiener byzantinistische studien",
    "wiener slavistisches jahrbuch",
    "römische historische mitteilungen",
    "ägypten und levante",
    "jahreshefte des österreichischen archäologischen institutes",
    "forschungen zur geschichte des mittelalters",
    "sitzungsberichte der philosophisch-historischen klasse",
    "denkschriften der philosophisch-historischen klasse",
    "mitteilungen der prähistorischen kommission",
    "studien zur geschichte der österreichisch-ungarischen monarchie",
    "corpus scriptorum ecclesiasticorum latinorum",
    "catalogus fossilium austriae",
    "altdeutsches namenbuch",
    "ortsnamenbuch des landes oberösterreich",
)

# Composite-Trigger: AT-akademische Domaenen + "Forschung/Studie/Publikation".
_AT_ACADEMIC_CONTEXT = (
    "österreichische geschichte", "oesterreichische geschichte",
    "österreichische archäologie", "oesterreichische archaeologie",
    "österreichische biographie", "oesterreichische biographie",
    "österreichische musikgeschichte", "oesterreichische musikgeschichte",
    "österreichische literaturwissenschaft",
    "österreichische kulturgeschichte",
    "österreichische byzantinistik",
    "österreichische slawistik", "österreichische slavistik",
    "byzantinistik",
    "keltische archäologie",
    "altösterreichisch", "altoesterreichisch",
    "habsburgermonarchie",
    "österreichisch-ungarische monarchie",
    "donaumonarchie",
)


def _has_acronym(claim_lc: str, acr: str) -> bool:
    """Wort-Boundary-Match fuer kurze Akronyme (öaw/oeaw)."""
    if not acr or acr not in claim_lc:
        return False
    pat = r"(?:^|[^a-zäöüß0-9])" + re.escape(acr) + r"(?:[^a-zäöüß0-9]|$)"
    return re.search(pat, claim_lc) is not None


def _claim_mentions_oeaw(claim_lc: str) -> bool:
    """Pure Trigger-Funktion (lowercase claim erwartet)."""
    if not claim_lc:
        return False

    # Direkte Mentions.
    if any(t in claim_lc for t in _EXPLICIT_TERMS):
        return True

    # Akronym-Match (mit Boundary).
    if any(_has_acronym(claim_lc, a) for a in _OEAW_ACRONYMS):
        return True

    # Lexika-/Reihen-Namen.
    if any(p in claim_lc for p in _OEAW_PUBLICATIONS):
        return True

    # AT-akademischer Kontext.
    if any(c in claim_lc for c in _AT_ACADEMIC_CONTEXT):
        return True

    return False


def claim_mentions_oeaw_cached(claim: str) -> bool:
    """Cached Wrapper — Trigger-Resolve cached pro Claim-String."""
    if not claim:
        return False
    key = claim.lower()
    cached = _TRIGGER_CACHE.get(key)
    if cached is not None:
        return cached
    result = _claim_mentions_oeaw(key)
    if len(_TRIGGER_CACHE) > 2048:
        _TRIGGER_CACHE.clear()
    _TRIGGER_CACHE[key] = result
    return result


# ---------------------------------------------------------------------------
# Set-Selection: welches OAI-Set passt zum Claim?
# ---------------------------------------------------------------------------
# Mapping setSpec → (Human-Name, Trigger-Keywords, Default-Subjekt-Label).
# Reihenfolge wichtig: spezifischere Sets vor "buecher" (Catch-All).
_SET_PROFILES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "oebl",
        "Oesterreichisches Biographisches Lexikon",
        (
            "oebl", "biographisches lexikon",
            "biographie", "biografie",
            "lebenslauf", "vita",
            "geboren", "gestorben",
            "österreichische persönlichkeit",
        ),
    ),
    (
        "ml",
        "Oesterreichisches Musiklexikon",
        (
            "musiklexikon", "oeml",
            "österreichische musik", "oesterreichische musik",
            "österreichischer komponist", "österreichische komponistin",
            "österreichische musikgeschichte",
            "wiener klassik", "wiener moderne musik",
            "volksmusik österreich",
            "komponist", "komponistin",
            "kapellmeister", "musikdirektor",
            "österreichische musiker", "wiener musiker",
        ),
    ),
    (
        "kl",
        "Lexikon zur keltischen Archaeologie",
        (
            "keltisch", "kelten",
            "keltische archäologie",
            "latènezeit", "hallstattzeit",
        ),
    ),
)


def _select_oeaw_set(claim_lc: str) -> tuple[str, str]:
    """Waehle das wahrscheinlichste OAI-Set fuer den Claim.

    Returns: (set_spec, human_set_name).
    Fallback: ("buecher", "Elektronische Publikationen OEAW").
    """
    for set_spec, set_name, keywords in _SET_PROFILES:
        if any(kw in claim_lc for kw in keywords):
            return set_spec, set_name
    return "buecher", "Elektronische Publikationen OEAW"


# ---------------------------------------------------------------------------
# Keyword-Extraktion fuer client-seitiges Set-Filtering
# ---------------------------------------------------------------------------
_STOP_WORDS = frozenset((
    "die", "der", "das", "den", "dem", "des",
    "ein", "eine", "einer", "eines", "einem", "einen",
    "und", "oder", "aber", "dass", "ob",
    "ist", "sind", "war", "waren", "wird", "werden", "wurde", "wurden",
    "hat", "haben", "habe", "hatte", "hatten",
    "mit", "ohne", "für", "fuer", "auf", "an", "am", "von", "vom",
    "zu", "zur", "zum", "bei", "nach", "vor", "über", "ueber", "unter",
    "nicht", "kein", "keine",
    "auch", "noch", "nur", "schon", "sehr",
    "ja", "nein", "wie", "was", "wer", "wann", "wo", "warum",
    "im", "in",
    "the", "a", "an", "of", "on", "at", "for", "with",
    "is", "are", "was", "were", "be", "been",
    "and", "or", "but", "if", "then",
    # OEAW-spezifische Filler
    "öaw", "oeaw", "akademie", "wissenschaften",
    "österreich", "österreichische", "österreichisch", "austria",
    "publikation", "veröffentlichung", "forschung",
    "studie", "studien", "buch", "buecher", "bücher",
))


def _extract_keywords(claim: str, analysis: dict | None = None) -> list[str]:
    """Extrahiere bis zu 5 Keywords fuer client-seitiges Filtering."""
    analysis = analysis or {}

    # Bevorzuge factcheck_queries[0] wenn vorhanden.
    fc_queries = analysis.get("factcheck_queries") or []
    candidates: list[str] = []
    if isinstance(fc_queries, list) and fc_queries:
        first = str(fc_queries[0] or "").strip()
        if first:
            candidates.append(first.lower())

    if claim:
        candidates.append(claim.lower())

    keywords: list[str] = []
    seen: set[str] = set()
    for text in candidates:
        tokens = re.findall(r"[a-zäöüß][a-zäöüß\-]{2,}", text)
        for tok in tokens:
            if tok in _STOP_WORDS:
                continue
            if tok in seen:
                continue
            seen.add(tok)
            keywords.append(tok)
            if len(keywords) >= 5:
                return keywords
    return keywords


# ---------------------------------------------------------------------------
# HTTP / XML-Parsing
# ---------------------------------------------------------------------------
def _parse_dc_records(xml_bytes: bytes) -> list[dict]:
    """Parse OAI-PMH ListRecords-Response zu Dict-Liste."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.debug(f"OEAW: XML-Parse-Error: {e}")
        return []

    records: list[dict] = []
    for rec in root.iter(f"{{{_NS['oai']}}}record"):
        header = rec.find(f"{{{_NS['oai']}}}header")
        if header is None:
            continue
        status = header.attrib.get("status", "")
        if status == "deleted":
            continue
        ident_el = header.find(f"{{{_NS['oai']}}}identifier")
        oai_id = (ident_el.text or "").strip() if ident_el is not None else ""

        meta = rec.find(f"{{{_NS['oai']}}}metadata")
        if meta is None:
            continue
        dc_root = meta.find(f"{{{_NS['oai_dc']}}}dc")
        if dc_root is None:
            continue

        def _multi(tag: str) -> list[str]:
            values: list[str] = []
            for el in dc_root.findall(f"{{{_NS['dc']}}}{tag}"):
                t = (el.text or "").strip()
                if t:
                    values.append(t)
            return values

        titles = _multi("title")
        if not titles:
            continue

        identifiers = _multi("identifier")
        # DC-identifier: erste URL = Permalink; weitere koennen DOI sein.
        url = ""
        doi = ""
        for ident in identifiers:
            if not url and ident.lower().startswith("http"):
                url = ident
            elif "doi:" in ident.lower() or ident.lower().startswith("10."):
                # "DOI: 10.1553/..." oder "10.1553/..." direkt
                m = re.search(r"(10\.\d{4,9}/[^\s]+)", ident)
                if m and not doi:
                    doi = m.group(1).rstrip(".,;")

        records.append({
            "oai_id": oai_id,
            "title": titles[0],
            "subjects": _multi("subject"),
            "description": (_multi("description") or [""])[0],
            "date": (_multi("date") or [""])[0],
            "type": (_multi("type") or [""])[0],
            "rights": (_multi("rights") or [""])[0],
            "language": (_multi("language") or [""])[0],
            "url": url,
            "doi": doi,
        })

    return records


def _set_cache_get(set_spec: str) -> list[dict] | None:
    entry = _SET_CACHE.get(set_spec)
    if not entry:
        return None
    ts, data = entry
    if (time.time() - ts) > SET_CACHE_TTL_S:
        _SET_CACHE.pop(set_spec, None)
        return None
    return data


def _set_cache_put(set_spec: str, data: list[dict]) -> None:
    if len(_SET_CACHE) > 32:
        _SET_CACHE.clear()
    _SET_CACHE[set_spec] = (time.time(), data)


async def _fetch_oai_set(set_spec: str) -> list[dict]:
    """Hole Records eines OAI-Sets (mit Cache). Returns [] bei Fehler."""
    if not set_spec:
        return []

    cached = _set_cache_get(set_spec)
    if cached is not None:
        logger.debug(f"OEAW: cache-hit fuer set '{set_spec}'")
        return cached

    url = (
        f"{OEAW_OAI_URL}?verb=ListRecords"
        f"&metadataPrefix=oai_dc"
        f"&set={quote_plus(set_spec)}"
    )

    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"OEAW HTTP {resp.status_code} fuer set '{set_spec}'"
            )
            return []
        records = _parse_dc_records(resp.content)
    except Exception as e:
        logger.debug(f"OEAW fetch failed fuer set '{set_spec}': {e}")
        return []

    _set_cache_put(set_spec, records)
    logger.info(f"OEAW: {len(records)} records geharvested fuer set '{set_spec}'")
    return records


# ---------------------------------------------------------------------------
# Filtering / Scoring
# ---------------------------------------------------------------------------
def _score_record(rec: dict, keywords: list[str]) -> int:
    """Einfaches Substring-Scoring: Titel-Match = 3, Description-Match = 1."""
    if not keywords:
        return 0
    title_lc = (rec.get("title") or "").lower()
    desc_lc = (rec.get("description") or "").lower()
    score = 0
    for kw in keywords:
        if kw in title_lc:
            score += 3
        elif kw in desc_lc:
            score += 1
    return score


def _filter_and_rank(
    records: list[dict],
    keywords: list[str],
    limit: int = MAX_RESULTS,
) -> list[dict]:
    """Filtere Records nach Keywords und sortiere nach Score absteigend."""
    if not records:
        return []
    if not keywords:
        # Ohne Keywords: erste N Records.
        return records[:limit]
    scored: list[tuple[int, dict]] = []
    for rec in records:
        s = _score_record(rec, keywords)
        if s > 0:
            scored.append((s, rec))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in scored[:limit]]


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _trim(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "..."


def _safe_id_suffix(oai_id: str) -> str:
    """oai:epub.oeaw.ac.at:0x0001cfef -> _0x0001cfef"""
    if not oai_id:
        return "unknown"
    last = oai_id.split(":")[-1]
    safe = "".join(c if c.isalnum() else "_" for c in last)
    return safe[:60] or "unknown"


def _year_from_date(date_str: str) -> str:
    """Extrahiere Jahr aus dc:date (YYYY oder YYYY-MM-DD)."""
    if not date_str:
        return ""
    m = re.match(r"(\d{4})", date_str)
    return m.group(1) if m else ""


def _format_record(rec: dict, set_name: str) -> dict | None:
    title = rec.get("title") or ""
    if not title:
        return None

    oai_id = rec.get("oai_id") or ""
    url = rec.get("url") or ""
    doi = rec.get("doi") or ""

    # Fallback-URL: DOI-Resolver oder OEAW-ID-Permalink.
    if not url and doi:
        url = f"https://doi.org/{doi}"
    if not url and oai_id:
        # oai:epub.oeaw.ac.at:0x... → epub.oeaw.ac.at Permalink (Best-Effort).
        last = oai_id.split(":")[-1]
        url = f"https://epub.oeaw.ac.at/?arp=0x{last.lstrip('0x')}" if last else "https://epub.oeaw.ac.at/"

    year = _year_from_date(rec.get("date", ""))
    desc_raw = rec.get("description", "")
    subjects = rec.get("subjects") or []
    language = rec.get("language", "")

    # display_value: Titel + Reihe + Jahr.
    bits = [title]
    series_bits: list[str] = []
    if set_name:
        series_bits.append(set_name)
    if year:
        series_bits.append(year)
    if series_bits:
        bits.append(f"({', '.join(series_bits)})")
    display_value = _trim(" ".join(bits), 280)
    if doi:
        display_value = _trim(display_value + f". DOI: {doi}", 320)

    # description: OEBL/ML-Beschreibung + Subjects + Sprache + Lizenz.
    desc_parts: list[str] = []
    if desc_raw:
        desc_parts.append(_trim(desc_raw, 400))
    if subjects:
        subj_short = ", ".join(subjects[:3])
        desc_parts.append(f"Themen: {subj_short}")
    if language:
        desc_parts.append(f"Sprache: {language}")
    desc_parts.append("Lizenz: Open Access (OEAW)")
    description = " - ".join(desc_parts)

    return {
        "indicator_name": _trim(f"OEAW: {title}", 300),
        "indicator": f"oeaw_epub_{_safe_id_suffix(oai_id)}",
        "country": "AUT",
        "country_name": "Oesterreich",
        "year": year or "-",
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": url,
        "source": "EPUB.OEAW (Oesterreichische Akademie der Wissenschaften)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_oeaw_epub(analysis: dict) -> dict:
    """Live-Lookup gegen EPUB.OEAW via OAI-PMH.

    Strategie:
      1. Trigger pruefen — sonst leere Antwort.
      2. Bestes OAI-Set fuer den Claim auswaehlen (oebl/ml/kl/buecher).
      3. Set einmal harvesten (24 h cache), client-seitig nach Keywords
         filtern und nach Substring-Score ranken.
      4. Top-MAX_RESULTS in Evidora-Format umformen.

    Politische Guardrails: Service zitiert nur Lexikon-/Publikations-
    Metadata + erste 400 Zeichen Beschreibung. Keine eigene Bewertung.
    """
    empty = {
        "source": "OEAW EPUB.OEAW",
        "type": "academic_at",
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
    if not _claim_mentions_oeaw(matchable):
        return empty

    set_spec, set_name = _select_oeaw_set(matchable)
    keywords = _extract_keywords(claim or original, analysis)

    # Query-Cache nach (set, keywords-hash)
    qkey = (set_spec, "|".join(sorted(keywords)))
    cached = _QUERY_CACHE.get(qkey)
    now = time.time()
    if cached and (now - cached[0] < QUERY_CACHE_TTL_S):
        logger.debug(f"OEAW: query-cache-hit fuer set='{set_spec}' kw={keywords}")
        return {
            "source": "OEAW EPUB.OEAW",
            "type": "academic_at",
            "results": cached[1],
        }

    records = await _fetch_oai_set(set_spec)
    if not records:
        return empty

    top = _filter_and_rank(records, keywords, limit=MAX_RESULTS)
    if not top:
        logger.info(
            f"OEAW: 0 Filter-Treffer in set '{set_spec}' fuer kw={keywords}"
        )
        return empty

    results: list[dict] = []
    seen_ids: set[str] = set()
    for rec in top:
        try:
            row = _format_record(rec, set_name)
        except Exception as e:
            logger.debug(f"OEAW: format-error: {e}")
            continue
        if not row:
            continue
        ind = row.get("indicator", "")
        if ind in seen_ids:
            continue
        seen_ids.add(ind)
        results.append(row)

    # Cache fuellen (auch bei nicht-leerem Ergebnis).
    if len(_QUERY_CACHE) > 256:
        _QUERY_CACHE.clear()
    _QUERY_CACHE[qkey] = (now, results)

    logger.info(
        f"OEAW: {len(results)} Treffer fuer set='{set_spec}' kw={keywords}"
    )
    return {
        "source": "OEAW EPUB.OEAW",
        "type": "academic_at",
        "results": results,
    }
