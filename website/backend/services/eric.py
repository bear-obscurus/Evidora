"""ERIC Live-Connector — Education Resources Information Center (US ED).

ERIC (https://eric.ed.gov) ist die zentrale Bildungsforschungs-Datenbank
des U.S. Department of Education / Institute of Education Sciences.
~1,6 Mio. Records, ~450k Volltext-PDFs seit 1966. Pflicht-Quelle bei
Bildungsforschungs-Claims (PISA / IGLU / TIMSS / Lesefoerderung /
Lehrer-Studien / "Methode X wirkt").

API: https://api.ies.ed.gov/eric/  (GET, JSON, kein Auth, Public Domain).

Beispiele:
  ?search=reading+comprehension&format=json&rows=5
  ?search=pisa&format=json&rows=5&peerreviewed=1
  ?search=title:"reading"+AND+publicationdateyear:2023&format=json

Lizenz: Public Domain (US-Bundesbehoerde) — Evidora-tauglich.

Komplementaer zu existierenden Quellen:
- bildung_pack.py: kuratiertes JSON-Pack mit DACH-Forschungsstand
- education_dach.py: deutschsprachige Bildungs-Statistiken
- ERIC: internationale Primaerquellen-Suche, peer-reviewed Studien

Politische Guardrails (siehe project_political_guardrails.md):
Bildungspolitik ist sensibel — der Service liefert NUR Studien-Zitate
und Abstracts. KEINE eigene Bewertung der politischen Implikationen.
Synthesizer-Layer entscheidet Verdict.

# WIRING fuer main.py:
# from services.eric import search_eric, claim_mentions_eric_cached
# if claim_mentions_eric_cached(claim):
#     tasks.append(cached("ERIC", search_eric, analysis))
#     queried_names.append("ERIC")
"""

from __future__ import annotations

import logging
import re
import time
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

ERIC_API = "https://api.ies.ed.gov/eric/"

# Felder explizit anfordern — sonst fehlen u. a. `source` und `url`.
ERIC_FIELDS = (
    "id,title,author,description,publicationdateyear,peerreviewed,"
    "publicationtype,educationlevel,source,url,subject,audience"
)

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
# Direkte Bildungs-Begriffe (DE) — Match-on-substring.
_ERIC_TERMS = (
    "eric ", " eric", "eric-datenbank", "eric datenbank",
    "bildungsstudie", "bildungsstudien",
    "bildungsforschung", "bildungs-forschung",
    "lernforschung", "lern-forschung",
    "schulforschung", "schul-forschung",
    "pisa-studie", "pisa studie", "pisa-test", "pisa test",
    "iglu-studie", "iglu studie",
    "timss-studie", "timss studie",
    "lese-kompetenz", "lesekompetenz",
    "mathematik-kompetenz", "mathematikkompetenz", "rechenkompetenz",
    "schreib-kompetenz", "schreibkompetenz",
    "bildungs-reform", "bildungsreform",
    "lehrer-studie", "lehrer studie", "lehrerstudie",
    "lehrerinnen-studie", "lehrer:innen-studie",
    "fruehfoerderung", "frueh-foerderung", "frühförderung",
    "lese-foerderung", "lesefoerderung", "leseförderung",
    "didaktik-studie", "didaktikstudie",
    "lernmethode wirkt", "lernmethode funktioniert",
    "unterrichtsmethode wirkt", "unterrichtsmethode funktioniert",
    # EN
    "education research", "educational research",
    "learning study", "learning research",
    "teaching effectiveness", "teaching method effectiveness",
    "reading comprehension", "reading research",
    "math achievement", "literacy research",
    "phonics instruction", "classroom research",
    "school research", "student achievement",
)

# Methoden-Wirkungs-Pattern: "<X> wirkt (nicht)" / "<X> funktioniert (nicht)"
_METHOD_EFFECT_RE = re.compile(
    r"\b(montessori|waldorf|jenaplan|reformpaedagogik|"
    r"frontalunterricht|frontal-unterricht|"
    r"projektunterricht|projekt-unterricht|"
    r"offener unterricht|offenes lernen|"
    r"binnendifferenzierung|inklusion(?:s-unterricht)?|"
    r"sitzenbleiben|klassenwiederholung|"
    r"ganztagsschule|ganztags-schule|"
    r"gesamtschule|mittelschule|gymnasium|"
    r"digitaler unterricht|digital learning|"
    r"tablet-klasse|laptop-klasse|"
    r"phonics|systematic phonics|"
    r"flipped classroom|hybrid learning)\b.*?\b(wirkt|funktioniert|"
    r"hilft|bringt nichts|bringt etwas|schadet|verbessert|verschlechtert)\b",
    re.IGNORECASE,
)

# Peer-Review-Trigger: bei diesen Indikatoren forcieren wir peerreviewed=1.
_PEER_REVIEW_TRIGGERS = (
    "wissenschaftliche studie",
    "wissenschaftliche untersuchung",
    "peer-reviewed", "peer reviewed",
    "begutachtete studie",
    "peer review",
    "wissenschaftlich erwiesen",
    "wissenschaftlich belegt",
)


def _claim_mentions_eric(claim_lc: str) -> bool:
    """Pure Trigger-Funktion (lowercase claim erwartet)."""
    if not claim_lc:
        return False
    if any(t in claim_lc for t in _ERIC_TERMS):
        return True
    # "studie" + Bildungs-Kontext (PISA/IGLU/TIMSS-Kombinationen).
    if "studie" in claim_lc or "study" in claim_lc:
        edu_context = any(t in claim_lc for t in (
            "pisa", "iglu", "timms", "timss",
            "schule", "schulkinder", "schulleist",
            "schueler", "schüler", "students",
            "lernen", "lehrer", "lehrkraft", "lehrkraefte",
            "unterricht", "didaktik",
            "kindergarten", "kita",
        ))
        if edu_context:
            return True
    # "Methode X wirkt/funktioniert" — Wirkungs-Claim.
    if _METHOD_EFFECT_RE.search(claim_lc):
        return True
    return False


def claim_mentions_eric_cached(claim: str) -> bool:
    """Cached Wrapper — Trigger-Resolve cached pro Claim-String."""
    if not claim:
        return False
    key = claim.lower()
    cached = _TRIGGER_CACHE.get(key)
    if cached is not None:
        return cached
    result = _claim_mentions_eric(key)
    # Kleiner Schutz gegen unbounded Wachstum.
    if len(_TRIGGER_CACHE) > 2048:
        _TRIGGER_CACHE.clear()
    _TRIGGER_CACHE[key] = result
    return result


# ---------------------------------------------------------------------------
# Query-Builder
# ---------------------------------------------------------------------------
# Bildungs-Stop-Words die wir bei der Keyword-Extraktion entfernen.
_STOP_WORDS = frozenset((
    "die", "der", "das", "den", "dem", "des",
    "ein", "eine", "einer", "eines", "einem", "einen",
    "und", "oder", "aber", "doch", "denn",
    "ist", "sind", "war", "waren", "wird", "werden", "wurde", "wurden",
    "hat", "haben", "habe", "hatte", "hatten",
    "kann", "koennen", "können", "konnte", "konnten",
    "muss", "muessen", "müssen", "musste", "mussten",
    "soll", "sollen", "sollte", "sollten",
    "darf", "duerfen", "dürfen", "durfte", "durften",
    "mit", "ohne", "fuer", "für", "auf", "in", "im", "an", "am", "von", "vom",
    "zu", "zur", "zum", "bei", "nach", "vor", "ueber", "über", "unter",
    "nicht", "kein", "keine", "keinen", "keiner",
    "auch", "noch", "nur", "schon", "sehr", "mehr", "weniger",
    "ja", "nein", "wie", "was", "wer", "wann", "wo", "warum", "wieso",
    "the", "a", "an", "of", "in", "on", "at", "for", "with", "without",
    "is", "are", "was", "were", "be", "been", "being",
    "has", "have", "had", "having",
    "do", "does", "did", "doing",
    "and", "or", "but", "if", "then", "than",
    "this", "that", "these", "those",
    "studie", "studien", "study", "studies",
    "forschung", "research", "untersuchung",
))

# Mapping DE → EN fuer typische Bildungs-Begriffe — ERIC ist EN-only.
_DE_EN_MAP = {
    "lesekompetenz": "reading literacy",
    "lese-kompetenz": "reading literacy",
    "leseverstehen": "reading comprehension",
    "lesefoerderung": "reading instruction",
    "leseförderung": "reading instruction",
    "mathematikkompetenz": "mathematics achievement",
    "mathematik-kompetenz": "mathematics achievement",
    "rechenkompetenz": "mathematics achievement",
    "schreibkompetenz": "writing skills",
    "schreib-kompetenz": "writing skills",
    "schueler": "students",
    "schüler": "students",
    "schulkinder": "students",
    "schulleistung": "academic achievement",
    "schulforschung": "school research",
    "lehrer": "teachers",
    "lehrkraft": "teachers",
    "lehrkraefte": "teachers",
    "unterricht": "instruction",
    "didaktik": "didactics teaching",
    "fruehfoerderung": "early intervention",
    "frühförderung": "early intervention",
    "kindergarten": "kindergarten",
    "kita": "early childhood education",
    "ganztagsschule": "all-day school",
    "ganztags-schule": "all-day school",
    "gesamtschule": "comprehensive school",
    "mittelschule": "middle school",
    "gymnasium": "academic secondary school",
    "inklusion": "inclusion special education",
    "sitzenbleiben": "grade retention",
    "klassenwiederholung": "grade retention",
    "montessori": "montessori method",
    "waldorf": "waldorf education",
    "phonics": "phonics",
    "binnendifferenzierung": "differentiated instruction",
    "frontalunterricht": "direct instruction",
    "projektunterricht": "project-based learning",
    "flipped classroom": "flipped classroom",
    "bildungsreform": "education reform",
    "bildungs-reform": "education reform",
    "bildungsforschung": "education research",
    "lernmethode": "learning method",
    "unterrichtsmethode": "teaching method",
    "pisa-studie": "pisa study",
    "iglu-studie": "pirls reading",  # IGLU = PIRLS international
    "timss-studie": "timss",
}


def _build_eric_query(claim: str, analysis: dict | None = None) -> str:
    """Baue eine ERIC-Suche aus Claim + optionalen Analysis-Queries.

    Strategie:
    1. Wenn analysis.factcheck_queries non-empty, die erste nehmen.
    2. Sonst: Claim normalisieren, DE→EN-Map anwenden, Stop-Words filtern,
       Top-4 Keywords als Plus-getrennte AND-Query bauen.
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

    # DE→EN-Substitution VOR Tokenisierung (Multi-Word-Maps).
    for de, en in _DE_EN_MAP.items():
        if de in text:
            text = text.replace(de, " " + en + " ")

    # Tokenisierung: alphanumerisch + Bindestrich.
    tokens = re.findall(r"[a-zA-Z][a-zA-Z\-]{2,}", text)

    # Filter Stop-Words + Lower-Casing.
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


def _should_force_peer_review(claim_lc: str) -> bool:
    """Wenn der Claim auf eine wissenschaftliche Studie zielt: Filter setzen."""
    if not claim_lc:
        return False
    return any(t in claim_lc for t in _PEER_REVIEW_TRIGGERS)


# ---------------------------------------------------------------------------
# HTTP-Layer
# ---------------------------------------------------------------------------
def _cache_key(query: str, peer_only: bool) -> str:
    return f"{query.lower()}|peer={int(peer_only)}"


def _cache_get(query: str, peer_only: bool) -> list[dict] | None:
    key = _cache_key(query, peer_only)
    entry = _QUERY_CACHE.get(key)
    if not entry:
        return None
    ts, data = entry
    if (time.time() - ts) > CACHE_TTL_S:
        _QUERY_CACHE.pop(key, None)
        return None
    return data


def _cache_put(query: str, peer_only: bool, data: list[dict]) -> None:
    key = _cache_key(query, peer_only)
    # Schutz vor unbounded Wachstum.
    if len(_QUERY_CACHE) > 512:
        _QUERY_CACHE.clear()
    _QUERY_CACHE[key] = (time.time(), data)


async def _fetch_eric(query: str, peer_only: bool) -> list[dict]:
    """Hole rohe Docs von ERIC API. Returns [] bei jedem Fehler."""
    if not query or len(query.strip()) < 3:
        return []

    # Cache-Lookup zuerst.
    cached = _cache_get(query, peer_only)
    if cached is not None:
        logger.debug(f"ERIC: cache-hit fuer '{query[:40]}' peer={peer_only}")
        return cached

    url = (
        f"{ERIC_API}?search={quote_plus(query)}"
        f"&format=json&rows={MAX_RESULTS}&fields={ERIC_FIELDS}"
    )
    if peer_only:
        url += "&peerreviewed=1"

    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"ERIC HTTP {resp.status_code} fuer '{query[:40]}'"
            )
            return []
        try:
            payload = resp.json()
        except Exception as e:
            logger.debug(f"ERIC JSON-Parse-Error: {e}")
            return []
    except Exception as e:
        logger.debug(f"ERIC fetch failed fuer '{query[:40]}': {e}")
        return []

    response = (payload or {}).get("response") or {}
    docs = response.get("docs") or []
    if not isinstance(docs, list):
        return []

    _cache_put(query, peer_only, docs)
    return docs


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
_HTML_ENTITIES = {
    "&quot;": '"', "&amp;": "&", "&lt;": "<", "&gt;": ">",
    "&apos;": "'", "&#39;": "'", "&nbsp;": " ",
}


def _clean(s) -> str:
    """Normalisiere ERIC-Text: HTML-Entities, Whitespace, strip."""
    if s is None:
        return ""
    text = str(s)
    for ent, repl in _HTML_ENTITIES.items():
        text = text.replace(ent, repl)
    return re.sub(r"\s+", " ", text).strip()


def _format_authors(authors) -> str:
    """ERIC liefert Autoren als Liste 'Nachname, Vorname'. Bis 5 + et al."""
    if not authors:
        return "Unbekannt"
    if isinstance(authors, str):
        authors = [authors]
    if not isinstance(authors, list):
        return "Unbekannt"
    cleaned = [_clean(a) for a in authors if a]
    cleaned = [a for a in cleaned if a]
    if not cleaned:
        return "Unbekannt"
    if len(cleaned) <= 5:
        return " / ".join(cleaned)
    return f"{' / '.join(cleaned[:5])} et al."


def _format_pub_type(ptypes) -> str:
    if not ptypes:
        return ""
    if isinstance(ptypes, str):
        ptypes = [ptypes]
    if not isinstance(ptypes, list):
        return ""
    return ", ".join(_clean(p) for p in ptypes if p)[:120]


def _format_doc(doc: dict) -> dict | None:
    """Forme einen ERIC-doc in das Evidora-Result-Schema um."""
    if not isinstance(doc, dict):
        return None

    eric_id = _clean(doc.get("id"))
    title = _clean(doc.get("title"))
    if not eric_id or not title:
        return None

    description_raw = _clean(doc.get("description"))
    authors_str = _format_authors(doc.get("author"))
    year_raw = doc.get("publicationdateyear")
    year = str(year_raw) if year_raw else "—"

    peer = doc.get("peerreviewed")
    # ERIC kodiert peer_reviewed als "T"/"F".
    is_peer = str(peer).upper() == "T" if peer is not None else False
    peer_str = "Peer-Reviewed" if is_peer else "nicht peer-reviewed"

    pub_type = _format_pub_type(doc.get("publicationtype"))
    pub_source = _clean(doc.get("source"))

    # URL: bevorzugt ERIC-Permalink (immer vorhanden); Original-DOI als Sekundaer.
    eric_url = f"https://eric.ed.gov/?id={eric_id}"
    original_url = _clean(doc.get("url"))

    # display_value: Kurz-Abstract ~200 Zeichen.
    abstract_short = (
        description_raw[:200] + "…"
        if len(description_raw) > 200 else description_raw
    )
    if not abstract_short:
        abstract_short = f"{authors_str} ({year}). {title}."

    # description: ausfuehrlicher Kontext mit Autoren + Typ + Peer-Status.
    desc_parts = [f"Autoren: {authors_str}"]
    if pub_type:
        desc_parts.append(f"Typ: {pub_type}")
    if pub_source:
        desc_parts.append(f"Publikation: {pub_source}")
    desc_parts.append(peer_str)
    description = ". ".join(desc_parts) + "."

    return {
        "indicator_name": (f"ERIC {eric_id}: {title}")[:300],
        "indicator": f"eric_{eric_id.lower()}",
        "country": "INT",
        "country_name": "International (US-Education-DB)",
        "year": year,
        "value": None,
        "display_value": abstract_short,
        "description": description,
        "url": eric_url,
        "secondary_url": original_url or None,
        "source": "ERIC (US Institute of Education Sciences)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_eric(analysis: dict) -> dict:
    """Live-Lookup gegen ERIC fuer Bildungsforschungs-Claims.

    Returns Dict im Standard-Evidora-Format mit bis zu MAX_RESULTS Treffern.
    Liefert leeres Result-Set, wenn Trigger nicht greift oder API leer.

    Politische Guardrails: Service zitiert nur Studien-Metadaten, keine
    eigene Verdict-Bewertung. Synthesizer-Layer entscheidet.
    """
    empty = {
        "source": "ERIC",
        "type": "education_research",
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
    if not _claim_mentions_eric(matchable):
        return empty

    query = _build_eric_query(claim or original, analysis)
    if not query:
        logger.debug("ERIC: keine brauchbare Query aus Claim ableitbar")
        return empty

    peer_only = _should_force_peer_review(matchable)

    docs = await _fetch_eric(query, peer_only)
    if not docs:
        # Fallback ohne Peer-Filter, falls peer-only zu eng war.
        if peer_only:
            docs = await _fetch_eric(query, False)
        if not docs:
            logger.info(
                f"ERIC: 0 Treffer fuer '{query[:40]}' (peer={peer_only})"
            )
            return empty

    results: list[dict] = []
    for doc in docs[:MAX_RESULTS]:
        try:
            r = _format_doc(doc)
        except Exception as e:
            logger.debug(f"ERIC: doc-format-error: {e}")
            continue
        if r:
            results.append(r)

    if not results:
        return empty

    logger.info(
        f"ERIC: {len(results)} Treffer fuer '{query[:40]}' "
        f"(peer-only={peer_only})"
    )
    return {
        "source": "ERIC",
        "type": "education_research",
        "results": results,
    }
