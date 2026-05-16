"""Getty Vocabularies Live-Connector — Kunsthistorische Terminologie via SPARQL.

Die Getty Vocabularies (J. Paul Getty Trust, Los Angeles) sind die de-facto
Referenz-Thesauri im Bereich Kunstgeschichte, Architektur, Konservierung:

- AAT (Art & Architecture Thesaurus): ~370.000 Begriffe für Stile,
  Materialien, Techniken, Objekttypen ("Barock", "Aquarell", "Kupferstich").
- TGN (Thesaurus of Geographic Names): ~2 Mio. historische +
  zeitgenössische Ortsnamen mit Hierarchie (Kontinent → Land → Region).
- ULAN (Union List of Artist Names): ~370.000 Künstler-Identitäten mit
  Lebensdaten, Wirkungsorten, Rollen-Bezeichnungen.

Komplementär zu existierenden Quellen:
- Wikipedia (#21): unstrukturierte Lead-Extracts.
- Wikidata: Personen-Lebensdaten/Politik (sehr breit).
- GETTY: spezialisierte Kunstterminologie + historische Geographie +
  präzise Künstler-IDs. Personen-Claims dürfen Getty + Wikidata parallel
  feuern (komplementär).

API: http://vocab.getty.edu/sparql.json (SPARQL-Endpoint, JSON-Response)
- Free, kein Key, polite User-Agent erforderlich
- ODC-By 1.0 (Open Data Commons Attribution)
- Response-Header: Link: <…by/1.0/>; rel="license"
- 30 s default Timeout — wir limitieren auf 20 s

Strategie: Label-Search-Pattern (skos:prefLabel + skos:scopeNote), max
3 Ergebnisse, Sprachen DE-bevorzugt, EN-Fallback. Bei mehreren Trigger-
Termen wird der erste plausible (≥3 Zeichen, im Trigger-Set ODER ≥1
extrahierte Entity) als Query-Term verwendet.

Wiring: NICHT in AUTHORITATIVE_INDICATORS. Cross-Cluster mit Wikidata:
KEIN Hard-Skip — beide Quellen dürfen parallel feuern.

Politische Guardrails: Reine Terminologie/Hierarchie-Daten. Keine
politische Bewertung. Bei Politik-Claims (Partei-Bewertung) NICHT triggern
— die Trigger-Liste enthält keinen einzigen politisch-normativen Term.

# WIRING für main.py:
# from services.getty import search_getty, claim_mentions_getty_cached
# if claim_mentions_getty_cached(claim):
#     tasks.append(cached("Getty Vocabularies", search_getty, analysis))
#     queried_names.append("Getty Vocabularies")
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from functools import lru_cache

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

GETTY_SPARQL_URL = "http://vocab.getty.edu/sparql.json"
GETTY_AAT_PREFIX = "http://vocab.getty.edu/aat/"
GETTY_TGN_PREFIX = "http://vocab.getty.edu/tgn/"
GETTY_ULAN_PREFIX = "http://vocab.getty.edu/ulan/"

# Polite User-Agent — Getty bevorzugt identifizierte Clients
_GETTY_HEADERS = {
    "User-Agent": "Evidora/1.0 (https://evidora.eu)",
    "Accept": "application/sparql-results+json",
}

SPARQL_TIMEOUT_S = 20.0
SPARQL_RESULT_LIMIT = 3

# In-Memory-Cache: key → (timestamp, result-dict)
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 24 * 3600.0  # 24 h


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Explizite Quelle-Mentions (immer triggern)
_EXPLICIT_TERMS = (
    "getty", "getty vocabularies", "getty vocabulary",
    "aat", "art and architecture thesaurus",
    "tgn", "thesaurus of geographic names",
    "ulan", "union list of artist names",
)

# Kunst-Stile / Epochen (DE + EN)
_ART_STYLES = (
    "barock", "baroque",
    "rokoko", "rococo",
    "klassizismus", "neoclassicism",
    "romantik", "romanticism",
    "biedermeier",
    "historismus", "historicism",
    "jugendstil", "art nouveau",
    "expressionismus", "expressionism",
    "impressionismus", "impressionism",
    "kubismus", "cubism",
    "surrealismus", "surrealism",
    "dadaismus", "dadaism",
    "renaissance",
    "gotik", "gothic",
    "romanik", "romanesque",
    "manierismus", "mannerism",
    "art déco", "art deco",
    "bauhaus",
    "wiener werkstätte",
    "wiener secession",
    "secessionsstil",
)

# Material / Technik
_MATERIAL_TECHNIK = (
    "kupferstich", "copperplate engraving",
    "radierung", "etching",
    "lithografie", "lithography", "lithographie",
    "holzschnitt", "woodcut",
    "aquarell", "watercolor", "watercolour",
    "ölgemälde", "oil painting",
    "fresko", "fresco",
    "tempera",
    "bronze-plastik", "bronze sculpture", "bronzeplastik",
    "marmor-skulptur", "marble sculpture",
    "tafelbild",
    "altarretabel", "altar retable",
    "triptychon", "triptych",
    "miniaturmalerei", "miniature painting",
    "tachismus", "informel",
)

# Künstler-Lebensdaten-Trigger (komplementär zu Wikidata)
_ARTIST_TRIGGERS = (
    "maler", "malerin", "painter",
    "bildhauer", "bildhauerin", "sculptor",
    "kupferstecher", "engraver",
    "radierer", "etcher",
    "zeichner", "zeichnerin", "draftsman",
    "kunsthistoriker", "kunsthistorikerin",
    "architekt", "architektin", "architect",
    "kunstwerk", "kunstwerke", "artwork",
)

# Historische Geographie
_HIST_GEO = (
    "antike stadt", "antike städte",
    "alter ortsname", "alte ortsnamen",
    "historischer ortsname",
    "historische region",
    "römische stadt",
    "griechische polis",
    "byzantinische stadt",
)


def _has_any(claim_lc: str, terms: tuple[str, ...]) -> bool:
    return any(t in claim_lc for t in terms)


def _claim_mentions_getty(claim_lc: str) -> bool:
    """Trigger-Check für Getty-Lookup.

    True bei:
    - Expliziter Mention ("Getty", "AAT", "TGN", "ULAN")
    - Kunst-Stil (Barock, Impressionismus …)
    - Material/Technik (Kupferstich, Aquarell …)
    - Künstler-Trigger + Lebensdaten/Werk-Frage
    - Historischer Ortsname (antike Stadt …)
    """
    if not claim_lc:
        return False

    if _has_any(claim_lc, _EXPLICIT_TERMS):
        return True
    if _has_any(claim_lc, _ART_STYLES):
        return True
    if _has_any(claim_lc, _MATERIAL_TECHNIK):
        return True
    if _has_any(claim_lc, _HIST_GEO):
        return True

    # Composite: Künstler-Trigger + Lebensdaten-/Werk-Frage
    has_artist = _has_any(claim_lc, _ARTIST_TRIGGERS)
    has_life_q = any(t in claim_lc for t in (
        "wann lebte", "wann geboren", "wann gestorben",
        "lebensdaten", "geburtsjahr", "todesjahr",
        "werke von", "werk von", "schuf",
        "gemalt von", "gemalt", "gezeichnet von",
    ))
    if has_artist and has_life_q:
        return True

    return False


@lru_cache(maxsize=2048)
def claim_mentions_getty_cached(claim: str) -> bool:
    """LRU-gecachter Trigger-Check (Hot-Path-friendly)."""
    return _claim_mentions_getty((claim or "").lower())


# ---------------------------------------------------------------------------
# SPARQL helpers
# ---------------------------------------------------------------------------
def _escape_sparql_literal(label: str) -> str:
    """SPARQL-Literal-Escaping — verhindert Query-Breakage bei
    Anführungszeichen / Backslashes."""
    return label.replace("\\", "\\\\").replace('"', '\\"')


def _build_aat_label_query(term: str) -> str:
    """Label-Search im AAT mit scope-note (pflicht: skos:scopeNote vorhanden,
    sonst zu viel Rauschen)."""
    safe = _escape_sparql_literal(term.lower())
    return f"""
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?subject ?label ?note WHERE {{
  ?subject skos:prefLabel ?label ;
           skos:scopeNote/rdf:value ?note .
  FILTER(CONTAINS(LCASE(STR(?label)), "{safe}"))
  FILTER(LANG(?label) = "de" || LANG(?label) = "en")
  FILTER(LANG(?note) = "de" || LANG(?note) = "en")
  FILTER(STRSTARTS(STR(?subject), "{GETTY_AAT_PREFIX}"))
}} LIMIT {SPARQL_RESULT_LIMIT}
""".strip()


# ---------------------------------------------------------------------------
# Term-extraction
# ---------------------------------------------------------------------------
# Pool aller "lookup-würdigen" Begriffe (für Term-Extraktion aus Claim/Entities)
_LOOKUP_TERMS: tuple[str, ...] = (
    _ART_STYLES + _MATERIAL_TECHNIK
)


def _extract_query_term(claim: str, analysis: dict) -> str | None:
    """Wähle den am besten passenden Begriff für den SPARQL-Lookup.

    Priorität:
    1. Substring-Match auf bekannten Kunst-/Material-Begriffen (im
       Claim-Text) — diese Begriffe haben sicher AAT-Einträge.
    2. Erste Entity aus analysis.entities mit ≥3 Zeichen — Fallback für
       Künstler-Namen / historische Ortsnamen.
    """
    if not claim:
        return None
    claim_lc = claim.lower()
    for term in _LOOKUP_TERMS:
        if term in claim_lc:
            return term

    entities = (analysis or {}).get("entities") or []
    for e in entities:
        if isinstance(e, str) and len(e.strip()) >= 3:
            return e.strip()
    return None


# ---------------------------------------------------------------------------
# SPARQL execution
# ---------------------------------------------------------------------------
async def _run_sparql(client, query: str) -> list[dict] | None:
    """Führt SPARQL-Query gegen Getty aus. Returns bindings-Liste oder None."""
    try:
        resp = await client.get(
            GETTY_SPARQL_URL,
            params={"query": query},
            follow_redirects=True,
        )
        if resp.status_code != 200:
            logger.debug(
                f"Getty SPARQL HTTP {resp.status_code} "
                f"(body: {resp.text[:120]!r})"
            )
            return None
        data = resp.json()
        return data.get("results", {}).get("bindings", []) or []
    except Exception as e:
        logger.debug(f"Getty SPARQL fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Result building
# ---------------------------------------------------------------------------
_AAT_ID_RE = re.compile(r"/aat/(\d+)")


def _aat_id_from_uri(uri: str) -> str | None:
    if not uri:
        return None
    m = _AAT_ID_RE.search(uri)
    return m.group(1) if m else None


def _trim(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


def _build_result_row(row: dict) -> dict | None:
    def v(key: str) -> str:
        cell = row.get(key) or {}
        return (cell.get("value") or "").strip()

    subject = v("subject")
    label = v("label")
    note = v("note")
    aat_id = _aat_id_from_uri(subject)
    if not (subject and label and aat_id):
        return None

    display_value = _trim(note or label, 280)
    description = _trim(
        note or "Getty AAT-Eintrag (Art & Architecture Thesaurus).",
        500,
    )

    return {
        "indicator_name": f"AAT: {label}",
        "indicator": f"getty_aat_{aat_id}",
        "country": "—",
        "country_name": "—",
        "year": "",
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": subject,
        "source": "Getty Vocabularies (ODC-By 1.0)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_getty(analysis: dict) -> dict:
    """Live-Lookup gegen Getty AAT für Kunst-/Material-/Stil-Begriffe.

    Returns Dict mit ≤3 strukturierten Terminologie-Treffern. Bei
    Trigger-Miss / 0 Treffern / API-Fehler: leere results-Liste.
    """
    empty = {
        "source": "Getty Vocabularies",
        "type": "art_terminology",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_getty(matchable):
        return empty

    term = _extract_query_term(matchable, analysis)
    if not term:
        return empty

    cache_key = f"aat::{term.lower()}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0] < _CACHE_TTL_S):
        logger.info(f"Getty: Cache-Hit für '{term[:40]}'")
        return cached[1]

    query = _build_aat_label_query(term)

    async with polite_client(
        timeout=SPARQL_TIMEOUT_S, headers=_GETTY_HEADERS
    ) as client:
        try:
            rows = await asyncio.wait_for(
                _run_sparql(client, query),
                timeout=SPARQL_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.info(f"Getty: SPARQL-Timeout für '{term[:40]}'")
            return empty

    if not rows:
        logger.info(f"Getty: 0 Treffer für '{term[:40]}'")
        _CACHE[cache_key] = (now, empty)
        return empty

    results: list[dict] = []
    seen_ids: set[str] = set()
    for row in rows[:SPARQL_RESULT_LIMIT]:
        try:
            built = _build_result_row(row)
        except Exception as e:
            logger.debug(f"Getty: Format-Fehler bei row: {e}")
            continue
        if not built:
            continue
        ind = built.get("indicator", "")
        if ind in seen_ids:
            continue
        seen_ids.add(ind)
        results.append(built)

    out = {
        "source": "Getty Vocabularies",
        "type": "art_terminology",
        "results": results,
    }
    _CACHE[cache_key] = (now, out)
    if results:
        logger.info(
            f"Getty: {len(results)} AAT-Treffer für '{term[:40]}'"
        )
    return out
