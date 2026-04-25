"""RIS — Rechtsinformationssystem des Bundes.

Datenquelle: BKA Open-Data API für das Rechtsinformationssystem
(`data.bka.gv.at/ris/api/v2.6/Bundesrecht`). Liefert Bundesgesetzblatt-
Kundmachungen (BGBl) — die offiziellen Veröffentlichungsereignisse
österreichischer Bundesgesetze, Verordnungen und Staatsverträge.

Response-Format pro Treffer:
- Kurztitel + Volltitel des Gesetzes/der Verordnung
- BGBl-Nummer + Ausgabedatum
- ELI-URL (European Legislation Identifier — stabile, zitierbare URL)
- Verlinkung auf authentisches PDF (ris.bka.gv.at/Dokumente/...)

Lizenz: PSI/OGD, Attribution erforderlich (Quelle: RIS, BKA).

Triggering: nur bei AT-Kontext + Legal-Keyword + extrahierbarer
Suchterm. Verhindert Treffer-Rauschen aus den ~18.600 Bundesnormen
bei Claims, die nur das Wort „Gesetz" generisch enthalten.

Caveat:
- Die API liefert BGBl-Kundmachungen, NICHT die konsolidierte aktuelle
  Fassung. Mehrere BGBl-Einträge pro Gesetz (Stammgesetz +
  Novellierungen) sind die Regel.
- Volltext steckt nicht in der API-Response — nur Metadaten + Link
  zum authentischen PDF/HTML.
- Treffer = das BGBl hat das Gesetz veröffentlicht. Politische
  Bewertungen („gerecht", „verfassungswidrig") sind NICHT Inhalt der
  RIS-Daten.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren Gesetzes-Existenz und -Veröffentlichungsdaten,
  bewerten keine Inhalte.
- Wir machen keine Aussagen über Verfassungs-/Gesetzeskonformität.
- Wir prognostizieren keine Gesetzesänderungen.
"""

import logging
import re
import time
from urllib.parse import urlencode

import httpx

logger = logging.getLogger("evidora")

API_BASE = "https://data.bka.gv.at/ris/api/v2.6/Bundesrecht"
CACHE_TTL = 3600  # 1h für Query-Cache (RIS aktualisiert wöchentlich)

# Per-Query-Cache: {query_string: (timestamp, results)}
_query_cache: dict[str, tuple[float, list[dict]]] = {}

# Legal-Domain-Keywords — claims, die mit höherer Wahrscheinlichkeit
# einen Gesetzesbezug haben. Englische Pendants für mehrsprachige
# Behauptungen ergänzt.
LEGAL_KEYWORDS = [
    # Gesetzes-Vokabular
    "gesetz", "gesetze", "gesetzlich", "law", "laws", "legal",
    "bundesgesetz", "bundesgesetzblatt", "bgbl",
    "verordnung", "verordnungen", "regulation", "regulations",
    "richtlinie", "directive",
    "staatsvertrag", "treaty",
    # Verfassungs-Vokabular
    "verfassung", "verfassungs", "constitution", "constitutional",
    "grundrecht", "grundrechte", "fundamental right",
    "menschenrecht", "menschenrechte", "human rights",
    # Strafrecht / Zivilrecht
    "strafrecht", "criminal law", "strafbar",
    "zivilrecht", "civil law",
    "verwaltungsrecht", "administrative law",
    # Konkrete Bezüge
    "paragraph", "paragraf", "§", "artikel ", "article ",
    "kundmachung", "novellierung", "novelle",
    "beschlossen", "verabschiedet", "kundgemacht", "in kraft",
    "passed", "enacted", "ratified",
    # Konkrete AT-Gesetze (Häufige Erwähnungen)
    "b-vg", "abgb", "stgb", "stpo", "avg", "egvg", "asylg", "fpg",
    "klimaschutzgesetz", "informationsfreiheitsgesetz",
    "mietrecht", "mietrechtsgesetz",
]

# Stop-Tokens für die Such-Term-Extraktion (zu generisch für RIS-Suche)
STOP_TOKENS = {
    "österreich", "austria", "deutschland", "germany", "europa", "europe",
    "frage", "behauptung", "aussage", "studie", "bericht", "jahr", "jahre",
    "zeit", "tag", "tage", "stunde", "stunden", "person", "personen",
    "land", "länder", "staat", "staaten", "regierung", "minister",
    "gesetz", "gesetze", "verordnung", "richtlinie",  # zu generisch
    "law", "laws", "regulation", "directive",
    "the", "and", "der", "die", "das", "den", "dem", "des",
    "ein", "eine", "einen", "einer", "eines",
    "und", "oder", "aber", "sondern",
}

# Maximale Treffer pro Suche, die wir an den Synthesizer weitergeben.
# Synthesizer kappt aktuell auf 5 Items pro Quelle, plus 1 Caveat-Block.
MAX_RESULTS = 4


def _claim_mentions_legal(claim: str) -> bool:
    """Check if claim mentions legal-domain keywords."""
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in LEGAL_KEYWORDS)


def _claim_mentions_austria(analysis: dict) -> bool:
    """Heuristic: claim has Austria context (or RIS-specific signal)."""
    claim_lower = analysis.get("claim", "").lower()
    if any(t in claim_lower for t in ("österreich", "austria", "ris", "bgbl",
                                     "b-vg", "abgb", "stgb", "stpo")):
        return True
    countries = analysis.get("ner_entities", {}).get("countries", [])
    return any("österreich" in c.lower() or "austria" in c.lower() for c in countries)


def _extract_search_terms(analysis: dict) -> list[str]:
    """Pull law-name candidates and specific terms from the analysis.

    Strategy (most specific first):
    1. Capitalized compound nouns ending in -gesetz/-verordnung/-recht/-ordnung
       (z.B. „Klimaschutzgesetz", „Wahlrechtsgesetz")
    2. Acronym patterns matching AT law abbreviations (B-VG, ABGB, ...)
    3. LLM-extracted entities + SpaCy noun chunks, filtered to non-stop tokens
    """
    claim = analysis.get("claim", "")
    terms: list[str] = []
    seen: set[str] = set()

    def add(t: str):
        cleaned = t.strip().strip(".,;:!?\"'„“”")
        if cleaned and cleaned.lower() not in seen and len(cleaned) >= 3:
            terms.append(cleaned)
            seen.add(cleaned.lower())

    # 1. Compound nouns ending in legal suffixes (German compounds are ideal)
    legal_suffixes = (
        "gesetz", "gesetze", "gesetzes",
        "verordnung", "verordnungen",
        "ordnung", "ordnungen",
        "vertrag", "verträge",
        "abkommen",
        "novelle", "novellierung",
    )
    # Match capitalized German words that contain a legal-suffix substring
    pattern = re.compile(
        r"\b[A-ZÄÖÜ][a-zA-ZäöüÄÖÜß0-9-]*(?:" + "|".join(legal_suffixes) + r")\w*\b"
    )
    for m in pattern.finditer(claim):
        add(m.group(0))

    # 2. AT-specific law abbreviations (mixed-case acronyms with hyphens)
    abbrev_pattern = re.compile(r"\b(?:B-VG|ABGB|StGB|StPO|AVG|EGVG|AsylG|FPG|GewO|EStG|UStG|ASVG|VfGG|VwGG|MRG|WRG|UrhG)\b")
    for m in abbrev_pattern.finditer(claim):
        add(m.group(0))

    # 3. § Paragraph-Verweise — nicht selbst ein Suchterm, aber starker Indikator
    # für Legal-Kontext. Gibt nur den umgebenden Token weiter.
    para_pattern = re.compile(r"§\s*\d+[a-zA-Z]?")
    for m in para_pattern.finditer(claim):
        add(m.group(0))

    # 4. LLM-extracted entities filtered to non-stop tokens (kürzeres Fallback)
    if len(terms) < 3:
        for ent in analysis.get("entities", []) or []:
            if isinstance(ent, str) and ent.lower() not in STOP_TOKENS and len(ent) >= 4:
                add(ent)
                if len(terms) >= 5:
                    break

    return terms[:5]  # cap to 5 terms — RIS-Queries sollen fokussiert bleiben


async def _query_ris(client: httpx.AsyncClient, params: dict) -> list[dict]:
    """Single RIS API call with the given query params."""
    try:
        resp = await client.get(
            f"{API_BASE}?{urlencode(params)}",
            headers={"Accept": "application/json"},
            timeout=15.0,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logger.warning(f"RIS: query {params} failed: {e}")
        return []
    docs = payload.get("OgdSearchResult", {}).get("OgdDocumentResults", {})
    refs = docs.get("OgdDocumentReference", []) or []
    if isinstance(refs, dict):
        refs = [refs]
    return refs


async def _search_ris_for_term(client: httpx.AsyncClient, term: str) -> list[dict]:
    """Query RIS for one term, preferring Titel over Suchworte for precision.

    Strategy:
    - `Titel=<term>` filtert nur das Titel-Feld der BGBl-Einträge — sehr
      präzise für konkrete Gesetzesnamen ("Klimaschutzgesetz",
      "Informationsfreiheitsgesetz", "B-VG").
    - `Suchworte=<term>` ist die Volltext-Suche, die auch in Anlagen +
      ausführlichen Texten matcht — viel rauschiger (z.B. liefert
      "Klimaschutzgesetz" 13 Hits, davon 4 Bundesfinanzgesetze, weil
      diese in ihren Anlagen das Wort enthalten).
    - Paragraph-Verweise wie "§ 7" stehen nicht im Titel — für sie ist
      nur Suchworte sinnvoll.
    """
    cache_key = term.lower().strip()
    now = time.time()
    cached = _query_cache.get(cache_key)
    if cached and (now - cached[0]) < CACHE_TTL:
        return cached[1]

    base_params = {
        "Seitengroesse": "5",
        "Seitennummer": "1",
        "Sortierung.SortDirection": "Descending",
    }

    refs: list[dict] = []
    # 1. Paragraph-Verweise → direkt Suchworte (Titel würde nie matchen)
    if term.lstrip().startswith("§"):
        refs = await _query_ris(client, {**base_params, "Suchworte": term})
    else:
        # 2. Default: Titel-Filter
        refs = await _query_ris(client, {**base_params, "Titel": term})
        # 3. Fallback: Suchworte, falls Titel-Filter leer ausging
        if not refs:
            refs = await _query_ris(client, {**base_params, "Suchworte": term})

    _query_cache[cache_key] = (now, refs)
    return refs


def _format_ris_entry(ref: dict, search_term: str) -> dict | None:
    """Convert a raw OgdDocumentReference into a synthesizer-compatible result."""
    data = ref.get("Data", {}).get("Metadaten", {})
    bg = data.get("Bundesrecht", {}) or {}
    allg = data.get("Allgemein", {}) or {}

    kurztitel = bg.get("Kurztitel") or "(ohne Kurztitel)"
    titel = allg.get("Titel") or bg.get("Titel") or kurztitel
    eli_url = bg.get("Eli") or allg.get("DokumentUrl")

    bgbl_meta = bg.get("BgblAuth") or bg.get("Bgbl") or {}
    bgbl_nr = bgbl_meta.get("Bgblnummer") or ""
    ausgabe = bgbl_meta.get("Ausgabedatum") or ""
    typ = bgbl_meta.get("Typ") or ""

    if not eli_url:
        return None

    # Build a compact title — Kurztitel + BGBl + Ausgabedatum
    title_parts = [kurztitel]
    if bgbl_nr:
        title_parts.append(bgbl_nr)
    if ausgabe:
        title_parts.append(f"Ausgabe {ausgabe}")
    headline = " — ".join(title_parts)

    # Build description: full Titel (often very long, truncate)
    desc_parts: list[str] = []
    if titel and titel != kurztitel:
        desc_parts.append(titel[:400])
    if typ:
        desc_parts.append(f"Typ: {typ}")
    desc = " | ".join(desc_parts) or "—"

    return {
        "indicator_name": f"RIS: {headline}",
        "indicator": "ris_bgbl",
        "country": "AUT",
        "country_name": "Austria",
        "year": ausgabe[:4] if ausgabe else "",
        "value": "",
        "display_value": "",
        "url": eli_url,
        "description": desc[:500],
        "search_term": search_term,  # for downstream debugging only
    }


async def search_ris(analysis: dict) -> dict:
    """Search RIS Bundesrecht for laws/regulations relevant to the claim.

    Triggers when claim has both AT context and a legal-domain keyword and
    we can extract at least one specific search term.
    """
    claim = analysis.get("claim", "")
    if not (_claim_mentions_legal(claim) and _claim_mentions_austria(analysis)):
        return {"source": "RIS — Rechtsinformationssystem", "type": "official_data", "results": []}

    terms = _extract_search_terms(analysis)
    if not terms:
        return {"source": "RIS — Rechtsinformationssystem", "type": "official_data", "results": []}

    async with httpx.AsyncClient(timeout=15.0) as client:
        all_refs_by_term: list[tuple[str, list[dict]]] = []
        for term in terms:
            refs = await _search_ris_for_term(client, term)
            if refs:
                all_refs_by_term.append((term, refs))

    if not all_refs_by_term:
        logger.info(f"RIS: no hits for terms={terms}")
        return {"source": "RIS — Rechtsinformationssystem", "type": "official_data", "results": []}

    # Aggregate + dedupe by ELI URL — same law often appears for multiple
    # search terms (e.g., search for both "B-VG" and "Bundesverfassungsgesetz").
    seen_urls: set[str] = set()
    results: list[dict] = []
    for term, refs in all_refs_by_term:
        for ref in refs:
            entry = _format_ris_entry(ref, term)
            if entry and entry["url"] not in seen_urls:
                seen_urls.add(entry["url"])
                # Strip internal field before emitting
                entry.pop("search_term", None)
                results.append(entry)
                if len(results) >= MAX_RESULTS:
                    break
        if len(results) >= MAX_RESULTS:
            break

    if not results:
        return {"source": "RIS — Rechtsinformationssystem", "type": "official_data", "results": []}

    # Caveat
    results.append({
        "indicator_name": "WICHTIGER KONTEXT: RIS-Daten",
        "indicator": "context",
        "country": "AUT",
        "country_name": "Austria",
        "year": "",
        "value": "",
        "display_value": "",
        "url": "https://www.ris.bka.gv.at/",
        "description": (
            "Das Rechtsinformationssystem des Bundes (RIS) ist die offizielle "
            "Datenbank des Bundeskanzleramts für österreichische Rechtsvorschriften. "
            "Einschränkungen: "
            "(1) Die API liefert BGBl-Kundmachungen — also die Veröffentlichungs"
            "ereignisse im Bundesgesetzblatt, NICHT die konsolidierte aktuelle "
            "Fassung. Ein Gesetz hat typischerweise mehrere BGBl-Einträge "
            "(Stammgesetz + Novellierungen). "
            "(2) Treffer = das BGBl hat das Gesetz veröffentlicht. RIS bewertet "
            "nicht, ob ein Gesetz politisch sinnvoll, verfassungskonform oder "
            "noch in Kraft ist — diese Einschätzungen sind getrennten Quellen "
            "(VfGH-Erkenntnisse, juristische Kommentare) vorbehalten. "
            "(3) Volltext steckt nicht in der API-Antwort — nur Metadaten und "
            "ein Link auf das authentische PDF/HTML auf ris.bka.gv.at. "
            "(4) Die Suche sucht im BGBl, nicht in den konsolidierten "
            "Normabschnitten — eine Suche nach „§ 7 B-VG“ liefert daher "
            "Kundmachungen, die das B-VG ändern, nicht den aktuellen Wortlaut "
            "des Paragraphen."
        ),
    })

    logger.info(f"RIS: {len(results) - 1} unique laws/regulations, terms={terms}")
    return {"source": "RIS — Rechtsinformationssystem", "type": "official_data", "results": results}
