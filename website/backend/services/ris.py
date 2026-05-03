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
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

API_BASE = "https://data.bka.gv.at/ris/api/v2.6/Bundesrecht"
GELTENDE_FASSUNG_BASE = (
    "https://www.ris.bka.gv.at/GeltendeFassung.wxe"
    "?Abfrage=Bundesnormen&Gesetzesnummer={nr}"
)
CACHE_TTL = 3600  # 1h für Query-Cache (RIS aktualisiert wöchentlich)

# Per-Query-Cache: {query_string: (timestamp, results)}
_query_cache: dict[str, tuple[float, list[dict]]] = {}

# Häufig zitierte AT-Gesetze mit ihrer RIS-Gesetzesnummer + Anzeige-Name.
# Verifiziert via Live-Probe gegen GeltendeFassung-URL (2026-04-26).
# Erweiterbar — bei „Bug G"-Treffern Wertepaare ergänzen.
LAW_REGISTRY: dict[str, tuple[str, str]] = {
    "b-vg":   ("10000138", "Bundes-Verfassungsgesetz (B-VG)"),
    "abgb":   ("10001622", "Allgemeines Bürgerliches Gesetzbuch (ABGB)"),
    "stgb":   ("10002296", "Strafgesetzbuch (StGB)"),
    "stpo":   ("10002326", "Strafprozessordnung (StPO)"),
    "mrg":    ("10002531", "Mietrechtsgesetz (MRG)"),
    "asvg":   ("10008147", "Allgemeines Sozialversicherungsgesetz (ASVG)"),
    "avg":    ("10005768", "Allgemeines Verwaltungsverfahrensgesetz (AVG)"),
    "asylg":  ("20004240", "Asylgesetz 2005 (AsylG)"),
    "fpg":    ("20004241", "Fremdenpolizeigesetz 2005 (FPG)"),
    "gewo":   ("10007517", "Gewerbeordnung 1994 (GewO)"),
    "estg":   ("10004570", "Einkommensteuergesetz 1988 (EStG)"),
    "ustg":   ("10004873", "Umsatzsteuergesetz 1994 (UStG)"),
    "urhg":   ("10001848", "Urheberrechtsgesetz (UrhG)"),
    "vfgg":   ("10000245", "Verfassungsgerichtshofgesetz (VfGG)"),
    # Bug U: ORF-Beitragsgesetz 2024 (BGBl I 112/2023, in Kraft 1.1.2024).
    # Die Volltext-Suche im RIS findet das Stammgesetz selbst nicht
    # zuverlässig (Suchworte "ORF-Beitragsgesetz" → 0 Hits, "ORF-Beitrag"
    # → 6 Hits aber Novellen/Verweise statt des Stammgesetzes), daher
    # zusätzlicher Direktlink zur konsolidierten Fassung über die
    # GeltendeFassung-Nummer.
    "orfbeitrg":  ("20008302", "ORF-Beitragsgesetz 2024 (ORF-BeitrG)"),
    "orf-beitrg": ("20008302", "ORF-Beitragsgesetz 2024 (ORF-BeitrG)"),
    # ----------------------------------------------------------------
    # Schulrecht (Stamm-Bundesgesetze, verifiziert 2026-04-26)
    # ----------------------------------------------------------------
    # SchUG ist DAS Lehrer-Dauerthema: Leistungsbeurteilung,
    # Aufsteigen, Sitzenbleiben, Ordnungsmaßnahmen, Suspendierung,
    # Fernbleiben/Erlaubnis, häusliche Übungen.
    "schug":     ("10009600", "Schulunterrichtsgesetz (SchUG)"),
    # Schultypen, Schulorganisation, Lehrpläne, Klassenschülerzahl.
    "schog":     ("10009265", "Schulorganisationsgesetz (SchOG)"),
    # Allgemeine Schulpflicht (9 Jahre), häuslicher Unterricht,
    # Externistenprüfung, Schulreife.
    "schpflg":   ("10009576", "Schulpflichtgesetz 1985 (SchPflG)"),
    # Schuljahr-Beginn/Ende, Ferienordnung, autonome Tage,
    # Unterrichtszeit pro Woche.
    "schzg":     ("10009575", "Schulzeitgesetz 1985 (SchZG)"),
    # Bildungsdirektionen seit 2019 — Nachfolger des B-SchAufsG;
    # regelt Schulaufsicht, Direktor:innen-Bestellung,
    # Bildungsdirektor:in.
    "bd-eg":     ("20009982", "Bildungsdirektionen-Einrichtungsgesetz (BD-EG)"),
    "bdeg":      ("20009982", "Bildungsdirektionen-Einrichtungsgesetz (BD-EG)"),
    # Pädagogische Hochschulen (Lehrerausbildung).
    "hg":        ("20011522", "Hochschulgesetz 2005 (HG)"),
    "hochschulg": ("20011522", "Hochschulgesetz 2005 (HG)"),
    # ----------------------------------------------------------------
    # Lehrer-Dienstrecht in voller Breite (verifiziert 2026-04-26)
    # ----------------------------------------------------------------
    # Pragmatisierte Pflichtschul-Lehrkräfte (Landeslehrer:innen).
    "ldg":       ("10008549", "Landeslehrer-Dienstrechtsgesetz 1984 (LDG)"),
    # Pragmatisierte Bundes-Lehrkräfte (AHS, BHS) — Beamten-Dienstrecht.
    "bdg":       ("10008470", "Beamten-Dienstrechtsgesetz 1979 (BDG)"),
    # Vertragsbedienstete im Bundesbereich (incl. AHS/BHS-VB,
    # ehem. „IIL"-Schema, neues pd-Schema).
    "vbg":       ("10008115", "Vertragsbedienstetengesetz 1948 (VBG)"),
    # Land- und forstwirtschaftliche Landesvertrags-Lehrpersonen
    # (LFS, Berufsschulen für Land-/Forstwirtschaft).
    "llvg":      ("10008235", "Land- und forstwirtschaftliches Landesvertragslehrpersonengesetz (LLVG)"),
    # Besoldung (Gehaltsstufen, Vorrückungen, Zulagen).
    "gehg":      ("10008163", "Gehaltsgesetz 1956 (GehG)"),
    # Pension der Bundesbeamt:innen (Ruhegenuss, Versorgungsbezüge).
    "pg":        ("10008210", "Pensionsgesetz 1965 (PG)"),
    "pg1965":    ("10008210", "Pensionsgesetz 1965 (PG)"),
}

# Verfassungsgesetze nutzen Artikel-Nummerierung (Art.) statt Paragraph (§).
# Bei §-Verweis auf B-VG/StGG fügen wir einen entsprechenden Hinweis hinzu.
CONSTITUTIONAL_LAWS = {"b-vg", "stgg"}

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
    # Bug R: Abgaben / Beiträge — AT-Bundesabgaben sind RIS-relevant
    # (z.B. ORF-Beitragsgesetz BGBl I 112/2023)
    "abgabe", "abgaben",
    "haushaltsabgabe", "rundfunkabgabe",
    "beitrag", "beiträge", "beitragsgesetz",
    "steuer", "steuern", "tax",
    # Bug S: Kanzler-Bestellung & Regierungsbildung — RIS B-VG Art. 70 etc.
    "regierungsbildung", "kanzleramt",
    "ernennung", "angelobung",
    # Wahlrecht (NR-WO, EU-WO etc.)
    "wahlrecht", "wahlordnung", "wahlgesetz",
    # Schul- und Lehrer-Dienstrecht — typische Lehrer-Topics, die in
    # Faktencheck-Anfragen ohne explizites "Gesetz" auftauchen.
    "schug", "schog", "schpflg", "schzg", "bd-eg", "ldg", "bdg", "vbg",
    "llvg", "gehg",
    "schulpflicht", "schulpflichtig",
    "sitzenbleiben", "klassenwiederholung",
    "leistungsbeurteilung", "schulnoten", "schulnote",
    "nicht genügend", "nicht genuegend",
    "sitzenzubleiben", "sitzen zu bleiben",  # LLM-Infinitiv-Variante
    "lehrplan", "lehrpläne",
    "schulferien", "autonome tage", "schulautonomer tag",
    "schulbesuch", "fernbleiben vom unterricht",
    "ordnungsmaßnahme", "ordnungsmassnahme", "schulordnung",
    "klassenvorstand", "supplierung", "mehrdienstleistung",
    "bildungsdirektion", "bildungsdirektor", "bildungsdirektorin",
    "schulaufsicht", "schulinspektor",
    "vertragslehrer", "vertragslehrerin", "landeslehrer",
    "pd-schema", "pädagogisches dienstrecht",
    "lehrergehalt", "lehrer-gehalt", "lehrergehälter",
    "anfangsgehalt", "gehaltsstufe", "vorrückung",
    "gehalt eines lehrers", "gehalt einer lehrerin",
    "verdienen lehrer", "verdienen lehrerinnen",
    "verdient ein lehrer", "verdient eine lehrerin",
    "lehrerausbildung", "lehrerinnenausbildung", "lehramtsstudium",
    "pädagogische hochschule",
    "häuslicher unterricht", "externistenprüfung",
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


# Bug R: AT-spezifische Marker, die eindeutig Österreich kennzeichnen.
# „ORF" ist eindeutig AT (deutsche/schweizer Rundfunkanstalten heißen
# anders). „Volkskanzler" und „freiheitlicher Kanzler" sind FPÖ-/AT-
# Begriffe.  „Klubobmann/Klubobfrau" ist AT-parlamentarische Sprache.
_AT_HARD_MARKERS = (
    "österreich", "austria", "österreichisch",
    "ris", "bgbl", "b-vg",
    "abgb", "stgb", "stpo", "asvg", "mrg", "asylg", "fpg",
    # Schul- & Lehrer-Dienstrecht: eindeutig AT-spezifische Acronyme
    # (DE hat keine SchUG/SchOG/SchPflG/SchZG, kein BD-EG, kein LLVG;
    # BDG/VBG/GehG/PG mit AT-Jahreszahlen). Diese zählen als AT-Kontext,
    # damit Claims wie „§ 14 SchOG regelt …" auch ohne explizites
    # „Österreich" RIS triggern.
    "schug", "schog", "schpflg", "schzg", "bd-eg", "bdeg", "llvg",
    "schulunterrichtsgesetz", "schulorganisationsgesetz",
    "schulpflichtgesetz", "schulzeitgesetz",
    "bildungsdirektionen-einrichtungsgesetz",
    "landeslehrer-dienstrechtsgesetz",
    "bdg 1979", "vbg 1948", "gehg 1956", "pg 1965",
    "pensionsgesetz 1965", "vertragsbedienstetengesetz 1948",
    "orf",  # AT-only Rundfunk
    "volkskanzler", "freiheitlicher kanzler", "freiheitliche kanzler",
    "nationalrat", "bundesrat",  # AT-Parlament (DE/CH-Bundesrat in
    # diesen Claims selten, aber wenn DE-Marker auch da → unten geblockt)
    "klubobmann", "klubobfrau", "klubobleute",
)

# Bug S: Politische System-Tokens, die in AT *und* DE auftreten —
# nur als AT zählen, wenn KEIN DE-Marker im Claim ist.
_AT_SOFT_TOKENS = (
    "kanzler", "bundeskanzler", "bundespräsident", "bundespraesident",
    "kanzleramt", "regierungsbildung", "regierungsbildner",
)

# Wenn diese DE-spezifischen Marker im Claim auftauchen, ist es eine
# Deutschland-Aussage — kein AT-RIS-Trigger.
_DE_MARKERS = (
    "grundgesetz", " gg ", " gg.", "art. 63 gg", "artikel 63 gg",
    "bundestag", "bundespräsidialamt",
    "bundesrepublik deutschland", "deutsche bundeskanzlerin",
    "deutscher bundeskanzler", "deutsche kanzlerin", "deutscher kanzler",
    "berlin",
)


def _claim_mentions_austria(analysis: dict) -> bool:
    """Heuristic: claim has Austria context (or RIS-specific signal).

    Three-stage check:
    1. Hard exclude: explicit DE-Marker → not AT.
    2. Hard include: explicit AT-Marker (Österreich, ORF, B-VG, etc.).
    3. Soft include: political-system tokens (Kanzler, Bundeskanzler,
       Bundespräsident) when no DE-Marker — Evidora is AT-focused, so
       in absence of a DE-cue we default to AT.
    4. NER fallback: if NER detected Österreich/Austria.
    """
    claim_lower = analysis.get("claim", "").lower()

    # 1. DE-Marker ⇒ nicht AT
    if any(de in claim_lower for de in _DE_MARKERS):
        return False

    # 2. Hard AT-Marker
    if any(at in claim_lower for at in _AT_HARD_MARKERS):
        return True

    # 3. Soft AT-Marker (politisches System ohne DE-Cue)
    if any(soft in claim_lower for soft in _AT_SOFT_TOKENS):
        return True

    # 4. NER-Fallback
    countries = analysis.get("ner_entities", {}).get("countries", [])
    return any("österreich" in c.lower() or "austria" in c.lower() for c in countries)


# Bug R: Volkstümliche Bezeichnungen vs. offizielle RIS-Termini.
# Wenn der Claim einen umgangssprachlichen Begriff enthält, ergänzen wir
# automatisch den offiziellen Gesetzes-Suchterm.
_TERM_SYNONYMS: dict[str, tuple[str, ...]] = {
    "haushaltsabgabe": ("ORF-Beitrag", "ORF-Beitragsgesetz"),
    "orf-haushaltsabgabe": ("ORF-Beitrag", "ORF-Beitragsgesetz"),
    "rundfunkabgabe": ("ORF-Beitrag", "ORF-Beitragsgesetz"),
    "rundfunkgebühr": ("ORF-Beitrag", "ORF-Beitragsgesetz"),
    "orf-zwangsgebühr": ("ORF-Beitrag", "ORF-Beitragsgesetz"),
    "orf-zwangssteuer": ("ORF-Beitrag", "ORF-Beitragsgesetz"),
    "gis-gebühr": ("ORF-Beitrag", "ORF-Beitragsgesetz"),
    "gis-beitrag": ("ORF-Beitrag", "ORF-Beitragsgesetz"),
}


def _extract_search_terms(analysis: dict) -> list[str]:
    """Pull law-name candidates and specific terms from the analysis.

    Strategy (most specific first):
    1. Capitalized compound nouns ending in -gesetz/-verordnung/-recht/-ordnung
       (z.B. „Klimaschutzgesetz", „Wahlrechtsgesetz")
    2. Acronym patterns matching AT law abbreviations (B-VG, ABGB, ...)
    3. Synonym expansion (z.B. „ORF-Haushaltsabgabe" → „ORF-Beitrag")
    4. LLM-extracted entities + SpaCy noun chunks, filtered to non-stop tokens
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
        # Bug R: Abgaben/Beiträge — z.B. „ORF-Haushaltsabgabe", „Beitragsgesetz"
        "abgabe", "abgaben",
        "beitrag", "beiträge",
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

    # 3b. Bug R: Synonym-Expansion — volkstümliche Begriffe (z.B.
    #     „ORF-Haushaltsabgabe") auf den offiziellen RIS-Suchbegriff
    #     („ORF-Beitrag") mappen, damit die Volltext-Suche Treffer findet.
    cl = claim.lower()
    for alias, official_terms in _TERM_SYNONYMS.items():
        if alias in cl:
            for ot in official_terms:
                add(ot)

    # 4. LLM-extracted entities filtered to non-stop tokens (kürzeres Fallback)
    if len(terms) < 3:
        for ent in analysis.get("entities", []) or []:
            if isinstance(ent, str) and ent.lower() not in STOP_TOKENS and len(ent) >= 4:
                add(ent)
                if len(terms) >= 5:
                    break

    return terms[:5]  # cap to 5 terms — RIS-Queries sollen fokussiert bleiben


def _extract_law_paragraph_refs(claim: str) -> list[tuple[str, str, str]]:
    """Find (paragraph_or_article_label, law_key_lower, law_display) triples.

    Bug G fix: claims that mention a specific paragraph AND a known AT law
    abbreviation deserve a direct link to the law's consolidated current
    version (GeltendeFassung) instead of relying on the BGBl-Suchworte
    fallback, which produces irrelevant results for paragraph-style queries.

    Pattern matches: "§ 7 B-VG", "§7 B-VG", "Art. 7 B-VG",
    "Artikel 15a B-VG" — paragraph/article reference followed by a known
    law abbreviation. Multiple matches are deduplicated.
    """
    refs: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    pattern = re.compile(
        r"(?P<kind>§|Art\.?|Artikel)\s*(?P<num>\d+[a-zA-Z]?)\s+"
        r"(?P<law>[A-ZÄÖÜ][\w\-\.]{1,15})",
        re.UNICODE,
    )
    for m in pattern.finditer(claim):
        law_norm = m.group("law").lower().rstrip(".").strip()
        if law_norm not in LAW_REGISTRY:
            continue
        kind_raw = m.group("kind").lower()
        kind = "Art." if kind_raw.startswith("a") else "§"
        ref_str = f"{kind} {m.group('num')}"
        key = (ref_str, law_norm)
        if key in seen:
            continue
        seen.add(key)
        refs.append((ref_str, law_norm, LAW_REGISTRY[law_norm][1]))
    return refs


# Bug S: Topic-basierte Direktlinks zu konsolidierten Gesetzen.
# Wenn der Claim kein "§ N B-VG"-Pattern enthält, aber thematisch klar
# auf ein bestimmtes Gesetz verweist (z.B. „Kanzler-Bestellung" → B-VG,
# „Asylverfahren" → AsylG), fügen wir einen GeltendeFassung-Direktlink
# als zusätzliches Hilfeangebot hinzu.
#
# Das Mapping ist konservativ — nur für Themen, die *eindeutig* einem
# Gesetz zuzuordnen sind. Bei Mehrdeutigkeit (z.B. „Steuer" → EStG,
# UStG, KStG, etc.) lassen wir die Volltext-Suche das Routing machen.
_TOPIC_TO_LAW: list[tuple[tuple[str, ...], str, str]] = [
    # (any-of-topic-keywords, law_key, friendly description for the entry)
    (
        ("kanzler-bestellung", "kanzlerbestellung", "ernennung des bundeskanzlers",
         "bundeskanzler ernennen", "regierungsbildung",
         # weichere Politik-Topics, die B-VG-Verfahren betreffen
         "stärkste partei", "stimmenstärkste partei",
         "wer wird kanzler", "kanzler stellen",
         "verfassung setzt", "verfassungs ", "verfassungsmäßig"),
        "b-vg",
        "Bestellung des Bundeskanzlers (Art. 70 B-VG)",
    ),
    (
        ("bundespräsident wahl", "bundespräsidentenwahl",
         "amtsantritt bundespräsident", "amtszeit bundespräsident",
         "wahl des bundespräsidenten"),
        "b-vg",
        "Wahl des Bundespräsidenten (Art. 60 B-VG)",
    ),
    (
        ("nationalrat zusammensetzung", "183 abgeordnete",
         "gesetzgebungsperiode", "legislaturperiode"),
        "b-vg",
        "Nationalrat (Art. 24 ff. B-VG)",
    ),
    (
        ("asylverfahren", "asylantrag", "asylbescheid",
         "asylgesuch", "subsidiärer schutz"),
        "asylg",
        "Asylgesetz 2005 (AsylG) — Verfahren und Voraussetzungen",
    ),
    (
        ("abschiebung", "schubhaft", "rückkehrentscheidung", "fremdenrecht"),
        "fpg",
        "Fremdenpolizeigesetz 2005 (FPG) — Aufenthaltsbeendigung",
    ),
    (
        ("mietzins", "mietzinsbeschränkung", "richtwertmietzins",
         "kategoriemietzins", "befristete miete"),
        "mrg",
        "Mietrechtsgesetz (MRG) — Mietzins und Mietverhältnisse",
    ),
    # Bug U: ORF-Beitragsgesetz — die Volltext-Suche findet das
    # Stammgesetz nicht zuverlässig.  Direktlink zur konsolidierten
    # Fassung als ergänzendes Hilfeangebot.
    (
        ("orf-haushaltsabgabe", "orf-beitrag", "orf-beitragsgesetz",
         "orf haushaltsabgabe", "orf-zwangsgebühr", "orf zwangsgebühr",
         "orf-zwangssteuer", "rundfunkabgabe", "rundfunkbeitrag",
         "gis-gebühr", "gis-beitrag"),
        "orfbeitrg",
        "ORF-Beitragsgesetz 2024 (BGBl I 112/2023) — "
        "Beitragshöhe, Pflichtige und Verfahren",
    ),
    # ----------------------------------------------------------------
    # Schulrecht — Topic-Mappings (Lehrer-Klassiker-Themen)
    # ----------------------------------------------------------------
    # SchUG: Leistungsbeurteilung, Aufsteigen, Sitzenbleiben,
    # Verhalten, Schulbesuch / Fernbleiben, Ordnungsmaßnahmen.
    (
        ("sitzenbleiben", "sitzen bleiben",
         # LLM-Analyzer paraphrasiert teilweise zu Infinitiv-mit-zu
         "sitzenzubleiben", "sitzen zu bleiben",
         "klassenwiederholung",
         "wiederholen einer schulstufe", "nicht aufsteigen",
         "leistungsbeurteilung", "noten", "notengebung",
         "nicht genügend", "nicht genuegend",
         "fünfer", "fuenfer", "schulnoten",
         "frühwarnsystem", "frühwarn",
         "ordnungsmaßnahme", "ordnungsmassnahme",
         "verhaltensvereinbarung", "schulordnung",
         "suspendierung von schülern", "schüler suspendieren",
         "schulbesuch", "fernbleiben vom unterricht",
         "schulpflichtverletzung", "schulpflicht-verletzung",
         "klassenforum", "schulforum", "schulgemeinschaftsausschuss",
         "schulgemeinschaftsausschuß", "sga",
         "mitwirkung schule", "elternverein",
         "hausübung", "hausaufgabe", "hausaufgaben",
         "schularbeit", "schularbeiten"),
        "schug",
        "Schulunterrichtsgesetz (SchUG) — Leistungsbeurteilung, "
        "Verhalten, Schulgemeinschaft",
    ),
    # SchOG: Schultypen, Klassengrößen, Lehrpläne, Schulstruktur.
    (
        ("schultyp", "schultypen", "klassenschülerzahl",
         "klassenschülerzahlen", "höchstzahl klasse",
         "klassengröße", "klassengroesse",
         "lehrplan", "lehrpläne",
         "neue mittelschule", "mittelschule strukturreform",
         "ahs aufnahme", "ahs-aufnahme",
         "schulstruktur", "gesamtschule",
         "polytechnische schule", "berufsschule struktur",
         "sonderpädagogischer förderbedarf", "spf",
         "inklusionsklasse", "integrationsklasse"),
        "schog",
        "Schulorganisationsgesetz (SchOG) — Schultypen, "
        "Klassengrößen und Lehrpläne",
    ),
    # SchPflG: 9-jährige Schulpflicht, häuslicher Unterricht,
    # Externistenprüfung.
    (
        ("schulpflicht", "schulpflichtig", "schulpflichtgesetz",
         "9 jahre schule", "neun jahre schulpflicht",
         "häuslicher unterricht", "häuslicher schulunterricht",
         "homeschooling österreich",
         "externistenprüfung", "externistenpruefung",
         "schulreife", "vorzeitige einschulung",
         "schulische rückstellung", "schulische rueckstellung"),
        "schpflg",
        "Schulpflichtgesetz 1985 (SchPflG) — Allgemeine Schulpflicht "
        "und häuslicher Unterricht",
    ),
    # SchZG: Ferienordnung, autonome Tage, Schultage.
    (
        ("schulferien", "schulzeit", "schulzeitgesetz",
         "sommerferien", "semesterferien", "weihnachtsferien",
         "osterferien", "pfingstferien", "energieferien",
         "autonome tage", "autonomer tag", "schulautonomer tag",
         "schulfreie tage", "ferienordnung",
         "unterrichtszeit pro woche", "schulwoche",
         "5-tage-woche schule", "fünf-tage-woche schule"),
        "schzg",
        "Schulzeitgesetz 1985 (SchZG) — Schuljahr, Ferien "
        "und Unterrichtszeit",
    ),
    # BD-EG: Schulaufsicht, Bildungsdirektion, Direktor:innen-Bestellung.
    (
        ("bildungsdirektion", "bildungsdirektor", "bildungsdirektorin",
         "schulaufsicht", "schulinspektor", "schulinspektorin",
         "schulqualitätsmanager", "schulqualitätsmanagerin",
         "qualitätsmanager schule", "qm schule",
         "direktor:in bestellung", "direktorinnen-bestellung",
         "schulleiter bestellung", "schulleiterin bestellung",
         "leitungsfunktion schule"),
        "bd-eg",
        "Bildungsdirektionen-Einrichtungsgesetz (BD-EG) — Schulaufsicht "
        "und Schulleitungs-Bestellung",
    ),
    # Hochschulgesetz: Pädagogische Hochschulen (Lehrerausbildung).
    (
        ("pädagogische hochschule", "pädagogische hochschulen",
         "paedagogische hochschule", "ph wien",
         "lehrerausbildung", "lehrerinnenausbildung",
         "lehramtsstudium", "primarstufe ausbildung",
         "sekundarstufe ausbildung", "ph studium"),
        "hg",
        "Hochschulgesetz 2005 (HG) — Pädagogische Hochschulen "
        "und Lehrer:innen-Ausbildung",
    ),
    # ----------------------------------------------------------------
    # Lehrer-Dienstrecht — Topic-Mappings
    # ----------------------------------------------------------------
    # LDG: Pflichtschul-pragmatisierte Lehrkräfte (Landeslehrer).
    (
        ("landeslehrer", "landeslehrerin", "landeslehrkraft",
         "pflichtschul-lehrer", "pflichtschullehrer",
         "pflichtschullehrkraft",
         "ldg 1984", "ldg-1984", "landeslehrer-dienstrecht",
         "landeslehrerin-dienstrecht",
         "pragmatisierter pflichtschullehrer",
         "pragmatisierte pflichtschullehrerin",
         "pragmatisiert pflichtschule"),
        "ldg",
        "Landeslehrer-Dienstrechtsgesetz 1984 (LDG) — "
        "Pragmatisierte Pflichtschul-Lehrkräfte",
    ),
    # BDG: Pragmatisierte Bundeslehrer (AHS, BHS).
    (
        ("bdg 1979", "bdg-1979", "beamten-dienstrecht",
         "beamtendienstrecht",
         "pragmatisierter bundeslehrer", "pragmatisierte bundeslehrerin",
         "pragmatisierte ahs", "pragmatisierte bhs",
         "ahs-lehrer pragmatisiert", "bhs-lehrer pragmatisiert",
         "definitivstellung lehrer",
         "ruhestandsversetzung", "amtstitel hofrat",
         "professor:in"),
        "bdg",
        "Beamten-Dienstrechtsgesetz 1979 (BDG) — Pragmatisierte "
        "Bundeslehrer:innen (AHS/BHS)",
    ),
    # VBG: Vertragsbedienstete (incl. neues pd-Schema).
    (
        ("vertragsbedienstete", "vertragslehrer", "vertragslehrerin",
         "vbg 1948", "vbg-1948",
         "pd-schema", "pd schema", "pädagogisches dienstrecht",
         "paedagogisches dienstrecht",
         "neues lehrerdienstrecht", "neues dienstrecht 2013",
         "vertraglich angestellte lehrerin", "vertraglich angestellter lehrer",
         "iil-schema", "iil schema",
         "kündigung lehrer", "befristung lehrervertrag"),
        "vbg",
        "Vertragsbedienstetengesetz 1948 (VBG) — Vertragslehrkräfte "
        "und pädagogisches Dienstrecht (pd-Schema)",
    ),
    # LLVG: Land- und forstwirtschaftliche Berufsschulen.
    (
        ("land- und forstwirtschaftliche schule",
         "landwirtschaftliche fachschule", "land- und forstwirtschaft schule",
         "lfs lehrer", "lfs-lehrer",
         "agrarpädagogik schule",
         "llvg"),
        "llvg",
        "Land- und forstwirtschaftliches Landesvertragslehrpersonengesetz "
        "(LLVG) — Lehrkräfte an LFS",
    ),
    # GehG: Lehrer-Besoldung, Vorrückungen, Zulagen.
    (
        ("gehaltsgesetz", "lehrergehalt", "lehrer-gehalt",
         "lehrergehälter", "lehrer-gehälter",
         "besoldung lehrer", "besoldung lehrerin",
         "vorrückung lehrer", "lehrer-vorrückung",
         "biennalsprünge", "biennalsprung",
         "zulage lehrer", "lehrerzulage",
         "leistungsprämie schule",
         "mehrdienstleistung", "mdl-vergütung", "mdl vergütung",
         "supplierung", "supplierungs-vergütung",
         "klassenvorstand", "kv-zulage",
         "lehrer-anfangsgehalt", "anfangsgehalt lehrer",
         # Häufige Frage-Phrasings ("Wie hoch ist das Anfangsgehalt
         # für Lehrer in Österreich?")
         "anfangsgehalt für lehrer", "anfangsgehalt für lehrerin",
         "gehalt für lehrer", "gehalt für lehrerin",
         "gehalt eines lehrers", "gehalt einer lehrerin",
         "verdient ein lehrer", "verdient eine lehrerin",
         "verdienen lehrer", "verdienen lehrerinnen",
         "lehrer verdienen", "lehrerin verdient",
         "wie viel lehrer verdienen"),
        "gehg",
        "Gehaltsgesetz 1956 (GehG) — Lehrer:innen-Besoldung, "
        "Vorrückungen, Zulagen",
    ),
    # PG: Beamten-Pension (für pragmatisierte Lehrer).
    (
        ("pensionsgesetz 1965", "pensionsgesetz1965",
         "ruhegenuss lehrer", "lehrer-pension beamtenrecht",
         "pension beamteter lehrer", "pension pragmatisierter lehrer",
         "frühpensionierung lehrer", "korridor-pension lehrer",
         "hacklerregelung lehrer", "schwerarbeitspension lehrer"),
        "pg",
        "Pensionsgesetz 1965 (PG) — Pension der pragmatisierten "
        "Lehrkräfte",
    ),
]


def _extract_topic_law_refs(claim: str) -> list[tuple[str, str]]:
    """Match claim text against _TOPIC_TO_LAW patterns.

    Returns list of (law_key, description) tuples for laws whose topics
    are mentioned in the claim. Used as fallback when no `§ N <Gesetz>`
    pattern is present.
    """
    cl = claim.lower()
    refs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for keywords, law_key, description in _TOPIC_TO_LAW:
        if any(kw in cl for kw in keywords):
            if law_key in seen:
                continue
            seen.add(law_key)
            refs.append((law_key, description))
    return refs


def _format_topic_law_entry(law_key: str, description: str) -> dict:
    """Build a synthesizer-compatible entry pointing at the consolidated
    current law text for a topic-matched claim (no § reference)."""
    if law_key not in LAW_REGISTRY:
        return {}
    gnr, display = LAW_REGISTRY[law_key]
    url = GELTENDE_FASSUNG_BASE.format(nr=gnr)
    return {
        "indicator_name": (
            f"RIS GeltendeFassung: {display} — "
            f"thematisch einschlägig"
        ),
        "indicator": "ris_geltende_fassung_topic",
        "country": "AUT",
        "country_name": "Austria",
        "year": "",
        "value": "",
        "display_value": "",
        "url": url,
        "description": (
            f"{description}. Direktlink zur konsolidierten aktuellen "
            f"Fassung des Gesetzes in der RIS-Datenbank Bundesnormen — "
            f"die Verfahrensvorschriften und Definitionen sind dort "
            f"vollständig nachzulesen."
        ),
    }


def _format_law_section_entry(
    ref_str: str, law_key: str, display: str
) -> dict:
    """Build a synthesizer-compatible entry that points at the consolidated
    current law text. Pure metadata link — the actual paragraph wording is
    on the linked RIS page (which is too large to fetch into context).
    """
    gnr, _ = LAW_REGISTRY[law_key]
    url = GELTENDE_FASSUNG_BASE.format(nr=gnr)

    description_parts = [
        f"Direktlink zur konsolidierten aktuellen Fassung des Gesetzes "
        f"in der RIS-Datenbank Bundesnormen. Der Wortlaut ist auf der "
        f"verlinkten Seite im Abschnitt {ref_str} einsehbar."
    ]
    # Hinweis: bei §-Verweis auf ein Verfassungsgesetz auf die korrekte
    # Artikel-Nummerierung hinweisen (Volkssprachgebrauch verwechselt das oft).
    if ref_str.startswith("§") and law_key in CONSTITUTIONAL_LAWS:
        # Extrahiere die Nummer (z.B. "7" aus "§ 7") und die kurze
        # Gesetzes-Abkürzung (z.B. "B-VG" aus "Bundes-Verfassungsgesetz (B-VG)").
        num = ref_str.split(" ", 1)[1] if " " in ref_str else ref_str.lstrip("§").strip()
        short_abbr = display.split("(")[-1].rstrip(")") if "(" in display else display
        description_parts.append(
            f"Hinweis: {display} verwendet Artikel-Nummerierung — "
            f"die Behauptung sollte korrekt „Art. {num} {short_abbr}\" zitieren, "
            f"nicht „§ {num} {short_abbr}\". Inhaltlich ist Art. {num} {short_abbr} gemeint."
        )

    return {
        "indicator_name": (
            f"RIS GeltendeFassung: {display} — {ref_str} "
            f"(konsolidierte aktuelle Fassung)"
        ),
        "indicator": "ris_geltende_fassung",
        "country": "AUT",
        "country_name": "Austria",
        "year": "",
        "value": "",
        "display_value": "",
        "url": url,
        "description": " ".join(description_parts),
    }


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
    # Bug Z: Der LLM-Analyzer paraphrasiert manche Inflektionsformen
    # weg (z.B. „sitzenbleiben" → „sitzenzubleiben"-Infinitiv,
    # „häuslicher Unterricht" → „häuslichen Unterricht"-Akkusativ).
    # Das bricht Substring-basiertes Topic-Matching.  Lösung: Topic-
    # und Section-Refs gegen die VEREINIGUNG aus User-Original und
    # LLM-Normalisierung matchen — wir kennen beide.
    original_claim = analysis.get("original_claim") or claim
    matchable = f"{original_claim} {claim}".strip()

    if not (_claim_mentions_legal(matchable) and _claim_mentions_austria(analysis)):
        return {"source": "RIS — Rechtsinformationssystem", "type": "official_data", "results": []}

    terms = _extract_search_terms(analysis)
    # Bug G: Paragraph-Direktlinks haben Vorrang. Auch wenn `terms` leer ist,
    # ein erkannter „§ N <Gesetz>"-Verweis lohnt eine Antwort (Direktlink zur
    # konsolidierten Fassung), daher prüfen wir Section-Refs zuerst.
    section_refs = _extract_law_paragraph_refs(matchable)
    # Bug S: Topic-Refs als zusätzlicher Pfad — Claims wie "Kanzler-Bestellung
    # nach der Verfassung" haben keinen "§ N B-VG"-Verweis, sind aber
    # eindeutig B-VG-relevant.
    topic_refs = _extract_topic_law_refs(matchable)

    if not terms and not section_refs and not topic_refs:
        return {"source": "RIS — Rechtsinformationssystem", "type": "official_data", "results": []}

    results: list[dict] = []
    seen_urls: set[str] = set()

    # 1. Section-Refs (§ N <Gesetz> oder Art. N <Gesetz>) zuerst — die sind
    #    die präzisesten Antworten für paragraphische Behauptungen.
    for ref_str, law_key, display in section_refs[:3]:
        entry = _format_law_section_entry(ref_str, law_key, display)
        if entry["url"] not in seen_urls:
            seen_urls.add(entry["url"])
            results.append(entry)

    # 1b. Topic-Refs als zusätzliche, themenspezifische Direktlinks.
    #     Werden NACH Section-Refs gerendert, weil weniger präzise.
    for law_key, description in topic_refs[:2]:
        entry = _format_topic_law_entry(law_key, description)
        if entry and entry["url"] not in seen_urls:
            seen_urls.add(entry["url"])
            results.append(entry)

    # 2. BGBl-Treffer als Ergänzung (auch wenn Section-Refs schon vorhanden,
    #    da Novellen + Stammgesetz zusätzlichen Kontext liefern).
    if terms:
        async with polite_client(timeout=15.0) as client:
            all_refs_by_term: list[tuple[str, list[dict]]] = []
            for term in terms:
                refs = await _search_ris_for_term(client, term)
                if refs:
                    all_refs_by_term.append((term, refs))

        for term, refs in all_refs_by_term:
            for ref in refs:
                entry = _format_ris_entry(ref, term)
                if entry and entry["url"] not in seen_urls:
                    seen_urls.add(entry["url"])
                    entry.pop("search_term", None)
                    results.append(entry)
                    if len(results) >= MAX_RESULTS:
                        break
            if len(results) >= MAX_RESULTS:
                break

    if not results:
        logger.info(f"RIS: no hits for terms={terms}, section_refs={section_refs}")
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
