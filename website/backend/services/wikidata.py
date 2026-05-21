"""Wikidata Live-Connector — Strukturierte Fakten via SPARQL.

Wikidata ist die maschinenlesbare Schwester von Wikipedia: jede Aussage
ist als Triple (Subjekt-Prädikat-Objekt) modelliert. Für Faktencheck-
Zwecke liefert sie:
- Personen-Lebensdaten (Geburt/Tod/Beruf/Partei)
- Politiker-Amtszeiten (Bundeskanzler von-bis, Präsident von-bis)
- Geographische Fakten (Hauptstadt, Einwohner, höchster Berg, längster Fluss)
- Organisations-Gründungsjahre
- Werk-Zuordnungen (Autor:in von Buch, Regie von Film)

Komplementär zu existierenden Quellen:
- Wikipedia (#21): unstrukturierte Lead-Extracts — gut für Kontext
- WIKIDATA: strukturierte Triples — gut für *präzise* Fakt-Verifikation
- GDELT: aktuelle News-Coverage
- Static-First-Packs: kuratierte Konsens-Daten

API: https://query.wikidata.org/sparql (SPARQL-Endpoint, JSON-Response)
- Free, kein Key
- Polite User-Agent + mailto-Field gibt Priorität (siehe _http_polite.py)
- 60s Timeout pro Query (wir limitieren auf 20s)
- Rate-Limit ~5 concurrent — bei Burst evtl. 429

Strategie: KURATIERTE SPARQL-Templates, kein free-form NLP-to-SPARQL.
~10 hardcoded Templates für die häufigsten Faktencheck-Patterns. Bei
Match auf Pattern-Trigger Template ausführen mit extrahierten Entitäten.

Trigger: claim_text matcht einen der Pattern-Trigger UND hat ≥1 Entity.

Wiring: NICHT in AUTHORITATIVE_INDICATORS — ist Live-Quelle, keine
kuratierte Konsens-DB. main.py imports + tasks.append + reranker
Indicator-Whitelist-Marker.

Limitations:
- SPARQL-Templates decken ~50 % der "wer/wann/wo"-Fragen ab
- Person-Label-Match strict (genaue Schreibweise nötig — kein Fuzzy)
- Komplexere Aggregations-Queries (z. B. "alle Bundeskanzler seit 1945")
  können timeouten — wir limiteren strikt auf LIMIT 5
- Wikidata-Daten manchmal lückenhaft (besonders ältere Ereignisse,
  kleinere Länder, lokale Politiker)
- Max 1 Template pro Claim (vermeidet 5+ SPARQL-Queries pile-up)
"""

import asyncio
import logging
import re
import time
from datetime import date as _date
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/{qid}"
WIKIDATA_ENTITY_DATA_URL = (
    "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
)

# In-Memory-Cache: entity-key → (timestamp, result)
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 3600.0  # 1h

# Maximale SPARQL-Wartezeit (Wikidata erlaubt 60 s, wir bleiben höflich)
SPARQL_TIMEOUT_S = 20.0
SPARQL_RESULT_LIMIT = 5


# ---------------------------------------------------------------------------
# SPARQL-Template-Registry
# ---------------------------------------------------------------------------
#
# Jedes Template hat:
#   - "name":      menschenlesbarer Bezeichner (für Logging/Result)
#   - "triggers":  Liste Lower-Case-Substrings im Claim
#   - "regex":     optional, wenn präzisere Entity-Extraktion möglich
#   - "sparql":    SPARQL-Query mit `{name}`-Placeholder
#   - "format":    Funktion, die Wikidata-JSON-Result-Row → display_string
#
# Wir verwenden in der Query rdfs:label statt skos:altLabel um *exakte*
# Hauptbezeichnungen zu treffen — Fuzzy-Match ist mit SPARQL teuer und
# führt zu Timeouts.

_TEMPLATES: list[dict] = [
    {
        "name": "person_lebensdaten",
        "triggers": [
            "geboren", "geburtsdatum", "geburtsjahr",
            "gestorben", "verstarb", "todesdatum", "todesjahr",
            "lebte von",
        ],
        "sparql": """
SELECT ?person ?personLabel ?birth ?birthPlaceLabel ?death ?deathPlaceLabel
       (GROUP_CONCAT(DISTINCT ?occLabel; separator=", ") AS ?occupations)
WHERE {{
  ?person rdfs:label "{name}"@de.
  ?person wdt:P31 wd:Q5.
  OPTIONAL {{ ?person wdt:P569 ?birth. }}
  OPTIONAL {{ ?person wdt:P19 ?birthPlace. }}
  OPTIONAL {{ ?person wdt:P570 ?death. }}
  OPTIONAL {{ ?person wdt:P20 ?deathPlace. }}
  OPTIONAL {{
    ?person wdt:P106 ?occ.
    ?occ rdfs:label ?occLabel. FILTER(LANG(?occLabel) = "de")
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
GROUP BY ?person ?personLabel ?birth ?birthPlaceLabel
         ?death ?deathPlaceLabel
LIMIT 5
""",
    },
    {
        "name": "politiker_amtszeit",
        "triggers": [
            "bundeskanzler", "bundeskanzlerin", "kanzler",
            "präsident", "präsidentin", "bundespräsident",
            "ministerpräsident", "ministerpräsidentin",
            "premierminister", "premierministerin",
            "amtszeit",
        ],
        "sparql": """
SELECT ?person ?personLabel ?positionLabel ?start ?end ?partyLabel
WHERE {{
  ?person rdfs:label "{name}"@de.
  ?person wdt:P31 wd:Q5.
  ?person p:P39 ?statement.
  ?statement ps:P39 ?position.
  OPTIONAL {{ ?statement pq:P580 ?start. }}
  OPTIONAL {{ ?statement pq:P582 ?end. }}
  OPTIONAL {{ ?person wdt:P102 ?party. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
ORDER BY DESC(?start)
LIMIT 5
""",
    },
    {
        "name": "land_hauptstadt",
        "triggers": ["hauptstadt"],
        "sparql": """
SELECT ?country ?countryLabel ?capital ?capitalLabel ?since
WHERE {{
  ?country rdfs:label "{name}"@de.
  ?country wdt:P31/wdt:P279* wd:Q6256.
  ?country p:P36 ?statement.
  ?statement ps:P36 ?capital.
  OPTIONAL {{ ?statement pq:P580 ?since. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
LIMIT 5
""",
    },
    {
        "name": "land_bevoelkerung",
        "triggers": [
            "einwohner", "einwohnerzahl", "bevölkerung",
        ],
        "sparql": """
SELECT ?country ?countryLabel ?population ?date
WHERE {{
  ?country rdfs:label "{name}"@de.
  ?country wdt:P31/wdt:P279* wd:Q6256.
  ?country p:P1082 ?statement.
  ?statement ps:P1082 ?population.
  OPTIONAL {{ ?statement pq:P585 ?date. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
ORDER BY DESC(?date)
LIMIT 3
""",
    },
    {
        "name": "organisation_gruendung",
        "triggers": [
            "gegründet", "gründung", "gründungsjahr",
            "founded", "etabliert",
        ],
        "sparql": """
SELECT ?org ?orgLabel ?inception ?countryLabel ?founderLabel
WHERE {{
  ?org rdfs:label "{name}"@de.
  ?org wdt:P31/wdt:P279* wd:Q43229.
  OPTIONAL {{ ?org wdt:P571 ?inception. }}
  OPTIONAL {{ ?org wdt:P17 ?country. }}
  OPTIONAL {{ ?org wdt:P112 ?founder. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
LIMIT 5
""",
    },
    {
        "name": "werk_autor",
        "triggers": [
            "geschrieben", "verfasst", "autor von", "autorin von",
            "produziert", "regie", "regisseur", "regisseurin",
        ],
        "sparql": """
SELECT ?work ?workLabel ?authorLabel ?directorLabel ?published
WHERE {{
  ?work rdfs:label "{name}"@de.
  OPTIONAL {{ ?work wdt:P50 ?author. }}
  OPTIONAL {{ ?work wdt:P57 ?director. }}
  OPTIONAL {{ ?work wdt:P577 ?published. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
LIMIT 5
""",
    },
    {
        "name": "erfindung_erfinder",
        "triggers": [
            "erfunden", "erfinder", "erfinderin", "entwickelt von",
            "entdeckt", "entdecker", "entdeckerin",
        ],
        "sparql": """
SELECT ?thing ?thingLabel ?inventorLabel ?discovererLabel ?inceptionDate
WHERE {{
  ?thing rdfs:label "{name}"@de.
  OPTIONAL {{ ?thing wdt:P61 ?discoverer. }}
  OPTIONAL {{ ?thing wdt:P178 ?inventor. }}
  OPTIONAL {{ ?thing wdt:P571 ?inceptionDate. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
LIMIT 5
""",
    },
    {
        "name": "geographie_berg",
        "triggers": [
            "höchster berg", "höchste berg", "höchsten berg",
            "höchste gipfel", "höchster gipfel",
        ],
        "sparql": """
SELECT ?mountain ?mountainLabel ?elevation ?countryLabel
WHERE {{
  ?country rdfs:label "{name}"@de.
  ?country wdt:P31/wdt:P279* wd:Q6256.
  ?mountain wdt:P31/wdt:P279* wd:Q8502.
  ?mountain wdt:P17 ?country.
  ?mountain wdt:P2044 ?elevation.
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
ORDER BY DESC(?elevation)
LIMIT 3
""",
    },
    {
        "name": "geographie_fluss",
        "triggers": [
            "längster fluss", "längsten fluss",
            "längste flüsse",
        ],
        "sparql": """
SELECT ?river ?riverLabel ?length ?countryLabel
WHERE {{
  ?country rdfs:label "{name}"@de.
  ?country wdt:P31/wdt:P279* wd:Q6256.
  ?river wdt:P31/wdt:P279* wd:Q4022.
  ?river wdt:P17 ?country.
  ?river wdt:P2043 ?length.
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
ORDER BY DESC(?length)
LIMIT 3
""",
    },
    {
        "name": "person_partei",
        "triggers": [
            "partei", "parteimitglied", "parteizugehörigkeit",
            "övp", "spö", "fpö", "grüne", "neos",
        ],
        "sparql": """
SELECT ?person ?personLabel ?partyLabel ?start ?end
WHERE {{
  ?person rdfs:label "{name}"@de.
  ?person wdt:P31 wd:Q5.
  ?person p:P102 ?statement.
  ?statement ps:P102 ?party.
  OPTIONAL {{ ?statement pq:P580 ?start. }}
  OPTIONAL {{ ?statement pq:P582 ?end. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
ORDER BY DESC(?start)
LIMIT 5
""",
    },
]


def _detect_template_for_claim(
    claim: str, analysis: dict
) -> tuple[str | None, dict]:
    """Identifiziert das passende SPARQL-Template für einen Claim.

    Returns (template_name, params) — params enthält ``name`` (Entity-
    Label, das in den SPARQL-Query injiziert wird). Wenn nichts passt,
    wird (None, {}) zurückgegeben.

    Strategie:
    1. Lower-Case-Substring-Match auf Trigger-Wörter
    2. Erste passende Entity (≥3 chars) aus analysis.entities als
       SPARQL-Label nehmen
    3. Maximal *eine* Template-Auswahl pro Claim — wir nehmen das erste
       Match in Template-Listen-Reihenfolge (spezifischere zuerst)
    """
    if not claim:
        return None, {}

    claim_lc = claim.lower()
    entities = (analysis or {}).get("entities", []) or []
    entities = [e for e in entities if e and len(e) >= 3]
    if not entities:
        return None, {}

    for tmpl in _TEMPLATES:
        for trig in tmpl["triggers"]:
            if trig in claim_lc:
                # Erste plausible Entity nehmen — die SPARQL-Templates
                # sind so geschrieben, dass das Label exakt gematcht
                # werden muss
                return tmpl["name"], {"name": entities[0]}

    return None, {}


def claim_triggers_wikidata(claim: str, analysis: dict) -> bool:
    """Schnell-Check ob der Claim eine Wikidata-Lookup rechtfertigt.

    Wird ggf. von main.py vorgeschaltet, um SPARQL-Calls für nicht-
    relevante Claims zu vermeiden.
    """
    name, _ = _detect_template_for_claim(claim, analysis)
    return name is not None


def _get_template(name: str) -> dict | None:
    for tmpl in _TEMPLATES:
        if tmpl["name"] == name:
            return tmpl
    return None


def _escape_sparql_label(label: str) -> str:
    """SPARQL-Literal-Escaping — verhindert Query-Breakage bei
    Anführungszeichen / Backslashes im Entity-Label."""
    return label.replace("\\", "\\\\").replace('"', '\\"')


def _qid_from_uri(uri: str) -> str | None:
    """Extrahiert ``Qxxxxx`` aus ``http://www.wikidata.org/entity/Qxxxxx``."""
    if not uri:
        return None
    m = re.search(r"/entity/(Q\d+)", uri)
    return m.group(1) if m else None


def _date_to_year(iso_date: str | None) -> str:
    """``2021-12-06T00:00:00Z`` → ``06.12.2021`` ODER fallback ``year``."""
    if not iso_date:
        return ""
    m = re.match(r"^(-?\d{4})-(\d{2})-(\d{2})", iso_date)
    if not m:
        return iso_date[:10]
    year, month, day = m.group(1), m.group(2), m.group(3)
    if month == "01" and day == "01":
        return year
    return f"{day}.{month}.{year}"


def _is_office_term_ended(end_iso: str | None) -> bool:
    """True wenn ``end_iso`` ein vollständiges, parsbares Datum ≤ heute ist.

    Wird in ``_format_row`` für ``politiker_amtszeit`` /
    ``person_partei`` genutzt, um Stichtagsbezug-Inversionen zu verhindern
    (Pattern aus lessons_learned.md: Synthesizer-Inversions-Falle bei
    "X ist aktuell Amts-Inhaber" — Wikidata liefert end-Datum, aber LLM
    interpretiert "2010 bis 2026" als noch-amtierend, obwohl 09.05.2026
    bereits in der Vergangenheit liegt).

    Konservativ: Nur bei klar parsbarem ISO-Datum ≤ heute True.
    Bei leerem / unparsbarem ``end`` (= noch amtierend laut Wikidata) False.
    """
    if not end_iso:
        return False
    m = re.match(r"^(-?\d{4})-(\d{2})-(\d{2})", end_iso)
    if not m:
        return False
    try:
        y, mo, da = int(m.group(1)), int(m.group(2)), int(m.group(3))
        end_d = _date(y, mo, da)
    except (ValueError, TypeError):
        return False
    return end_d <= _date.today()


def _format_row(template_name: str, row: dict) -> tuple[str, str, str]:
    """Wikidata-SPARQL-Result-Row → (display_value, entity_qid, label).

    Greift auf die rohen ``{var: {value: ...}}``-Strukturen zurück.
    """
    def v(key: str) -> str:
        cell = row.get(key) or {}
        return (cell.get("value") or "").strip()

    qid = _qid_from_uri(
        v("person") or v("country") or v("org") or v("work")
        or v("thing") or v("mountain") or v("river")
    ) or ""

    if template_name == "person_lebensdaten":
        label = v("personLabel") or "Person"
        parts = []
        b = _date_to_year(v("birth"))
        if b:
            place = v("birthPlaceLabel")
            parts.append(
                f"Geboren {b}" + (f" in {place}" if place else "")
            )
        d = _date_to_year(v("death"))
        if d:
            place = v("deathPlaceLabel")
            parts.append(
                f"Gestorben {d}" + (f" in {place}" if place else "")
            )
        occ = v("occupations")
        if occ:
            parts.append(f"Beruf: {occ}")
        display = f"{label}: " + (
            ". ".join(parts) if parts else "keine Lebensdaten in Wikidata"
        )
        return display, qid, label

    if template_name == "politiker_amtszeit":
        label = v("personLabel") or "Person"
        pos = v("positionLabel") or "Amt"
        end_iso = v("end")
        start = _date_to_year(v("start"))
        end = _date_to_year(end_iso) or "heute"
        party = v("partyLabel")
        bits = [pos]
        if start:
            bits.append(f"({start} – {end})")
        if party:
            bits.append(f"[{party}]")
        body = f"{label}: " + " ".join(bits)
        # Stichtagsbezug-Schutz: Wenn end-Datum < heute, dann ist die
        # Amtszeit beendet. Synthesizer-Prompt erkennt "STRUKTURELL FALSCH:"
        # Prefix als authoritative Counter-Evidenz und korrigiert
        # Präsens-Aussagen ("X ist amtierend") zu mostly_false/false.
        # Pattern: lessons_learned.md, Synthesizer-Inversions-Falle.
        if _is_office_term_ended(end_iso):
            today_iso = _date.today().isoformat()
            return (
                f"STRUKTURELL FALSCH: {label} hatte die Position "
                f"'{pos}' nur bis {end} (heute: {today_iso}) — "
                f"laut Wikidata seitdem NICHT MEHR in dieser Funktion. "
                f"Präsens-Aussagen 'ist {pos}' / 'ist amtierender …' "
                f"sind ohne neuere Quelle nicht mehr zutreffend. "
                f"Roh-Daten: {body}",
                qid,
                label,
            )
        return body, qid, label

    if template_name == "land_hauptstadt":
        label = v("countryLabel") or "Land"
        cap = v("capitalLabel") or "?"
        since = _date_to_year(v("since"))
        text = f"Hauptstadt von {label}: {cap}"
        if since:
            text += f" (seit {since})"
        return text, qid, label

    if template_name == "land_bevoelkerung":
        label = v("countryLabel") or "Land"
        pop = v("population") or "?"
        date = _date_to_year(v("date"))
        try:
            pop_fmt = f"{int(float(pop)):,}".replace(",", ".")
        except ValueError:
            pop_fmt = pop
        text = f"{label}: {pop_fmt} Einwohner"
        if date:
            text += f" (Stand {date})"
        return text, qid, label

    if template_name == "organisation_gruendung":
        label = v("orgLabel") or "Organisation"
        inc = _date_to_year(v("inception"))
        country = v("countryLabel")
        founder = v("founderLabel")
        bits = [label]
        if inc:
            bits.append(f"gegründet {inc}")
        if country:
            bits.append(f"({country})")
        if founder:
            bits.append(f"durch {founder}")
        return ", ".join(bits), qid, label

    if template_name == "werk_autor":
        label = v("workLabel") or "Werk"
        author = v("authorLabel")
        director = v("directorLabel")
        pub = _date_to_year(v("published"))
        bits = [label]
        if author:
            bits.append(f"Autor: {author}")
        if director:
            bits.append(f"Regie: {director}")
        if pub:
            bits.append(f"erschienen {pub}")
        return ", ".join(bits), qid, label

    if template_name == "erfindung_erfinder":
        label = v("thingLabel") or "Sache"
        inventor = v("inventorLabel")
        discoverer = v("discovererLabel")
        inc = _date_to_year(v("inceptionDate"))
        bits = [label]
        if inventor:
            bits.append(f"Erfinder: {inventor}")
        if discoverer:
            bits.append(f"Entdecker: {discoverer}")
        if inc:
            bits.append(f"({inc})")
        return ", ".join(bits), qid, label

    if template_name == "geographie_berg":
        label = v("mountainLabel") or "Berg"
        elev = v("elevation")
        country = v("countryLabel")
        try:
            elev_fmt = f"{int(float(elev)):,} m".replace(",", ".")
        except ValueError:
            elev_fmt = (elev + " m") if elev else "?"
        text = f"{label} ({elev_fmt})"
        if country:
            text += f", {country}"
        return text, qid, label

    if template_name == "geographie_fluss":
        label = v("riverLabel") or "Fluss"
        length = v("length")
        country = v("countryLabel")
        try:
            length_fmt = f"{int(float(length)):,} km".replace(",", ".")
        except ValueError:
            length_fmt = (length + " km") if length else "?"
        text = f"{label} ({length_fmt})"
        if country:
            text += f", {country}"
        return text, qid, label

    if template_name == "person_partei":
        label = v("personLabel") or "Person"
        party = v("partyLabel") or "?"
        end_iso = v("end")
        start = _date_to_year(v("start"))
        end = _date_to_year(end_iso) or "heute"
        bits = [f"{label}: Partei {party}"]
        if start:
            bits.append(f"({start} – {end})")
        body = " ".join(bits)
        # Wie bei politiker_amtszeit — Stichtagsbezug-Schutz für
        # Partei-Mitgliedschafts-Claims. Bei mehrfachen Rows (LIMIT 5)
        # wird das pro Row entschieden; nur die abgeschlossene Mitgliedschaft
        # bekommt den Marker.
        if _is_office_term_ended(end_iso):
            today_iso = _date.today().isoformat()
            return (
                f"STRUKTURELL FALSCH: {label} war bei Partei "
                f"'{party}' nur bis {end} (heute: {today_iso}) — "
                f"laut Wikidata seitdem KEINE laufende Mitgliedschaft "
                f"in dieser Partei. Präsens-Aussagen 'ist Mitglied …' "
                f"sind ohne neuere Quelle nicht mehr zutreffend. "
                f"Roh-Daten: {body}",
                qid,
                label,
            )
        return body, qid, label

    # Fallback
    return str(row)[:200], qid, ""


async def _run_sparql(client, query: str) -> list[dict] | None:
    """Führt einen SPARQL-Query aus und gibt die ``bindings``-Liste zurück.

    Returns Liste von Result-Rows ODER None bei Fehler/Timeout.
    """
    headers = {
        "Accept": "application/sparql-results+json",
    }
    try:
        resp = await client.get(
            WIKIDATA_SPARQL_URL,
            params={"query": query},
            headers=headers,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            logger.debug(
                f"Wikidata SPARQL HTTP {resp.status_code} "
                f"(body: {resp.text[:120]!r})"
            )
            return None
        data = resp.json()
        return data.get("results", {}).get("bindings", []) or []
    except Exception as e:
        logger.debug(f"Wikidata SPARQL fetch failed: {e}")
        return None


def _topic_for_template(template_name: str) -> str:
    """Pro Template ein semantischer Topic-Tag (für UI-/Confidence-Calc)."""
    return {
        "person_lebensdaten":     "wikidata_person",
        "politiker_amtszeit":     "wikidata_politik_amt",
        "land_hauptstadt":        "wikidata_geographie_hauptstadt",
        "land_bevoelkerung":      "wikidata_demographie",
        "organisation_gruendung": "wikidata_organisation",
        "werk_autor":             "wikidata_werk",
        "erfindung_erfinder":     "wikidata_erfindung",
        "geographie_berg":        "wikidata_geographie_berg",
        "geographie_fluss":       "wikidata_geographie_fluss",
        "person_partei":          "wikidata_politik_partei",
    }.get(template_name, "wikidata_structured_fact")


def _description_for_template(template_name: str) -> str:
    return {
        "person_lebensdaten": (
            "Wikidata strukturierte Fakten zu Person: Geburts-/Sterbe"
            "datum, -ort, Beruf."
        ),
        "politiker_amtszeit": (
            "Wikidata strukturierte Politiker-Amtszeit: Position, "
            "Anfangs- + End-Datum, Partei."
        ),
        "land_hauptstadt": (
            "Wikidata strukturierte Land-Hauptstadt-Beziehung mit "
            "optionalem Beginn-Datum."
        ),
        "land_bevoelkerung": (
            "Wikidata strukturierte Bevölkerungsangabe (Wert + "
            "Stichtag/Erhebungsjahr)."
        ),
        "organisation_gruendung": (
            "Wikidata strukturierte Organisations-Gründung: Datum, "
            "Land, Gründer:in."
        ),
        "werk_autor": (
            "Wikidata strukturierte Werk-Zuordnung: Autor:in / Regie / "
            "Erscheinungsdatum."
        ),
        "erfindung_erfinder": (
            "Wikidata strukturierte Erfindungs-/Entdeckungs-Zuordnung "
            "mit optionalem Datum."
        ),
        "geographie_berg": (
            "Wikidata strukturierte Berg-Daten: Höhe + Land."
        ),
        "geographie_fluss": (
            "Wikidata strukturierte Fluss-Daten: Länge + Land."
        ),
        "person_partei": (
            "Wikidata strukturierte Partei-Mitgliedschaft mit Anfangs- "
            "und End-Datum."
        ),
    }.get(template_name, "Wikidata strukturierte Fakten.")


async def search_wikidata(analysis: dict) -> dict:
    """Live-Lookup gegen Wikidata SPARQL für Claim-Entities.

    Returns Dict mit ≤3 strukturierten Fakt-Treffern. Wenn kein Template
    passt oder kein Treffer in Wikidata existiert, werden 0 Treffer
    geliefert (kein Error).

    Strategie:
    1. _detect_template_for_claim → Template + Entity-Label
    2. Cache-Lookup (1 h TTL)
    3. SPARQL-Query (LIMIT 5, 20 s Timeout)
    4. Top-3 Rows formatieren → display_value + Wikidata-URLs
    """
    empty = {"source": "Wikidata", "type": "structured_fact", "results": []}

    claim = (analysis or {}).get("claim", "") or ""
    template_name, params = _detect_template_for_claim(claim, analysis)
    if not template_name or not params.get("name"):
        return empty

    template = _get_template(template_name)
    if not template:
        return empty

    entity_label = params["name"]
    cache_key = f"{template_name}::{entity_label.lower()}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0] < _CACHE_TTL_S):
        logger.info(
            f"Wikidata: Cache-Hit für '{entity_label[:40]}' "
            f"({template_name})"
        )
        return cached[1]

    sparql = template["sparql"].format(
        name=_escape_sparql_label(entity_label)
    )

    async with polite_client(timeout=SPARQL_TIMEOUT_S) as client:
        try:
            rows = await asyncio.wait_for(
                _run_sparql(client, sparql),
                timeout=SPARQL_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.info(
                f"Wikidata: SPARQL-Timeout für "
                f"'{entity_label[:40]}' ({template_name})"
            )
            return empty

    if not rows:
        logger.info(
            f"Wikidata: 0 Treffer für '{entity_label[:40]}' "
            f"({template_name})"
        )
        # Negativ-Cache hilft Wiederholungen
        _CACHE[cache_key] = (now, empty)
        return empty

    # Row-Cap nach Template wählen:
    # - Politiker:innen haben oft >5 Ämter — bei strengem ``rows[:3]`` fiel
    #   z.B. Rishi Sunaks Prime-Minister-Amt 2022-2024 raus (Position #4 in
    #   DESC(start)-Order hinter Trustee/Oppositionsführer/Mitglied 59. Parl.)
    #   → STRUKTURELL-FALSCH-Marker griff nicht. Wir lassen daher alle 5
    #   SPARQL-Rows durch, damit historische Ämter sichtbar bleiben.
    # - Andere Templates bleiben bei :3 (kürzere Display-Listen).
    if template_name in ("politiker_amtszeit", "person_partei"):
        rows = rows[:5]
    else:
        rows = rows[:3]

    # Templates, bei denen mehrere Rows derselben Person/Entity erwünscht
    # sind — z.B. mehrere politische Ämter oder Partei-Mitgliedschaften
    # pro Politiker:in. Sonst dedupliziert die qid-Schutzklausel unten
    # alle bis auf das jüngste Amt weg (Bug 2026-05-22: Rishi Sunak's
    # PM-Amt 2022-2024 verschwand hinter seinem aktuellen Trustee-Amt
    # 2025-heute, weil ``ORDER BY DESC(?start)`` das Trustee-Amt zuerst
    # zurückgab und alle weiteren Sunak-Rows verworfen wurden).
    _allow_multi_row_per_qid = template_name in (
        "politiker_amtszeit", "person_partei",
    )

    results: list[dict] = []
    seen_qids: set[str] = set()
    for row in rows:
        try:
            display, qid, label = _format_row(template_name, row)
        except Exception as e:
            logger.debug(
                f"Wikidata: Format-Fehler bei row "
                f"({template_name}): {e}"
            )
            continue

        if not _allow_multi_row_per_qid:
            if qid and qid in seen_qids:
                continue
            if qid:
                seen_qids.add(qid)

        url = WIKIDATA_ENTITY_URL.format(qid=qid) if qid else (
            "https://query.wikidata.org/"
        )
        secondary_url = (
            WIKIDATA_ENTITY_DATA_URL.format(qid=qid) if qid else ""
        )

        indicator_name = (
            f"{label} (Wikidata {qid})" if (label and qid) else
            (f"{label} (Wikidata)" if label else "Wikidata")
        )

        results.append({
            "indicator_name": indicator_name,
            "indicator": "wikidata_fact",
            "country": "—",
            "year": "—",
            "topic": _topic_for_template(template_name),
            "display_value": display[:500],
            "description": _description_for_template(template_name)[:200],
            "url": url,
            "secondary_url": secondary_url,
            "source": "Wikidata (CC0)",
        })

    out = {
        "source": "Wikidata",
        "type": "structured_fact",
        "results": results,
    }
    _CACHE[cache_key] = (now, out)
    if results:
        logger.info(
            f"Wikidata: {len(results)} strukturierte Fakten geliefert "
            f"für '{entity_label[:40]}' ({template_name})"
        )
    return out
