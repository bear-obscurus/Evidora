"""BMI Volksbegehren — Liste aller österreichischen Volksbegehren der
zweiten Republik.

Datenquelle: Bundesministerium für Inneres (BMI), Abt. III/6
(Wahlangelegenheiten).  Die kanonische Liste aller bundesweiten
Volksbegehren seit 1964 wird vom BMI als HTML-Tabelle gepflegt:

  https://www.bmi.gv.at/411/Alle_Volksbegehren_der_zweiten_Republik.aspx

Warum BMI (und nicht Parlament.gv.at)?
-------------------------------------
Das österreichische Parlament publiziert Volksbegehren als
Verhandlungsgegenstand, aber die Filter-API des Parlaments hat keinen
zuverlässigen Filter, der ausschließlich auf Volksbegehren narrowet
(``FBEZ=VBG`` liefert ein Klubmitglieder-Schema; Versuche mit
``listeId=10006`` und Variationen davon haben keinen funktionierenden
Endpunkt produziert — siehe ``hard_to_implement.md``).  Das BMI ist für
Wahlangelegenheiten zuständig und führt die offizielle Statistik der
Eintragungen — d.h. **das BMI ist für diesen Faktentyp die kanonische
Primärquelle**, nicht das Parlament.

WICHTIG — IP-Block durch Myra Cloud
-----------------------------------
Die BMI-Site sitzt hinter Myra Cloud (deutscher DDoS-Schutz), das
Hetzner-VPS-IP-Bereiche routinemäßig auf der Bot-Block-Liste hat.
Live-Test 2026-04-26 vom Hetzner-Server:
``302 Moved Temporarily → /myracloud-blocked/...``

Architektur-Konsequenz: **Static-First + optionaler Online-Refresh.**
- Primärquelle: ``data/volksbegehren.json`` (im Repo, 109 Einträge).
- Online-Refresh wird beim Prefetch versucht — wenn er klappt,
  überschreibt er den Cache, ansonsten bleibt der Static-Stand aktiv.
- Der Static-Stand kann lokal manuell refreshed werden via
  ``scripts/refresh_volksbegehren.py`` (von einer nicht geblockten IP).

Die Liste ändert sich nur bei Eintragungswochen (typ. 1–3 pro Jahr) —
ein paar Wochen Staleness sind tolerabel.

v1-Umfang:
- Vollständige Liste aller Volksbegehren (1964-heute)
- Pro Volksbegehren: Jahr, Betreff, Eintragungszeitraum,
  Anzahl gültiger Eintragungen, Stimmbeteiligung in %, Rang nach
  Stimmbeteiligung, Initiatorin/Initiator
- Suchen nach Betreff (Substring) oder Jahr
- Top-N nach Eintragungszahl (z.B. "erfolgreichstes Volksbegehren")

Lizenz: BMI-Inhalte sind als amtliche Werke gemeinfrei (§ 7 UrhG-AT).
Zitation: BMI, „Alle Volksbegehren der zweiten Republik" (abgerufen
{Abfrage-Datum}).

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren reine Eintragungszahlen, geben keine Bewertung der
  Volksbegehrens-Inhalte ab.
- Wir nehmen keine politischen Klassifikationen vor (z.B. „links/rechts").
- Mehrdeutige oder ähnliche Betreffe (z.B. zwei Konsumentenschutz-VBG
  in verschiedenen Jahren) werden alle gelistet — der Synthesizer und
  die User entscheiden, welches gemeint ist.
"""

import html as htmllib
import json
import logging
import os
import re
import time

import httpx

from services._http_polite import USER_AGENT
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BMI_VBG_URL = (
    "https://www.bmi.gv.at/411/Alle_Volksbegehren_der_zweiten_Republik.aspx"
)
BMI_VBG_BASE = "https://www.bmi.gv.at/"

# Static-shipped JSON, gepflegt via ``scripts/refresh_volksbegehren.py``.
# Pfad relativ zum Backend-Root (``data/`` neben ``services/``).
STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "volksbegehren.json",
)

# Polite-identified Header für den optionalen Live-Fallback (BMI-OGD
# ist Open Government Data — keine Browser-Maskierung nötig).
_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
    "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
}

VBG_CACHE_TTL = 86400  # 24h — die Liste ändert sich allenfalls bei neuen VBG

# Cache structure:
# {
#   "entries": [ {jahr, betreff, zeitraum, anzahl, beteiligung, rang,
#                 unterstuetzt, url}, ... ],
#   "fetched_at": float,
# }
_cache: dict | None = None
_cache_time: float = 0.0


# Trigger-Vokabular: alles, was auf Volksbegehren / direkte Demokratie
# hinweist.  Pluralformen explizit, weil deutsche Substantive sonst
# Substring-Probleme haben.
VBG_KEYWORDS = [
    # Volksbegehren
    "volksbegehren", "volksbegehrens",
    # Eintragungswoche / Eintragung
    "eintragungswoche", "eintragungszeitraum", "eintragungen",
    "stimmbeteiligung", "unterstützungserklärung",
    "unterstützungserklärungen",
    # Allgemeine direkte Demokratie (oft gemeinsam mit VBG erwähnt)
    "volksabstimmung", "volksabstimmungen",
    "volksbefragung", "volksbefragungen",
    # Englisch (für englische Claims über AT)
    "popular initiative", "citizens' initiative",
    "austrian referendum", "people's initiative",
]

# AT-Kontext: dasselbe Schema wie parlament_at.py — ein VBG-Keyword
# allein triggert nicht, weil "volksbegehren" auch in CH/DE-Kontexten
# auftauchen kann.  Diese Liste ist bewusst etwas enger als die generische
# AT-Kontext-Liste (z.B. ohne reine Bundesländernamen), weil ein
# Volksbegehren immer ein Bundes-Volksbegehren ist — Landes-Volksbegehren
# regelt jedes Bundesland eigenständig und wir decken nur die Bundes-Ebene
# ab.
VBG_AT_CONTEXT = [
    "österreich", "austria", "österreichisch",
    "republik österreich",
    "wien", "vienna",
    # Wahlamtsbegriffe, die fast ausschließlich AT-spezifisch sind
    "bundesinnenministerium", "bmi-wahlen",
    # Wenn ein VBG-Name im Claim steht (z.B. "Don't smoke", "Frauen-VBG")
    # zählt das nicht automatisch als AT-Kontext — der User muss das
    # Land explizit oder über einen Stadtnamen mitliefern.  Der zusätzliche
    # _claim_mentions_known_vbg-Check unten fängt explizite Namen ab.
]


def _claim_mentions_known_vbg(claim: str, entries: list[dict]) -> bool:
    """True wenn der Claim einen bekannten Volksbegehren-Betreff
    erwähnt — das gilt als AT-Kontext, weil Volksbegehren genuin
    österreichisch sind.

    Wir matchen gegen die ersten 3 inhaltstragenden Wörter des Betreffs
    (>= 5 Zeichen, ohne Stopwords/„volksbegehren" selbst), um false
    positives wie „Das Volksbegehren in der Schweiz" zu vermeiden — dort
    wäre sonst das Wort „volksbegehren" doppelt zählend ein Match.
    """
    cl = claim.lower()
    for entry in entries:
        betreff = (entry.get("betreff") or "").lower()
        # Signifikante Wörter, dedupliziert + Stopwords raus
        all_words = re.findall(r"[a-zäöüß]{5,}", betreff)
        seen: set[str] = set()
        words: list[str] = []
        for w in all_words:
            if w in _STOPWORDS or w == "volksbegehren":
                continue
            if w in seen:
                continue
            seen.add(w)
            words.append(w)
            if len(words) >= 3:
                break
        # Mindestens zwei verschiedene Inhaltswörter müssen im Claim
        # vorkommen, damit ein zufälliges „lebens" oder „schutz" nicht reicht.
        hits = sum(1 for w in words if w in cl)
        if hits >= 2:
            return True
        # Fallback: 1 Inhaltswort + exakt passende Jahreszahl reicht auch —
        # die Kombination ist hochspezifisch (siehe „Don't-smoke 2018").
        jahr = entry.get("jahr")
        if hits >= 1 and jahr is not None and re.search(rf"\b{jahr}\b", cl):
            return True
    return False


def _claim_mentions_volksbegehren(claim: str, entries: list[dict]) -> bool:
    """True wenn der Claim Volksbegehren-Vokabular plus AT-Kontext nennt."""
    cl = claim.lower()
    has_kw = any(kw in cl for kw in VBG_KEYWORDS)
    if not has_kw:
        return False
    has_at = any(kw in cl for kw in VBG_AT_CONTEXT)
    if has_at:
        return True
    # Fallback: bekannter Volksbegehrens-Name im Claim
    return _claim_mentions_known_vbg(claim, entries)


def claim_mentions_volksbegehren_cached(claim: str) -> bool:
    """Synchronous gate for the request hot path.

    Primary path: read the prefetch cache (filled by
    ``data_updater.prefetch_all`` at startup).  Fallback path: if the
    cache is empty (e.g. a request hits before the first prefetch
    completes), do a sync load from the static JSON so we never miss
    a trigger.  The static JSON is always shipped with the image, so
    this fallback is fast (~1 ms file read).
    """
    entries: list[dict] | None = None
    if _cache:
        entries = _cache.get("entries") or None
    if not entries:
        entries = _load_static_json()
    if not entries:
        return False
    return _claim_mentions_volksbegehren(claim, entries)


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


def _strip_tags(s: str) -> str:
    """Strip HTML tags and decode entities, normalising whitespace."""
    if not s:
        return ""
    # <br> wird zu Leerzeichen (sonst kleben Datums-Bestandteile aneinander)
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = htmllib.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_link(td_html: str) -> str | None:
    """Extract the first href from an HTML table cell, absolutising."""
    m = re.search(r'href="([^"]+)"', td_html)
    if not m:
        return None
    href = htmllib.unescape(m.group(1))
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.bmi.gv.at" + href
    return BMI_VBG_BASE + href


def _parse_int(s: str) -> int | None:
    """Parse a German-formatted integer like ``832.353`` or ``1.361.562``."""
    if not s:
        return None
    cleaned = re.sub(r"[^0-9]", "", s)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_float(s: str) -> float | None:
    """Parse a German-formatted decimal like ``17,27`` (comma as
    decimal separator). Drops trailing ``(rang)`` if present."""
    if not s:
        return None
    # Nur die erste Zahl extrahieren (manche Zellen haben "17,27 (3)")
    m = re.search(r"(\d+(?:[,.]\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None


def _parse_table(html: str) -> list[dict]:
    """Parse the BMI HTML table into a list of dicts."""
    table_match = re.search(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)
    if not table_match:
        return []

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_match.group(1), re.DOTALL)
    parsed: list[dict] = []

    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 6:
            # Header (<th>) oder Sub-Header — überspringen
            continue
        jahr_raw = _strip_tags(cells[0])
        betreff = _strip_tags(cells[1])
        link = _extract_link(cells[1])
        zeitraum = _strip_tags(cells[2])
        anzahl = _parse_int(_strip_tags(cells[3]))
        beteiligung = _parse_float(_strip_tags(cells[4]))
        rang_raw = _strip_tags(cells[5])
        unterstuetzt = _strip_tags(cells[6]) if len(cells) > 6 else ""

        try:
            jahr = int(jahr_raw)
        except (TypeError, ValueError):
            jahr = None

        try:
            rang = int(re.sub(r"[^0-9]", "", rang_raw)) if rang_raw else None
        except ValueError:
            rang = None

        if jahr is None and not betreff:
            continue

        parsed.append({
            "jahr": jahr,
            "betreff": betreff,
            "zeitraum": zeitraum,
            "anzahl": anzahl,
            "beteiligung": beteiligung,
            "rang": rang,
            "unterstuetzt": unterstuetzt,
            "url": link,
        })

    return parsed


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


def _load_static_json() -> list[dict] | None:
    """Load the shipped static JSON snapshot of the BMI list.

    This is the **primary** data source in production because Myra
    Cloud blocks Hetzner IPs from reaching the BMI page directly.
    """
    if not os.path.exists(STATIC_JSON_PATH):
        logger.warning(
            f"BMI Volksbegehren: static JSON not found at {STATIC_JSON_PATH}"
        )
        return None
    try:
        with open(STATIC_JSON_PATH, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"BMI Volksbegehren: static JSON load failed: {e}")
        return None
    entries = payload.get("entries") or []
    if not entries:
        return None
    return entries


async def _try_online_refresh(
    client: httpx.AsyncClient,
) -> list[dict] | None:
    """Best-effort online fetch from BMI.

    Returns the parsed entries on success, ``None`` on any failure
    (HTTP error, redirect to Myra block page, empty parse).  Logs at
    INFO on failure — this is expected on production hosts where the
    IP is blocked, not an error.
    """
    try:
        resp = await client.get(
            BMI_VBG_URL,
            headers=_HEADERS,
            follow_redirects=False,
        )
    except httpx.HTTPError as e:
        logger.info(f"BMI Volksbegehren: online refresh skipped ({e})")
        return None

    # Myra-Block redirects to /myracloud-blocked/... — never follow.
    if resp.status_code in (301, 302, 303, 307, 308):
        loc = resp.headers.get("location", "")
        logger.info(
            f"BMI Volksbegehren: online refresh blocked "
            f"(HTTP {resp.status_code} → {loc[:80]})"
        )
        return None
    if resp.status_code != 200:
        logger.info(
            f"BMI Volksbegehren: online refresh got HTTP {resp.status_code}"
        )
        return None

    entries = _parse_table(resp.text)
    if not entries:
        logger.info("BMI Volksbegehren: online refresh parser returned 0")
        return None

    logger.info(
        f"BMI Volksbegehren: online refresh succeeded ({len(entries)} entries)"
    )
    return entries


async def fetch_volksbegehren(
    client: httpx.AsyncClient | None = None,
) -> dict | None:
    """Static-first loader for the BMI Volksbegehren list.

    Strategy:
    1. Load the shipped static JSON (always available, no network).
    2. Best-effort attempt an online refresh — if it succeeds (typically
       only on IPs not blocked by Myra Cloud), use the fresher data.
    3. Cache the result for 24 h.

    Returns ``{"entries": [...], "fetched_at": float, "source": str}``
    or ``None`` if even the static JSON is missing/invalid (which would
    indicate a deploy/build problem).
    """
    global _cache, _cache_time
    now = time.time()
    if _cache is not None and (now - _cache_time) < VBG_CACHE_TTL:
        return _cache

    # 1. Always start from static snapshot — no network, never fails on
    #    blocked hosts.
    entries = _load_static_json()
    source = "static"

    # 2. Best-effort online refresh.
    close_client = False
    if client is None:
        client = polite_client(timeout=30.0)
        close_client = True
    try:
        fresh = await _try_online_refresh(client)
        if fresh:
            entries = fresh
            source = "online"
    finally:
        if close_client:
            await client.aclose()

    if not entries:
        logger.warning(
            "BMI Volksbegehren: no entries available — neither static "
            "JSON nor online fetch produced data"
        )
        return None

    _cache = {"entries": entries, "fetched_at": now, "source": source}
    _cache_time = now
    logger.info(
        f"BMI Volksbegehren: {len(entries)} entries cached (source={source})"
    )
    return _cache


# ---------------------------------------------------------------------------
# Search / scoring
# ---------------------------------------------------------------------------


# Tokens, die wir bei der Betreff-Suche ignorieren — sie liefern keine
# inhaltliche Unterscheidungskraft und blähen das Match-Set auf.
_STOPWORDS = {
    "der", "die", "das", "und", "oder", "von", "zur", "zum", "des",
    "den", "dem", "ein", "eine", "einer", "eines", "für", "auf", "an",
    "in", "im", "mit", "ohne", "über", "unter", "volksbegehren",
    "österreich", "österreichs", "österreichisch", "österreichische",
    "österreichischen", "österreichischer",
    # Englisch (Claims können englisch sein)
    "the", "and", "for", "with", "of", "on", "in", "to",
    "austrian", "austria",
}


def _significant_words(text: str) -> list[str]:
    text = text.lower()
    return [
        w for w in re.findall(r"[a-zäöüß0-9]{4,}", text)
        if w not in _STOPWORDS
    ]


def _score_entry(claim: str, entry: dict) -> int:
    """Word-overlap score between claim and entry betreff/jahr."""
    cl = claim.lower()
    claim_words = set(_significant_words(claim))
    if not claim_words:
        return 0
    betreff_words = set(_significant_words(entry.get("betreff") or ""))
    overlap = len(claim_words & betreff_words)
    score = overlap * 10  # Wort-Match wiegt schwer

    # Jahres-Bonus: wenn der Claim die exakte Jahreszahl nennt — ABER
    # nicht wenn die Zahl als Startpunkt einer Aggregat-Aussage gemeint
    # ist ("seit 1964", "ab 1964", "since 1964").  Sonst werden Aggregat-
    # Claims wie „weniger als 50 Volksbegehren seit 1964" fälschlich
    # auf das ÖRF-Volksbegehren 1964 gemappt und der Aggregat-Pfad
    # unterdrückt (Bug O).
    jahr = entry.get("jahr")
    if jahr is not None and re.search(rf"\b{jahr}\b", cl):
        if not re.search(rf"\b(seit|ab|since|from)\s+{jahr}\b", cl):
            score += 50  # sehr starkes Signal

    return score


# Aggregat-Phrasen: Claims, die nach der Gesamtzahl bzw. einer
# Mengen-Aussage über *alle* Volksbegehren fragen.  Bei diesen Claims
# wollen wir den ``vbg_count_total``-Eintrag immer mitliefern, damit
# das LLM nicht die Zahl aus dem Trainingswissen erfindet.
_AGGREGATE_PATTERNS = [
    r"\bwie viele\b",
    r"\bhow many\b",
    r"\binsgesamt\b",
    r"\bin total\b",
    r"\banzahl der\b",
    r"\bweniger als\s+\d",
    r"\bmehr als\s+\d",
    r"\bfewer than\s+\d",
    r"\bless than\s+\d",
    r"\bmore than\s+\d",
    r"\büber\s+\d+\s+(volksbegehren|popular)",
    r"\bunter\s+\d+\s+(volksbegehren|popular)",
    r"\bgesamt(zahl|anzahl)\b",
    r"\bseit\s+\d{4}\b",  # "seit 1964 X Volksbegehren"
]


def _claim_is_aggregate(claim: str) -> bool:
    """True wenn der Claim nach einer Gesamt-/Mengenaussage fragt.

    Beispiele:
    - "Wie viele Volksbegehren gab es in Österreich?"
    - "In Österreich gab es seit 1964 weniger als 50 Volksbegehren."
    - "Insgesamt mehr als 100 bundesweite Volksbegehren."
    """
    cl = claim.lower()
    return any(re.search(p, cl) for p in _AGGREGATE_PATTERNS)


def _claim_asks_for_top(claim: str) -> str | None:
    """Returns the kind of "top" the claim is asking for, or None.

    - "anzahl"       — meiste Eintragungen / erfolgreichstes Volksbegehren
    - "beteiligung"  — höchste Stimmbeteiligung in %
    - "neueste"      — zuletzt durchgeführt
    - "älteste"      — frühestes Volksbegehren
    """
    cl = claim.lower()
    if any(p in cl for p in [
        "erfolgreichste", "erfolgreichstes",
        "meiste eintragungen", "meiste unterschriften",
        "größte volksbegehren", "größtes volksbegehren",
        "most successful", "most signatures",
    ]):
        return "anzahl"
    if any(p in cl for p in [
        "höchste stimmbeteiligung", "höchste beteiligung",
        "highest turnout",
    ]):
        return "beteiligung"
    if any(p in cl for p in [
        "letzte volksbegehren", "letztes volksbegehren",
        "neueste volksbegehren", "neuestes volksbegehren",
        "jüngste volksbegehren", "most recent",
    ]):
        return "neueste"
    if any(p in cl for p in [
        "erstes volksbegehren", "frühestes volksbegehren",
        "earliest", "first popular initiative",
    ]):
        return "älteste"
    return None


# ---------------------------------------------------------------------------
# Public search entrypoint
# ---------------------------------------------------------------------------


async def search_volksbegehren(analysis: dict) -> dict:
    """Public entrypoint — returns matching Volksbegehren entries.

    Input: standard analysis dict ({"claim": ..., ...}).
    Output: ``{"source": "BMI Volksbegehren", "type": "official_data",
              "results": [...]}``
    """
    empty = {
        "source": "BMI Volksbegehren",
        "type": "official_data",
        "results": [],
    }

    data = await fetch_volksbegehren()
    if not data:
        return empty

    entries = data.get("entries") or []
    if not entries:
        return empty

    claim = analysis.get("claim", "")
    if not _claim_mentions_volksbegehren(claim, entries):
        return empty

    total_entries = len(entries)
    earliest_year = min(
        (e["jahr"] for e in entries if e["jahr"]), default=None
    )
    latest_year = max(
        (e["jahr"] for e in entries if e["jahr"]), default=None
    )

    # Sind wir im "Top"-Modus? Dann nur 1-3 Top-Einträge.
    top_kind = _claim_asks_for_top(claim)
    is_aggregate = _claim_is_aggregate(claim)
    matched: list[dict] = []
    if top_kind == "anzahl":
        matched = sorted(
            [e for e in entries if e.get("anzahl") is not None],
            key=lambda e: -(e["anzahl"] or 0),
        )[:3]
    elif top_kind == "beteiligung":
        matched = sorted(
            [e for e in entries if e.get("beteiligung") is not None],
            key=lambda e: -(e["beteiligung"] or 0),
        )[:3]
    elif top_kind == "neueste":
        matched = sorted(
            [e for e in entries if e.get("jahr") is not None],
            key=lambda e: -(e["jahr"] or 0),
        )[:3]
    elif top_kind == "älteste":
        matched = sorted(
            [e for e in entries if e.get("jahr") is not None],
            key=lambda e: (e["jahr"] or 9999),
        )[:3]
    else:
        # Standard: Wort-Overlap-Scoring
        scored = [(e, _score_entry(claim, e)) for e in entries]
        scored = [(e, s) for e, s in scored if s >= 10]  # min. 1 Wort-Match
        scored.sort(key=lambda kv: -kv[1])
        matched = [e for e, _s in scored[:5]]

    # Bug O Fix: Aggregat-Eintrag (Gesamtzahl) — als autoritatives Datum
    # für Aggregat-Claims zwingend mitliefern.  Bei spezifischen Claims
    # nur dann, wenn gar kein Match gefunden wurde.
    def _aggregate_entry() -> dict:
        return {
            "indicator_name": (
                f"Volksbegehren in Österreich seit "
                f"{earliest_year or 1964}: {total_entries} insgesamt"
            ),
            "indicator": "vbg_count_total",
            "country": "AUT",
            "country_name": "Österreich",
            "year": str(latest_year) if latest_year else "",
            "value": total_entries,
            "display_value": (
                f"{total_entries} bundesweite Volksbegehren von "
                f"{earliest_year or 1964} bis {latest_year or 'heute'}"
            ),
            "description": (
                f"Vollständige BMI-Liste aller bundesweiten Volksbegehren der "
                f"zweiten Republik. Stand: {total_entries} Einträge zwischen "
                f"{earliest_year or 1964} und {latest_year or 'heute'}. Diese "
                f"Zahl ist autoritativ — die BMI-Liste ist die offizielle "
                f"Gesamtstatistik der Bundeswahlbehörde."
            ),
            "url": BMI_VBG_URL,
        }

    if not matched:
        # Kein direkter Match — liefere wenigstens die Gesamtkennzahlen
        # zurück, damit der Synthesizer "Es gibt insgesamt N VBG" sagen
        # kann.
        return {
            "source": "BMI Volksbegehren",
            "type": "official_data",
            "results": [_aggregate_entry()],
        }

    results: list[dict] = []

    # Bug N Fix: Bei Top-Modus prepend einen Summary-Eintrag, der dem LLM
    # klar macht, dass die folgenden Einträge die *vollständige* Spitze
    # sind — nicht eine isolierte Auswahl.  Sonst antwortet das LLM mit
    # „nicht überprüfbar, weil keine Vergleichsdaten" obwohl die Daten
    # autoritativ sind.
    if top_kind:
        kind_de = {
            "anzahl": "absoluter Eintragungszahl",
            "beteiligung": "Stimmbeteiligung in % (BMI-Rang)",
            "neueste": "Jahr (zuletzt durchgeführt)",
            "älteste": "Jahr (frühestes)",
        }[top_kind]
        top1 = matched[0]
        top1_value = ""
        if top_kind == "anzahl" and top1.get("anzahl"):
            top1_value = f"{top1['anzahl']:,} Eintragungen".replace(",", ".")
        elif top_kind == "beteiligung" and top1.get("beteiligung") is not None:
            top1_value = (
                f"{top1['beteiligung']:.2f} % Beteiligung"
                .replace(".", ",")
            )
        elif top_kind in ("neueste", "älteste") and top1.get("jahr"):
            top1_value = f"Jahr {top1['jahr']}"
        results.append({
            "indicator_name": (
                f"Spitzenreiter nach {kind_de} (vollständige BMI-Liste, "
                f"{total_entries} Volksbegehren {earliest_year or 1964}–"
                f"{latest_year or 'heute'})"
            ),
            "indicator": f"vbg_top_{top_kind}",
            "country": "AUT",
            "country_name": "Österreich",
            "year": str(top1.get("jahr") or ""),
            "value": top1.get("anzahl") if top_kind == "anzahl"
                     else top1.get("beteiligung") if top_kind == "beteiligung"
                     else top1.get("jahr"),
            "display_value": (
                f"Rang 1: {top1.get('betreff') or ''} "
                f"({top1.get('jahr')}) — {top1_value}"
                if top1_value else
                f"Rang 1: {top1.get('betreff') or ''} ({top1.get('jahr')})"
            ),
            "description": (
                f"Diese Reihung ist autoritativ: sie basiert auf der "
                f"vollständigen BMI-Liste aller {total_entries} bundesweiten "
                f"Volksbegehren der zweiten Republik. Kein anderes "
                f"Bundes-Volksbegehren in dieser Liste hat einen höheren "
                f"Wert nach dem genannten Kriterium erreicht."
            ),
            "url": BMI_VBG_URL,
        })

    for i, entry in enumerate(matched, 1):
        jahr = entry.get("jahr")
        betreff = entry.get("betreff") or ""
        anzahl = entry.get("anzahl")
        beteiligung = entry.get("beteiligung")
        rang = entry.get("rang")
        zeitraum = entry.get("zeitraum") or ""
        unterstuetzt = entry.get("unterstuetzt") or ""
        url = entry.get("url") or BMI_VBG_URL

        # display_value: kompakte Mini-Zusammenfassung
        parts: list[str] = []
        if anzahl is not None:
            parts.append(f"{anzahl:,} Eintragungen".replace(",", "."))
        if beteiligung is not None:
            parts.append(f"{beteiligung:.2f} % Stimmbeteiligung".replace(".", ","))
        if rang is not None:
            parts.append(f"Rang {rang} (nach Beteiligung)")
        display_value = " · ".join(parts) if parts else betreff

        # description: Eintragungszeitraum + Initiator (für Kontext)
        descr_parts: list[str] = []
        if zeitraum:
            descr_parts.append(f"Eintragungszeitraum: {zeitraum}")
        if unterstuetzt:
            descr_parts.append(f"Initiiert/unterstützt durch: {unterstuetzt}")
        description = ". ".join(descr_parts) if descr_parts else None

        # Bug N Fix: bei Top-Modus den Rang im indicator_name vorne dran,
        # damit das LLM nicht jede Zeile als isoliertes Faktum liest.
        rank_prefix = ""
        if top_kind == "anzahl":
            rank_prefix = f"Rang {i} nach Eintragungen (von {total_entries}): "
        elif top_kind == "beteiligung":
            rank_prefix = f"Rang {i} nach Stimmbeteiligung (von {total_entries}): "
        elif top_kind == "neueste":
            rank_prefix = f"Rang {i} (jüngste, von {total_entries}): "
        elif top_kind == "älteste":
            rank_prefix = f"Rang {i} (älteste, von {total_entries}): "

        indicator_name = (
            f"{rank_prefix}Volksbegehren {jahr}: {betreff}" if jahr
            else f"{rank_prefix}Volksbegehren: {betreff}"
        )

        results.append({
            "indicator_name": indicator_name,
            "indicator": "vbg_entry",
            "country": "AUT",
            "country_name": "Österreich",
            "year": str(jahr) if jahr else "",
            "value": anzahl if anzahl is not None else "",
            "display_value": display_value,
            "description": description,
            "url": url,
        })

    # Bug O Fix: Bei aggregaten Claims zusätzlich den Aggregat-Eintrag
    # liefern, damit das LLM die Gesamtzahl aus den Daten zieht und sie
    # nicht aus dem Trainingswissen erfindet ("51 weil Klimavolksbegehren
    # 2022 = Rang 51", obwohl die Quelle 109 Einträge enthält).
    if is_aggregate:
        results.insert(0, _aggregate_entry())

    # Methodik-Caveat
    results.append({
        "indicator_name": "WICHTIGER KONTEXT: Volksbegehren-Daten",
        "indicator": "context",
        "country": "",
        "country_name": "",
        "year": "",
        "value": "",
        "display_value": "",
        "url": "https://www.bmi.gv.at/411/start.aspx",
        "description": (
            "Datenquelle: Bundesministerium für Inneres (BMI), Abt. III/6 "
            "(Wahlangelegenheiten) — die offizielle Statistik der "
            "Eintragungen zu allen bundesweiten Volksbegehren der zweiten "
            "Republik. "
            "Einschränkungen: "
            "(1) Nur Bundes-Volksbegehren — Landes-Volksbegehren werden "
            "von den Bundesländern eigenständig geregelt und sind hier "
            "nicht enthalten. "
            "(2) Volksbegehren ≠ Volksabstimmung ≠ Volksbefragung — die "
            "drei Instrumente direkter Demokratie sind unterschiedlich "
            "geregelt; diese Quelle deckt nur Volksbegehren ab. "
            "(3) Wir nehmen keine Bewertung der Inhalte vor — die Zahlen "
            "geben ausschließlich Eintragungen wieder, keine politische "
            "Wirkung oder rechtliche Folgen. "
            "(4) Reihung nach 'Rang' erfolgt nach Stimmbeteiligung in % "
            "(nicht nach absoluter Eintragungszahl), so wie es das BMI "
            "publiziert. "
            "(5) Das Instrument 'Volksbegehren' wurde in Österreich mit dem "
            "Volksbegehrensgesetz BGBl 344/1963 eingeführt (in Kraft seit "
            "1. Juli 1964) — vor 1964 gab es in Österreich keine "
            "Volksbegehren, weder in der Ersten Republik noch in der "
            "Monarchie. Die BMI-Liste deckt damit das vollständige Korpus "
            "dieses Instruments ab; Aussagen wie 'erfolgreichstes "
            "Volksbegehren in der Geschichte Österreichs' sind über die "
            "BMI-Liste vollständig verifizierbar (kein historisches Dunkelfeld "
            "vor 1964)."
        ),
    })

    return {
        "source": "BMI Volksbegehren",
        "type": "official_data",
        "results": results,
    }
