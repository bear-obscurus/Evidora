"""Secrets aus den Logs halten — nach WERT, nicht nach Parametername.

Gefunden am 2026-09-07 beim Debuggen des Politik-Guards: In der Ausgabe von
``docker compose logs backend`` stand

    HTTP Request: GET https://factchecktools.googleapis.com/v1alpha1/
    claims:search?query=...&key=AIza... "HTTP/1.1 200 OK"

Der Google-Fact-Check-Key im Klartext, bei **jedem** Aufruf. Nicht durch einen
Fehler im Code — ``httpx`` loggt auf INFO die vollstaendige Request-URL, und
der Key steht dort als Query-Parameter, weil die Google-API ihn so erwartet.

WARUM NACH WERT UND NICHT NACH PARAMETERNAME
--------------------------------------------
Der naheliegende Filter waere „schwaerze alles hinter ``key=``". Er ist falsch
in beide Richtungen:

  - Er schwaerzt zu viel. ``oecd.py`` schickt einen SDMX-Datenselektor als
    ``key=.GWP.PT_WG_SAL_M_D._Z._Z.MEDIAN._T`` — kein Geheimnis, sondern genau
    die Information, die man im Log braucht, um zu sehen, welcher Datensatz
    abgefragt wurde.
  - Er schwaerzt zu wenig. ``europeana`` nennt seinen Parameter ``wskey``,
    ``pubmed``/``cochrane``/``dpla`` nennen ihn ``api_key``, ``uspstf``
    ``key``. Wer Namen aufzaehlt, vergisst welche — dieselbe Falle wie die
    Frontex-Flexionsformen (#141) und die handgepflegten Schreibweisen-
    Zwillinge (#143).

Deshalb kommen die zu schwaerzenden Werte aus der UMGEBUNG: jede Variable,
deren Name auf ein Geheimnis hindeutet, wird zur Suchmaske. Ein neuer
Konnektor mit einem neuen Key ist damit automatisch abgedeckt, ohne dass hier
jemand etwas nachtraegt.

ZWEI SCHICHTEN, WEIL ES ZWEI LECK-PFADE GIBT
--------------------------------------------
1. ``httpx`` loggt die URL als ARGUMENT, nicht im Meldungstext:

       logger.info('HTTP Request: %s %s "%s %d %s"', request.method,
                   request.url, ...)

   Ein Filter, der nur ``record.msg`` anfasst, tut hier schlicht nichts. Die
   ``LogRecord``-Fabrik greift frueher als jeder Handler und erwischt ``msg``
   UND ``args`` — unabhaengig davon, welche Handler spaeter dazukommen.

2. Tracebacks. ``httpx`` legt die volle URL in den Exception-Text:

       Client error '404 Not Found' for url 'https://…?key=…'

   Der Traceback entsteht erst im Formatter, also haengt dort eine zweite
   Schicht. ``main.py`` loggt an vier Stellen mit ``exc_info=True``.

DIE ANFRAGE SELBST (4.11.2026)
------------------------------
Die Datenschutzerklaerung sagt: „Deine eingegebenen Behauptungen werden nicht
geloggt." Gemessen am 4.11.2026 im Prod-Log stimmte das nicht:

  - ``httpx`` schrieb 884 von 2.686 Request-URLs MIT Suchbegriffen aus dem
    Claim — ``openalex.org/works?search=child+height+booster+seat+…+age+5``
    zu „Mein Kind ist groß genug für die Sitzerhöhung mit 5 Jahren", und
    Wikipedia traegt den Begriff im PFAD (``/page/summary/Österreichs…``).
  - 12 eigene Log-Zeilen gaben den Claim selbst aus
    (``verdict_cache: STORED [de] 'Väter …'``), 61 weitere in 29 Diensten die
    daraus gebauten Suchbegriffe (``EFSA: 0 results for 'spinach …'``).

Derselbe Grundsatz wie bei den Keys: nach WERT, nicht nach Name. Wer Stellen
aufzaehlt, vergisst welche — die Suchbegriffe heissen ``term``, ``query``,
``search_term``, ``keywords``, ``used_query``, ``safe_entities`` …
``main.py`` meldet deshalb die Woerter jeder Anfrage an (``merke_anfrage``):
den Claim, und nach der Analyse die Felder, die aus ihm gebaut sind
(``ANFRAGE_FELDER``). Diese Woerter werden in JEDER Logzeile der naechsten
``ANFRAGE_GEDAECHTNIS_S`` Sekunden durch ``‹…›`` ersetzt — auch in URLs, die
dafuer vorher entschluesselt werden (``+``, ``%C3%96``). Eine URL ohne
angemeldetes Wort bleibt Zeichen fuer Zeichen erhalten, der OECD-Selektor
also auch.

Bewusst GLOBAL statt pro Anfrage (``ContextVar``): Ein Log aus einem Thread
oder einem spaeter gestarteten Task saehe den Kontext nicht. Schwaerzt eine
Anfrage nebenbei die Woerter einer gleichzeitigen zweiten, kostet das nur
Lesbarkeit.

Kategorie und Unterkategorie der Analyse werden NICHT angemeldet — die nennt
die Datenschutzerklaerung ausdruecklich als protokolliert.

Was bleibt: Begriffe, die ein Dienst selbst ableitet und die in keinem
angemeldeten Feld stehen (ein Synonym aus einem eigenen Woerterbuch, ein
aufgeloestes Wikidata-Label). Sie verraten das Thema, nicht den Satz.

BEWUSSTE GRENZEN
----------------
- Werte unter 8 Zeichen werden ignoriert. Ein kurzes oder versehentlich
  gesetztes Geheimnis wuerde sonst quer durch alle Logzeilen matchen.
- Nicht gesetzte Variablen (leerer String) zaehlen nie mit.
- Die Umgebung wird EINMAL beim Installieren gelesen, so wie die Konnektoren
  ihre Keys auch beim Import lesen. Wer einen Key rotiert, startet ohnehin
  neu — sonst laeuft der alte Key weiter.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
import time
from urllib.parse import unquote_plus

from services._schreibweise import normalisiere

# Namensmuster fuer Umgebungsvariablen, die ein Geheimnis tragen. Bewusst
# breit: lieber eine harmlose Variable mitschwaerzen als eine echte uebersehen.
_GEHEIM_NAME = re.compile(
    r"(API_KEY|_KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|_PW)$"
)

# Kuerzere Werte werden ignoriert — siehe „Bewusste Grenzen" oben.
MIN_LAENGE = 8

# Namen, die trotz passendem Muster KEIN Geheimnis sind.
_AUSNAHMEN = frozenset({
    # Pfad auf eine Datei, kein Wert; taucht in Logs ohnehin nur als Pfad auf
    # und ist fuer die Fehlersuche nuetzlich.
    "GOOGLE_APPLICATION_CREDENTIALS",
})

_installiert = False


def geheimnisse(umgebung: dict[str, str] | None = None) -> list[tuple[str, str]]:
    """(Variablenname, Wert) aller gesetzten Geheimnisse, laengste zuerst.

    Die Sortierung ist wichtig: enthaelt ein Geheimnis ein anderes als
    Teilstring, muss das laengere zuerst ersetzt werden, sonst bleibt ein
    Rest stehen.
    """
    umg = os.environ if umgebung is None else umgebung
    raus = []
    for name, wert in umg.items():
        if name in _AUSNAHMEN or not _GEHEIM_NAME.search(name):
            continue
        wert = (wert or "").strip()
        if len(wert) >= MIN_LAENGE:
            raus.append((name, wert))
    raus.sort(key=lambda p: len(p[1]), reverse=True)
    return raus


# --------------------------------------------------------------------------
# Die Anfrage selbst — siehe „DIE ANFRAGE SELBST" oben
# --------------------------------------------------------------------------

#: Die Felder der Analyse, die aus dem Claim gebaut sind. ``category`` und
#: ``subcategory`` fehlen mit Absicht.
ANFRAGE_FELDER = ("claim", "original_claim", "entities", "ner_entities",
                  "pubmed_queries", "factcheck_queries", "spacy_keywords")

#: So lange bleiben die Woerter einer Anfrage geschwaerzt. Eine Pruefung
#: dauert hoechstens ~330 s (SSE-Timeout); der Rest ist Puffer fuer
#: nachlaufende Tasks.
ANFRAGE_GEDAECHTNIS_S = 900

#: Kuerzere Woerter traegt fast jede Logzeile ("eine", "mit", "for").
MIN_WORT = 4

SCHWARZ = "‹…›"

# Funktionswoerter ab MIN_WORT Buchstaben — sie sagen nichts ueber den Claim
# und stehen in vielen Logzeilen.
_FUNKTIONSWOERTER = frozenset("""
    aber alle allen aller alles also auch beim bereits dass dein deine diese
    dieser dieses doch durch eine einem einen einer eines etwa euer gegen
    haben hatte hatten heute hier ihre ihren immer jede jeden jeder jedes
    kann kein keine keinen keiner mehr muss nach nicht noch nur oder ohne
    rund schon sehr sein seine seit sich sind ueber unter viel viele vielen
    weil wenn werden wieder wird wurde wurden zwar zwischen
    about after also been before being could does from have into more most
    only other over some such than that their them then there these they
    this those very were what when where which while will with would your
""".split())

_WORT_IM_FELD = re.compile(r"[^\W\d_]+")
_WORT_IM_LOG = re.compile(r"[^\W_]+")
_URL = re.compile(r"https?://[^\s'\"<>]+")

_anfrage_lock = threading.Lock()
_anfrage_woerter: dict[str, float] = {}       # Wort -> Ablauf (monotonic)
_schnappschuss: tuple[float, frozenset] = (0.0, frozenset())

# Pro Prozess neu: ``claim_kennung`` ordnet Zeilen EINER Laufzeit einander zu
# ("EXACT HIT" zu "STORED"), laesst sich aber nicht gegen eine Liste
# vermuteter Claims pruefen.
_SALZ = os.urandom(16)


def claim_kennung(text: str) -> str:
    """Kennung statt Text, fuer Logzeilen ueber einen Claim."""
    h = hashlib.sha256(_SALZ + (text or "").encode("utf-8")).hexdigest()
    return f"#{h[:8]} ({len(text or '')} Zeichen)"


_STRUKTUR = re.compile(r'("[A-Za-z_]+"\s*:)|([^\W\d_]+)|(\d+)')


def nur_struktur(text: str) -> str:
    """Rohantwort des Modells ohne ihren Inhalt — fuer Parse-Fehler.

    JSON-Schluessel bleiben stehen, jeder andere Buchstabenlauf wird ``x``,
    jede Zahl ``0``. Klammern, Kommas und Anfuehrungszeichen bleiben, und
    genau die braucht man, um zu sehen, WARUM ein JSON nicht parst.

    >>> nur_struktur('{"summary": "Väter nehmen 4,1 % …", "verdict": "tr')
    '{"summary": "x x 0,0 % …", "verdict": "x'
    """
    def ersatz(m):
        if m.group(1):
            return m.group(1)
        return "x" if m.group(2) else "0"
    return _STRUKTUR.sub(ersatz, text or "")


def _sammle_texte(wert, raus: list) -> None:
    if isinstance(wert, str):
        raus.append(wert)
    elif isinstance(wert, dict):
        for v in wert.values():
            _sammle_texte(v, raus)
    elif isinstance(wert, (list, tuple, set)):
        for v in wert:
            _sammle_texte(v, raus)


def merke_anfrage(*werte) -> int:
    """Meldet die Woerter einer Anfrage zum Schwaerzen an.

    Nimmt Strings, Listen und Dicts (verschachtelt). Fuer die Analyse nur
    die Felder aus ``ANFRAGE_FELDER`` uebergeben. Gibt die Zahl der neu oder
    erneut angemeldeten Woerter zurueck.
    """
    texte: list[str] = []
    for w in werte:
        _sammle_texte(w, texte)
    woerter = set()
    for t in texte:
        for wort in _WORT_IM_FELD.findall(t.lower()):
            gefaltet = normalisiere(wort).strip()
            if (len(wort) >= MIN_WORT and wort not in _FUNKTIONSWOERTER
                    and gefaltet not in _FUNKTIONSWOERTER):
                for form in (wort, gefaltet):
                    woerter.add(form)
                    # Auch die Staemme: angemeldet "Ministers", im Log
                    # "Minister". Die Gegenrichtung (Log "Österreichs" zu
                    # "österreich") prueft _ist_angemeldet.
                    for k in (1, 2, 3):
                        if len(form) - k >= 5:
                            woerter.add(form[:-k])
    woerter.discard("")
    if not woerter:
        return 0
    ablauf = time.monotonic() + ANFRAGE_GEDAECHTNIS_S
    global _schnappschuss
    with _anfrage_lock:
        for w in woerter:
            _anfrage_woerter[w] = ablauf
        _schnappschuss = (0.0, frozenset())     # beim naechsten Lesen neu
    return len(woerter)


def vergiss_anfragen() -> None:
    """Fuer Tests: alle angemeldeten Woerter verwerfen."""
    global _schnappschuss
    with _anfrage_lock:
        _anfrage_woerter.clear()
        _schnappschuss = (0.0, frozenset())


def aktive_woerter() -> frozenset:
    """Die zur Zeit geschwaerzten Woerter. Abgelaufene fallen heraus."""
    global _schnappschuss
    jetzt = time.monotonic()
    gueltig_bis, woerter = _schnappschuss
    if jetzt < gueltig_bis:
        return woerter
    with _anfrage_lock:
        for w in [w for w, t in _anfrage_woerter.items() if t <= jetzt]:
            del _anfrage_woerter[w]
        woerter = frozenset(_anfrage_woerter)
        naechster = min(_anfrage_woerter.values(), default=jetzt + 60)
        _schnappschuss = (min(naechster, jetzt + 60), woerter)
    return woerter


def _ist_angemeldet(wort: str, woerter: frozenset) -> bool:
    w = wort.lower()
    if w in woerter or normalisiere(w).strip() in woerter:
        return True
    # Flexion: "Österreichs" zu "österreich", "Schweinehaltern" zu
    # "schweinehalter" — bis drei Buchstaben Endung, Stamm ab fuenf.
    for k in (1, 2, 3):
        if len(w) - k >= 5 and w[:-k] in woerter:
            return True
    return False


def schwaerze_anfrage(text: str, woerter: frozenset | None = None) -> str:
    """Ersetzt angemeldete Woerter durch ``SCHWARZ`` — URLs zuerst entschluesselt."""
    if woerter is None:
        woerter = aktive_woerter()
    if not woerter or not text:
        return text

    def wort(m):
        return SCHWARZ if _ist_angemeldet(m.group(0), woerter) else m.group(0)

    def url(m):
        roh = m.group(0)
        lesbar = unquote_plus(roh)
        neu = _WORT_IM_LOG.sub(wort, lesbar)
        return neu if neu != lesbar else roh

    text = _URL.sub(url, text)
    return _WORT_IM_LOG.sub(wort, text)


def redigiere(text: str, geheim: list[tuple[str, str]]) -> str:
    """Ersetzt jeden Geheimwert durch ``<redigiert:VARIABLENNAME>`` und
    schwaerzt danach die Woerter der laufenden Anfragen.

    Der Name bleibt sichtbar: bei einem Leck will man wissen, WELCHER Key
    betroffen war, ohne ihn zu sehen.
    """
    return schwaerze_anfrage(_ersetze_geheimnisse(text, geheim))


def _ersetze_geheimnisse(text: str, geheim: list[tuple[str, str]]) -> str:
    for name, wert in geheim:
        if wert in text:
            text = text.replace(wert, f"<redigiert:{name}>")
    return text


def _redigiere_wert(wert, geheim):
    """Ein einzelnes Log-Argument saeubern.

    Argumente sind nicht immer Strings — ``httpx`` reicht ein ``httpx.URL``
    durch. Deshalb wird jedes Objekt zu Text gemacht und NUR dann ersetzt,
    wenn dort tatsaechlich ein Geheimnis steckt; sonst bleibt das Original mit
    seinem Typ erhalten (``%d`` auf einem str waere sonst ein Fehler).
    """
    if isinstance(wert, str):
        return redigiere(wert, geheim)
    if isinstance(wert, (int, float, bool, type(None))):
        return wert
    try:
        text = str(wert)
    except Exception:
        return wert
    sauber = redigiere(text, geheim)
    return sauber if sauber != text else wert


def _saeubere_record(record: logging.LogRecord, geheim) -> None:
    if isinstance(record.msg, str):
        # Bei Dict-Argumenten stehen Schluessel im Format ("%(name)s") — ein
        # geschwaerztes Wort dort waere ein KeyError beim Formatieren. Die
        # Werte selbst werden unten geschwaerzt.
        record.msg = (_ersetze_geheimnisse(record.msg, geheim)
                      if isinstance(record.args, dict)
                      else redigiere(record.msg, geheim))
    args = record.args
    if isinstance(args, tuple):
        record.args = tuple(_redigiere_wert(a, geheim) for a in args)
    elif isinstance(args, dict):
        record.args = {k: _redigiere_wert(v, geheim) for k, v in args.items()}


class RedigierenderFormatter(logging.Formatter):
    """Zweite Schicht: der fertige Text, inklusive Traceback.

    Umschliesst den vorhandenen Formatter, statt ihn zu ersetzen — sonst
    verliert man dessen Format-String.
    """

    def __init__(self, innerer: logging.Formatter, geheim):
        super().__init__()
        self._innerer = innerer
        self._geheim = geheim

    def format(self, record: logging.LogRecord) -> str:
        return redigiere(self._innerer.format(record), self._geheim)


def installiere(logger: logging.Logger | None = None) -> int:
    """Beide Schichten einhaengen. Gibt die Zahl der erkannten Geheimnisse.

    Idempotent: ein zweiter Aufruf tut nichts. Die Fabrik wird an die
    bestehende angehaengt, nicht ersetzt. Seit 4.11.2026 auch OHNE
    Geheimnisse — die Woerter der Anfragen brauchen dieselben Schichten.
    """
    global _installiert
    if _installiert:
        return 0
    geheim = geheimnisse()

    vorherige_fabrik = logging.getLogRecordFactory()

    def fabrik(*args, **kwargs):
        record = vorherige_fabrik(*args, **kwargs)
        _saeubere_record(record, geheim)
        return record

    logging.setLogRecordFactory(fabrik)

    # Nicht nur die Wurzel: uvicorn haengt eigene Handler an "uvicorn" und
    # "uvicorn.access", und die propagieren nicht.
    if logger is not None:
        ziele = [logger]
    else:
        ziele = [logging.getLogger()] + [
            lg for lg in logging.Logger.manager.loggerDict.values()
            if isinstance(lg, logging.Logger)]
    for lg in ziele:
        for handler in lg.handlers:
            innerer = handler.formatter or logging.Formatter()
            if not isinstance(innerer, RedigierenderFormatter):
                handler.setFormatter(RedigierenderFormatter(innerer, geheim))

    _installiert = True
    return len(geheim)
