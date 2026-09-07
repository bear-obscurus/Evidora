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

import logging
import os
import re

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


def redigiere(text: str, geheim: list[tuple[str, str]]) -> str:
    """Ersetzt jeden Geheimwert durch ``<redigiert:VARIABLENNAME>``.

    Der Name bleibt sichtbar: bei einem Leck will man wissen, WELCHER Key
    betroffen war, ohne ihn zu sehen.
    """
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
        record.msg = redigiere(record.msg, geheim)
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
    bestehende angehaengt, nicht ersetzt.
    """
    global _installiert
    if _installiert:
        return 0
    geheim = geheimnisse()
    if not geheim:
        return 0

    vorherige_fabrik = logging.getLogRecordFactory()

    def fabrik(*args, **kwargs):
        record = vorherige_fabrik(*args, **kwargs)
        _saeubere_record(record, geheim)
        return record

    logging.setLogRecordFactory(fabrik)

    wurzel = logger or logging.getLogger()
    for handler in wurzel.handlers:
        innerer = handler.formatter or logging.Formatter()
        if not isinstance(innerer, RedigierenderFormatter):
            handler.setFormatter(RedigierenderFormatter(innerer, geheim))

    _installiert = True
    return len(geheim)
