"""API-Keys dürfen nicht in den Logs stehen.

Gefunden am 2026-09-07 beim Debuggen des Politik-Guards. In
`docker compose logs backend` stand bei **jedem** Fact-Check-Aufruf:

    HTTP Request: GET https://factchecktools.googleapis.com/v1alpha1/
    claims:search?query=…&key=AIza… "HTTP/1.1 200 OK"

Kein Fehler im Evidora-Code: `httpx` loggt auf INFO die vollständige
Request-URL, und die Google-API erwartet den Key als Query-Parameter.

**Zwei Leck-Pfade, deshalb zwei Schichten.** Der erste ist der unauffälligere:
`httpx` reicht die URL als *Argument* durch (`logger.info('… %s …',
request.url)`), nicht im Meldungstext. Ein Filter, der nur `record.msg`
säubert — die naheliegende Implementierung — täte hier schlicht gar nichts.
Der zweite ist der Traceback: `httpx`-Exceptions tragen die volle URL
(„Client error '404 Not Found' for url '…key=…'"), und `main.py` loggt an vier
Stellen mit `exc_info=True`.

**Geschwärzt wird nach WERT, nicht nach Parametername.** Der naheliegende
Filter „alles hinter `key=`" ist in beide Richtungen falsch:

    zu viel:  oecd.py schickt key=.GWP.PT_WG_SAL_M_D._Z._Z.MEDIAN._T — ein
              SDMX-Datenselektor, also genau die Information, die man im Log
              braucht, um zu sehen, welcher Datensatz abgefragt wurde.
    zu wenig: europeana nennt den Parameter `wskey`, pubmed/cochrane/dpla
              nennen ihn `api_key`, uspstf `key`. Wer Namen aufzählt, vergisst
              welche — dieselbe Falle wie #141 und #143.

Die Suchmasken kommen daher aus der Umgebung. Ein neuer Konnektor mit einem
neuen Key ist automatisch abgedeckt, ohne dass jemand hier etwas nachträgt.
"""

import io
import logging
import sys
import warnings
from pathlib import Path

import httpx
import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services import _log_redaktion as LR  # noqa: E402

KEY = "AIzaSyFAKEKEY_nur_fuer_den_Test_12345"
KEY2 = "pubmedfakekey9876543210"
OECD_SELEKTOR = ".GWP.PT_WG_SAL_M_D._Z._Z.MEDIAN._T"

GEHEIM = [("GOOGLE_FACTCHECK_API_KEY", KEY), ("PUBMED_API_KEY", KEY2)]


# --------------------------------------------------------------------------
# Was als Geheimnis gilt
# --------------------------------------------------------------------------

def test_erkennt_geheimnisse_am_namensmuster():
    """Nicht am Konnektor-Namen, sondern am Muster — sonst fehlt der nächste."""
    umgebung = {
        "GOOGLE_FACTCHECK_API_KEY": KEY,
        "PUBMED_API_KEY": KEY2,
        "SOME_NEW_CONNECTOR_TOKEN": "tokenwert1234567",
        "IRGENDWAS_SECRET": "secretwert123456",
        "DB_PASSWORD": "passwortwert1234",
        "ALLOWED_ORIGINS": "http://localhost:3000",
        "LOG_LEVEL": "INFO",
    }
    namen = {n for n, _ in LR.geheimnisse(umgebung)}
    assert namen == {"GOOGLE_FACTCHECK_API_KEY", "PUBMED_API_KEY",
                     "SOME_NEW_CONNECTOR_TOKEN", "IRGENDWAS_SECRET",
                     "DB_PASSWORD"}


def test_kurze_und_leere_werte_zaehlen_nicht():
    """Ein leerer oder sehr kurzer Wert würde quer durch alle Zeilen matchen
    und das Log unlesbar machen — der teurere Fehler von beiden."""
    umgebung = {"A_API_KEY": "", "B_API_KEY": "   ", "C_API_KEY": "kurz",
                "D_API_KEY": "1234567", "E_API_KEY": "12345678"}
    assert [n for n, _ in LR.geheimnisse(umgebung)] == ["E_API_KEY"]


def test_credentials_pfad_ist_kein_wert():
    """GOOGLE_APPLICATION_CREDENTIALS zeigt auf eine Datei. Der Pfad im Log
    hilft bei der Fehlersuche und verrät nichts."""
    umgebung = {"GOOGLE_APPLICATION_CREDENTIALS": "/opt/Evidora/gcp-sa.json"}
    assert LR.geheimnisse(umgebung) == []


def test_laengste_zuerst():
    """Steckt ein Geheimnis als Teilstring in einem anderen, muss das längere
    zuerst ersetzt werden — sonst bleibt ein Rest stehen."""
    umgebung = {"A_API_KEY": "kurzgeheim1234", "B_API_KEY": "kurzgeheim1234_und_mehr"}
    werte = [w for _, w in LR.geheimnisse(umgebung)]
    assert werte == sorted(werte, key=len, reverse=True)
    geheim = LR.geheimnisse(umgebung)
    assert "kurzgeheim1234" not in LR.redigiere("x kurzgeheim1234_und_mehr y", geheim)


# --------------------------------------------------------------------------
# Der Kern: beide Leck-Pfade
# --------------------------------------------------------------------------

@pytest.fixture
def log_mit_redaktion(monkeypatch):
    """Frisches Logging mit installierter Redaktion, danach sauber zurück."""
    monkeypatch.setenv("GOOGLE_FACTCHECK_API_KEY", KEY)
    monkeypatch.setenv("PUBMED_API_KEY", KEY2)
    puffer = io.StringIO()
    logging.basicConfig(level=logging.INFO, stream=puffer, force=True)
    alte_fabrik = logging.getLogRecordFactory()
    monkeypatch.setattr(LR, "_installiert", False)
    anzahl = LR.installiere()
    assert anzahl >= 2
    yield puffer
    logging.setLogRecordFactory(alte_fabrik)
    logging.basicConfig(level=logging.INFO, force=True)


def _httpx_zeile(url):
    """Exakt so loggt httpx — die URL als ARGUMENT, nicht im Meldungstext."""
    logging.getLogger("httpx").info(
        'HTTP Request: %s %s "%s %d %s"', "GET", httpx.URL(url),
        "HTTP/1.1", 200, "OK")


def test_key_im_query_parameter_wird_geschwaerzt(log_mit_redaktion):
    """Der Fall, der das ausgelöst hat."""
    _httpx_zeile("https://factchecktools.googleapis.com/v1alpha1/"
                 f"claims:search?query=test&key={KEY}")
    text = log_mit_redaktion.getvalue()
    assert KEY not in text
    assert "<redigiert:GOOGLE_FACTCHECK_API_KEY>" in text
    assert "factchecktools.googleapis.com" in text, "Host muss lesbar bleiben"


def test_argument_pfad_wird_erwischt_nicht_nur_die_meldung(log_mit_redaktion):
    """Die eigentliche Falle: httpx steckt die URL in `args`, nicht in `msg`.
    Ein Filter auf `record.msg` allein wäre wirkungslos gewesen."""
    aufgezeichnet = {}

    class Merker(logging.Handler):
        def emit(self, record):
            aufgezeichnet["msg"] = record.msg
            aufgezeichnet["args"] = record.args

    logging.getLogger("httpx").addHandler(Merker())
    try:
        _httpx_zeile(f"https://x.test/a?key={KEY}")
    finally:
        logging.getLogger("httpx").handlers.clear()
    assert KEY not in str(aufgezeichnet["args"]), aufgezeichnet["args"]
    assert KEY not in str(aufgezeichnet["msg"])


def test_traceback_wird_geschwaerzt(log_mit_redaktion):
    """httpx-Exceptions tragen die volle URL; main.py loggt viermal mit
    exc_info=True."""
    url = f"https://factchecktools.googleapis.com/v1alpha1/x?key={KEY}"
    try:
        httpx.Response(404, request=httpx.Request("GET", url)).raise_for_status()
    except Exception:
        logging.getLogger("evidora").error("ClaimReview fehlgeschlagen",
                                           exc_info=True)
    text = log_mit_redaktion.getvalue()
    assert "for url" in text, "Traceback fehlt — Test misst nichts"
    assert KEY not in text


def test_zweiter_key_mit_anderem_parameternamen(log_mit_redaktion):
    """pubmed/cochrane/dpla nennen ihn `api_key`, europeana `wskey`."""
    _httpx_zeile(f"https://eutils.ncbi.nlm.nih.gov/e?db=pubmed&api_key={KEY2}")
    text = log_mit_redaktion.getvalue()
    assert KEY2 not in text
    assert "<redigiert:PUBMED_API_KEY>" in text


# --------------------------------------------------------------------------
# Die Gegenprobe — der Grund für „nach Wert, nicht nach Name"
# --------------------------------------------------------------------------

def test_oecd_datenselektor_bleibt_unversehrt(log_mit_redaktion):
    """`key=` ist bei OECD-SDMX kein Geheimnis, sondern der Datensatz-Selektor
    — und damit genau die Information, die ein Log tragen soll. Ein Filter auf
    den Parameternamen hätte sie zerstört."""
    _httpx_zeile(f"https://sdmx.oecd.org/public/rest/data/X?key={OECD_SELEKTOR}")
    text = log_mit_redaktion.getvalue()
    assert OECD_SELEKTOR in text
    assert "<redigiert" not in text


def test_normale_logzeilen_bleiben_unberuehrt(log_mit_redaktion):
    logging.getLogger("evidora").info("Reranker using shared Sentence "
                                      "Transformer instance")
    logging.getLogger("evidora").warning("freedom_house: static JSON fehlt")
    text = log_mit_redaktion.getvalue()
    assert "Reranker using shared Sentence Transformer instance" in text
    assert "freedom_house: static JSON fehlt" in text
    assert "<redigiert" not in text


# --------------------------------------------------------------------------
# Robustheit
# --------------------------------------------------------------------------

def test_nicht_string_argumente_behalten_ihren_typ():
    """`%d` auf einem str wäre ein TypeError. Nur Argumente, die tatsächlich
    ein Geheimnis enthalten, werden zu Text."""
    assert LR._redigiere_wert(200, GEHEIM) == 200
    assert LR._redigiere_wert(None, GEHEIM) is None
    assert LR._redigiere_wert(1.5, GEHEIM) == 1.5
    url = LR._redigiere_wert(httpx.URL(f"https://x.test/?key={KEY}"), GEHEIM)
    assert isinstance(url, str) and KEY not in url


def test_installieren_ist_idempotent(monkeypatch):
    """Ein zweiter Aufruf darf die Fabrik nicht ein zweites Mal stapeln."""
    monkeypatch.setenv("GOOGLE_FACTCHECK_API_KEY", KEY)
    monkeypatch.setattr(LR, "_installiert", False)
    alte = logging.getLogRecordFactory()
    try:
        assert LR.installiere() >= 1
        assert LR.installiere() == 0
    finally:
        logging.setLogRecordFactory(alte)
        monkeypatch.setattr(LR, "_installiert", False)


def test_ohne_geheimnisse_passiert_nichts(monkeypatch):
    """In einer Umgebung ohne Keys bleibt die Fabrik unangetastet — kein
    Overhead auf dem Log-Pfad."""
    for name, _ in LR.geheimnisse():
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(LR, "_installiert", False)
    alte = logging.getLogRecordFactory()
    assert LR.installiere() == 0
    assert logging.getLogRecordFactory() is alte


# --------------------------------------------------------------------------
# Verdrahtung
# --------------------------------------------------------------------------

def test_main_installiert_vor_dem_ersten_request():
    """Die Reihenfolge entscheidet: nach basicConfig (die Handler müssen
    existieren), vor dem ersten HTTP-Aufruf."""
    quelle = (BACKEND / "main.py").read_text(encoding="utf-8")
    assert "_log_redaktion import installiere" in quelle
    i_basic = quelle.index("logging.basicConfig")
    i_red = quelle.index("_redaktion_installieren()")
    assert i_basic < i_red, "Redaktion vor basicConfig — die Handler fehlen dann"
