"""Eingegebene Behauptungen stehen nicht im Log — auch nicht als Suchbegriff.

Die Datenschutzerklaerung (frontend/i18n.js, Abschnitt d) sagt: „Deine
eingegebenen Behauptungen werden nicht geloggt." Gemessen am 4.11.2026 im
Prod-Log stimmte das nicht:

* ``httpx`` protokollierte 884 von 2.686 Request-URLs MIT Suchbegriffen aus
  dem Claim, etwa ``openalex.org/works?search=child+height+booster+seat+…+age+5``
  zu „Mein Kind ist groß genug für die Sitzerhöhung mit 5 Jahren". Wikipedia
  traegt den Begriff sogar im PFAD (``/page/summary/%C3%96sterreichs…``);
* zwoelf eigene Zeilen gaben den Claim selbst aus
  (``verdict_cache: STORED [de] 'Väter nehmen …'``), 61 weitere in 29
  Diensten die daraus gebauten Suchbegriffe (``EFSA: 0 results for '…'``);
* bei Parse-Fehlern stand die Rohantwort des Modells im Log, also Claim und
  Begruendung.

Der Fix folgt dem Grundsatz der Key-Schwaerzung von 2026-09-07: nach WERT,
nicht nach Name. ``main.py`` meldet die Woerter jeder Anfrage an, die
Log-Redaktion ersetzt sie in jeder Zeile — auch in URLs, die dafuer vorher
entschluesselt werden. Die direkten Stellen geben eine gesalzene Kennung aus
(``claim_kennung``), die Parse-Fehler nur die Struktur (``nur_struktur``).
Ein AST-Gate haelt neue Stellen fern.

Keine Netzabfrage in diesen Tests.
"""

import ast
import io
import logging
import threading
from pathlib import Path

import httpx
import pytest

from services import _log_redaktion as LR

BACKEND = Path(__file__).resolve().parents[1]

#: Live 4.11.2026, Messung 3c des Audits: Claim und die httpx-Zeile dazu.
SITZ_CLAIM = "Mein Kind ist groß genug für die Sitzerhöhung mit 5 Jahren"
SITZ_ANALYSE = {
    "claim": "Ein fünfjähriges Kind ist groß genug für eine Sitzerhöhung",
    "category": "health", "subcategory": "child_safety",
    "entities": ["Sitzerhöhung", "Kind"],
    "pubmed_queries": ["child height booster seat safety",
                       "booster seat height requirements children"],
    "factcheck_queries": ["Sitzerhöhung Kind Alter"],
}
SITZ_URL = ("https://api.openalex.org/works?search=child+height+booster+seat+"
            "safety+booster+seat+height+requirements+children&filter="
            "is_retracted%3Afalse%2Ctype%3Aarticle&per_page=5")


@pytest.fixture
def log_puffer(monkeypatch):
    """Frisches Logging mit installierter Redaktion, danach sauber zurueck."""
    puffer = io.StringIO()
    logging.basicConfig(level=logging.INFO, stream=puffer, force=True)
    alte = logging.getLogRecordFactory()
    monkeypatch.setattr(LR, "_installiert", False)
    LR.vergiss_anfragen()
    LR.installiere()
    yield puffer
    LR.vergiss_anfragen()
    logging.setLogRecordFactory(alte)
    logging.basicConfig(level=logging.INFO, force=True)


def _httpx_zeile(url):
    """Exakt so loggt httpx — die URL als ARGUMENT."""
    logging.getLogger("httpx").info(
        'HTTP Request: %s %s "%s %d %s"', "GET", httpx.URL(url),
        "HTTP/1.1", 200, "OK")


def _anmelden():
    LR.merke_anfrage(SITZ_CLAIM)
    LR.merke_anfrage({k: SITZ_ANALYSE.get(k) for k in LR.ANFRAGE_FELDER})


# --------------------------------------------------------------------------
# Der Anlass: httpx-URLs
# --------------------------------------------------------------------------

def test_giftprobe_ohne_anmeldung_steht_der_suchbegriff_im_log(log_puffer):
    """Sonst bewiese der naechste Test nichts."""
    _httpx_zeile(SITZ_URL)
    assert "booster" in log_puffer.getvalue()


def test_suchbegriffe_in_der_url_werden_geschwaerzt(log_puffer):
    _anmelden()
    _httpx_zeile(SITZ_URL)
    text = log_puffer.getvalue()
    for wort in ("booster", "height", "children", "safety"):
        assert wort not in text, wort
    assert "api.openalex.org" in text, "Host muss lesbar bleiben"
    assert "HTTP/1.1" in text


def test_begriff_im_pfad_und_prozentkodiert(log_puffer):
    """Wikipedia: der Begriff steht im PFAD, Umlaute als %C3%96."""
    LR.merke_anfrage("Österreichs E-Wirtschaft ist zu 80 Prozent erneuerbar")
    _httpx_zeile("https://de.wikipedia.org/api/rest_v1/page/summary/"
                 "%C3%96sterreichs%20E-Wirtschaft")
    text = log_puffer.getvalue()
    assert "sterreich" not in text and "Wirtschaft" not in text, text
    assert "de.wikipedia.org" in text


def test_eine_url_ohne_angemeldetes_wort_bleibt_zeichengleich(log_puffer):
    """Der OECD-Selektor aus test_log_redaktion — Gegenprobe mit aktiven Woertern."""
    _anmelden()
    url = "https://sdmx.oecd.org/public/rest/data/X?key=.GWP.PT_WG_SAL_M_D._Z._Z.MEDIAN._T"
    _httpx_zeile(url)
    assert url in log_puffer.getvalue()


def test_traceback_mit_url_wird_geschwaerzt(log_puffer):
    """httpx legt die volle URL in den Exception-Text."""
    _anmelden()
    try:
        httpx.Response(404, request=httpx.Request("GET", SITZ_URL)).raise_for_status()
    except Exception:
        logging.getLogger("evidora").error("OpenAlex fehlgeschlagen", exc_info=True)
    text = log_puffer.getvalue()
    assert "for url" in text, "Traceback fehlt — Test misst nichts"
    assert "booster" not in text


# --------------------------------------------------------------------------
# Eigene Logzeilen
# --------------------------------------------------------------------------

def test_eigene_zeile_mit_suchbegriff(log_puffer):
    """So schreiben ~60 Dienste: f-String mit dem Suchbegriff."""
    _anmelden()
    term = SITZ_ANALYSE["pubmed_queries"][0]
    logging.getLogger("evidora").info(f"EFSA: 0 results for '{term[:80]}'")
    text = log_puffer.getvalue()
    assert "EFSA: 0 results for" in text
    assert "booster" not in text and "child" not in text


def test_flexion_wird_mitgeschwaerzt(log_puffer):
    LR.merke_anfrage("Die meisten Schweinehalter in Österreich haben nur ein paar Tiere")
    logging.getLogger("evidora").info("Treffer: Schweinehaltern, Österreichs Bauern")
    text = log_puffer.getvalue()
    assert "Schweinehaltern" not in text and "Österreichs" not in text, text
    assert "Bauern" in text, "nicht angemeldet, bleibt"


def test_funktionswoerter_bleiben(log_puffer):
    LR.merke_anfrage("Antibiotika helfen nicht gegen eine Erkältung")
    logging.getLogger("evidora").info("Quelle lieferte nicht eine Zeile")
    assert "lieferte nicht eine Zeile" in log_puffer.getvalue()


def test_kategorie_wird_nicht_angemeldet():
    """Die Kategorie nennt die Datenschutzerklaerung ausdruecklich als geloggt."""
    assert "category" not in LR.ANFRAGE_FELDER
    assert "subcategory" not in LR.ANFRAGE_FELDER


def test_angemeldete_woerter_verfallen(monkeypatch):
    LR.vergiss_anfragen()
    jetzt = [1000.0]
    monkeypatch.setattr(LR.time, "monotonic", lambda: jetzt[0])
    LR.merke_anfrage("Sitzerhöhung")
    assert LR.schwaerze_anfrage("x Sitzerhöhung") == f"x {LR.SCHWARZ}"
    jetzt[0] += LR.ANFRAGE_GEDAECHTNIS_S + 1
    assert LR.schwaerze_anfrage("x Sitzerhöhung") == "x Sitzerhöhung"
    LR.vergiss_anfragen()


def test_auch_aus_einem_thread(log_puffer):
    """Global statt ContextVar: ein Thread saehe den Kontext der Anfrage nicht."""
    _anmelden()
    t = threading.Thread(target=lambda: logging.getLogger("evidora").info(
        "Thread: Sitzerhöhung geprüft"))
    t.start()
    t.join()
    assert "Sitzerhöhung" not in log_puffer.getvalue()


# --------------------------------------------------------------------------
# Robustheit: Formate duerfen nicht brechen
# --------------------------------------------------------------------------

def test_dict_argumente_brechen_nicht(log_puffer):
    """Ein geschwaerztes Wort im Format ("%(name)s") waere ein KeyError."""
    LR.merke_anfrage("Der Name des Ministers ist falsch")
    logging.getLogger("evidora").info("Feld %(name)s gesetzt", {"name": "Minister"})
    text = log_puffer.getvalue()
    assert "Feld" in text and "Minister" not in text, text
    assert "Logging error" not in text


def test_tupel_argumente_behalten_ihre_typen(log_puffer):
    """uvicorn.access entpackt record.args — die Typen muessen bleiben."""
    LR.merke_anfrage("Ein check ohne Sinn")
    rec = logging.getLogRecordFactory()("uvicorn.access", logging.INFO, "x", 1,
                                        '%s - "%s %s HTTP/%s" %d',
                                        ("1.2.3.4:5", "POST", "/api/check", "1.1", 200), None)
    assert isinstance(rec.args[4], int)
    rec.getMessage()


def test_feldnamen_vor_dem_gleichheitszeichen_bleiben(log_puffer):
    """Live 4.11.2026: "Mein Kind ist …" schwaerzte "llm_usage kind=analysis"."""
    LR.merke_anfrage("Mein Kind ist groß genug, brown sugar")
    logging.getLogger("evidora").info("AHRQ: 0 hits kind=brown term=sugar")
    text = log_puffer.getvalue()
    assert "kind=" in text and "term=" in text, text
    assert "brown" not in text and "sugar" not in text, "Werte werden geschwaerzt"


def test_unterstrich_trennt_wie_im_wikipedia_titel(log_puffer):
    LR.merke_anfrage("Kamala Harris hat gesagt")
    _httpx_zeile("https://en.wikipedia.org/api/rest_v1/page/summary/Kamala_Harris")
    text = log_puffer.getvalue()
    assert "Kamala" not in text and "Harris" not in text, text


def test_die_usage_zeile_bleibt_lesbar(log_puffer):
    """Die Cache-Messung liest sie (project_offen_cache_anteil)."""
    from services import ollama
    LR.merke_anfrage("latest analysis of medium sized cars")   # Woerter der Zeile
    logging.getLogger("evidora").info(
        f"{ollama.USAGE_LOG_PREFIX} kind=analysis model=mistral-medium-latest "
        f"attempt=1 cached_tokens=0")
    assert "kind=analysis model=mistral-medium-latest" in log_puffer.getvalue()


def test_usage_praefix_ist_synchron():
    from services import ollama
    assert ollama.USAGE_LOG_PREFIX + " " in LR._OHNE_ANFRAGE_INHALT


def test_die_usage_zeile_enthaelt_keinen_claim():
    """Die Ausnahme ist nur haltbar, solange die Zeile keine Claim-Variable traegt."""
    quelle = (BACKEND / "services" / "ollama.py").read_text(encoding="utf-8")
    baum = ast.parse(quelle)
    for n in ast.walk(baum):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr == "info" and n.args
                and "USAGE_LOG_PREFIX" in ast.dump(n.args[0])):
            namen = {x.id for x in ast.walk(n.args[0]) if isinstance(x, ast.Name)}
            assert namen <= {"USAGE_LOG_PREFIX", "kind", "model", "attempt", "stream_flag",
                             "prompt", "cached", "reasoning", "completion", "total"}, namen
            return
    pytest.fail("usage-Zeile in ollama.py nicht gefunden")


# --------------------------------------------------------------------------
# Kennung und Struktur statt Text
# --------------------------------------------------------------------------

def test_claim_kennung_ordnet_zu_ohne_den_text():
    a = LR.claim_kennung("Väter nehmen in Österreich selten Karenz")
    assert a == LR.claim_kennung("Väter nehmen in Österreich selten Karenz")
    assert a != LR.claim_kennung("Väter nehmen in Österreich oft Karenz")
    assert "Väter" not in a and "Karenz" not in a
    assert a.endswith("(40 Zeichen)")


def test_claim_kennung_ist_gesalzen(monkeypatch):
    """Ohne Salz liesse sich ein vermuteter Claim gegen das Log pruefen."""
    a = LR.claim_kennung("Väter nehmen in Österreich selten Karenz")
    monkeypatch.setattr(LR, "_SALZ", b"anderes salz 123")
    assert LR.claim_kennung("Väter nehmen in Österreich selten Karenz") != a


def test_nur_struktur_zeigt_warum_ein_json_nicht_parst():
    roh = '{"summary": "Väter nehmen 4,1 % der Tage", "verdict": "tr'
    s = LR.nur_struktur(roh)
    assert '"summary":' in s and '"verdict":' in s
    assert "Väter" not in s and "4,1" not in s
    assert s.endswith('"x'), "abgeschnittenes Ende bleibt sichtbar"


# --------------------------------------------------------------------------
# Verdrahtung in main.py
# --------------------------------------------------------------------------

def _main():
    return (BACKEND / "main.py").read_text(encoding="utf-8")


def test_der_claim_wird_vor_dem_cache_angemeldet():
    """Der Cache loggt EXACT HIT, bevor irgendetwas anderes laeuft."""
    q = _main()
    assert q.index("merke_anfrage(claim)") < q.index("_vc.get(claim, lang)")


def test_die_analyse_wird_vor_ihrer_logzeile_angemeldet():
    q = _main()
    i = q.index("merke_anfrage({k: analysis.get(k) for k in ANFRAGE_FELDER})")
    assert q.index("analysis = enrich_entities(claim, analysis)") < i
    assert i < q.index('logger.info(f"Analysis done:')


# --------------------------------------------------------------------------
# AST-Gate: keine neue Logzeile mit dem Claim
# --------------------------------------------------------------------------

LOGGER_NAMEN = {"logger", "log", "logging", "_logger", "LOGGER", "_log"}
METHODEN = {"debug", "info", "warning", "warn", "error", "exception", "critical", "log"}
#: Variablen, die den Claim tragen — nur durch einen dieser Aufrufe hindurch erlaubt.
CLAIM_NAMEN = {"claim", "original_claim", "claim_lc", "full_claim", "claim_text", "raw_claim"}
#: Rohantworten des Modells.
INHALT_NAMEN = {"content", "retry_content", "last_content"}
#: Schluessel der Analyse mit Claim-Inhalt.
CLAIM_SCHLUESSEL = {"claim", "original_claim", "entities", "pubmed_queries", "factcheck_queries"}
ERLAUBT = {"len", "claim_kennung", "nur_struktur"}


def _verstoesse_in(knoten):
    raus = []

    def geh(n):
        if isinstance(n, ast.Call):
            f = n.func
            fname = f.id if isinstance(f, ast.Name) else (f.attr if isinstance(f, ast.Attribute) else "")
            if fname in ERLAUBT:
                return
            if (fname == "get" and n.args and isinstance(n.args[0], ast.Constant)
                    and n.args[0].value in CLAIM_SCHLUESSEL):
                raus.append(f".get({n.args[0].value!r})")
        if isinstance(n, ast.Name) and n.id in CLAIM_NAMEN | INHALT_NAMEN:
            raus.append(n.id)
        if (isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant)
                and n.slice.value in CLAIM_SCHLUESSEL):
            raus.append(f"[{n.slice.value!r}]")
        for kind in ast.iter_child_nodes(n):
            geh(kind)

    geh(knoten)
    return raus


def _logger_aufrufe():
    dateien = sorted((BACKEND / "services").glob("*.py")) + [BACKEND / "main.py"]
    for p in dateien:
        baum = ast.parse(p.read_text(encoding="utf-8"))
        for n in ast.walk(baum):
            if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                    and n.func.attr in METHODEN):
                continue
            v = n.func.value
            vn = v.id if isinstance(v, ast.Name) else (v.attr if isinstance(v, ast.Attribute) else "")
            if vn in LOGGER_NAMEN:
                yield p, n


def test_keine_logzeile_gibt_den_claim_aus():
    verstoesse = []
    for p, n in _logger_aufrufe():
        for arg in list(n.args) + [k.value for k in n.keywords]:
            for v in _verstoesse_in(arg):
                verstoesse.append(f"{p.relative_to(BACKEND)}:{n.lineno} {v}")
    assert not verstoesse, "\n".join(verstoesse)


def test_das_gate_findet_den_alten_fall():
    """Giftprobe: die Zeile, die im Prod-Log stand."""
    alt = ast.parse('logger.info(f"verdict_cache: STORED [{lang}] {claim[:60]!r}")')
    assert _verstoesse_in(alt.body[0].value.args[0]) == ["claim"]
    neu = ast.parse('logger.info(f"verdict_cache: STORED [{lang}] {claim_kennung(claim)}")')
    assert _verstoesse_in(neu.body[0].value.args[0]) == []


def test_das_gate_sieht_genug_logger_aufrufe():
    """Sonst liefe es leer und waere gruen."""
    assert sum(1 for _ in _logger_aufrufe()) > 500
