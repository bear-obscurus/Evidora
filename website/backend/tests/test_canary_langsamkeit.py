"""Der Kanarienvogel meldet Langsamkeit, nicht nur Fehler.

Am 20.11.2026 dauerte eine Synthese 128 s. Der Kanarienvogel kannte nur
zwei Befunde: "kein brauchbares Ergebnis" (Exit 1) und "nicht erreichbar"
(Exit 3). Eine Pruefung, die durchlaeuft, aber zwei Minuten braucht, war
fuer ihn gesund.

Diese Tests fahren ``main()`` komplett durch, mit einem simulierten
Backend und einer simulierten Uhr: ``urlopen`` im Werkzeug wird durch eine
Attrappe ersetzt, die SSE-Zeilen liefert und dabei die Uhr vorstellt. Kein
Netz, kein Modell, keine echte Wartezeit. Die Attrappe faengt auch den
ntfy-Push ab, damit der Test pruefen kann, WAS gemeldet wird.

Die Attrappe sitzt bewusst auf der HTTP-Ebene und nicht an den inneren
Funktionen: So laufen dieselben Faelle gegen den alten Code (Gegenbeweis)
und gegen den neuen.
"""
import importlib.util
import json
import os
import sys

import pytest

TOOLS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(
    __file__))), "tools")

BASE = "http://backend.test:8000"
#: Ein Topic, der in keiner Meldung auftauchen darf.
WEBHOOK = "https://ntfy.example/geheimer-topic-4711"


def _lade_werkzeug():
    spec = importlib.util.spec_from_file_location(
        "canary_check_unter_test", os.path.join(TOOLS, "canary_check.py"))
    modul = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modul)
    return modul


class Uhr:
    def __init__(self):
        self.jetzt = 1000.0

    def __call__(self):
        return self.jetzt


class _Antwort:
    """Was ``urlopen`` zurueckgibt: Kontextmanager, ``read()``, iterierbar."""

    def __init__(self, uhr, zeilen=(), koerper=b""):
        self._uhr = uhr
        self._zeilen = zeilen
        self._koerper = koerper

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._koerper

    def __iter__(self):
        for warte, zeile in self._zeilen:
            self._uhr.jetzt += warte
            yield (zeile + "\n").encode("utf-8")


def _sse(analyse_s, suche_s, synthese_s, verdict=True, cache_hit=False):
    """Ein SSE-Strom wie aus ``/api/check``: (Wartezeit davor, Zeile)."""
    z = [(0.0, "event: step"),
         (0.0, 'data: {"step": "analyze"}'), (0.0, "")]
    if cache_hit:
        z += [(0.1, "event: step"), (0.0, 'data: {"step": "cache_hit"}'),
              (0.0, "")]
    else:
        z += [(analyse_s, "event: step"), (0.0, 'data: {"step": "search"}'),
              (0.0, ""),
              (suche_s, "event: step"),
              (0.0, 'data: {"step": "synthesize"}'), (0.0, ""),
              (synthese_s / 2, "event: synth_chunk"),
              (0.0, 'data: {"text": "{\\"verdict\\": \\"tr"}'), (0.0, "")]
        synthese_s = synthese_s / 2
    if verdict:
        ergebnis = {"verdict": "true", "confidence": 0.85,
                    "source_coverage": {"with_results": 5}}
        z += [(0.0 if cache_hit else synthese_s, "event: result"),
              (0.0, "data: " + json.dumps(ergebnis)), (0.0, "")]
    else:
        z += [(synthese_s, "event: error"),
              (0.0, 'data: {"detail": "Fehler bei der Ergebnis-Synthese."}'),
              (0.0, "")]
    z += [(0.0, "event: done"), (0.0, "data: {}"), (0.0, "")]
    return z


def _lauf(monkeypatch, capsys, sse=None, erreichbar=True):
    """Fuehrt ``main()`` aus. Ergebnis: (Exit-Code, Pushes, stdout)."""
    cc = _lade_werkzeug()
    uhr = Uhr()
    pushes = []

    def falsches_urlopen(req, timeout=None):
        url = getattr(req, "full_url", req)
        if url.startswith(WEBHOOK):
            pushes.append({"titel": req.get_header("Title"),
                           "text": req.data.decode("utf-8"),
                           "kopf": dict(req.header_items())})
            return _Antwort(uhr)
        if not erreichbar:
            raise ConnectionRefusedError(111, "Connection refused")
        if url.endswith("/api/health/full"):
            uhr.jetzt += 0.05
            return _Antwort(uhr, koerper=json.dumps(
                {"details": {"llm_auth": "HTTP 200"}}).encode())
        if url.endswith("/api/check"):
            return _Antwort(uhr, zeilen=sse)
        raise AssertionError(f"unerwartete URL {url}")

    monkeypatch.setattr(cc.urllib.request, "urlopen", falsches_urlopen)
    # raising=False: Der alte Code hat keine Uhr. Fuer ihn ist das ein
    # unbenutztes Attribut, und die Faelle laufen trotzdem durch.
    monkeypatch.setattr(cc, "_uhr", uhr, raising=False)
    monkeypatch.setenv("EVIDORA_ALERT_WEBHOOK", WEBHOOK)
    monkeypatch.delenv("EVIDORA_TEST_API_KEY", raising=False)
    monkeypatch.delenv("EVIDORA_CANARY_URL", raising=False)
    monkeypatch.setattr(sys, "argv", ["canary_check.py", "--url", BASE])
    code = cc.main()
    return code, pushes, capsys.readouterr().out, cc


# --- Unter der Schwelle: kein Befund ------------------------------------

def test_normaler_lauf_ist_kein_befund(monkeypatch, capsys):
    # Wie ein typischer Morgen seit 5.11.: ~12 s gesamt, ~5 s Synthese.
    code, pushes, _, _ = _lauf(monkeypatch, capsys, _sse(2, 5, 5))
    assert code == 0
    assert pushes == []


def test_knapp_unter_beiden_schwellen_ist_kein_befund(monkeypatch, capsys):
    # 59 s gesamt, davon 29 s Synthese.
    code, pushes, _, _ = _lauf(monkeypatch, capsys, _sse(10, 19.9, 29))
    assert code == 0
    assert pushes == []


def test_cache_treffer_ohne_synthese_ist_kein_befund(monkeypatch, capsys):
    code, pushes, _, _ = _lauf(monkeypatch, capsys,
                               _sse(0, 0, 0, cache_hit=True))
    assert code == 0
    assert pushes == []


# --- Ueber der Schwelle: Befund mit eigenem Exit-Code -------------------

def test_langsame_gesamtdauer_ist_befund(monkeypatch, capsys):
    # 75 s gesamt, die Synthese selbst ist mit 5 s normal: Die Zeit geht
    # in Analyse und Quellen verloren.
    code, pushes, _, cc = _lauf(monkeypatch, capsys, _sse(30, 40, 5))
    assert code == cc.LANGSAM_EXIT
    assert len(pushes) == 1
    assert "langsam" in pushes[0]["titel"].lower()
    assert "Gesamt" in pushes[0]["text"]
    ueberschritten = (pushes[0]["text"].split("Ueberschritten")[-1]
                      .split("Gemessen")[0])
    assert "Gesamt" in ueberschritten
    assert "Synthese" not in ueberschritten


def test_langsame_synthese_ist_befund(monkeypatch, capsys):
    # Wie am 20.11.: Die Synthese allein braucht zu lange (40 s), die
    # Pruefung insgesamt bleibt mit 50 s unter ihrer Schwelle.
    code, pushes, _, cc = _lauf(monkeypatch, capsys, _sse(3, 7, 40))
    assert code == cc.LANGSAM_EXIT
    assert len(pushes) == 1
    assert "Synthese" in pushes[0]["text"]


def test_beides_langsam_nennt_beides(monkeypatch, capsys):
    code, pushes, _, cc = _lauf(monkeypatch, capsys, _sse(3, 7, 128))
    assert code == cc.LANGSAM_EXIT
    assert len(pushes) == 1
    text = pushes[0]["text"]
    assert "Gesamt" in text and "Synthese" in text
    assert "128" in text


def test_langsam_hat_einen_eigenen_exit_code_und_ist_ein_befund(
        monkeypatch, capsys):
    code, _, _, cc = _lauf(monkeypatch, capsys, _sse(3, 7, 128))
    assert code not in (0, 1, 2, 3)
    assert code in cc.BEFUND_EXITS
    # Kollidiert nicht mit dem, was Wrapper und Docker selbst liefern:
    # 64 (Aufruf), 75 (flock), 125-127 (docker), 137 (OOM).
    assert code not in (64, 75, 125, 126, 127, 137)


def test_langsam_meldung_eigener_titel(monkeypatch, capsys):
    """Langsam ist nicht kaputt: anderer Titel als der Fehler-Alarm."""
    _, langsam, _, _ = _lauf(monkeypatch, capsys, _sse(3, 7, 128))
    _, kaputt, _, _ = _lauf(monkeypatch, capsys, _sse(1, 1, 1,
                                                      verdict=False))
    assert langsam and kaputt
    assert langsam[0]["titel"] != kaputt[0]["titel"]


def test_langsam_meldung_ohne_claim_und_ohne_topic(monkeypatch, capsys):
    code, pushes, out, cc = _lauf(monkeypatch, capsys, _sse(3, 7, 128))
    assert code == cc.LANGSAM_EXIT and len(pushes) == 1
    p = pushes[0]
    alles = p["text"] + " " + json.dumps(p["kopf"], ensure_ascii=False)
    assert cc.CANARY_CLAIM not in alles
    assert "Wien" not in alles
    assert "geheimer-topic-4711" not in alles
    assert "geheimer-topic-4711" not in out


def test_dauern_stehen_im_log(monkeypatch, capsys):
    """Damit die naechste Schwellenwahl Daten hat: canary.log hatte bis
    21.11.2026 keine einzige Dauer."""
    _, _, out, _ = _lauf(monkeypatch, capsys, _sse(2, 5, 5))
    assert "gesamt=" in out and "synthese=" in out


# --- Fehler wie bisher ----------------------------------------------------

def test_fehler_bleibt_exit_1_auch_wenn_langsam(monkeypatch, capsys):
    # Kein Verdict UND langsam: Der Fehler ist der wichtigere Befund.
    code, pushes, _, _ = _lauf(monkeypatch, capsys,
                               _sse(30, 40, 50, verdict=False))
    assert code == 1
    assert len(pushes) == 1
    assert pushes[0]["titel"] == "Evidora Kanarienvogel ALARM"


def test_nicht_erreichbar_bleibt_exit_3(monkeypatch, capsys):
    code, pushes, _, _ = _lauf(monkeypatch, capsys, erreichbar=False)
    assert code == 3
    assert len(pushes) == 1
    assert "nicht erreichbar" in pushes[0]["titel"]


@pytest.mark.parametrize("sse", [_sse(2, 5, 5), _sse(3, 7, 128)])
def test_genau_ein_push_je_lauf(monkeypatch, capsys, sse):
    _, pushes, _, _ = _lauf(monkeypatch, capsys, sse)
    assert len(pushes) <= 1
