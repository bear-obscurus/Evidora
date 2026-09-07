"""Der Monitor, der WGI gefunden hätte — und der Schutz gegen sein Wegdriften.

WGI lieferte vom 17.05. bis 07.09.2026 für jedes Land null Ergebnisse. Die
Weltbank quittiert eine kaputte Abfrage mit **HTTP 200 und einem Fehler-Objekt
im Rumpf**; `raise_for_status` greift nicht, der Konnektor cachte still eine
leere Liste, und im Log stand `returned 0 results` — von „diese Quelle hat
nichts" nicht zu unterscheiden. Gemeldet hat es nichts: der Kanarienvogel
(#107) prüft die *Pipeline* mit einem Claim, die CI war grün, und kein Test
spricht mit einer echten API.

Diese Tests prüfen zwei Dinge, und **keiner von beiden geht ins Netz** — sonst
wäre die Suite von der Verfügbarkeit fremder Server abhängig:

  1. **Die Logik.** Erkennt der Monitor ein Fehler-Objekt als „kaputte
     Abfrage" und unterscheidet es von „stumm"? Gegen synthetische Nutzlasten,
     inklusive der echten Weltbank-Fehlermeldung.

  2. **Der Drift-Schutz.** Der Monitor hält seine eigenen URLs — bewusst, damit
     er ohne Import der Service-Module auskommt (⛔ kein zweiter schwerer
     Python-Prozess neben dem Prod-Container). Der Preis ist eine zweite
     Kopie, und eine Zahl in zwei Kopien driftet. Deshalb prüft ein Test, dass
     jede Sonden-URL noch als Konstante im zugehörigen Service steht — und für
     WGI zusätzlich, dass Quelle und ID-Präfix übereinstimmen.

Was diese Tests NICHT leisten: sie sagen nichts darüber, ob die Quelle heute
antwortet. Das ist der Job des Cron-Laufs — und genau deshalb gibt es ihn.
"""

import ast
import importlib.util
import json
import re
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

_spec = importlib.util.spec_from_file_location(
    "quellen_lebenszeichen", BACKEND / "tools" / "quellen_lebenszeichen.py")
QM = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(QM)

# Die echte Antwort der Weltbank auf die vier Monate lang laufende Abfrage.
WELTBANK_FEHLER = json.dumps(
    [{"message": [{"id": "120", "key": "Invalid value",
                   "value": "The provided parameter value is not valid"}]}])
WELTBANK_LEER = json.dumps([{"page": 0, "pages": 0, "total": 0}, None])
WELTBANK_GUT = json.dumps(
    [{"page": 1, "total": 2},
     [{"countryiso3code": "AUT", "date": "2024", "value": 1.68},
      {"countryiso3code": "AUT", "date": "2023", "value": 1.66}]])


# --------------------------------------------------------------------------
# 1) Die Logik — kaputt ist nicht dasselbe wie leer
# --------------------------------------------------------------------------

def test_weltbank_fehlerobjekt_wird_erkannt():
    """Der Kern. Ohne diese Unterscheidung sieht ein Totalausfall aus wie
    „zu diesem Land liegt nichts vor"."""
    assert QM._p_weltbank(WELTBANK_FEHLER) == -1


def test_weltbank_leer_ist_nicht_kaputt():
    assert QM._p_weltbank(WELTBANK_LEER) == 0


def test_weltbank_daten_werden_gezaehlt():
    assert QM._p_weltbank(WELTBANK_GUT) == 2


@pytest.mark.parametrize("zeilen,erwartet", [
    (-1, "kaputte_abfrage"),
    (0, "stumm"),
    (3, "ok"),
])
def test_zeilenzahl_wird_zum_status(monkeypatch, zeilen, erwartet):
    """Die Abbildung Zeilenzahl → Status, ohne Netz."""
    class _Antwort:
        status = 200
        def read(self): return b"{}"
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(QM.urllib.request, "urlopen", lambda *a, **k: _Antwort())
    sonde = ("Test", "wgi", "https://example.test/x", {}, lambda t: zeilen, None)
    assert QM.sonde_laufen(sonde, 5)["status"] == erwartet


def test_alle_auffaelligen_zustaende_loesen_alarm_aus():
    """Stille ist der Fehler, gegen den das hier gebaut ist — sie darf nicht
    versehentlich als „nur ein Hinweis" durchgehen."""
    assert QM.ALARM == {"kaputte_abfrage", "stumm", "http_fehler",
                        "unlesbar", "unerreichbar"}
    assert "ok" not in QM.ALARM and "uebersprungen" not in QM.ALARM


def test_fehlender_token_ist_kein_alarm(monkeypatch):
    """Ein nicht gesetzter Token ist eine Konfigurations-Aussage, kein
    Quellen-Ausfall. Ein Wächter, der dafür schreit, wird abgeschaltet (#128)."""
    monkeypatch.delenv("UCDP_TOKEN", raising=False)
    sonde = next(s for s in QM.SONDEN if s[5] == "UCDP_TOKEN")
    e = QM.sonde_laufen(sonde, 5)
    assert e["status"] == "uebersprungen"
    assert e["status"] not in QM.ALARM


def test_token_geht_an_die_richtige_stelle():
    """UCDP erwartet einen Header, Google einen Query-Parameter. Wer das
    vertauscht, bekommt 401 statt Daten — und der Monitor meldete dann einen
    Ausfall, den es nicht gibt."""
    assert QM.TOKEN_ALS_HEADER["UCDP_TOKEN"] == "x-ucdp-access-token"
    assert QM.TOKEN_ALS_PARAMETER["GOOGLE_FACTCHECK_API_KEY"] == "key"
    for _n, _s, _u, _p, _pr, var in QM.SONDEN:
        if var:
            assert var in QM.TOKEN_ALS_HEADER or var in QM.TOKEN_ALS_PARAMETER, var


# --------------------------------------------------------------------------
# 2) Drift-Schutz — die zweite Kopie darf nicht auseinanderlaufen
# --------------------------------------------------------------------------

def _url_konstanten(service: str) -> list[str]:
    """Alle http-Zeichenketten aus dem Service — auch die in f-Strings."""
    quelle = (BACKEND / "services" / f"{service}.py").read_text(encoding="utf-8")
    raus = [m.group(0) for m in re.finditer(r"https?://[^\"'\s{}]+", quelle)]
    return raus


@pytest.mark.parametrize("sonde", QM.SONDEN, ids=lambda s: s[1])
def test_sonden_url_steht_noch_im_service(sonde):
    """Der Monitor hält seine URLs selbst — sonst müsste er die
    Service-Module importieren, und die ziehen den Reranker mit (⛔ kein
    zweiter schwerer Python-Prozess neben dem Prod-Container). Der Preis ist
    eine zweite Kopie; dieser Test ist die Gegenmassnahme."""
    _name, service, url, *_ = sonde
    konstanten = _url_konstanten(service)
    assert any(url.startswith(k) or k.startswith(url.split("?")[0][:40])
               for k in konstanten), (
        f"{service}: keine passende URL-Konstante zu {url}\n"
        f"gefunden: {konstanten[:5]}")


def test_wgi_sonde_folgt_der_quelle_und_dem_praefix():
    """Die beiden Werte, deren stille Umstellung den Ausfall verursacht hat."""
    from services import wgi
    sonde = next(s for s in QM.SONDEN if s[1] == "wgi")
    assert sonde[3]["source"] == wgi.SOURCE_ID
    assert wgi.API_ID_PRAEFIX in sonde[2]


def test_jede_sonde_nennt_einen_existierenden_service():
    for _n, service, *_ in QM.SONDEN:
        assert (BACKEND / "services" / f"{service}.py").exists(), service


def test_sonden_namen_sind_eindeutig():
    namen = [s[0] for s in QM.SONDEN]
    assert len(set(namen)) == len(namen), namen


def test_abdeckung_ist_ehrlich_dokumentiert():
    """Der Monitor deckt einen TEIL der Live-Quellen ab. Das muss im Docstring
    stehen, damit niemand aus „alle Sonden grün" auf „alle Quellen leben"
    schliesst."""
    doku = QM.__doc__ or ""
    assert "WAS ER NICHT KANN" in doku
    assert "TEIL der Live-Quellen" in doku


# --------------------------------------------------------------------------
# Verdrahtung
# --------------------------------------------------------------------------

def test_exit_code_1_bei_stummer_quelle(monkeypatch, capsys):
    """Der Cron-Job braucht einen Exit-Code, an dem er hängen kann."""
    monkeypatch.setattr(QM, "SONDEN", [
        ("Kaputt", "wgi", "https://example.test/x", {}, lambda t: -1, None)])
    monkeypatch.setattr(QM, "_post_alert", lambda *a, **k: None)

    class _Antwort:
        status = 200
        def read(self): return b"{}"
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(QM.urllib.request, "urlopen", lambda *a, **k: _Antwort())
    monkeypatch.setattr(sys, "argv", ["quellen_lebenszeichen.py"])
    assert QM.main() == 1
    assert "KAPUTTE_ABFRAGE" in capsys.readouterr().out.upper()


def test_alarm_nennt_quelle_und_grund(monkeypatch):
    """Ein Push, der nur „etwas ist kaputt" sagt, kostet eine Fehlersuche."""
    gesendet = {}
    monkeypatch.setattr(QM, "SONDEN", [
        ("Weltbank-Test", "wgi", "https://example.test/x", {}, lambda t: -1, None)])
    monkeypatch.setattr(QM, "_post_alert",
                        lambda w, t, m: gesendet.update(titel=t, text=m))

    class _Antwort:
        status = 200
        def read(self): return b"{}"
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(QM.urllib.request, "urlopen", lambda *a, **k: _Antwort())
    monkeypatch.setattr(sys, "argv", ["quellen_lebenszeichen.py"])
    QM.main()
    assert "Weltbank-Test" in gesendet["text"]
    assert "kaputte_abfrage" in gesendet["text"]
    assert "1 Quelle" in gesendet["titel"]
