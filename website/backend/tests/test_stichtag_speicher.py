"""Der Stichtags-Wächter wurde sechs Tage lang erschlagen, bevor er zählte.

Seit PR #249 laufen die Wartungs-Werkzeuge in einem eigenen Container mit
224 MB Speicherlimit. ``stichtag_check.py`` liest jede ``data/*.json``
komplett mit ``json.loads(pfad.read_text())`` — und
``cordis_projects_slim.json`` ist 108 MiB gross. Auf prod gemessen am
18.10.2026:

    ./run_evidora_tool.sh stichtag_check.py --strict
    exit=137                        # OOM-Kill
    Logdatei: nur "Container … Creating / Created", sonst nichts

Sechs Tage (13.–18.10.) schrieb der Cron daraufhin ``stichtag_check
FUNDE`` — einen Befund, den es nie gab, weil das Werkzeug nie bis zur
Auswertung kam. Dieselbe Ursache wie bei der Lebenszeichen-Sonde in #250;
damals habe ich nur deren Logdatei angesehen und diese nicht.

Der Code WOLLTE diese Dateien bereits überspringen: am ``except`` stand
``# generierte Caches u. Ä.``, in der Annahme, sie würden am Parsen
scheitern. ``cordis_projects_slim.json`` parst einwandfrei — der Prozess
stirbt vorher. Eine Annahme, die nie geprüft wurde.

Dependency-light: nur Dateien und Pfade, kein Docker, kein Netz.
"""
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS = os.path.join(BACKEND, "tools")
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, TOOLS)

from _generiert import (  # noqa: E402
    GENERIERTE_CACHES,
    MAX_LESBAR_BYTES,
    ist_generiert,
    ueberspringen,
)
import stichtag_check  # noqa: E402


# --- Die Ursache ------------------------------------------------------

def test_der_cordis_cache_wird_uebersprungen():
    """Der konkrete Killer. 108 MiB in einem 224-MB-Container."""
    assert ist_generiert("cordis_projects_slim.json")
    assert ueberspringen("cordis_projects_slim.json") is not None


def test_der_grund_steht_im_rueckgabewert():
    """Nicht ``True``, sondern WARUM — der Aufrufer gibt es aus.

    Lehre aus #251: Ein übersprungener Teil ist kein Befund über die
    Quelle, aber er gehört in die Ausgabe. Still zu überspringen sieht
    aus wie ein saubererer Lauf.
    """
    grund = ueberspringen("mitre_attack.json")
    assert isinstance(grund, str) and "generiert" in grund


def test_eine_kuratierte_datei_wird_gelesen(tmp_path):
    klein = tmp_path / "irgendein_pack.json"
    klein.write_text(json.dumps({"facts": []}), encoding="utf-8")
    assert ueberspringen(str(klein)) is None


def test_die_groessen_schranke_faengt_unbekannte_namen(tmp_path):
    """Das Sicherheitsnetz unter der Namensliste.

    Ein künftiger generierter Cache unter neuem Namen würde das Werkzeug
    sonst wieder erschlagen — und die Namensliste schützt nur gegen das,
    was wir schon kennen.
    """
    gross = tmp_path / "noch_unbekannt.json"
    gross.write_bytes(b'{"facts": []}' + b" " * (MAX_LESBAR_BYTES + 1))
    grund = ueberspringen(str(gross))
    assert grund is not None and "Leselimit" in grund


def test_die_schranke_liegt_ueber_der_groessten_kuratierten_datei():
    """Sonst überspringt das Netz echte Fakten.

    Gemessen am 18.10.2026: die größte kuratierte Datei lag bei 6 MiB.
    Das Gate rechnet es jedes Mal neu, statt die Zahl zu glauben.
    """
    groesste = 0
    name = ""
    for d in os.listdir(DATA):
        if not d.endswith(".json") or d in GENERIERTE_CACHES:
            continue
        if d.startswith((".", "_")):
            continue
        groesse = os.path.getsize(os.path.join(DATA, d))
        if groesse > groesste:
            groesste, name = groesse, d
    assert groesste, "keine kuratierte Datei gefunden"
    assert groesste < MAX_LESBAR_BYTES, (
        f"{name} ist {groesste // 1048576} MiB und würde vom "
        f"Sicherheitsnetz übersprungen")


# --- Der Scanner selbst -----------------------------------------------

def test_scanne_meldet_die_uebersprungenen(tmp_path):
    """Die Rückgabe ist ``(Funde, Uebersprungene)``, nicht nur Funde."""
    (tmp_path / "cordis_projects_slim.json").write_text("{}", encoding="utf-8")
    (tmp_path / "echt_pack.json").write_text(json.dumps({"facts": [
        {"id": "x", "data": {"a": "Stand Januar 2019: irgendwas."}}]}),
        encoding="utf-8")
    from datetime import date
    funde, uebersprungen = stichtag_check.scanne(
        tmp_path, date(2026, 10, 18), 24)
    assert [n for n, _g in uebersprungen] == ["cordis_projects_slim.json"]
    assert any(f["id"] == "x" for f in funde), (
        "Die echte Datei wurde mit übersprungen — dann prüft der "
        "Wächter gar nichts mehr")


def test_scanne_ueberlebt_die_echte_datenlage():
    """Der Lauf, der auf prod sechs Tage mit exit 137 starb.

    Lokal fehlen die generierten Caches (sie sind untracked), deshalb
    beweist das nicht den Speicherverbrauch — aber es beweist, dass der
    Rückgabe-Vertrag mit den echten Daten hält.
    """
    from datetime import date
    funde, uebersprungen = stichtag_check.scanne(
        __import__("pathlib").Path(DATA), date(2026, 10, 18), 24)
    assert isinstance(funde, list) and isinstance(uebersprungen, list)


# --- Die Zahl, die der Wächter gefunden hat ---------------------------

def _fakt(datei, kennung):
    with open(os.path.join(DATA, datei), encoding="utf-8") as fh:
        d = json.load(fh)
    for f in d.get("facts") or []:
        if (f.get("id") or f.get("topic")) == kennung:
            return f
    raise AssertionError(f"{kennung} nicht in {datei}")


#: EZB, Key ECB interest rates, wirksam ab 16.09.2026. Von der
#: Primärquelle geholt und mit der QA50H-Antwort gegengeprüft, die
#: denselben Hauptrefinanzierungssatz nannte.
EZB_2026 = {"Einlagezins": "2,50", "Hauptrefinanzierungssatz": "2,65",
            "Spitzenrefinanzierungssatz": "2,90"}

EZB_FAKT = ("finanzen_pack.json", "negativzins_volkstheorie_2026")


def test_die_aktuellen_ezb_saetze_stehen_im_fakt():
    t = json.dumps(_fakt(*EZB_FAKT), ensure_ascii=False)
    for name, wert in EZB_2026.items():
        assert f"{wert} %" in t, f"{name} {wert} % fehlt"
    assert "16.09.2026" in t, "Der Stichtag der Sätze fehlt"


def test_die_alten_saetze_stehen_nur_noch_als_vergangenheit():
    """3,5 % und 3,65 % waren der Stand Oktober 2024. Sie dürfen nicht
    mehr als aktueller Satz dastehen."""
    t = json.dumps(_fakt(*EZB_FAKT), ensure_ascii=False)
    for alt in ("3,5 %", "3,65 %", "3,9 %"):
        assert alt not in t, f"{alt} steht noch im Fakt"


def test_der_stichtags_fund_ist_weg():
    """Der Wächter-Marker selbst: ``Stand Oktober 2024``."""
    t = json.dumps(_fakt(*EZB_FAKT), ensure_ascii=False)
    assert "Stand Oktober 2024" not in t


@pytest.mark.parametrize("kennung", [
    "negativzins_volkstheorie_2026", "zinseszins_2026",
    "aktien_gluecksspiel_2026"])
def test_keine_alte_inflationsrate_als_aktuelle(kennung):
    """Die 2,2 % von 2024 standen in DREI Fakten, einer davon als
    "Bei aktueller Inflation (DE 2024: 2,2 %)". Der Stichtags-Wächter
    hat nur EINEN gemeldet, weil nur dort "Stand Oktober 2024" stand —
    die anderen zwei fand erst die Inventur."""
    t = json.dumps(_fakt("finanzen_pack.json", kennung), ensure_ascii=False)
    assert "2,2 %" not in t, "alte Inflationsrate noch drin"
    assert "3,3 %" in t, "aktuelle Inflationsrate (09/2026) fehlt"


def test_die_real_rendite_ist_jetzt_bei_beiden_negativ():
    """Inhaltliche Umkehr, nicht nur ein Datums-Update: 2024 war Festgeld
    real positiv (+0,8 %), bei 3,3 % Inflation ist es das nicht mehr."""
    t = json.dumps(_fakt(*EZB_FAKT), ensure_ascii=False)
    assert "0,26 %" in t and "1,87 %" in t, "Einlagenzinsen 04/2026 fehlen"
    assert "+0,8 %" not in t, "die alte positive Real-Rendite steht noch da"


def test_die_quellen_der_zinsen_sind_benannt():
    """Die Einlagenzinsen sind EURORAUM-Werte, nicht deutsche. Steht das
    nicht dabei, liest das Modell sie als deutsche Zahlen."""
    t = json.dumps(_fakt(*EZB_FAKT), ensure_ascii=False)
    assert "Bundesbank" in t
    assert "EURORAUM" in t.upper()
