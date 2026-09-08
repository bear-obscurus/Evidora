"""„Viel" und „kaum" haben keine Schwelle — ohne Bezugsgröße stimmt das
Modell zu, egal in welche Richtung die Behauptung zeigt.

QA50F-Befund 5: „Über die östliche Landgrenze kommen kaum noch Menschen in
die EU" → `true@0.85` bei Evidenz 0. Der Evidenz-Teil war nach #171 erledigt
(ein echter Frontex-Beleg). Übrig blieb das Verdict — und das Spiegel-Paar
zeigt, dass es kein Zufall war:

    „kommen kaum noch Menschen"        -> true@0.9
    „kommen weiterhin viele Menschen"  -> true@0.9

Beide Richtungen wahr, auf derselben Zahl (3.209). Logisch unmöglich. Zur
Kontrolle dieselben zwei Sätze für die zentrale Mittelmeerroute (16.454):
dort trennt das System korrekt (`false@0.85` / `true@0.9`). Der Unterschied
ist nicht die Logik, sondern die Grauzone.

Die Bezugsgröße stand die ganze Zeit in denselben Daten: 3.209 von rund
61.000 sind 5 %, die größte Einreise-Route hat 20.232. Der `indicator_name`
der Routen-Zeile trug bis hierher **nicht einmal die Zahl selbst**.
"""

import asyncio
import json
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.frontex import _einordnung, _ist_ausreise, search_frontex  # noqa: E402

DATEN = json.loads(
    (BACKEND / "data" / "frontex.json").read_text(encoding="utf-8")
)["data"]


def _name(claim):
    r = asyncio.run(search_frontex({"claim": claim})).get("results") or []
    assert r, f"Frontex liefert nichts fuer: {claim}"
    return r[0]["indicator_name"]


# --------------------------------------------------------------------------
# Die Zahl und ihre Bezugsgröße
# --------------------------------------------------------------------------

def test_routen_zeile_traegt_die_zahl():
    """Vorher stand im indicator_name nur der Routen-Name — die Zahl steckte
    allein im display_value, der nicht zwangsläufig ungekürzt in den Prompt
    geht."""
    n = _name("Über die östliche Landgrenze kommen kaum noch Menschen in die EU")
    assert "3.209" in n, n


def test_routen_zeile_traegt_den_anteil():
    n = _name("Über die östliche Landgrenze kommen kaum noch Menschen in die EU")
    assert "5 %" in n and "61.000" in n, n


def test_routen_zeile_nennt_die_groesste_einreise_route():
    """Ein Anteil allein sagt wenig; erst der Vergleich macht „viel" oder
    „kaum" entscheidbar."""
    n = _name("Über die östliche Landgrenze kommen kaum noch Menschen in die EU")
    assert "Östliches Mittelmeer" in n and "20.232" in n, n


def test_indicator_name_bleibt_unter_der_prompt_kappung():
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "MAX_STR = 400" in quelle, "Kappungs-Grenze hat sich geaendert"
    for claim in ("Über die östliche Landgrenze kommen kaum noch Menschen in die EU",
                  "Über die zentrale Mittelmeerroute kommen viele Menschen",
                  "Über den Ärmelkanal kommen viele Menschen"):
        n = _name(claim)
        assert len(n) <= 400, f"{len(n)} Zeichen: {n}"


# --------------------------------------------------------------------------
# Ausreisen sind keine Einreisen
# --------------------------------------------------------------------------

def test_ausreise_route_wird_erkannt():
    kanal = [r for r in DATEN["routen"] if "rmelkanal" in r["name"]][0]
    assert _ist_ausreise(kanal)


def test_ausreise_erkennung_haelt_auch_ohne_das_feld():
    """Die Datei ist handgepflegt (es gibt kein refresh_frontex.py). Wer eine
    Route ergänzt und `richtung` vergisst, schreibt den Hinweis trotzdem."""
    assert _ist_ausreise({"hinweis": "zaehlt AUSREISEN aus der EU"})
    assert _ist_ausreise({"richtung": "ausreise"})
    assert not _ist_ausreise({"hinweis": "aktivste Route im Zeitraum"})


def test_ausreise_route_ist_nie_die_groesste_route():
    """Der Ärmelkanal ist mit 22.469 die grösste Zahl im Datensatz — und die
    falscheste Vergleichsgrösse für „kommen in die EU". Ohne diesen Ausschluss
    hätte die Einordnung eine Ausreise als Einreise-Spitzenreiter verkauft."""
    for r in DATEN["routen"]:
        if _ist_ausreise(r):
            continue
        assert "rmelkanal" not in _einordnung(r, DATEN), r["name"]


def test_ausreise_route_bekommt_keinen_anteil_an_der_einreise_summe():
    """22.469 von 61.000 wären 37 % — nur steckt der Ärmelkanal in diesen
    61.000 gar nicht drin. Die Daten sagen das selbst."""
    kanal = [r for r in DATEN["routen"] if _ist_ausreise(r)][0]
    t = _einordnung(kanal, DATEN)
    assert "%" not in t, t
    assert "AUSREISEN" in t, t


def test_gesamtzahl_ist_die_summe_der_einreise_routen():
    """Die Rechtfertigung des Nenners, nachgerechnet: ohne den Ärmelkanal
    summieren sich die Routen auf die ausgewiesene Gesamtzahl. Driftet das
    auseinander, ist jeder Anteil falsch."""
    summe = sum(r["detektionen"] for r in DATEN["routen"] if not _ist_ausreise(r))
    gesamt = DATEN["detektionen_eu_gesamt_approx"]
    assert abs(summe - gesamt) / gesamt < 0.02, (
        f"Einreise-Routen summieren {summe}, ausgewiesen sind {gesamt}")


# --------------------------------------------------------------------------
# Robustheit
# --------------------------------------------------------------------------

@pytest.mark.parametrize("kaputt", [
    {"detektionen": None},
    {"detektionen": "viele"},
    {},
])
def test_fehlende_zahl_erzeugt_keine_einordnung(kaputt):
    assert _einordnung(kaputt, DATEN) == ""


def test_fehlende_gesamtzahl_erzeugt_keine_einordnung():
    ohne = dict(DATEN, detektionen_eu_gesamt_approx=0)
    assert _einordnung({"detektionen": 3209}, ohne) == ""


def test_einordnung_bewertet_nicht():
    """Wir liefern die Bezugsgröße, das Urteil bleibt beim Verdict — sonst
    hätten wir die Grauzone nur an eine andere Stelle verschoben."""
    t = _einordnung({"name": "Östliche Landgrenze", "detektionen": 3209}, DATEN)
    for wort in ("kaum", "viele", "wenige", "gering", "hoch", "dramatisch"):
        assert wort not in t.lower(), f"Wertung in der Einordnung: {wort} — {t}"
