"""Tempolimit: zwei UBA-Studien, zwei Zahlen — und die richtige Zuordnung.

Anlass (QA50F-Adjudikation, 2026-09-25). Der Claim "Die Tempolimit-Studie des
Umweltbundesamts stammt von Friedrich" bekam live true@0.9. Die Pruefung
zeigte: Das stimmt — aber nicht fuer die Zahl, die der Fakt daran haengte.

Es gibt ZWEI UBA-Veroeffentlichungen:

    "Klimaschutz durch Tempolimit", Texte 38/2020
        Lange, Hendzlik, Schmied (UBA)
        Datenjahr 2018, nur Pkw + leichte Nutzfahrzeuge auf Bundesautobahnen
        (39,1 Mio. t CO2e): T130 -1,9 Mio. t, T120 -2,6, T100 -5,4

    "Modellierung der Umweltwirkung von Tempolimit-Massnahmen", Texte 176/2024
        Friedrich, Bawidamann, Schmaus (Universitaet Stuttgart, ISV)
        gesamter Strassenverkehr: THG -2,2 % (T130/100) bis -8,1 % (T100/80),
        NOx -5,1 bis -16,1 %, PM -3,6 bis -11,4 %

Der Fakt fuehrte beide als eine zusammen, schrieb die Tonnen-Minderung
Friedrich zu und nannte 2023 als Jahr. Die frueher zitierte BASt-Auswertung
mit -22 und -34 Prozent liess sich nicht auffinden und ist raus.

Keine Netzabfrage in diesen Tests.
"""

import json
import re
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"
sys.path.insert(0, str(BACKEND))

from services._struct_marker import has_false_verdict_override  # noqa: E402
from services._topic_match import substring_or_composite_match as trifft  # noqa: E402

PACK = json.loads((DATA / "mobilitaet_pack.json").read_text(encoding="utf-8"))
F = next(x for x in PACK["facts"] if x.get("id") == "mobilitaet-tempolimit_130_2026")
TEXT = F["headline"] + " " + json.dumps(F["data"], ensure_ascii=False)


@pytest.mark.parametrize("wert", ["38/2020", "39,1", "1,9", "2,6", "5,4"])
def test_tonnen_studie(wert):
    assert wert in F["data"]["uba_2020_tonnen"], wert


@pytest.mark.parametrize("wert", ["176/2024", "2,2 %", "8,1 %", "5,1", "16,1", "3,6", "11,4"])
def test_prozent_studie(wert):
    assert wert in F["data"]["uba_2024_prozente"], wert


def test_zuordnung_ist_ausdruecklich():
    """Der Kern des Befunds: welche Zahl aus welcher Studie."""
    z = F["data"]["welche_zahl_aus_welcher_studie"]
    assert "1,9" in z and "38/2020" in z and "Lange" in z
    assert "176/2024" in z and "Friedrich" in z
    assert "Friedrich hat die Tonnen-Zahl nicht berechnet" in z


def test_beide_autorengruppen_stehen_im_fakt():
    assert "Lange" in TEXT and "Hendzlik" in TEXT and "Schmied" in TEXT
    assert "Friedrich" in TEXT and "Bawidamann" in TEXT and "Schmaus" in TEXT


def test_bezugsgroessen_werden_unterschieden():
    notiz = " ".join(F["context_notes"])
    assert "Bezugsgröße" in notiz
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "Pkw und leichte Nutzfahrzeuge" in k and "gesamten Straßenverkehr" in k


def test_methodik_der_modellierung():
    m = F["data"]["uba_2024_methodik"]
    assert "HBEFA" in m and "TomTom" in m and "75 %" in m


def test_unfallzahlen_sind_raus_und_das_steht_da():
    u = F["data"]["unfallwirkung"]
    assert "nicht auffinden" in u
    assert "-22 %" not in TEXT and "-34 %" not in TEXT


@pytest.mark.parametrize("falsch", [
    "UBA-Studie 2023",
    "Bandbreite 1,7-2,1",
    "250.000 Personen",
    "Polen 140",
    "~5,3",
])
def test_unbelegte_angaben_sind_weg(falsch):
    assert falsch not in TEXT, falsch


def test_quellen_sind_die_beiden_publikationsseiten():
    assert "klimaschutz-durch-tempolimit" in F["source_url"]
    assert "modellierung-der-umweltwirkung-von-tempolimit" in F["secondary_url"]


def test_kein_struktureller_falsch_marker():
    assert not has_false_verdict_override(F["data"]["kernsatz_fuer_synthesizer"])


@pytest.mark.parametrize("phrasing", F["claim_phrasings_handled"])
def test_phrasings_treffen(phrasing):
    assert trifft(F, phrasing.lower()), phrasing


def test_datenstand_ist_benannt():
    notiz = " ".join(F["context_notes"])
    assert "Datenstand" in notiz and re.search(r"20\d\d", notiz)


def test_prompt_felder_bleiben_unter_der_kuerzung():
    zu_lang = {k: len(v) for k, v in F["data"].items()
               if k != "kernsatz_fuer_synthesizer" and len(v) > 400}
    assert not zu_lang, zu_lang
