"""SUV und Sicherheit: gemessen wird die Frontgeometrie, nicht das Etikett.

Anlass (2026-09-24), zehnter Inhaltskonflikt aus #185. Der Fakt berief sich
auf eine BASt-Untersuchung "Sicherheit bei SUV-Beteiligung" (GIDAS-Datenbank
2018-2022), die sich nicht auffinden laesst, und nannte Zahlen, die auch in
den anderen zitierten Quellen nicht stehen:

    "SUVs ab 4.500 lbs: 2,5-faches Fussgaenger-Tot-Risiko"
    "BASt 2023: Kleinwagen-Insassen 1,5-2 x hoeheres Risiko"
    "EuroNCAP: SUV-Insassenschutz 89 % vs. Limousine 86 %"
    "~50-100 zusaetzliche Verkehrstote in DE pro Jahr durch den SUV-Boom"
    "ICCT-Berechnung: 200 kg mehr = +5 % Kollisionsenergie"

Was die zitierte IIHS-Auswertung tatsaechlich sagt: Sie hat 17.897 Unfaelle
zwischen je einem Fahrzeug und einem Fussgaenger ausgewertet und fuer 2.958
Modelle Fronthoehe und -winkel vermessen. Entscheidend ist die Geometrie:

    Haube ueber 40 Zoll, geneigter Grill      +45 % Toetungsrisiko
    Haube ueber 40 Zoll, steile Front         +44 %
    Haube 30-40 Zoll, steile Front            +26 %
    flache Motorhaube (bis 15 Grad)           +25 %

Dazu Tyndall (Economics of Transportation 2021): Haette man in den USA das
SUV-Wachstum 2000-2019 durch Pkw ersetzt, waeren rund 1.100 Fussgaenger
nicht gestorben — und fuer eine bessere Insassensicherheit durch groessere
Fahrzeuge findet die Arbeit keinen Beleg.

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
F = next(x for x in PACK["facts"] if x.get("id") == "mobilitaet-suv_sicherheit_2026")
TEXT = F["headline"] + " " + json.dumps(F["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Die IIHS-Zahlen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["17.897", "2.958", "45 %", "44 %", "40 Zoll", "30 Zoll"])
def test_iihs_kennzahlen(wert):
    assert wert in F["data"]["iihs_frontgeometrie"], wert


@pytest.mark.parametrize("wert", ["26 %", "25 %", "30 und 40 Zoll", "15 Grad"])
def test_mittlere_hoehe_und_haubenneigung(wert):
    assert wert in F["data"]["iihs_mittlere_hoehe"], wert


def test_kontrollvariablen_sind_genannt():
    """Ohne sie waere der Effekt nicht von Tempo und Alter zu trennen."""
    g = F["data"]["iihs_frontgeometrie"]
    assert "Notbremsassistent" in g and "Tempolimit" in g


def test_zoll_werden_umgerechnet():
    notiz = " ".join(F["context_notes"])
    assert "101,6" in notiz and "76,2" in notiz


# --------------------------------------------------------------------------
# Tyndall
# --------------------------------------------------------------------------

def test_tyndall_2021_mit_doi_und_zahl():
    t = F["data"]["tyndall_flottenwirkung"]
    assert "10.1016/j.ecotra.2021.100219" in t
    assert "1.100" in t and "2000 und 2019" in t


def test_insassenseite_wird_nicht_umgedreht():
    """Die Gegenthese 'SUV schuetzt die Insassen besser' bleibt offen —
    belegt ist nur, dass ein Flottenbeleg dafuer fehlt."""
    t = F["data"]["tyndall_flottenwirkung"]
    assert "keinen Beleg" in t
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "Umkehrung nicht automatisch" in k


def test_tyndall_2024_mit_doi():
    assert "10.1016/j.ecotra.2024.100342" in F["data"]["tyndall_fronthoehe"]


# --------------------------------------------------------------------------
# Einordnung
# --------------------------------------------------------------------------

def test_us_kontext_ist_als_us_kontext_gekennzeichnet():
    u = F["data"]["us_kontext"]
    assert "80 %" in u and "7.400" in u
    assert "US-Zahlen" in u or "US-Fahrzeug" in u


def test_uebertragbarkeit_wird_eingeschraenkt():
    e = F["data"]["uebertragbarkeit"]
    assert "USA" in e and "Pick-ups" in e
    assert "nicht finden" in e or "nicht auffindbar" in e


def test_geometrie_statt_etikett():
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "nicht das Etikett" in k
    assert "Frontgeometrie" in k
    notiz = " ".join(F["context_notes"])
    assert "Fahrzeugklasse" in notiz


# --------------------------------------------------------------------------
# Die alten Zahlen sind weg
# --------------------------------------------------------------------------

@pytest.mark.parametrize("falsch", [
    "4.500 lbs",
    "2,5 ×",
    "1,8 ×",
    "BASt 2023",
    "GIDAS",
    "EuroNCAP",
    "50-100 zusätzliche Verkehrstote",
    "+5 % Kollisions-Energie",
    "Marktanteil DE 2010",
])
def test_unbelegte_angaben_sind_weg(falsch):
    assert falsch not in TEXT, falsch


def test_quellen_sind_die_zitierten():
    assert "iihs.org" in F["source_url"]
    assert "10.1016/j.ecotra.2021.100219" in F["secondary_url"]
    assert "bast.de" not in json.dumps(F, ensure_ascii=False)


# --------------------------------------------------------------------------
# Marker, Trigger, Datenstand
# --------------------------------------------------------------------------

def test_kein_struktureller_falsch_marker():
    """Der Fakt verneint beide Pauschalthesen — keine Verdict-Direktive."""
    assert not has_false_verdict_override(F["data"]["kernsatz_fuer_synthesizer"])


@pytest.mark.parametrize("phrasing", F["claim_phrasings_handled"])
def test_phrasings_treffen(phrasing):
    assert trifft(F, phrasing.lower()), phrasing


@pytest.mark.parametrize("claim", [
    "Sind SUV im Stadtverkehr gefährlicher?",
    "Ein SUV schützt die eigene Familie besser",
    "SUVs töten mehr Fußgänger",
])
def test_batterie(claim):
    assert trifft(F, claim.lower()), claim


def test_datenstand_ist_benannt():
    notiz = " ".join(F["context_notes"])
    assert "Datenstand" in notiz and re.search(r"20\d\d", notiz)


def test_prompt_felder_bleiben_unter_der_kuerzung():
    zu_lang = {k: len(v) for k, v in F["data"].items()
               if k != "kernsatz_fuer_synthesizer" and len(v) > 400}
    assert not zu_lang, zu_lang
