"""Eurobarometer-Zahlen: aus den amtlichen Tabellen statt aus der Luft.

Anlass (2026-09-22): Beim Reparieren der toten Beleg-Links (#185) fiel der
Fakt `at_demokratie_zufriedenheit` auf: Er nannte für 2024 eine
AT-Demokratie-Zufriedenheit von 67 % bei einem EU-Mittel von 53 %. Die
amtliche Tabelle (Volume A, Frage SD18a, Anteil "Total 'Satisfied'") weist
für Herbst 2024 aus: AT 60 %, EU-27 55 %. Ebenso wenig stimmten die
Vertrauenswerte (Parlament 38 % statt 58 %, Justiz 71 % statt 79 %), die
Zeitreihe 2014–2024 und die Zuschreibung des Demokratie Monitors an das
"IFES" — er stammt von Foresight (vormals SORA).

Die Zahlen hier stammen aus den XLSX-Tabellen der Europäischen Kommission
(data.europa.eu, Standard-Eurobarometer 96, 100, 101, 102 und 105), am
22.9.2026 ausgelesen:

    SD18a "zufrieden"   EU27   AT   DE   FR
    Winter 2021/22       56    62   72   47
    Herbst 2023          55    61   62   45
    Frühjahr 2024        58    63   61   50
    Herbst 2024          55    60   56   39
    Frühjahr 2026        60    65   61   56

Keine Netzabfrage in diesen Tests.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"
sys.path.insert(0, str(BACKEND))

from services._struct_marker import has_false_verdict_override  # noqa: E402
from services._topic_match import substring_or_composite_match as trifft  # noqa: E402


def _fakt(datei: str, fakt_id: str) -> dict:
    def such(o):
        if isinstance(o, dict):
            if o.get("id") == fakt_id:
                return o
            for v in o.values():
                if (r := such(v)) is not None:
                    return r
        elif isinstance(o, list):
            for v in o:
                if (r := such(v)) is not None:
                    return r
        return None
    f = such(json.loads((DATA / datei).read_text(encoding="utf-8")))
    assert f is not None, (datei, fakt_id)
    return f


AT = _fakt("demokratie_pack.json", "at_demokratie_zufriedenheit_2026")
EU = _fakt("eurobarometer.json", "eb_demokratie_2024")
# Geprueft wird, was die Fakten AUSSAGEN (headline + data). Die
# "Korrigiert"-Notiz in context_notes benennt die alten Falschwerte
# absichtlich — sie darf die Pruefung nicht ausloesen.
AT_TEXT = AT["headline"] + " " + json.dumps(AT["data"], ensure_ascii=False)
EU_TEXT = EU["headline"] + " " + json.dumps(EU["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Österreich-Fakt
# --------------------------------------------------------------------------

def test_at_nennt_die_aktuelle_welle_mit_feldzeit():
    assert "Eurobarometer 105" in AT["headline"]
    assert "12.3.–5.4.2026" in AT["headline"]


@pytest.mark.parametrize("wert", ["65 %", "60 %", "61 %", "56 %"])
def test_at_headline_hat_die_amtlichen_werte(wert):
    """AT 65, EU-27 60, DE 61, FR 56 (Frühjahr 2026)."""
    assert wert in AT["headline"], AT["headline"]


def test_at_alte_falschzahlen_sind_weg():
    for falsch in ("67 %", "EU-Mittel 53 %", "Parlament 38 %", "Regierung 32 %",
                   "Justiz 71 %", "2014 76 %"):
        assert falsch not in AT_TEXT, falsch


def test_at_verlauf_ist_vollstaendig():
    v = AT["data"]["at_verlauf_eurobarometer"]
    for wert in ("62 %", "61 %", "63 %", "60 %", "65 %"):
        assert wert in v, (wert, v)
    assert "56 %, 55 %, 58 %, 55 %, 60 %" in v, "EU-Vergleichsreihe fehlt"


@pytest.mark.parametrize("paar", [("Justiz 79 %", "58 %"), ("Parlament 55 %", "37 %"),
                                  ("Regierung 51 %", "37 %"), ("politische Parteien 36 %", "25 %")])
def test_at_vertrauenswerte_stammen_aus_qa6(paar):
    wert, eu = paar
    v = AT["data"]["eb105_vertrauen_institutionen"]
    assert wert in v, (wert, v)
    assert eu in v


def test_at_demokratie_monitor_ist_richtig_zugeordnet():
    d = AT["data"]["demokratie_monitor"]
    assert "Foresight" in d and "SORA" in d
    assert "IFES" not in AT_TEXT, "Der Monitor stammt nicht vom IFES"
    assert "43 %" in d and "34 %" in d
    assert "ANDERE Frage" in d, "Die Fragestellung unterscheidet sich vom Eurobarometer"


def test_at_ordnet_die_gaengigen_claims_ein():
    k = AT["data"]["kernsatz_fuer_synthesizer"]
    assert "'Das Demokratie-Vertrauen in Österreich ist im freien Fall' ist damit nicht belegt" in k
    assert "'Österreich hat die höchste Demokratie-Zufriedenheit' ist falsch" in k
    assert "Platz 9 von 27" in k


def test_at_quelle_zeigt_auf_die_eb105_erhebung():
    assert AT["source_url"] == "https://europa.eu/eurobarometer/surveys/detail/3613"


# --------------------------------------------------------------------------
# EU-Fakt (zweite Kopie derselben Zahl)
# --------------------------------------------------------------------------

def test_eu_nennt_60_prozent_statt_der_spanne():
    assert "60 %" in EU["headline"]
    for falsch in ("58-62", "58–62", "-8 PP", "verteidigen"):
        assert falsch not in EU_TEXT, falsch


def test_eu_verlauf_und_spanne():
    v = EU["data"]["eu_verlauf"]
    for wert in ("Winter 2021/22 56 %", "Herbst 2023 55 %", "Frühjahr 2024 58 %",
                 "Herbst 2024 55 %", "Frühjahr 2026 60 %"):
        assert wert in v, (wert, v)
    s = EU["data"]["laender_spanne_2026"]
    assert "DK 91 %" in s and "EL 28 %" in s


def test_eu_trennt_demokratie_von_regierungszufriedenheit():
    k = EU["data"]["kernsatz_fuer_synthesizer"]
    assert "NICHT Regierungs-Zufriedenheit" in k
    assert "37 %" in k and "25 %" in k


def test_eu_datenstand_nennt_die_welle():
    assert "105" in EU["data"]["datenstand"] and "2026" in EU["data"]["datenstand"]


# --------------------------------------------------------------------------
# Beide Kopien muessen dieselbe Welle und dieselben Werte tragen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["AT 65 %", "DE 61 %", "FR 56 %"])
def test_beide_fakten_nennen_dieselben_werte(wert):
    """Lehrgeld: eine Zahl in zwei Kopien driftet unbemerkt."""
    assert wert in AT["data"]["eb105_zufriedenheit"], wert
    assert wert in EU["data"]["laender_spanne_2026"], wert


def test_beide_fakten_verweisen_auf_dieselbe_erhebung():
    assert AT["source_url"] == EU["source_url"]


# --------------------------------------------------------------------------
# Inversions-Falle und Trigger
# --------------------------------------------------------------------------

@pytest.mark.parametrize("fakt,name", [(AT, "AT"), (EU, "EU")])
def test_kein_struktureller_falsch_marker(fakt, name):
    """Beide Fakten bejahen Claims ('Mehrheit ist zufrieden') und verneinen
    andere ('freier Fall') — ein FALSCH-Marker wuerde die zutreffenden
    mitreissen."""
    assert not has_false_verdict_override(fakt["data"]["kernsatz_fuer_synthesizer"]), name


@pytest.mark.parametrize("phrasing", AT["claim_phrasings_handled"])
def test_at_phrasings_treffen(phrasing):
    assert trifft(AT, phrasing.lower()), phrasing


@pytest.mark.parametrize("claim", [
    "Die Demokratie in Österreich ist in der Krise",
    "Das Vertrauen in die österreichische Justiz ist hoch",
    "Eurobarometer: Zufriedenheit mit der Demokratie",
])
def test_at_batterie(claim):
    assert trifft(AT, claim.lower()), claim
