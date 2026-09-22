"""Agrarbudget und Treibhausgas-Bilanz: nachrechenbare Zahlen.

Anlass (2026-09-22), vierter und fünfter Inhaltskonflikt aus #185:

    Agrar:  "~1.1 Mrd EU-GAP + ~600 Mio nationaler Top-Up = ~7 Mrd EUR/Jahr"
            Die Gleichung geht nicht auf. Grüner Bericht 2025 (Datenjahr
            2024): 2.652 Mio. Euro Agrarbudget insgesamt.
            Dazu "80 % der GAP-Mittel an 20 % der Höfe (BOKU 2023)" — die
            Verteilungstabelle des Berichts ergibt rund 52 %.

    Klima:  "8.0 Mt CO2e, ~10 % der AT-Gesamt-Emissionen (von 80 Mt)"
            Amtlich 2024: 8,4 Mt von 66,6 Mt, also 12,6 %. Auch die
            Sektorreihung (Verkehr 23, Industrie 22, Energie 18) stimmte
            nicht: Energie und Industrie 29,0, Verkehr 19,5.

Der Konzentrations-Test rechnet die Verteilung aus der Größenklassen-
Tabelle im Fakt selbst nach — so kann die Aussage "rund 52 %" nicht von
den Klassenwerten wegdriften.

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

PACK = json.loads((DATA / "landwirtschaft_pack.json").read_text(encoding="utf-8"))


def _fakt(fakt_id: str) -> dict:
    f = next((x for x in PACK["facts"] if x.get("id") == fakt_id), None)
    assert f is not None, fakt_id
    return f


AGRAR = _fakt("agrar_subventionen_at_2026")
KLIMA = _fakt("klima_landwirtschaft_2026")
AGRAR_TEXT = AGRAR["headline"] + " " + json.dumps(AGRAR["data"], ensure_ascii=False)
KLIMA_TEXT = KLIMA["headline"] + " " + json.dumps(KLIMA["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Agrarbudget
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["2.652", "603", "1.364", "685", "49 %", "28 %", "23 %"])
def test_agrarbudget_2024(wert):
    assert wert in AGRAR["data"]["budget_2024"], (wert, AGRAR["data"]["budget_2024"])


def test_budget_summanden_ergeben_das_budget():
    """603 + 1.364 + 685 = 2.652 — die Rechnung, an der die alte Fassung scheiterte."""
    assert 603 + 1364 + 685 == 2652


def test_alte_sieben_milliarden_sind_weg():
    for falsch in ("7 Mrd", "~7 Mrd EUR/Jahr", "1.1 Mrd EUR aus EU-Topf"):
        assert falsch not in AGRAR_TEXT, falsch


def test_direktzahlungen_kennzahlen():
    v = AGRAR["data"]["verteilung_direktzahlungen_2024"]
    assert "101.027" in v and "575,14" in v and "5.693" in v
    assert "60,46 %" in v


def test_konzentration_ist_aus_der_tabelle_nachgerechnet():
    """Die Klassen im Fakt muessen die Aussage 'rund 52 %' tragen."""
    v = AGRAR["data"]["verteilung_direktzahlungen_2024"]
    # Klassen mit vollstaendigen Angaben (Faelle / Anteil / Summe in Mio.)
    klassen = [(61081, 133.9), (23579, 168.0), (10165, 123.0), (3225, 55.1),
               (1361, 30.2), (621, 16.9), (548, 18.7)]
    for faelle, _ in klassen:
        assert f"{faelle:,}".replace(",", ".") in v, faelle
    # kleine Klassen ohne Betrag im Text: Werte aus dem Gruenen Bericht
    rest = [(197, 8.8), (92, 5.0), (53, 3.4), (63, 5.3), (28, 3.2), (14, 3.8)]
    alle = sorted(klassen + rest, key=lambda k: k[1] / k[0], reverse=True)
    gesamt_faelle = sum(k[0] for k in alle)
    gesamt_summe = sum(k[1] for k in alle)
    assert abs(gesamt_faelle - 101027) <= 1, gesamt_faelle
    assert abs(gesamt_summe - 575.1) < 1.0, gesamt_summe
    ziel, kum_f, kum_e = round(gesamt_faelle * 0.2), 0, 0.0
    for anz, betrag in alle:
        if kum_f + anz <= ziel:
            kum_f, kum_e = kum_f + anz, kum_e + betrag
        else:
            kum_e += betrag * (ziel - kum_f) / anz
            break
    anteil = kum_e / gesamt_summe
    assert 0.48 <= anteil <= 0.56, anteil
    assert "rund 52 %" in AGRAR["data"]["konzentration_nachgerechnet"]


def test_80_20_wird_ausdruecklich_verneint():
    k = AGRAR["data"]["kernsatz_fuer_synthesizer"]
    assert "'80 % der Mittel gehen an 20 % der Höfe' trifft auf die österreichischen Direktzahlungen also NICHT zu" in k
    assert "BOKU" not in AGRAR_TEXT, "die erfundene BOKU-Studie ist raus"


def test_agrar_quelle_ist_der_gruene_bericht():
    assert "gruenerbericht2025" in AGRAR["source_url"]
    assert AGRAR["secondary_url"].endswith(".pdf")
    # Der erfundene Link darf nicht mehr als Quelle dienen; in der
    # "Korrigiert"-Notiz wird er absichtlich benannt.
    assert "jcr:fixme" not in AGRAR["source_url"] + AGRAR["secondary_url"]
    assert "jcr:fixme" not in AGRAR_TEXT


# --------------------------------------------------------------------------
# Treibhausgas-Bilanz
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["66,6", "29,0", "19,5", "8,4", "5,8", "2,3", "1,7"])
def test_sektorzahlen_2024(wert):
    assert wert in KLIMA["data"]["sektoren_2024"], (wert, KLIMA["data"]["sektoren_2024"])


def test_sektorsumme_ergibt_die_gesamtemission():
    assert round(29.0 + 19.5 + 8.4 + 5.8 + 2.3 + 1.7, 1) == 66.7  # Rundung der Einzelwerte
    assert "Summe 66,6" in KLIMA["data"]["sektoren_2024"]


def test_anteil_der_landwirtschaft_ist_korrekt():
    assert abs(8.4 / 66.6 * 100 - 12.6) < 0.1
    assert "12,6 %" in KLIMA["headline"] and "12,6 %" in KLIMA["data"]["sektoren_2024"]


def test_alte_klimazahlen_sind_weg():
    for falsch in ("80 Mt", "8.0 Mt", "Verkehr 23 Mt", "Industrie 22 Mt", "1990: 9.5 Mt"):
        assert falsch not in KLIMA_TEXT, falsch


def test_klimaschutzgesetz_hoechstmenge():
    k = KLIMA["data"]["klimaschutzgesetz"]
    assert "42,7" in k and "43,0" in k


def test_methodik_nennt_den_zeithorizont():
    """Methan-Aequivalente haengen am Zeithorizont — das war im alten Fakt
    eine unkommentierte 80-fach-Angabe."""
    m = KLIMA["data"]["methodik"]
    assert "IPCC" in m and "20-Jahres-Wert" in m


def test_nicht_belegte_klimaanpassungs_zahlen_sind_weg():
    for falsch in ("+1.5°C 1980-2024", "Hitze-Tage", "Sankt Pölten"):
        assert falsch not in KLIMA_TEXT, falsch


# --------------------------------------------------------------------------
# Marker, Trigger, Quellen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("fakt,name", [(AGRAR, "agrar"), (KLIMA, "klima")])
def test_kein_struktureller_falsch_marker(fakt, name):
    assert not has_false_verdict_override(fakt["data"]["kernsatz_fuer_synthesizer"]), name


@pytest.mark.parametrize("fakt,name", [(AGRAR, "agrar"), (KLIMA, "klima")])
def test_phrasings_treffen(fakt, name):
    fehlt = [p for p in fakt["claim_phrasings_handled"] if not trifft(fakt, p.lower())]
    assert not fehlt, (name, fehlt)


@pytest.mark.parametrize("claim", [
    "Wie viel Agrarförderung bekommen Österreichs Bauern?",
    "Die Agrarförderung ist ungerecht verteilt",
])
def test_agrar_batterie(claim):
    assert trifft(AGRAR, claim.lower()), claim


@pytest.mark.parametrize("claim", [
    "Wie hoch sind die Treibhausgas-Emissionen Österreichs?",
    "Die Landwirtschaft verursacht die meisten Emissionen",
])
def test_klima_batterie(claim):
    assert trifft(KLIMA, claim.lower()), claim


def test_beide_fakten_tragen_ihren_datenstand():
    for f in (AGRAR, KLIMA):
        notiz = " ".join(f["context_notes"])
        assert "Datenstand" in notiz, f["id"]
        assert re.search(r"20\d\d", notiz), f["id"]
