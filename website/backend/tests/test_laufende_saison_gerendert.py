"""Drei Befunde, eine Ursache: der Renderer kannte die laufende Saison nicht.

QA50I-Befunde #24, #79 und #80 sahen wie drei Probleme aus:

    #24  "Die Grippewelle in Deutschland ist dieses Jahr besonders stark"
         -> false@0.85, begruendet mit dem Hoechstwert 8.661 der Saison
            2025/26
    #79  "Die Grippewelle ist dieses Jahr schwaecher als letztes Jahr"
         -> true@0.85, 8.661 (2025/26) gegen 9.290 (2024/25)
    #80  "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854"
         -> unverifiable@0.1 mit 0 Belegen

Es war ein Problem. PR #264 hat die laufende Saison 2026/27 in den Fakt
geschrieben — mit 6.640 (KW 40), 6.854 (KW 41), der Sperre "ZWEI WOCHEN
SIND KEIN SAISONVERLAUF" und dem fairen Kalenderwochen-Vergleich. Alles
als PROSA in drei Feldern. ``services/rki_surveillance.py`` rendert aber
nur Praefix-Felder und hat diese drei NIE ausgeliefert.

Gemessen am 21.10.2026: Im Ergebnis stand weder ``6.854`` noch ``KW 41``.
Das Modell konnte die laufende Saison nicht kennen — also antwortete es
auf "dieses Jahr" mit der letzten ABGESCHLOSSENEN Saison. Die Sperre aus
#264 band nicht an die Zeitangabe, weil sie den Prompt nie erreichte.

## Das Budget war nie das Problem

Das ``display_value`` des ersten Treffers je Quelle bekommt 1.200 Zeichen
(``PROMPT_MAX_DISPLAY``), belegt waren 344. Es fehlte der Renderer, nicht
der Platz. Nach dem Fix: 745 Zeichen, und die Saison-Hoechstwerte stehen
weiter dahinter.

Das ist die Lehre "Daten-Refresh ohne Renderer-Refresh reisst die Quelle
still ab" — und #264 ist hineingelaufen. Elf Felder des Fakts erreichen
die Ausgabe ueberhaupt nicht; die drei der laufenden Saison waren die
teuersten davon.

## Was der Fix NICHT macht

Die Prosa-Felder bleiben. Sie tragen die vollstaendige Begruendung und
gehen in die ``description``; die strukturierten Felder stehen zusaetzlich
daneben. Nichts ersetzt, was belegt ist (#235).

Die Kalenderwoche steht in ``laufende_saison_stand_kw``, NICHT im
Feldnamen. Ein Feldname ``kw41_vergleich_...`` haette beim naechsten
Refresh umbenannt werden muessen — dieselbe Falle wie ``total_criteria``
in #270 und ``rank`` in #266, nur vorher gesehen.

Dependency-light: nur Daten und Renderer, kein Netz, kein Modell.
"""
import asyncio
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

import services.rki_surveillance as rki  # noqa: E402
from services.synthesizer import (  # noqa: E402
    PROMPT_MAX_DISPLAY,
    PROMPT_MAX_STR,
)

ATEM = "rki_atemwegsinfekte_saisonvergleich"

#: Die Werte der laufenden Saison, aus RKI-Open-Data (PR #264).
LAUFEND = {"are": 6854, "vorwoche": 6640, "kw": "KW 41/2026",
           "label": "2026/27", "meldewochen": 2}

#: Der faire Vergleich: dieselbe Kalenderwoche ueber sechs Saisonen.
GLEICHE_KW = {
    "2021_22": 6497, "2022_23": 7170, "2023_24": 6918,
    "2024_25": 8568, "2025_26": 7584, "2026_27": 6854,
}


def _fakt():
    with open(os.path.join(DATA, "rki_surveillance.json"), encoding="utf-8") as fh:
        d = json.load(fh)
    return [f for f in d["facts"] if f.get("id") == ATEM][0]


def _ergebnis(claim):
    a = {"original_claim": claim, "claim": claim, "queries": [claim]}
    out = asyncio.run(rki.search_rki_surveillance(a))
    res = out.get("results") or []
    assert res, f"kein Treffer fuer {claim!r}"
    return res[0]


# --- Die drei Claims aus dem Lauf ------------------------------------

@pytest.mark.parametrize("claim", [
    "Die Grippewelle in Deutschland ist dieses Jahr besonders stark",
    "Die Grippewelle ist dieses Jahr schwächer als letztes Jahr",
    "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854",
])
def test_die_laufende_saison_steht_in_der_ausgabe(claim):
    """Der Kern: Ohne diese Zahlen kann das Modell "dieses Jahr" nicht von
    der letzten abgeschlossenen Saison unterscheiden."""
    dv = _ergebnis(claim)["display_value"]
    assert "6.854" in dv, "der aktuelle Wert fehlt"
    assert LAUFEND["kw"] in dv, "die Meldewoche fehlt"
    assert "2026/27" in dv, "die laufende Saison ist nicht benannt"


@pytest.mark.parametrize("claim", [
    "Die Grippewelle ist dieses Jahr schwächer als letztes Jahr",
    "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854",
])
def test_die_sperre_steht_bei_den_zahlen(claim):
    """Eine Sperre, die den Prompt nicht erreicht, ist keine Sperre.

    Sie stand in #264 im Fakt — aber in einem Feld, das der Renderer nicht
    kennt.

    Der Wortlaut ist seit QA50I #79 kuerzer: Statt "vergleicht einen
    Anfangswert mit einem Gipfel" steht jetzt die ANWEISUNG "NICHT die
    Saison-Hoechstwerte dafuer vergleichen" direkt am fertigen
    Jahresvergleich. Grund: Der vollstaendige Vergleich musste in die
    ersten 400 Zeichen passen (das Budget ab Rang 1), und eine halbe
    Rechnung ist schlechter als keine. Die ausfuehrliche Formulierung
    bleibt als Aufzeichnung im Prosa-Feld.
    """
    dv = _ergebnis(claim)["display_value"]
    assert "kein Saisonverlauf" in dv
    assert "KEIN Höchstwert" in dv
    assert "NICHT die Saison-Höchstwerte" in dv
    # Die lange Fassung bleibt in den Daten, auch wenn sie nicht mehr
    # gerendert wird — nichts loeschen, was belegt ist (#235).
    assert "Anfangswert mit einem Gipfel" in _fakt()["data"][
        "laufende_saison_2026_27"]


@pytest.mark.parametrize("claim", [
    "Die Grippewelle ist dieses Jahr schwächer als letztes Jahr",
    "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854",
])
def test_der_faire_vergleich_steht_daneben(claim):
    """Eine Sperre allein laesst die Frage "und wie stark ist es nun?"
    offen. Dieselbe Kalenderwoche ueber sechs Saisonen antwortet sie."""
    dv = _ergebnis(claim)["display_value"]
    assert "Dieselbe Kalenderwoche" in dv
    for wert in ("6.497", "7.170", "6.918", "8.568", "7.584"):
        assert wert in dv, f"Vergleichswert {wert} fehlt"


def test_die_saison_hoechstwerte_bleiben_erhalten():
    """Die laufende Saison steht VOR den Hoechstwerten — verdraengen darf
    sie sie nicht. Das Budget reicht fuer beides."""
    dv = _ergebnis("Grippewelle Deutschland Höchstwert")["display_value"]
    for wert in ("11.204", "10.434", "9.290", "8.661"):
        assert wert in dv, f"Saison-Höchstwert {wert} fehlt"
    # Gegen den SATZANFANG des Hoechstwert-Bausteins, nicht gegen eine
    # einzelne Zahl: Eine Giftprobe, die "11.204" an den vorderen Baustein
    # anhaengte, lief vorher durch, weil "LAUFENDE SAISON" trotzdem zuerst
    # stand. Der Test prueft jetzt die Reihenfolge der BAUSTEINE.
    i_laufend = dv.index("LAUFENDE SAISON")
    i_peaks = dv.index("Höchstwert der GrippeWeb-ARE-Inzidenz")
    assert i_laufend < i_peaks, (
        "die Hoechstwerte stehen vor der laufenden Saison — bei einer "
        "Kuerzung faellt dann das Aktuelle weg")
    i_vergleich = dv.index("Dieselbe Kalenderwoche")
    assert i_laufend < i_vergleich < i_peaks, (
        f"Reihenfolge {i_laufend}/{i_vergleich}/{i_peaks}: Sperre, fairer "
        f"Vergleich, dann Hoechstwerte")


# --- Der fertige Jahresvergleich (QA50I #79) -------------------------

@pytest.mark.parametrize("claim", [
    "Die Grippewelle ist dieses Jahr schwächer als letztes Jahr",
    "Ist die Grippewelle dieses Jahr stärker als im Vorjahr?",
    "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854",
])
def test_der_jahresvergleich_steht_fertig_gerechnet_da(claim):
    """QA50I #79: Nach #273 stand der faire Vergleich im Prompt — als
    Reihe von sechs Zahlen. Das Modell nahm trotzdem die Hoechstwerte
    (8.661 gegen 9.290), also "letzte gegen vorletzte Saison" statt
    "dieses Jahr gegen letztes". 3/3 deterministisch.

    Zwei Zahlen hinzulegen und auf die richtige Verknuepfung zu hoffen ist
    der Fall, fuer den gilt: LLM-Zahlenvergleich -> fertiger Satz.
    """
    dv = _ergebnis(claim)["display_value"]
    assert "DIESES JAHR GEGEN LETZTES JAHR" in dv
    assert "2026/27 6.854 gegen 2025/26 7.584" in dv
    assert "niedriger" in dv
    assert "NICHT die Saison-Höchstwerte" in dv


def test_der_jahresvergleich_passt_vollstaendig_ins_rang1_budget():
    """Eine halbe Rechnung ist schlechter als keine.

    Die erste Fassung stand hinter dem Sperr-Baustein, und bei 400
    Zeichen brach der Satz mitten in der Vergleichszahl ab ("gegen
    2025/2"). Deshalb steht er jetzt ZUERST und beide Bausteine sind
    gekuerzt.
    """
    dv = _ergebnis(
        "Die Grippewelle ist dieses Jahr schwächer als letztes Jahr")[
        "display_value"]
    kopf = dv[:PROMPT_MAX_STR]
    assert "2026/27 6.854 gegen 2025/26 7.584" in kopf
    assert "% niedriger" in kopf
    assert dv.startswith("DIESES JAHR GEGEN LETZTES JAHR")


def test_die_prozentzahl_ist_gerechnet_nicht_gefuehrt():
    """6.854 gegen 7.584 sind 9,6 %, gerundet 10 %. Ein Literal haette
    beim naechsten Meldewochen-Refresh nicht mehr gepasst."""
    d = _fakt()["data"]
    jetzt = d["vergleich_gleiche_kw_je_100k_2026_27"]
    vorher = d["vergleich_gleiche_kw_je_100k_2025_26"]
    erwartet = round(abs((jetzt - vorher) / vorher * 100))
    dv = _ergebnis(
        "Die Grippewelle ist dieses Jahr schwächer als letztes Jahr")[
        "display_value"]
    assert f"{erwartet} % niedriger" in dv, (erwartet, dv[:200])
    with open(os.path.join(BACKEND, "services", "rki_surveillance.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    assert "10 % niedriger" not in quelle, "die Prozentzahl steht als Literal"


def test_der_vergleich_faellt_weg_wenn_eine_saison_fehlt():
    """Kein "None" in der Ausgabe, wenn die Vorsaison fehlt."""
    d = dict(_fakt()["data"])
    d.pop("vergleich_gleiche_kw_je_100k_2025_26")
    satz = rki._jahresvergleich(d, "vergleich_gleiche_kw_je_100k_",
                                "dieselbe Kalenderwoche")
    assert "None" not in satz
    # Mit nur einer Saison ist kein Vergleich moeglich.
    nur_eine = {"vergleich_gleiche_kw_je_100k_2026_27": 6854}
    assert rki._jahresvergleich(nur_eine, "vergleich_gleiche_kw_je_100k_",
                                "x") == ""


def test_die_richtung_folgt_den_zahlen():
    """Bei hoeheren Werten muss "höher" stehen, nicht "niedriger"."""
    hoch = {"vergleich_gleiche_kw_je_100k_2025_26": 6000,
            "vergleich_gleiche_kw_je_100k_2026_27": 7500}
    satz = rki._jahresvergleich(hoch, "vergleich_gleiche_kw_je_100k_", "KW")
    assert "25 % höher" in satz, satz
    assert "niedriger" not in satz


# --- Das Budget -------------------------------------------------------

def test_die_ausgabe_passt_ins_prompt_budget():
    """Das Budget war nie das Problem: 1.200 Zeichen stehen zur Verfuegung,
    belegt waren vor dem Fix 344."""
    dv = _ergebnis("In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854")[
        "display_value"]
    assert len(dv) <= PROMPT_MAX_DISPLAY, (len(dv), PROMPT_MAX_DISPLAY)
    assert len(dv) > 500, (
        f"nur {len(dv)} Zeichen — stehen die neuen Bausteine wirklich drin?")
    assert PROMPT_MAX_DISPLAY >= 1200


# --- Die strukturierten Felder ---------------------------------------

@pytest.mark.parametrize("feld,wert", [
    ("laufende_saison_label", "2026/27"),
    ("laufende_saison_stand_kw", "KW 41/2026"),
    ("laufende_saison_meldewochen", 2),
    ("grippeweb_are_laufend_je_100k", 6854),
    ("grippeweb_are_laufend_vorwoche_je_100k", 6640),
])
def test_die_laufende_saison_steht_strukturiert_im_fakt(feld, wert):
    assert _fakt()["data"][feld] == wert


@pytest.mark.parametrize("saison,wert", sorted(GLEICHE_KW.items()))
def test_der_kalenderwochen_vergleich_steht_strukturiert(saison, wert):
    assert _fakt()["data"][f"vergleich_gleiche_kw_je_100k_{saison}"] == wert


def test_die_kalenderwoche_steht_nicht_im_feldnamen():
    """Ein Feldname ``kw41_vergleich_...`` haette beim naechsten Refresh
    umbenannt werden muessen — dieselbe Falle wie ``total_criteria`` in
    #270 und ``rank`` in #266, nur vorher gesehen."""
    felder = list(_fakt()["data"])
    assert not [k for k in felder if "kw41" in k or "kw_41" in k], felder
    assert "laufende_saison_stand_kw" in felder


def test_die_prosa_bleibt_erhalten():
    """Nichts ersetzen, was belegt ist (#235). Die Prosa traegt die
    vollstaendige Begruendung und geht in die description."""
    d = _fakt()["data"]
    assert "ZWEI WOCHEN SIND KEIN SAISONVERLAUF" in d["laufende_saison_2026_27"]
    assert "DIESELBE Kalenderwoche" in d["laufende_saison_fairer_vergleich"]
    assert "6.854" in d["laufende_saison_2026_27"]


def test_die_begruendung_der_umstellung_steht_im_fakt():
    """Sonst legt der naechste Refresh die Prosa wieder allein ab."""
    b = _fakt()["data"]["strukturierte_felder_begruendung"]
    assert "NIE ausgeliefert" in b
    assert "#264" in b
    assert "Feldnamen" in b


def test_die_neuen_felder_halten_die_zeichengrenze():
    """400 Zeichen je Datenfeld. ``context`` liegt mit 466 darueber, ist
    aber Altbestand und nicht Teil dieser Aenderung — gemeldet, nicht
    nebenbei umgeschrieben."""
    d = _fakt()["data"]
    neu = [k for k in d if k.startswith(
        ("laufende_saison_label", "laufende_saison_stand_kw",
         "laufende_saison_meldewochen", "grippeweb_are_laufend",
         "vergleich_gleiche_kw", "strukturierte_felder"))]
    assert len(neu) == 12, neu
    for k in neu:
        if isinstance(d[k], str):
            assert len(d[k]) <= 400, (k, len(d[k]))


# --- Der Renderer liest die Felder auch ------------------------------

def test_der_renderer_kennt_jedes_neue_feld():
    """Die eigentliche Lehre dieses PRs: Ein Feld in den Daten, das kein
    Renderer liest, ist nicht im Produkt.

    tools/feldzugriffe_pruefen.py prueft die Gegenrichtung (liest der
    Service ein Feld, das es nicht gibt). Hier die Hinrichtung.
    """
    with open(os.path.join(BACKEND, "services", "rki_surveillance.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    for feld in ("laufende_saison_label", "laufende_saison_stand_kw",
                 "laufende_saison_meldewochen",
                 "grippeweb_are_laufend_je_100k",
                 "grippeweb_are_laufend_vorwoche_je_100k"):
        assert feld in quelle, f"{feld} wird nirgends gelesen"
    assert "vergleich_gleiche_kw_je_100k_" in quelle


def test_die_meldewochen_zahl_kommt_aus_den_daten():
    """"ZWEI MELDEWOCHEN" als Literal haette gelogen, sobald die Saison
    zehn Wochen laeuft."""
    with open(os.path.join(BACKEND, "services", "rki_surveillance.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    assert "laufende_saison_meldewochen" in quelle
    assert "ZWEI MELDEWOCHEN" not in quelle
    dv = _ergebnis("In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854")[
        "display_value"]
    assert "2 Meldewochen" in dv


def test_ohne_die_felder_faellt_der_baustein_weg():
    """``_fuege`` laesst einen Baustein weg, dessen Felder fehlen — es darf
    kein "None" in der Ausgabe stehen."""
    d = dict(_fakt()["data"])
    d.pop("grippeweb_are_laufend_je_100k")
    text = rki._fuege(
        (f"X {d.get('grippeweb_are_laufend_je_100k')}",
         ["grippeweb_are_laufend_je_100k"]),
        ("Y", []), d=d)
    assert text == "Y"
    assert "None" not in text
