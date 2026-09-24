"""Pestizid-Rueckstaende: drei Kontrollprogramme, drei Quoten.

Anlass (2026-09-24), achter Inhaltskonflikt aus #185. Der Fakt nannte:

    "EFSA Annual Report 2024 (2022er-Daten, ~88.000 Proben): 96,4 %
     innerhalb MRL, 3,6 % darueber, davon 1,5 % rechtlich relevant"
    "AT-AGES 2023/2024: ~3.500 Proben/Jahr, 99,4 % konform, 21
     Ueberschreitungen"
    "53 % der Proben mit Mehrfach-Rueckstaenden, Median 2-3 Wirkstoffe"
    "Glyphosat ~3.500 t/Jahr in AT"

Nachgelesen im EFSA-Bericht fuer das Datenjahr 2024 (EFSA Journal 2026,
doi:10.2903/j.efsa.2026.10054, ueber Europe PMC frei lesbar):

    125.882 Proben in DREI Programmen mit drei verschiedenen Quoten
      86.449  nationale Programme        96,7 % innerhalb,  1,8 % nicht konform
       9.842  EU-Kontrollprogramm        97,6 % innerhalb,  1,2 % nicht konform
      39.433  verschaerfte Einfuhr       94,5 % innerhalb,  3,6 % nicht konform

Die 3,6 % des alten Fakts waren also die Quote der Einfuhr-Risikostichprobe,
ausgegeben als EU-Gesamtquote. Und ueber dem Hoechstgehalt (3,3 %) ist nicht
dasselbe wie nicht konform (1,8 %) — dazwischen liegt die Messunsicherheit.

Oesterreich: Die AGES-Schwerpunktaktion A-918-23 untersuchte 849 Proben mit
25 Beanstandungen, nicht 3.500 mit 21. Und in Oesterreich wurden 2024
insgesamt 5.232 t Pflanzenschutzmittel in Verkehr gebracht, davon 1.015 t
Herbizide (Eurostat) — die behauptete Glyphosat-Menge lag ueber der
gesamten Herbizidmenge.

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
F = next(x for x in PACK["facts"] if x.get("id") == "pestizid_rueckstaende_2026")
TEXT = F["headline"] + " " + json.dumps(F["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Die drei Programme
# --------------------------------------------------------------------------

@pytest.mark.parametrize("feld,werte", [
    ("efsa_2024_national", ["86.449", "83.591", "96,7 %", "50.524", "58,4 %",
                            "2.858", "3,3 %", "1.591", "1,8 %"]),
    ("efsa_2024_eu_programm", ["9.842", "9.608", "97,6 %", "234", "2,4 %", "113", "1,2 %"]),
    ("efsa_2024_einfuhr", ["39.433", "37.263", "94,5 %", "2.170", "5,5 %", "1.403", "3,6 %"]),
])
def test_programmzahlen(feld, werte):
    for w in werte:
        assert w in F["data"][feld], (feld, w)


def test_programme_summieren_sich_zur_gesamtzahl():
    """86.449 + 39.433 = 125.882 — die Headline nennt die Summe."""
    assert 86449 + 39433 == 125882
    assert "125.882" in F["headline"]


def test_die_drei_quoten_stehen_nebeneinander():
    """Genau hier ging die alte Fassung verloren: Sie machte aus der
    Einfuhr-Quote eine EU-Gesamtquote."""
    k = F["data"]["kernsatz_fuer_synthesizer"]
    for q in ("1,2 %", "1,8 %", "3,6 %"):
        assert q in k, q
    assert "welches Programm gemeint ist" in k


def test_ueber_mrl_ist_nicht_nicht_konform():
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "Messunsicherheit" in k
    notiz = " ".join(F["context_notes"])
    assert "zwei verschiedene Zahlen" in notiz


def test_mrl_ist_kein_giftigkeitswert():
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "396/2005" in k
    assert "keine Giftigkeitsschwelle" in k
    assert "nicht automatisch ein Gesundheitsrisiko" in k


# --------------------------------------------------------------------------
# Bio, Herkunft, Mehrfachrueckstaende
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["5.939", "6,9 %", "4.774", "80,4 %", "1.114",
                                  "18,8 %", "51", "0,9 %", "21", "0,4 %"])
def test_bio_zahlen(wert):
    assert wert in F["data"]["bio_2024"], wert


def test_bio_ausnahme_tierische_erzeugnisse():
    """Die einzige Warengruppe, in der Bio nicht besser abschneidet — Kupfer."""
    b = F["data"]["bio_2024"]
    assert "tierischen Erzeugnissen" in b and "Kupfer" in b


@pytest.mark.parametrize("wert", ["2,1 %", "1,0 %", "8,3 %", "5,2 %", "14,8 %"])
def test_herkunftsvergleich(wert):
    assert wert in F["data"]["herkunft_eu_drittland"], wert


@pytest.mark.parametrize("wert", ["56,9 %", "34,9 %", "78 %", "73,5 %", "66,1 %", "17 "])
def test_mehrfachrueckstaende(wert):
    assert wert in F["data"]["mehrfach_rueckstaende"], wert


def test_cocktail_effekt_mit_dois():
    c = F["data"]["cocktail_effekt"]
    assert "10.2903/j.efsa.2020.6087" in c and "10.2903/j.efsa.2020.6088" in c
    assert "unter der Schwelle" in c


# --------------------------------------------------------------------------
# Oesterreich
# --------------------------------------------------------------------------

def test_ages_schwerpunktaktion():
    a = F["data"]["oesterreich_ages"]
    assert "849" in a and "25" in a and "A-918-23" in a
    assert "Schwarzaugenbohnen" in a


def test_ages_vorjahr():
    v = F["data"]["oesterreich_vorjahr"]
    assert "826" in v and "30" in v and "A-918-22" in v


@pytest.mark.parametrize("wert", ["5.232", "2.221", "1.875", "1.015", "290"])
def test_at_verkaufsmengen(wert):
    assert wert in F["data"]["at_verbrauch"], wert


def test_glyphosat_gruppe_ist_kleiner_als_die_herbizidmenge():
    """Die alte Angabe (3.500 t Glyphosat) uebertraf die gesamte
    Herbizidmenge Oesterreichs — daran war sie erkennbar falsch."""
    v = F["data"]["at_verbrauch"]
    assert "Organophosphor-Herbizide" in v and "Glyphosat" in v
    assert 290 < 1015 < 5232


# --------------------------------------------------------------------------
# Die alten Zahlen sind weg
# --------------------------------------------------------------------------

@pytest.mark.parametrize("falsch", [
    "88.353",
    "96.4 %",
    "99.4 %",
    "3.500 AT-Lebensmittel-Proben",
    "53 % der Proben mit Mehrfach-Rückständen",
    "Median 2-3",
    "Glyphosat ~3.500 t/Jahr",
    "Kortenkamp 2018",
    "Farm-to-Fork",
    "Rosinen 5-8 %",
])
def test_unbelegte_angaben_sind_weg(falsch):
    assert falsch not in TEXT, falsch


def test_quelle_ist_der_aktuelle_bericht():
    assert "PMC13140735" in F["source_url"]
    assert "10.2903/j.efsa.2026.10054" in F["source_label"]
    assert "efsajournal/pub/8957" not in json.dumps(F, ensure_ascii=False)


# --------------------------------------------------------------------------
# Marker, Trigger, Datenstand
# --------------------------------------------------------------------------

def test_kein_struktureller_falsch_marker():
    """Der Fakt verneint 'voller Pestizide' und bejaht 'auch in Bio sind
    Rueckstaende' — kein Verdict-Override."""
    assert not has_false_verdict_override(F["data"]["kernsatz_fuer_synthesizer"])


@pytest.mark.parametrize("phrasing", F["claim_phrasings_handled"])
def test_phrasings_treffen(phrasing):
    assert trifft(F, phrasing.lower()), phrasing


@pytest.mark.parametrize("claim", [
    "Sind in unserem Obst Pestizide?",
    "Bio-Obst enthält keine Pestizide",
    "Wie viele Proben überschreiten den Grenzwert?",
    "Ist Importware stärker belastet?",
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
