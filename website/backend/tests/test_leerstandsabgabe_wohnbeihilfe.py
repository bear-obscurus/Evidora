"""Leerstandsabgabe und Wohnbeihilfe: Rechtsstand statt Hausnummern.

Anlass (2026-09-23), sechster und siebter Inhaltskonflikt aus #185:

    Leerstandsabgabe: "Vorarlberg 1.000-3.000 EUR/Jahr, Tirol 1.500-2.000
        EUR/Jahr, beide seit 2024; VfGH G54/22 vom 2.10.2023; ~600 Wohnungen
        zurueck auf den Mietmarkt (Empirica)."
        Nachgelesen: Die Steiermark war zuerst (StZWAG, 1.10.2022), Tirol
        folgte am 1.1.2023 (TFLAG), Vorarlberg am 1.1.2024 (ZAG). Die
        Betraege stehen je Monat (Tirol: 10-215 Euro nach Nutzflaeche) oder
        je Quadratmeter und Jahr (Vorarlberg: 8,20/14,10/18,50 Euro mit
        Jahresdeckeln 1.230/2.115/2.775 Euro) im Gesetz. Ein Erkenntnis
        G54/22 zur Leerstandsabgabe gibt es in der RIS-Judikatur nicht.

    Wohnbeihilfe: "~280.000 Bezieher in AT, davon ~140.000 in Wien,
        Auszahlungsvolumen ~330 Mio. Euro."
        Amtlich: Wien hatte 2023 31.043 Bezieher (Statistisches Jahrbuch der
        Stadt Wien 2025, Tabelle 10.1.5); die Ausgaben fuer ganz Oesterreich
        lagen 2024 bei 265 Mio. Euro (ESSOSS). Der Fakt lag beim Vierfachen.

Die Wirkungsfrage bleibt offen und soll offen bleiben: Fuer Oesterreich gibt
es keine veroeffentlichte Evaluierung. Der einzige kausale Befund stammt aus
Frankreich (Segu 2020, -13 % Leerstandsquote) — deshalb prueft dieser Test
auch, dass die Aussage zu Oesterreich als "nicht erhoben" markiert bleibt und
nicht wieder durch eine Schaetzung ersetzt wird.

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


def _fakt(datei: str, fakt_id: str) -> dict:
    d = json.loads((DATA / datei).read_text(encoding="utf-8"))
    f = next((x for x in d["facts"] if x.get("id") == fakt_id), None)
    assert f is not None, (datei, fakt_id)
    return f


LEER = _fakt("wohnen_pack.json", "leerstandsabgabe_wirkung_2026")
WBH = _fakt("sozialstaat_pack.json", "wohnbeihilfe_at_2026")

# Geprueft wird die Aussage (headline + data); die "Korrigiert"-Notiz in
# context_notes benennt die alten Zahlen absichtlich.
LEER_TEXT = LEER["headline"] + " " + json.dumps(LEER["data"], ensure_ascii=False)
WBH_TEXT = WBH["headline"] + " " + json.dumps(WBH["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Leerstandsabgabe: Inkrafttreten
# --------------------------------------------------------------------------

@pytest.mark.parametrize("land,datum", [
    ("Steiermark", "1.10.2022"),
    ("Tirol", "1.1.2023"),
    ("Vorarlberg", "1.1.2024"),
])
def test_inkrafttreten_je_bundesland(land, datum):
    assert land in LEER["headline"] and datum in LEER["headline"], (land, datum)


def test_die_these_beide_erst_2024_ist_ausdruecklich_verneint():
    k = LEER["data"]["kernsatz_fuer_synthesizer"]
    assert "nicht beide erst 2024" in k


def test_steiermark_war_zuerst():
    s = LEER["data"]["steiermark_stzwag"]
    assert "StZWAG" in s and "LGBl. Nr. 46/2022" in s and "1.10.2022" in s


# --------------------------------------------------------------------------
# Leerstandsabgabe: die Betraege stehen im Gesetz
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["10 bis 25", "20 bis 50", "30 bis 70", "45 bis 100",
                                  "60 bis 135", "75 bis 175", "90 bis 215"])
def test_tiroler_monatsbetraege(wert):
    assert wert in LEER["data"]["tirol_bis_2025"], (wert, LEER["data"]["tirol_bis_2025"])


def test_tiroler_betraege_sind_monatlich_nicht_jaehrlich():
    t = LEER["data"]["tirol_bis_2025"]
    assert "je Monat" in t and "Vorbehaltsgemeinden" in t


@pytest.mark.parametrize("wert", ["18,50", "14,10", "8,20", "2.775", "2.115", "1.230"])
def test_vorarlberger_saetze(wert):
    assert wert in LEER["data"]["vorarlberg_saetze"], (wert, LEER["data"]["vorarlberg_saetze"])


def test_vorarlberg_ist_eine_zweitwohnungsabgabe():
    """Vorarlberg hat keine eigene Leerstandsabgabe — das war im Fakt falsch."""
    v = LEER["data"]["vorarlberg_zag"]
    assert "keine eigene Leerstandsabgabe" in v
    assert "26 Wochen" in v


def test_tiroler_uebergang_ist_benannt():
    assert "31.12.2025" in LEER["data"]["tirol_seit_2026"]
    assert "30 v. H." in LEER["data"]["tirol_basismietwerte"]
    assert "Basismietwerte" in LEER["data"]["tirol_basismietwerte"]


# --------------------------------------------------------------------------
# Leerstandsabgabe: die erfundenen Angaben sind weg
# --------------------------------------------------------------------------

@pytest.mark.parametrize("falsch", [
    "1.000 Euro/Jahr",
    "1.500 Euro/Jahr",
    "3.000 Euro/Jahr",
    "G54/22 vom 2.10.2023: Bundeslaender",
    "Art. 17 Bundes-Verfassungsgesetz",
    "12,5 % Mietwert ab 1. Jahr",
    "INSEE 2018",
])
def test_unbelegte_angaben_sind_weg(falsch):
    assert falsch not in LEER_TEXT, falsch


def test_das_erfundene_vfgh_urteil_ist_als_nicht_auffindbar_markiert():
    k = LEER["data"]["kompetenz"]
    assert "G54/22" in k and "nicht auffindbar" in k
    assert "F-VG 1948" in k


def test_600_wohnungen_nur_noch_als_widerlegte_zahl():
    """Die Zahl darf im Fakt stehen — aber nur als das, was sie ist."""
    w = LEER["data"]["wirkung_oesterreich"]
    assert "600 Wohnungen" in w and "unbelegt entfernt" in w
    assert "keine veröffentlichte Evaluierung" in w
    # und nirgends mehr als Befund
    assert "zurück auf Mietmarkt geschätzt" not in LEER_TEXT


# --------------------------------------------------------------------------
# Leerstandsabgabe: Wirkungsevidenz
# --------------------------------------------------------------------------

def test_frankreich_studie_mit_doi_und_effekt():
    f = LEER["data"]["wirkung_frankreich"]
    assert "10.1016/j.jpubeco.2019.104079" in f
    assert "13 %" in f and "1997 bis 2001" in f
    assert "Segú" in f and "Journal of Public Economics 185" in f


def test_franzoesische_saetze_sind_aktuell():
    f = LEER["data"]["frankreich_rechtsstand"]
    assert "17 %" in f and "34 %" in f
    assert "vor 2023" in f, "der alte Satz muss als alt gekennzeichnet sein"


def test_beide_richtungen_bleiben_beantwortbar():
    k = LEER["data"]["kernsatz_fuer_synthesizer"]
    assert "widerlegt" in k and "nicht gedeckt" in k


def test_vfgh_judikatur_nennt_aufhebung_und_bestand():
    v = LEER["data"]["vfgh_gemeindeverordnungen"]
    assert "V85/2025" in v and "Ehrwald" in v
    for gz in ("V220/2025", "V11/2026", "V266/2025"):
        assert gz in v, gz


# --------------------------------------------------------------------------
# Wohnbeihilfe: amtliche Zahlen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["111", "174", "399", "343", "273", "259", "254",
                                  "243", "237", "266", "265"])
def test_essoss_ausgabenreihe(wert):
    assert wert in WBH["data"]["ausgaben_oesterreich"], wert


@pytest.mark.parametrize("jahr,zahl,aufwand", [
    ("2006", "54.784", "86,1"),
    ("2015", "45.381", "70,1"),
    ("2020", "39.979", "59,6"),
    ("2022", "34.129", "45,7"),
    ("2023", "31.043", "43,5"),
])
def test_wiener_reihe(jahr, zahl, aufwand):
    w = WBH["data"]["wien_bezieher"]
    assert jahr in w and zahl in w and aufwand in w, (jahr, zahl, aufwand)


def test_wiener_modellwechsel_ist_erklaert():
    m = WBH["data"]["wien_modellwechsel"]
    assert "1.3.2024" in m and "LGBl. Nr. 7/2024" in m
    assert "23.985" in m and "22.925" in m


def test_aufwandssteigerung_ist_nachgerechnet():
    """43,5 -> 58,2 Mio. Euro sind rund 34 % — die Zahl im Fakt muss dazu passen."""
    assert round((58.2 / 43.5 - 1) * 100) == 34
    assert "rund 34 %" in WBH["data"]["wien_modellwechsel"]


def test_summe_der_beiden_modelle_ist_keine_bezieherzahl():
    notiz = " ".join(WBH["context_notes"])
    assert "keine Bezieherzahl" in notiz


# --------------------------------------------------------------------------
# Wohnbeihilfe: die alten Zahlen sind weg
# --------------------------------------------------------------------------

@pytest.mark.parametrize("falsch", [
    "~280.000 Bezieher gesamt",
    "Wien ~140.000",
    "~330 Mio EUR/Jahr",
    "Zenz/Schoibl",
    "ca. 600 Wohnungen wurden zurueck auf den Mietmarkt gebracht",
])
def test_unbelegte_wohnbeihilfe_zahlen_sind_weg(falsch):
    assert falsch not in WBH_TEXT, falsch


def test_fehlende_bundesweite_bezieherzahl_ist_benannt():
    k = WBH["data"]["kernsatz_fuer_synthesizer"]
    assert "wird nicht veröffentlicht" in k
    assert "280.000" in k and "keine Quelle" in k


def test_mietbeihilfe_ist_abgegrenzt():
    m = WBH["data"]["wien_mietbeihilfe_abgrenzung"]
    assert "4.651" in m and "9.955" in m
    assert "1.1.2016" in m


# --------------------------------------------------------------------------
# Keine zweite Kopie derselben Zahl
# --------------------------------------------------------------------------

def test_kein_fakt_behauptet_mehr_leerstandsabgabe_seit_2024():
    """Die Startdaten standen in zwei Fakten — die Kopie darf nicht driften."""
    treffer = []
    for datei in ("wohnen_pack.json", "sozialstaat_pack.json"):
        s = (DATA / datei).read_text(encoding="utf-8")
        for m in re.finditer(r"Leerstand[s-]?[Aa]bgabe seit 2024", s):
            treffer.append((datei, s[max(0, m.start() - 60):m.end() + 60]))
    assert not treffer, treffer


def test_wohnbeihilfe_fakt_verweist_auf_den_leerstands_fakt():
    a = WBH["data"]["leerstandsabgabe_abgrenzung"]
    assert "1.10.2022" in a and "1.1.2023" in a and "1.1.2024" in a
    assert "keine Wohnbeihilfe-Maßnahme" in a


# --------------------------------------------------------------------------
# Marker, Trigger, Quellen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("fakt,name", [(LEER, "leerstand"), (WBH, "wohnbeihilfe")])
def test_kein_struktureller_falsch_marker(fakt, name):
    """Beide Fakten bejahen und verneinen je nach Claim — kein Verdict-Override."""
    assert not has_false_verdict_override(fakt["data"]["kernsatz_fuer_synthesizer"]), name


@pytest.mark.parametrize("fakt,name", [(LEER, "leerstand"), (WBH, "wohnbeihilfe")])
def test_phrasings_treffen(fakt, name):
    fehlt = [p for p in fakt["claim_phrasings_handled"] if not trifft(fakt, p.lower())]
    assert not fehlt, (name, fehlt)


@pytest.mark.parametrize("claim", [
    "Die Leerstandsabgabe in Vorarlberg beträgt 3.000 Euro im Jahr",
    "Tirol hat die Leerstandsabgabe 2024 eingeführt",
    "Bringt die Leerstandsabgabe Wohnungen zurück auf den Markt?",
    "Wohnungsleerstandsabgabe Steiermark Höhe",
    "Ist die Leerstandsabgabe verfassungswidrig?",
])
def test_leerstand_batterie(claim):
    assert trifft(LEER, claim.lower()), claim


@pytest.mark.parametrize("claim", [
    "Wie hoch ist die Wohnbeihilfe in Wien?",
    "In Österreich beziehen 280.000 Menschen Wohnbeihilfe",
    "Wie viel gibt Österreich für Wohnbeihilfe aus?",
    "Bekommt man in jedem Bundesland gleich viel Wohnbeihilfe?",
])
def test_wohnbeihilfe_batterie(claim):
    assert trifft(WBH, claim.lower()), claim


def test_quellen_sind_die_zitierten():
    assert "Gesetzesnummer=20000923" in LEER["source_url"]      # TFLAG
    assert "Gesetzesnummer=20001747" in LEER["secondary_url"]   # ZAG Vorarlberg
    assert "wien.gv.at/statistik/publikationen/jahrbuch" in WBH["source_url"]
    assert WBH["secondary_url"].endswith("Sozialschutz_1990-2024_STAT.pdf")


@pytest.mark.parametrize("fakt,name", [(LEER, "leerstand"), (WBH, "wohnbeihilfe")])
def test_beide_fakten_tragen_ihren_datenstand(fakt, name):
    notiz = " ".join(fakt["context_notes"])
    assert "Datenstand" in notiz, name
    assert re.search(r"20\d\d", notiz), name


@pytest.mark.parametrize("fakt,name", [(LEER, "leerstand"), (WBH, "wohnbeihilfe")])
def test_prompt_felder_bleiben_unter_der_kuerzung(fakt, name):
    """Der Synthesizer kuerzt jedes Feld auf 400 Zeichen — ausser dem
    Kernsatz soll deshalb kein Feld darueber liegen."""
    zu_lang = {k: len(v) for k, v in fakt["data"].items()
               if k != "kernsatz_fuer_synthesizer" and len(v) > 400}
    assert not zu_lang, (name, zu_lang)
