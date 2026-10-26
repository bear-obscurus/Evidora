"""Der Tages-Anteil stand fuenffach zu hoch — und der Trend verkehrt.

Anlass war Befund #55 aus QA50I: "Vaeter nehmen in Oesterreich selten
Karenz" bekam ``mostly_false``. Die Notiz dazu lautete, die 22-28 % muessten
geprueft werden, weil Anteil der VAETER und Anteil der KARENZMONATE zwei
Dinge sind. Beim Nachsehen war es schlimmer als eine Verwechslung.

Zwei Fakten antworten auf diesen Claim. Beide nannten den TAGES-Anteil:

    sozialstaat_pack / kinderbetreuungsgeld_2026
      "Vater-Beteiligung 2024 ca. 22 % der KBG-Bezugsmonate"
      "2008 nur ~3 % der KBG-Monate ... 2014 ~9 %. 2018 ~17 %. 2024 ~22 %"

    gleichstellung_pack / vereinbarkeit_familie_beruf_2026
      "AT Statistik Austria 2024: aehnliche Zahlen, Vaeter-Karenz-Quote ~28 %"

Gemessen hat der Rechnungshof: 2022 entfielen **4,1 %** der genehmigten
KBG-Anspruchstage auf Maenner, auf Frauen 95,9 % — "nur etwa jeder
25. Anspruchstag" (Reihe Bund 2024/31, Auswertung aus OeGK-Daten). Der
Anteil ist nicht gestiegen: Der Vorbericht Bund 2020/24 nannte 4,5 % seit
2009, 2016 waren es 4,4 %. Die Fakten behaupteten also einen Anstieg von
3 % auf 22 %, wo in Wahrheit Stagnation auf rund 4 % stand — falsch im
Wert UND in der Richtung.

## Die dritte Messgroesse, die es gar nicht gibt

Beide Fakten sprachen von einer "Vaeter-KARENZ-Quote". Der Rechnungshof
haelt in derselben Textzahl fest:

    Statistische Daten zur Aufteilung der arbeitsrechtlichen Elternkarenz
    zwischen Frauen und Maennern gab es nicht. Daher war die
    gebraeuchlichste Kennzahl ... der Bezug von Kinderbetreuungsgeld durch
    diesen. Das tatsaechliche Ausmass der Vaeterbeteiligung konnte dadurch
    allerdings nur annaehernd bestimmt werden.

Eine Karenz-Quote ist also nie gemessen worden. Jede Zahl, die so heisst,
ist eine KBG-Zahl.

## Was der Personen-Anteil wirklich ist

Das BKA veroeffentlicht ihn je Geburtsjahrgang: Geburtsjahr 2022
**15,40 %** — 13.067 Vaeter mit Beteiligung von 84.847 abgeschlossenen
Faellen (Stand 27.1.2026), nach System einkommensabhaengig 25,59 % gegen
pauschales Konto 2,96 bis 10,83 %. Das BKA-Wirkungsmonitoring zaehlt
Familienzeitbonus mit und kommt auf 22,6 % (2024). Keine dieser Zahlen ist
28 %, und keine ist ein Tages-Anteil.

Der Rechnungshof nennt auch den Grund fuer die Luecke zwischen 15,40 % und
4,1 %: Bei Geburten des Jahres 2020 bezogen Frauen durchschnittlich 470
Tage, Maenner 124 Tage. Wer Personen zaehlt, gewichtet zwei Monate wie ein
ganzes Jahr.

## Der internationale Vergleich stand auf der falschen Messgroesse

Beide Fakten verglichen den oesterreichischen PERSONEN-Anteil mit
nordeuropaeischen TAGE-Anteilen und schlossen "AT mittleres Niveau" bzw.
"DACH liegt unter Skandinavien-Niveau". Auf einer Messgroesse
(Nordic Statistics SOSU22 aus Nososco, Anteil der von Maennern bezogenen
Leistungstage, Stand 23.10.2025) sieht es anders aus:

    Island 36,3 %   Norwegen 32,7 %   Schweden 31,4 %   Finnland 12,2 %
    Oesterreich 4,1 %  (alle 2022)

Das ist rund ein Achtel, nicht mittleres Niveau. Die zitierten 45-50 % fuer
Schweden und Island sind TAKE-UP-RATES, also Personen-Anteile; die OECD
selbst schreibt im Bericht "Paid leave for fathers" (Oktober 2025) rund
20-30 % der Leistungstage fuer Island, Norwegen, Portugal und Schweden und
fuehrt fuer Oesterreich keinen Tage-Anteil.

Auch die deutsche Zahl war falsch: "79 % Vaeter nehmen Elterngeld" gegen
tatsaechlich 46,4 % der 2023 geborenen Kinder (Destatis, Stand 26.6.2026),
bei beendeten Bezugsdauern von 14,0 Monaten der Muetter gegen 3,4 der
Vaeter.

## Ein Zensus, der VERWORFEN wurde — und warum

Naheliegend waere ein Gate ueber alle Datendateien: Wo ein
oesterreichischer Vaeter-Anteil mit einem Tages- oder Monats-Wort im selben
Satz steht, darf die Zahl nicht ueber rund 6 % liegen. Drei Fassungen
gemessen:

1. Fenster nur LINKS der Prozentzahl: 0 von 3 Giftproben fallen (das
   Messgroessen-Wort steht meist RECHTS — "22 % der KBG-Bezugsmonate"),
   dazu 4 Fehltreffer auf korrektem Text.
2. Fenster beidseitig, Geschlecht an das NAECHSTE Subjekt gebunden:
   immer noch 0 von 4, weil der eigene Satz-Splitter an "ca." trennte und
   die Prozentzahl damit in einem Fragment ohne "Vater" landete. Dieselbe
   Falle wie #227 — deshalb jetzt ``teile_in_einheiten`` aus dem Projekt.
3. Mit dem Projekt-Splitter: 3 von 5 Giftproben fallen, aber ein
   Fehltreffer auf korrektem Text bleibt (der OECD-Satz mit "20-30 %" und
   "Leistungstage"), und zwei Giftproben halten weiter, weil ein blosses
   "AT " nicht als Oesterreich-Marker taugt, ohne auf jedem "at" im
   Deutschen anzuschlagen.

Ein Gate, das auf richtigem Text feuert und zwei von fuenf echten
Befunden uebersieht, ist kein Gate. Verworfen und hier aufgeschrieben,
damit es niemand erneut baut. Gepruefte Alternative ist das, was unten
steht: die Werte und die Messgroessen-Bezeichnung in den beiden Fakten
selbst, plus die Giftproben, die auf dem ALTEN Stand fallen.

Dependency-light: nur Daten und Trigger-Logik, kein Netz.
"""
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

from services._topic_match import substring_or_composite_match  # noqa: E402
from services.synthesizer import PROMPT_MAX_STR  # noqa: E402

#: Der Fakt, der die oesterreichischen KBG-Zahlen traegt.
KBG = ("sozialstaat_pack.json", "kinderbetreuungsgeld_2026")

#: Der Fakt, der den DE/AT/Nordeuropa-Vergleich traegt.
VEREINBARKEIT = ("gleichstellung_pack.json", "vereinbarkeit_familie_beruf_2026")

#: Felder, die VOR dieser Aenderung schon ueber 400 Zeichen lagen. Sie
#: gehoeren nicht zu diesem Befund und werden gemeldet, nicht umgeschrieben.
VORBESTEHEND_ZU_LANG = {
    ("sozialstaat_pack.json", "lalive_zweimueller_2009"),
    ("gleichstellung_pack.json", "iab_motherhood_penalty"),
}


def _fakt(datei: str, fid: str) -> dict:
    with open(os.path.join(DATA, datei), encoding="utf-8") as fh:
        d = json.load(fh)
    treffer = [f for f in d["facts"] if f.get("id") == fid]
    assert len(treffer) == 1, f"{fid} kommt {len(treffer)}x in {datei} vor"
    return treffer[0]


def _volltext(datei: str, fid: str) -> str:
    f = _fakt(datei, fid)
    teile = [str(f.get(k, "")) for k in ("headline", "scope", "source_label",
                                         "source_url", "secondary_url")]
    teile += [v for v in f["data"].values() if isinstance(v, str)]
    teile += [str(x) for x in (f.get("context_notes") or [])]
    teile += [str(x) for x in (f.get("claim_phrasings_handled") or [])]
    return " | ".join(teile)


# --------------------------------------------------------------- die Werte

@pytest.mark.parametrize("wert", [
    "4,1 %",            # Anspruchstage 2022
    "95,9 %",           # die Gegenseite derselben Messgroesse
    "15,40 %",          # Personen, Geburtsjahr 2022
    "22,6 %",           # Personen inkl. Familienzeitbonus, 2024
    "470 Tage",         # Bezugsdauer Frauen, Geburten 2020
    "124 Tage",         # Bezugsdauer Maenner, Geburten 2020
    "Bund 2024/31",     # der Bericht, der die 4,1 % traegt
])
def test_die_gemessenen_werte_stehen_im_kbg_fakt(wert):
    assert wert in _volltext(*KBG)


@pytest.mark.parametrize("wert", [
    "46,4 %",           # DE Vaeterbeteiligung, Geburtsjahr 2023
    "14,0 Monate",      # DE beendete Bezugsdauer Muetter
    "3,4 Monate",       # DE beendete Bezugsdauer Vaeter
    "4,1 %",            # AT auf der Tage-Messgroesse
    "36,3 %",           # Island
    "32,7 %",           # Norwegen
    "31,4 %",           # Schweden
    "12,2 %",           # Finnland
])
def test_die_gemessenen_werte_stehen_im_vereinbarkeits_fakt(wert):
    assert wert in _volltext(*VEREINBARKEIT)


# ------------------------------------------------------------- Giftproben
# Jede dieser Zeichenketten stand bis zum 26.10.2026 in den Daten. Faellt
# eine dieser Pruefungen, ist der widerlegte Wert zurueckgekehrt.

@pytest.mark.parametrize("widerlegt", [
    "22 % der KBG-Bezugsmonate",
    "ca. 22 % der KBG",
    "~3 % der KBG-Monate",
    "2018 ~17 %",
    "2024 ~22 %",
    "SE 45 %",
    "IS 50 %",
    "DK 35 %",
    "AT 22 %",
    "AT mittleres Niveau",
    "Paternity leave use rates",
])
def test_kein_widerlegter_kbg_wert_steht_noch_da(widerlegt):
    assert widerlegt not in _volltext(*KBG)


@pytest.mark.parametrize("widerlegt", [
    "79 % Väter nehmen Elterngeld",
    "Väter-Karenz-Quote ~28 %",
    "Väter-Anteil an Elternzeit-Tagen 30 %",
    "Väter-Anteil 45 %",
    "Norwegen 25 %",
    "DACH liegt unter Skandinavien-Niveau",
    "Skandinavien 30-90 %",
    "AT ähnlich",
    "~25,5 %",
    "Anstieg seit 2010 um 18 pp",
])
def test_kein_widerlegter_vergleichswert_steht_noch_da(widerlegt):
    assert widerlegt not in _volltext(*VEREINBARKEIT)


# ------------------------------------------------- die Messgroessen selbst

@pytest.mark.parametrize("datei,fid", [KBG, VEREINBARKEIT])
def test_beide_messgroessen_sind_benannt(datei, fid):
    """Eine Zahl ohne ihre Messgroesse ist der ganze Befund."""
    text = _volltext(datei, fid).lower()
    assert "anspruchstage" in text or "tage-messgröße" in text \
        or "tage-messgroesse" in text
    assert "personen" in text


@pytest.mark.parametrize("datei,fid", [KBG, VEREINBARKEIT])
def test_die_warnung_gegen_das_vermischen_steht_da(datei, fid):
    text = _volltext(datei, fid).lower()
    assert "nicht vermischen" in text or "nicht vermischt" in text


def test_die_fehlende_karenz_statistik_ist_festgehalten():
    """Der Rechnungshof sagt: Zahlen zur Elternkarenz gibt es nicht."""
    text = _volltext(*KBG)
    assert "liegen nicht vor" in text
    assert "Vaeter-Karenzgesetz" in text or "Väter-Karenzgesetz" in text


def test_der_tages_anteil_wird_nicht_als_anstieg_erzaehlt():
    """Die alte Fassung behauptete 3 % -> 22 %. Gemessen ist Stagnation."""
    text = _volltext(*KBG).lower()
    assert "nicht gestiegen" in text or "stagniert" in text


def test_take_up_rate_ist_als_personen_mass_gekennzeichnet():
    text = _volltext(*VEREINBARKEIT).lower()
    assert "take-up-rates" in text
    assert "keine tage-anteile" in text


# ------------------------------------------------------- das Prompt-Budget
# Lehre aus #135/#136: Das Modell sieht nur den Anfang. Was den Claim
# entscheidet, muss in den ersten PROMPT_MAX_STR Zeichen stehen.

def test_die_entscheidende_zahl_steht_im_budget():
    kern = _fakt(*KBG)["data"]["kernsatz_fuer_synthesizer"]
    kopf = kern[:PROMPT_MAX_STR]
    assert "4,1 %" in kopf, f"4,1 % erst ab Zeichen {kern.find('4,1 %')}"
    assert "ANSPRUCHSTAGE" in kopf
    assert "MESSGROESSEN" in kopf


def test_die_antwort_auf_den_claim_steht_im_kernsatz():
    kern = _fakt(*KBG)["data"]["kernsatz_fuer_synthesizer"]
    assert "selten Karenz" in kern
    assert "ZUTREFFEND" in kern


# ------------------------------------------------------------- Retrieval

@pytest.mark.parametrize("claim", [
    "Väter nehmen in Österreich selten Karenz",
    "Vaeter nehmen in Oesterreich selten Karenz",
    "Wie viele Väter gehen in Österreich in Karenz?",
    "Nur wenige Männer nehmen Karenz in Österreich",
    "Der Väter-Anteil an der Karenz ist in Österreich mittleres Niveau",
])
@pytest.mark.parametrize("datei,fid", [KBG, VEREINBARKEIT])
def test_der_claim_erreicht_beide_fakten(claim, datei, fid):
    assert substring_or_composite_match(_fakt(datei, fid), claim.lower())


def test_die_phrasing_liste_nennt_den_befund():
    """Die AT-Phrasings stehen beim AT-Fakt, die Vergleichs-Phrasing beim
    Vergleichs-Fakt — sonst waere jeder Treffer ein 'Fremdtreffer'."""
    kbg = _fakt(*KBG).get("claim_phrasings_handled") or []
    assert any("selten Karenz" in p for p in kbg)
    assert any("Wie viele Väter" in p for p in kbg)

    verg = _fakt(*VEREINBARKEIT).get("claim_phrasings_handled") or []
    assert any("mittleres Niveau" in p for p in verg)


# ------------------------------------------------------------ Feldlaengen

@pytest.mark.parametrize("datei,fid", [KBG, VEREINBARKEIT])
def test_kein_neues_feld_sprengt_das_budget(datei, fid):
    """<= 400 Zeichen, ausser kernsatz_fuer_synthesizer.

    Die zwei vorbestehenden Ueberlaengen sind namentlich ausgenommen und in
    VORBESTEHEND_ZU_LANG dokumentiert — sie gehoeren nicht zu diesem Befund.
    """
    zu_lang = {
        k for k, v in _fakt(datei, fid)["data"].items()
        if isinstance(v, str) and len(v) > 400
        and k != "kernsatz_fuer_synthesizer"
        and (datei, k) not in VORBESTEHEND_ZU_LANG
    }
    assert not zu_lang, zu_lang


@pytest.mark.parametrize("datei,feld", sorted(VORBESTEHEND_ZU_LANG))
def test_die_vorbestehenden_ueberlaengen_sind_noch_da(datei, feld):
    """Verhindert, dass die Ausnahmeliste still zur Altlast wird.

    Faellt diese Pruefung, wurde das Feld gekuerzt — dann gehoert es aus
    VORBESTEHEND_ZU_LANG heraus.
    """
    fid = KBG[1] if datei == KBG[0] else VEREINBARKEIT[1]
    assert len(_fakt(datei, fid)["data"][feld]) > 400


# ----------------------------------------------------------------- Quellen

def test_der_bmsgpk_link_ist_ersetzt():
    """Er antwortete mit 200 und landete auf der Ministeriums-STARTSEITE."""
    text = _volltext(*KBG)
    assert "bmsgpk.gv.at/themen/familie/kinderbetreuungsgeld" not in text
    assert "bundeskanzleramt.gv.at/agenda/familie/kinderbetreuungsgeld" in text


def test_der_bmfsfj_link_ist_ersetzt():
    """Er antwortete mit 200 und landete auf lokale-buendnisse-fuer-familie.de."""
    text = _volltext(*VEREINBARKEIT)
    assert "elterngeldstatistik-2024-218596" not in text
    assert "destatis.de" in text


def test_der_rechnungshof_bericht_ist_verlinkt():
    assert "2024_31_Kinderbetreuungsgeld_FuP.pdf" in _volltext(*KBG)


# -------------------------------------------------------------------- ACOG
# Zwei Links lieferten 404. ACOG hat /clinical/clinical-guidance/.../articles/
# auf /clinical/guidance/ umgestellt und das "and" aus dem FAQ-Pfad entfernt.
# Beide Ziele sind DIESELBEN Dokumente wie vorher, nur unter neuer Adresse.

def _repro() -> str:
    with open(os.path.join(DATA, "reproduktion_pack.json"), encoding="utf-8") as fh:
        return fh.read()


@pytest.mark.parametrize("tot", [
    "/clinical/clinical-guidance/practice-bulletin/articles/2018/12/optimizing-postpartum-care",
    "/womens-health/faqs/combined-hormonal-birth-control-pill-patch-and-ring",
])
def test_kein_toter_acog_pfad_steht_noch_da(tot):
    assert tot not in _repro()


@pytest.mark.parametrize("lebend", [
    "https://www.acog.org/clinical/guidance/committee-opinion/2018/05/optimizing-postpartum-care",
    "https://www.acog.org/womens-health/faqs/combined-hormonal-birth-control-pill-patch-ring",
])
def test_der_neue_acog_pfad_steht_da(lebend):
    assert lebend in _repro()


def test_736_heisst_committee_opinion_nicht_practice_bulletin():
    """Der alte Pfad nannte eine Bulletin-Gattung, die es hier nicht gibt."""
    text = _repro()
    assert "ACOG Committee Opinion 736" in text
    assert "ACOG Practice Bulletin +" not in text
