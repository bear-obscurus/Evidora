"""Jede dokumentierte Phrasing trifft ihren eigenen Fakt. Ohne Ausnahme.

``claim_phrasings_handled`` ist eine Zusage: So formuliert erreicht ein Claim
diesen Fakt. Am 29.9.2026 war die Zusage in **53 Fällen falsch** — auf 40
Fakten in vier Dateien, gemessen mit der echten Pipeline:

    20  awmf.json          (16 Fakten)
    14  iqs_bildung.json   ( 9 Fakten)
    12  who_hearing.json   (10 Fakten)
     7  ams_wifo.json      ( 5 Fakten)

48 der 53 scheiterten an genau EINER Gruppe, und das Muster war überall
dasselbe: Die Phrasing nennt die Sache beim Fachbegriff, die Trigger-Gruppe
verlangt den Oberbegriff.

    awmf_epilepsie   "Lamotrigin und Levetiracetam sind Erstlinien-Antiepileptika"
                     c/0 verlangt epilepsie|anfall|krampfanfall
    awmf_parkinson   "Tiefe Hirnstimulation ist bei motorischen Fluktuationen …"
                     c/0 verlangt parkinson|morbus parkinson|ipd

## Zwei verschiedene Ursachen, zwei verschiedene Fixes

1. DER TRIGGER KANNTE DAS WORT NICHT. Dann wurde die Gruppe ergänzt — mit
   spezifischen Begriffen („wie viele", „bedarf", „hörgerät", „aspirin"),
   nicht mit breiten. Wo der Wirkstoff schon in der NACHBAR-Gruppe stand,
   ging das nicht: Ein Token in zwei Gruppen derselben Regel ist
   Stichwort-Degeneration, und dafür gibt es seit #211 ein eigenes Gate.
   Diese sechs Fälle bekamen eine eigene ``trigger_all``-Regel, die den
   Wirkstoff gegen seinen Kontext stellt.

2. DIE PHRASING VERSPRACH MEHR, ALS SIE HERGAB. Sechs Phrasings ließen die
   Dimension weg, die der Fakt zum Korrektsein braucht — meist das Land:

       "Bildungs-Krise PISA"                    -> + " Österreich"
       "Migrationshintergrund schlechtere PISA" -> + " Österreich"
       "Klassen DACH-Vergleich"                 -> "Klassen-Größe DACH-…"
       "Bau-Arbeiter werden taub"               -> "… werden vom Lärm taub"

   Der Trigger ist hier im Recht: ``nbb_2024_pisa_2022_at_results`` führt
   Österreich-Daten, und eine Phrasing ohne Land hätte deutsche PISA-Claims
   mitgenommen. Nicht das Gate wurde gelockert, sondern die Zusage
   berichtigt.

## Messung

    verlorene Muss-Treffer   53 -> 0     (alle 2.616 Phrasings, exakter Pass)
    Fremdtreffer-Paare      335 -> 346

Die elf neuen Paare sind einzeln gesichtet und stehen unten. Acht stammen
aus den Phrasing-Korrekturen: „Migrationshintergrund schlechtere PISA" traf
vorher GAR NICHTS und erreicht mit dem Land jetzt vier passende Fakten,
darunter ``oeif_pisa_migrationshintergrund_2022``. Das ist kein Leck,
sondern das Routing, das die Phrasing immer gemeint hat.

Keine Netzabfrage, kein Modell. Laufzeit unter einer Sekunde.
"""

import copy
import glob
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

from services._topic_match import substring_or_composite_match  # noqa: E402

TF = ("trigger_keywords", "trigger_composite", "trigger_all")


def _fid(it):
    return it.get("id") or it.get("topic") or ""


def _laden():
    alle = []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if not isinstance(v, list):
                continue
            for it in v:
                if isinstance(it, dict) and any(it.get(t) for t in TF):
                    alle.append((os.path.basename(p), it))
    return alle


ALLE = _laden()
DATEIEN = sorted({d for d, _it in ALLE})
FAKT = {_fid(it): it for _d, it in ALLE}


# ---------------------------------------------------------------------------
# Das Gate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("datei", DATEIEN)
def test_jede_phrasing_trifft_ihren_fakt(datei):
    """Ohne Ausnahmeliste. Wer eine Phrasing hinschreibt, die nicht trifft,
    sieht es hier — nicht erst in einer QA-Runde ein halbes Jahr spaeter."""
    verloren = [(_fid(it), ph) for d, it in ALLE if d == datei
                for ph in it.get("claim_phrasings_handled") or []
                if not substring_or_composite_match(it, ph.lower())]
    assert not verloren, f"{datei}: {len(verloren)} Phrasings treffen nicht — {verloren}"


def test_gate_schlaegt_an():
    """Gift-Probe: einem Fakt seine Krankheits-Gruppe nehmen. Das Gate MUSS
    seine Phrasings melden, sonst beweist die Null oben nichts."""
    it = copy.deepcopy(FAKT["awmf_epilepsie"])
    it["trigger_keywords"] = []
    it["trigger_composite"][0] = ["gibtesnicht"]
    it["trigger_all"] = []
    verloren = [ph for ph in it["claim_phrasings_handled"]
                if not substring_or_composite_match(it, ph.lower())]
    assert len(verloren) == len(it["claim_phrasings_handled"]), verloren


def test_alle_fakten_sind_erfasst():
    """Sonst prueft das Gate eine Teilmenge und meldet trotzdem gruen."""
    assert len(ALLE) >= 660, len(ALLE)
    assert sum(len(it.get("claim_phrasings_handled") or []) for _d, it in ALLE) >= 2600


# ---------------------------------------------------------------------------
# Die vier Bloecke, je ein Beleg
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fid,phrasing", [
    # awmf: Wirkstoff genannt, Krankheit verlangt -> eigene trigger_all-Regel
    ("awmf_epilepsie", "Lamotrigin und Levetiracetam sind Erstlinien-Antiepileptika"),
    ("awmf_parkinson", "Tiefe Hirnstimulation ist bei motorischen Fluktuationen indiziert"),
    ("awmf_rheumatoide_arthritis", "Biologika sind bei MTX-Versagen indiziert"),
    # awmf: Verfahren/Parameter genannt -> Gruppe ergaenzt
    ("awmf_nvl_diabetes_typ2", "HbA1c-Ziel sollte bei jedem Diabetiker unter 6,5 liegen"),
    ("awmf_copd", "Lungenrehabilitation ist bei COPD evidenzbasiert"),
    # iqs_bildung: Mengen-Marker fehlte
    ("nbb_2024_lehrkraefte_pensionierung", "Wie viele Lehrer in Österreich"),
    ("nbb_2024_bildungsausgaben_bip", "Wie viel gibt Österreich für Bildung aus"),
    # who_hearing
    ("praevention_kosten_effekt_2026", "Hoergeraet-Subvention lohnt sich"),
    ("ototoxische_medikamente_2026", "Aspirin schadet dem Gehoer"),
    # ams_wifo
    ("ams_at_langzeit_2024", "Wie lange sind Arbeitslose in Österreich arbeitslos"),
    ("ams_at_pressespiegel_jahresbericht_2024", "Wie aktuell sind AMS-Statistiken"),
])
def test_block_belege(fid, phrasing):
    assert phrasing in (FAKT[fid].get("claim_phrasings_handled") or []), (
        f"{fid}: Phrasing umformuliert? {phrasing!r}")
    assert substring_or_composite_match(FAKT[fid], phrasing.lower())


def test_bundeslaender_stehen_in_der_land_gruppe():
    """``nbb_2024_ganztagsschule_quote`` fuehrte wien/vorarlberg/tirol in der
    ASPEKT-Gruppe. „Ganztagsschule Anteil Wien" scheiterte darum an der
    LAND-Gruppe. Sie dorthin zu kopieren waere Stichwort-Degeneration
    gewesen — sie sind verschoben, nicht verdoppelt."""
    it = FAKT["nbb_2024_ganztagsschule_quote"]
    aspekt, land = it["trigger_composite"][1], it["trigger_composite"][2]
    for ort in ("wien", "vorarlberg", "tirol"):
        assert ort in land, ort
        assert ort not in aspekt, ort


# ---------------------------------------------------------------------------
# Die korrigierten Phrasings
# ---------------------------------------------------------------------------

KORRIGIERT = [
    ("nbb_2024_pisa_2022_at_results", "Bildungs-Krise PISA",
     "Bildungs-Krise PISA Österreich", "AT-Fakt, Phrasing nannte kein Land"),
    ("nbb_2024_migrant_leistungs_spread", "Migrationshintergrund schlechtere PISA",
     "Migrationshintergrund schlechtere PISA Österreich", "dito"),
    ("nbb_2024_migrant_leistungs_spread", "Schüler mit Migrationshintergrund Bildung",
     "Schüler mit Migrationshintergrund Bildung Österreich", "dito"),
    ("nbb_2024_klassengroesse_dach", "Klassen DACH-Vergleich",
     "Klassen-Größe DACH-Vergleich", "Fakt misst die Groesse, Phrasing nannte sie nicht"),
    ("nbb_2024_spf_bestand_at", "Inklusion Schule Österreich",
     "Inklusions-Quote Schule Österreich", "Fakt ist eine Quote"),
    ("laerm_arbeit_2026", "Bau-Arbeiter werden taub",
     "Bau-Arbeiter werden vom Lärm taub", "Fakt ist laermbedingter Hoerverlust"),
]


@pytest.mark.parametrize("fid,alt,neu,_warum", KORRIGIERT,
                         ids=[k[0] + "-" + k[1][:18] for k in KORRIGIERT])
def test_korrigierte_phrasing_steht_so_da(fid, alt, neu, _warum):
    phr = FAKT[fid].get("claim_phrasings_handled") or []
    assert neu in phr, f"{fid}: {neu!r} fehlt"
    assert alt not in phr, f"{fid}: die alte Fassung {alt!r} ist zurueck"


@pytest.mark.parametrize("fid,alt,_neu,_warum", KORRIGIERT,
                         ids=[k[0] + "-" + k[1][:18] for k in KORRIGIERT])
def test_die_alte_fassung_traf_wirklich_nicht(fid, alt, _neu, _warum):
    """Belegt, dass die Korrektur noetig war und nicht Kosmetik."""
    assert not substring_or_composite_match(FAKT[fid], alt.lower())


# ---------------------------------------------------------------------------
# Die elf neuen Fremdtreffer — gesichtet
# ---------------------------------------------------------------------------

# Fakten, die durch diese Runde eine fremde dokumentierte Phrasing dazu
# bekamen. Jede Zeile ist gesichtet: am Thema oder ausdruecklich erwuenscht.
NEUE_FREMDTREFFER = {
    # "diagnose" in die ADHS-Leitlinie — eine Diagnose-Zunahme beruehrt die
    # Diagnosekriterien, die der Fakt beschreibt.
    ("awmf_adhs", "ADHS-Diagnose nimmt dramatisch zu"),
    # Die korrigierten Phrasings erreichen jetzt AUCH die thematisch
    # passenden Nachbar-Fakten. Vorher trafen sie gar nichts.
    ("inklusions_quoten_effekte_2026", "Inklusions-Quote Schule Österreich"),
    ("oeif_migrationshintergrund_at_2023", "Migrationshintergrund schlechtere PISA Österreich"),
    ("oeif_migrationshintergrund_at_2023", "Schüler mit Migrationshintergrund Bildung Österreich"),
    ("oeif_migrationssaldo_at_2023", "Migrationshintergrund schlechtere PISA Österreich"),
    ("oeif_migrationssaldo_at_2023", "Schüler mit Migrationshintergrund Bildung Österreich"),
    ("oeif_pisa_migrationshintergrund_2022", "Migrationshintergrund schlechtere PISA Österreich"),
    ("pisa_2022_dach", "Bildungs-Krise PISA Österreich"),
    ("pisa_2022_dach", "Migrationshintergrund schlechtere PISA Österreich"),
    ("timss_2023_at", "Bildungs-Krise PISA Österreich"),
    # "zu wenig" in die Bildungsausgaben-Gruppe: Ein Lehrermangel-Claim
    # erreicht damit auch die Ausgabenzahlen. Unpraezise, aber einschlaegig.
    ("nbb_2024_bildungsausgaben_bip", "zu wenige Lehrer Österreich"),
}


@pytest.mark.parametrize("fid,phrasing", sorted(NEUE_FREMDTREFFER))
def test_gesichteter_fremdtreffer_besteht(fid, phrasing):
    """Wer einen davon wieder schliesst, dreht die Zeile um — dann ist die
    Sichtung von damals ueberholt und gehoert neu begruendet."""
    assert substring_or_composite_match(FAKT[fid], phrasing.lower())
