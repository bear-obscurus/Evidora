"""Der Jahres-Nachfilter der Analyse-Stufe (services/_jahresfilter.py).

Messung 5.11.2026: 20 von 98 Analysen erfanden Jahreszahlen (2023/2024), vor
allem in den factcheck_queries, die 17 Dienste nutzen. Mit dem Datum in der
Nutzernachricht (#316, 21.11.2026) haengte das Modell stattdessen "2026" an
Gegenwarts-Claims (27 bis 30 von 98). Eine Regelzeile im Prompt half nicht.

Der Filter entfernt aus factcheck_queries, pubmed_queries und entities jedes
Jahr, das weder im Claim steht noch aus ihm folgt (Regel E). Die Batterie:
Jahr im Claim bleibt, relatives Jahr bleibt, erfundenes Jahr fliegt raus,
Index mit Ausgabejahr im Claim bleibt.

Die Claims hier sind ausgedacht, keiner stammt von Nutzern oder aus dem
QA-Katalog. Modellfrei: reine Logik, kein Netz.
"""
import datetime
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services._jahresfilter import erlaubte_jahre, jahre_nachfiltern  # noqa: E402

HEUTE = datetime.date(2026, 11, 21)


def _filter(claim, fc=(), pm=(), ent=(), heute=HEUTE):
    a = {"claim": claim, "category": "other", "factcheck_queries": list(fc),
         "pubmed_queries": list(pm), "entities": list(ent)}
    weg = jahre_nachfiltern(claim, a, heute)
    return a, weg


# --- Jahr im Claim bleibt -------------------------------------------------------

@pytest.mark.parametrize("claim, query", [
    ("Die Arbeitslosigkeit in Österreich sank 2025", "Arbeitslosigkeit Österreich 2025"),
    ("2026 kamen weniger Menschen über das Mittelmeer als 2025", "Mittelmeer Ankünfte 2025 2026"),
    ("Die Wintersaison 2026/27 beginnt früher", "Wintersaison 2026/27 Beginn"),
    ("Die Wintersaison 2026/27 beginnt früher", "Saison 2027"),
    ("In der Kalenderwoche 41 2026 stieg die Rate", "Rate KW 41 2026"),
    ("Schon 1949 war der Pegel niedrig", "Pegel 1949 Niedrigwasser"),
])
def test_jahr_im_claim_bleibt(claim, query):
    a, weg = _filter(claim, fc=[query])
    assert a["factcheck_queries"] == [query]
    assert weg == []


def test_der_claim_als_eigener_suchbegriff_verliert_nichts():
    """So sieht die Notfall-Antwort aus: Der Claim selbst ist der Suchbegriff."""
    for claim in ("Ab 2027 steigt die Pension um 3 Prozent",
                  "Zwischen 2010 und 2015 sank die Quote",
                  "Heute ist es kälter als 1990, also gibt es keinen Klimawandel",
                  "Die Grenze liegt seit 1.1.2026 bei 551,10 Euro"):
        a, weg = _filter(claim, fc=[claim], ent=[claim])
        assert a["factcheck_queries"] == [claim] and a["entities"] == [claim]
        assert weg == []


# --- erfundenes Jahr fliegt raus --------------------------------------------------

def test_erfundenes_jahr_fliegt_aus_allen_drei_feldern():
    a, weg = _filter("Die Inflation in Österreich ist hoch",
                     fc=["Inflation Österreich 2026", "Teuerung Österreich 2023 2024"],
                     pm=["inflation Austria 2024"],
                     ent=["Inflation", "2026", "Österreich"])
    assert a["factcheck_queries"] == ["Inflation Österreich", "Teuerung Österreich"]
    assert a["pubmed_queries"] == ["inflation Austria"]
    assert a["entities"] == ["Inflation", "Österreich"]
    assert sorted(weg) == [2023, 2024, 2024, 2026, 2026]


def test_falsch_aufgeloestes_relatives_jahr_fliegt_raus():
    a, _ = _filter("Letztes Jahr gab es mehr Asylanträge", fc=["Asylanträge 2024", "Asylanträge 2025"])
    assert a["factcheck_queries"] == ["Asylanträge", "Asylanträge 2025"]


def test_grenzfall_letzte_jahre_ohne_zahl_traegt_kein_jahr():
    """'in den letzten Jahren' ohne N: Grenzfall nach Regel E, zaehlt als erfunden."""
    a, weg = _filter("Die Luft in Graz hat sich in den letzten Jahren verbessert",
                     fc=["Graz Feinstaub Entwicklung 2020 2026"])
    assert a["factcheck_queries"] == ["Graz Feinstaub Entwicklung"]
    assert weg == [2020, 2026]


# --- relatives Jahr bleibt --------------------------------------------------------

@pytest.mark.parametrize("claim, bleibt, faellt", [
    ("Heuer gab es mehr Asylanträge", 2026, 2025),
    ("Die Mieten sind aktuell so hoch wie nie", 2026, 2024),
    ("Das Wetter in Linz ist heute trüb", 2026, 2025),
    ("Letztes Jahr gab es mehr Asylanträge", 2025, 2024),
    ("Im Vorjahr sank die Quote", 2025, 2026),
    ("Vor drei Jahren war die Inflation zweistellig", 2023, 2022),
    ("Vor 10 Jahren kamen mehr Menschen", 2016, 2015),
    ("Nächstes Jahr steigt die Pension", 2027, 2026),
    ("Naechstes Jahr steigt die Pension", 2027, 2026),
    ("Unemployment rose last year", 2025, 2026),
])
def test_relatives_jahr_bleibt(claim, bleibt, faellt):
    a, _ = _filter(claim, fc=[f"Thema {bleibt}", f"Anderes {faellt}"])
    assert a["factcheck_queries"] == [f"Thema {bleibt}", "Anderes"]


def test_in_den_letzten_n_jahren_ist_ein_bereich():
    a, _ = _filter("In den letzten fünf Jahren stiegen die Mieten um 30 Prozent",
                   fc=["Mieten 2021 2026", "Mieten 2020"])
    assert a["factcheck_queries"] == ["Mieten 2021 2026", "Mieten"]


def test_das_tagesdatum_bleibt_wenn_der_claim_heute_sagt():
    a, _ = _filter("Das Wetter in Linz ist heute trüb", fc=["Wetter Linz 21.11.2026"])
    assert a["factcheck_queries"] == ["Wetter Linz 21.11.2026"]


def test_das_bezugsjahr_kommt_aus_dem_heutigen_datum():
    """Am 3.1.2027 ist 'letztes Jahr' 2026, nicht 2025."""
    claim = "Letztes Jahr gab es mehr Asylanträge"
    a, _ = _filter(claim, fc=["Asylanträge 2026", "Asylanträge 2025"], heute=datetime.date(2027, 1, 3))
    assert a["factcheck_queries"] == ["Asylanträge 2026", "Asylanträge"]


def test_die_laufende_saison_umfasst_zwei_kalenderjahre():
    """Grippesaison heuer: J-1 .. J+1 (so las schon die Grundlinie 5.11. 'dieses Jahr')."""
    a, _ = _filter("Die Grippesaison ist heuer besonders mild",
                   fc=["Grippesaison 2025/26", "Grippesaison 2026/27", "Grippe 2023/24"])
    assert a["factcheck_queries"] == ["Grippesaison 2025/26", "Grippesaison 2026/27", "Grippe"]


def test_um_letztes_jahr_gibt_es_kein_plus_minus_eins():
    """Nur die laufende Saison reicht ueber zwei Jahre. 'letztes Jahr' bleibt J-1,
    auch mit Saisonwort: Ein Modell ohne Datum schrieb 2024 fuer 'dieses Jahr'
    (Grundlinie 5.11., #79), und das darf nicht durchrutschen."""
    a, weg = _filter("Die Grippewelle war im Vorjahr stark", fc=["Grippewelle 2024", "Grippewelle 2025"])
    assert a["factcheck_queries"] == ["Grippewelle", "Grippewelle 2025"]
    assert weg == [2024]


def test_ohne_saisonwort_gibt_es_kein_plus_minus_eins():
    a, _ = _filter("Die Inflation ist heuer hoch", fc=["Inflation 2025", "Inflation 2026"])
    assert a["factcheck_queries"] == ["Inflation", "Inflation 2026"]


# --- Index mit Ausgabejahr --------------------------------------------------------

def test_index_mit_ausgabejahr_im_claim_bleibt():
    a, weg = _filter("Österreich lag im MIPEX 2020 auf Rang 20",
                     fc=["MIPEX 2020 Österreich Rang", "MIPEX 2020 2026"],
                     ent=["MIPEX 2020", "Österreich"])
    assert a["factcheck_queries"] == ["MIPEX 2020 Österreich Rang", "MIPEX 2020"]
    assert a["entities"] == ["MIPEX 2020", "Österreich"]
    assert weg == [2026]


def test_index_ohne_jahr_im_claim_verliert_das_erfundene_ausgabejahr():
    a, _ = _filter("Ungarn liegt im Transformationsindex weit hinten",
                   fc=["BTI 2026 Ungarn Rang", "Bertelsmann Transformation Index 2024 Ungarn"])
    assert a["factcheck_queries"] == ["BTI Ungarn Rang", "Bertelsmann Transformation Index Ungarn"]


# --- Begrenzung durch ein Jahr im Claim, Ereignisse (Regel E.3/E.4) ------------------

@pytest.mark.parametrize("claim, bleibt, faellt", [
    ("Die Pressefreiheit in Serbien hat sich seit 2023 verschlechtert", [2024, 2025, 2026], [2022]),
    ("Vor 2022 kam das meiste Gas aus Russland", [2020, 2021], [2023]),
    ("Zwischen 2010 und 2015 sank die Geburtenrate", [2012], [2016]),
    ("In den 2010er Jahren wuchs die Wirtschaft", [2014], [2021]),
    ("Seit Corona arbeiten mehr Menschen von daheim", [2021, 2026], [2018]),
    ("Der Brexit hat den Handel geschwächt", [2016, 2020], [2026]),
    ("Das Pariser Abkommen verpflichtet auch Österreich", [2015], [2026]),
])
def test_begrenzung_und_ereignis_aus_dem_claim(claim, bleibt, faellt):
    fc = [f"Thema {j}" for j in bleibt] + [f"Fremd {j}" for j in faellt]
    a, weg = _filter(claim, fc=fc)
    assert a["factcheck_queries"] == [f"Thema {j}" for j in bleibt] + ["Fremd"]
    assert weg == faellt


def test_teilweise_erlaubter_bereich_behaelt_das_erlaubte_jahr():
    a, weg = _filter("Die Pressefreiheit in Serbien hat sich seit 2023 verschlechtert",
                     fc=["Serbien Pressefreiheit 2020-2026"])
    assert a["factcheck_queries"] == ["Serbien Pressefreiheit 2026"]
    assert weg == [2020]


# --- was mit dem Jahr wegfaellt, und was keine Jahreszahl ist -----------------------

@pytest.mark.parametrize("vorher, nachher", [
    ("Wetter Linz 20.11.2025", "Wetter Linz"),
    ("Inflation im Jahr 2026 Österreich", "Inflation Österreich"),
    ("Teuerung (2026)", "Teuerung"),
    ("Inflation November 2026 Österreich", "Inflation Österreich"),
    ("Inflation 2026-11-21", "Inflation"),
    ("Mieten 2026, Wien", "Mieten, Wien"),
    ("Pension 2026-Erhöhung", "Pension Erhöhung"),
    ("Quote 2023 vs. 2024", "Quote"),
])
def test_datum_monat_und_bindewort_gehen_mit(vorher, nachher):
    a, _ = _filter("Die Inflation ist hoch", fc=[vorher])
    assert a["factcheck_queries"] == [nachher]


def test_kein_haengendes_vs_wenn_ein_vergleichsjahr_bleibt():
    a, _ = _filter("Die Arbeitslosigkeit in Österreich sank 2025", fc=["Arbeitslosigkeit 2024 vs 2025"])
    assert a["factcheck_queries"] == ["Arbeitslosigkeit 2025"]


def test_ein_betrag_ist_keine_jahreszahl():
    a, weg = _filter("Ein Arzt verdient zu wenig",
                     fc=["Arzt Gehalt 2000 Euro", "Arzt Gehalt € 1950", "Arzt 2026 Personen"])
    assert a["factcheck_queries"] == ["Arzt Gehalt 2000 Euro", "Arzt Gehalt € 1950", "Arzt 2026 Personen"]
    assert weg == []


def test_nach_einem_betrag_mit_waehrung_ist_die_zahl_wieder_ein_jahr():
    """'220 Euro 2024': Das 'Euro' gehoert zur 220, 2024 ist ein Jahr
    (Vorpruefung 21.11.2026, Grundlinie #91)."""
    a, weg = _filter("Schwarzfahren soll künftig mehr kosten", fc=["Schwarzfahren Strafe 220 Euro 2024"])
    assert a["factcheck_queries"] == ["Schwarzfahren Strafe 220 Euro"]
    assert weg == [2024]


def test_tausenderpunkt_im_claim_zaehlt_als_woertlich():
    a, weg = _filter("Der Bonus beträgt 2.000 pro Kopf", fc=["Bonus 2000 pro Kopf"])
    assert a["factcheck_queries"] == ["Bonus 2000 pro Kopf"] and weg == []


def test_leere_und_doppelte_eintraege_fallen_weg():
    a, _ = _filter("Die Inflation ist hoch",
                   fc=["Inflation Österreich 2026", "Inflation Österreich 2025", "2026"],
                   ent=["2026", "21.11.2026", "Inflation"])
    assert a["factcheck_queries"] == ["Inflation Österreich"]
    assert a["entities"] == ["Inflation"]


def test_claim_und_andere_felder_bleiben_unberuehrt():
    a = {"claim": "Die Inflation ist 2026 hoch", "category": "economy", "subcategory": "inflation",
         "eurostat_relevant": True, "factcheck_queries": ["Inflation 2026", 7, None],
         "entities": [["verschachtelt 2020"]]}
    weg = jahre_nachfiltern("Die Inflation ist hoch", a, HEUTE)
    assert a["claim"] == "Die Inflation ist 2026 hoch"
    assert a["factcheck_queries"] == ["Inflation", 7, None]
    assert a["entities"] == [["verschachtelt 2020"]]
    assert a["subcategory"] == "inflation" and a["eurostat_relevant"] is True
    assert "pubmed_queries" not in a
    assert weg == [2026]


def test_ohne_jahr_aendert_sich_nichts():
    fc = ["Inflation Österreich", "Teuerung (Verbraucherpreise) - aktuell"]
    a, weg = _filter("Die Inflation ist hoch", fc=fc, ent=["COVID-19", "G20", "ISO 14001"])
    assert a["factcheck_queries"] == fc
    assert a["entities"] == ["COVID-19", "G20", "ISO 14001"]
    assert weg == []


def test_keine_analyse_kein_absturz():
    assert jahre_nachfiltern("Die Inflation ist hoch", None, HEUTE) == []
    assert jahre_nachfiltern("", {"factcheck_queries": "Inflation 2026"}, HEUTE) == [2026]


def test_erlaubte_jahre_sind_eine_menge_im_gueltigen_bereich():
    jahre = erlaubte_jahre("Vor 2022 war alles anders, heute nicht", HEUTE)
    assert 2021 in jahre and 2026 in jahre and 2022 in jahre
    assert 2023 not in jahre and 2025 not in jahre
    assert min(jahre) >= 1900 and max(jahre) <= 2099
