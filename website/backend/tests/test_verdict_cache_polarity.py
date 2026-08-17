"""Polaritäts-Guard des semantischen Verdict-Caches.

QA100-Verifikation 2026-07-27: Satz-Embeddings kodieren die ARGUMENT-
REIHENFOLGE praktisch nicht. Die Prod-Logs zeigten drei Cache-Treffer
zwischen bedeutungs-GEGENTEILIGEN Claims, alle weit über der 0.92-
Schwelle:

    cos=0.961  'Windkraft liefert mehr Strom als Photovoltaik'
           ->  'photovoltaik liefert mehr strom als windkraft'
    cos=0.976  'Kernkraft klimaschädlicher als Kohle'
           ->  'kohle klimaschädlicher als kernkraft'
    cos=0.967  'Kohle klimaFREUNDLICHER als Kernkraft'
           ->  'kohle klimaSCHÄDLICHER als kernkraft'

Wirkung ohne Guard: Wer die umgekehrte Vergleichsfrage stellt, bekommt
das exakt gegenteilige Verdict mit voller Konfidenz ausgeliefert, ohne
dass die Pipeline je läuft — und zwar genau bei der Claim-Familie, für
die der L4-Layer eigene Muster hat (G/G2/H/I/J).

Die Asymmetrie ist beabsichtigt: ein zu scharfer Guard kostet nur
Latenz (Cache-Miss -> volle Pipeline), ein zu laxer liefert ein
invertiertes Verdict. Deshalb pinnt diese Suite BEIDE Richtungen —
die Blocker-Fälle und die Paraphrasen, die weiterhin treffen müssen.

Dependency-light: reine Guard-Logik, kein Modell, kein Netz.
"""

import pytest

from services.verdict_cache import (
    _comparison_operands,
    _direction_flipped,
    _has_stem,
    _operands_swapped,
    _polarity_mismatch,
)


# --- Die drei Live-Log-Paare (müssen blocken) ---

def test_live_paar_vertauschte_operanden_windkraft_pv():
    assert _polarity_mismatch(
        "Windkraft liefert in Österreich mehr Strom als Photovoltaik",
        "photovoltaik liefert in österreich mehr strom als windkraft") is True


def test_live_paar_vertauschte_operanden_kernkraft_kohle():
    assert _polarity_mismatch(
        "Dass Kernkraft klimaschädlicher wäre als Kohle, ist ja wohl Unsinn",
        "dass kohle klimaschädlicher wäre als kernkraft, ist ja wohl "
        "unsinn") is True


def test_live_paar_antonym_tausch():
    """Gleiche Reihenfolge, nur das Richtungswort gekippt — der
    Operanden-Check greift hier NICHT, der Antonym-Check muss."""
    assert _polarity_mismatch(
        "Dass Kohle klimafreundlicher wäre als Kernkraft, ist ja wohl Unsinn",
        "dass kohle klimaschädlicher wäre als kernkraft, ist ja wohl "
        "unsinn") is True


# --- Transfer auf andere Domänen ---

@pytest.mark.parametrize("a,b", [
    ("In Kärnten ist der Ausländeranteil höher als in Niederösterreich",
     "in niederösterreich ist der ausländeranteil höher als in kärnten"),
    ("Strom ist in Deutschland teurer als in Frankreich",
     "strom ist in deutschland billiger als in frankreich"),
    ("Wien hat mehr Einwohner als Hamburg",
     "hamburg hat mehr einwohner als wien"),
    ("Die Arbeitslosigkeit steigt stärker als die Inflation",
     "die arbeitslosigkeit sinkt stärker als die inflation"),
])
def test_gegenteilige_vergleiche_blocken(a, b):
    assert _polarity_mismatch(a, b) is True


# --- Echte Paraphrasen müssen weiterhin treffen (Cache-Nutzen) ---

@pytest.mark.parametrize("a,b", [
    ("Ist Spinat eisenreicher als Grünkohl?",
     "hat spinat mehr eisen als grünkohl?"),
    ("Windkraft liefert in Österreich mehr Strom als Photovoltaik",
     "liefert windkraft in österreich mehr strom als photovoltaik?"),
    ("Hat Spinat viel Eisen?", "ist spinat eisenreich?"),
    ("Österreich hat eine hohe Steuerquote",
     "die steuerquote in österreich ist hoch"),
])
def test_paraphrasen_treffen_weiter(a, b):
    assert _polarity_mismatch(a, b) is False


def test_beide_seiten_tragen_beide_pole_kein_flip():
    """Enthält eine Seite BEIDE Richtungswörter ('gefährlicher … oder
    sicherer'), ist nichts gekippt — der Guard verlangt je genau einen
    Pol pro Seite."""
    assert _polarity_mismatch(
        "Ist Atomkraft gefährlicher als Kohle oder sicherer?",
        "ist atomkraft gefährlicher als kohle?") is False


# --- Bausteine ---

def test_comparison_operands_ueberspringt_funktionswoerter():
    assert _comparison_operands(
        "Dass Impfungen gefährlicher wären als die Krankheit, ist Unsinn"
    )[1] == "krankheit"
    assert _comparison_operands("Spinat ist eisenreich") is None


def test_operands_swapped_braucht_beide_richtungen():
    """Nur wenn JEDES Vergleichs-Objekt auf der Gegenseite vor dem 'als'
    steht, sind die Rollen wirklich vertauscht. Ein bloß anderes
    Vergleichs-Objekt (Wien/Graz vs. Wien/Linz) ist KEIN Rollentausch —
    das sind zwei verschiedene Fragen, die der Cosine ohnehin trennt."""
    assert _operands_swapped("Wien ist größer als Graz",
                             "graz ist größer als wien") is True
    assert _operands_swapped("Wien ist größer als Graz",
                             "wien ist größer als linz") is False


def test_comparison_operands_ignoriert_zu_kurze_tokens():
    """Der Mindestlängen-Filter hält Einzelbuchstaben und Kürzel aus der
    Operanden-Erkennung heraus."""
    assert _comparison_operands("Wien ist größer als B") is None


def test_has_stem_ignoriert_un_praefix():
    """'unsicher' ist kein 'sicher' — sonst würde der Antonym-Check auf
    einer verneinten Form falsch anschlagen."""
    assert _has_stem({"sicherer"}, "sicher") is True
    assert _has_stem({"unsicherer"}, "sicher") is False
    assert _has_stem({"ungefährlich"}, "gefährlich") is False


def test_direction_flipped_braucht_gegenpol_auf_der_anderen_seite():
    """Ein einseitig fehlendes Richtungswort ist bloß eine Umformulierung."""
    assert _direction_flipped({"mehr", "strom"}, {"weniger", "strom"}) is True
    assert _direction_flipped({"mehr", "strom"}, {"strom", "liefert"}) is False


# --- Die bestehenden Guard-Zweige bleiben intakt ---

def test_negations_guard_unveraendert():
    assert _polarity_mismatch("Spinat ist eisenreich",
                              "spinat ist nicht eisenreich") is True
    assert _polarity_mismatch("Spinat ist nicht eisenreich",
                              "spinat ist kein eisenlieferant") is False


def test_zahlen_guard_unveraendert():
    assert _polarity_mismatch("PISA-Schnitt 2018 über OECD",
                              "pisa-schnitt 2022 über oecd") is True


# --- QA50D 2026-08-08: fehlende Antonym-Paare -------------------------
# Im deployten Container reproduziert: bei diesen Paaren schwieg der
# Guard, waehrend die Kontrollen (Operanden-Tausch, schaedlich/
# freundlich) korrekt griffen. `_operands_swapped` fing sie nicht auf,
# weil auf beiden Seiten dasselbe Vergleichsobjekt steht ("... als
# Maenner") — es ist eben KEIN Rollentausch, sondern ein Richtungswechsel.
# Produktionswirkung: wer die Gegenfrage stellt, bekommt das gegenteilige
# Verdict mit voller Konfidenz, ohne dass die Pipeline laeuft.

@pytest.mark.parametrize("a,b", [
    ("Frauen gehen in Österreich häufiger zum Arzt als Männer.",
     "Frauen gehen in Österreich seltener zum Arzt als Männer."),
    ("Männer gehen öfter ins Stadion als Frauen.",
     "Männer gehen seltener ins Stadion als Frauen."),
    ("Der Winter war heuer wärmer als im Vorjahr.",
     "Der Winter war heuer kälter als im Vorjahr."),
    ("Die Wartezeit ist länger als früher.",
     "Die Wartezeit ist kürzer als früher."),
    ("Die Bevölkerung wird älter.", "Die Bevölkerung wird jünger."),
    ("Diese Variante ist leichter als die andere.",
     "Diese Variante ist schwerer als die andere."),
])
def test_qa50d_ergaenzte_antonyme_blocken(a, b):
    assert _polarity_mismatch(a, b) is True, (a, b)


def test_echte_paraphrase_wird_weiterhin_durchgelassen():
    """Die entscheidende Gegenprobe: waere sie rot, haette die
    Erweiterung den Cache still deaktiviert statt ihn zu schaerfen."""
    assert _polarity_mismatch(
        "Frauen gehen in Österreich häufiger zum Arzt als Männer.",
        "Gehen Frauen in Österreich häufiger zum Arzt als Männer?"
    ) is False


# ---------------------------------------------------------------------------
# Messgrößen-Paare (2026-08-17, bei der Live-Verifikation von #321 gemessen)
#
# Keine Richtungs-Antonyme, sondern zwei VERSCHIEDENE Größen für dasselbe
# Thema. Log-Belege aus prod b85d1f1:
#   SEMANTIC HIT cos=0.985  'Der Fleischverbrauch …' -> 'der fleischverzehr …'
#   SEMANTIC HIT cos=0.963  'Österreich kommt beim Pro-Kopf-Verbrauch …'
#                            -> 'der fleischverzehr …'
# Beide Male lieferte der Cache das mostly_false des VERZEHRS-Claims auf die
# korrekte VERBRAUCHS-Frage — invertiertes Verdict mit voller Konfidenz, ohne
# dass die Pipeline lief.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("a,b", [
    # Die zwei live gemessenen Paare, wörtlich
    ("Der Fleischverzehr in Österreich liegt bei fast 65 Kilo pro Kopf im Jahr.",
     "Der Fleischverbrauch in Österreich liegt bei fast 65 Kilo pro Kopf im Jahr."),
    ("Der Fleischverzehr in Österreich liegt bei fast 65 Kilo pro Kopf im Jahr.",
     "Österreich kommt beim Pro-Kopf-Verbrauch von Fleisch auf etwa 65 Kilogramm jährlich."),
    # Dieselbe Klasse aus QA100 #44: Kapazität ist nicht Erzeugung
    ("Die installierte Windkraft-Kapazität in Österreich steigt seit Jahren.",
     "Die Windkraft-Erzeugung in Österreich steigt seit Jahren."),
    ("Der Bruttolohn liegt in Österreich bei rund 3.500 Euro monatlich.",
     "Der Nettolohn liegt in Österreich bei rund 3.500 Euro monatlich."),
])
def test_messgroessen_paare_blocken(a, b):
    assert _polarity_mismatch(a, b) is True, (a, b)


def test_messgroessen_paraphrase_trifft_weiter():
    """Muss-Treffer-Kontrolle: dieselbe Messgröße, andere Formulierung —
    der Cache muss weiter greifen, sonst hat die Schärfung ihn abgeschaltet."""
    assert _polarity_mismatch(
        "Der Fleischverzehr in Österreich liegt bei fast 65 Kilo pro Kopf im Jahr.",
        "Liegt der Fleischverzehr in Österreich bei fast 65 Kilo pro Kopf und Jahr?"
    ) is False


def test_beide_messgroessen_auf_einer_seite_kein_mismatch():
    """Ein Claim, der BEIDE Größen nennt, ist kein Gegensatz zum anderen —
    sonst würde jede Nachfrage zum differenzierenden Fakt den Cache sprengen."""
    assert _polarity_mismatch(
        "Verbrauch und Verzehr von Fleisch in Österreich 2024",
        "Fleischverbrauch und Fleischverzehr in Österreich 2024"
    ) is False
