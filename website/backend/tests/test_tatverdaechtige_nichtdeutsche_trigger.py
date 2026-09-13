"""QA50F-Befund 6: die amtliche Bezeichnung traf den amtlichen Fakt nicht.

Befund 6 lautete „Verdachts- statt Verurteiltenstatistik": „Migranten begehen
in Deutschland überproportional viele Straftaten" bekam `mostly_true@0.85`,
belegt mit PKS-Tatverdächtigen, als wären es Täter.

Nachgemessen am 2026-09-13 mit vier sachlich formulierten Varianten:

    v1  „Migranten begehen … überproportional viele Straftaten"
        -> mostly_false@0.85, nennt die Zahlen ausdruecklich als
           TATVERDAECHTIGE und die demografischen Einschraenkungen
    v4  dasselbe verneint -> mostly_true@0.85 (beide Richtungen konsistent)
    v2  „Nichtdeutsche sind unter den Tatverdächtigen der deutschen
        Polizeilichen Kriminalstatistik überrepräsentiert"
        -> unverifiable@0.1, zweimal: „keine Daten zur PKS Deutschlands"
    v3  „Nichtdeutsche werden … überproportional oft verurteilt"
        -> unverifiable@0.1 — es gibt keine Verurteilten-Daten

v2 ist der Satz, fuer den die PKS die RICHTIGE Messgroesse ist — und genau
er bekam nichts, waehrend der unschaerfere „begehen"-Satz die BKA-Zahlen
bekam. `source_coverage` zeigte: bei v1 lieferten „Eurostat Crime + DACH PKS"
und „Migrations-Konsens" Treffer, bei v2 fehlten beide ganz. Kein
Verwerfen, sondern kein Ausloesen: „Nichtdeutsche" — die amtliche
BKA-Bezeichnung — stand in keiner Personen-Gruppe der beiden Fakten, und
dem Migrations-Fakt fehlte „tatverdächtig".

Beidseitig gepinnt, wie im Ueber-Trigger-Sweep: die Formulierungen ueber die
Statistik muessen treffen, Alltagssaetze mit denselben Woertern nicht.
Gegen 1.173 Projekt-Claims loest die Erweiterung keinen einzigen neu aus.

BEWUSST GESCHLOSSEN BLEIBT der Verurteilten-Satz. Ihn mit Tatverdaechtigen-
Fakten zu beantworten waere exakt der Fehler aus Befund 6 — eine Verdachts-
zahl als Beleg fuer Schuldsprueche. Solange keine Verurteilten-Statistik im
Projekt liegt, ist `unverifiable` dort die ehrliche Antwort.
"""

import json
import os

import pytest

from services._topic_match import substring_or_composite_match

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

FAKTEN = (
    ("eu_crime.json", "auslander_kriminalitaet_de_2024"),
    ("migration_pack.json", "migration_kriminalitaet_2026"),
)


def _fakt(filename, fid):
    with open(os.path.join(DATA, filename), encoding="utf-8") as fh:
        facts = json.load(fh)["facts"]
    seq = facts if isinstance(facts, list) else list(facts.values())
    for it in seq:
        if it.get("id") == fid:
            return it
    raise AssertionError(f"{fid} nicht in {filename}")


MUSS = [
    # die gemessene Luecke, woertlich
    "Nichtdeutsche sind unter den Tatverdächtigen der deutschen Polizeilichen Kriminalstatistik überrepräsentiert",
    "Der Anteil nichtdeutscher Tatverdächtiger in Deutschland liegt bei über 40 Prozent",
    "Nicht-Deutsche machen 41,8 Prozent der Tatverdächtigen in Deutschland aus",
    "Ausländische Tatverdächtige sind in Deutschland überrepräsentiert",
    "Menschen ohne deutschen Pass sind in der deutschen Kriminalstatistik überrepräsentiert",
    "Tatverdächtige in Deutschland nach Staatsangehörigkeit",
    "Wie hoch ist der Ausländeranteil unter Tatverdächtigen in der BRD",
    # und die bisherige Hauptformulierung muss weiter treffen
    "Migranten begehen in Deutschland überproportional viele Straftaten",
]

NICHT = [
    "Die deutsche Kriminalstatistik zeigt weniger Einbrüche",
    "Nicht deutsche Autos sind in Deutschland beliebter",
    "Die Staatsangehörigkeit und Einbürgerung in Deutschland wurden reformiert",
    "Ausländische Investitionen in Deutschland sind gestiegen",
    "Nichtdeutsche Studierende an deutschen Hochschulen",
    "Die Kriminalstatistik in Deutschland wird jedes Frühjahr veröffentlicht",
    "Ausländische Fachkräfte in der deutschen Pflege",
]


@pytest.mark.parametrize("filename,fid", FAKTEN)
@pytest.mark.parametrize("claim", MUSS)
def test_formulierungen_ueber_die_statistik_treffen(filename, fid, claim):
    assert substring_or_composite_match(_fakt(filename, fid), claim.lower()), (fid, claim)


@pytest.mark.parametrize("filename,fid", FAKTEN)
@pytest.mark.parametrize("claim", NICHT)
def test_alltagssaetze_mit_denselben_woertern_treffen_nicht(filename, fid, claim):
    assert not substring_or_composite_match(_fakt(filename, fid), claim.lower()), (fid, claim)


@pytest.mark.parametrize("filename,fid", FAKTEN)
def test_verurteilten_satz_bekommt_keine_tatverdaechtigen_zahlen(filename, fid):
    """Der Kern von Befund 6: eine Verdachtszahl ist kein Beleg fuer
    Schuldsprueche. Ohne Verurteilten-Statistik ist `unverifiable` richtig."""
    claim = "Nichtdeutsche werden in Deutschland überproportional oft strafrechtlich verurteilt"
    assert not substring_or_composite_match(_fakt(filename, fid), claim.lower())


@pytest.mark.parametrize("filename,fid", FAKTEN)
def test_amtliche_bezeichnung_steht_in_der_personengruppe(filename, fid):
    """„Nichtdeutsche" ist die Kategorie des BKA. Ohne sie verfehlt der Fakt
    genau die Saetze, die die Statistik korrekt zitieren."""
    gruppe = _fakt(filename, fid)["trigger_composite"][0]
    assert "nichtdeutsch" in gruppe and "nicht deutsch" in gruppe, gruppe


@pytest.mark.parametrize("filename,fid", FAKTEN)
def test_gemessene_formulierung_ist_als_zusage_dokumentiert(filename, fid):
    """`claim_phrasings_handled` ist eine Zusage, die
    test_topic_packs_exact_only gegen die Trigger prueft."""
    zusagen = _fakt(filename, fid).get("claim_phrasings_handled") or []
    assert any("nichtdeutsch" in z.lower() for z in zusagen), zusagen
