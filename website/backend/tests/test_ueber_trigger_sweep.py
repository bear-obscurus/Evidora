"""Über-Trigger-Sweep QA50D: vier reale Cross-Topic-Lecks (2026-08-17).

Der Sweep begann mit einer Korrektur der eigenen Messung: die im
QA50D-Report genannten Quoten (Landwirtschaft-Konsens 30 %, DESTATIS 38 %
…) waren zu einem großen Teil ein **Artefakt des Cache-Bust-Suffixes**
`(Pruefsatz-NNN)` — darin steckt `"efsa"` (Pr-UEFSA-tz), ein legitimer
Trigger des Landwirtschafts-Packs. Ohne Suffix liegen alle statischen
Packs im einstelligen Bereich.

Übrig blieben vier ECHTE Lecks, alle nach demselben Muster: ein kurzes
Token versteckt sich in einem Alltagswort, und der AND-Partner ist
ebenfalls generisch.

  EIGE     ["eige"] AND ["eu", …]   -> "eige" ⊂ EIGEntlich, "eu" ⊂ LEUte
  ETER     keyword "eter"           -> "eter" ⊂ KilomETER
  NEUTRAL  ["österreich"] AND ["abgeschafft"] — beide generisch
  MEDIEN   ["anzeigen"] AND ["österreich"] — Straf-Anzeigen ≠ Inserate

Jeder Fix ist hier beidseitig gepinnt: das Leck muss zu bleiben UND die
eigenen Themen müssen weiter treffen. Ein Trigger-Fix ohne
Muss-Treffer-Kontrolle schaltet den Fakt sonst still ab.

Dependency-light: reine Trigger-Tests, kein Netz/LLM.
"""
import json
import os

import pytest

from services._topic_match import substring_or_composite_match

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "data")


def _fact(filename, topic):
    with open(os.path.join(DATA, filename), encoding="utf-8") as fh:
        d = json.load(fh)
    for it in d.get("facts") or []:
        if (it.get("topic") or it.get("id")) == topic:
            return it
    raise AssertionError(f"{topic} nicht in {filename}")


CASES = [
    # (Datei, Topic, Claim, soll_treffen)
    # --- EIGE Gender Equality Index ---
    ("gleichstellung_pack.json", "gender_equality_index_konsens",
     "Sind wir in Österreich eigentlich schon über 9 Millionen Leute?", False),
    ("gleichstellung_pack.json", "gender_equality_index_konsens",
     "Gibt es in Österreich eigentlich noch die Wehrpflicht?", False),
    ("gleichstellung_pack.json", "gender_equality_index_konsens",
     "Österreich ist gemeinsam mit Schweden und Finnland 1995 der EU beigetreten.",
     False),
    ("gleichstellung_pack.json", "gender_equality_index_konsens",
     "Österreich ist beim Gleichstellung-Index Spitze", True),
    ("gleichstellung_pack.json", "gender_equality_index_konsens",
     "Der EIGE Gender Equality Index zeigt fast Gleichstellung in Europa", True),
    # --- ETER Hochschulregister ---
    ("eter.json", "overview_eu",
     "In Österreich gibt es mehr als 2.000 Kilometer Autobahnen und Schnellstraßen.",
     False),
    ("eter.json", "overview_eu",
     "Wie viele Hochschulen gibt es in Europa laut ETER-Register?", True),
    ("eter.json", "overview_eu",
     "Die europäischen Universitäten im Bologna-Prozess", True),
    ("eter.json", "overview_eu",
     "Wie viele Studierende gibt es in der EU?", True),
    # --- AT-Neutralität ---
    ("sicherheitspolitik_pack.json", "at_neutralitaet_recht_konsens",
     "Die Studiengebühren in Österreich sind abgeschafft", False),
    ("sicherheitspolitik_pack.json", "at_neutralitaet_recht_konsens",
     "Österreich ist neutral, deswegen darf es gar keine Soldaten ins Ausland schicken.",
     True),
    ("sicherheitspolitik_pack.json", "at_neutralitaet_recht_konsens",
     "Das Neutralitätsgesetz 1955 verbietet einen NATO-Beitritt", True),
    ("sicherheitspolitik_pack.json", "at_neutralitaet_recht_konsens",
     "Österreich darf keinen Bündnissen beitreten", True),
    # --- MedienTransparenz ---
    ("medientransparenz.json", "medientransparenz_overview",
     "Die Anzeigen wegen Cybercrime sind in Österreich stark gestiegen.", False),
    ("medientransparenz.json", "medientransparenz_overview",
     "Werbung für Glücksspiel ist in Österreich verboten", False),
    ("medientransparenz.json", "medientransparenz_overview",
     "Die Krone bekommt am meisten Inserate von der öffentlichen Hand.", True),
    ("medientransparenz.json", "medientransparenz_overview",
     "In der Inseratenaffäre Kurz wurden die Beinschab-Studien gefälscht", True),
]


@pytest.mark.parametrize("filename,topic,claim,soll", CASES)
def test_trigger_leck_geschlossen(filename, topic, claim, soll):
    got = substring_or_composite_match(_fact(filename, topic), claim.lower())
    assert got is soll, f"{topic}: match={got}, erwartet {soll} für {claim!r}"


# --- Contract: die konkreten Kurz-Tokens bleiben draussen ---

@pytest.mark.parametrize("filename,topic,verboten", [
    ("gleichstellung_pack.json", "gender_equality_index_konsens", ("eige", "eu")),
    ("eter.json", "overview_eu", ("eter", "eu")),
])
def test_kurz_tokens_nicht_in_schwachen_composites(filename, topic, verboten):
    """Diese Tokens verstecken sich in Alltagswörtern (EIGEntlich, LEUte,
    KilomETER).

    Die Gefahr entsteht NICHT durch das Token allein, sondern durch die
    Kombination mit einem schwachen AND-Partner: `["eige"] AND ["eu", …]`
    trifft jeden Satz mit „eigentlich" und „Leute". In einer Regel mit
    drei oder mehr AND-Gruppen (z. B. eige + index + zeigt +
    gleichstellung) ist dasselbe Token unkritisch.

    Deshalb pinnt dieser Test die tatsächliche Sicherheits-Eigenschaft:
    blanke Kurz-Tokens sind in `trigger_keywords` (reiner Substring-Match)
    und in Composites mit weniger als 3 Gruppen verboten. Gebundene
    Varianten wie 'eu-' oder ' eter ' bleiben überall erlaubt.
    """
    it = _fact(filename, topic)
    schwach = list(it.get("trigger_keywords") or [])
    comp = it.get("trigger_composite") or []
    if 0 < len(comp) < 3:
        for grp in comp:
            schwach += list(grp)
    for rule in it.get("trigger_all") or []:
        if 0 < len(rule) < 3:
            for grp in rule:
                schwach += list(grp)
    for tok in verboten:
        assert tok not in schwach, (
            f"{topic}: blankes Token {tok!r} steht in einem schwachen "
            f"Trigger (Keyword oder Composite mit <3 AND-Gruppen) — genau "
            f"diese Konstellation erzeugte die Cross-Topic-Treffer")


def test_suffix_artefakt_dokumentiert():
    """Der Cache-Bust-Suffix `(Pruefsatz-NNN)` enthält 'efsa'. Dieser Test
    hält die Erkenntnis fest, damit künftige Messungen den Suffix nicht
    erneut für ein Produktionsproblem halten."""
    assert "efsa" in "pruefsatz"
