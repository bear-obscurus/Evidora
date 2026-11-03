"""Die Bestandsgrößen standen im Fakt — und Fragen danach kamen nicht an.

#302 hat die amtliche Verteilung nach Bestandsgröße in den
Massentierhaltungs-Fakt geschrieben (Agrarstrukturerhebung 2023: 859
Halter:innen mit mehr als 800 Schweinen halten 39 % aller Schweine). Die
Prüfung danach: 3 von 5 Fragen nach Bestandsgrößen erreichten den Fakt
nicht, nur den Schwester-Fakt ``viehbestand_at_2026`` mit den reinen
Tierzahlen. Dasselbe Muster wie #301.

## Gemessen, bevor gebaut wurde

Batterie, Erwartung je Zeile vorab festgelegt: **2 von 10** Formulierungen
trafen. Jede Fehlmenge scheiterte an Gruppe 2 der Composite-Regel; Gruppe 1
ist über ``schwein`` immer erfüllt — auch bei jeder Kontrolle. Gruppe 2 ist
deshalb der gefährliche Hebel: ``betriebe`` dort hätte jede
Schweinepest-Meldung („30 Prozent der Betriebe") an den Fakt gezogen.

Was Bestandsgrößen-Fragen auszeichnet, ist eine **Dreierkombination**:
Halter-Wort **und** Schwein **und** Mengen-Hinweis. Die Schweinepest-
Kontrollen haben die ersten beiden, nicht das dritte. Daraus zwei
``trigger_all``-Regeln:

    R1  [betrieb, schweinehalter, bauern] x [schwein] x [Mengen-Hinweis]
    R2  [kleinbäuerlich, kleinstrukturiert, familienbetrieb] x [schwein, tierhaltung]

Ergebnis: **10 von 10**, 0 von 16 Kontrollen (exakt UND über den
Tippfehler-Pass), 0 neue Treffer auf 11.032 Korpus-Claims, 0 auf den
Phrasings aller anderen Fakten, 0 Muss-Treffer verloren, die Batterie aus
#301 unversehrt.

## Drei Dinge hat erst die Messung gezeigt

1. **``tieren`` steckt in „protes-TIEREN"** — und in jedem Verb auf -tieren
   (diskutieren, profitieren, exportieren). „Schweinebauern in
   Großbritannien protestieren" feuerte. Die #293-Klasse. Jetzt gebunden:
   ``" tieren "``; „Betriebe mit 600 Tieren" trifft weiter.
2. **Der Tippfehler-Pass hält „halten" für einen Tippfehler von „halter"**
   (ein Buchstabe Abstand) — die #208-Klasse. Weil ``" halten "``
   gleichzeitig Mengen-Hinweis ist, erfüllte EIN Verb zwei Gruppen. Zwei
   echte Korpus-Claims über Wiener Wohnen und Schweinefleisch wurden so
   gezogen. Jetzt ``schweinehalter`` statt ``halter``.
3. **„die meisten"** zog „Die meisten Schweinebetriebe diskutieren über neue
   Ställe" — vorab als Kontrolle eingetragen. Für die Batterie
   entbehrlich, also gestrichen.

Ungebunden zog ``halten`` zudem „erHALTEN" und „VerHALTEN"; die Bindung
war vorher gemessen, nicht angenommen.

## Grundlage

Der ClaimReview-Korpus enthält null Claims zu diesem Thema; er misst hier
nur Über-Trigger. Die Formulierungen sind von mir (siehe #301).

Keine Netzabfrage in diesen Tests.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._tippfehler import tippfehler_match  # noqa: E402
from services._topic_match import substring_or_composite_match  # noqa: E402

PACK = BACKEND / "data" / "landwirtschaft_pack.json"
ZIEL = "massentierhaltung_empirie_2026"


def _fakt():
    d = json.loads(PACK.read_text(encoding="utf-8"))
    t = [f for f in d["facts"] if (f.get("id") or f.get("topic")) == ZIEL]
    assert len(t) == 1
    return t[0]


def _trifft(claim):
    return substring_or_composite_match(_fakt(), claim.lower())


def _tolerant(claim):
    return tippfehler_match(_fakt(), claim.lower())


SOLL = [
    "In Österreich halten 4 Prozent der Betriebe 40 Prozent der Schweine",
    "Die meisten Schweinehalter in Österreich haben nur ein paar Tiere",
    "Österreichs Schweinehaltung ist kleinbäuerlich strukturiert",
    "Wenige Großbetriebe halten die meisten Schweine in Österreich",
    "Großbetriebe mit mehr als 800 Schweinen halten fast 40 Prozent aller Schweine",
    "Die Hälfte der österreichischen Schweine lebt in Betrieben mit mehr als 600 Tieren",
    "In Österreich gibt es kaum große Schweinemastbetriebe",
    "Die meisten Schweine in Österreich stehen in kleinen Familienbetrieben",
    "Österreichische Schweinebauern halten im Schnitt nur wenige Tiere",
    "Ein Großteil der Schweine in Österreich wird in Betrieben mit über 400 Tieren gehalten",
]

#: Halter-Wort und Schwein, aber keine Frage nach Bestandsgrößen — und die
#: Fälle, die je einen gemessenen Fehltreffer ausgelöst haben.
NICHT = [
    "Die Schweinepest zwingt viele Betriebe zum Aufgeben",
    "Schweinehalter bekommen 2024 mehr Förderung",
    "Viele Betriebe in Österreich halten an alten Heizungen fest",
    "Hundehalter müssen in Wien eine Abgabe zahlen",
    "Die kleinbäuerliche Landwirtschaft in Afrika ernährt die Welt",
    "Schweine halten die Sommerhitze kaum aus",
    "Ein Großteil der Betriebe in Österreich sind Familienbetriebe",
    "Wegen der Schweinepest mussten Betriebe 10.000 Tiere keulen",
    # gemessen: protesTIEREN
    "Schweinebauern in Großbritannien protestieren gegen niedrige Preise",
    "Bauern protestieren gegen niedrige Preise für Schweine",
    # gemessen: erHALTEN, VerHALTEN
    "Schweinehalter erhalten Hilfe nach dem Hochwasser",
    "Das Verhalten von Schweinen in Betrieben wird kaum erforscht",
    # gemessen: "die meisten" + diskuTIEREN
    "Die meisten Schweinebetriebe diskutieren über neue Ställe",
    # gemessen: Tippfehler-Pass "halten" ~ "halter"
    "Aus Rücksicht auf andere Mieter soll man bei der Zubereitung von "
    "Schweinefleisch die Fenster geschlossen halten",
]


@pytest.mark.parametrize("claim", SOLL)
def test_bestandsgroessen_frage_erreicht_den_fakt(claim):
    assert _trifft(claim), claim


@pytest.mark.parametrize("claim", NICHT)
def test_kontrolle_erreicht_den_fakt_nicht(claim):
    assert not _trifft(claim), claim


@pytest.mark.parametrize("claim", NICHT)
def test_kontrolle_auch_nicht_ueber_den_tippfehler_pass(claim):
    assert not _tolerant(claim), claim


# --------------------------------------------------------------------------
# Die drei gemessenen Fallen, je einzeln gepinnt
# --------------------------------------------------------------------------

def _r1():
    regeln = [r for r in _fakt()["trigger_all"] if "schweinehalter" in r[0]]
    assert len(regeln) == 1
    return regeln[0]


def test_tieren_ist_gebunden():
    menge = _r1()[2]
    assert " tieren " in menge and "tieren" not in menge


def test_halten_ist_gebunden():
    menge = _r1()[2]
    for form in (" halten ", " hält ", " gehalten "):
        assert form in menge, form
    assert "halten" not in menge


def test_schweinehalter_statt_halter():
    """Der Tippfehler-Pass liest "halten" als Tippfehler von "halter"."""
    halter = _r1()[0]
    assert "schweinehalter" in halter and "halter" not in halter


def test_die_meisten_ist_kein_mengen_hinweis():
    assert "die meisten" not in _r1()[2]


def test_betriebe_steht_nicht_in_gruppe_zwei():
    """Sonst zieht jede Schweinepest-Meldung den Fakt."""
    g2 = _fakt()["trigger_composite"][1]
    assert not any("betrieb" in t and t.strip() not in ("großbetrieb", "grossbetrieb", "kleinbetrieb") for t in g2)


def test_die_struktur_regel_braucht_ein_tierwort():
    r2 = [r for r in _fakt()["trigger_all"] if "kleinbäuerlich" in r[0]][0]
    nur = {"trigger_all": [r2]}
    assert substring_or_composite_match(nur, "die schweinehaltung ist kleinbäuerlich")
    assert not substring_or_composite_match(nur, "die landwirtschaft ist kleinbäuerlich")


# --------------------------------------------------------------------------
# Korpus — nur wo er liegt (gitignoriert)
# --------------------------------------------------------------------------

_INDEX = BACKEND / "data" / "claimreview_index.json"


@pytest.mark.skipif(not _INDEX.exists(), reason="claimreview_index.json gitignoriert")
def test_die_wiener_wohnen_claims_bleiben_draussen():
    """Die zwei echten Korpus-Claims, die der Tippfehler-Pass zog."""
    claims = [r["claim"] for r in json.loads(_INDEX.read_text(encoding="utf-8"))
              if r.get("claim") and "Wiener Wohnen" in r["claim"]
              and "Schweinefleisch" in r["claim"]]
    assert claims, "die Anlass-Claims fehlen im Korpus — Test pruefen"
    for c in claims:
        assert not _tolerant(c) and not _trifft(c), c[:80]
