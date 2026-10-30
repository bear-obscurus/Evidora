"""Der Superlativ wanderte auf die laufende Saison — und „besonders stark"
war keine vage Angabe.

Nachgemessen am 26.10.2026, dreimal **zeichengleich** (also keine Varianz):

    Claim:   "Die Grippewelle in Deutschland ist dieses Jahr besonders stark"
    erwartet: unverifiable (akzeptabel mixed)
    live:     mostly_false @ 0.85
    Summary: "Die aktuelle Grippewelle 2026/27 … liegt mit 6.854 … unter
              dem Vorjahreswert (7.584) und ist die schwächste der letzten
              vier Saisonen (Höchstwert 2025/26: 8.661 …)"

⚠️ **Meine Notiz zu diesem Befund war in beiden Hälften falsch.** Sie
sagte „Label richtig, aber die Begründung vergleicht gegen die
Saison-Höchstwerte".

## Erstens: das Label ist nicht richtig

`hat_vage_mengenangabe` kannte **Mengen**wörter („kaum", „viele") und keine
**Intensitäts**urteile. „besonders stark" hat so wenig eine Schwelle wie
„viele" — der 0,7-Deckel griff nicht, und eine Auslegungsfrage ging mit
0,85 hinaus.

Gemessen über 12.155 echte Claims, zwei Fassungen:

    bloß "besonders|sehr|extrem"   45 Claims neu — darunter Unsinn
                                   ("wir setzen uns SEHR für behinderte
                                   Menschen ein", "verhielt sich SEHR
                                   ungebührlich"). VERWORFEN.
    gebunden an ein Adjektiv       10 Claims neu, 9 davon gewollt

Die neun: „Atomkraft hat eine sehr niedrige CO2-Bilanz", „Napoleon war
besonders klein", „Polizeigewalt ist nur sehr selten", „Winter in der
Antarktis extrem kalt", „diese Art ist sehr selten", „treten sehr häufig
Thrombosen auf", „Corona-Tote in Schweden sehr niedrig", „PCR-Tests nur
eine sehr geringe Fehlerquote", „Betreuungsquote sehr niedrig". Der zehnte
ist ein Grenzfall (ein Foto bei „besonders niedrigen Temperaturen") und
kostet 0,15 Konfidenz, sonst nichts — der Deckel rührt das Verdict nie an.

## Zweitens: es war kein verbotener Vergleich, sondern eine Zuordnung

Der `display_value` trägt die Sperre aus #273/#275 — *„NICHT die
Saison-Höchstwerte dafür vergleichen"* — und das Modell hat sie **befolgt**:
Es verglich KW 41 gegen KW 41 (6.854 gegen 7.584).

Der Fehler steckt woanders. In den Daten steht der Superlativ über die
**abgeschlossene** Saison:

    "Die Saison 2025/26 war … die SCHWÄCHSTE der letzten vier Saisonen"

In der Antwort hängt er an „Die aktuelle Grippewelle **2026/27**" — einer
Saison mit zwei Meldewochen. Das ist dieselbe Klasse wie die Meloni-Drift
(#122) und „Zuordnung reicht nicht — Umdeutung VERBIETEN": eine wahre
Aussage über A, serviert als Aussage über B.

⚠️ Und ein eigener Messfehler: Mein erster Drift-Detektor meldete für alle
drei Läufe „nein", weil er „schwächste" innerhalb von 120 Zeichen nach
„2026/27" verlangte — der Abstand ist größer. Der Satz sagt es
unmissverständlich. Ein Detektor, der seinen Anlass nicht findet, taugt
nichts (#229); hier zählt das Lesen.

## Wo die Sperre NICHT hinkonnte

Erster Versuch: an den `indicator_name` angehängt. Gemessen: die
``headline`` ist **399 von 400 Zeichen** (`PROMPT_MAX_STR`) — der Zusatz
wird abgeschnitten, und es träfe genau ihn. Der Fehlermodus aus #131.

Sie steht jetzt im `display_value`, direkt nach dem Jahresvergleich: dort
waren 404 Zeichen frei, sie liegt innerhalb der ersten 400 (überlebt also
auch bei Rang ≥ 1), und dieses Feld hat das Modell nachweislich gelesen —
die Nachbar-Sperre wurde befolgt.

Die Zahl der Meldewochen kommt AUS DEN DATEN. Ein „zwei Meldewochen" im
Text hätte den nächsten Refresh stumm überlebt (#74/#133).

## Die Kontrollen

    #79 "schwächer als letztes Jahr"        true @ 0.85, korrekt
        (KW 41 gegen KW 41, Höchstwerte nur als Zusatz)
    #80 "KW 41 2026 ARE-Rate 6.854"         true @ 0.85, kein Höchstwert

Beide bleiben ungedeckelt — sie nennen keine vage Angabe.

Dependency-light: Postprocess und RKI-Dienst, kein Netz.
"""
import asyncio
import copy
import json
import os
import re
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services.rki_surveillance import (  # noqa: E402
    search_rki_surveillance,
)
from services.synthesizer import PROMPT_MAX_STR  # noqa: E402
from services.verdict_postprocess import (  # noqa: E402
    VAGE_MENGE_CONFIDENCE_CAP,
    apply_vage_menge_cap,
    hat_vage_mengenangabe,
)

#: Die neun gewollten Treffer aus der Korpus-Messung, woertlich.
GEWOLLT = [
    "Die Grippewelle in Deutschland ist dieses Jahr besonders stark",
    "Atomkraft hat eine sehr niedrige CO2-Bilanz.",
    "Napoleon war besonders klein.",
    "Polizeigewalt ist nur sehr selten bei uns in Deutschland und in Bayern.",
    "Der Winter in der Antarktis war dieses Jahr extrem kalt",
    "Der Bildunterschrift zufolge ist diese Art sehr selten.",
    "In Zusammenhang mit Corona-Impfungen treten sehr häufig Thrombosen auf.",
    "Ist die Zahl der Corona-Toten in Schweden seit dem Sommer sehr niedrig?",
    "Selbst wenn PCR-Tests nur eine sehr geringe Fehlerquote haben",
]

#: Die VERWORFENE Fassung haette diese gedeckelt — sie duerfen es nicht sein.
NICHT_GEWOLLT = [
    "Die bayerische AfD und wir als AfD-Fraktion setzen uns sehr für "
    "behinderte Menschen ein",
    "Die Frau, die das Auto fuhr, verhielt sich sehr ungebührlich",
    "Die Regierung hat besonders betont, dass die Lage ernst ist",
    "Der Minister hat sich sehr geäußert",
]


@pytest.mark.parametrize("claim", GEWOLLT)
def test_intensitaets_urteile_sind_vage(claim):
    assert hat_vage_mengenangabe(claim), claim


@pytest.mark.parametrize("claim", NICHT_GEWOLLT)
def test_blosse_verstaerker_sind_keine_vage_angabe(claim):
    """Giftprobe gegen die verworfene Fassung (45 statt 10 Treffer)."""
    assert not hat_vage_mengenangabe(claim), claim


@pytest.mark.parametrize("claim", [
    # Eine Schwelle macht die Aussage pruefbar — der Deckel schweigt.
    "Die ARE-Rate lag besonders stark über 8.000 je 100.000",
    "Besonders hoch: mehr als 50 Prozent der Befragten",
    # Vergleiche gehoeren Muster O/P, nicht dem Deckel.
    "Die Grippewelle ist dieses Jahr schwächer als letztes Jahr",
    # Eine datierte Einzelzahl ist nichts Vages.
    "In der Kalenderwoche 41 2026 lag die ARE-Rate bei 6.854",
])
def test_pruefbare_claims_bleiben_ungedeckelt(claim):
    assert not hat_vage_mengenangabe(claim), claim


def test_der_deckel_aendert_das_verdict_nicht():
    """Vertrag des Deckels: nur Konfidenz und Hinweis."""
    res = {"verdict": "mostly_false", "confidence": 0.85,
           "summary": "…", "nuance": "Der Trend zeigt einen Rückgang."}
    out = apply_vage_menge_cap(
        dict(res), "Die Grippewelle in Deutschland ist dieses Jahr "
                   "besonders stark")
    assert out["verdict"] == "mostly_false"
    assert out["confidence"] == VAGE_MENGE_CONFIDENCE_CAP
    assert "ohne feste Schwelle" in out["nuance"]


def test_der_hinweis_nennt_auch_ein_intensitaets_beispiel():
    """Er nannte nur „kaum"/„viele" — das traf den neuen Fall nicht."""
    res = {"verdict": "true", "confidence": 0.9, "summary": "…"}
    out = apply_vage_menge_cap(dict(res), "Napoleon war besonders klein")
    assert "besonders stark" in out["nuance"]


# ------------------------------------------------- die Zuordnungs-Sperre

def _zeile() -> dict:
    r = asyncio.run(search_rki_surveillance(
        {"claim": "Die Grippewelle in Deutschland ist dieses Jahr "
                  "besonders stark"}))
    zeilen = r.get("results") or []
    assert zeilen, "RKI lieferte nichts"
    return zeilen[0]


def test_der_superlativ_traegt_seinen_geltungsbereich():
    """Der Fix sitzt dort, wo der Superlativ steht: in der headline.

    Sie ist der `indicator_name` und wird NIE gekuerzt (395 von 400). Der
    Satz nennt jetzt "die SCHWÄCHSTE der vier ABGESCHLOSSENEN Saisonen" —
    damit ist er nicht mehr auf eine laufende Saison uebertragbar.
    """
    name = _zeile()["indicator_name"]
    assert "ABGESCHLOSSENEN Saisonen" in name, name[:220]
    assert "der letzten vier Saisonen" not in name


def test_die_headline_hat_wieder_luft():
    """395 statt 399 — ein Feld am Anschlag ist selbst ein Risiko.

    Zwei Versuche, die Sperre woanders unterzubringen, sind am Budget
    gescheitert, und BEIDE hat ein bestehendes Gate gefangen:
      * ein eigener Satz im display_value (255 Zeichen) -> "KW 41/2026"
        fiel aus den ersten 400 heraus
      * eine Klausel von 102 Zeichen -> "KEIN Hoechstwert" fiel heraus
    Die ersten 400 Zeichen des display_value tragen laut #275 bereits
    vier Pflichten. Deshalb loest die headline das Problem selbst.
    """
    name = _zeile()["indicator_name"]
    assert len(name) <= PROMPT_MAX_STR - 3, len(name)


def test_die_laufende_saison_bleibt_als_gegengewicht():
    """Die headline nennt die abgeschlossene Saison, der display_value die
    laufende — beide muessen da sein, sonst traegt der Geltungsbereich
    nichts."""
    zeile = _zeile()
    assert "2025/26" in zeile["indicator_name"]
    kopf = zeile["display_value"][:PROMPT_MAX_STR]
    assert "LAUFENDE SAISON 2026/27" in kopf
    assert "2 Meldewochen sind kein Saisonverlauf" in kopf


def test_die_alte_sperre_steht_weiter_da():
    """#273/#275 duerfen nicht verdraengt werden — das Modell hat sie
    befolgt."""
    dv = _zeile()["display_value"]
    assert "NICHT die Saison-Höchstwerte dafür vergleichen" in dv
    assert "DIESES JAHR GEGEN LETZTES JAHR" in dv
    # Der Jahresvergleich bleibt VORNE: er ist die direkte Antwort (#275).
    assert dv.index("DIESES JAHR") < dv.index("LAUFENDE SAISON")


# ---------------------------------------------------------------------------
# "wie viel(e)" ist ein Interrogativ, keine Mengenangabe (30.10.2026)
#
# Gefunden beim Nachmessen eines ANDEREN offenen Punktes: Die
# WCAG-Zaehlfrage war langst gefixt, aber sie bekam den Deckel aus dieser
# Datei. Live gemessen:
#
#     "Wie viele Erfolgskriterien hat WCAG 2.2?"   true @ 0,70
#     "WCAG 2.2 hat 86 Erfolgskriterien"           true @ 0,80
#
# Beide Antworten nannten dieselbe richtige Zahl (86 aktive, 87
# gelistet, SC 4.1.1 obsolet). Die FRAGE bekam trotzdem den Deckel und
# dazu den Hinweis "nutzt eine Angabe ohne feste Schwelle (etwa 'kaum',
# 'viele')" — eine Auskunft, die nicht stimmt. Die Form der Eingabe ist
# kein Grund fuer einen Deckel (#232).
#
# Gemessen ueber 11.032 echte Claims: 251 -> 245 gedeckelt. Die sechs
# sind alle Zaehlfragen.

@pytest.mark.parametrize("claim", [
    "Wie viele Erfolgskriterien hat WCAG 2.2?",
    "Wie viele Einwohner hat Österreich?",
    "Wie viel kostet ein Faktencheck?",
    "Wie viele Femizide gab es 2024?",
    # Woertlich aus dem Korpus, die beiden klarsten der sechs:
    "Wie viele Menschen sind im Pandemie-Jahr 2020 gestorben?",
    "Wie viel Kosten entstehen in Deutschland durch Flüchtlinge?",
    # Auch mitten im Satz, nicht nur am Anfang:
    "Niemand weiß, wie viele es wirklich sind",
])
def test_zaehlfragen_werden_nicht_gedeckelt(claim):
    assert not hat_vage_mengenangabe(claim), claim


@pytest.mark.parametrize("claim", [
    # ⚠️ DER Fall, der eine claim-weite Ausnahme verboten haette: hier
    # stehen "so viele" UND "wie" im selben Satz. Geprueft wird deshalb
    # die linke Nachbarschaft JEDES Treffers, nicht der ganze Claim.
    "So viele Menschen wie nie zuvor sind gekommen",
    "Es gibt viele Femizide in Österreich",
    "Kaum jemand nutzt das KlimaTicket",
    "Über die östliche Landgrenze kommen kaum noch Menschen",
    "Zahlreiche Studien belegen das",
])
def test_echte_mengenangaben_bleiben_gedeckelt(claim):
    assert hat_vage_mengenangabe(claim), claim


def test_die_alte_zusammenschreibung_war_nie_betroffen():
    """„wieviele" trifft _VAGE_MENGE gar nicht — die linke Wortgrenze
    verhindert es schon. Festgehalten, damit niemand den Interrogativ-Fall
    fuer sie nachbaut und dabei die Wortgrenze aufweicht."""
    assert not hat_vage_mengenangabe("Wieviele Menschen starben 2020?")


def test_der_deckel_aus_284_bleibt_unberuehrt():
    """Die Intensitaets-Haelfte wird vor der Mengen-Pruefung entschieden
    — eine Zaehlfrage darf sie nicht aushebeln."""
    assert hat_vage_mengenangabe(
        "Wie viele Menschen sind besonders stark betroffen?")
