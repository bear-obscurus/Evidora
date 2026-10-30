"""Die Superlativ-Erkennung war eine geschlossene Liste aus sechs Woertern.

Aufgefallen in #287: Der Zaehlweisen-Guard sah Fluss-Claims nie, weil
``_SUPERLATIVE_PHRASE`` nur hoechste/niedrigste/geringste/groesste/
meiste/staerkste kennt — „laengste" fehlte. Das ist die
#140/#141-Falle: Formen aufzaehlen scheitert.

## Was gemessen wurde, bevor etwas gebaut wurde

**11.032 echte Claims** (ClaimReview-Index):

    enge Sechs-Wort-Liste            34 Claims
    Orthografie-Fassung              61 Claims   (+27)

⚠️ **Die naive Fassung ``[a-z]+ste[nrms]?`` wurde gemessen und
VERWORFEN.** Sie zog 69 „Adjektive" an, groesstenteils Substantive auf
-ste/-sten:

    24x ersten        6x immunsystem     5x polizisten    5x bauernproteste
     4x kosten        4x proteste        3x liste         2x briefkaesten
     2x fenster       1x kueste          1x alster        1x westen

Dieselbe Klasse wie „dina" in KarDINAele (#283). Was sie trennt, ist die
deutsche GROSS-/Kleinschreibung: Das Substantiv ist gross, das Adjektiv
klein (#278). Deshalb laeuft die Regex auf dem ORIGINALTEXT. Ergebnis:
27 neue Claims, **null** Substantiv-Fehltreffer.

⚠️ **Mein erster Messlauf hat den Claim vorher kleingeschrieben** und
damit genau das Signal zerstoert, das ich suchte (#227). Und im Dienst
ist das keine Nachlaessigkeit, sondern Architektur: Zeile 1963/1964 in
``apply_verdict_postprocessing`` schreibt Claim und Summary EINMAL oben
klein, alle Abhaengigen sehen nur das. Deshalb wird das Original jetzt
durchgereicht.

## Vier Abhaengige, zwei Richtungen

``_SUPERLATIVE_PHRASE`` speist:

1. ``_claim_ist_vergleichsfrage`` -> Zaehlweisen-Guard. Wirkt auf dem
   CLAIM und WERTET AB — die riskante Richtung.
2. ``_summary_refutes_superlative`` und
3. ``_superlative_attributed_elsewhere``. Beide BREMSEN den
   Confirm-Flip von Pattern A/E. Mehr Feuern ist dort die sichere
   Richtung; der Code sagt das selbst („Fehlerkosten sind asymmetrisch").
4. ``_SUPERLATIVE_ADJECTIVES`` fuer die Verneinungs-Zweige.
   **Gemessen und NICHT angefasst:** Im ganzen Korpus gibt es genau
   EINEN verneinten Superlativ, und sein Wort steht in der Liste. Keine
   Luecke, kein Umbau.

## Was die Aenderung bewirkt — und was nicht

    2.823 Projekt-Formulierungen   93 -> 105 erkannt (+12)
    98 echte QA50I-Summaries       12 neu sichtbar fuer die Bremsen,
                                   0 Verdict-Aenderungen (kein Ueber-Trigger)
    Ueber-Trigger-Gate             0 fremde Abwertungen (test_zaehlweise)

Die +12 sind echte Rang-Behauptungen: „wichtigste COPD-Therapie",
„kleinste Staat der Welt", „giftigsten Tiere", „friedlichste Religion",
„beste Eisenquelle".

⚠️ **Der Anlass aus #287 wird damit NICHT geloest.** Fluss-Claims
erreichen den Zaehlweisen-Guard jetzt — aber die Wikidata-Fluss-Zeilen
deklarieren ueberhaupt keine Zaehlweisen, also greift er dort weiterhin
nicht. „Der richtige Fix loest nicht immer den gemessenen Fall"
(#231/#232).

Auf dem Korpus ist der Effekt der Claim-Seite **0**: Keiner der 27 neuen
Claims trifft auf einen der zwei Fakten, die Zaehlweisen deklarieren.
Live nachweisbar ist er an natuerlich formulierten Behauptungen, die der
Guard heute uebersieht — gegen prod gemessen:

    "Frauen sind die häufigsten Opfer von Tötungsdelikten in
     Deutschland"                                        false @ 0,85
    "Der Partner ist der häufigste Täter bei Morden an
     Frauen in Österreich"                               true  @ 0,85

Beide ruhen auf dem Femizid-Fakt (dieselbe Beleg-URL), beide nennen
keine Zaehlweise — dasselbe Spiegelpaar-Muster, das den Guard 2026
ueberhaupt veranlasst hat. Der erste Fall ist klar
zaehlweisen-abhaengig (die Summary nennt 43,4 % bei vollendeten Taten
gegen 84,7 % im Partnerschaftskontext). ⚠️ Beim zweiten ist die
Abwertung eher vorsichtig als notwendig: Die Rangfolge haelt unter allen
drei Zaehlweisen. Der Guard kann das nicht unterscheiden — das ist seine
dokumentierte Eigenschaft („ohne Zaehlweise keine Seite"), nicht neu.

Dependency-light: Postprocess + Datenpack, kein Netz.
"""
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
DATA = os.path.join(BACKEND, "data")

from services._struct_marker import render_data_with_marker  # noqa: E402
from services._zaehlweise import deklarierte_namen  # noqa: E402
from services.verdict_postprocess import (  # noqa: E402
    _SUPERLATIVE_ADJECTIVES,
    _ZW_KONFIDENZ_DECKEL,
    _claim_ist_vergleichsfrage,
    _claim_negates_superlative,
    apply_verdict_postprocessing,
    nennt_superlativ,
)

#: Wortwoertlich aus dem Korpus — Adjektive, die die Sechs-Wort-Liste
#: NICHT kennt. 26 verschiedene in 27 Claims.
ECHTE_SUPERLATIVE = [
    ("Der April 2023 ist der kälteste seit über 90 Jahren.", "kälteste"),
    ("Deutschland liegt nur auf Platz 188 der wärmsten Länder der Welt.",
     "wärmsten"),
    ("Der Kollege Krah ist einer der faulsten Abgeordneten im Parlament.",
     "faulsten"),
    ("Steve Jobs gehörte zu den reichsten Menschen der Welt.", "reichsten"),
    ("Im Alter von 109 Jahren gehört er zu den ältesten Deutschen.",
     "ältesten"),
    ("Schweden zählt zu den wohlhabendsten Ländern auf dem Globus.",
     "wohlhabendsten"),
    ("Drosten ist einer der bekanntesten Mediziner Deutschlands geworden.",
     "bekanntesten"),
    ("Öl ist nach Wasser die zweithäufigste Flüssigkeit.", "zweithäufigste"),
    ("Die zwei letzten Rentenerhöhungen waren die allerhöchsten.",
     "allerhöchsten"),
    ("Österreich hat die strengsten Maßnahmen.", "strengsten"),
    ("Die Donau ist der längste Fluss Europas.", "längste"),
    ("Frauen sind die häufigsten Opfer von Tötungsdelikten.", "häufigsten"),
]


@pytest.mark.parametrize("claim,wort", ECHTE_SUPERLATIVE)
def test_echte_superlative_werden_erkannt(claim, wort):
    assert nennt_superlativ(claim) == wort, claim


#: Die Giftprobe gegen die VERWORFENE naive Fassung — alles Substantive
#: auf -ste/-sten, wortwoertlich aus dem Korpus.
SUBSTANTIVE = [
    "Der DStGB beziffert die Kosten für Zahnarztbesuche von Asylbewerbern.",
    "Demonstrationen gegen die derzeit geltenden Corona-Maßnahmen: die "
    "Polizisten griffen ein.",
    "Aus dem Bericht geht hervor, dass die Impfstoffe das Immunsystem "
    "schädigen.",
    "Traktorkolonnen sind bei den Bauernprotesten in Deutschland unterwegs.",
    "In Weilheim stecken Umschläge in den Briefkästen.",
    "Die Liste stammt von Marx und ist im Britischen Museum zu finden.",
    "Aus Rücksicht auf andere Mieter sollen die Fenster geschlossen bleiben.",
    "Vom Getreideabkommen hat vor allem der Westen profitiert.",
    "Weil das Foto manipuliert wurde, stehen Menschen in der Alster.",
    "Die IDF hätten die Zivilisten gewarnt.",
    "Menschen in Syrien fliehen vor den Islamisten.",
    "Die US-Krankenschwester wird in sozialen Medien gezeigt.",
    "In Deutschland wird mehr Geld für das Gesundheitssystem gebraucht.",
]


@pytest.mark.parametrize("claim", SUBSTANTIVE)
def test_substantive_auf_ste_sind_keine_superlative(claim):
    """Die naive Fassung zog 69 solcher Woerter an — dieselbe Klasse wie
    „dina" in KarDINAele (#283). Die Orthografie trennt sie."""
    assert nennt_superlativ(claim) == "", claim


@pytest.mark.parametrize("claim", [
    "Der erste Covid-Patient in Deutschland lag in München.",
    "Die Erneuerbaren haben den ersten größeren Stromausfall verursacht.",
    "Mehr CO2 als Deutschland in den nächsten 100 Jahren.",
    "Die zwei letzten Rentenerhöhungen lagen darüber.",
    "Die aktuellste Studie zeigt keinen Effekt.",
    "Der jüngste Mpox-Ausbruch wurde durch die Impfung ausgelöst.",
    "Nach den neuesten Zahlen sind es mehr.",
])
def test_reihenfolge_und_aktualitaet_sind_kein_rang(claim):
    """Superlativ GEFORMT, aber kein Rang auf einer messbaren Groesse.
    Im Korpus die haeufigste Gruppe — „ersten" allein 24x."""
    assert nennt_superlativ(claim) == "", claim


def test_juengste_und_aelteste_liegen_auf_verschiedenen_seiten():
    """Nicht geraten, sondern gemessen: Im Korpus war jeder
    „juengste"-Treffer zeitlich und jeder „aelteste" ein Rang."""
    assert nennt_superlativ("Der jüngste Ausbruch begann im Mai.") == ""
    assert nennt_superlativ("Er gehört zu den ältesten Deutschen.") \
        == "ältesten"


@pytest.mark.parametrize("claim", [
    "frauen werden meistens von ihren partnern getötet",
    "die zahlen werden größtenteils bestätigt",
])
def test_die_linke_wortgrenze_ist_pflicht(claim):
    """Ohne sie kommt das „den" aus „werden" — der Fehler aus dem
    Phrasings-Sweep in test_zaehlweise."""
    assert nennt_superlativ(claim) == "", claim


def test_grossgeschriebene_eingaben_haben_KEINEN_rueckfall():
    """⚠️ Zuerst war ein All-Caps-Rueckfall drin, wie in #278 — dieser
    Test hat ihn erledigt: Ohne Schreibung ist „DIE KOSTEN" ein
    Superlativ, der Rueckfall holt also genau die Substantive zurueck,
    wegen derer die Funktion existiert.

    Er ist auch unnoetig. Die 21 durchgehend grossgeschriebenen Claims
    im Korpus verlieren nichts: Die sechs alten Woerter werden weiter
    auf der kleingeschriebenen Fassung geprueft.
    """
    assert nennt_superlativ("DIE HÖCHSTE RATE IN GANZ EUROPA") == ""
    assert nennt_superlativ("DIE KOSTEN SIND ZU HOCH") == ""
    # Kein Verlust gegenueber heute: der alte Pfad traegt sie.
    assert _claim_ist_vergleichsfrage("DIE HÖCHSTE RATE IN GANZ EUROPA".lower())


def test_eigennamen_bleiben_draussen():
    """„Der Oberste Gerichtshof" ist ein Name, kein Rang — und die
    Orthografie sagt es."""
    assert nennt_superlativ("Der Oberste Gerichtshof hat entschieden.") == ""
    assert nennt_superlativ("der oberste Schädel ragt heraus") == "oberste"


def test_leere_eingabe():
    assert nennt_superlativ("") == ""
    assert nennt_superlativ(None) == ""


# ------------------------------------------------- die Vergleichs-Frage

def test_ohne_original_bleibt_das_alte_verhalten():
    """Rueckwaerts-kompatibel: Alle bestehenden Aufrufer uebergeben nur
    die kleingeschriebene Fassung."""
    assert _claim_ist_vergleichsfrage("wien hat den höchsten wert")
    assert not _claim_ist_vergleichsfrage(
        "die donau ist der längste fluss europas")


def test_mit_original_trifft_auch_der_neue_superlativ():
    assert _claim_ist_vergleichsfrage(
        "die donau ist der längste fluss europas",
        "Die Donau ist der längste Fluss Europas")


def test_die_sechs_alten_woerter_treffen_weiter():
    """Regressions-Gate: Die Orthografie-Fassung ERSETZT die Liste nicht,
    sie ergaenzt sie — sonst gingen kleingeschriebene Eingaben verloren."""
    for adj in _SUPERLATIVE_ADJECTIVES:
        assert _claim_ist_vergleichsfrage(f"wien hat die {adj} rate"), adj


def test_der_verneinungs_zweig_bleibt_unberuehrt():
    """Gemessen: EIN verneinter Superlativ im ganzen Korpus, sein Wort
    steht in der Liste. Keine Luecke — also kein Umbau."""
    assert _claim_negates_superlative(
        "deutschland hat gar nicht die niedrigste mordrate")
    assert not _claim_negates_superlative("deutschland hat die höchste rate")


# ---------------------------------------------------- Ende zu Ende, live

def _femizid_quellen():
    with open(os.path.join(DATA, "gleichstellung_pack.json"),
              encoding="utf-8") as fh:
        fakten = json.load(fh)["facts"]
    fakt = next(f for f in fakten
                if (f.get("id") or f.get("topic")) == "femizide_at_de_2026")
    assert deklarierte_namen(fakt.get("data") or {}), "Marker verloren"
    url = "https://www.coordination-vaw.gv.at/dam/jcr:676b18d0"
    quellen = [{"source": "Gleichstellung-Konsens (EIGE + Eurostat)",
                "results": [{"display_value":
                             render_data_with_marker(fakt.get("data") or {}),
                             "url": url}]}]
    belege = [{"source": "Gleichstellung-Konsens (EIGE + Eurostat + OECD)",
               "finding": "…", "url": url}]
    return quellen, belege


#: Die zwei live gegen prod gemessenen Faelle, wortwoertlich.
LIVE = [
    ("Frauen sind die häufigsten Opfer von Tötungsdelikten in Deutschland",
     "false"),
    ("Der Partner ist der häufigste Täter bei Morden an Frauen in Österreich",
     "true"),
]


@pytest.mark.parametrize("claim,verdict", LIVE)
def test_der_guard_erreicht_die_haeufigste_claims(claim, verdict):
    """Heute laeuft er an ihnen vorbei, weil „haeufigste" nicht in der
    Sechs-Wort-Liste steht — obwohl der deklarierende Fakt das Verdict
    traegt (dieselbe Beleg-URL)."""
    quellen, belege = _femizid_quellen()
    res = {"verdict": verdict, "confidence": 0.85, "summary": "Platzhalter.",
           "nuance": "", "evidence": belege}
    apply_verdict_postprocessing(res, quellen, claim)
    assert res["verdict"] == "mixed", claim
    assert res["confidence"] == _ZW_KONFIDENZ_DECKEL
    assert "was gezaehlt wird" in res["nuance"]


def test_ohne_superlativ_kein_eingriff():
    """Giftprobe: derselbe Fakt, dieselben Belege, aber eine Behauptung
    ohne Rang-Anspruch."""
    quellen, belege = _femizid_quellen()
    res = {"verdict": "true", "confidence": 0.85,
           "summary": "Platzhalter.", "nuance": "", "evidence": belege}
    apply_verdict_postprocessing(
        res, quellen,
        "In Österreich wurden 2024 insgesamt 40 Frauen ermordet")
    assert res["verdict"] == "true"
    assert res["confidence"] == 0.85
