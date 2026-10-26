"""Zwei Wächter haben ihre eigene Begründung überschrieben.

Beide Befunde sind bei der Live-Prüfung von #276 und #277 aufgefallen, in
derselben Funktion, mit derselben Gestalt: Ein Postprocess-Wächter dreht ein
RICHTIGES Modell-Label auf ein falsches, während die Summary daneben weiter
das Gegenteil sagt. Der Code nennt diese Klasse an einer Stelle selbst "the
most credibility-damaging error class".

## Befund A — Muster O hielt ein Nomen für einen Komparativ

    Claim:   "Österreich wird vom Climate Action Tracker als kritisch
              unzureichend bewertet."        (falsch — Overall ist
                                              'Insufficient')
    Modell:  false @ 0.95
    Summary: "… bewertet Österreich … mit 'Insufficient', NICHT mit
              'kritisch unzureichend'."
    Nuance:  "Die Behauptung verwechselt die Bewertungskategorien."
    geliefert: true @ 0.85

``_O_KOMPARATIV_RE`` sucht ``\\b([a-zäöüß]{4,})er\\s+als\\b``. In "Climate
Action **Tracker als**" findet das "track" + "er" + " als" — formgleich mit
"puenktlicher als". Danach standen die Partnerwörter in der Summary, die
Verneinung ("nicht") lag RECHTS der Prüfstelle, und Muster O drehte das
Label.

Das ist kein Randfall: Nomen auf -er sind im Deutschen häufig, und "als"
nach einem Nomen ist die normale Gleichsetzung ("gilt als", "bezeichnet
als", "wird als X bewertet"). Gemessen an 11.032 echten Faktencheck-Claims
aus ``claimreview_index``: 132 Claims kann Muster O zerlegen, 8 davon sind
keine Vergleiche — "AfD-Wähler als Ratten", "Bilder als Beweise", "ein
anderes Silvester als sonst", "Bücher", "Europäer", "Besucher",
"IV-Empfänger". Knapp jeder sechzehnte.

Der Fix nimmt die ORTHOGRAFIE, nicht eine Wortliste: Deutsch schreibt
Nomen groß, ein Komparativ in "A ist Xer als B" steht klein. Gemessen:

    stress_tests (Projekt-Korpus)   1.123 Claims   50 zerlegbar   0 verworfen
    claimreview_index               11.032 Claims  132 zerlegbar  8 verworfen
    QA50I                           97 Claims      16 zerlegbar   0 verworfen

Der einzige Verlust wäre ein Komparativ am SATZANFANG ("Schneller als
erwartet werden …") — der ist ausgenommen. Trägt der Claim keine
Kleinbuchstaben, sagt die Orthografie nichts, dann greift die Regel nicht.

## Befund B — die Bestätigung in der falschen Satzform

Direkt nach dem Deploy von #277, an genau dem Claim, dessen Daten dort
korrigiert wurden:

    Claim:   "Vaeter nehmen in Oesterreich selten Karenz."   (wahr)
    Modell:  true @ 0.95
    Summary: "In Österreich entfielen 2022 nur 4,1 % der
              KBG-Anspruchstage auf Männer … Beide Messgrößen
              BESTÄTIGEN, DASS Väter in Österreich selten Karenz nehmen."
    geliefert: mostly_false @ 0.85

Der STRUKTURELL-FALSCH-Override hat eine Relevanz-Wache: Bejaht die Summary
den Claim, ist der STRUKTURELL-Marker vermutlich zu einem ANDEREN Satz
eingesammelt worden, und der Override schweigt. Diese Wache kannte zwölf
feste Wendungen — "ist korrekt", "wird bestätigt", "bestätigt dies" — aber
nicht die aktive Mehrzahl mit dass-Satz. Also sah sie keine Bestätigung,
hielt den fremden Marker für einschlägig (Verhältnis 2/10 = 20 %, oberhalb
der Tier-1-Schwelle von 15 %) und überschrieb ein richtiges Label.

Dieselbe Lehre wie #140/#141: Formulierungen aufzählen scheitert.

**Ein erster Fix war messbar schlechter.** Nur den Wortstamm zu nehmen
("bestaetig…") brachte auf 98 QA50I-Summaries zwei neue Treffer, und beide
hatten das Label ``false``: "… bestätigen, dass Homöopathie KEINE Wirkung
hat" und "Mammographien sind NICHT verboten". Dort bejaht der dass-Satz das
GEGENTEIL des Claims. Nicht das Verb entscheidet, sondern der dass-Satz —
deshalb jetzt zwei Bedingungen: keine Verneinung im dass-Satz, und der
dass-Satz wiederholt den Claim (mindestens zwei und mindestens die Hälfte
seiner Inhaltswörter).

So gemessen: EIN neuer Treffer auf denselben 98 Summaries, mit dem Label
``true`` ("Die UNHCR-Daten zeigen, dass die Zahl der Binnenvertriebenen …")
— eine echte Bejahung.

Dependency-light: nur verdict_postprocess und Daten, kein Netz.
"""
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

from services.verdict_postprocess import (  # noqa: E402
    apply_verdict_postprocessing,
    komparativ_ist_nomen,
    summary_bejaht_per_dass_satz,
    vergleich_aus_claim,
)

# --------------------------------------------------------------- Befund A

#: Der Live-Fall vom 26.10.2026, Wort fuer Wort wie geliefert.
A_CLAIM = ("Österreich wird vom Climate Action Tracker als kritisch "
           "unzureichend bewertet.")
A_SUMMARY = ("Der Climate Action Tracker (CAT) bewertet Österreich aktuell "
             "(Stand 22.10.2026) mit 'Insufficient' (unzureichend), nicht "
             "mit 'kritisch unzureichend'.")
A_NUANCE = ("CAT bewertet Österreich als 'Insufficient', was zwei Stufen "
            "über 'Critically insufficient' liegt. Die Behauptung "
            "verwechselt die Bewertungskategorien.")


def _lauf(claim, verdict, summary, nuance="", quellen=None, konfidenz=0.95):
    ergebnis = {
        "verdict": verdict,
        "confidence": konfidenz,
        "summary": summary,
        "nuance": nuance,
        "evidence": [{"source": "Quelle", "type": "study",
                      "finding": "Befund", "strength": "strong"}],
    }
    return apply_verdict_postprocessing(ergebnis, quellen or [], claim)


def test_der_live_fall_behaelt_sein_false():
    """Die eigentliche Regression: true @ 0.85 statt false."""
    out = _lauf(A_CLAIM, "false", A_SUMMARY, A_NUANCE)
    assert out["verdict"] == "false"


def test_das_label_widerspricht_der_summary_nicht_mehr():
    """Die Summary sagt 'nicht kritisch unzureichend' — das Label muss das
    teilen, nicht bejahen."""
    out = _lauf(A_CLAIM, "false", A_SUMMARY, A_NUANCE)
    assert out["verdict"] not in ("true", "mostly_true")


@pytest.mark.parametrize("claim", [
    # Giftproben: jede dieser Formen wurde vor dem 26.10.2026 als
    # Komparativ gelesen. Alle sind Nomen auf -er vor "als".
    "Österreich wird vom Climate Action Tracker als kritisch unzureichend bewertet.",
    "Bundespräsident bezeichnet AfD-Wähler als Ratten",
    "In einem Facebook-Post werden mehrere Bilder als vermeintliche Beweise gezeigt.",
    "Die Nacht vom 31.12.2020 war in Deutschland ein anderes Silvester als sonst",
    "Der Minister bezeichnete die Europäer als Vorbild.",
    "Die Zeitung führt den Sprecher als Quelle an.",
])
def test_nomen_vor_als_ist_kein_komparativ(claim):
    assert komparativ_ist_nomen(claim)


@pytest.mark.parametrize("claim", [
    # Echte Vergleiche — Muster O muss weiter arbeiten duerfen.
    "Die ÖBB sind pünktlicher als die Deutsche Bahn",
    "Covid-19 ist nicht gefährlicher als eine Grippe-Erkrankung",
    "Das Bundeskanzleramt in Berlin ist wesentlich größer als das Weiße Haus in Washington",
    "Solarmodul-Abfall ist giftiger als Atommüll.",
    "Omikron ist für Geimpfte ansteckender als für Ungeimpfte",
    "In Österreich wurden 2024 mehr Männer als Frauen ermordet",
    "Weibliche Beschäftigte verdienen weniger als männliche",
])
def test_echte_vergleiche_bleiben_vergleiche(claim):
    assert not komparativ_ist_nomen(claim)


def test_komparativ_am_satzanfang_bleibt_erhalten():
    """"Schneller als erwartet" steht gross, ist aber ein Komparativ."""
    assert not komparativ_ist_nomen(
        "Schneller als erwartet werden noch heuer Impfungen verfügbar sein")
    assert not komparativ_ist_nomen(
        "Die Lage ist ernst. Schneller als erwartet steigen die Zahlen.")


def test_durchgehende_grossschreibung_traegt_kein_signal():
    """Ohne Kleinbuchstaben sagt die Orthografie nichts — die Regel
    schweigt, statt jeden Vergleich zu verwerfen."""
    assert not komparativ_ist_nomen(
        "ÖSTERREICH WIRD VOM CLIMATE ACTION TRACKER ALS UNZUREICHEND BEWERTET")
    assert not komparativ_ist_nomen("DIE ÖBB SIND PÜNKTLICHER ALS DIE DB")


def test_mehr_und_weniger_sind_nie_nomen():
    """Der zweite Zweig der Regex braucht die Pruefung nicht."""
    assert not komparativ_ist_nomen("Dresden hatte mehr Tote als Auschwitz")
    assert not komparativ_ist_nomen("Wer weniger als 8 Gläser trinkt, dehydriert")


def test_ohne_claim_keine_aussage():
    assert not komparativ_ist_nomen("")
    assert not komparativ_ist_nomen("Die Erde ist rund")


@pytest.mark.parametrize("korpus,feld", [
    ("tools/stress_test_100_claims.json", "claim"),
])
def test_kein_vergleich_im_projekt_korpus_geht_verloren(korpus, feld):
    """Im eigenen Korpus darf die Regel NICHTS verwerfen.

    Gemessen am 26.10.2026 ueber 1.123 Claims aus tools/stress_tests/ plus
    diese Datei: 50 von Muster O zerlegbar, 0 verworfen. Gepinnt wird die
    Null, nicht die 50 — die Korpus-Groesse darf wachsen.
    """
    pfad = os.path.join(BACKEND, korpus)
    with open(pfad, encoding="utf-8") as fh:
        daten = json.load(fh)
    claims = []
    stapel = [daten]
    while stapel:
        o = stapel.pop()
        if isinstance(o, dict):
            c = o.get(feld)
            if isinstance(c, str) and len(c) > 10:
                claims.append(c)
            stapel.extend(o.values())
        elif isinstance(o, list):
            stapel.extend(o)
    verworfen = [c for c in claims
                 if vergleich_aus_claim(c.lower()) and komparativ_ist_nomen(c)]
    assert not verworfen, verworfen


# --------------------------------------------------------------- Befund B

B_CLAIM = "Vaeter nehmen in Oesterreich selten Karenz."
B_SUMMARY = (
    "In Österreich entfielen 2022 nur 4,1 % der "
    "Kinderbetreuungsgeld-Anspruchstage auf Männer (Rechnungshof 2024/31), "
    "und 15,40 % der Väter (Geburtsjahr 2022) bezogen überhaupt Karenz. "
    "Beide Messgrößen bestätigen, dass Väter in Österreich selten Karenz "
    "nehmen.")

#: Zwei von zehn Ergebnissen tragen einen STRUKTURELL-FALSCH-Marker — wie
#: live gemessen (Verhaeltnis 20 %). Die Marker gehoeren zu ANDEREN Thesen.
B_QUELLEN = [{"results": [
    {"display_value": "STRUKTURELL FALSCH: Die These 'KBG ist nur fuer "
                      "Frauen' ist unzutreffend."},
    {"display_value": "STRUKTURELL FALSCH: Die These 'Mutterschaft "
                      "beeinflusst Karriere kaum' ist falsch."},
] + [{"display_value": f"Ergebnis {i} ohne Marker"} for i in range(8)]}]


def test_der_zweite_live_fall_behaelt_sein_true():
    out = _lauf(B_CLAIM, "true", B_SUMMARY, quellen=B_QUELLEN)
    assert out["verdict"] == "true"


def test_die_bejahung_im_dass_satz_wird_erkannt():
    assert summary_bejaht_per_dass_satz(B_CLAIM, B_SUMMARY)


@pytest.mark.parametrize("claim,summary", [
    # Giftproben fuer den ERSTEN, verworfenen Fix: der dass-Satz bejaht das
    # GEGENTEIL. Beide am 26.10.2026 in QA50I-Summaries mit Label 'false'
    # gemessen.
    ("Homöopathie wirkt besser als Placebo",
     "Systematische Reviews bestätigen, dass Homöopathie keine Wirkung über "
     "Placebo hinaus hat."),
    ("Mammographien sind in der Schweiz verboten",
     "Die Quellen belegen, dass Mammographien in der Schweiz nicht verboten "
     "sind."),
    ("Die Zahl der Anträge ist gestiegen",
     "Die Daten zeigen, dass die Zahl der Anträge nicht gestiegen ist."),
])
def test_verneinung_im_dass_satz_ist_keine_bejahung(claim, summary):
    assert not summary_bejaht_per_dass_satz(claim, summary)


def test_fremdes_thema_im_dass_satz_ist_keine_bejahung():
    """Wenig Ueberschneidung mit dem Claim => kein Bezug."""
    assert not summary_bejaht_per_dass_satz(
        "Väter nehmen in Österreich selten Karenz",
        "Die Statistik bestätigt, dass die Mieten in Wien gestiegen sind.")


def test_zu_kurzer_claim_bleibt_unbewertet():
    """Unter zwei Inhaltswoertern traegt die Ueberschneidung nichts."""
    assert not summary_bejaht_per_dass_satz(
        "Wien", "Die Daten bestätigen, dass Wien wächst.")


def test_der_override_feuert_weiter_wenn_er_soll():
    """Ohne Bejahung in der Summary bleibt der Override scharf.

    Sonst waere der Fix ein Loch: Der Override existiert, weil das Modell
    einen echten STRUKTURELL-Marker uebergeht.
    """
    out = _lauf("KBG bekommen nur Frauen", "true",
                "Das Kinderbetreuungsgeld steht beiden Elternteilen zu.",
                quellen=B_QUELLEN)
    assert out["verdict"] == "mostly_false"


def test_der_override_feuert_bei_dominantem_marker():
    """Ab 50 % Marker-Anteil greift er auch gegen eine bejahende Summary."""
    quellen = [{"results": [
        {"display_value": "STRUKTURELL FALSCH: Die These ist unzutreffend."},
        {"display_value": "Ergebnis ohne Marker"},
    ]}]
    out = _lauf(B_CLAIM, "true", B_SUMMARY, quellen=quellen)
    assert out["verdict"] == "mostly_false"


# ------------------------------------------- beide Befunde, eine Gestalt

@pytest.mark.parametrize("claim,verdict,summary,nuance", [
    (A_CLAIM, "false", A_SUMMARY, A_NUANCE),
    (B_CLAIM, "true", B_SUMMARY, ""),
])
def test_kein_wachter_dreht_das_label_gegen_die_summary(
        claim, verdict, summary, nuance):
    """Die gemeinsame Regel: Das gelieferte Label stimmt mit dem ueberein,
    das die Begruendung traegt."""
    quellen = B_QUELLEN if verdict == "true" else []
    out = _lauf(claim, verdict, summary, nuance, quellen=quellen)
    assert out["verdict"] == verdict, (
        f"Label gedreht: {verdict} -> {out['verdict']}")
