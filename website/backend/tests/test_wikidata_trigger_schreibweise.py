"""Die Wikidata-Trigger waren nicht normalisiert — und „regie" steckte in
„Regierung".

Anlass: Beim Live-Verifizieren von #291 habe ich selbst „laengste Fluss"
getippt und keine Antwort bekommen. Gemessen: **6 von 10 Templates**
haben Umlaute in den Triggern, und ``_detect_template_for_claim`` rief
``normalisiere`` nicht auf. Die ASCII-Schreibung erreichte Wikidata nie
— dieselbe Klasse wie #143/#144 und #280.

## ⚠️ Der Anlass ist NICHT der Gewinn

Faltung allein gemessen: ueber 11.032 echte Claims **0 neue richtige
Treffer und 6 neue FEHLtreffer**. Der Trigger „spö" faltet zu „spoe",
und das steckt in niederlaendischem „spoedwet", „rampspoed",
„Tesla-spoelen". Ohne Bindung waere die Faltung ein Rueckschritt
gewesen.

Und die ASCII-Form kommt in den verfuegbaren Korpora GAR NICHT vor: in
13.855 Texten (11.032 Claims + 2.823 Projekt-Formulierungen) trifft
keiner der 25 umlaut-behafteten Trigger in seiner ae/oe/ue-Schreibung.
Beides ist redigierter Text, keine Tastatureingabe — die Begruendung
fuer die Toleranz bleibt die aus #143/#144 (ohne Umlaut-Tastatur tippen
Leute das staendig), nicht eine Messung an diesen Triggern. **Das ist
eine Robustheits-Aenderung ohne messbaren Nutzen in den Daten, die wir
haben.**

## Der messbare Gewinn ist ein anderer

Die Messung hat einen vorbestehenden Defekt aufgedeckt: Der Trigger
``regie`` traf **116 von 11.032 Claims und in KEINEM davon als Wort** —
er steckt in Regierung, Bundesregierung, regieren, Energie. In **89**
davon gewann ``werk_autor`` das erste Match, und der Dienst fragte
Wikidata nach dem AUTOR eines Werks, fuer Saetze wie:

    "Sprit-Preise explodieren: Regierung profitiert, während
     Autofahrer leiden"
    "Die Bayrische Regierung hat ihre Krankenhäuser angewiesen …"

Dieselbe Klasse wie „dina" in KarDINAele (#283).

Korpus-Sweep gegen HEAD, **90 Aenderungen, 0 echte Verluste**:

    84x  werk_autor    -> None             (Regierung-Fehltreffer)
     3x  werk_autor    -> person_partei    (war doppelt getroffen)
     2x  person_partei -> None             ("Spöttisch" ist nicht die SPÖ)
     1x  werk_autor    -> organisation_gruendung

Jede der vier Gruppen einzeln angesehen — keine war ein gewollter
Treffer.

## Was NICHT gebunden wurde, und warum

Binden ist eine Abwaegung je Token, nicht eine Regel (#207/#211).
Gemessen aendert es bei 25 Triggern etwas — aber bei den meisten sind
die Compound-Treffer GEWOLLT:

    präsident   168 frei / 59 gebunden   (Vizepräsident, Präsidentschaft)
    kanzler      78 / 3                  (Bundeskanzler, Vizekanzler)
    grüne        78 / 15                 (Grünen, grünes)
    partei       31 / 11                 (Parteien, Parteitag)

Die wuerde Binden still loeschen. Gebunden sind deshalb genau die zwei,
bei denen die Messung zeigt, dass der Compound-Treffer in ein FREMDES
Wort faellt: ``" regie "`` und ``" spö "``.

Dependency-light: nur die Template-Registry und die Auswahl, kein Netz.
"""
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services._flexion import ist_gebunden  # noqa: E402
from services._schreibweise import norm_terme, normalisiere  # noqa: E402
from services.wikidata import (  # noqa: E402
    _TEMPLATES,
    _TRIGGER_NORM,
    _detect_template_for_claim,
    _trigger_trifft,
)


def _erkenne(claim, entities=("Österreich",)):
    name, _ = _detect_template_for_claim(claim.lower(),
                                         {"entities": list(entities)})
    return name


# ------------------------------------------------ die ASCII-Schreibweise

@pytest.mark.parametrize("ascii_form,umlaut_form,erwartet", [
    ("Welcher ist der hoechste Berg Oesterreichs?",
     "Welcher ist der höchste Berg Österreichs?", "geographie_berg"),
    ("Welcher ist der laengste Fluss Oesterreichs?",
     "Welcher ist der längste Fluss Österreichs?", "geographie_fluss"),
    ("Wer ist der Bundespraesident von Oesterreich?",
     "Wer ist der Bundespräsident von Österreich?", "politiker_amtszeit"),
    ("Wie gross ist die Bevoelkerung von Oesterreich?",
     "Wie groß ist die Bevölkerung von Österreich?", "land_bevoelkerung"),
    ("Wann wurde die Organisation gegruendet?",
     "Wann wurde die Organisation gegründet?", "organisation_gruendung"),
])
def test_beide_schreibweisen_treffen_dasselbe_template(
        ascii_form, umlaut_form, erwartet):
    """Der Anlass: Ich habe selbst „laengste Fluss" getippt und nichts
    bekommen. ⚠️ In den verfuegbaren Korpora kommt diese Schreibung
    allerdings 0x vor — siehe Modul-Docstring."""
    assert _erkenne(umlaut_form) == erwartet, umlaut_form
    assert _erkenne(ascii_form) == erwartet, ascii_form


def test_auch_der_bindestrich_faellt_weg():
    """``normalisiere`` macht aus jedem Trennzeichen ein Leerzeichen —
    das war in #143/#144 der groessere Posten (198 Ausfaelle gegen 127)."""
    assert _erkenne("Was ist der höchste-Berg Österreichs?") \
        == "geographie_berg"


# --------------------------------------------------- „regie" in Regierung

@pytest.mark.parametrize("claim", [
    # Woertlich aus dem Korpus, die klarsten der 84:
    "Sprit-Preise explodieren: Regierung profitiert, während Autofahrer leiden",
    "Die Bayrische Regierung hat ihre Krankenhäuser angewiesen",
    "Die Bundesregierung sieht keinen gesetzlichen Handlungsbedarf",
    # Und die anderen Woerter, in denen der Trigger steckt:
    "Wer soll das Land regieren?",
    "Die Energiekrise ist hausgemacht",
])
def test_regierung_ist_keine_film_regie(claim):
    assert _erkenne(claim) != "werk_autor", claim


@pytest.mark.parametrize("claim", [
    "Wer führte die Regie bei dem Film?",
    "Die Regie lag bei einem Oesterreicher.",
    "Regie: Michael Haneke",
    "Bei dem Film war die Regie",
])
def test_echte_regie_trifft_weiter(claim):
    """Binden darf den gewollten Treffer nicht kosten — am Satzanfang,
    am Satzende und vor Satzzeichen (#207: ohne
    ``wortgrenzen_fassung`` waere Binden still Loeschen)."""
    assert _erkenne(claim) == "werk_autor", claim


# ------------------------------------------------------- „spö" in spoed

@pytest.mark.parametrize("claim", [
    "De spoedwet werd met twee jaar verlengd, tot september 2023.",
    "Deze beelden tonen rampspoed in Noorwegen",
    "Spöttisch setzt sich ein Facebook-Post mit der Rolle von Vulkanen",
])
def test_spoed_und_spoettisch_sind_nicht_die_spoe(claim):
    """Die 6 Fehltreffer, die die Faltung allein gebracht haette."""
    assert _erkenne(claim) != "person_partei", claim


@pytest.mark.parametrize("claim", [
    "Ist Babler Mitglied der SPÖ?",
    "Der SPÖ-Vorsitzende hat gesagt",
    "Er war in der SPÖ.",
    "SPÖ, ÖVP und Grüne",
])
def test_die_echte_spoe_trifft_weiter(claim):
    assert _erkenne(claim) == "person_partei", claim


def test_die_template_reihenfolge_bleibt_wie_sie_war():
    """⚠️ Mein erster Testfall hier war falsch, nicht der Code: „SPÖ und
    ÖVP REGIEREN gemeinsam" trifft ``regieren`` aus
    ``politiker_amtszeit``, und das Template steht vor
    ``person_partei``. Das ist gewolltes, vorbestehendes Verhalten —
    festgehalten, damit es nicht jemand als Fehler „repariert"."""
    assert _erkenne("SPÖ und ÖVP regieren gemeinsam") == "politiker_amtszeit"


# ------------------------------------------- die Mechanik selbst (Gates)

def test_jedes_template_hat_gefaltete_trigger():
    """Zensus als Gate (#238): ein neues Template faellt hier auf, auch
    wenn niemand an diesen Test denkt."""
    assert set(_TRIGGER_NORM) == {t["name"] for t in _TEMPLATES}
    for t in _TEMPLATES:
        assert _TRIGGER_NORM[t["name"]] == norm_terme(*t["triggers"])
        assert len(_TRIGGER_NORM[t["name"]]) == len(t["triggers"])


def test_kein_trigger_verliert_seine_bindung_durch_die_faltung():
    """DER Fallstrick: Wuerde ``normalisiere`` die Rand-Leerzeichen
    strippen, waere jede Bindung still weg — und „regie" traefe wieder
    116 Claims. ``normalisiere`` strippt bewusst nicht (#207)."""
    gebunden = [trig for t in _TEMPLATES for trig in t["triggers"]
                if ist_gebunden(trig)]
    assert gebunden, "kein gebundener Trigger mehr — Messung pruefen"
    for trig in gebunden:
        assert ist_gebunden(normalisiere(trig)), trig


def test_die_gefalteten_trigger_tragen_keine_umlaute_mehr():
    for name, trigs in _TRIGGER_NORM.items():
        for trig in trigs:
            assert not any(c in trig for c in "äöüß"), (name, trig)


def test_gebundener_trigger_trifft_nur_als_wort():
    """Der Vertrag von ``_trigger_trifft``, direkt geprueft."""
    assert _trigger_trifft("die regie lag bei ihm",
                           " die regie lag bei ihm ", " regie ")
    assert not _trigger_trifft("die regierung profitiert",
                               " die regierung profitiert ", " regie ")
    # Ungebundene Trigger verhalten sich wie vorher: reiner Substring.
    assert _trigger_trifft("die regierung profitiert",
                           " die regierung profitiert ", "regierung")


def test_keine_flexionstoleranz_eingeschleppt():
    """Die Auswahl hatte nie Flexionstoleranz — ``_trigger_trifft`` darf
    sie nicht mitbringen, sonst aendert sich die Semantik aller zehn
    Templates auf einmal."""
    assert not _trigger_trifft("der hoechsten berge", " der hoechsten berge ",
                               "hoechster berg")
