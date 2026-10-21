"""Die genannte Quelle wurde durch eine andere ersetzt.

QA50I-Befund #88. "Ungarn ist laut Polity5 eine vollstaendige Demokratie"
bekam ``false@0.85`` — widerlegt mit V-Dem 2025 (0,32 im
Liberal-Demokratie-Index). Die Behauptung ist aber eine Aussage UEBER
Polity5, und was Polity5 sagt, ist nachpruefbar.

## Die Pruefung an der Quelle

Am 21.10.2026 gegen den maschinenlesbaren Datensatz geprueft
(``open-numbers/ddf--inscr--polity5``, Variablen ``polity`` und
``polity2``): Ungarn hat **+10 durchgehend von 2010 bis 2018**. Unser
kuratierter Auszug stimmt fuer diesen Wert.

Dass Polity Ungarns Rueckbau nicht erfasst hat, ist ein bekannter
Kritikpunkt am Index — und kein Fehler unserer Datei. Genau deshalb ist
die Frage "was sagt Polity5" von der Frage "wie steht es wirklich"
verschieden.

## Was "Full Democracy" bei Polity bedeutet

Das Polity5-Manual verwendet den Begriff vier Mal, und zwar mit ZWEI
verschiedenen Bedeutungen:

    S. 845   "a single regime score that ranges from +10 (full democracy)
              to -10 (full autocracy)"          -> Skalen-Endpunkt, genau +10
    S. 1889  "partial democracy (POLITY values +1 to +6) or full
              democracy (POLITY values +7 to +10)"  -> Bereich, fuer die
              Regime-Transitions-Kodierung

Unsere Datei fuehrt die Endpunkt-Lesart (+10). Unter BEIDEN Lesarten ist
ein Land mit +10 eine Full Democracy — die Doppeldeutigkeit aendert die
Antwort also nicht. Sie gehoert trotzdem festgehalten.

Nebenbei gemessen: "anocracy", "open anocracy", "closed anocracy" und
"consolidated democracy" stehen im Manual NULL Mal. Die Projektseite nennt
nur drei Kategorien (autocracies -10..-6, anocracies -5..+5, democracies
+6..+10); unsere fuenfstufige Fassung ist die feinere Einteilung der
Polity-Global-Reports. Das steht jetzt in ``anocracy_hinweis``, damit
niemand sie fuer die Projektseiten-Fassung haelt.

## Warum eine Daten-Sperre und kein Guard

Der naheliegende Guard waere: "Claim nennt Quelle A, Antwort widerlegt mit
Quelle B -> abwerten". Prototypisch gebaut und verworfen. Der
Unterscheider zu Befund #31 haengt an EINEM Wort: Dort sagt der Satz ueber
Freedom House "nimmt KEINE Regime-Klassifikation vor" und enthaelt den
Kernbegriff des Claims nicht; hier sagt der Satz ueber Polity5 "als 'volle
Demokratie' eingestuft" und enthaelt ihn. Eine Paraphrase ("Staatsform")
haette den Guard ausgehebelt, und bei Politik-Claims sind Fehltreffer
besonders teuer (Guardrail 3).

Die Daten-Sperre ist der Weg, der in diesem Projekt traegt — und sie hat
ein Vorbild in derselben Domaene: ``services/freedom_house.py`` haengt
seit der QA vom 06.09.2026 bei Regime-Claims an, dass FIW Freiheitsgrade
und keine Staatsform misst. Das ist die Gegenrichtung desselben Fehlers:
Dort wird einer Quelle eine Aussage zugeschrieben, die sie nicht macht,
hier wird die genannte Quelle ersetzt.

## Die Reihenfolge war die Arbeit

Erst stand die Sperre HINTER der Skala-Beschreibung — und ``[:300]`` hat
sie abgeschnitten. Jetzt steht sie vorn, und der Cut liegt bei 400, dem
echten Prompt-Budget (``PROMPT_MAX_STR``). Dieselbe Lehre wie der
V-Dem-Caveat in ``services/vdem.py``, der bewusst NACH dem Cut steht.

Dependency-light: nur Daten und Service, kein Netz, kein Modell.
"""
import asyncio
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

import services.polity5 as p5  # noqa: E402
from services.synthesizer import PROMPT_MAX_STR  # noqa: E402


def _daten():
    with open(os.path.join(DATA, "polity5.json"), encoding="utf-8") as fh:
        return json.load(fh)


def _ergebnis(claim):
    a = {"claim": claim, "original_claim": claim, "queries": [claim]}
    res = asyncio.run(p5.search_polity5(a)).get("results") or []
    assert res, f"kein Treffer fuer {claim!r}"
    return res[0]


#: Aus dem Datensatz geprueft (open-numbers/ddf--inscr--polity5).
HUN_2010_2018 = 10


# --- Die Sperre -------------------------------------------------------

@pytest.mark.parametrize("claim", [
    "Ungarn ist laut Polity5 eine vollständige Demokratie",
    "Ist Ungarn nach Polity5 eine Demokratie?",
    # Diese zwei brauchen das neu ergaenzte, bare " polity "-Token: Vorher
    # kannte die Keyword-Liste nur Komposita ("polity 5", "polity score"),
    # und "laut Polity" traf NICHTS. Von diesen Testfaellen aufgedeckt.
    "Polen ist laut Polity eine Autokratie",
    "Russland hat laut Polity ein autoritäres Regime",
])
def test_regime_claims_ohne_jahr_bekommen_die_sperre(claim):
    """Der Kern: Eine Aussage UEBER Polity5 darf nicht mit einem anderen
    Index widerlegt werden."""
    de = _ergebnis(claim)["description"]
    assert "nur bis 2018" in de
    assert "fragt nach Polity5" in de, "die Pointe der Sperre fehlt"
    assert "V-Dem oder Freedom House" in de


@pytest.mark.parametrize("claim", [
    # MIT Regime-Begriff UND Jahr — nur so isoliert der Fall die
    # Jahres-Ausnahme. Eine erste Fassung nahm "Ungarn hatte 2018 laut
    # Polity5 einen Polity-Score von +10": Dort schweigt die Warnung
    # schon, weil kein Regime-Begriff drinsteht, und die Giftprobe zur
    # Jahres-Ausnahme HIELT.
    "Ungarn war 2018 laut Polity5 eine vollständige Demokratie",
    "War Polen 1990 laut Polity eine Demokratie?",
    "Ungarn hatte 2018 laut Polity5 einen Polity-Score von +10",
])
def test_claims_mit_jahresangabe_bekommen_sie_nicht(claim):
    """Mit Jahresangabe ist die Frage datiert und beantwortbar — die
    Sperre kostet dann nur Prompt-Budget, das die Zahlen brauchen
    (gleiche Begruendung wie bei Freedom House)."""
    de = _ergebnis(claim)["description"]
    assert "fragt nach Polity5" not in de
    assert "Skala von -10" in de


def test_claims_ohne_regime_begriff_bekommen_sie_nicht():
    de = _ergebnis("Welchen Polity-Score hat Ungarn?")["description"]
    assert "fragt nach Polity5" not in de
    assert "Skala von -10" in de


def test_die_sperre_steht_vorn_und_ueberlebt_die_kuerzung():
    """Erst stand sie HINTER der Skala-Beschreibung und wurde von
    ``[:300]`` abgeschnitten — die Sperre war das Erste, was wegfiel."""
    de = _ergebnis("Ungarn ist laut Polity5 eine vollständige Demokratie")[
        "description"]
    assert de.startswith("WICHTIG: Polity5 beurteilt Regime nur bis 2018")
    assert len(de) <= PROMPT_MAX_STR, (len(de), PROMPT_MAX_STR)
    kopf = de[:PROMPT_MAX_STR]
    assert "fragt nach Polity5" in kopf


def test_der_cut_entspricht_dem_echten_prompt_budget():
    """300 war enger als das Budget und schnitt mitten im Satz ab."""
    assert p5._BESCHREIBUNG_MAX == PROMPT_MAX_STR == 400
    with open(os.path.join(BACKEND, "services", "polity5.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    # Geprueft wird die VERWENDUNG, nicht der Volltext: Der Kommentar an
    # der Fundstelle zitiert "description[:300]" absichtlich, damit der
    # naechste den Grund kennt. Eine erste Fassung dieses Tests schlug
    # daran fehl — dieselbe Falle wie in #270 und #272.
    assert "description[:_BESCHREIBUNG_MAX]" in quelle
    assert 'description[:300],' not in quelle


# --- Der Wert selbst --------------------------------------------------

def test_ungarn_steht_mit_zehn_in_den_daten():
    """Am 21.10.2026 gegen den Datensatz geprueft: +10 durchgehend
    2010-2018, in ``polity`` UND ``polity2``."""
    rec = _daten()["scores_2018"]["HUN"]
    assert rec["score"] == HUN_2010_2018
    assert rec["category"] == "full_democracy"


def test_die_stichprobe_ist_im_audit_vermerk_festgehalten():
    """Sonst prueft der naechste denselben Wert wieder — und #267 hatte
    nur geprueft, dass es keinen neueren Release gibt, nicht die
    Einzelwerte."""
    flag = _daten()["audit_flag"]
    assert flag.startswith("STICHPROBE 2026-10-21")
    assert "open-numbers" in flag
    assert "2010 bis 2018" in flag
    assert "Kritikpunkt am Index" in flag


def test_die_ausgabe_nennt_score_label_und_jahr():
    """Die Kategorie steht im display_value (1.200 Zeichen bei Rang 0),
    nicht in der gekuerzten description."""
    dv = _ergebnis("Ungarn ist laut Polity5 eine vollständige Demokratie")[
        "display_value"]
    assert "+10" in dv
    assert "Full Democracy" in dv
    assert "2018" in dv


# --- Die Kategorien ---------------------------------------------------

def test_die_doppelte_manual_definition_ist_festgehalten():
    h = _daten()["kategorien_hinweis"]
    assert "ZWEIMAL" in h
    assert "+7 bis +10" in h
    assert "Endpunkt-Lesart" in h


def test_der_anocracy_befund_ist_festgehalten():
    """"anocracy" steht im Polity5-Manual NULL Mal; die Projektseite nennt
    nur drei Kategorien."""
    h = _daten()["anocracy_hinweis"]
    assert "NICHT im Polity5-Manual" in h
    assert "Global-Reports" in h
    assert "drei Kategorien" in h


def test_die_skala_kommt_aus_den_daten_nicht_aus_dem_code():
    """Die Kategorie-Bereiche standen als Literal im Service und haetten
    bei jeder Korrektur in den Daten driften koennen (#74/#270)."""
    with open(os.path.join(BACKEND, "services", "polity5.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    assert 'data.get("regime_categories")' in quelle
    for literal in ('"Open Anocracy (+1..+5)"', '"Democracy (+6..+9)"'):
        assert literal not in quelle, f"{literal} steht wieder im Code"
    # Verhaltens-Probe: Eine Aenderung in den Daten muss in der Ausgabe
    # ankommen — ein Literal koennte das nicht.
    de = _ergebnis("Welchen Polity-Score hat Ungarn?")["description"]
    for v in _daten()["regime_categories"].values():
        assert v["label_en"] in de or len(de) >= PROMPT_MAX_STR


def test_der_iso2_fallback_erfindet_keinen_fremden_code():
    """``iso3[:2]`` machte bei BTI aus COD ein CO (Kolumbien, #268)."""
    with open(os.path.join(BACKEND, "services", "polity5.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    assert "primary_iso3[:2]" not in quelle
    assert "iso3[:2]" not in quelle


# --- Die Gegenrichtung bei Freedom House bleibt ----------------------

def test_die_freedom_house_warnung_bleibt_bestehen():
    """Befund #31 derselben Batterie: Dort schreibt der Synthesizer einer
    Quelle eine Klassifikation zu, die sie nicht vornimmt. Die Warnung
    dafuer steht seit der QA vom 06.09.2026 in services/freedom_house.py
    und ist der Grund, warum #31 jetzt 3/3 richtig antwortet.
    """
    with open(os.path.join(BACKEND, "services", "freedom_house.py"),
              encoding="utf-8") as fh:
        quelle = fh.read()
    assert "FIW misst FREIHEITSGRADE, keine Staatsform" in quelle
    assert "_REGIME_BEGRIFFE" in quelle


def test_beide_warnungen_nutzen_dieselben_regime_begriffe():
    """Die zwei Listen duerfen nicht auseinanderlaufen — sonst greift die
    eine Sperre bei einem Claim, die andere nicht."""
    from services.freedom_house import _REGIME_BEGRIFFE as FH
    gemeinsam = {"demokratie", "demokratisch", "diktatur", "autokratie",
                 "regime", "staatsform"}
    fh_norm = {t.strip() for t in FH}
    p5_norm = {t.strip() for t in p5._REGIME_BEGRIFFE}
    for begriff in gemeinsam:
        assert begriff in fh_norm, f"{begriff} fehlt bei Freedom House"
        assert begriff in p5_norm, f"{begriff} fehlt bei Polity5"
