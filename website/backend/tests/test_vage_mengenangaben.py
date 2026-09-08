"""„Kaum" und „viele" haben keine Schwelle — und in der Grauzone stimmt das
Modell beiden Richtungen zu.

QA50F-Befund 5, als Spiegel-Paar gegen die Live-Instanz gemessen:

    „Über die östliche Landgrenze kommen kaum noch Menschen"       true@0.9
    „Über die östliche Landgrenze kommen weiterhin viele Menschen" true@0.9

Beide Richtungen wahr, auf derselben Zahl (3.209 Detektionen). Logisch
unmöglich. Dieselben zwei Sätze für die zentrale Mittelmeerroute (16.454)
trennt das System korrekt — der Unterschied ist nicht die Logik, sondern die
Grauzone.

#174 gab dem Modell die Bezugsgrösse (5 % aller Grenzübertritte, grösste
Route 20.232). Nachgemessen: **die Zahlen kommen in der Summary an, der
Widerspruch bleibt.** Bessere Daten lösen ihn nicht.

Dieser Deckel löst ihn auch nicht. Er verhindert, dass eine Auslegungsfrage
mit 0,9 Selbstsicherheit ausgeliefert wird, und sagt dem Leser, woran es
liegt.
"""

import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import (  # noqa: E402
    VAGE_MENGE_CONFIDENCE_CAP, apply_vage_menge_cap, hat_vage_mengenangabe,
)


def _kappe(claim, verdict="true", conf=0.9):
    s = {"verdict": verdict, "confidence": conf}
    return apply_vage_menge_cap(s, claim)


# --------------------------------------------------------------------------
# Erkennung
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim", [
    "Über die östliche Landgrenze kommen kaum noch Menschen in die EU",
    "Über die östliche Landgrenze kommen weiterhin viele Menschen",
    "Zahlreiche Betriebe haben geschlossen",
    "So gut wie keine Menschen kommen noch",
    "Unzählige Haushalte sind betroffen",
])
def test_mengenwoerter_ohne_schwelle_werden_erkannt(claim):
    assert hat_vage_mengenangabe(claim)


@pytest.mark.parametrize("claim", [
    "Vielleicht ist die Vielfalt gross",
    "Die Vielzahl der Regelungen ist bekannt",
    "Wenigstens ist es billiger geworden",
])
def test_wortgrenzen_halten(claim):
    """„viel" als Substring fängt „vielleicht" und „Vielfalt" — dieselbe
    Klasse wie das „alle "/„Kristalle" der EZB-Regex."""
    assert not hat_vage_mengenangabe(claim)


@pytest.mark.parametrize("claim", [
    "Über die Landgrenze kommen mehr als 3000 Menschen",
    "Es sind weniger als 5000 Personen",
    "Der Anteil liegt unter 5 %",
    "Mindestens 20 Prozent sind betroffen",
])
def test_eigene_schwelle_schliesst_die_regel_aus(claim):
    """Nennt der Claim selbst eine Grenze, ist er prüfbar — dann ist ein
    bestimmtes Verdict mit hoher Konfidenz genau richtig."""
    assert not hat_vage_mengenangabe(claim)


def test_jahreszahl_ist_keine_schwelle():
    """Sonst würde jeder Claim mit Jahresangabe aus der Regel fallen —
    „2026 kommen kaum noch Menschen" ist genauso eine Auslegungsfrage."""
    assert hat_vage_mengenangabe("2026 kommen kaum noch Menschen")


def test_leerer_claim():
    assert not hat_vage_mengenangabe("")


# --------------------------------------------------------------------------
# Die Kappung
# --------------------------------------------------------------------------

def test_beide_richtungen_werden_gleich_behandelt():
    """Der Kern des Befundes: das Spiegel-Paar bekam zweimal 0,9. Nach dem
    Deckel bekommt es zweimal den Deckel — der Widerspruch bleibt sichtbar,
    aber er wird nicht mehr als Gewissheit verkauft."""
    a = _kappe("Über die östliche Landgrenze kommen kaum noch Menschen")
    b = _kappe("Über die östliche Landgrenze kommen weiterhin viele Menschen")
    assert a["confidence"] == b["confidence"] == VAGE_MENGE_CONFIDENCE_CAP


def test_verdict_bleibt_unangetastet():
    """In der Grauzone ist die Antwort nicht falsch, nur nicht so sicher wie
    sie aussah. Ein richtiges Verdict wegzuwerfen macht den Dienst
    schlechter, nicht ehrlicher (dieselbe Abwägung wie #166)."""
    s = _kappe("Es kommen kaum noch Menschen", verdict="mostly_false")
    assert s["verdict"] == "mostly_false"


def test_hinweis_wird_angehaengt():
    s = _kappe("Es kommen kaum noch Menschen")
    assert "ohne feste Schwelle" in (s.get("nuance") or "")


def test_bestehende_nuance_bleibt_erhalten():
    s = {"verdict": "true", "confidence": 0.9, "nuance": "Vorhandener Text."}
    apply_vage_menge_cap(s, "Es kommen kaum noch Menschen")
    assert s["nuance"].startswith("Vorhandener Text.")
    assert "ohne feste Schwelle" in s["nuance"]


def test_niedrige_konfidenz_bleibt_unberuehrt():
    """Der Deckel deckelt, er hebt nicht an — und ohne Kappung auch kein
    Hinweis, sonst steht er unter jeder ohnehin vorsichtigen Antwort."""
    s = _kappe("Es kommen kaum noch Menschen", conf=0.4)
    assert s["confidence"] == 0.4
    assert not s.get("nuance")


@pytest.mark.parametrize("verdict", ["unverifiable", "mixed"])
def test_unsichere_verdicts_sind_ausgenommen(verdict):
    """Dort ist die Unsicherheit schon ausgesprochen."""
    s = _kappe("Es kommen kaum noch Menschen", verdict=verdict)
    assert s["confidence"] == 0.9


def test_claim_ohne_mengenwort_bleibt_unberuehrt():
    s = _kappe("Die Demokratie in Polen hat sich seit 2023 verbessert")
    assert s["confidence"] == 0.9
    assert not s.get("nuance")


def test_beobachtbarkeits_flag():
    s = _kappe("Es kommen kaum noch Menschen")
    assert s.get("_vage_menge") is True


def test_englischer_hinweis():
    s = {"verdict": "true", "confidence": 0.9}
    apply_vage_menge_cap(s, "Es kommen kaum noch Menschen", lang="en")
    assert "threshold" in (s.get("nuance") or "")


# --------------------------------------------------------------------------
# Prompt-Regel und Verdrahtung
# --------------------------------------------------------------------------

def test_prompt_regel_ist_gesetzt():
    """Der Deckel kann nur die Konfidenz senken. Ob ein „mixed" richtiger
    wäre, kann nur das Modell entscheiden — dafür die Regel."""
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "VAGE MENGENANGABEN" in quelle
    assert "würdest" in quelle, "Selbstprüfung gegen das Gegenteil fehlt"


def test_cap_ist_in_der_pipeline_verdrahtet():
    """Eine gruene Suite beweist nichts ueber ungerufenen Code."""
    quelle = (BACKEND / "main.py").read_text(encoding="utf-8")
    assert "apply_vage_menge_cap(synthesis, claim, lang)" in quelle
