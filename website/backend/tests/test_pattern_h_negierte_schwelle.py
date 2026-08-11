"""Pattern H: negierte Schwelle kehrt die Richtung um (QA50D #311).

Live-Befund 2026-08-08 (Prod 3718f03): Claim "Die Inflation in Österreich
liegt NICHT über 10 Prozent" — sachlich WAHR (2,9 %). Die Summary nannte
die richtigen Werte ("…deutlich unter 10 %"), das Label sagte ``false``,
und Pattern H schwieg.

Wurzel: ``_h_confirm_ok`` schaltete bei JEDER Negation im Claim die
bestätigende Richtung komplett ab — der einzige Fall, in dem H helfen
könnte, war der einzige, den es nicht anfassen durfte.

Der Gegenbeweis zeigte, dass die Klasse SYMMETRISCH kaputt war: die
widerlegende Richtung trug gar kein Negations-Gate und feuerte
fälschlich in die Gegenrichtung. Vor dem Fix:
  "nicht über 10 %" + Werte 2,9 % → false  (falsch, soll true)
  "nicht über 2 %"  + Werte 2,9 % → true   (falsch, soll false)

Fix: nicht abschalten, sondern die Richtung UMKEHREN — aber nur bei
einer an die Schwelle GEBUNDENEN Negation (unmittelbar davor, max. 25
Zeichen). Eine ungebundene Negation irgendwo im Satz bleibt konservativ
behandelt (keine Umkehr, keine Bestätigung).

Dependency-light: reine Kaskaden-Tests, kein Netz/LLM.
"""
import pytest

from services.verdict_postprocess import apply_verdict_postprocessing

# Wörtliche Live-Summary aus dem QA50D-Hauptlauf (#311)
LIVE = ("Die aktuelle Inflationsrate in Österreich liegt laut Eurostat "
        "(2024: 2,9 %) und Statistik Austria (2024: 2,9 %) deutlich unter "
        "10 %. Auch die Prognose für 2025 (Eurostat: 3,6 %) bleibt weit "
        "darunter.")


def _run(claim, verdict, summary, confidence=0.95):
    return apply_verdict_postprocessing(
        {"verdict": verdict, "confidence": confidence, "summary": summary},
        [], claim)


# --- Der Live-Fall, beide Richtungen ---

def test_live_311_negierte_schwelle_wird_bestaetigt():
    """Claim WAHR, Label falsch → H muss auf true korrigieren."""
    r = _run("Die Inflation in Österreich liegt nicht über 10 Prozent.",
             "false", LIVE)
    assert r["verdict"] == "true", r


def test_negierte_schwelle_wird_auch_widerlegt():
    """Gegenrichtung: Claim FALSCH (2,9 > 2), Label true → false.
    Vor dem Fix lieferte genau dieser Fall fälschlich 'true'."""
    r = _run("Die Inflation in Österreich liegt nicht über 2 Prozent.",
             "true", LIVE)
    assert r["verdict"] == "false", r


@pytest.mark.parametrize("claim,label,erwartet", [
    # "nicht unter X" == "mindestens X"
    ("Die Inflation in Österreich liegt nicht unter 2 Prozent.",
     "false", "true"),
    ("Die Inflation in Österreich liegt nicht unter 10 Prozent.",
     "true", "false"),
    # weitere gebundene Negationsformen
    ("Die Inflation in Österreich liegt keineswegs über 10 Prozent.",
     "false", "true"),
    ("Die Inflation in Österreich liegt niemals über 10 Prozent.",
     "false", "true"),
])
def test_gebundene_negationsformen(claim, label, erwartet):
    r = _run(claim, label, LIVE)
    assert r["verdict"] == erwartet, (claim, r)


# --- Nicht-negiert: unveraendertes Verhalten (QA50B-Schutz) ---

def test_nicht_negiert_widerlegend_unveraendert():
    r = _run("Die Inflation in Österreich liegt über 10 Prozent.",
             "true", LIVE)
    assert r["verdict"] == "false", r


def test_nicht_negiert_bestaetigend_unveraendert():
    r = _run("Die Inflation in Österreich liegt unter 10 Prozent.",
             "false", LIVE)
    assert r["verdict"] == "true", r


# --- Konservativ: UNGEBUNDENE Negation ---

def test_ungebundene_negation_kehrt_nicht_um():
    """'nicht' gehört zum Nebensatz, nicht zur Schwelle — H darf weder
    umkehren noch bestätigen (Verhalten wie vor dem Fix)."""
    r = _run("Die Inflation, die nicht überraschend kam, liegt über 10 "
             "Prozent.", "false", LIVE)
    assert r["verdict"] == "false", r


def test_ungebundene_negation_blockt_bestaetigung_weiterhin():
    r = _run("Die Inflation ist kein Problem und liegt unter 10 Prozent.",
             "false", LIVE)
    assert r["verdict"] == "false", r


def test_negation_zu_weit_weg_zaehlt_als_ungebunden():
    """>25 Zeichen Abstand → keine Bindung, konservativ."""
    r = _run("Die Inflation ist nicht das, worüber alle reden, sie liegt "
             "über 10 Prozent.", "false", LIVE)
    assert r["verdict"] == "false", r


# --- Grenzfall-Schutz bleibt ---

def test_toleranz_grenzfall_wird_nicht_bewertet():
    """0,5-%-Toleranz gilt auch bei negierter Schwelle."""
    r = _run("Österreich hat nicht mehr als 9,2 Millionen Einwohner.",
             "true",
             "Österreich hatte am 1.1.2025 genau 9.197.213 Einwohner.")
    assert r["verdict"] == "true", r


def test_schlussformel_sperre_bleibt_bestehen():
    """#309 ist bewusst NICHT gefixt: eine explizite Konklusion in der
    Summary sperrt H weiterhin. Dieser Test pinnt die Entscheidung,
    damit sie nicht versehentlich aufgeweicht wird."""
    r = _run("Sind wir in Österreich eigentlich schon über 9 Millionen "
             "Leute?", "false",
             "Die aktuelle Bevölkerung Österreichs liegt laut Eurostat am "
             "1. Januar 2025 bei 9.197.213 Personen, also unter 9 "
             "Millionen. Die Behauptung, Österreich sei bereits über 9 "
             "Millionen Einwohner, ist damit falsch.")
    assert r["verdict"] == "false", r
