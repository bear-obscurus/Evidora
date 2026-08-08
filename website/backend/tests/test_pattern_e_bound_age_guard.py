"""Pattern E: Schranken- und Alters-Angaben sind keine Messwerte (QA50D #316).

Live-Befund 2026-08-08 (Prod 3718f03), zweimal reproduziert: Der Claim
"In Österreich liegt die Jugendarbeitslosigkeit über 20 Prozent" ist FALSCH,
das LLM lieferte in allen sechs Läufen korrekt ``false`` — und Pattern E
invertierte es zweimal auf ``true`` @0.95:

    Factual-content consistency: threshold claim 'über 20.0' confirmed
    by summary value 25.0 (verdict was 'false')

Die "25" stammte aus "die Jugendarbeitslosigkeit (unter 25)", die "24" aus
"(15-24 Jahre)" — ein Schranken- bzw. ein Alters-Qualifikator, kein Messwert.
Beide Summaries enden wörtlich mit einer expliziten Widerlegung
("Keine Quelle bestätigt Werte über 20 %"), die der Override überstimmte.

Pattern H kennt beide Guards seit QA50B (Alters-Skip bei der Schwellen-Wahl,
Schranken-Präfix-Ausschluss in ``_entity_percent_from_summary``); nach E
wurden sie nie portiert — und E läuft VOR H, ist also der effektive Guard.

Dependency-light: reine Kaskaden-Tests, kein Netz/LLM.
"""

import pytest

from services.verdict_postprocess import (
    apply_verdict_postprocessing,
    _is_bound_or_age,
)

CLAIM = "In Österreich liegt die Jugendarbeitslosigkeit über 20 Prozent"


def _run(claim, verdict, summary, confidence=0.95):
    result = {"verdict": verdict, "confidence": confidence,
              "summary": summary}
    return apply_verdict_postprocessing(result, [], claim)


# --- Die zwei echten Live-Summaries ---

def test_live_hauptlauf_schranke_unter_25_kippt_nicht():
    """Wörtliche Summary aus dem QA50D-Hauptlauf (19:52:50 UTC)."""
    r = _run(
        CLAIM, "false",
        "Die Jugendarbeitslosenquote in Österreich lag 2025 nach "
        "ILO-Methodik bei 11,5 % (AT-Durchschnitt) und in Wien als "
        "höchstem Bundesland bei 17,7 %. Die AMS-Methodik (registrierte "
        "Arbeitslose) zeigt für 2024 eine Quote von 7,0 % (Gesamt), wobei "
        "die Jugendarbeitslosigkeit (unter 25) bei 32.037 Personen lag, "
        "aber keine Quote über 20 % erreicht.")
    assert r["verdict"] == "false", r


def test_live_runde3_altersangabe_15_24_jahre_kippt_nicht():
    """Wörtliche Summary aus Re-Run-Runde 3 (20:15:46 UTC)."""
    r = _run(
        CLAIM, "false",
        "Die Jugendarbeitslosenquote in Österreich liegt laut AMS-Methodik "
        "2024 bei 7,0 % (Jahresdurchschnitt) und nach ILO-Methodik 2025 bei "
        "11,5 % (15-24 Jahre). Selbst in Wien, dem Bundesland mit der "
        "höchsten Quote, beträgt sie 17,7 % (ILO, 2025). Keine Quelle "
        "bestätigt Werte über 20 %.")
    assert r["verdict"] == "false", r


def test_live_runde1_ohne_altersangabe_bleibt_false():
    """Kontrolle: die Runden, in denen E ohnehin schwieg, bleiben stabil."""
    r = _run(
        CLAIM, "false",
        "Die Jugendarbeitslosenquote in Österreich liegt laut Eurostat "
        "(2026) bei 8,0-9,9 % und nach Statistik Austria (2025, ILO) bei "
        "11,5 % (Bundesland Wien: 17,7 %). Keine Quelle bestätigt Werte "
        "über 20 %.")
    assert r["verdict"] == "false", r


# --- GEGENRICHTUNG: Pattern E muss weiter bestätigen können ---

def test_e_bestaetigt_weiterhin_echten_punktwert():
    """Der Original-Zweck von E (Bug #79): Summary nennt einen echten
    Wert über der Schwelle → korrektes true. Darf nicht kaputtgehen."""
    r = _run(
        "Das KlimaTicket kostet über 1000 Euro", "false",
        "Das KlimaTicket Österreich kostet im Vollpreis 1.095 € pro Jahr.")
    assert r["verdict"] == "true", r


def test_e_bestaetigt_bei_altersangabe_UNTER_der_schwelle_trotzdem():
    """Eine Altersangabe darf den Wert daneben nicht mit-entwerten:
    der echte Messwert bestätigt weiter."""
    r = _run(
        "In Österreich liegt die Jugendarbeitslosigkeit über 10 Prozent",
        "false",
        "Die Jugendarbeitslosenquote (15-24 Jahre) lag 2025 bei 11,5 %.")
    assert r["verdict"] == "true", r


# --- Der Helfer isoliert ---

@pytest.mark.parametrize("text,frag", [
    ("die jugendarbeitslosigkeit (unter 25) bei 32.037 personen", "25"),
    ("werte von unter 20 prozent", "20"),
    ("bis zu 30 prozent der haushalte", "30"),
    ("maximal 50 euro", "50"),
    ("höchstens 12 monate", "12"),
    ("nach ilo-methodik 2025 bei 11,5 % (15-24 jahre)", "24"),
    ("die 25-jährigen sind betroffen", "25"),
    ("kinder unter 6 jahren", "6"),
])
def test_helper_erkennt_schranke_oder_alter(text, frag):
    start = text.index(frag)
    assert _is_bound_or_age(text, start, start + len(frag)) is True, text


@pytest.mark.parametrize("text,frag", [
    ("das klimaticket kostet 1.095 € pro jahr", "1.095"),
    ("die quote lag bei 11,5 % im jahr 2025", "11,5"),
    ("insgesamt 32.037 personen waren betroffen", "32.037"),
    ("der anteil beträgt 17,7 prozent", "17,7"),
])
def test_helper_laesst_echte_messwerte_durch(text, frag):
    start = text.index(frag)
    assert _is_bound_or_age(text, start, start + len(frag)) is False, text


# --- Widerlegungs-Formen ("keine ... über X") ---

@pytest.mark.parametrize("summary", [
    "Die Quote lag bei 11,5 %. Keine Quelle bestätigt Werte über 20 %.",
    "Die Quote lag bei 11,5 %, aber keine Quote über 20 % erreicht.",
    "Die Quote lag bei 11,5 %; kein Bundesland liegt über 20 %.",
    "Die Quote liegt deutlich unter 20 %.",
    "Die Quote liegt nicht über 20 %.",
])
def test_widerlegungs_formen_verhindern_bestaetigung(summary):
    r = _run(CLAIM, "false", summary)
    assert r["verdict"] == "false", (summary, r)


def test_dezimal_grenze_widerlegt_nicht_faelschlich():
    """Pattern-F-Lehre: `\\b` beendet eine Zahl nicht. 'unter 20,5' darf
    die Schwelle '20' NICHT als widerlegt gelten lassen — 20,5 > 20."""
    r = _run(
        "In Wien liegt die Jugendarbeitslosigkeit über 20 Prozent", "false",
        "Die Quote in Wien lag 2025 knapp unter 20,5 %, konkret bei 20,4 %.")
    assert r["verdict"] == "true", r
