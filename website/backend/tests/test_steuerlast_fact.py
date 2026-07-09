"""Tests für den 🔵-Opus-kuratierten Steuerlast-Fakt
(50-Claim-Test 2026-07-09, Befund #35).

Befund: „Reiche zahlen prozentual weniger Steuern als die Mittelschicht"
→ unverifiable@0.1 trotz gut belegter, differenzierter Faktenlage. Ideal:
mixed — Erwerbseinkommen progressiv (Claim falsch) vs. effektive Gesamtlast
der Superreichen regressiv (Claim zutreffend; Momentum/Oxfam 2024: ~26 %
Milliardär vs. ~42 % Durchschnittsfamilie).
"""

import json
import os

import pytest

from services._topic_match import substring_or_composite_match

_DATA = os.path.join(
    os.path.dirname(__file__), "..", "data", "wirtschaftspolitik_pack.json"
)


def _fact():
    d = json.load(open(_DATA, encoding="utf-8"))
    for f in d["facts"]:
        if f["id"] == "effektive_steuerlast_reiche_mittelschicht_2026":
            return f
    raise AssertionError("Steuerlast-Fakt fehlt")


_F = _fact()


@pytest.mark.parametrize("claim", [
    "Reiche zahlen prozentual weniger Steuern als die Mittelschicht",
    "Superreiche zahlen weniger Steuern als normale Arbeitnehmer",
    "Milliardäre zahlen kaum Steuern",
    "Vermögende zahlen anteilig weniger als der Mittelstand",
    "Wer arbeitet zahlt mehr als wer besitzt",
])
def test_trigger_fires(claim):
    assert substring_or_composite_match(_F, claim.lower()), claim


@pytest.mark.parametrize("claim", [
    "Die Steuern sind letztes Jahr gestiegen",          # kein Subjekt-Marker
    "Reiche Ernte auf den Feldern in Niederösterreich",  # 'reiche' ohne Steuer
    "Der Millionär spendete für das Krankenhaus",        # Subjekt ohne Steuer
])
def test_trigger_no_false_positives(claim):
    assert not substring_or_composite_match(_F, claim.lower()), claim


def test_kernsatz_mixed_directive_and_metric_separation():
    ks = _F["data"]["kernsatz_fuer_synthesizer"]
    assert "MIXED" in ks and "mixed@~0.6" in ks
    # beide Seiten der Differenzierung
    assert "PROGRESSIV" in ks and "47,0 %" in ks       # OECD-Abgabenkeil
    assert "~26 %" in ks and "~42 %" in ks              # Momentum-Zahlen
    # Metriken-Trennung (Einkommen vs. Vermögen)
    assert "GEMESSEN AM VERMÖGEN" in ks
    assert "NICHT vermischen" in ks or "nicht vermischen" in ks.lower()
    # Guardrails
    assert "KEINE steuerpolitische Empfehlung" in ks
    assert "KEINE Partei-Zuordnung" in ks


def test_sources_verified():
    assert "momentum-institut.at" in _F["source_url"]
    assert "taxobservatory" in _F["secondary_url"]
