"""Tests für den Zyrtec/Cetirizin-Schläfrigkeit-Nuance-Fakt (2026-07-11).

Befund (50-Claim-QA #5, Rest-Delta): Nach dem Antihistaminika-Marken→INN-Fix
(#69) feuerten die Live-Quellen für „Macht Zyrtec schläfrig?", aber der
Synthesizer schwankte über drei Läufe zwischen mixed@0.75 / unverifiable@0.1 /
mostly_false@0.85 — je nach RSS-/PubMed-Lotterie. Ehrliches Verdict ist
teils-teils: Cetirizin ist 2. Generation (sediert viel weniger als alte
Antihistaminika, viele merken nichts), gilt aber als das sedierendste seiner
Klasse (~14 % vs. ~6 % Placebo, Auto-fahr-Hinweis). Kuratierter Fakt mit
exaktem Trigger stabilisiert die Quelle + rahmt teils-teils.

Bewusst OHNE STRUKTURELL-FALSCH-Override-Token: der Marker biast Richtung
false und der L2-Override würde bei LLM-`mostly_true` sogar `mostly_false`
erzwingen (Fehlrichtung). Die teils-teils-Botschaft trägt die Headline
(truncation-sicher), die parsebare `mixed`-Direktive speist den Floor.

Dependency-light: JSON + Trigger, kein Netz/LLM.
"""

import json
import os

import pytest

from services._struct_marker import has_false_verdict_override
from services._topic_match import substring_or_composite_match

_DATA = os.path.join(
    os.path.dirname(__file__), "..", "data", "gesundheits_autoritaeten_pack.json"
)


def _fact() -> dict:
    d = json.load(open(_DATA, encoding="utf-8"))
    for f in d["facts"]:
        if f["id"] == "cetirizin_sedierung_konsens_2026":
            return f
    raise AssertionError("cetirizin_sedierung_konsens_2026 fehlt im Pack")


@pytest.mark.parametrize("claim", [
    "Macht Zyrtec schläfrig?",
    "Macht Cetirizin müde?",
    "Zyrtec macht schläfrig und müde",
    "Kann man mit Zyrtec Auto fahren?",
    "Wird man von Reactine benommen?",
    "Macht Cetirizin somnolent?",
    "Beeinträchtigt Zyrtec die Fahrtüchtigkeit?",
])
def test_trigger_fires(claim):
    assert substring_or_composite_match(_fact(), claim.lower()), claim


@pytest.mark.parametrize("claim", [
    "Ist Zyrtec rezeptfrei?",              # Marke ohne Sedierungs-Bezug
    "Hilft Cetirizin bei Heuschnupfen?",   # Wirkstoff ohne Sedierung
    "Wie schnell wirkt Zyrtec?",           # Marke, keine Müdigkeit
    "Macht Diphenhydramin müde?",          # anderer Wirkstoff (1. Gen)
])
def test_trigger_does_not_fire_off_topic(claim):
    assert not substring_or_composite_match(_fact(), claim.lower()), claim


def test_no_struct_false_override_token():
    """Kein Marker-Token — sonst würde der L2-Override Richtung mostly_false
    kippen. Ziel ist mixed, nicht false."""
    f = _fact()
    assert not has_false_verdict_override(f["data"]["kernsatz_fuer_synthesizer"])
    assert not has_false_verdict_override(f["headline"])


def test_headline_carries_teils_teils_message():
    """Die teils-teils-Botschaft muss in der Headline stehen — sie wird dem
    display_value vorangestellt und überlebt die 400-Zeichen-Truncation, im
    Gegensatz zu tief im data-Dict liegenden Feldern."""
    head = _fact()["headline"].lower()
    assert "teils-teils" in head
    assert "2. generation" in head
    assert "14 %" in head or "14%" in head
    assert "placebo" in head


def test_kernsatz_carries_parseable_mixed_directive():
    ks = _fact()["data"]["kernsatz_fuer_synthesizer"]
    assert "mixed" in ks.lower()
    assert "verdict" in ks.lower()
    # Vom Pack-Direktiven-Floor parsebar (confidence_calibration._DIRECTIVE_RE)
    from services.confidence_calibration import _DIRECTIVE_RE
    hits = [
        (m.group(1).lower(), m.group(2))
        for m in _DIRECTIVE_RE.finditer(ks)
    ]
    assert ("mixed", "0.6") in hits, hits


def test_source_urls_are_verified_not_fabricated():
    f = _fact()
    # Verifizierte MedlinePlus-Cetirizin-Seite (oral) + Du-2016-Meta-Analyse
    assert f["source_url"] == "https://medlineplus.gov/druginfo/meds/a623043.html"
    assert f["secondary_url"] == "https://pubmed.ncbi.nlm.nih.gov/26990040/"
