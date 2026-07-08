"""Tests für den Voltadol/Diclofenac-Nieren-Nuance-Fakt (Live-Verif 2026-07-07).

Befund: Nach dem Marken→INN-Fix (#58) feuerten die Fachquellen für „Voltadol
schädigt die Nieren", aber der Synthesizer machte daraus ein pauschales
`true @ 0.9` — ohne die entscheidende Gel-vs-Tabletten-Nuance (Voltadol ist
in AT primär das Gel mit ~6 % Resorption; das reale AKI-Risiko betrifft v. a.
die orale Form). Kuratierter Fakt liefert die Nuance → mixed.

Dependency-light: JSON + Trigger, kein Netz/LLM.
"""

import json
import os

import pytest

from services._topic_match import substring_or_composite_match

_DATA = os.path.join(
    os.path.dirname(__file__), "..", "data", "gesundheits_autoritaeten_pack.json"
)


def _fact() -> dict:
    d = json.load(open(_DATA, encoding="utf-8"))
    for f in d["facts"]:
        if f["id"] == "diclofenac_niere_konsens_2026":
            return f
    raise AssertionError("diclofenac_niere_konsens_2026 fehlt im Pack")


@pytest.mark.parametrize("claim", [
    "Voltadol schädigt die Nieren",
    "Ist Diclofenac schlecht für die Nieren?",
    "Voltaren Gel schadet den Nieren",
    "Kann Voltadol zu Nierenversagen führen?",
])
def test_trigger_fires(claim):
    assert substring_or_composite_match(_fact(), claim.lower()), claim


@pytest.mark.parametrize("claim", [
    "Voltadol hilft gegen Rückenschmerzen",   # Marke ohne Nieren-Bezug
    "Meine Nieren tun weh",                    # Nieren ohne Wirkstoff
    "Diclofenac gegen Entzündungen",          # Wirkstoff ohne Nieren
])
def test_trigger_does_not_fire_off_topic(claim):
    assert not substring_or_composite_match(_fact(), claim.lower()), claim


def test_kernsatz_carries_mixed_directive_and_dosage_nuance():
    ks = _fact()["data"]["kernsatz_fuer_synthesizer"]
    # MIXED-Leitlinie statt pauschal true
    assert "mixed" in ks.lower()
    assert "teils-teils" in ks.lower()
    # Die entscheidende Darreichungsform-Differenzierung
    assert "topisch" in ks.lower() and "oral" in ks.lower()
    assert "6 %" in ks or "6%" in ks  # Gel-Resorption
    assert "triple whammy" in ks.lower()


def test_fact_has_both_dosage_forms_documented():
    data = _fact()["data"]
    assert data.get("systemisch_oral"), "orale Form fehlt"
    assert data.get("topisch_gel"), "topische Form fehlt"


def test_source_url_is_topical_not_fabricated():
    # Quelle muss die verifizierte MedlinePlus-Diclofenac-Seite sein
    assert _fact()["source_url"] == "https://medlineplus.gov/druginfo/meds/a689002.html"
