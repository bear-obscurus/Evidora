"""Tests für den deskriptiven Volkskanzler-Fakt (Live-Verif 2026-07-07).

Befund (Kickl): „Herbert Kickl hat sich als Volkskanzler bezeichnen lassen"
blieb „nicht überprüfbar", weil der Wikipedia-Service nur den Artikel-SUMMARY
des Lemmas „Volkskanzler" abruft (NS-Begriffsgeschichte) — die Kickl-Passage
steht im Artikel-Body und wird nie abgerufen. Kein Retrieval-Fix möglich →
kuratierter at_factbook-Fakt.

Guardrail-Pflicht (project_political_guardrails.md): Der Fakt ist rein
deskriptiv (dokumentierter Sprachgebrauch), enthält KEINE Partei-Bewertung
und KEINE eigene Links-Rechts-Einstufung. Diese Tests pinnen das mit.

Dependency-light: JSON + Trigger + Builder, kein Netz/LLM.
"""

import json
import os

import pytest

from services.at_factbook import (
    _build_volkskanzler_results,
    _claim_matches_any_topic,
    _claim_mentions_volkskanzler,
)

_DATA = os.path.join(os.path.dirname(__file__), "..", "data", "at_factbook.json")


def _fact() -> dict:
    d = json.load(open(_DATA, encoding="utf-8"))
    for f in d["facts"]:
        if f["id"] == "volkskanzler_kickl_at":
            return f
    raise AssertionError("volkskanzler_kickl_at fehlt in at_factbook.json")


# --- Trigger -----------------------------------------------------------------
@pytest.mark.parametrize("claim", [
    "Herbert Kickl hat sich als Volkskanzler bezeichnen lassen",
    "Kickl nennt sich selbst Volkskanzler",
    "Die FPÖ plakatiert Kickl als Volkskanzler",
    "War der Volkskanzler-Begriff Teil des freiheitlichen Wahlkampfs?",
])
def test_trigger_fires_on_kickl_volkskanzler(claim):
    assert _claim_mentions_volkskanzler(claim.lower()), claim
    assert "volkskanzler_kickl_at" in _claim_matches_any_topic(claim)


@pytest.mark.parametrize("claim", [
    # Volkskanzler OHNE Kickl-/FPÖ-Bezug → rein historisches Thema, kein Match
    "Hitler wurde in der NS-Propaganda als Volkskanzler bezeichnet",
    # Kickl OHNE Volkskanzler → anderes Thema
    "Herbert Kickl war Innenminister",
    "Der Bundeskanzler von Österreich heißt Stocker",
])
def test_trigger_does_not_fire_off_topic(claim):
    assert not _claim_mentions_volkskanzler(claim.lower()), claim


# --- Builder -----------------------------------------------------------------
def test_builder_output_descriptive_and_directive_separated():
    fact = _fact()
    results = _build_volkskanzler_results(fact, "kickl volkskanzler")
    assert len(results) == 1
    r = results[0]
    assert r["indicator"] == "factbook_volkskanzler"
    # display_value = user-sichtbarer Faktentext: deskriptiv, OHNE Direktive
    assert "Kernsatz fuer synthesizer" not in r["display_value"]
    assert "zutreffend" in r["display_value"].lower()
    assert "2023" in r["display_value"] or "2023" in str(r["year"])
    # Verdict-Direktive in der description (Prompt sieht sie; Export strippt
    # sie via _export_sanitize — dort getestet)
    assert "Kernsatz fuer synthesizer:" in r["description"]
    assert "true bei 0.9" in r["description"]


def test_export_sanitize_strips_directive_from_description():
    """Integrations-Pin: die Direktive in der description wird von der
    Export-Sanitization entfernt, der deskriptive Kontext bleibt."""
    from services._export_sanitize import sanitize_sources_for_export
    fact = _fact()
    results = _build_volkskanzler_results(fact, "kickl volkskanzler")
    out = sanitize_sources_for_export([{"source": "AT Factbook", "results": results}])
    desc = out[0]["results"][0]["description"]
    assert "Kernsatz fuer synthesizer" not in desc
    assert "true bei 0.9" not in desc
    assert "NS-Propaganda" in desc  # deskriptiver Kontext bleibt


# --- Guardrails --------------------------------------------------------------
def test_fact_is_descriptive_no_party_rating():
    fact = _fact()
    blob = json.dumps(fact, ensure_ascii=False).lower()
    # Pflicht-Selbstverpflichtungen vorhanden
    assert fact.get("guardrails"), "guardrails-Feld fehlt"
    assert "deskriptiv" in blob
    # Keine Bewertungs-Vokabeln über die Partei (rechtsextrem/gefährlich/...)
    for verboten in ("rechtsextrem", "gefährlich", "radikal", "extremistisch"):
        assert verboten not in blob, f"Bewertungs-Vokabel '{verboten}' im Fakt"


# --- Wiring ------------------------------------------------------------------
def test_indicator_is_reranker_authoritative():
    from services.reranker import _AUTHORITATIVE_INDICATORS
    assert "factbook_volkskanzler" in _AUTHORITATIVE_INDICATORS


def test_at_factbook_counts_as_authoritative_pack():
    from services.confidence_calibration import _has_authoritative_pack
    assert _has_authoritative_pack(["AT Factbook"])
