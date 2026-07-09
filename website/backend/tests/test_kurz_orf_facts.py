"""Tests für die 🔵-Opus-kuratierten Fakten Kurz-Freispruch + ORF-Beitrag
(50-Claim-Test 2026-07-09, Fails #38 + #29).

#38: „Sebastian Kurz wurde rechtskräftig wegen Falschaussage verurteilt"
     → war unverifiable@0.1; real: LG-Ersturteil 2/2024, OLG-Freispruch
     5/2025, rechtskräftig → Claim ist FALSCH.
#29: „Der ORF finanziert sich über die GIS-Gebühr" → war true@0.88;
     real: ORF-Beitrag ersetzt GIS seit 1.1.2024 → größtenteils falsch.

Inhalte von Opus recherchiert + formuliert (URLs 200-verifiziert),
Trigger von Fable entschärft (kein nacktes 'kurz' im composite — Adverb-
Falle 'kurz nach/kurz gesagt').
"""

import json
import os

import pytest

from services._topic_match import substring_or_composite_match
from services.at_factbook import (
    _build_orf_finanzierung_results,
    _claim_matches_any_topic,
    _claim_mentions_orf_finanzierung,
)

_DATA = os.path.join(os.path.dirname(__file__), "..", "data")


def _ruling(rid):
    d = json.load(open(os.path.join(_DATA, "at_courts.json"), encoding="utf-8"))
    for r in d["rulings"]:
        if r["id"] == rid:
            return r
    raise AssertionError(f"{rid} fehlt in at_courts.json")


def _fact(fid):
    d = json.load(open(os.path.join(_DATA, "at_factbook.json"), encoding="utf-8"))
    for f in d["facts"]:
        if f["id"] == fid:
            return f
    raise AssertionError(f"{fid} fehlt in at_factbook.json")


_KURZ = _ruling("olg_wien_kurz_falschaussage_freispruch_2025")
_ORF = _fact("orf_beitrag_finanzierung_2024")


# --- Kurz: Trigger ------------------------------------------------------------
@pytest.mark.parametrize("claim", [
    "Sebastian Kurz wurde rechtskräftig wegen Falschaussage verurteilt",
    "Wurde Kurz wegen Falschaussage verurteilt?",
    "Kurz ist freigesprochen worden",
    "Sebastian Kurz und die ÖBAG-Aussage im Untersuchungsausschuss",
    "Hat Ex-Kanzler Kurz einen Meineid geleistet?",
])
def test_kurz_trigger_fires(claim):
    assert substring_or_composite_match(_KURZ, claim.lower()), claim


@pytest.mark.parametrize("claim", [
    # Adverb-Falle: 'kurz' als Zeitwort darf NICHT auf den Rechtsfakt laufen
    "Kurz nach der Wahl wurde das Gesetz beschlossen",
    "Die Verhandlung war kurz und wurde vertagt",
    "Kurz gesagt: die Steuern steigen",
    # Verurteilungs-Claims ohne Kurz-Bezug
    "Der Angeklagte wurde wegen Betrugs verurteilt",
])
def test_kurz_trigger_no_adverb_false_positives(claim):
    assert not substring_or_composite_match(_KURZ, claim.lower()), claim


def test_kurz_kerninhalt_directive():
    k = _KURZ["kerninhalt"]
    assert "rechtskräftig" in k and "Freispruch" in k
    assert "FALSCH" in k                      # (a) rechtskräftig verurteilt = falsch
    assert "erstinstanzlich" in k.lower() or "Erstinstanz" in k
    assert "26.05.2025" in k and "23.02.2024" in k
    # Neutralität: keine Bewertung von Person/Partei
    assert "Unschuldsvermutung" in k


# --- ORF: Trigger -------------------------------------------------------------
@pytest.mark.parametrize("claim", [
    "Der ORF finanziert sich über die GIS-Gebühr",
    "Wird der ORF über GIS finanziert?",
    "Wie hoch ist der ORF-Beitrag pro Monat in Euro?",
    "Die GIS wurde abgeschafft, oder?",
    "ORF finanziert sich über die Haushaltsabgabe",
])
def test_orf_trigger_fires(claim):
    assert _claim_mentions_orf_finanzierung(claim.lower()), claim
    assert "orf_finanzierung_at" in _claim_matches_any_topic(claim)


@pytest.mark.parametrize("claim", [
    "Der ORF berichtet über die Wahl",          # ORF ohne Finanzierungs-Bezug
    "Die Logistik-Firma GIS liefert pünktlich",  # GIS ohne Abschaffungs-Bezug
    "Der Mitgliedsbeitrag im Verein beträgt 20 Euro",
])
def test_orf_trigger_no_false_positives(claim):
    assert not _claim_mentions_orf_finanzierung(claim.lower()), claim


# --- ORF: Builder -------------------------------------------------------------
def test_orf_builder_separates_display_and_directive():
    results = _build_orf_finanzierung_results(_ORF, "orf gis")
    assert len(results) == 1
    r = results[0]
    assert r["indicator"] == "factbook_orf_finanzierung"
    assert "15,3" in r["display_value"]          # Basisbetrag user-sichtbar
    assert "Kernsatz fuer synthesizer" not in r["display_value"]
    assert "Kernsatz fuer synthesizer:" in r["description"]
    assert "GRÖSSTENTEILS FALSCH" in r["description"]  # Leitlinie im Prompt


def test_orf_export_sanitize_strips_directive():
    from services._export_sanitize import sanitize_sources_for_export
    results = _build_orf_finanzierung_results(_ORF, "orf gis")
    out = sanitize_sources_for_export([{"source": "AT Factbook", "results": results}])
    desc = out[0]["results"][0]["description"]
    assert "Kernsatz fuer synthesizer" not in desc
    assert "VfGH-Erkenntnis G 226/2022" in desc   # deskriptiver Kontext bleibt


# --- Wiring -------------------------------------------------------------------
def test_orf_indicator_is_reranker_authoritative():
    from services.reranker import _AUTHORITATIVE_INDICATORS
    assert "factbook_orf_finanzierung" in _AUTHORITATIVE_INDICATORS
