"""Tests für den pack-bewussten Evidence-Strength-Cap (Live-Verif 2026-07-07).

Befund (Antisemitismus): Der Synthesizer lieferte korrekt raw_conf 0.95 (die
Pack-Verdict-Direktive false@0.9 wirkte), aber calibrate_confidence deckelte
auf 0.70 — der evidence_strength-Cap war pack-UNbewusst und wertete die
LLM-„weak"-getaggten Pack-Zeilen ab, obwohl ein AUTHORITATIVE-Pack feuerte.

Fix: _evidence_strength_cap ist jetzt pack-bewusst (milder bei has_pack),
analog zu SOURCE_COUNT_CAPS_WITH_PACK. Diese Tests pinnen, dass (a) der
Antisemitismus-Fall durchkommt und (b) ohne Pack alles wie bisher deckelt.

Dependency-light: reine Rechenlogik, kein Netz/LLM.
"""

from services.confidence_calibration import (
    _evidence_strength_cap,
    calibrate_confidence,
)

# Ein AUTHORITATIVE-Pack-Quellenname (muss einen Marker aus
# AUTHORITATIVE_PACK_MARKERS enthalten — "Konsens" ist einer davon).
_PACK_SRC = "Religionsgemeinschaften-Konsens (ADL + IHRA + RIAS)"
_WEAK_EVIDENCE = [{"strength": "weak"}, {"strength": "weak"}]


def test_weak_evidence_cap_relaxed_with_pack():
    assert _evidence_strength_cap(_WEAK_EVIDENCE, has_pack=True) == 0.90
    assert _evidence_strength_cap(_WEAK_EVIDENCE, has_pack=False) == 0.70


def test_empty_evidence_cap_relaxed_with_pack():
    assert _evidence_strength_cap([], has_pack=True) == 0.85
    assert _evidence_strength_cap([], has_pack=False) == 0.70


def test_two_plus_moderate_no_cap_with_pack():
    ev = [{"strength": "moderate"}, {"strength": "moderate"}]
    assert _evidence_strength_cap(ev, has_pack=True) is None
    assert _evidence_strength_cap(ev, has_pack=False) == 0.85


def test_antisemitism_case_end_to_end():
    """Der reale Live-Fall: 5 Quellen mit Treffern, Pack gefeuert, weak
    evidence, raw 0.95 → soll bei 0.90 landen (nicht 0.70)."""
    calibrated, dbg = calibrate_confidence(
        raw_conf=0.95,
        source_coverage={"with_results": 5},
        evidence=_WEAK_EVIDENCE,
        sources_used=[_PACK_SRC, "DataCommons ClaimReview", "EUvsDisinfo",
                      "GDELT v2 GKG", "Wikipedia"],
        claim="Juden sind schuld an der Weltverschwörung",
    )
    assert dbg["authoritative_pack"] is True
    assert dbg["cap_evidence_strength"] == 0.90
    assert calibrated == 0.90, f"erwartet 0.90, war {calibrated}"


def test_no_pack_still_caps_weak_evidence():
    """Regressionsschutz: OHNE Pack bleibt der strenge 0.70-Cap."""
    calibrated, dbg = calibrate_confidence(
        raw_conf=0.95,
        source_coverage={"with_results": 5},
        evidence=_WEAK_EVIDENCE,
        sources_used=["PubMed", "Semantic Scholar", "OpenAlex"],
        claim="Irgendeine wissenschaftliche Behauptung",
    )
    assert dbg["authoritative_pack"] is False
    assert calibrated == 0.70


def test_low_confidence_unverifiable_untouched():
    calibrated, _ = calibrate_confidence(
        raw_conf=0.10, source_coverage={"with_results": 5},
        evidence=_WEAK_EVIDENCE, sources_used=[_PACK_SRC],
        claim="x",
    )
    assert calibrated == 0.10
