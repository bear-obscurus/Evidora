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


# ---------------------------------------------------------------------------
# Pack-Direktiven-Floor (50-Claim-QA 2026-07-09)
# ---------------------------------------------------------------------------
from services.confidence_calibration import (  # noqa: E402
    extract_pack_directive_floor,
)

# Reale kernsatz-Formate aus den kuratierten Packs:
_HOLOCAUST_TEXT = ("VERDICT-DIREKTIVE: Verdict false bei 0.95 Konfidenz — "
                   "KEINE falsche Ausgewogenheit …")
_KURZ_TEXT = ("VERDICT-LEITLINIE: … 'rechtskräftig verurteilt' = FALSCH "
              "(false, Konfidenz 0.85-0.9), weil …")
_STEUER_TEXT = ("VERDICT-LEITLINIE: Der Claim ist MIXED/teils-teils "
                "(mixed@~0.6), weil er von der Bezugsgröße abhängt.")
_ORF_TEXT = ("VERDICT-LEITLINIE: (a) 'über GIS' = GRÖSSTENTEILS FALSCH "
             "(mostly_false, Konfidenz 0.85-0.9) — …")


def _src(name, text):
    return [{"source": name, "results": [{"display_value": text}]}]


def test_floor_parses_real_directive_formats():
    cases = [
        (_HOLOCAUST_TEXT, "false", 0.95),
        (_KURZ_TEXT, "false", 0.85),          # Range → untere Grenze
        (_STEUER_TEXT, "mixed", 0.6),
        (_ORF_TEXT, "mostly_false", 0.85),
    ]
    for text, verdict, expected in cases:
        floor = extract_pack_directive_floor(
            _src("Geschichts-Faktencheck (DÖW + USHMM)", text), verdict)
        assert floor == expected, f"{verdict}: {floor} != {expected}"


def test_floor_requires_label_match():
    # Direktive sagt false — finales Verdict mixed (z.B. Differenzierungs-
    # Klausel griff) → KEIN Floor
    assert extract_pack_directive_floor(
        _src("Geschichts-Faktencheck", _HOLOCAUST_TEXT), "mixed") is None


def test_floor_requires_authoritative_pack_source():
    # gleiche Direktive aus einer Nicht-Pack-Quelle → KEIN Floor
    assert extract_pack_directive_floor(
        _src("PubMed", _HOLOCAUST_TEXT), "false") is None


def test_floor_never_for_unverifiable_and_needs_verdict_word():
    assert extract_pack_directive_floor(
        _src("Geschichts-Faktencheck", _HOLOCAUST_TEXT), "unverifiable") is None
    # Text ohne 'verdict'-Kennung → kein Floor (Schutz vor Zufalls-Zahlen)
    no_kw = _src("Geschichts-Faktencheck", "false bei 0.95 Konfidenz ohne Kennung")
    assert extract_pack_directive_floor(no_kw, "false") is None


def test_floor_lifts_capped_holocaust_case():
    """End-to-End des Live-Falls: LLM 0.8, Pack, weak evidence → ohne Floor
    bleibt 0.8; mit Direktiven-Floor 0.95 → 0.95."""
    kwargs = dict(
        raw_conf=0.8,
        source_coverage={"with_results": 2},
        evidence=_WEAK_EVIDENCE,
        sources_used=["Geschichts-Faktencheck (DÖW + USHMM + bpb)", "Wikipedia"],
        claim="Der Holocaust hat so nie stattgefunden.",
    )
    without, _ = calibrate_confidence(**kwargs)
    assert without == 0.8
    with_floor, dbg = calibrate_confidence(**kwargs, directive_floor=0.95)
    assert with_floor == 0.95
    assert dbg["floor_applied"] is True


def test_floor_lifts_incoherent_low_false():
    """Schattenregierungs-Fall: LLM false@0.15 trotz Klarfall-Pack —
    Floor hebt auch im early-return-Pfad."""
    lifted, dbg = calibrate_confidence(
        raw_conf=0.15, source_coverage={"with_results": 2},
        evidence=[], sources_used=["Verschwoerungen-Faktencheck (BVerfG)"],
        claim="x", directive_floor=0.85,
    )
    assert lifted == 0.85 and dbg["floor_applied"] is True


def test_floor_never_lowers():
    val, dbg = calibrate_confidence(
        raw_conf=0.92, source_coverage={"with_results": 5},
        evidence=[{"strength": "strong"}, {"strength": "strong"}],
        sources_used=[_PACK_SRC], claim="x", directive_floor=0.6,
    )
    assert val == 0.92 and dbg["floor_applied"] is False


def test_marker_drift_fixed_for_real_source_names():
    """Marker-Drift 2026-07-09: die realen Service-Source-Namen von
    geschichte/verschwoerungen/at_courts matchen jetzt die Marker."""
    from services.confidence_calibration import _has_authoritative_pack
    assert _has_authoritative_pack(["Geschichts-Faktencheck (DÖW + USHMM + bpb + Konsens)"])
    assert _has_authoritative_pack(["Verschwoerungen-Faktencheck (BVerfG + Verfassungsschutz)"])
    assert _has_authoritative_pack(["VfGH + VwGH Schlüsselerkenntnisse"])
