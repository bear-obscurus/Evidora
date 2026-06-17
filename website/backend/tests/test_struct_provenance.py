"""Bug #47 Wurzel-Fix (Safe-Subset) — Provenance-Gating für STRUKTURELL-Marker.

Deterministische Unit-Tests (LLM-frei) für die zwei Kernstücke:
  1. ``_struct_marker.render_data_with_marker`` — emittiert je nach
     ``data._matched_exact`` den exakten oder den auflösbaren Cosine-Prefix.
  2. ``reranker.resolve_struct_marker_provenance`` — degradiert Cosine-Marker
     zu Klartext, WENN ein exakter Anker existiert; sonst restauriert er sie
     zu normalem ``STRUKTURELL FALSCH:``.

Hintergrund: off-topic Pack-Einträge werden via Cosine-Backup (≥0.45) in den
Prompt gezogen und tragen fälschlich STRUKTURELL-Marker (gemessen: Krone-Claim
= 1 exakter + 6 cosine-Kontaminations-Marker). Der exakte Trigger vs.
Cosine-Backup ist der kategoriale Diskriminator.
"""

import copy

from services._struct_marker import (
    render_data_with_marker,
    STRUCT_EXACT_PREFIX,
    STRUCT_COSINE_PREFIX,
)
from services.reranker import resolve_struct_marker_provenance


# Ein kernsatz, der has_false_verdict_override() triggert (enthält
# "VERDICT-LEITLINIE" aus FALSE_VERDICT_OVERRIDE_TOKENS).
_OVERRIDE_KERNSATZ = "VERDICT-LEITLINIE: Die Behauptung ist widerlegt."


class TestRenderProvenancePrefix:
    def test_default_no_flag_is_exact(self):
        # Aufrufer ohne find_matching_items (z.B. Wikidata) → exakt-Prefix
        out = render_data_with_marker({"kernsatz_fuer_synthesizer": _OVERRIDE_KERNSATZ})
        assert out.startswith(STRUCT_EXACT_PREFIX)

    def test_matched_exact_true_is_exact(self):
        out = render_data_with_marker(
            {"kernsatz_fuer_synthesizer": _OVERRIDE_KERNSATZ, "_matched_exact": True})
        assert out.startswith(STRUCT_EXACT_PREFIX)
        assert STRUCT_COSINE_PREFIX not in out

    def test_matched_cosine_false_is_cosine_variant(self):
        out = render_data_with_marker(
            {"kernsatz_fuer_synthesizer": _OVERRIDE_KERNSATZ, "_matched_exact": False})
        assert out.startswith(STRUCT_COSINE_PREFIX)
        # KRITISCH: darf den autoritativen Substring NICHT enthalten
        assert "STRUKTURELL FALSCH:" not in out

    def test_no_override_no_prefix(self):
        out = render_data_with_marker(
            {"kernsatz_fuer_synthesizer": "Neutrale Erläuterung ohne Override.",
             "_matched_exact": False, "wert": "42 Prozent"})
        assert STRUCT_EXACT_PREFIX not in out
        assert STRUCT_COSINE_PREFIX not in out
        assert "42 Prozent" in out

    def test_flag_never_rendered(self):
        out = render_data_with_marker(
            {"kernsatz_fuer_synthesizer": _OVERRIDE_KERNSATZ,
             "_matched_exact": False, "wert": "x"})
        assert "_matched_exact" not in out
        assert "matched exact" not in out.lower()


def _src(*display_values, source="Pack"):
    return {"source": source, "results": [{"display_value": dv} for dv in display_values]}


_EXACT = "STRUKTURELL FALSCH: durch exakten Trigger widerlegt."
_COSINE = "STRUKTURELL_COSINE_FALSCH: nur thematisch nah."


class TestResolveStructProvenance:
    def test_no_cosine_is_noop(self):
        src = [_src(_EXACT, "normaler treffer")]
        before = copy.deepcopy(src)
        resolve_struct_marker_provenance(src)
        assert src == before

    def test_exact_present_downgrades_cosine(self):
        # Krone-Signatur: 1 exakt + viele cosine → cosine zu Klartext
        src = [_src(_EXACT), _src(_COSINE), _src(_COSINE)]
        resolve_struct_marker_provenance(src)
        dvs = [r["display_value"] for sd in src for r in sd["results"]]
        # exakter Marker bleibt
        assert any("STRUKTURELL FALSCH:" in d for d in dvs)
        # cosine-Marker entschärft: KEIN strukturelles Signal mehr
        cosine_dvs = [d for d in dvs if "thematisch nah" in d]
        assert len(cosine_dvs) == 2
        for d in cosine_dvs:
            assert "STRUKTURELL" not in d          # Prefix komplett weg
            assert "thematisch nah" in d           # Inhalt bleibt als Info

    def test_no_exact_restores_cosine(self):
        # Kurkuma-Signatur: 0 exakt + nur cosine → zu normalem Marker
        src = [_src(_COSINE), _src(_COSINE)]
        resolve_struct_marker_provenance(src)
        dvs = [r["display_value"] for sd in src for r in sd["results"]]
        for d in dvs:
            assert d.startswith("STRUKTURELL FALSCH:")
            assert STRUCT_COSINE_PREFIX not in d

    def test_krone_scenario_1exact_6cosine(self):
        src = [_src(_EXACT)] + [_src(_COSINE) for _ in range(6)]
        resolve_struct_marker_provenance(src)
        dvs = [r["display_value"] for sd in src for r in sd["results"]]
        exact_markers = [d for d in dvs if "STRUKTURELL FALSCH:" in d]
        # Nur der eine exakte überlebt als autoritativer Marker
        assert len(exact_markers) == 1

    def test_handles_nondict_and_missing_results(self):
        src = ["garbage", {"source": "X"}, _src(_COSINE)]
        # darf nicht crashen; ohne exakten Anker → restauriert
        resolve_struct_marker_provenance(src)
        assert src[2]["results"][0]["display_value"].startswith("STRUKTURELL FALSCH:")


class TestRenderResolveIntegration:
    def test_cosine_marker_downgraded_when_exact_anchor_present(self):
        # Render einen exakten + einen cosine-Marker, dann auflösen
        exact_dv = render_data_with_marker(
            {"kernsatz_fuer_synthesizer": _OVERRIDE_KERNSATZ, "_matched_exact": True})
        cosine_dv = render_data_with_marker(
            {"kernsatz_fuer_synthesizer": _OVERRIDE_KERNSATZ, "_matched_exact": False})
        src = [_src(exact_dv), _src(cosine_dv)]
        resolve_struct_marker_provenance(src)
        out_exact = src[0]["results"][0]["display_value"]
        out_cosine = src[1]["results"][0]["display_value"]
        assert out_exact.startswith("STRUKTURELL FALSCH:")
        assert "STRUKTURELL" not in out_cosine     # entschärft
