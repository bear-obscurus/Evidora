"""Tests für die Export-Sanitization (Audit 2026-07-07).

Befund: Im User-Export (PDF/SSE) stand wörtlich „Kernsatz fuer synthesizer:
WICHTIG: …" — ein interner Synthesizer-Hinweis. Diese Tests pinnen, dass
interne Marker aus den user-sichtbaren Quellen entfernt werden, faktische
Inhalte aber erhalten bleiben.

Dependency-light: reine String-/Dict-Logik.
"""

from services._export_sanitize import (
    _sanitize_text,
    sanitize_sources_for_export,
)
from services._struct_marker import STRUCT_EXACT_PREFIX


def test_kernsatz_label_segment_removed():
    s = ("Kernsatz fuer synthesizer: WICHTIG: das ist intern | "
         "Akademischer konsens: das ist faktisch und bleibt")
    out = _sanitize_text(s)
    assert "Kernsatz fuer synthesizer" not in out
    assert "WICHTIG: das ist intern" not in out
    assert "Akademischer konsens" in out
    assert "faktisch und bleibt" in out


def test_struktrurell_prefix_stripped_but_facts_kept():
    s = (f"{STRUCT_EXACT_PREFIX} WICHTIG für die Bewertung: interne Direktive | "
         "Fehlende belege: es gibt keine Belege | Quelle: bpb")
    out = _sanitize_text(s)
    assert "STRUKTURELL FALSCH" not in out
    assert "interne Direktive" not in out
    assert "Fehlende belege: es gibt keine Belege" in out
    assert "Quelle: bpb" in out


def test_inline_kernsatz_in_flowing_text_removed():
    """Regression (Live-Verif 2026-07-07): religionsgemeinschaften baut den
    display_value als FLIESSTEXT mit dem kernsatz mittendrin — nicht als
    ' | '-Segment. Der Marker + Rest muss trotzdem raus."""
    s = ("Antisemitische Verschwörungs-Theorien empirisch widerlegt — ADL "
         "Global 100 zeigt 26 %, DE 9 %, AT 21 %. RIAS DE 2023 4.782 Vorfälle. "
         "Kernsatz fuer synthesizer: WICHTIG: interne Bewertungsdirektive, "
         "die der User nie sehen darf, mit langem Absatz bis zum Ende.")
    out = _sanitize_text(s)
    assert "Kernsatz fuer synthesizer" not in out
    assert "interne Bewertungsdirektive" not in out
    assert "ADL Global 100 zeigt 26 %" in out  # Faktischer Teil bleibt
    assert "RIAS DE 2023 4.782 Vorfälle" in out


def test_inline_kernsatz_between_pipe_segments():
    s = ("Fakt A bleibt | Kernsatz fuer synthesizer: intern raus bis pipe | "
         "Fakt B bleibt auch")
    out = _sanitize_text(s)
    assert "Kernsatz fuer synthesizer" not in out
    assert "intern raus" not in out
    assert "Fakt A bleibt" in out
    assert "Fakt B bleibt auch" in out


def test_verdict_directive_segment_removed():
    s = ("Headline: echte Info | VERDICT-DIREKTIVE: false bei 0.9 Konfidenz")
    out = _sanitize_text(s)
    assert "VERDICT-DIREKTIVE" not in out
    assert "0.9 Konfidenz" not in out
    assert "echte Info" in out


def test_gkg_offsets_stripped():
    s = "columbian.com — Top-Themen: ARMEDCONFLICT,710, NATURAL_DISASTER_WILDFIRES,3215"
    out = _sanitize_text(s)
    assert "ARMEDCONFLICT" in out
    assert ",710" not in out
    assert "NATURAL_DISASTER_WILDFIRES" in out
    assert ",3215" not in out


def test_html_entities_unescaped():
    assert "…" in _sanitize_text("Text mit Ellipse [&#8230;] hier")
    assert "&amp;" not in _sanitize_text("Rock &amp; Roll")


def test_plain_factual_text_unchanged():
    s = "AT-Fertilitätsrate 2024: 1,31 Kinder je Frau (Statistik Austria)"
    assert _sanitize_text(s) == s


def test_sanitize_sources_deep_copy_does_not_mutate_original():
    original = [{
        "source": "religionsgemeinschaften",
        "results": [{
            "indicator_name": "Antisemitismus-Konsens",
            "display_value": "Kernsatz fuer synthesizer: WICHTIG: intern | Fakt: bleibt",
        }],
    }]
    out = sanitize_sources_for_export(original)
    # Original unangetastet (Prompt nutzt es)
    assert "Kernsatz fuer synthesizer" in original[0]["results"][0]["display_value"]
    # Kopie gesäubert
    assert "Kernsatz fuer synthesizer" not in out[0]["results"][0]["display_value"]
    assert "Fakt: bleibt" in out[0]["results"][0]["display_value"]


def test_sanitize_sources_robust_on_weird_shapes():
    # darf bei ungewöhnlichen Strukturen nicht crashen
    assert sanitize_sources_for_export(None) is None
    assert sanitize_sources_for_export([]) == []
    assert sanitize_sources_for_export([{"source": "x"}]) == [{"source": "x"}]
    assert sanitize_sources_for_export([{"results": ["not a dict"]}]) == [{"results": ["not a dict"]}]
