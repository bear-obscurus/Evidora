"""Golden-/Charakterisierungs-Suite für die Verdict-Override-Kaskade.

Deterministisches Regressions-Netz für ``verdict_postprocess.apply_verdict_
postprocessing`` — OHNE LLM, ohne Netzwerk, in <1 s. Jeder Case pinnt einen
konkreten Override-Zweig oder eine Reihenfolge-Interaktion auf seinen
AKTUELLEN Output (verdict + ggf. confidence).

Zweck: bevor die Kaskade (#2 Cosine-Wurzel-Fix, #6 Registry, #3 Phase B/D)
umgebaut wird, friert dieses Netz das heutige Verhalten ein. Jede
Verhaltens-Änderung durch einen Refactor lässt mindestens einen Case
fehlschlagen → man sieht sofort, WAS sich geändert hat.

Es sind Charakterisierungs-Tests: sie behaupten, was der Code HEUTE tut,
nicht zwingend, was „richtig" wäre. Auffälligkeiten sind im Sprint-Log
notiert. Erweitern, wenn ein neuer Zweig/Guard dazukommt.

Branch-Abdeckung (apply_verdict_postprocessing):
  1 STRUKTURELL-Override + Tier-1/Tier-2-Relevanz-Guards
  2 Confidence-Cap für unverifiable
  3 Wikipedia-only + normativer Term → mixed
  4 4-Tier-Consistency (true/mostly_true/mostly_false/false) + Regex
  4b Factual-Content (Superlativ / Rekord-Jahr / "über X" / Medical-Denial /
     Kompetenz-Urteil / Trend-"kaum") + struct-fired-Ausnahme
  5 AMS/ILO-Dual-Methodik-Guard (nach Consistency)
  6 Wahlprognose-Guard (finale Autorität)
"""

import copy

import pytest

from services.verdict_postprocess import apply_verdict_postprocessing as P


def _r(verdict, confidence, summary=""):
    """Baue ein LLM-Ergebnis-Dict (Eingang der Kaskade)."""
    return {"verdict": verdict, "confidence": confidence, "summary": summary}


def _src(*display_values, source="Pack"):
    """Eine Quelle mit N Results, je ein display_value."""
    return {"source": source, "results": [{"display_value": dv} for dv in display_values]}


_STR = "STRUKTURELL FALSCH: durch die Quellenlage widerlegt."
_NRM = "normaler thematischer Treffer ohne Marker"


# (id, claim, result_in, source_results, expect_verdict, expect_confidence|None)
CASES = [
    # --- Branch 1: STRUKTURELL-Override + Guards ---
    ("struct_fire_100pct",
     "irgendein faktenclaim",
     _r("true", 0.90, "Die Quellenlage ist eindeutig."),
     [_src(_STR)],
     "mostly_false", 0.85),

    ("struct_fire_from_mostly_true",
     "irgendein faktenclaim",
     _r("mostly_true", 0.80, "Die Daten sprechen dafür."),
     [_src(_STR, _STR, _NRM)],            # 2/3 = 67% ≥ 50% → fire
     "mostly_false", 0.85),

    ("struct_fire_ratio50_edge",
     "irgendein faktenclaim",
     _r("true", 0.90, "neutrale zusammenfassung"),
     [_src(_STR, _NRM)],                  # 1/2 = 50% → else-Zweig → fire
     "mostly_false", 0.85),

    ("struct_tier1_skip_low_ratio",
     "irgendein faktenclaim",
     _r("true", 0.90, "Die Behauptung ist plausibel."),
     [_src(_STR, _NRM, _NRM, _NRM, _NRM, _NRM, _NRM, _NRM, _NRM, _NRM)],  # 1/10=10%<15%
     "true", 0.90),

    ("struct_tier2_skip_confirm",
     "irgendein faktenclaim",
     _r("true", 0.90, "Die Behauptung trifft zu."),   # summary confirms
     [_src(_STR, _STR, _NRM, _NRM, _NRM)],             # 2/5 = 40% (15-50%)
     "true", 0.90),

    ("struct_fire_ratio40_no_confirm",
     "irgendein faktenclaim",
     _r("true", 0.90, "Die Datenlage ist gemischt."),  # KEIN confirm
     [_src(_STR, _STR, _NRM, _NRM, _NRM)],             # 2/5 = 40% → fire
     "mostly_false", 0.85),

    ("struct_no_marker_noop",
     "irgendein faktenclaim",
     _r("true", 0.90, "neutrale zusammenfassung"),
     [_src(_NRM, _NRM)],
     "true", 0.90),

    ("struct_verdict_false_skips_block",
     "irgendein faktenclaim",
     _r("false", 0.85, "neutrale zusammenfassung"),
     [_src(_STR)],                        # Marker da, aber verdict=false → Block übersprungen
     "false", 0.85),

    # Interaktion: STRUKTURELL feuert, Summary sagt "true" → behalten (#48-Guard)
    ("struct_fired_summary_true_kept",
     "irgendein faktenclaim",
     _r("true", 0.90, "Die Behauptung ist korrekt."),
     [_src(_STR)],
     "mostly_false", 0.85),

    # --- Branch 2: Confidence-Cap unverifiable ---
    ("unverif_cap_high_conf",
     "ein nicht prüfbarer claim",
     _r("unverifiable", 0.70, "neutrale zusammenfassung"),
     [_src(_NRM)],
     "unverifiable", 0.15),

    ("unverif_already_low",
     "ein nicht prüfbarer claim",
     _r("unverifiable", 0.10, "neutrale zusammenfassung"),
     [_src(_NRM)],
     "unverifiable", 0.10),

    # --- Branch 3: Wikipedia-only + normativer Term → mixed ---
    ("wiki_normative_rechtsextrem_to_mixed",
     "Die Partei XY ist rechtsextrem",
     _r("unverifiable", 0.10,
        "Laut DE-Wikipedia wird die Partei XY als rechtsextrem klassifiziert."),
     [_src("Wikipedia: Partei XY", source="Wikipedia")],
     "mixed", 0.50),

    ("wiki_normative_faschistisch_to_mixed",
     "Die AfD ist faschistisch",
     _r("unverifiable", 0.15,
        "Laut Wikipedia wird die AfD teils als rechtsextrem beschrieben."),
     [_src("Wikipedia: AfD", source="Wikipedia")],
     "mixed", 0.50),

    ("wiki_normative_no_classification_stays",
     "Die Partei XY ist rechtsextrem",
     _r("unverifiable", 0.10, "Es liegen keine eindeutigen Quellen vor."),
     [_src(_NRM)],
     "unverifiable", 0.10),

    ("normative_term_but_not_unverifiable",
     "Die Partei XY ist rechtsextrem",
     _r("false", 0.80, "neutrale zusammenfassung"),
     [_src(_NRM)],
     "false", 0.80),

    # --- Branch 4: 4-Tier-Consistency ---
    ("consistency_false_to_true",
     "ein claim",
     _r("false", 0.80, "Die Behauptung ist korrekt."),
     [_src(_NRM)],
     "true", 0.80),

    ("consistency_true_to_false",
     "ein claim",
     _r("true", 0.90, "Die Behauptung ist falsch."),
     [_src(_NRM)],
     "false", 0.90),

    ("consistency_true_to_mostly_true",
     "ein claim",
     _r("true", 0.90, "Die Behauptung ist größtenteils richtig."),
     [_src(_NRM)],
     "mostly_true", 0.90),

    ("consistency_false_to_mostly_false",
     "ein claim",
     _r("false", 0.80, "Die Behauptung ist größtenteils falsch."),
     [_src(_NRM)],
     "mostly_false", 0.80),

    ("consistency_regex_behauptung_dass",
     "ein claim",
     _r("false", 0.80, "Die Behauptung, dass X tatsächlich gilt, ist korrekt."),
     [_src(_NRM)],
     "true", 0.80),

    ("consistency_conspiracy_to_false",
     "Der Deep State steuert die Regierung",
     _r("mostly_false", 0.85,
        "Das ist ein unbelegtes Verschwörungsnarrativ ohne empirische Belege."),
     [_src(_NRM)],
     "false", 0.85),

    ("consistency_agreement_noop",
     "ein claim",
     _r("true", 0.90, "Die Behauptung ist korrekt."),
     [_src(_NRM)],
     "true", 0.90),

    # --- Branch 4b: Factual-Content ---
    ("factual_superlative_to_true",
     "Die Krone bekommt in Österreich die meisten Inserate",
     _r("false", 0.85,
        "Die Krone erhält mit 22,4 Mio. Euro die meisten Inserate."),
     [_src(_NRM)],
     "true", 0.85),

    ("factual_record_year_to_true",
     "Wien hatte 2024 das wärmste Jahr seit Messbeginn",
     _r("false", 0.85,
        "Das wärmste Jahr war 2024 mit 13,0 °C in Wien."),
     [_src(_NRM)],
     "true", 0.85),

    ("factual_threshold_ueber_to_true",
     "Das Produkt kostet über 1000 Euro",
     _r("false", 0.85, "Es kostet aktuell 1.095 Euro."),
     [_src(_NRM)],
     "true", 0.85),

    ("medical_healing_denial_to_false",
     "Kurkuma heilt Krebs",
     _r("mostly_false", 0.85,
        "Kurkuma heilt Krebs nicht; es gibt keine ausreichende Evidenz."),
     [_src(_NRM)],
     "false", 0.85),

    ("competence_ruling_to_mostly_false",
     "Der Berliner Mietendeckel war verfassungskonform",
     _r("false", 0.90,
        "Das Bundesverfassungsgericht hob das Gesetz wegen fehlender "
        "Gesetzgebungskompetenz des Landes auf."),
     [_src(_NRM)],
     "mostly_false", 0.90),

    ("trend_kaum_to_mostly_true",
     "Österreich hat seine CO2-Emissionen kaum gesenkt",
     _r("true", 0.85, "Die Emissionen sanken um 16 Prozent."),
     [_src(_NRM)],
     "mostly_true", 0.85),

    ("trend_kaum_out_of_range_noop",
     "Österreich hat seine CO2-Emissionen kaum gesenkt",
     _r("true", 0.85, "Die Emissionen sanken um 2 Prozent."),
     [_src(_NRM)],
     "true", 0.85),

    # --- Branch 5: AMS/ILO-Dual-Methodik ---
    ("ams_ilo_in_range_to_mixed",
     "Die Arbeitslosenquote in Österreich beträgt 5 Prozent",
     _r("mostly_false", 0.80, "Nach ILO-Methodik liegt die Quote bei 4,9%."),
     [_src(_NRM)],
     "mixed", 0.65),

    ("ams_ilo_out_of_range_noop",
     "Die Arbeitslosenquote in Österreich beträgt 7 Prozent",
     _r("mostly_false", 0.80, "Nach AMS-Methodik liegt die Quote bei 7%."),
     [_src(_NRM)],
     "mostly_false", 0.80),

    # --- Branch 6: Wahlprognose-Guard ---
    ("wahlprognose_party_to_unverifiable",
     "Die FPÖ wird die nächste Nationalratswahl gewinnen",
     _r("mostly_false", 0.70, "neutrale zusammenfassung"),
     [_src(_NRM)],
     "unverifiable", 0.10),

    ("wahlprognose_no_party_noop",
     "Die Opposition wird die nächste Wahl gewinnen",
     _r("false", 0.80, "neutrale zusammenfassung"),
     [_src(_NRM)],
     "false", 0.80),

    # --- Reihenfolge-Interaktionen (Guards MÜSSEN nach Consistency laufen) ---
    # Pinnt die in lessons_learned als "reihenfolge-fragil" markierten Pfade.
    ("ordering_ams_overrides_consistency",
     "Die Arbeitslosenquote in Österreich liegt bei 5 Prozent",
     _r("false", 0.80,
        "Die Behauptung ist größtenteils falsch; nach ILO-Methodik 4,9%."),
     [_src(_NRM)],
     "mixed", 0.65),   # consistency→mostly_false, dann AMS/ILO→mixed

    ("ordering_wahlprognose_overrides_consistency",
     "Die FPÖ wird die nächste Nationalratswahl gewinnen",
     _r("mostly_true", 0.70, "Die Behauptung ist überwiegend falsch."),
     [_src(_NRM)],
     "unverifiable", 0.10),  # consistency→mostly_false, dann Wahlprognose→unverifiable

    # --- Pattern-Varianten ---
    ("struct_tier_boundary_015_no_confirm_fires",
     "irgendein faktenclaim",
     _r("true", 0.90, "neutrale zusammenfassung"),   # KEIN confirm
     [_src(*([_STR] * 3 + [_NRM] * 17))],            # 3/20 = 15% → nicht <15% → fire
     "mostly_false", 0.85),

    ("consistency_english_true",
     "a claim",
     _r("false", 0.80, "Therefore the claim is true."),
     [_src(_NRM)],
     "true", 0.80),

    ("consistency_english_false",
     "a claim",
     _r("true", 0.90, "Therefore the claim is false."),
     [_src(_NRM)],
     "false", 0.90),

    ("factual_niedrigste_to_true",
     "Österreich hat die niedrigste Quote im Vergleich",
     _r("false", 0.85, "Österreich hat die niedrigste Quote im Vergleich."),
     [_src(_NRM)],
     "true", 0.85),

    ("wahlprognose_staerkste_kraft",
     "Die SPÖ wird stärkste Kraft bei der nächsten Wahl",
     _r("false", 0.80, "neutrale zusammenfassung"),
     [_src(_NRM)],
     "unverifiable", 0.10),

    # --- Pattern F: superlative/threshold REFUTED by summary, verdict=true ---
    # (Bugs #52/#74/#81 aus dem 100-Gap-Claim-Lauf 2026-06-27)
    ("patternF_super_counterleader_single",   # #52
     "Österreich hat unter allen EU-Staaten die niedrigste Wohneigentumsquote.",
     _r("true", 0.90,
        "Österreich hat eine Wohneigentumsquote von 55,3 % (Eurostat 2023), "
        "während Deutschland mit 49,1 % die niedrigste Quote aller EU-Staaten "
        "aufweist."),
     [_src(_NRM)],
     "false", 0.90),

    ("patternF_threshold_refuted",            # #74
     "Elektroautos haben in Österreich im Mittel eine Reichweite von über "
     "500 km unter realen Winterbedingungen.",
     _r("true", 0.70,
        "ADAC-Wintertests 2024 zeigen, dass die Real-Reichweite typischerweise "
        "250–420 km beträgt, also deutlich unter 500 km liegt."),
     [_src(_NRM)],
     "false", 0.70),

    ("patternF_super_counterleader_multi",    # #81
     "Österreich hat in Europa die höchsten Pro-Kopf-Gesundheitsausgaben, die "
     "höchste Lebenserwartung und die niedrigste Arbeitslosigkeit zugleich.",
     _r("true", 0.70,
        "Österreich hat 2024 hohe Pro-Kopf-Gesundheitsausgaben, aber die "
        "höchste Lebenserwartung in Europa hat die Schweiz mit 83,7 Jahren."),
     [_src(_NRM)],
     "mostly_false", 0.70),

    # Kontrolle: legitim wahre Superlative/Schwellen dürfen NICHT kippen.
    ("patternF_control_super_legit_true",
     "Rumänien hat die höchste Wohneigentumsquote der EU.",
     _r("true", 0.90,
        "Rumänien weist mit 96 % die höchste Wohneigentumsquote aller "
        "EU-Staaten auf, gefolgt von Kroatien und der Slowakei."),
     [_src(_NRM)],
     "true", 0.90),

    ("patternF_control_threshold_legit_true",
     "Der ID.7 hat über 600 km WLTP-Reichweite.",
     _r("true", 0.85,
        "Mit 615 km WLTP-Reichweite liegt der ID.7 klar über 600 km."),
     [_src(_NRM)],
     "true", 0.85),

    # --- Pure No-Op-Baselines ---
    ("clean_true_noop",
     "ein gut belegter claim",
     _r("true", 0.95, "Die Daten stützen die Behauptung umfassend."),
     [_src(_NRM)],
     "true", 0.95),

    ("clean_false_noop",
     "ein widerlegter claim",
     _r("false", 0.85, "Die Behauptung steht im Widerspruch zu den Daten."),
     [_src(_NRM)],
     "false", 0.85),

    ("empty_sources_noop",
     "ein claim ohne treffer",
     _r("unverifiable", 0.10, ""),
     [],
     "unverifiable", 0.10),
]


@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_verdict_postprocess_golden(case):
    cid, claim, result_in, sources, exp_verdict, exp_conf = case
    out = P(copy.deepcopy(result_in), copy.deepcopy(sources), claim)
    assert out["verdict"] == exp_verdict, (
        f"{cid}: verdict={out['verdict']!r} erwartet {exp_verdict!r}"
    )
    if exp_conf is not None:
        assert out["confidence"] == exp_conf, (
            f"{cid}: confidence={out['confidence']!r} erwartet {exp_conf!r}"
        )
