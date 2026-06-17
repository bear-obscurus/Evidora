"""Verdict-Post-Processing — die Override-Kaskade nach der LLM-Synthese.

Aus ``synthesizer.synthesize_results`` ausgelagert (verhaltens-erhaltend),
damit die Ausfuehrungs-Reihenfolge EXPLIZIT an einer Stelle steht und jeder
Override einzeln unit-testbar ist (ohne LLM-Call).

Reihenfolge (kritisch! mehrere Schritte muessen NACH dem Consistency-Check
laufen, sonst werden sie zurueckueberschrieben):
  1. STRUKTURELL-FALSCH-Override (+ Tier-1/Tier-2-Relevanz-Guards)
  2. Confidence-Cap fuer unverifiable
  3. Wikipedia-only + normativer Term -> mixed
  4. 4-Tier-Consistency-Check (Summary vs. Verdict)
  5. Factual-Content-Consistency (Superlativ / Rekord-Jahr / "ueber X" /
     Medical-Healing-Denial / Kompetenz-Urteil / Trend-"kaum")
  6. Consistency-Fix anwenden
  7. AMS/ILO-Dual-Methodik-Guard (NACH 6)
  8. Wahlprognose-Guard / Politik-Tabu (NACH 6, finale Autoritaet)

Eingang: ``result`` (LLM-Output-Dict), ``source_results`` (gerankte Quellen),
``original_claim``. Mutiert ``result`` und gibt es zurueck.
"""

import logging
import re

logger = logging.getLogger("evidora")


def apply_verdict_postprocessing(result, source_results, original_claim):
    # STRUKTURELL FALSCH post-processing override (Defense-in-Depth):
    # If curated packs delivered a STRUKTURELL FALSCH marker but the LLM
    # returned true/mostly_true anyway, enforce mostly_false programmatically.
    # This catches cases where the LLM ignores the prompt rule despite seeing
    # the marker — known Mistral behavioral issue for nuanced legal/factual claims.
    #
    # RELEVANCE GUARD (2026-05-25): The override must NOT fire when the
    # STRUKTURELL source addresses a DIFFERENT sub-claim than the user asked.
    # Example: User asks "Werther-Effekt führt zu Suiziden" (TRUE) but
    # mental_health_pack's "Über Suizid darf man nicht reden ist FALSE"
    # marker fires because it matches via cosine — false positive.
    # Guard: If LLM summary CONFIRMS the claim (using broader detection),
    # the STRUKTURELL source is likely a topic-mismatch → skip override.
    if result.get("verdict") in ("true", "mostly_true"):
        has_struct_marker = False
        struct_sources_count = 0
        total_results_count = 0
        for source_data in source_results:
            if not isinstance(source_data, dict):
                continue
            for r in source_data.get("results", []):
                total_results_count += 1
                dv = r.get("display_value", "")
                if isinstance(dv, str) and "STRUKTURELL FALSCH:" in dv:
                    has_struct_marker = True
                    struct_sources_count += 1
        if has_struct_marker:
            # Relevance guard: check if LLM summary confirms the claim.
            # If yes, the STRUKTURELL source likely addresses a different
            # sub-claim (topic mismatch via cosine similarity).
            summary_lc = result.get("summary", "").lower()
            summary_confirms_claim = any(p in summary_lc for p in (
                "ist korrekt", "trifft zu", "ist zutreffend",
                "ist richtig", "tatsächlich", "wird bestätigt",
                "bestätigt dies", "stimmt", "ist belegt",
                "ist faktisch korrekt", "faktisch richtig",
                "ist wissenschaftlich belegt",
            ))
            # Also detect "Behauptung, dass ..., ist korrekt" patterns
            # where comma-separated clauses interrupt the match
            import re
            if not summary_confirms_claim:
                summary_confirms_claim = bool(re.search(
                    r"behauptung.{0,120}ist (korrekt|richtig|zutreffend|wahr|belegt)",
                    summary_lc
                ))
            # Dominance check (2026-05-25 v2): Two-tier guard:
            # Tier 1: If STRUKTURELL ratio < 15%, this is ALWAYS a
            #   topic mismatch (e.g., 1/58 = 1.7% — one unrelated
            #   pack matched via cosine). Skip override unconditionally.
            # Tier 2: If ratio 15-50% AND summary confirms claim,
            #   likely topic mismatch. Skip override.
            # Override fires when: ratio ≥ 50% (dominant) OR
            #   ratio 15-50% but summary doesn't confirm.
            struct_ratio = struct_sources_count / max(total_results_count, 1)
            if struct_ratio < 0.15:
                # Tier 1: extremely low ratio — certainly a mismatch
                logger.warning(
                    f"STRUKTURELL FALSCH override SKIPPED (Tier 1): "
                    f"ratio {struct_sources_count}/{total_results_count} "
                    f"= {struct_ratio:.1%} < 15%. Almost certainly a "
                    f"topic mismatch via cosine. Keeping LLM verdict "
                    f"'{result.get('verdict')}' @ {result.get('confidence')}."
                )
            elif summary_confirms_claim and struct_ratio < 0.5:
                # Tier 2: low ratio + summary confirms → mismatch
                logger.warning(
                    f"STRUKTURELL FALSCH override SKIPPED (Tier 2): "
                    f"LLM summary confirms claim + ratio "
                    f"{struct_sources_count}/{total_results_count} "
                    f"= {struct_ratio:.0%} < 50%. Likely topic mismatch. "
                    f"Keeping LLM verdict "
                    f"'{result.get('verdict')}' @ {result.get('confidence')}."
                )
            else:
                old_verdict = result["verdict"]
                old_conf = result.get("confidence", 0)
                result["verdict"] = "mostly_false"
                result["confidence"] = 0.85
                result["_struct_override_fired"] = True  # explicit flag
                logger.warning(
                    f"STRUKTURELL FALSCH override: LLM returned "
                    f"'{old_verdict}' @ {old_conf} despite STRUKTURELL "
                    f"FALSCH marker in sources (ratio {struct_sources_count}/"
                    f"{total_results_count} = {struct_ratio:.0%}). "
                    f"Enforcing 'mostly_false' @ 0.85."
                )

    # Cap confidence for unverifiable verdicts
    if result.get("verdict") == "unverifiable" and result.get("confidence", 0) > 0.15:
        logger.warning(
            f"Capping confidence from {result['confidence']} to 0.15 for unverifiable verdict"
        )
        result["confidence"] = 0.15

    # --- Wikipedia-only + normativer Term → mixed statt unverifiable ---
    # (Bugs #39/#40, 2026-06-04)
    # Wenn das LLM "unverifiable" sagt, aber Wikipedia eine klare
    # Klassifikation liefert (sichtbar in der Summary), sollte das
    # Verdict "mixed" sein — Wikipedia liefert ja Kontext zum Zitieren.
    from services.confidence_calibration import (
        _is_wikipedia_only, _claim_has_normative_term,
        NORMATIVE_POLITICAL_TERMS,
    )
    if (result.get("verdict") == "unverifiable"
            and _claim_has_normative_term(original_claim)):
        # Check if summary contains a Wikipedia classification
        _summary_lc = result.get("summary", "").lower()
        _has_wp_classification = any(t in _summary_lc for t in (
            "wikipedia", "laut de-wikipedia", "laut en-wikipedia",
            "wikipedia bezeichnet", "wikipedia klassifiziert",
            "wikipedia sagt", "wikipedia beschreibt",
            "sozialdemokrati", "rechtsextrem", "rechtspopulist",
            "linksextrem", "populistisch", "faschistisch",
            "konservativ", "liberal", "christdemokrat",
        ))
        if _has_wp_classification:
            logger.warning(
                f"Wikipedia-normative-term guard: LLM returned "
                f"'unverifiable' for normative-term claim, but "
                f"summary contains Wikipedia classification. "
                f"Overriding to 'mixed' @ 0.50."
            )
            result["verdict"] = "mixed"
            result["confidence"] = 0.50

    # --- AMS/ILO Dual-Methodik-Guard (Bug #63, 2026-06-04) ---
    # AMS/ILO guard moved to AFTER consistency check (see below line ~1380).
    import re
    _claim_lc = (original_claim or "").lower()

    # Consistency check: detect when summary text contradicts verdict.
    # Extended 2026-05-25: broader pattern detection including comma-
    # separated clauses ("Behauptung, dass X, ist korrekt") and
    # German indirect-speech patterns.
    # Extended 2026-06-04: detect factual-content-confirms-claim even
    # when LLM conclusion says "falsch" (Bug #47 Krone, #51 Wien).
    summary_lower = result.get("summary", "").lower()
    claim_lower = (original_claim or "").lower()
    verdict = result.get("verdict", "")
    verdict_from_summary = None

    # Check for explicit verdict statements in summary
    # (2026-06-04: split into 4 tiers: true, mostly_true, mostly_false, false)
    true_patterns = [
        "behauptung ist daher wahr", "behauptung ist wahr",
        "behauptung ist korrekt", "behauptung ist richtig",
        "claim is true", "claim is correct", "therefore true",
        "ist faktisch korrekt", "ist faktisch richtig",
        "ist sachlich korrekt", "ist sachlich richtig",
    ]
    mostly_true_patterns = [
        "ist größtenteils richtig", "ist überwiegend richtig",
        "ist größtenteils korrekt", "ist überwiegend korrekt",
        "ist größtenteils wahr", "ist überwiegend wahr",
        "ist im wesentlichen korrekt", "ist im kern richtig",
    ]
    mostly_false_patterns = [
        # "größtenteils falsch" = mostly_false, NOT false!
        # Bug #60/#69: these were in false_patterns, causing
        # false@0.85 when the LLM actually said "mostly false".
        "ist größtenteils falsch", "ist überwiegend falsch",
        "größtenteils nicht korrekt", "überwiegend nicht korrekt",
        "ist größtenteils nicht richtig",
    ]
    false_patterns = [
        "behauptung ist daher falsch", "behauptung ist falsch",
        "behauptung ist nicht korrekt", "behauptung ist nicht richtig",
        "claim is false", "claim is incorrect", "therefore false",
        "ist faktisch falsch", "ist sachlich falsch",
        # Conspiracy-specific strong-false signals (Bug #72)
        "verschwörungsnarrativ ohne", "verschwörungstheorie ohne",
        "unbelegtes verschwörungsnarrativ", "unbelegte verschwörungstheorie",
        "ohne empirische belege",
        # Medical wirksamkeit denial (Bug #82)
        "heilt krebs aber nicht", "heilt krebs jedoch nicht",
        "keine eigenständige heilmethode", "kein heilmittel",
        "keine ausreichende wirksamkeit",
        "bisher keine ausreichende", "nicht nachgewiesen am menschen",
    ]

    if any(p in summary_lower for p in true_patterns):
        verdict_from_summary = "true"
    elif any(p in summary_lower for p in mostly_true_patterns):
        verdict_from_summary = "mostly_true"
    elif any(p in summary_lower for p in false_patterns):
        verdict_from_summary = "false"
    elif any(p in summary_lower for p in mostly_false_patterns):
        verdict_from_summary = "mostly_false"

    # Extended regex: "Behauptung, dass ..., ist korrekt/wahr/richtig"
    if not verdict_from_summary:
        import re
        if re.search(
            r"behauptung.{0,150}ist (korrekt|richtig|zutreffend|wahr|belegt)",
            summary_lower
        ):
            verdict_from_summary = "true"
        elif re.search(
            r"behauptung.{0,150}ist (falsch|inkorrekt|nicht korrekt|nicht richtig|widerlegt)",
            summary_lower
        ):
            verdict_from_summary = "false"

    # Factual-content consistency (2026-06-04, Bugs #47 + #51):
    # Detect when summary factual content CONFIRMS the claim but
    # verdict is false/mostly_false. This catches cases where the
    # LLM cites correct data ("Krone erhält die meisten", "wärmstes
    # Jahr 2024") but arrives at the wrong conclusion — often caused
    # by unrelated STRUKTURELL markers from other packs pulled in
    # via cosine-similarity.
    if verdict in ("false", "mostly_false") and not verdict_from_summary:
        import re
        factual_confirms = False

        # Pattern A: Superlative/ranking claims where summary
        # explicitly confirms the ranking ("erhält die meisten",
        # "hat die höchste", "ist Spitzenreiter")
        superlative_claim = any(t in claim_lower for t in (
            "meisten", "höchste", "größte", "stärkste",
            "niedrigste", "geringste", "spitzenreiter",
        ))
        if superlative_claim:
            superlative_confirmed = any(t in summary_lower for t in (
                "die meisten", "die höchste", "der größte",
                "die stärkste", "spitzenreiter", "rang #1",
                "an erster stelle", "die niedrigste",
            ))
            if superlative_confirmed:
                factual_confirms = True
                logger.info(
                    f"Factual-content consistency: superlative claim "
                    f"confirmed in summary but verdict='{verdict}'"
                )

        # Pattern B: Record-year claims where summary confirms
        # the claimed year IS the record year
        if not factual_confirms:
            record_match = re.search(
                r"wärmst\w* jahr[^.]{0,40}?(\d{4})", summary_lower
            )
            if record_match:
                record_year = record_match.group(1)
                if record_year in claim_lower:
                    factual_confirms = True
                    logger.info(
                        f"Factual-content consistency: record year "
                        f"{record_year} confirmed in summary but "
                        f"verdict='{verdict}'"
                    )

        # Pattern E (Bug #79 + #82): "über X" / "mehr als X" threshold
        # claims where the summary cites a concrete number that exceeds
        # the claimed threshold.
        # Examples:
        #   Claim: "kostet über 1000 Euro" + Summary: "1.095 €" → 1095>1000 → true
        #   Claim: "über eine Million Wohnungen" + Summary: "1,9 Mio" → 1.9M>1M → true
        if not factual_confirms:
            _WORD_NUMBERS = {
                "eine million": 1_000_000, "einer million": 1_000_000,
                "1 million": 1_000_000,
                "zwei millionen": 2_000_000, "drei millionen": 3_000_000,
                "eine milliarde": 1_000_000_000,
                "einer milliarde": 1_000_000_000,
                "hundert": 100, "tausend": 1_000,
                "zehntausend": 10_000, "hunderttausend": 100_000,
            }
            threshold_val = None
            threshold_unit = None  # None, "mio", "mrd"

            # Step 1: extract threshold from claim
            # Try word-based numbers first ("über eine Million")
            for word, val in _WORD_NUMBERS.items():
                pattern_w = rf"(?:über|ueber|mehr\s+als|mindestens)\s+{re.escape(word)}"
                if re.search(pattern_w, claim_lower):
                    threshold_val = val
                    break

            # Try digit-based thresholds ("über 1000", "über 1.000")
            if threshold_val is None:
                thr_match = re.search(
                    r"(?:über|ueber|mehr\s+als|mindestens)\s+"
                    r"(\d{1,3}(?:[. ]\d{3})+(?:,\d+)?|\d+(?:,\d+)?)"
                    r"(?:\s*(?:mio\.?|millionen?|mrd\.?|milliarden?))?",
                    claim_lower,
                )
                if thr_match:
                    raw = thr_match.group(1)
                    # German: dots as thousand separator, comma as decimal
                    cleaned = raw.replace(" ", "").replace(".", "")
                    cleaned = cleaned.replace(",", ".")
                    try:
                        threshold_val = float(cleaned)
                    except ValueError:
                        pass
                    # Check for Mio/Mrd multiplier
                    after_num = claim_lower[thr_match.end():]
                    if re.match(r"\s*(?:mio\.?|millionen?)", after_num):
                        threshold_val = (threshold_val or 0) * 1_000_000
                        threshold_unit = "mio"
                    elif re.match(r"\s*(?:mrd\.?|milliarden?)", after_num):
                        threshold_val = (threshold_val or 0) * 1_000_000_000
                        threshold_unit = "mrd"

            if threshold_val is not None and threshold_val > 0:
                # Step 2: extract candidate numbers from summary
                # Match patterns like "1.095 €", "1,9 Mio.", "286.000"
                # Only capture the number — do NOT consume
                # mio/€ suffixes, so after_s can detect them.
                num_candidates = re.finditer(
                    r"(\d{1,3}(?:[.\xa0]\d{3})+(?:,\d+)?|\d+(?:,\d+)?)",
                    summary_lower,
                )
                for nm in num_candidates:
                    raw_s = nm.group(1)
                    cleaned_s = raw_s.replace(" ", "").replace(".", "")
                    cleaned_s = cleaned_s.replace(",", ".")
                    try:
                        summary_num = float(cleaned_s)
                    except ValueError:
                        continue
                    after_s = summary_lower[nm.end():]
                    if re.match(r"\s*(?:mio\.?|millionen?)", after_s):
                        summary_num *= 1_000_000
                    elif re.match(r"\s*(?:mrd\.?|milliarden?)", after_s):
                        summary_num *= 1_000_000_000

                    # Step 3: compare — summary number must plausibly
                    # relate to same domain (within 100x of threshold)
                    if (summary_num > threshold_val
                            and summary_num < threshold_val * 100):
                        factual_confirms = True
                        logger.info(
                            f"Factual-content consistency: threshold "
                            f"claim 'über {threshold_val}' confirmed "
                            f"by summary value {summary_num} "
                            f"(verdict was '{verdict}')"
                        )
                        break

        if factual_confirms:
            verdict_from_summary = "true"

    # Pattern C-med (Bug #82): Medical healing claims where summary
    # denies efficacy. Claim says "heilt/heilen/heilung" but summary
    # says "keine ... evidenz/wirksamkeit/belege" → false.
    # Uses regex to handle LLM phrasing variance.
    if (verdict in ("mostly_false",) and not verdict_from_summary
            and any(t in claim_lower for t in (
                "heilt", "heilen", "heilung", "heilmittel", "cures",
            ))):
        if re.search(
            r"keine\w?\s+ausreichende?\w?\s+\w*(?:evidenz|wirksamkeit|belege|nachweise)",
            summary_lower
        ) or re.search(
            r"(?:heilt|heilung).{0,30}(?:nicht|keine|kein)",
            summary_lower
        ):
            verdict_from_summary = "false"
            logger.info(
                f"Medical-healing-denial pattern: claim contains "
                f"healing term + summary denies efficacy → false"
            )

    # Pattern C (Bug #6): Competence rulings — when a court struck
    # down a law on COMPETENCE grounds (not substance), the claim
    # "X war verfassungskonform" is mostly_false (not false), because
    # the policy content itself wasn't ruled unconstitutional.
    if (verdict in ("false",) and not verdict_from_summary
            and any(t in claim_lower for t in (
                "verfassungskonform", "verfassungsmäßig",
                "grundgesetzkonform", "grundgesetzmäßig",
            ))
            and any(t in summary_lower for t in (
                "gesetzgebungskompetenz", "kompetenz",
                "zuständigkeit", "föderale",
            ))):
        verdict_from_summary = "mostly_false"
        logger.info(
            f"Competence-ruling pattern: claim about "
            f"constitutionality + summary mentions competence "
            f"→ mostly_false (nuance: substance not ruled on)"
        )

    # Pattern D (Bug #97): Trend claims with "kaum" where data
    # shows modest but non-zero reduction → mostly_true (not true).
    # "kaum gesenkt" + data shows e.g. 16% reduction = mostly_true.
    if (verdict in ("true",) and not verdict_from_summary
            and any(t in claim_lower for t in (
                "kaum", "wenig", "minimal", "geringfügig",
                "kaum gesenkt", "kaum reduziert",
            ))):
        pct_match = re.search(
            r"(\d+(?:[.,]\d+)?)\s*(?:prozent|%|indexpunkt)",
            summary_lower,
        )
        if pct_match:
            try:
                pct_val = float(pct_match.group(1).replace(",", "."))
                if 5.0 < pct_val < 30.0:
                    verdict_from_summary = "mostly_true"
                    logger.info(
                        f"Trend-kaum pattern: claim says 'kaum' + "
                        f"data shows {pct_val}% change → mostly_true "
                        f"(non-trivial reduction, not 'kaum' = 0)"
                    )
            except ValueError:
                pass

    if verdict_from_summary and verdict_from_summary != verdict:
        # Don't override if the STRUKTURELL override ACTUALLY fired
        # (explicit flag set in the override block above) — UNLESS
        # the factual-content check confirmed the claim via high-
        # confidence patterns (superlative/record). These patterns
        # indicate a STRUKTURELL topic-mismatch, not a legitimate
        # override (Bug #48 Griechenland regression, 2026-06-04).
        struct_fired = result.get("_struct_override_fired", False)
        factual_content_confirmed = (factual_confirms
                                     if "factual_confirms" in dir()
                                     else False)
        if (struct_fired and verdict_from_summary == "true"
                and not factual_content_confirmed):
            logger.info(
                f"Verdict consistency: summary says 'true' but STRUKTURELL "
                f"override active (no factual-content match) — keeping "
                f"'{verdict}'."
            )
        else:
            logger.warning(
                f"Verdict consistency fix: JSON verdict='{verdict}' "
                f"contradicts summary (detected '{verdict_from_summary}'). "
                f"Correcting to '{verdict_from_summary}'."
            )
            result["verdict"] = verdict_from_summary

    # --- AMS/ILO Dual-Methodik-Guard (NACH Consistency-Check, 2026-06-05) ---
    # MUST run AFTER consistency check, otherwise it gets overridden
    # by "größtenteils falsch" detection in the summary.
    if (result.get("verdict") in ("mostly_false", "false")
            and any(t in _claim_lc for t in ("arbeitslos", "arbeitslosenquote",
                                              "arbeitslosigkeit"))
            and any(t in _claim_lc for t in ("österreich", "oesterreich",
                                              "austria", " at "))
            and result.get("summary", "")):
        summary_lc = result["summary"].lower()
        has_ams_mention = any(t in summary_lc for t in (
            "ams-methodik", "ams methodik", "ams-quote", "nach ams",
            "nationale definition", "registerarbeitslosigkeit",
        ))
        has_ilo_mention = any(t in summary_lc for t in (
            "ilo", "eurostat", "internationale", "labour force",
        ))
        has_ilo_in_claim = any(t in _claim_lc for t in (
            "ilo", "nach ilo", "ilo-methode", "ilo-quote",
            "eurostat-methode", "labour force",
        ))
        has_ams_in_claim = any(t in _claim_lc for t in (
            "ams-methode", "ams-quote", "nach ams",
            "nationale berechnung", "registerarbeitslos",
        ))
        claim_pct_match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:prozent|%)", _claim_lc)
        if claim_pct_match and (has_ams_mention or has_ilo_mention
                                or has_ilo_in_claim or has_ams_in_claim):
            try:
                claimed_val = float(claim_pct_match.group(1).replace(",", "."))
                if 4.0 <= claimed_val <= 6.0:  # ILO-Bereich
                    old_v = result["verdict"]
                    result["verdict"] = "mixed"
                    result["confidence"] = min(result.get("confidence", 0.7), 0.65)
                    logger.warning(
                        f"AMS/ILO dual-method guard: claim value "
                        f"{claimed_val}% is in ILO range. Overriding "
                        f"'{old_v}' → 'mixed' @ {result['confidence']:.2f}. "
                        f"Both AMS (~7%) and ILO (~5%) are valid."
                    )
            except ValueError:
                pass

    # --- Wahlprognose-Guard (Politik-Tabu #2, Bug #38, 2026-06-04) ---
    # MUST run AFTER the consistency check, otherwise the consistency
    # check detects "überwiegend falsch" in the summary and overrides
    # unverifiable back to false. This guard has final authority.
    import re
    _claim_lc = (original_claim or "").lower()
    _PROGNOSE_PATTERNS = (
        r"\b(wird|werden|dürfte|könnte|soll)\b.{0,40}\bwahl\w*\b.{0,20}\b(gewinnen|verlieren|siegen)",
        r"\b(wird|werden|dürfte|könnte|soll)\b.{0,30}\b(stärkste|stärkster|schwächste|erste|erster)\b.{0,20}\b(partei|kraft|fraktion)",
        r"\bwahl\w*\b.{0,20}\b(gewinnen|verlieren|siegen)\b.{0,20}\b(wird|werden|dürfte)",
        r"\bnächste\w*\b.{0,20}\b(wahl|nationalratswahl|landtagswahl|europawahl|bundestag)",
    )
    _PARTY_TOKENS_SHORT = (
        "fpö", "fpoe", "spö", "spoe", "övp", "oevp", "neos",
        "grüne", "gruene", "afd", "cdu", "csu", "spd", "linke",
    )
    is_prognose = (
        any(re.search(p, _claim_lc) for p in _PROGNOSE_PATTERNS)
        and any(t in _claim_lc for t in _PARTY_TOKENS_SHORT)
    )
    if is_prognose and result.get("verdict") != "unverifiable":
        old_v = result["verdict"]
        old_c = result.get("confidence", 0)
        result["verdict"] = "unverifiable"
        result["confidence"] = 0.10
        result["nuance"] = (
            "Wahlprognosen sind keine überprüfbaren Fakten. "
            "Evidora bewertet nur abgeschlossene Wahlergebnisse, "
            "keine Vorhersagen über zukünftige Wahlen."
        )
        logger.warning(
            f"Wahlprognose-Guard: overriding '{old_v}' @ {old_c} → "
            f"'unverifiable' @ 0.10 for prediction claim."
        )

    # Clean up internal flags before returning
    result.pop("_struct_override_fired", None)

    return result


# Confidence-Ceiling, wenn die Claim-Analyse auf den _fallback degradiert ist.
# Bewusst moderat (nicht 0.0): viele Static-First-Packs + Faktencheck-RSS
# triggern auf dem Roh-Claim-Text und liefern auch ohne Analyzer-JSON solide
# Belege. Der Cap verhindert nur das selbstsichere Verdict auf reduzierter
# Quellen-Basis — er nullt eine gut belegte Bewertung nicht.
ANALYSIS_FALLBACK_CONFIDENCE_CAP = 0.5


def apply_analysis_fallback_cap(synthesis, analysis, lang="de", cap=ANALYSIS_FALLBACK_CONFIDENCE_CAP):
    """Quick-Win #5 — Degraded-Analysis-Guard.

    Wenn ``claim_analyzer.analyze_claim`` nach dem Retry nur den
    ``_fallback_analysis``-Stub liefern konnte (unparsebares Mistral-JSON),
    sind alle wiss./med./klima-/wirtschafts-Quellen-Flags auf False gesetzt —
    der med./wiss. Pipeline-Zweig faellt weg, das Verdict steht auf duennerer
    Basis als normal. Diese Funktion cappt dann die Konfidenz und haengt einen
    transparenten Hinweis an (analog zur Motivation von Fix #1: kein
    selbstsicheres Verdict auf degradierter Analyse ausliefern).

    Mutiert ``synthesis`` und gibt es zurueck. No-op, wenn kein ``_fallback``
    vorliegt. Bei bereits ``unverifiable``-Verdict nur Observability-Flag, kein
    Cap (Konfidenz ist dort schon 0.0).
    """
    if not (analysis or {}).get("_fallback"):
        return synthesis

    synthesis["_analysis_fallback"] = True

    if synthesis.get("verdict") == "unverifiable":
        return synthesis

    orig_conf = synthesis.get("confidence") or 0.0
    if orig_conf > cap:
        synthesis["confidence"] = cap

    caveat = (
        "Hinweis: Die automatische Claim-Analyse war eingeschraenkt — diese "
        "Bewertung stuetzt sich auf eine reduzierte Quellenauswahl und ist "
        "entsprechend vorsichtig zu interpretieren."
        if lang == "de" else
        "Note: Automated claim analysis was degraded — this verdict draws on a "
        "reduced set of sources and should be read with caution."
    )
    existing = synthesis.get("nuance")
    synthesis["nuance"] = f"{existing} {caveat}".strip() if existing else caveat

    logger.warning(
        "Analysis fallback active — confidence capped %.2f→%.2f, caveat added",
        orig_conf, synthesis.get("confidence") or 0.0,
    )
    return synthesis
