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


_COMPARISON_COUNTRIES = (
    "deutschland", "schweiz", "niederlande", "frankreich", "italien",
    "rumänien", "kroatien", "slowakei", "tschechien", "ungarn", "polen",
    "spanien", "schweden", "dänemark", "norwegen", "belgien", "österreich",
    "luxemburg", "portugal", "griechenland", "finnland", "irland",
    "bulgarien", "slowenien", "estland", "lettland", "litauen", "malta",
    "zypern",
)

_SUPERLATIVE_PHRASE = (
    r"(?:(?:die|der|das|den)\s+"
    r"(?:höchste|niedrigste|geringste|größte|meiste\w*|stärkste)\w*"
    r"|spitzenreiter|an erster stelle|an der spitze|schlusslicht)"
)

# Adjektive für den Verneinungs-Zweig — muss dasselbe Adjektiv wie im
# Claim sein, damit "nicht die höchste" einen "niedrigste"-Claim nicht
# fälschlich als widerlegt markiert.
_SUPERLATIVE_ADJECTIVES = (
    "höchste", "niedrigste", "geringste", "größte", "meisten", "stärkste",
)

# AT-Bundesländer für den numerischen Entitäts-Vergleich (Pattern G) —
# QA50B #8 2026-07-12: "Kärnten höher als NÖ" wurde false@0.9, obwohl die
# Summary selbst "Kärnten 13,94 %, NÖ 12,57 %" nannte. Umlaut-Varianten,
# weil Claims/Summaries beide Schreibungen tragen.
_AT_BUNDESLAENDER = (
    "wien", "niederösterreich", "niederoesterreich", "oberösterreich",
    "oberoesterreich", "salzburg", "tirol", "vorarlberg", "kärnten",
    "kaernten", "steiermark", "burgenland",
)

# Negativ-Prädikate für den L2-Tier-2b-Skip (QA50B #48): Ein Claim, der
# ein NEGATIVES Prädikat VERNEINT ("so schlecht ist X gar nicht"), zeigt
# in dieselbe Richtung wie der Mythos-widerlegende STRUKTURELL-Marker —
# der Override würde ein korrektes true invertieren. Mythos-Claims
# verneinen dagegen neutrale/positive Fakten ("nicht menschengemacht"),
# die hier bewusst NICHT gelistet sind.
_NEGATIVE_PREDICATES = (
    "schlecht", "katastrophal", "unpünktlich", "unpuenktlich",
    "unzuverlässig", "unzuverlaessig", "marode", "kaputt", "gescheitert",
    "miserabel", "chaotisch", "überlastet", "ueberlastet",
)


def _claim_negates_negative_predicate(claim_lower):
    """True bei Doppel-Verneinungs-Claims wie 'So schlecht ist die
    ÖBB-Pünktlichkeit gar nicht'. Die Negation muss ANS PRÄDIKAT
    GEBUNDEN sein (Review-Befund 2026-07-12: ein freies Fenster ließ
    'Die ÖBB ist schlecht, weil KEIN Zug pünktlich fährt' — also die
    BEHAUPTUNG des Negativ-Prädikats — den Override aushebeln). Zwei
    gebundene Formen: (a) Negation direkt vor dem Prädikat, (b)
    'so <Prädikat> … gar nicht' im selben Teilsatz (kein Komma-
    Übersprung)."""
    for pred in _NEGATIVE_PREDICATES:
        if re.search(
                r"\b(?:nicht|keineswegs|gar nicht)\s+"
                r"(?:so\s+|besonders\s+|wirklich\s+)?" + re.escape(pred),
                claim_lower):
            return True
        if re.search(
                r"\bso\s+" + re.escape(pred) +
                r"\b[^.,;!?]{0,60}?\b(?:gar nicht|nicht|keineswegs)\b",
                claim_lower):
            return True
    return False


def _claim_negates_superlative(claim_lower):
    """True, wenn der Claim seinen Superlativ selbst VERNEINT ('kriegt
    gar nicht die meisten Inserate'). Pattern A/E dürfen dann nicht
    confirm-true flippen: Summary bestätigt den Superlativ fürs
    Claim-Subjekt → der negierte Claim ist damit WIDERLEGT, das
    LLM-false ist korrekt (QA50B #19: false→true-Flip, 3× reproduziert)."""
    return bool(re.search(
        r"(?:gar nicht|nicht|keineswegs)\s+(?:die|der|das|den)\s+"
        r"(?:" + "|".join(_SUPERLATIVE_ADJECTIVES) + r")",
        claim_lower))


def _parse_de_number(raw, tail=""):
    """'9.197.213' / '11,7' / '1,9' + Mio/Mrd-Suffix im tail → float."""
    cleaned = raw.replace("\xa0", "").replace(" ", "").replace(".", "")
    cleaned = cleaned.replace(",", ".")
    try:
        val = float(cleaned)
    except ValueError:
        return None
    if re.match(r"\s*(?:mio\.?|millionen?)", tail):
        val *= 1_000_000
    elif re.match(r"\s*(?:mrd\.?|milliarden?)", tail):
        val *= 1_000_000_000
    return val


def _entity_percent_from_summary(entity, summary_norm, exclude=None):
    """Den EINDEUTIGEN Prozentwert, den die Summary der Entität
    zuschreibt ('kärnten 13,94 %').

    Live-Härtungen 2026-07-12: (1) ALLE Vorkommen scannen — das erste
    ist oft die wertlose Claim-Wiederholung ('… sagt, der Anteil in
    Kärnten sei höher …'); (2) pro Fenster nur der ERSTE Wert (nächste
    Attribution), Fenster endet an Satzgrenze und nächster Entität;
    (3) Verlaufsangaben 'von X % auf Y %' machen das Vorkommen
    mehrdeutig → skip (Review-Befund: historischer Wert drehte die
    Relation); (4) Werte über Vorkommen hinweg dürfen nur Rundungs-
    Varianten sein ('13,94' vs. '13,9', ±0,1 pp) — sonst None;
    (5) optional ``exclude``: Schwellen-Echo ('… also unter 10 %')
    zählt nicht als Entitäts-Wert. Kein Fix ist besser als ein
    falscher."""
    vals = []
    for m in re.finditer(re.escape(entity), summary_norm):
        window = summary_norm[m.end():m.end() + 55]
        window = re.split(r"[.;]", window)[0]
        cut = len(window)
        for other in _AT_BUNDESLAENDER + _COMPARISON_COUNTRIES:
            if other == entity or other in entity or entity in other:
                continue
            pos = window.find(other)
            if 0 <= pos < cut:
                cut = pos
        window = window[:cut]
        if re.search(r"von\s+\d[\d,]*\s*%\s+auf\s+", window):
            continue
        v = None
        for pm in re.finditer(r"(\d{1,3}(?:,\d+)?)\s*%", window):
            # Schranken-Angaben ("liegt unter 15 %") sind KEINE
            # Punktwerte — Live-Variante #9 (2026-07-12): das zweite
            # Vorkommen trug "unter 15 %" und brach das Cluster.
            before = window[max(0, pm.start() - 22):pm.start()]
            if re.search(r"(?:unter|über|ueber|weniger als|mehr als|"
                         r"bis zu|maximal|mindestens|höchstens|"
                         r"hoechstens)\s*$", before):
                continue
            cand = _parse_de_number(pm.group(1))
            if cand is None:
                continue
            if (exclude is not None and exclude > 0
                    and abs(cand - exclude) / exclude <= 0.005):
                continue
            v = cand
            break
        if v is None:
            continue
        vals.append(v)
    if not vals:
        return None
    ref = vals[0]
    return ref if all(abs(v - ref) <= 0.1 for v in vals) else None


def _summary_refutes_superlative(claim_lower, summary_lower):
    """True, wenn die Summary den Superlativ einem ANDEREN Land als dem
    Claim-Subjekt zuschreibt oder ihn fürs Claim-Subjekt explizit verneint
    ("… nicht Österreich", "hat nicht die niedrigste …"). Verhindert, dass
    Pattern A/E ein korrektes false/mostly_false fälschlich auf true
    flippen, nur weil die Superlativ-Phrase ("die niedrigste") irgendwo in
    der Summary vorkommt (Bug #52/#81, aufgedeckt im 100-Gap-Claim-Lauf
    2026-06-27; Wortstellungs-/Tausenderpunkt-Lücken gefixt 2026-07-06
    nach dem Mordraten-Drift)."""
    claim_countries = {c for c in _COMPARISON_COUNTRIES if c in claim_lower}
    if not claim_countries:
        return False
    # Tausender-Punkte neutralisieren ("100.000" -> "100000"), damit die
    # [^.]{0,70}-Fenster unten nicht an Zahlen-Punkten abbrechen.
    summary_norm = re.sub(r"(?<=\d)\.(?=\d)", "", summary_lower)
    # Verneinter Claim-Superlativ in deutscher Wortstellung: die Negation
    # steht am Superlativ, nicht am Land ("Deutschland hat NICHT die
    # niedrigste Mordrate"). Eng gefasst auf dasselbe Adjektiv wie im Claim.
    for adj in _SUPERLATIVE_ADJECTIVES:
        if adj in claim_lower and re.search(
                r"\b(?:nicht|keineswegs)\s+(?:die|der|das|den)\s+" + adj,
                summary_norm):
            return True
    # Explizite Verneinung fürs Claim-Subjekt: "… nicht Österreich"
    for c in claim_countries:
        if re.search(r"nicht\s+(?:das\s+|die\s+)?" + re.escape(c), summary_norm):
            return True
    # Counter-Leader: Superlativ-Phrase nahe einem ANDEREN Land
    for c in _COMPARISON_COUNTRIES:
        if c in claim_countries or c not in summary_norm:
            continue
        if (re.search(_SUPERLATIVE_PHRASE + r"[^.]{0,70}" + re.escape(c),
                      summary_norm)
                or re.search(re.escape(c) + r"[^.]{0,70}" + _SUPERLATIVE_PHRASE,
                             summary_norm)):
            return True
    return False


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
            elif (struct_ratio < 0.5
                    and _claim_negates_negative_predicate(
                        (original_claim or "").lower())):
                # Tier 2b (QA50B #48, 2026-07-12): Der Claim VERNEINT ein
                # negatives Prädikat ("so schlecht ist die ÖBB-Pünktlich-
                # keit gar nicht") — er zeigt in DIESELBE Richtung wie der
                # Mythos-widerlegende Marker. LLM-true ist dann korrekt;
                # der Override würde invertieren. Tier 2 griff nicht, weil
                # die Summary datenbasiert bestätigt ("94,2 %, sehr gut")
                # statt mit expliziter "ist korrekt"-Phrase.
                logger.warning(
                    f"STRUKTURELL FALSCH override SKIPPED (Tier 2b): "
                    f"claim negates a negative predicate (anti-myth "
                    f"direction) + ratio {struct_sources_count}/"
                    f"{total_results_count} = {struct_ratio:.0%} < 50%. "
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
        # Adverb-Toleranz (QA50B #19, 2026-07-12): "ist DAHER falsch" /
        # "ist DAMIT falsch" matchte nicht — verdict_from_summary blieb
        # leer und Pattern A konnte ein korrektes false auf true flippen
        # (Phrasing-Lotterie: mal fing die Regex, mal nicht).
        _ADV = r"(?:daher\s+|damit\s+|somit\s+|also\s+|deshalb\s+)?"
        if re.search(
            r"behauptung.{0,150}ist " + _ADV +
            r"(korrekt|richtig|zutreffend|wahr|belegt)",
            summary_lower
        ):
            verdict_from_summary = "true"
        elif re.search(
            r"behauptung.{0,150}ist " + _ADV +
            r"(falsch|inkorrekt|nicht korrekt|nicht richtig|widerlegt)",
            summary_lower
        ):
            verdict_from_summary = "false"
        # Invertierte Wortstellung: "Damit/Daher/Somit ist die Behauptung
        # falsch" — Verb vor dem Subjekt (2026-07-06, Mordraten-Drift:
        # ohne diesen Zweig blieb verdict_from_summary leer und Pattern A
        # konnte ein korrektes false auf true flippen).
        elif re.search(
            r"ist die behauptung\s+(?:daher\s+|damit\s+|somit\s+|also\s+)?"
            r"(korrekt|richtig|zutreffend|wahr|belegt)",
            summary_lower
        ):
            verdict_from_summary = "true"
        elif re.search(
            r"ist die behauptung\s+(?:daher\s+|damit\s+|somit\s+|also\s+)?"
            r"(falsch|inkorrekt|nicht korrekt|nicht richtig|widerlegt)",
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
        # Top-N-Zugehörigkeits-Claims ("zu den fünf größten Gruppen")
        # sind KEINE Superlativ-Claims — Pattern A's Confirm-Logik gilt
        # dort nicht ("der größte" matcht als Substring in "der größten
        # Gruppen" und bestätigt nichts). Zuständig ist Pattern J mit
        # Subjekt-gebundenem Rang (Review-Befund 2026-07-12).
        if re.search(r"(?:zu|unter)\s+den\s+(?:\w+|\d{1,2})\s+größten",
                     claim_lower):
            superlative_claim = False
        if superlative_claim:
            superlative_confirmed = any(t in summary_lower for t in (
                "die meisten", "die höchste", "der größte",
                "die stärkste", "spitzenreiter", "rang #1",
                "an erster stelle", "die niedrigste",
            ))
            # Guard (Bug #52/#81): the superlative phrase appearing in the
            # summary is NOT a confirmation if it is attributed to a
            # different country ("die niedrigste … hat Deutschland") or
            # negated for the claim subject ("nicht Österreich"). Only then
            # does the LLM's correct false/mostly_false survive.
            # Guard 2 (QA50B #19, 2026-07-12): claims that NEGATE their own
            # superlative ("kriegt gar nicht die meisten Inserate") — the
            # summary confirming the superlative REFUTES the negated claim;
            # the LLM's false is correct and must not be flipped.
            if (superlative_confirmed
                    and not _claim_negates_superlative(claim_lower)
                    and not _summary_refutes_superlative(
                        claim_lower, summary_lower)):
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

        # Pattern B2 (Bug #96, EBA-Inversion): record-LOW claims confirmed
        # by the summary asserting a historical/record low ("ist auf einem
        # historischen Tiefstand", "Rekordtief", "so niedrig wie nie").
        # Mirror of the record-year pattern. Keyword-based, but with a
        # verb-prefix requirement (the low must be ASSERTED of the subject,
        # not just a past record mentioned) plus a contradiction guard,
        # because a false→true override is the most credibility-damaging
        # error class — stay narrow.
        if not factual_confirms:
            record_low_claim = any(t in claim_lower for t in (
                "niedrigste seit", "niedrigsten seit", "niedrigster seit",
                "rekordtief", "tiefstand", "so niedrig wie nie",
                "so wenig wie nie", "allzeittief", "niedrigste je",
            ))
            record_low_confirmed = re.search(
                r"(?:liegt|liege|ist|sind|lag|lagen|befindet sich|erreicht\w*)"
                r"[^.]{0,40}?(?:historische\w*\s+)?"
                r"(?:tiefstand|tiefststand|rekordtief|allzeittief)"
                r"|so niedrig wie nie|niedrigste seit|niedrigsten seit",
                summary_lower,
            )
            record_low_contradicted = any(t in summary_lower for t in (
                "höchststand", "höchster stand", "rekordhoch", "allzeithoch",
                "gestiegen", "angestiegen", "kein tiefstand",
                "nicht der niedrigste", "nicht die niedrigste",
                "nicht auf einem tiefstand",
            ))
            if (record_low_claim and record_low_confirmed
                    and not record_low_contradicted):
                factual_confirms = True
                logger.info(
                    f"Factual-content consistency: record-low claim "
                    f"confirmed in summary but verdict='{verdict}'"
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

            # Refutation guard (Bug #74): if the summary explicitly states
            # the value is BELOW the threshold ("deutlich unter 500"), the
            # "über X" claim is refuted — do NOT treat it as confirmed.
            _threshold_refuted = False
            if threshold_val is not None and threshold_val == int(threshold_val):
                if re.search(
                    r"(?:unter|weniger\s+als|nicht\s+über|nicht\s+mehr\s+als)\s+"
                    r"(?:rund\s+|ca\.?\s*|etwa\s+|knapp\s+|deutlich\s+)?"
                    + re.escape(str(int(threshold_val))) + r"\b",
                    summary_lower,
                ):
                    _threshold_refuted = True

            if (threshold_val is not None and threshold_val > 0
                    and not _threshold_refuted):
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

                    # Skip bare year-like numbers (Bug #74): "ADAC-Tests
                    # 2024" must not count as a confirming value for an
                    # "über 500"-style threshold.
                    if (1900 <= summary_num <= 2100 and not re.match(
                            r"\s*(?:mio|mrd|millionen|milliarden|€|euro|eur)",
                            after_s)):
                        continue

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

    # Pattern C-inverse (QA50B #34, 2026-07-12): claim "war verfassungs-
    # widrig" (OHNE inhaltlich/materiell-Qualifier und OHNE -konform) +
    # Summary bestätigt Nichtigkeit/Kompetenzwidrigkeit → formal KORREKT
    # (2 BvF 1/20: "mit dem GG unvereinbar und nichtig"). Der LLM über-
    # nuanciert deterministisch zu false ("nur formal").
    if (verdict in ("false", "mostly_false") and not verdict_from_summary
            and re.search(r"verfassungswidrig|grundgesetzwidrig",
                          claim_lower)
            and not re.search(r"inhaltlich|materiell|verfassungskonform|"
                              r"grundgesetzkonform|verfassungsgemäß",
                              claim_lower)
            and re.search(r"nichtig|gesetzgebungskompetenz|"
                          r"kompetenzwidrig|mit dem grundgesetz "
                          r"unvereinbar", summary_lower)):
        verdict_from_summary = "mostly_true"
        logger.info(
            "Competence-ruling INVERSE pattern: formal "
            "'verfassungswidrig' claim + summary confirms "
            "nullification → mostly_true"
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

    # Pattern F (Bugs #52/#74/#81, 2026-06-27): the INVERSE of Pattern A/E.
    # A superlative / "über X" comparison claim that the summary REFUTES,
    # while the verdict still says true/mostly_true. The LLM researches and
    # states the refutation correctly ("Deutschland ist mit 49,1 % die
    # niedrigste", "deutlich unter 500 km", "die höchste ... hat die
    # Schweiz") but leaves verdict=true. Pattern A/E only handle the
    # confirm-direction, so these self-contradictions slip through.
    # → push to false / mostly_false.
    if verdict in ("true", "mostly_true") and not verdict_from_summary:
        _SUPER_TERMS = ("niedrigste", "geringste", "höchste", "größte",
                        "meisten", "stärkste")
        superlative_claim = any(t in claim_lower for t in _SUPER_TERMS)
        refuted = False
        target_refute = "false"

        # F-1: threshold refuted — claim "über/mehr als X", summary "unter X".
        thr = re.search(
            r"(?:über|ueber|mehr\s+als|mindestens)\s+"
            r"(\d{1,3}(?:[. ]\d{3})*(?:,\d+)?|\d+(?:,\d+)?)", claim_lower)
        if thr:
            cleaned_t = (thr.group(1).replace(" ", "")
                         .replace(".", "").replace(",", "."))
            try:
                tv = float(cleaned_t)
            except ValueError:
                tv = None
            if tv is not None:
                num_variants = [re.escape(thr.group(1))]
                if tv == int(tv):
                    num_variants.append(str(int(tv)))
                num_re = "|".join(num_variants)
                if re.search(
                    r"(?:unter|weniger\s+als|nicht\s+über|nicht\s+mehr\s+als)\s+"
                    r"(?:rund\s+|ca\.?\s*|etwa\s+|knapp\s+|deutlich\s+)?"
                    rf"(?:{num_re})\b", summary_lower):
                    refuted = True
                    target_refute = "false"

        # F-2: counter-leader — summary attributes the superlative to a
        # DIFFERENT country than the claim's subject, flagged by an
        # adversative ("aber", "während", "jedoch"). Requires the claim to
        # name its own subject country, so we only fire on genuine
        # cross-country comparisons (avoids flipping legit-true claims).
        if not refuted and superlative_claim:
            _COUNTRIES = (
                "deutschland", "schweiz", "niederlande", "frankreich",
                "italien", "rumänien", "kroatien", "slowakei", "tschechien",
                "ungarn", "polen", "spanien", "schweden", "dänemark",
                "norwegen", "belgien", "österreich", "luxemburg", "portugal",
                "griechenland", "finnland", "irland", "bulgarien", "slowenien",
                "estland", "lettland", "litauen", "malta", "zypern")
            claim_countries = {c for c in _COUNTRIES if c in claim_lower}
            adversative = any(a in summary_lower for a in (
                "aber ", "jedoch", "allerdings", "während ", "hingegen",
                " sondern ", "tatsächlich", "in wirklichkeit"))
            if claim_countries and adversative:
                sp = (r"(?:(?:die|der|das)\s+(?:höchste|niedrigste|geringste|"
                      r"größte|meiste\w*|stärkste)\w*|spitzenreiter|"
                      r"an erster stelle|an der spitze|schlusslicht)")
                for c in _COUNTRIES:
                    if c in claim_countries or c not in summary_lower:
                        continue
                    if (re.search(sp + r"[^.]{0,70}" + re.escape(c),
                                  summary_lower)
                            or re.search(re.escape(c) + r"[^.]{0,70}" + sp,
                                         summary_lower)):
                        refuted = True
                        multi = ("zugleich" in claim_lower
                                 or "gleichzeitig" in claim_lower
                                 or sum(claim_lower.count(t)
                                        for t in _SUPER_TERMS) >= 2)
                        target_refute = "mostly_false" if multi else "false"
                        break

        if refuted:
            verdict_from_summary = target_refute
            logger.info(
                f"Factual-content consistency (Pattern F): superlative/"
                f"threshold claim REFUTED by summary but verdict='{verdict}' "
                f"→ {target_refute}")

    # --- Numerische Relations-Muster G/H/I/J (QA50B, 2026-07-12) ---
    # Gemeinsame Wurzel von 5 reproduzierbaren Fehl-Verdicts: die Summary
    # nennt die RICHTIGEN Zahlen, aber das Label kippt — mal roh vom LLM
    # ("Kärnten 13,94 > NÖ 12,57, damit FALSCH"), mal per Phrasing-
    # Lotterie der Schlussformel. Diese Muster werten die Relation aus
    # den summary-eigenen Zahlen aus (lesen + rechnen statt Label
    # trauen) und überstimmen dann auch eine anderslautende
    # Schlussformel. Jedes Muster feuert nur bei eindeutiger Extraktion:
    # Einheiten-Kohärenz, Jahreszahlen-Ausschluss, Schwellen-Echo-
    # Ausschluss, Größenordnungs-Fenster.
    _numeric_fix = None
    _numeric_reason = ""
    # Tausenderpunkte neutralisieren ("9.197.213" → "9197213"), Komma
    # bleibt Dezimaltrenner.
    _sum_norm = re.sub(r"(?<=\d)\.(?=\d)", "", summary_lower)
    _share_claim = any(t in claim_lower for t in (
        "anteil", "quote", "rate", "prozent", "%"))

    # Pattern G — Entitäts-Vergleich (#8): "In Kärnten ist der
    # Ausländeranteil höher als in Niederösterreich" + Summary trägt
    # beide %-Werte → Relation selbst auswerten. Nur %-Claims (Anteil/
    # Quote), nur bei exakt 2 erkannten Entitäten mit je eindeutig
    # zugeschriebenem Wert.
    if _share_claim and verdict in ("true", "mostly_true",
                                    "false", "mostly_false"):
        cmp_m = re.search(
            r"\b(höher|hoeher|größer|groesser|mehr|niedriger|kleiner|"
            r"geringer|weniger)\b[^.]{0,40}?\bals\b", claim_lower)
        if cmp_m:
            _dir_up = cmp_m.group(1) in (
                "höher", "hoeher", "größer", "groesser", "mehr")
            _ents = [e for e in (_AT_BUNDESLAENDER + _COMPARISON_COUNTRIES)
                     if e in claim_lower]
            # Substring-Falle: "österreich" ⊂ "niederösterreich" —
            # Teil-Entitäten verwerfen.
            _ents = [e for e in _ents
                     if not any(e != o and e in o for o in _ents)]
            _ents = sorted(set(_ents), key=lambda e: claim_lower.find(e))
            if len(_ents) == 2:
                _va = _entity_percent_from_summary(_ents[0], _sum_norm)
                _vb = _entity_percent_from_summary(_ents[1], _sum_norm)
                if _va is not None and _vb is not None and _va != _vb:
                    _claim_true = (_va > _vb) if _dir_up else (_va < _vb)
                    _target = "true" if _claim_true else "false"
                    if ((_claim_true and verdict in ("false", "mostly_false"))
                            or (not _claim_true
                                and verdict in ("true", "mostly_true"))):
                        _numeric_fix = _target
                        _numeric_reason = (
                            f"Pattern G Entitäts-Vergleich: {_ents[0]} "
                            f"{_va} % vs. {_ents[1]} {_vb} %")

    # Pattern H — Schwellenwert beidseitig (#9/#14): "unter 10 Prozent" /
    # "über 9,3 Millionen" bei verdict true, obwohl ALLE plausiblen
    # Summary-Werte auf der falschen Seite der Schwelle liegen.
    # Review-Härtungen 2026-07-12: (a) nur wenn KEINE Schlussformel
    # erkannt wurde — eine explizite, zum Label konsistente Konklusion
    # wird nicht von ungebundenen Zahlen überstimmt; (b) Schwellen-Wahl
    # bevorzugt Treffer MIT Einheit und überspringt Alters-
    # Qualifikatoren ("Unter 25-Jährige" ist keine Schwelle);
    # (c) %-Kandidaten werden bei bekannter Claim-Entität an deren
    # Satzfenster gebunden (fremde Vergleichswerte wie "Ö-Schnitt
    # 20,4 %" refuten sonst einen korrekten Burgenland-Claim);
    # (d) Jahres-Ausschluss nur für NACKTE Vierstellen-Tokens ("2025",
    # nicht "2.050 Einwohner").
    if (_numeric_fix is None and not verdict_from_summary
            and verdict in ("true", "mostly_true")):
        _thr_cands = []
        for tm in re.finditer(
                r"\b(unter|über|ueber|mehr als|weniger als|mindestens|"
                r"höchstens|hoechstens)\s+"
                r"(\d{1,3}(?:[. ]\d{3})+(?:,\d+)?|\d+(?:,\d+)?)\s*"
                r"(prozent|%|mio\.?|millionen?|mrd\.?|milliarden?)?",
                claim_lower):
            if re.match(r"\s*-?\s*jährig|\s*jahre\b",
                        claim_lower[tm.end():]):
                continue  # Alters-Qualifikator, keine Schwelle
            _thr_cands.append(tm)
        # Einheiten-tragende Treffer zuerst (der echte Schwellenwert)
        _thr_cands.sort(key=lambda m: 0 if m.group(3) else 1)
        thr_m = _thr_cands[0] if _thr_cands else None
        if thr_m:
            _thr = _parse_de_number(thr_m.group(2), thr_m.group(3) or "")
            _thr_is_pct = (thr_m.group(3) or "") in ("prozent", "%")
            _up = thr_m.group(1) in ("über", "ueber", "mehr als",
                                     "mindestens")
            if _thr and _thr > 0:
                _cands = []
                if _thr_is_pct:
                    _claim_ents = [
                        e for e in (_AT_BUNDESLAENDER
                                    + _COMPARISON_COUNTRIES)
                        if e in claim_lower]
                    _claim_ents = [
                        e for e in _claim_ents
                        if not any(e != o and e in o
                                   for o in _claim_ents)]
                    if len(_claim_ents) == 1:
                        v = _entity_percent_from_summary(
                            _claim_ents[0], _sum_norm, exclude=_thr)
                        if v is not None:
                            _cands.append(v)
                    elif not _claim_ents:
                        for pm in re.finditer(r"(\d{1,3}(?:,\d+)?)\s*%",
                                              _sum_norm):
                            v = _parse_de_number(pm.group(1))
                            if v is not None:
                                _cands.append(v)
                    # >=2 Entitäten: ambig — Pattern-G-Territorium
                else:
                    for nm in re.finditer(
                            r"(\d{1,3}(?:[.\xa0 ]\d{3})+(?:,\d+)?"
                            r"|\d+(?:,\d+)?)", summary_lower):
                        _tail = summary_lower[nm.end():]
                        v = _parse_de_number(nm.group(1), _tail)
                        if v is None:
                            continue
                        # NACKTE Jahreszahl ("2025") ausschließen —
                        # "2.050" (Tausenderpunkt) ist ein Wert
                        if (re.fullmatch(r"(?:19|20)\d\d", nm.group(1))
                                and not re.match(
                                    r"\s*(?:mio|mrd|millionen|"
                                    r"milliarden|%|prozent|€|euro|eur)",
                                    _tail)):
                            continue
                        _cands.append(v)
                # Schwellen-Echo (die Schwelle selbst zitiert) +
                # Größenordnungs-Fenster
                _cands = [v for v in _cands
                          if abs(v - _thr) / _thr > 0.005
                          and _thr / 10 <= v <= _thr * 10]
                if _cands:
                    _above = [v for v in _cands if v > _thr]
                    _below = [v for v in _cands if v < _thr]
                    if _up and not _above and _below:
                        _numeric_fix = "false"
                        _numeric_reason = (
                            f"Pattern H Schwelle: Claim '>{_thr:g}', "
                            f"Summary-Werte alle darunter ({_below[:3]})")
                    elif not _up and not _below and _above:
                        _numeric_fix = "false"
                        _numeric_reason = (
                            f"Pattern H Schwelle: Claim '<{_thr:g}', "
                            f"Summary-Werte alle darüber ({_above[:3]})")

    # Pattern I — Verhältnis (#10): "mehr als doppelt so hoch" bei
    # verdict true, obwohl der Faktor aus den beiden Summary-%-Werten
    # darunter liegt (36,8/20,4 = 1,80 < 2). Nur strikte Claims
    # ("mehr als"), nur bei exakt zwei distinkten %-Werten.
    if (_numeric_fix is None and not verdict_from_summary
            and verdict in ("true", "mostly_true")):
        ratio_m = re.search(
            r"(?:mehr als|über|ueber)\s+(?:(doppelt|zwei\s?mal|dreimal|"
            r"drei\s?mal|viermal|vier\s?mal)\s+so\s+"
            r"(?:hoch|hohe\w*|groß|gross|viel|stark)"
            # QA50C #45: Verbformen "(mehr als) verdoppelt/verdreifacht"
            # — Faktor 1,92 wurde als "mehr als verdoppelt" bestätigt.
            r"|ver(doppelt|dreifacht|vierfacht))", claim_lower)
        if ratio_m:
            _w = (ratio_m.group(1) or ratio_m.group(2)).replace(" ", "")
            _factor = {"doppelt": 2, "zweimal": 2, "dreimal": 3,
                       "viermal": 4, "dreifacht": 3, "vierfacht": 4}[_w]
            _pcts = sorted({_parse_de_number(pm.group(1))
                            for pm in re.finditer(
                                r"(\d{1,3}(?:,\d+)?)\s*%", _sum_norm)
                            if _parse_de_number(pm.group(1))})
            if len(_pcts) == 2 and _pcts[0] > 0:
                _ratio = _pcts[1] / _pcts[0]
                if _ratio <= _factor * 0.98:
                    _numeric_fix = "false"
                    _numeric_reason = (
                        f"Pattern I Verhältnis: {_pcts[1]}/{_pcts[0]} = "
                        f"{_ratio:.2f} < Faktor {_factor}")

    # Pattern J — Top-N-Zugehörigkeit (#12): "gehören zu den zehn
    # größten Gruppen" + Summary "Rang 11" → Rang > N widerlegt die
    # Zugehörigkeit (und umgekehrt). Review-Härtung 2026-07-12: der
    # Rang muss ans CLAIM-SUBJEKT gebunden sein — der erste "Rang N"
    # der Summary kann eine fremde Entität betreffen ("Deutsche auf
    # Rang 1; Syrer erst auf Rang 6"). Subjekt-Stems = Claim-Wörter
    # ≥5 Zeichen minus Struktur-Vokabular, auf 6 Zeichen gekürzt
    # (matcht "Afghanen" ↔ "afghanische"); Bindung rückwärts im
    # selben Satzfenster vor dem Rang.
    if (_numeric_fix is None and not verdict_from_summary
            and verdict in ("true", "mostly_true",
                            "false", "mostly_false")):
        topn_m = re.search(
            r"(?:zu|unter)\s+den\s+(zehn|zwanzig|fünf|fuenf|drei|\d{1,2})"
            r"\s+größten|top[\s-](\d{1,2})\b", claim_lower)
        if topn_m:
            _w2n = {"drei": 3, "fünf": 5, "fuenf": 5, "zehn": 10,
                    "zwanzig": 20}
            _raw_n = topn_m.group(1) or topn_m.group(2) or ""
            _n = _w2n.get(_raw_n) or (int(_raw_n) if _raw_n.isdigit()
                                      else None)
            _J_STOP = {
                "gehören", "gehoeren", "zählen", "zaehlen", "größten",
                "groessten", "gruppen", "gruppe", "österreich",
                "oesterreich", "ausländergruppen", "auslaendergruppen",
                "ausländer", "auslaender", "migranten",
                "migrantengruppen", "herkunftsgruppen", "staaten",
                "länder", "laender", "weltweit", "bevölkerung",
                "bevoelkerung", "unter", "zwischen",
            }
            _subj = {w[:6] for w in re.findall(r"[a-zäöüß]{5,}",
                                               claim_lower)
                     if w not in _J_STOP}
            _rank = None
            for rm in re.finditer(r"\brang\s+(\d{1,3})\b",
                                  summary_lower):
                _back = summary_lower[max(0, rm.start() - 70):rm.start()]
                # Satzgrenze = ". " oder ";" — NICHT der Tausenderpunkt
                # in "55.116 Personen" (der riss die Bindung ab)
                _back = re.split(r"\.\s|;", _back)[-1]
                if any(s in _back for s in _subj):
                    _rank = int(rm.group(1))
                    break
            if _n and _rank is not None:
                _member = _rank <= _n
                if _member and verdict in ("false", "mostly_false"):
                    _numeric_fix = "true"
                    _numeric_reason = (f"Pattern J Top-N: Rang {_rank} "
                                       f"<= {_n}")
                elif not _member and verdict in ("true", "mostly_true"):
                    _numeric_fix = "false"
                    _numeric_reason = (f"Pattern J Top-N: Rang {_rank} "
                                       f"> {_n}")

    if _numeric_fix and _numeric_fix != verdict:
        logger.warning(
            f"Numeric-relation fix ({_numeric_reason}): verdict "
            f"'{verdict}' → '{_numeric_fix}' — Summary-Zahlen schlagen "
            f"das Label.")
        verdict_from_summary = _numeric_fix

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
