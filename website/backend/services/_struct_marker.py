"""Geteilter STRUKTURELL-FALSCH-Marker-Helfer für Pack-Services.

Hintergrund — Synthesizer-Inversions-Falle (Pattern aus
lessons_learned.md): Kurierte Pack-Disclaimers werden vom LLM-
basierten Synthesizer (Mistral) gegen "in seltenen Fällen ist X
möglich"-Live-Quellen verloren. Lösung: explizites
``STRUKTURELL FALSCH:``-Prefix im display_value, das der
Synthesizer-Prompt als authoritative Counter-Evidenz behandelt
(siehe ``services/synthesizer.py`` 'STRUKTURELL FALSCH' marker rule).

Bisher war die Marker-Logik in
``gesundheits_autoritaeten_pack.py`` und ``verschwoerungen_pack.py``
dupliziert mit zwei separaten Token-Listen, die zu driften begannen.
Dieses Modul vereinigt:

1. ``FALSE_VERDICT_OVERRIDE_TOKENS`` — vereinigte Token-Liste mit
   Phrasen, die einen harten Verdict-Override signalisieren.
2. ``has_false_verdict_override(kernsatz)`` — Detektion ob ein
   kernsatz_fuer_synthesizer-String einen Override aktiviert.
3. ``render_data_with_marker(d, skip_keys=())`` — Standard-Render von
   data-dicts mit automatischem STRUKTURELL-Prefix-Wrap.

Wikidata behält seinen eigenen ``_struct_marker(kind=...)``-Helper mit
4 Kind-Varianten (amt/mitgliedschaft/aufloesung/hauptstadt), weil dort
der Marker dynamisch aus end_date + active_positions abgeleitet wird,
nicht aus kernsatz-Tokens.
"""

from __future__ import annotations

# Vereinigte Override-Token-Liste — Superset der vorigen separaten
# Listen aus gesundheits_autoritaeten_pack.py + verschwoerungen_pack.py
# plus Agent-2-Audit-Erweiterungen (mobilitaet/onkologie/tech_ki/
# alltags_mythen-typische Phrasen). Alle Vergleiche erfolgen auf
# UPPERcased kernsatz-Text.
FALSE_VERDICT_OVERRIDE_TOKENS: tuple[str, ...] = (
    # --- Explizite Verdict-Direktiven ---
    "VERDICT MOSTLY_FALSE", "VERDICT IS MOSTLY_FALSE",
    "VERDICT MUSS MOSTLY_FALSE", "VERDICT MOSTLY_FALSE SEIN",
    "MOSTLY_FALSE — VERDICT", "VERDICT MOSTLY_FALSE.",
    "VERDICT DARF NICHT MOSTLY_TRUE", "VERDICT DARF NICHT TRUE",
    "VERDICT-LEITLINIE", "VERDICT MUSS FALSE",
    "VERDICT MOSTLY_FALSE OR FALSE",
    # --- "IST FALSE/FALSCH"-Klassiker ---
    "IST FALSCH. VERDICT", "IST FALSE. VERDICT",
    " IST FALSE", " IST MOSTLY_FALSE",
    "MOSTLY_FALSE BEI",
    "SIND FALSCH UND UNBELEGT", "IST UNBELEGT UND WIDERLEGT",
    "IST FALSCH UND WISSENSCHAFTLICH WIDERLEGT",
    "FALSCH UND WISSENSCHAFTLICH WIDERLEGT",
    "IST ALS FALSCH EINZUORDNEN", "ALS FALSCH EINZUORDNEN",
    "IST FALSCH/NICHT BELEGT", "SIND FALSCH/NICHT BELEGT",
    "IST FALSCH UND TECHNISCH UNMÖGLICH",
    "IST FALSCH UND TECHNISCH UNMOEGLICH",
    # --- Verschwörungs-Narrative ---
    "VERSCHWÖRUNGS-NARRATIV OHNE BELEG",
    "VERSCHWOERUNGS-NARRATIV OHNE BELEG",
    "PHYSIKALISCH UNMÖGLICH UND FALSCH",
    "PHYSIKALISCH UNMOEGLICH UND FALSCH",
    "TECHNISCH UNMÖGLICH UND FALSCH",
    "TECHNISCH UNMOEGLICH UND FALSCH",
    # --- Wissenschaftliche Evidenz fehlt ---
    "IST NICHT DURCH WISSENSCHAFTLICHE EVIDENZ GESTÜTZT",
    "IST NICHT DURCH WISSENSCHAFTLICHE EVIDENZ GESTUETZT",
    # --- Empirisch-Token-Familie (Agent 2-Audit: mobilitaet,
    #     inklusion, demokratie, landwirtschaft, wohnen, arbeitsmarkt,
    #     wirtschaftspolitik formulieren häufig in diesem Stil) ---
    "IST EMPIRISCH FALSCH", "SIND EMPIRISCH FALSCH",
    "IST EMPIRISCH WIDERLEGT", "SIND EMPIRISCH WIDERLEGT",
    "IST EMPIRISCH NICHT BELEGT", "SIND EMPIRISCH NICHT BELEGT",
    "IST EMPIRISCH ÜBERZEICHNET", "IST EMPIRISCH UEBERZEICHNET",
    "EMPIRISCH WIDERLEGT",
    # --- Differenziert-Falsch-Familie (mobilitaet, wohnen,
    #     wirtschaftspolitik, religionsgemeinschaften, datenschutz) ---
    "IST DIFFERENZIERT FALSCH", "SIND DIFFERENZIERT FALSCH",
    "SIND BEIDE DIFFERENZIERT FALSCH",
    "IST FALSCH/IRREFÜHREND", "IST FALSCH/IRREFUEHREND",
    "IST EINZUORDNEN ALS FALSCH/IRREFÜHREND",
    "IST EINZUORDNEN ALS FALSCH/IRREFUEHREND",
    # --- Evidenz-Lücke ---
    "KEINE EVIDENZ FÜR", "KEINE EVIDENZ FUER",
    # --- Harte "ist FALSCH."-Pattern (tech_ki Audit 2026-05-22) ---
    # Mit Punkt am Ende, um differenzierte "ist falsch, weil"-Phrasen
    # NICHT zu matchen. Leading space verhindert false positives
    # wie "kontrastistfalschformulierungen".
    " IST FALSCH.", " SIND FALSCH.",
    "IST FALSCH/MOSTLY_FALSE",
    "FALSCH/MOSTLY_FALSE.",
)


def has_false_verdict_override(kernsatz: str | None) -> bool:
    """True wenn der kernsatz_fuer_synthesizer einen harten Verdict-
    Override-Token enthält (case-insensitive). Bei leerem kernsatz False.
    """
    if not kernsatz:
        return False
    upper = kernsatz.upper()
    return any(token in upper for token in FALSE_VERDICT_OVERRIDE_TOKENS)


def render_data_with_marker(
    d: dict,
    skip_keys: tuple[str, ...] = ("context",),
) -> str:
    """Standard-Render von data-dicts zu Synthesizer-tauglichem Text.

    Wenn der ``kernsatz_fuer_synthesizer`` einen FALSE-Verdict-Override-
    Token enthält, wird der gesamte display-Wert mit ``STRUKTURELL
    FALSCH:`` geprefixt — der Synthesizer-Prompt erkennt das als
    authoritative Counter-Evidenz.

    ``skip_keys``: Keys die nicht in den display-Wert sollen. Default
    schließt ``context`` aus (war in beiden vorigen Implementierungen
    so). Beim STRUKTURELL-Pfad wird ``kernsatz_fuer_synthesizer``
    zusätzlich übersprungen (steckt schon im Prefix).
    """
    kernsatz = d.get("kernsatz_fuer_synthesizer", "")
    is_override = has_false_verdict_override(kernsatz)
    parts: list[str] = []
    if is_override:
        parts.append(f"STRUKTURELL FALSCH: {kernsatz}")
        skip_keys = tuple(set(skip_keys) | {"kernsatz_fuer_synthesizer"})
    for key, val in d.items():
        if key in skip_keys:
            continue
        if isinstance(val, str) and val.strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)
