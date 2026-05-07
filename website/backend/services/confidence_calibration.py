"""Konfidenz-Kalibrierung — Gegen LLM-Übersicherheit.

Beobachtetes Problem (Sprint 2026-05-03 Daten-Analyse, 178 Claims):
  - 70.8 % aller Konfidenzen exakt 0.95
  - 86.5 % im Bereich 0.85-0.95
  - Mittlere Bereiche (0.50-0.85) fast leer
  - Bimodal: hochkonfident ODER very-low ('unverifiable' 0.0-0.10)

Ursachen:
  1. LLM tendiert zu 'round numbers' (0.85, 0.9, 0.95)
  2. Synthesizer-Prompt ankert auf '95-100% bei klarer Evidenz' ohne
     präzise Mittel-Bereich-Beispiele
  3. Strafanreiz fehlt: zu hohe Konfidenz wird nie bestraft

Lösung (Hybrid, Vorschlag C):
  1. Prompt mit granularer 6-Stufen-Skala (in synthesizer.py)
  2. Post-Processing-Cap basierend auf objektiven Quellen-Metriken
     (diese Datei)
  3. Authoritative-Pack-Boost: kuratierte Static-First-Packs (CDC, BfV,
     BAMF, BfR…) zählen als methodisch starke Quellen — deren Hits
     werden NICHT unfair gecappt nur weil "nur 1 Quelle"

Verwendung in main.py:
    from services.confidence_calibration import calibrate_confidence
    synthesis["confidence"] = calibrate_confidence(
        raw_conf=synthesis["confidence"],
        source_coverage=synthesis["source_coverage"],
        evidence=synthesis.get("evidence", []),
        sources_used=hit_names,
    )
"""

from __future__ import annotations

import logging

logger = logging.getLogger("evidora")

# Substring-Markers für Authoritative-Static-First-Packs in den
# Source-Names. Diese Packs zählen als methodisch starke Einzelquellen
# weil sie hand-kuratierte Inhalte aus behördlichen / akademischen
# Top-Quellen aggregieren.
AUTHORITATIVE_PACK_MARKERS = (
    "Esoterik-Pack",
    "Geschichts-Pack",
    "Geschichte-Pack",
    "Verschwörungen",
    "Tech-/KI-Faktencheck",
    "Gesundheits-Autoritäten",
    "Tier-/Natur-Mythen",
    "Ernährungs-Mythen",
    "Recht/Rechtsmythen",
    "Energie/Klima-Politik",
    "Migrations-Konsens",
    "Geographie",
    "Eurobarometer",
    "Finanzen-Mythen",
    "Bildungs-Mythen",
    "Internationale Quellen",
    "DESTATIS",
    "Sport-/Fitness-Mythen",
    "Kunst-/Kultur-Mythen",
    "Geschichts-Mythen-2",
    "Reproduktions-Medizin-Konsens",
    "Onkologie-Konsens",
    "Mental-Health-Konsens",
    "Substanzen-Konsens",
    "Digital-Familie-Konsens",
    "Geldanlage-Konsens",
    "Alltags-Mythen-Konsens",
    "Verkehrssicherheit-Konsens",
    "Tierhaltung-Konsens",
    "Cybersecurity-Konsens",
    "Lebensmittel-Sicherheit-Konsens",
    "Gleichstellung-Konsens",
    "Religionsgemeinschaften-Konsens",
    "Wirtschaftspolitik-Konsens",
    "Wohnen-Konsens",
    "Arbeitsmarkt-Konsens",
    "Mobilität-Konsens",
    "Datenschutz-Konsens",
    "Sozialstaat-Konsens",
    "Demokratie-Konsens",
    "Landwirtschaft-Konsens",
    "Welthandel-Konsens",
    "Inklusion-Konsens",
    "Sicherheitspolitik-Konsens",
)

# Caps abhängig von Quellen-Anzahl (n_sources_with_results).
# Generiert für den FALL OHNE authoritative Pack — strenger.
SOURCE_COUNT_CAPS = {
    1: 0.65,
    2: 0.80,
    3: 0.88,
    4: 0.93,
    # 5+: kein Cap (raw_conf bleibt unverändert)
}

# Caps wenn authoritative Pack gefeuert hat — milder, weil Pack als
# methodisch starke Einzelquelle zählt.
SOURCE_COUNT_CAPS_WITH_PACK = {
    1: 0.85,  # nur Pack ohne Begleit-Quellen
    2: 0.92,
    3: 0.95,
    # 4+: kein Cap
}

# Quellen, die als "crowdsourced encyclopedia" gelten und keine
# eigenständige peer-reviewed Verifikation darstellen. Bei AUSSCHLIESSLICH
# diesen Quellen wird die Konfidenz auf 0.55 gecappt (Verdict bleibt
# erlaubt, aber kommuniziert "weiche Quellenlage").
WIKIPEDIA_ONLY_MARKERS = (
    "Wikipedia",
    "Wikidata",
)
WIKIPEDIA_ONLY_CAP = 0.55

# Politisch normativ-wertende Klassifikatoren — bei Auftreten im Claim
# UND nur Wikipedia/Wikidata-Treffern wird zusätzlich verschärft auf 0.50.
# Diese Liste deckt die häufigsten umstrittenen Polit-Labels ab.
NORMATIVE_POLITICAL_TERMS = (
    "rechtsextrem", "rechts-extrem", "right-wing extremist",
    "linksextrem", "links-extrem", "left-wing extremist",
    "extremistisch", "extremist",
    "populistisch", "populist", "populism",
    "autoritär", "autoritaer", "authoritarian",
    "antidemokratisch", "anti-demokratisch", "anti-democratic",
    "verfassungsfeindlich", "verfassungs-feindlich",
    "revisionistisch", "revisionist",
    "identitär", "identitaer", "identitarian",
    "faschistisch", "fascist",
    "totalitär", "totalitaer", "totalitarian",
    "rechtsradikal", "rechts-radikal",
    "linksradikal", "links-radikal",
)
NORMATIVE_TERM_WIKIPEDIA_ONLY_CAP = 0.50


def _has_authoritative_pack(sources_used: list[str]) -> bool:
    """Prüft, ob ein kuratierter Static-First-Pack unter den
    Quellen-mit-Ergebnissen ist."""
    if not sources_used:
        return False
    return any(
        any(marker.lower() in src.lower() for marker in AUTHORITATIVE_PACK_MARKERS)
        for src in sources_used
    )


def _is_wikipedia_only(sources_used: list[str]) -> bool:
    """True wenn ALLE Quellen-mit-Treffern aus Wikipedia/Wikidata sind
    (mindestens eine, und keine anderen).

    Beispiele:
        ["Wikipedia"] → True
        ["Wikidata", "Wikipedia"] → True
        ["Wikipedia", "GDELT v2 GKG"] → False (GDELT ist andere Quelle)
        ["Wohnen-Konsens"] → False (Static-First-Pack ist andere Quelle)
        [] → False (kein Source — kein Cap zu rechtfertigen)
    """
    if not sources_used:
        return False
    for src in sources_used:
        is_wp = any(m.lower() in src.lower() for m in WIKIPEDIA_ONLY_MARKERS)
        if not is_wp:
            return False
    return True  # alle sind Wikipedia/Wikidata


def _claim_has_normative_term(claim: str | None) -> bool:
    """True wenn der Claim einen politisch-wertenden Klassifikator-Term
    enthält (rechtsextrem, populistisch, autoritär, etc.).

    Diese Begriffe sind in Politikwissenschaft + Rechtswissenschaft
    umstritten und sollten nicht als selbst-vergebene Wertungen
    übernommen werden — nur als zitierte Beobachtung mit klarem Quellen-
    Verweis. Substring-Match auf lowercased claim.
    """
    if not claim:
        return False
    claim_lc = claim.lower()
    return any(t in claim_lc for t in NORMATIVE_POLITICAL_TERMS)


def _evidence_strength_cap(evidence: list[dict]) -> float | None:
    """Cap basierend auf Mix der evidence[].strength-Werte.

    Returns ein Cap-Wert (z.B. 0.75) oder None wenn kein zusätzlicher
    Cap nötig ist.
    """
    if not evidence:
        return 0.70  # keine Evidenz-Items → niedrig
    strong = sum(1 for e in evidence if isinstance(e, dict)
                 and e.get("strength") == "strong")
    moderate = sum(1 for e in evidence if isinstance(e, dict)
                   and e.get("strength") == "moderate")
    weak = sum(1 for e in evidence if isinstance(e, dict)
               and e.get("strength") == "weak")
    total = strong + moderate + weak
    if total == 0:
        # Strength fehlt im Output — kein zusätzlicher Cap, auf
        # Source-Count-Cap verlassen
        return None

    if strong == 0 and moderate == 0:
        return 0.70  # nur weak
    if strong == 0 and moderate < 2:
        return 0.78  # 1 moderate, kein strong
    if strong == 0:
        return 0.85  # 2+ moderate, kein strong
    if strong < 2:
        return 0.90  # 1 strong + Begleit
    # 2+ strong → kein zusätzlicher Cap
    return None


def calibrate_confidence(
    raw_conf: float | None,
    source_coverage: dict | None = None,
    evidence: list[dict] | None = None,
    sources_used: list[str] | None = None,
    claim: str | None = None,
) -> tuple[float, dict]:
    """Kalibriert Konfidenz-Wert vom Synthesizer auf Basis objektiver
    Quellen-Metriken.

    Returns:
        (calibrated_conf, debug_info_dict)

    debug_info enthält:
        - raw_conf: Original-Wert
        - cap_source_count: Cap aus Quellen-Anzahl (None wenn kein Cap)
        - cap_evidence_strength: Cap aus Evidence-Strength (None wenn kein Cap)
        - cap_wikipedia_only: 0.55 wenn nur Wikipedia/Wikidata, sonst None
        - cap_normative_wikipedia: 0.50 wenn Wikipedia-only + normativer Term, sonst None
        - authoritative_pack: bool ob ein Pack gefeuert hat
        - wikipedia_only: bool ob nur Wikipedia/Wikidata Treffer hatten
        - normative_term: bool ob Claim einen politisch-wertenden Term enthält
        - n_sources_with_results: Anzahl der Quellen mit Treffern
        - applied_cap: der Cap-Wert, der finally angewandt wurde (None wenn kein Cap)
    """
    debug = {
        "raw_conf": raw_conf,
        "cap_source_count": None,
        "cap_evidence_strength": None,
        "cap_wikipedia_only": None,
        "cap_normative_wikipedia": None,
        "authoritative_pack": False,
        "wikipedia_only": False,
        "normative_term": False,
        "n_sources_with_results": 0,
        "applied_cap": None,
    }

    if raw_conf is None:
        return 0.0, debug

    # Edge-Case: very-low conf für 'unverifiable' bleibt unverändert
    if raw_conf <= 0.15:
        return raw_conf, debug

    # Source-Coverage extrahieren
    source_coverage = source_coverage or {}
    n_with_results = source_coverage.get("with_results", 0)
    debug["n_sources_with_results"] = n_with_results

    # Authoritative-Pack-Check
    has_pack = _has_authoritative_pack(sources_used or [])
    debug["authoritative_pack"] = has_pack

    # Source-Count-Cap auswählen
    caps_table = SOURCE_COUNT_CAPS_WITH_PACK if has_pack else SOURCE_COUNT_CAPS
    cap_source = caps_table.get(n_with_results)
    debug["cap_source_count"] = cap_source

    # Evidence-Strength-Cap
    cap_evidence = _evidence_strength_cap(evidence or [])
    debug["cap_evidence_strength"] = cap_evidence

    # Wikipedia-only-Cap (Crowdsourced-Quelle ist methodisch schwächer
    # als peer-reviewed / behördlich kuratiert)
    wp_only = _is_wikipedia_only(sources_used or [])
    debug["wikipedia_only"] = wp_only
    cap_wp = WIKIPEDIA_ONLY_CAP if wp_only else None
    debug["cap_wikipedia_only"] = cap_wp

    # Verschärfung: Wikipedia-only + normativer Term im Claim →
    # zusätzlich auf 0.50, weil normativer Term + crowdsourced-Quelle
    # = doppeltes Bias-Risiko
    has_normative = _claim_has_normative_term(claim)
    debug["normative_term"] = has_normative
    cap_norm_wp = (
        NORMATIVE_TERM_WIKIPEDIA_ONLY_CAP
        if (wp_only and has_normative)
        else None
    )
    debug["cap_normative_wikipedia"] = cap_norm_wp

    # Strengster Cap gewinnt
    candidate_caps = [
        c for c in (cap_source, cap_evidence, cap_wp, cap_norm_wp)
        if c is not None
    ]
    if not candidate_caps:
        return raw_conf, debug
    final_cap = min(candidate_caps)
    debug["applied_cap"] = final_cap

    if raw_conf > final_cap:
        logger.info(
            f"confidence_calibration: capped {raw_conf:.2f} → {final_cap:.2f} "
            f"(n_sources={n_with_results}, pack={has_pack}, wp_only={wp_only}, "
            f"normative={has_normative}, cap_src={cap_source}, "
            f"cap_ev={cap_evidence}, cap_wp={cap_wp}, cap_norm_wp={cap_norm_wp})"
        )
        return final_cap, debug

    return raw_conf, debug
