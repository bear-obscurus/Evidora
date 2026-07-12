"""Numerische Relations-Muster G/H/I/J + Claim-Negations-Gates (QA50B).

50er-Transfer-QA 2026-07-11/12: 6 reproduzierbare Fehl-Verdicts mit einer
gemeinsamen Wurzel — die Summary nennt die richtigen Zahlen, aber das
Label kippt. Der L4-Layer kannte Superlativ/Rekord/Kompetenz/Healing/
kaum, aber NICHT: Entitäts-Vergleich, Schwellenwert, Verhältnis,
Top-N-Zugehörigkeit, Claim-seitige Negation.

Jeder Fix hier ist gegen den echten Live-Fall gepinnt + Gegen-Fälle,
die die BESTEHENDEN Muster schützen (ein Confirm-Override, der falsch
feuert, ist schlimmer als keiner).

Dependency-light: reine Kaskaden-Tests, kein Netz/LLM.
"""

import pytest

from services.verdict_postprocess import apply_verdict_postprocessing


def _run(claim, verdict, summary, confidence=0.9, source_results=None):
    result = {"verdict": verdict, "confidence": confidence,
              "summary": summary}
    return apply_verdict_postprocessing(
        result, source_results or [], claim)


# --- Pattern G: Entitäts-Vergleich (QA50B #8) ---

def test_g_kaernten_noe_false_wird_true():
    """Live 3× reproduziert: Summary rechnet 'Kärnten > NÖ' korrekt vor
    und folgert trotzdem FALSCH — die Zahlen schlagen das Label UND die
    eigene Schlussformel."""
    r = _run(
        "In Kärnten ist der Ausländeranteil höher als in Niederösterreich",
        "false",
        "Daten von Statistik Austria (1.1.2026) zeigen: Kärnten 13,94 %, "
        "Niederösterreich 12,57 %, also Kärnten > Niederösterreich. "
        "Damit ist die Behauptung FALSCH.")
    assert r["verdict"] == "true", r


def test_g_gegenrichtung_true_wird_false():
    r = _run(
        "In Niederösterreich ist der Ausländeranteil höher als in Kärnten",
        "true",
        "Statistik Austria: Kärnten 13,94 %, Niederösterreich 12,57 %.")
    assert r["verdict"] == "false", r


def test_g_feuert_nicht_bei_nur_einem_wert():
    r = _run(
        "In Kärnten ist der Ausländeranteil höher als in Niederösterreich",
        "false",
        "Kärnten hat laut Statistik Austria 13,94 % Ausländeranteil.")
    assert r["verdict"] == "false", r


def test_g_substring_falle_oesterreich_in_niederoesterreich():
    """'österreich' ⊂ 'niederösterreich' darf nicht als dritte Entität
    zählen — sonst bricht die Exakt-2-Bedingung nie."""
    r = _run(
        "In Kärnten ist der Ausländeranteil höher als in Niederösterreich",
        "false",
        "Kärnten 13,94 %, Niederösterreich 12,57 % (Statistik Austria).")
    assert r["verdict"] == "true", r


# --- Pattern H: Schwellenwert beidseitig (QA50B #9 + #14) ---

def test_h_burgenland_unter_10_true_wird_false():
    r = _run(
        "Im Burgenland liegt der Ausländeranteil unter 10 Prozent",
        "true",
        "Laut Statistik Austria (1.1.2026) liegt der Anteil der "
        "Nicht-Österreicher:innen im Burgenland bei 11,7 %.")
    assert r["verdict"] == "false", r


def test_h_ueber_9_3_millionen_true_wird_false():
    """#14: Jahreszahlen (2025) müssen ausgeschlossen bleiben,
    Tausenderpunkt-Zahlen (9.197.213) korrekt parsen."""
    r = _run(
        "Österreich hat inzwischen über 9,3 Millionen Einwohner",
        "true",
        "Eurostat gibt für Österreich am 1. Januar 2025 eine Bevölkerung "
        "von 9.197.213 Personen an, die World Bank schätzt 9.208.163 für "
        "2025. Bei rund 9,3 Millionen liegt die Behauptung im Rahmen.",
        confidence=0.95)
    assert r["verdict"] == "false", r


def test_h_feuert_nicht_wenn_wert_ueber_schwelle_bestaetigt():
    """Bitcoin-/Preis-Klasse (Pattern-E-Domäne): bestätigende Werte über
    der Schwelle dürfen kein Refute auslösen."""
    r = _run(
        "Das Gerät kostet über 1000 Euro",
        "true",
        "Der Listenpreis liegt bei 1.095 € laut Hersteller.")
    assert r["verdict"] == "true", r


def test_h_feuert_nicht_bei_reinem_schwellen_echo():
    """Summary zitiert nur die Schwelle selbst ('unter 10 %') — keine
    unabhängigen Werte, kein Fix."""
    r = _run(
        "Im Burgenland liegt der Ausländeranteil unter 10 Prozent",
        "true",
        "Ob der Wert unter 10 % liegt, lässt sich nicht belegen.")
    assert r["verdict"] == "true", r


# --- Pattern I: Verhältnis (QA50B #10) ---

def test_i_mehr_als_doppelt_bei_faktor_1_8_wird_false():
    r = _run(
        "Wien hat einen mehr als doppelt so hohen Ausländeranteil wie "
        "der Österreich-Schnitt",
        "true",
        "Wien hat laut Statistik Austria (1.1.2026) einen Ausländeranteil "
        "von 36,8 %, während der Österreich-Schnitt bei 20,4 % liegt.")
    assert r["verdict"] == "false", r


def test_i_feuert_nicht_wenn_faktor_erreicht():
    r = _run(
        "Wien hat einen mehr als doppelt so hohen Ausländeranteil wie "
        "der Österreich-Schnitt",
        "true",
        "Wien liegt bei 45,0 %, der Österreich-Schnitt bei 20,0 %.")
    assert r["verdict"] == "true", r


def test_i_feuert_nicht_bei_drei_prozentwerten():
    """Mehr als zwei %-Werte = ambig — kein Fix (welche zwei vergleichen?)."""
    r = _run(
        "Wien hat einen mehr als doppelt so hohen Ausländeranteil wie "
        "der Österreich-Schnitt",
        "true",
        "Wien 36,8 %, Österreich 20,4 %, Burgenland 11,7 %.")
    assert r["verdict"] == "true", r


# --- Pattern J: Top-N-Zugehörigkeit (QA50B #12) ---

def test_j_rang_11_widerlegt_top_10():
    r = _run(
        "Afghanen gehören zu den zehn größten Ausländergruppen in "
        "Österreich",
        "true",
        "Laut Statistik Austria (1.1.2026) liegen Afghanen mit 55.116 "
        "Personen auf Rang 11 der größten Ausländergruppen in Österreich, "
        "nicht in den Top 10.")
    assert r["verdict"] == "false", r


def test_j_rang_9_bestaetigt_top_10():
    r = _run(
        "Ukrainer gehören zu den zehn größten Ausländergruppen in "
        "Österreich",
        "false",
        "Ukrainische Staatsangehörige liegen mit 94.030 Personen auf "
        "Rang 9 der größten Gruppen.")
    assert r["verdict"] == "true", r


# --- Claim-Negations-Gate für Pattern A (QA50B #19) ---

def test_a_gate_negierter_superlativ_bleibt_false():
    """'kriegt GAR NICHT die meisten' + Summary bestätigt den Superlativ
    → der negierte Claim ist widerlegt, LLM-false ist korrekt. Vorher
    flippte Pattern A auf true (3× reproduziert), sobald die
    Schlussformel-Regex die Phrasing-Lotterie verlor."""
    r = _run(
        "Die Krone kriegt gar nicht die meisten öffentlichen Inserate",
        "false",
        "Die Krone (Mediaprint) erhielt 2024 mit 22,4 Mio. € die meisten "
        "öffentlichen Inserate und ist damit Spitzenreiter vor Heute.")
    assert r["verdict"] == "false", r


def test_a_positiver_superlativ_flippt_weiter():
    """Regressions-Schutz Bug #47: der ORIGINALE Pattern-A-Fall (positiver
    Krone-Claim, LLM-false trotz bestätigender Daten) muss weiter auf
    true korrigiert werden."""
    r = _run(
        "Die Krone bekommt die meisten Inserate von der öffentlichen Hand",
        "false",
        "Die Krone erhielt 2024 mit 22,4 Mio. € die meisten öffentlichen "
        "Inserate und ist damit Spitzenreiter vor Heute und oe24.")
    assert r["verdict"] == "true", r


# --- Adverb-Toleranz der Schlussformel (QA50B #19-Varianz) ---

def test_schlussformel_mit_adverb_wird_erkannt():
    """'Die Behauptung ist DAHER falsch' setzte verdict_from_summary
    nicht — die Phrasing-Lotterie entschied, ob Pattern A durchkam."""
    r = _run(
        "Irgendein Vergleichs-Claim ohne Zahlen",
        "true",
        "Die Quellen zeigen das Gegenteil. Die Behauptung ist daher "
        "falsch.")
    assert r["verdict"] == "false", r


# --- L2 Tier-2b: Negation eines Negativ-Prädikats (QA50B #48) ---

def _struct_sources(n_total=4, n_struct=1):
    results = []
    for i in range(n_total):
        dv = ("STRUKTURELL FALSCH: Der Mythos ist widerlegt."
              if i < n_struct else f"Datenpunkt {i}")
        results.append({"display_value": dv})
    return [{"source": "Verkehr Österreich", "results": results}]


def test_tier2b_doppelnegation_skippt_struct_override():
    """'So schlecht ist die ÖBB-Pünktlichkeit gar nicht' zeigt in
    DIESELBE Richtung wie der Mythos-widerlegende Marker — der Override
    invertierte live ein korrektes true zu mostly_false@0.85 (3×
    reproduziert). Tier 2 griff nicht, weil die Summary datenbasiert
    bestätigt statt mit 'ist korrekt'-Phrase."""
    r = _run(
        "So schlecht ist die ÖBB-Pünktlichkeit gar nicht",
        "true",
        "Die ÖBB-Pünktlichkeit lag 2024 im Nahverkehr bei 94,2 % und im "
        "Fernverkehr bei 88,7 %, deutlich über der Deutschen Bahn.",
        source_results=_struct_sources(4, 1))
    assert r["verdict"] == "true", r
    assert not r.get("_struct_override_fired")


def test_tier2b_mythos_via_negation_feuert_weiter():
    """Kontroll-Fall: 'nicht menschengemacht' ist eine Negation, aber
    KEIN Negativ-Prädikat — der Mythos wird per Negation BEHAUPTET,
    der Override muss weiter feuern."""
    r = _run(
        "Der Klimawandel ist nicht menschengemacht",
        "true",
        "Mehrere Quellen diskutieren die Ursachen des Klimawandels.",
        source_results=_struct_sources(4, 1))
    assert r["verdict"] == "mostly_false", r
    assert r["confidence"] == 0.85, r


def test_tier1_skip_bleibt_unveraendert():
    """Tier-1-Regression: ratio < 15 % bleibt unbedingter Skip."""
    r = _run(
        "Der Klimawandel ist nicht menschengemacht",
        "true",
        "Mehrere Quellen diskutieren die Ursachen.",
        source_results=_struct_sources(10, 1))
    assert r["verdict"] == "true", r


# --- Bestehende Muster unberührt (Stichproben) ---

def test_mordraten_negation_bleibt_geschuetzt():
    """Bug #52/#81-Klasse: Superlativ einem anderen Land zugeschrieben →
    Pattern A darf weiterhin NICHT flippen."""
    r = _run(
        "Deutschland hat die niedrigste Mordrate aller EU-Staaten",
        "false",
        "Die niedrigste Mordrate hat Luxemburg, nicht Deutschland.")
    assert r["verdict"] == "false", r


def test_kompetenz_urteil_pattern_unveraendert():
    """Pattern C (Bug #6): Kompetenz-Urteil bleibt mostly_false."""
    r = _run(
        "Der Berliner Mietendeckel war verfassungskonform",
        "false",
        "Das BVerfG erklärte das Gesetz wegen fehlender "
        "Gesetzgebungskompetenz für nichtig.")
    assert r["verdict"] == "mostly_false", r

# --- Review-Befunde (adversarialer 3-Linsen-Review, 2026-07-12) ---
# Alle 6 waren gegen die ERSTE Fassung der Muster reproduzierbar —
# hier gepinnt, damit sie nie wieder scharf werden.

def test_review_j_rang_ohne_subjektbindung_feuert_nicht():
    """Erster 'Rang N' der Summary gehört einer FREMDEN Entität —
    ungebundenes Matching flippte false→true."""
    r = _run(
        "Syrer gehören zu den fünf größten Ausländergruppen in Österreich",
        "false",
        "Die größte Gruppe sind deutsche Staatsangehörige auf Rang 1; "
        "Syrer folgen erst auf Rang 6 der größten Gruppen.")
    assert r["verdict"] == "false", r


def test_review_j_spiegelfall_fremder_rang_zuerst():
    r = _run(
        "Deutsche gehören zu den drei größten Ausländergruppen in "
        "Österreich",
        "true",
        "Während türkische Staatsangehörige auf Rang 4 liegen, sind "
        "Deutsche auf Rang 1 die größte Gruppe.")
    assert r["verdict"] == "true", r


def test_review_h_altersqualifikator_ist_keine_schwelle():
    """'Unter 25-Jährige' wurde als Schwelle 25 geparst und kippte ein
    korrektes true — Schwellen-Wahl muss Einheiten-Treffer bevorzugen
    und Alters-Tails überspringen."""
    r = _run(
        "Unter 25-Jährige stellen mehr als 40 Prozent der Arbeitslosen "
        "in Wien",
        "true",
        "Laut AMS sind 40,2 Prozent der Arbeitslosen in Wien unter 25 "
        "Jahre alt.")
    assert r["verdict"] == "true", r


def test_review_tier2b_behauptetes_negativpraedikat_skippt_nicht():
    """'Die ÖBB ist schlecht, weil KEIN Zug pünktlich fährt' BEHAUPTET
    das Negativ-Prädikat — die freie Fenster-Negation hebelte den
    STRUKT-Override aus. Negation muss ans Prädikat gebunden sein."""
    r = _run(
        "Die ÖBB ist schlecht, weil kein Zug pünktlich fährt",
        "true",
        "Diverse Daten ohne Confirm-Phrase.",
        source_results=_struct_sources(4, 1))
    assert r["verdict"] == "mostly_false", r


def test_review_g_verlaufsangabe_verhindert_fix():
    """'sank von 14,1 % auf 12,57 %' lieferte den HISTORISCHEN Wert als
    Entitäts-Wert und drehte die Relation — mehrdeutige Fenster dürfen
    nicht feuern."""
    r = _run(
        "In Kärnten ist der Ausländeranteil höher als in Niederösterreich",
        "true",
        "Laut Statistik Austria sank der Anteil in Niederösterreich von "
        "14,1 % auf 12,57 %, während Kärnten bei 13,94 % liegt.")
    assert r["verdict"] == "true", r


def test_review_h_tausenderpunkt_zahl_ist_keine_jahreszahl():
    """'2.050 Einwohner' fiel in den 1900–2100-Jahresfilter, der
    bestätigende Wert verschwand und ein Nebenwert (310) refutete —
    Jahres-Ausschluss gilt nur für nackte Vierstellen-Tokens."""
    r = _run(
        "Der Ort hat über 1.900 Einwohner",
        "true",
        "Der Ort zählt 2.050 Einwohner, davon 310 Ausländer.")
    assert r["verdict"] == "true", r


def test_review_h_fremde_prozentwerte_refuten_nicht():
    """Ö-Schnitt 20,4 % und Wien 36,8 % sind FREMD-Entitäten — sie
    dürfen einen Burgenland-Claim nicht widerlegen, wenn die Summary
    dem Subjekt keinen Wert zuschreibt."""
    r = _run(
        "Im Burgenland liegt der Ausländeranteil unter 10 Prozent",
        "true",
        "Der Österreich-Schnitt liegt bei 20,4 %, in Wien sogar bei "
        "36,8 %; das Burgenland liegt deutlich darunter.")
    assert r["verdict"] == "true", r


def test_review_pattern_a_top_n_claims_ausgenommen():
    """'der größte' matcht als Substring in 'der größtEN Gruppen' —
    Pattern A bestätigte damit Top-N-Claims, die es gar nicht
    beurteilen kann (Pattern-J-Territorium)."""
    r = _run(
        "Syrer gehören zu den fünf größten Ausländergruppen in Österreich",
        "false",
        "Die größte Gruppe sind deutsche Staatsangehörige auf Rang 1 "
        "der größten Gruppen.")
    assert r["verdict"] == "false", r
