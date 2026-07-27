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


# --- Live-Verifikations-Nachzügler (Prod-Summaries 2026-07-12) ---

def test_g_live_claim_wiederholung_und_gerundete_zweitwerte():
    """Prod-Summary #8: erstes Entitäts-Vorkommen ist die wertlose
    Claim-Wiederholung, spätere Vorkommen streuen gerundete Varianten
    ('13,9' neben '13,94'). Alle Vorkommen scannen + Rundungs-Cluster
    tolerieren."""
    r = _run(
        "In Kärnten ist der Ausländeranteil höher als in "
        "Niederösterreich (l4w-8)",
        "false",
        "Die Behauptung sagt, der Ausländeranteil in Kärnten sei höher "
        "als in Niederösterreich. Daten von Statistik Austria (1.1.2026) "
        "zeigen: Kärnten 13,94 %, Niederösterreich 12,57 %. Damit ist "
        "Kärnten höher, aber die Differenz beträgt nur 1,37 "
        "Prozentpunkte. Die Behauptung ist jedoch falsch, da die exakten "
        "Werte Kärnten (13,9 %) und Niederösterreich (12,6 %) in der "
        "Quelle als 13,94 % und 12,57 % angegeben sind.")
    assert r["verdict"] == "true", r


def test_h_live_schwellen_echo_im_entitaets_fenster():
    """Prod-Summary #9: 'bei 11,7 %, also unter 10 %' — das Schwellen-
    Echo steht IM Entitäts-Fenster und brach die Eindeutigkeit; per
    exclude-Filter zählt nur der echte Subjekt-Wert."""
    r = _run(
        "Im Burgenland liegt der Ausländeranteil unter 10 Prozent (l4w-9)",
        "true",
        "Laut Statistik Austria (1.1.2026) liegt der Anteil der "
        "Nicht-Österreicher:innen im Burgenland bei 11,7 %, also unter "
        "10 % ist nicht korrekt, aber knapp darüber. Die Behauptung ist "
        "damit fast korrekt.")
    assert r["verdict"] == "false", r


def test_h_live_schranken_angabe_bricht_cluster_nicht():
    """Prod-Variante 3 von #9: zweites Entitäts-Vorkommen trägt eine
    Schranken-Angabe ('unter 15 %') statt eines Punktwerts — Schranken
    sind keine Attributions-Werte."""
    r = _run(
        "Im Burgenland liegt der Ausländeranteil unter 10 Prozent (l4y-9)",
        "true",
        "Laut Statistik Austria (1.1.2026) liegt der Anteil der "
        "Nicht-Österreicher:innen im Burgenland bei 11,7 %, was unter "
        "10 % widerlegt. Allerdings zeigt der ÖIF-Integrationsbericht "
        "2023, dass der Ausländeranteil im Burgenland unter 15 % liegt, "
        "und die AT Factbook-Daten bestätigen 11,7 % für 2026.",
        confidence=0.95)
    assert r["verdict"] == "false", r


def test_c_inverse_formale_verfassungswidrigkeit():
    """QA50B #34: 'war verfassungswidrig' + Summary bestätigt Nichtig-
    keit/Kompetenz → mostly_true; 'verfassungskonform' bleibt bei
    Pattern C (mostly_false)."""
    r = _run("Der Berliner Mietendeckel war verfassungswidrig", "false",
             "Das BVerfG kippte den Mietendeckel nicht wegen inhaltlicher "
             "Verfassungswidrigkeit, sondern wegen fehlender "
             "Gesetzgebungskompetenz Berlins; das Gesetz wurde für "
             "nichtig erklärt.")
    assert r["verdict"] == "mostly_true", r
    r2 = _run("Der Mietendeckel war inhaltlich verfassungswidrig", "mostly_false",
              "Das Gesetz wurde wegen fehlender Gesetzgebungskompetenz "
              "für nichtig erklärt; inhaltlich erging kein Urteil.")
    assert r2["verdict"] == "mostly_false", r2


def test_i_verbform_mehr_als_verdoppelt():
    """QA50C #45: 'mehr als verdoppelt' bei Faktor 1,92 → false;
    Verbform fehlte im Pattern-I-Regex."""
    r = _run(
        "Der Ausländeranteil in Österreich hat sich seit 2010 mehr als "
        "verdoppelt",
        "true",
        "Der Anteil stieg laut Statistik Austria auf 20,4 %; 2010 lag er "
        "bei 10,6 %.")
    assert r["verdict"] == "false", r


# --- Pattern G2: Entitäts-Vergleich ohne Geografie (QA100 #44) ---
#
# Alle Summaries in diesem Block sind die ECHTEN Prod-Ausgaben vom
# 2026-07-26 (Fixtures ≠ echte LLM-Summaries, 3-Iterationen-Lehre) —
# der Live-Fall scheiterte an ZWEI Details, die kein Fixture getroffen
# hätte: dem _share_claim-Gate (der Claim sagt nirgends "Anteil") und
# dem 55-Zeichen-Fenster, das ausgerechnet das '%' hinter "13,8"
# abschnitt.

_S44 = ("Laut APG/E-Control lag der Anteil von Windkraft an der "
        "österreichischen Stromproduktion 2024 bei 13,8 %, der von "
        "Photovoltaik bei 7,5 %. Windkraft liefert damit fast doppelt "
        "so viel Strom wie Photovoltaik, nicht mehr.")


def test_g2_windkraft_pv_false_wird_true():
    """QA100 #44, 5/5 deterministisch: Summary nennt beide Werte und
    dementiert dann verbal ('… nicht mehr'). Pattern G kannte nur
    Bundesländer/Länder."""
    r = _run("Windkraft liefert in Österreich mehr Strom als Photovoltaik",
             "false", _S44)
    assert r["verdict"] == "true", r


def test_g2_gegenrichtung_true_wird_false():
    """Überkorrektur-Schutz: dieselbe Summary muss den umgekehrten
    Claim auf false ziehen."""
    r = _run("Photovoltaik liefert in Österreich mehr Strom als Windkraft",
             "true", _S44)
    assert r["verdict"] == "false", r


def test_g2_feuert_nicht_bei_zwei_geo_entitaeten():
    """Zwei Länder im Claim = Pattern-G-Territorium; G2 darf dort nicht
    mit seiner generischen Extraktion dazwischenfunken."""
    r = _run("Frankreich stößt pro Kopf mehr CO2 aus als Deutschland",
             "false",
             "Frankreich liegt bei 4,3 t pro Kopf, Deutschland bei 7,5 t.")
    assert r["verdict"] == "false", r


def test_g2_feuert_nicht_bei_mehrdeutigem_subjekt():
    """Zwei A-Kandidaten mit je einem Wert → mehrdeutig → kein Fix
    (konservativ scheitern)."""
    r = _run("Windkraft liefert in Österreich mehr Strom als Photovoltaik",
             "false",
             "Windkraft kam auf 13,8 %. Strom aus Biomasse lag bei 4,2 %. "
             "Photovoltaik erreichte 7,5 %.")
    assert r["verdict"] == "false", r


def test_g2_ignoriert_massgroessen_als_entitaet():
    """'Einwohner' ist eine Maßgröße, keine Vergleichs-Entität — sonst
    würde 'Wien hat mehr Einwohner als Hamburg' die Maßgröße als
    Subjekt greifen."""
    from services.verdict_postprocess import _generic_comparison_pair
    assert _generic_comparison_pair(
        "Wien hat mehr Einwohner als Hamburg") is None


def test_g2_substring_falle_strom_in_stromproduktion():
    """Ohne Wortgrenzen sammelt 'strom' den Wert aus
    'stromproduktion … 13,8 %' ein und macht das Subjekt mehrdeutig."""
    from services.verdict_postprocess import _entity_percent_from_summary
    sn = _S44.lower()
    assert _entity_percent_from_summary(
        "strom", sn, word_boundary=True,
        others=("windkraft", "photovoltaik"), window_len=90) is None
    assert _entity_percent_from_summary(
        "windkraft", sn, word_boundary=True,
        others=("strom", "photovoltaik"), window_len=90) == 13.8


# --- Pattern K: Anti-Mythos-Meta-Claim (QA100 #24) ---

_S24 = ("Atomkraft verursacht laut IPCC AR6 (2022) und OWID nur 12 g "
        "CO₂/kWh im Lifecycle, Kohle 820 g/kWh (Braunkohle bis "
        "1.054 g/kWh). Damit ist Kernkraft deutlich klimafreundlicher "
        "als Kohle.")


def test_k_kernkraft_unsinn_false_wird_true():
    """QA100 #24, 5/5 deterministisch: Die Summary BESTÄTIGT den
    Meta-Claim ('klimafreundlicher als Kohle'), das Label sagt false.
    Tier-2b kannte nur 'gar nicht'-Negationen am Prädikat."""
    r = _run("Dass Kernkraft klimaschädlicher wäre als Kohle, ist ja "
             "wohl Unsinn", "false", _S24, confidence=0.95)
    assert r["verdict"] == "true", r


def test_k_gegenrichtung_mythos_bleibt_false():
    """QA100 #26 (Kontroll-Claim): Doppelnegation ohne '<adj>er als'-
    Vergleich → K greift nicht, das korrekte mostly_false bleibt."""
    r = _run("Dass Bio-Landwirtschaft mehr Fläche pro Ertrag braucht, "
             "ist doch längst widerlegt", "mostly_false",
             "Bio-Landwirtschaft erzielt pro Hektar rund 20-25 % "
             "geringere Erträge als konventionelle Landwirtschaft und "
             "benötigt daher mehr Fläche für die gleiche Erntemenge.")
    assert r["verdict"] == "mostly_false", r


def test_k_feuert_nicht_wenn_summary_den_claim_komparativ_traegt():
    """Steht der Claim-Komparativ selbst in der Summary, ist die Lage
    mehrdeutig → kein Fix."""
    r = _run("Dass Kernkraft klimaschädlicher wäre als Wind, ist ja wohl "
             "Unsinn", "false",
             "Studien zeigen, dass Kernkraft im Lebenszyklus "
             "klimaschädlicher als Wind ist.")
    assert r["verdict"] == "false", r


def test_k_braucht_die_zurueckweisung():
    """Ohne umgangssprachliche Zurückweisung ist es kein Meta-Claim —
    ein normaler Vergleichs-Claim darf nicht geflippt werden."""
    r = _run("Kernkraft ist klimaschädlicher als Kohle", "false", _S24)
    assert r["verdict"] == "false", r


def test_k_bindet_antonym_ans_vergleichsobjekt():
    """Das Antonym muss dem im Claim genannten Objekt zugeschrieben
    sein — ein freies 'sicherer' irgendwo in der Summary reicht nicht."""
    r = _run("Dass Impfungen gefährlicher wären als die Krankheit, ist "
             "ja wohl Unsinn", "false",
             "Der Straßenverkehr ist sicherer als früher. Zu Impfungen "
             "liegen keine Vergleichsdaten vor.")
    assert r["verdict"] == "false", r
    r2 = _run("Dass Impfungen gefährlicher wären als die Krankheit, ist "
              "ja wohl Unsinn", "false",
              "Impfungen sind laut RKI deutlich sicherer als die "
              "Krankheit selbst.")
    assert r2["verdict"] == "true", r2


def test_k_funktionswoerter_auf_er_sind_kein_komparativ():
    """'oder'/'unter' enden auf -er, sind aber keine Komparative."""
    from services.verdict_postprocess import _antimythos_flip
    c = "Dass Corona oder als Grippe zu sehen ist, ist ja wohl Unsinn"
    assert _antimythos_flip(c, c.lower(), "grippe ist harmloser") is False


# --- Live-Varianten aus der Verifikation nach dem Deploy (2026-07-27) ---
#
# Die 3-Iterationen-Regel in Reinform: dieselbe Sachlage, zwei völlig
# verschiedene LLM-Formulierungen. Variante B ("Die Behauptung … ist
# widerlegt") widerlegt die EINGEBETTETE Aussage — die 4-Tier-Schluss-
# formel liest das als 'false' für den Meta-Claim. Deshalb darf Pattern K
# die Schlussformel überstimmen, sobald die Formel-Spanne P zitiert und
# KEIN Dismissal-Token enthält.

_C24 = "Dass Kernkraft klimaschädlicher wäre als Kohle, ist ja wohl Unsinn"


def test_k_live_variante_b_widerlegungs_formel():
    r = _run(_C24, "false",
             "Atomkraft verursacht mit 12 g CO₂/kWh im Lifecycle deutlich "
             "weniger Treibhausgase als Kohle (820 g/kWh) und Braunkohle "
             "(1054 g/kWh). Die Behauptung, Kernkraft sei klimaschädlicher "
             "als Kohle, ist damit widerlegt.", confidence=0.95)
    assert r["verdict"] == "true", r


def test_k_widerlegte_zurueckweisung_kippt_nicht():
    """Bezieht sich die Widerlegung auf die ZURÜCKWEISUNG selbst
    (Dismissal-Token in der Formel-Spanne), bleibt false stehen."""
    r = _run(_C24, "false",
             "Die Behauptung, dass es Unsinn sei, Kernkraft als "
             "klimaschädlicher als Kohle zu bezeichnen, ist widerlegt.",
             confidence=0.95)
    assert r["verdict"] == "false", r


def test_g2_ueberlebt_falsche_verbale_konklusion():
    """Live-Variante #44 vom 27.07.: die Summary zieht aus 13,8 vs. 7,5
    die glatt falsche Konklusion 'weniger Strom' — die Zahlen müssen
    das Label trotzdem tragen."""
    r = _run("Windkraft liefert in Österreich mehr Strom als Photovoltaik",
             "false",
             "Laut APG/E-Control lag der Anteil von Windkraft an der "
             "österreichischen Stromproduktion 2024 bei 13,8 %, der von "
             "Photovoltaik bei 7,5 %. Windkraft liefert damit weniger "
             "Strom als Photovoltaik.")
    assert r["verdict"] == "true", r


def test_k_negierte_paraphrase_der_zurueckweisung_kippt_nicht():
    """Live-Gegenprobe 27.07. (#903): Das LLM paraphrasiert die
    Zurückweisung als NEGATION statt sie zu zitieren — 'Die Behauptung,
    Kohle sei NICHT klimaschädlicher als Kernkraft, ist damit widerlegt'.
    Auch das gilt dem Meta-Claim, nicht dem Mythos: Kohle IST
    klimaschädlicher, die Zurückweisung ist also falsch."""
    r = _run("Dass Kohle klimaschädlicher wäre als Kernkraft, ist ja wohl "
             "Unsinn", "false",
             "Kohle verursacht mit 820 g CO₂/kWh (Braunkohle: 1054 g/kWh) "
             "deutlich höhere Lifecycle-CO₂-Emissionen als Kernkraft "
             "(12 g/kWh). Die Behauptung, Kohle sei nicht klimaschädlicher "
             "als Kernkraft, ist damit widerlegt.", confidence=0.95)
    assert r["verdict"] == "false", r


def test_k_live_variante_d_pronomen_und_verteidigung():
    """Vierte Live-Variante von #24 (27.07., nach dem Pack-Flut-Fix) —
    zwei neue Hürden auf einmal:

    'Damit ist Kernkraft deutlich klimafreundlicher als Kohle – die
     Behauptung, sie sei klimaschädlicher, ist falsch.'

    (1) Das Vergleichs-Objekt kollabiert im Formel-Fenster zum Pronomen
        ('sie'), 'Kohle' steht nur im Satz davor — Weg B darf das Objekt
        deshalb in der ganzen Summary suchen, nicht nur in der Spanne.
    (2) Das LLM lieferte hier KORREKT 'true'; erst die 4-Tier-Schluss-
        formel-Erkennung las 'die Behauptung … ist falsch' als Aussage
        über den META-Claim und hätte gekippt. K muss ein bereits
        richtiges Label also auch VERTEIDIGEN, nicht nur korrigieren."""
    r = _run(_C24, "true",
             "Atomkraft verursacht laut IPCC AR6 (2022) und OWID nur 12 g "
             "CO₂/kWh im Lifecycle, Kohle 820 g/kWh (Braunkohle 1.054 "
             "g/kWh). Damit ist Kernkraft deutlich klimafreundlicher als "
             "Kohle – die Behauptung, sie sei klimaschädlicher, ist falsch.",
             confidence=0.95)
    assert r["verdict"] == "true", r


def test_k_verteidigung_kippt_keinen_echten_mythos():
    """Gegenprobe zur Verteidigungs-Logik: Wenn die Schlussformel
    tatsächlich dem Meta-Claim gilt (Negations-Paraphrase), bleibt das
    Kippen auf false erhalten — auch bei rohem 'true'."""
    r = _run("Dass Kohle klimaschädlicher wäre als Kernkraft, ist ja wohl "
             "Unsinn", "true",
             "Kohle verursacht mit 820 g CO₂/kWh deutlich höhere "
             "Emissionen als Kernkraft (12 g/kWh). Die Behauptung, Kohle "
             "sei nicht klimaschädlicher als Kernkraft, ist damit "
             "widerlegt.", confidence=0.95)
    assert r["verdict"] == "false", r


# --- Pattern H symmetrisch (QA100 #34/#8, 2026-07-27) ---
#
# H korrigierte bisher nur true->false bei VERFEHLTER Schwelle. Live
# sichtbar an "sind des über 9 millionen?": die Summary nannte 9.197.213
# und 9.208.163, formulierte aber "knapp unter 9,2 Millionen" — das LLM
# verglich gegen 9,2 statt gegen 9 und landete auf false. Die
# Bestätigungs-Richtung schließt das; die Widerlegungs-Richtung bleibt
# unverändert.

def test_h_bestaetigt_ueber_schwelle_live_34():
    r = _run("wieviele leute leben eigentlich in österreich? sind des "
             "über 9 millionen?", "false",
             "Laut Eurostat lebte Österreich am 1. Januar 2025 mit "
             "9.197.213 Personen knapp unter 9,2 Millionen Einwohnern. "
             "Die World Bank bestätigt für 2025 9.208.163 Einwohner.")
    assert r["verdict"] == "true", r


def test_h_bestaetigt_unter_schwelle():
    r = _run("Die Inflation in Österreich lag unter 3 Prozent", "false",
             "Der Verbraucherpreisindex stieg 2024 in Österreich um 2,9 %.")
    assert r["verdict"] == "true", r


def test_h_bestaetigung_respektiert_entitaets_bindung():
    """Der fremde Wert (Wien 36,8 %) darf die Bestätigung für das
    Claim-Subjekt (Burgenland 11,7 %) weder auslösen noch blockieren."""
    summary = ("Laut Statistik Austria liegt der Ausländeranteil im "
               "Burgenland bei 11,7 %, in Wien dagegen bei 36,8 %.")
    r = _run("Im Burgenland liegt der Ausländeranteil unter 15 Prozent",
             "false", summary)
    assert r["verdict"] == "true", r
    # Gegenrichtung mit derselben Summary
    r2 = _run("Im Burgenland liegt der Ausländeranteil unter 10 Prozent",
              "true", summary)
    assert r2["verdict"] == "false", r2


def test_h_bestaetigung_bei_zwei_entitaeten_ambig_kein_fix():
    r = _run("In Wien und im Burgenland liegt der Anteil unter 15 Prozent",
             "false",
             "Laut Statistik Austria liegt der Ausländeranteil im "
             "Burgenland bei 11,7 %, in Wien dagegen bei 36,8 %.")
    assert r["verdict"] == "false", r


def test_h_bestaetigung_negations_gate():
    """Bei einem NEGIERTEN Schwellen-Claim widerlegen dieselben Werte den
    Claim, statt ihn zu bestätigen — die Bestätigungs-Richtung muss
    schweigen."""
    r = _run("Österreich hat nicht über 9 Millionen Einwohner", "false",
             "Laut Statistik Austria leben 9.197.213 Personen in "
             "Österreich.")
    assert r["verdict"] == "false", r


def test_h_widerlegung_unveraendert():
    """Die seit QA50B live erprobte Richtung darf sich nicht verändern."""
    r = _run("Im Burgenland liegt der Ausländeranteil unter 10 Prozent",
             "true",
             "Laut Statistik Austria liegt der Anteil im Burgenland "
             "bei 11,7 %.")
    assert r["verdict"] == "false", r
    r2 = _run("Die Jugendarbeitslosigkeit in Spanien liegt über 40 Prozent",
              "true",
              "Die Jugendarbeitslosigkeit in Spanien lag 2024 bei 26,5 %.")
    assert r2["verdict"] == "false", r2


# --- Pattern F: Dezimal-Grenze der Widerlegungs-Erkennung (QA100 #34) ---

def test_f_unter_92_widerlegt_keinen_ueber_9_claim():
    """Live-Blocker von #34: F-1 suchte "unter <Zahl>" mit `\\b` und
    matchte "unter 9" mitten in "knapp unter 9,2 Millionen". Damit galt
    ein Claim "über 9 Millionen" als widerlegt, obwohl 9,2 > 9 ihn
    BESTÄTIGT — und weil F verdict_from_summary setzte, kam Pattern H
    gar nicht mehr zum Zug."""
    r = _run("wieviele leute leben eigentlich in österreich? sind des "
             "über 9 millionen?", "true",
             "Laut Eurostat lebte Österreich am 1. Januar 2025 mit "
             "9.197.213 Personen knapp unter 9,2 Millionen Einwohnern. "
             "Die World Bank schätzt die Bevölkerung 2025 auf 9.208.163. "
             "Beide Quellen bestätigen, dass Österreich über 9 Millionen "
             "Einwohner hat.", confidence=0.95)
    assert r["verdict"] == "true", r


def test_f_echte_widerlegung_bleibt_erhalten():
    """Die Gegenrichtung darf nicht verloren gehen: eine echte
    'deutlich unter X'-Aussage widerlegt den Schwellen-Claim weiter."""
    r = _run("Der Bremsweg beträgt über 500 Meter", "true",
             "Die ADAC-Tests zeigen einen Bremsweg von deutlich unter "
             "500 Metern.")
    assert r["verdict"] == "false", r
    r2 = _run("Österreich hat über 9 Millionen Einwohner", "true",
              "Die Bevölkerung lag 2015 mit 8,58 Millionen deutlich unter "
              "9 Millionen Personen.")
    assert r2["verdict"] == "false", r2


def test_h_grenzfall_unter_echo_schwelle_bleibt_unangetastet():
    """Werte innerhalb von 0,5 % der Schwelle gelten als Schwellen-Echo
    und werden bewusst NICHT bewertet — kein Fix ist besser als ein
    falscher."""
    r = _run("Österreich hat über 9 Millionen Einwohner", "true",
             "Die Bevölkerung lag mit 8.978.929 knapp unter 9.000.000 "
             "Personen.")
    assert r["verdict"] == "true", r
