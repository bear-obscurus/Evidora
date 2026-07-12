"""QA50B-Reste (2026-07-12): Orbán-Trigger, AT-Steuerlast-Ranking, Wien-Wahl 2025.

Dependency-light: Trigger/Daten-Contracts, kein Netz/LLM.
"""
import json
import os
import re

from services._struct_marker import has_false_verdict_override
from services._topic_match import substring_or_composite_match

_DATA = os.path.join(os.path.dirname(__file__), "..", "data")


def test_wikidata_amtszeit_kennt_regiert():
    """QA50B #25: 'Orbán REGIERT Ungarn noch immer' erreichte das
    politiker_amtszeit-Template nie (nur Amts-Substantive triggerten),
    obwohl Wikidata das End-Datum (PM bis 09.05.2026) führt."""
    svc = open(os.path.join(_DATA, "..", "services", "wikidata.py"),
               encoding="utf-8").read()
    assert '"regiert"' in svc and '"an der macht"' in svc


def _steuer_fact():
    d = json.load(open(os.path.join(_DATA, "wirtschaftspolitik_pack.json"),
                       encoding="utf-8"))
    return next(f for f in d["facts"]
                if f["id"] == "at_steuerlast_eu_ranking_2026")


def test_steuerlast_trigger_beide_richtungen():
    f = _steuer_fact()
    for c in ("Österreich hat nicht die höchste Steuerlast in der EU",
              "Österreich hat die höchste Steuerlast der EU",
              "Österreich ist Europameister bei den Abgaben",
              "In Österreich sind die Steuern am höchsten"):
        assert substring_or_composite_match(f, c.lower()), c
    for c in ("Deutschland hat die höchsten Steuern der Welt",
              "Österreich hat hohe Steuern",
              "Die höchsten Berge Österreichs"):
        assert not substring_or_composite_match(f, c.lower()), c


def test_steuerlast_richtungs_fakt_ohne_struct_token():
    """Richtungs-sensibler Fakt (höchste→nicht korrekt, nicht-höchste→
    korrekt) darf KEINEN False-Override-Token tragen — der L2-Override
    würde die wahre Richtung invertieren (Zyrtec-/Voltadol-Muster)."""
    f = _steuer_fact()
    assert not has_false_verdict_override(f["data"]["kernsatz_fuer_synthesizer"])
    assert not has_false_verdict_override(f["headline"])
    assert "Rang 4" in f["headline"] and "52,6" in f["headline"]


def test_wien_wahl_2025_eintrag_und_trigger():
    """QA50B #30: amtliches Wien-Ergebnis (Stadt Wien GR251) als
    deskriptiv-historische Quelle — Guardrail 'Wahlergebnisse erlaubt'."""
    d = json.load(open(os.path.join(_DATA, "wahlen.json"), encoding="utf-8"))
    e = next(x for x in d["elections"] if x.get("type") == "GRW_W")
    assert e["year"] == 2025
    spoe = next(r for r in e["results"] if r["short"] == "SPÖ")
    assert spoe["percent"] == 39.38 and spoe["seats"] == 43
    assert max(e["results"], key=lambda r: r["percent"])["short"] == "SPÖ"

    from services.wahlen import WAHL_TYPE_KEYWORDS
    assert WAHL_TYPE_KEYWORDS.get("wien-wahl") == "GRW_W"
    assert WAHL_TYPE_KEYWORDS.get("gemeinderatswahl") == "GRW_W"

    svc = open(os.path.join(_DATA, "..", "services", "wahlen.py"),
               encoding="utf-8").read()
    pat = re.compile(r"(die\s+)?(wien-?wahl|gemeinderatswahl|landtagswahl)"
                     r"(\s+\d{4})?\s+gewonnen\b")
    assert pat.search("die spö hat die wien-wahl 2025 gewonnen")
    assert '"GRW_W": "Wiener Gemeinderats- und Landtagswahl"' in svc


# --- Niedrig-Punkte #34/#33/#4 (2026-07-12, zweiter Sweep) ---

def test_mietendeckel_leitlinie_richtungs_sensibel():
    """#34: Die alte Leitlinie erklärte ALLE 'verfassungswidrig'-Phrasings
    zu mostly_false — formal ist 'verfassungswidrig' aber KORREKT
    (2 BvF 1/20: 'mit dem GG unvereinbar und nichtig'). Neu: richtungs-
    sensibel + OHNE VERDICT-LEITLINIE-Token (der STRUKT-Marker hätte via
    L2 die wahre Richtung invertiert)."""
    d = json.load(open(os.path.join(_DATA, "wohnen_pack.json"),
                       encoding="utf-8"))
    f = next(x for x in (d.get("facts") or d.get("topics"))
             if x["id"] == "mietendeckel_berlin_bilanz_2026")
    ks = f["data"]["kernsatz_fuer_synthesizer"]
    assert not has_false_verdict_override(ks)
    assert "mostly_true bei 0.85" in ks          # formal-korrekt-Richtung
    assert "mostly_false (Konfidenz 0.9)" in ks  # verfassungskonform-Richtung
    assert "Kompetenzwidrigkeit IST eine Form der Verfassungswidrigkeit" in ks


def test_kopftuch_rechtslage_2026_und_trigger():
    """#33: 'An Österreichs Volksschulen gilt ein Kopftuchverbot' →
    Stand Juli 2026 teils-teils (beschlossen 11.12.2025, sanktionswirksam
    erst 1.9.2026). Ruling trägt Update + triggert auf Bestands-Phrasings.
    ⚠️ Refresh-Marker: ab 1.9.2026 kippt die Bewertung auf zutreffend."""
    d = json.load(open(os.path.join(_DATA, "at_courts.json"),
                       encoding="utf-8"))
    r = next(x for x in d["rulings"]
             if x["id"] == "vfgh_g_4_2020_kopftuchverbot")
    assert r["kerninhalt"].startswith("RECHTSLAGE Stand Juli 2026")  # Cap-Schatten: vorne
    assert "1.9.2026" in r["kerninhalt"] and "mixed" in r["kerninhalt"]
    assert substring_or_composite_match(
        r, "an österreichs volksschulen gilt ein kopftuchverbot")
    assert not substring_or_composite_match(
        r, "in frankreich gilt ein laizitätsgesetz")


def _ibu_fact():
    d = json.load(open(os.path.join(
        _DATA, "gesundheits_autoritaeten_pack.json"), encoding="utf-8"))
    return next(f for f in d["facts"]
                if f["id"] == "ibuprofen_niere_konsens_2026")


def test_ibuprofen_fakt_trigger_und_mixed_ziel():
    """#4: NSAR-Klassen-Transfer vom Diclofenac-Fakt — Zyrtec-Muster
    (kein Override-Token, parseable mixed-Direktive)."""
    f = _ibu_fact()
    for c in ("Ibuprofen geht auf die Nieren",
              "Nurofen ist schlecht für die Nieren",
              "Kann Ibuprofen zu Nierenversagen führen?"):
        assert substring_or_composite_match(f, c.lower()), c
    for c in ("Ibuprofen hilft gegen Kopfschmerzen",
              "Meine Nieren tun weh",
              "Voltadol schädigt die Nieren"):
        assert not substring_or_composite_match(f, c.lower()), c
    assert not has_false_verdict_override(f["data"]["kernsatz_fuer_synthesizer"])
    assert not has_false_verdict_override(f["headline"])
    from services.confidence_calibration import _DIRECTIVE_RE
    hits = [(m.group(1).lower(), m.group(2)) for m in
            _DIRECTIVE_RE.finditer(f["data"]["kernsatz_fuer_synthesizer"])]
    assert ("mixed", "0.6") in hits, hits


# --- QA50C-Sweep (7 Befunde, 2026-07-12 spät) ---

def test_qa50c_einwohner_vergleichs_gate_und_satz():
    """#5: 'Wien hat mehr Einwohner als NÖ' → Gate + fertiger Satz."""
    from services.at_factbook import (_claim_mentions_citizenship,
                                      _build_citizenship_results)
    c = "wien hat mehr einwohner als niederösterreich"
    assert _claim_mentions_citizenship(c)
    d = json.load(open(os.path.join(_DATA, "at_factbook.json"),
                       encoding="utf-8"))
    f = next(x for x in d["facts"]
             if x["id"] == "staatsbuergerschaft_bevoelkerung_2026")
    blk = next(r for r in _build_citizenship_results(f, c)
               if r["indicator_name"].startswith("Anteil Nicht-AT"))
    assert "Wien 2.040.914 — MEHR als Niederösterreich (1.729.541)" in \
        blk["display_value"].replace("Einwohner: ", "Einwohner-kompakt: ") \
        or "MEHR als Niederösterreich" in blk["display_value"]


def test_qa50c_steuer_fakt_eu_claims_ohne_at():
    """#41: 'Belgien hat die höchste Abgabenlast in der EU' muss den
    Ranking-Fakt erreichen (EU-Alternative zur AT-Gruppe)."""
    f = _steuer_fact()
    assert substring_or_composite_match(
        f, "belgien hat die höchste steuer- und abgabenlast auf arbeit in der eu")


def test_qa50c_gemeinderat_gate_und_null_mandate():
    """#29: 'im Wiener Gemeinderat vertreten' → GRW_W-Gate; 0-Mandate-
    Parteien werden explizit als NICHT vertreten gerendert."""
    from services.wahlen import WAHL_TYPE_KEYWORDS, _make_party_entry
    assert WAHL_TYPE_KEYWORDS.get("wiener gemeinderat") == "GRW_W"
    d = json.load(open(os.path.join(_DATA, "wahlen.json"), encoding="utf-8"))
    e = next(x for x in d["elections"] if x["type"] == "GRW_W")
    kpoe = next(r for r in e["results"] if r["short"] == "KPÖ")
    disp = _make_party_entry(e, kpoe)["display_value"]
    assert "0 Mandate" in disp and "NICHT im Gremium vertreten" in disp
    oevp = next(r for r in e["results"] if r["short"] == "ÖVP")
    assert "-10,78 pp" in _make_party_entry(e, oevp)["display_value"]


def test_qa50c_benko_fakt():
    """#35: U-Haft-Fakt (Opus-recherchiert, orf.at-Quelle), Unschulds-
    vermutung für offene Verfahren enthalten."""
    d = json.load(open(os.path.join(_DATA, "at_courts.json"),
                       encoding="utf-8"))
    r = next(x for x in d["rulings"] if x["id"] == "benko_uhaft_krida_2026")
    assert r["kerninhalt"].startswith("STAND JULI 2026")
    assert "Unschuldsvermutung" in r["kerninhalt"]
    assert substring_or_composite_match(r, "rené benko sitzt in untersuchungshaft")
