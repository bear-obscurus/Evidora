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
