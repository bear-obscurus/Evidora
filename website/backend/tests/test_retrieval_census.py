"""Retrieval-Zensus: Klassifikation + Parser (2026-08-21).

Das Werkzeug beantwortet eine strategische Frage — „ist Evidoras Genauigkeit
durch Retrieval oder durch Verdict-Logik begrenzt?" — und eine falsche
Klassifikation wuerde die Roadmap in die falsche Richtung schicken. Deshalb
ist der Kern rein (keine I/O) und hier gepinnt.

Gepinnt wird besonders:
  * die B/E-Trennung. Die erste Fassung warf „keine Evidenz" in EINEN Topf,
    egal ob das Verdict stimmte — damit erschienen 54 % als Fehlerklasse,
    obwohl knapp die Haelfte davon zufaellig richtig lag. Wer ohne Evidenz
    richtig liegt, hat nicht recht, sondern Glueck; das gehoert getrennt
    ausgewiesen, aber nicht als Fehler gezaehlt.
  * der mehrzeilige Import-Stil in main.py. Die erste Parser-Fassung las nur
    einzeilige Imports und verfehlte 8 von 137 Gates — eine stille
    Teilabdeckung, die eine huebsche und falsche Zahl produziert haette.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

import retrieval_census as rc  # noqa: E402


def _r(sources_n, evidence_n, verdict):
    return {"id": 1, "sources_n": sources_n, "evidence_n": evidence_n,
            "sources": ["X"] * sources_n, "verdict": verdict}


# ---------------------------------------------------------------------------
# verdict_ok
# ---------------------------------------------------------------------------

def test_verdict_ok_expected_und_acceptable():
    assert rc.verdict_ok("true", "true", [])
    assert rc.verdict_ok("mostly_true", "true", ["true", "mostly_true"])
    assert not rc.verdict_ok("false", "true", ["true", "mostly_true"])
    assert not rc.verdict_ok(None, "true", ["true"])


# ---------------------------------------------------------------------------
# Klassifikation
# ---------------------------------------------------------------------------

def test_klasse_a_nichts_abgerufen():
    assert rc.classify(_r(0, 0, "unverifiable"), "true", ["true"]) == "A"


def test_klasse_b_keine_evidenz_und_falsch():
    assert rc.classify(_r(4, 0, "unverifiable"), "true", ["true"]) == "B"


def test_klasse_e_keine_evidenz_aber_richtig():
    """Der Fall, den die erste Fassung mit B verwechselt hat."""
    assert rc.classify(_r(12, 0, "true"), "true", ["true"]) == "E"


def test_klasse_c_evidenz_und_richtig():
    assert rc.classify(_r(5, 3, "true"), "true", ["true", "mostly_true"]) == "C"


def test_klasse_d_evidenz_aber_falsch():
    """Die EINZIGE Klasse, die ein Befund der Verdict-Logik ist."""
    assert rc.classify(_r(5, 3, "false"), "true", ["true"]) == "D"


def test_klassifikation_ist_defensiv_bei_luecken():
    """Ein unvollstaendiger Record darf den Zensus nicht abbrechen — und
    soll konservativ als Retrieval-Problem erscheinen, nicht als Erfolg."""
    assert rc.classify({"sources": ["a", "b"], "verdict": "true"},
                       "true", ["true"]) == "E"   # evidence_n fehlt -> 0
    assert rc.classify({}, "true", ["true"]) == "A"


# ---------------------------------------------------------------------------
# join_runs
# ---------------------------------------------------------------------------

def test_join_meldet_verwaiste_und_fehlende():
    claims = [{"id": 1, "claim": "a", "expected": "true", "acceptable": ["true"]},
              {"id": 2, "claim": "b", "expected": "false", "acceptable": []}]
    results = [dict(_r(3, 1, "true"), id=1), dict(_r(3, 1, "false"), id=99)]
    joined, verwaist, fehlend = rc.join_runs(claims, results)
    assert [j["id"] for j in joined] == [1]
    assert verwaist == [99], "Ergebnis ohne Claim muss gemeldet werden"
    assert fehlend == [2], "Claim ohne Ergebnis muss gemeldet werden"
    assert joined[0]["zensus"] == "C"


# ---------------------------------------------------------------------------
# Parser gegen echte main.py-Schreibweisen
# ---------------------------------------------------------------------------

MAIN_FIXTURE = '''
from services.landwirtschaft_pack import search_landwirtschaft, claim_mentions_landwirtschaft_cached
from services.withdrawn_drugs import (
    claim_mentions_withdrawn_drugs_cached, search_withdrawn_drugs,
)

    if claim_mentions_landwirtschaft_cached(claim):
        tasks.append(cached("Landwirtschaft-Konsens", search_landwirtschaft, analysis))
        queried_names.append("Landwirtschaft-Konsens (AGES + EFSA)")
    if claim_mentions_withdrawn_drugs_cached(claim):
        tasks.append(cached(
            "Wikipedia Withdrawn Drugs",
            search_withdrawn_drugs, analysis,
        ))
        queried_names.append("Wikipedia Withdrawn Drugs")
'''


def test_parse_dispatches_einzeilig_und_mehrzeilig():
    d = dict(rc.parse_dispatches(MAIN_FIXTURE))
    assert d["claim_mentions_landwirtschaft_cached"] == "Landwirtschaft-Konsens"
    assert d["claim_mentions_withdrawn_drugs_cached"] == "Wikipedia Withdrawn Drugs", \
        "mehrzeiliges cached( ... ) muss erkannt werden"


def test_parse_gate_modules_erkennt_geklammerten_import():
    """Regressions-Pin: die erste Fassung las nur einzeilige Imports und
    verfehlte damit 8 von 137 Gates."""
    m = rc.parse_gate_modules(MAIN_FIXTURE)
    assert m["claim_mentions_landwirtschaft_cached"] == "landwirtschaft_pack"
    assert m["claim_mentions_withdrawn_drugs_cached"] == "withdrawn_drugs"


def test_parser_findet_alle_gates_der_echten_main_py():
    """Kontrolle gegen die echte Datei: jedes erkannte Dispatch-Gate muss
    auch eine Modul-Zuordnung haben, sonst ist die Abdeckung stillschweigend
    unvollstaendig."""
    main_py = Path(__file__).resolve().parents[1] / "main.py"
    src = main_py.read_text(encoding="utf-8")
    dispatches = rc.parse_dispatches(src)
    module = rc.parse_gate_modules(src)
    assert len(dispatches) > 100, "Dispatch-Muster passt nicht mehr auf main.py"
    ohne = [g for g, _ in dispatches if g not in module]
    assert not ohne, f"Gates ohne Import-Zuordnung: {ohne}"


# ---------------------------------------------------------------------------
# Quellen-Statistik (Ueber-Trigger-Detektor)
# ---------------------------------------------------------------------------

def test_quellen_statistik_zaehlt_treffer_je_quelle():
    joined = [{"sources": ["A", "B"]}, {"sources": ["A"]}, {"sources": []}]
    stat = rc.quellen_statistik(joined)
    assert stat["A"] == 2 and stat["B"] == 1
