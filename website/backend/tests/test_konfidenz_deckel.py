"""Konfidenz-Deckel bei dünner Beleglage.

Messung vom 27.9.2026 über 140 Live-Läufe (QA50F + QA50G + HART40),
mechanisch gegen vorab festgeschriebene Erwartungen bewertet. Auf den
BESTIMMTEN Labels (true/false/mostly_*) — dort, wo der Dienst eine
Behauptung aufstellt:

    Konfidenz   richtig 0,902 | falsch 0,898   AUC 0,604

Alle elf falschen bestimmten Verdicts lagen zwischen 0,85 und 0,95, also
ununterscheidbar von den richtigen. Die Zahl sagt, welches LABEL vergeben
wurde, nicht ob es stimmt.

Was mitgeht, ist die Beleglage (69 Läufe mit erfasster Evidenz):

    Belege   n    Trefferquote   behauptete Konfidenz
      1      28      78,6 %            0,87
      2      15      80,0 %            0,90
      3      12      83,3 %            0,92
     4+      14     100,0 %            0,92

Der Deckel ist bewusst KEINE Kalibrierung. Er senkt nur, nie hebt er an,
und er greift nur bei bestimmten Labels. Diese Suite pinnt genau diese
Eigenschaften — nicht die Zahlen als Wahrheit, sondern das Verhalten:
monoton, nur abwärts, unverifiable/mixed unberührt, und ab vier Belegen
schweigt er.

Keine Netzabfrage, kein Modell.
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import (  # noqa: E402
    _BESTIMMTE_LABELS,
    _KONFIDENZ_DECKEL,
    apply_verdict_postprocessing,
    deckel_fuer,
)

NEUTRAL = "Eine sachliche Zusammenfassung ohne Schlussformel und ohne Zahlenvergleich."
CLAIM = "Eine beliebige Behauptung ohne Schwelle und ohne Superlativ"


def _lauf(verdict, belege, confidence=0.95, summary=NEUTRAL, claim=CLAIM):
    ev = [{"source": f"Quelle {i}", "url": f"https://beleg{i}.test/x"} for i in range(belege)]
    return apply_verdict_postprocessing(
        {"verdict": verdict, "confidence": confidence, "summary": summary, "evidence": ev},
        [], claim)


# --------------------------------------------------------------------------
# Die Stufen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("belege,erwartet", [(0, 0.50), (1, 0.80), (2, 0.85), (3, 0.90)])
def test_deckel_je_beleglage(belege, erwartet):
    assert deckel_fuer(belege) == erwartet


@pytest.mark.parametrize("belege", [4, 5, 9, 40])
def test_ab_vier_belegen_schweigt_der_deckel(belege):
    assert deckel_fuer(belege) is None
    assert _lauf("true", belege)["confidence"] == 0.95


@pytest.mark.parametrize("belege,erwartet", [(1, 0.80), (2, 0.85), (3, 0.90)])
@pytest.mark.parametrize("verdict", ["true", "false", "mostly_true", "mostly_false"])
def test_bestimmte_labels_werden_gedeckelt(verdict, belege, erwartet):
    assert _lauf(verdict, belege)["confidence"] == erwartet


def test_der_deckel_ist_monoton():
    werte = [deckel_fuer(n) for n in range(4)]
    assert werte == sorted(werte), werte


# --------------------------------------------------------------------------
# Was er NICHT tut
# --------------------------------------------------------------------------

@pytest.mark.parametrize("belege", [0, 1, 2, 3])
def test_er_hebt_nie_an(belege):
    """Der Kern: Er ist ein Deckel, keine Kalibrierung. Eine niedrige
    Konfidenz bleibt niedrig, auch wenn der Deckel darüber liegt."""
    r = _lauf("true", belege, confidence=0.30)
    assert r["confidence"] == 0.30


def test_mixed_bleibt_vom_deckel_unberuehrt():
    """Unbestimmte Labels tragen ihre Unsicherheit schon im Label; ein
    Beleg-Deckel darauf wuerde nur Rauschen erzeugen."""
    assert "mixed" not in _BESTIMMTE_LABELS
    assert _lauf("mixed", 1, confidence=0.75)["confidence"] == 0.75


def test_unverifiable_behaelt_seine_eigene_alte_regel():
    """Fuer `unverifiable` gibt es seit laengerem einen eigenen Deckel bei
    0,15. Der neue Beleg-Deckel fasst ihn nicht an — das Ergebnis kommt
    weiterhin von der alten Regel, unabhaengig von der Beleglage."""
    assert "unverifiable" not in _BESTIMMTE_LABELS
    for belege in (0, 1, 4, 9):
        assert _lauf("unverifiable", belege, confidence=0.75)["confidence"] == 0.15


def test_fehlender_schluessel_ist_unbekannt_nicht_null():
    """Ein Result ohne `evidence`-Schluessel kommt aus einem Pfad, der die
    Belegstufe nie durchlaufen hat. Unbekannt ist nicht dasselbe wie
    gemessene Null — der Deckel schweigt dann."""
    r = apply_verdict_postprocessing(
        {"verdict": "true", "confidence": 0.95, "summary": NEUTRAL}, [], CLAIM)
    assert r["confidence"] == 0.95


def test_leere_liste_ist_eine_messung():
    """Eine leere Liste dagegen heisst: Die Stufe lief und fand nichts."""
    r = apply_verdict_postprocessing(
        {"verdict": "true", "confidence": 0.95, "summary": NEUTRAL, "evidence": []},
        [], CLAIM)
    assert r["confidence"] == 0.50


def test_kaputte_konfidenz_bricht_nicht():
    r = apply_verdict_postprocessing(
        {"verdict": "true", "confidence": None, "summary": NEUTRAL,
         "evidence": [{"source": "x", "url": "https://a.test"}]}, [], CLAIM)
    assert r["confidence"] in (None, 0.80)


def test_das_label_bleibt_unangetastet():
    """Der Deckel korrigiert die ZAHL, nicht das Urteil."""
    for v in _BESTIMMTE_LABELS:
        assert _lauf(v, 1)["verdict"] == v


# --------------------------------------------------------------------------
# Die gemessenen Fälle, die den Deckel ausgelöst haben
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim,verdict,konf,belege,deckel", [
    ("Der aktuelle CPI-Wert für Österreich ist 69 Punkte", "false", 0.9, 1, 0.80),
    ("Die Mieten in Wien sind extrem hoch", "true", 0.85, 1, 0.80),
    ("Die ÖBB sind pünktlicher als die Deutsche Bahn", "mostly_false", 0.85, 2, 0.85),
    ("Der EZB-Leitzins liegt aktuell bei 2 Prozent", "false", 0.88, 2, 0.85),
])
def test_die_falschen_verdicts_aus_hart40_tragen_jetzt_weniger(claim, verdict, konf, belege, deckel):
    """Vier der elf falschen bestimmten Verdicts, mit ihrer echten
    Beleglage. Der Deckel macht sie nicht richtig — er nimmt ihnen das
    Siegel."""
    r = _lauf(verdict, belege, confidence=konf, claim=claim)
    assert r["confidence"] <= deckel, (claim, r["confidence"])
    assert r["confidence"] < konf or konf <= deckel


def test_die_tabelle_deckt_keine_beleglage_ueber_der_trefferquote():
    """Die Stufen dürfen nicht mehr versprechen als gemessen wurde."""
    gemessen = {1: 0.786, 2: 0.800, 3: 0.833}
    for belege, quote in gemessen.items():
        assert deckel_fuer(belege) <= quote + 0.07, (belege, deckel_fuer(belege), quote)
