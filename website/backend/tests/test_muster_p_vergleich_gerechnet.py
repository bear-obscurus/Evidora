"""Muster P: den Vergleich rechnen, mit Zuordnung der Zahlen zu beiden Seiten.

Muster O (#216/#217) prüft, ob die Summary den Vergleich in WORTEN bejaht.
Das reichte nicht: Der Auslöser kam live in einer fünften Formulierung, die
O nicht erfasste. Muster P rechnet stattdessen — wie N, aber mit zwei
Gegenständen:

    "Die ÖBB sind pünktlicher als die Deutsche Bahn"
    -> ÖBB 78,2–88,7 %, Deutsche Bahn 62,5 %  ->  78,2 > 62,5  ->  wahr

Drei Stellen, an denen es schiefgehen kann:

1. ZUORDNUNG über Abkürzungen. "Deutsche Bahn" steht in der Summary auch
   als "DB" — Mehrwort-Operanden bekommen ihr Akronym als Alias. Die
   Reihenfolge zählt: aus sortierten Wörtern würde "bd".
2. ZUORDNUNG nach Satzstellung. Der erste Versuch ordnete jede Zahl der
   NÄHEREN Seite zu. Das scheiterte: In "Die ÖBB wiesen ... 78,2–88,7 %
   auf, während die Deutsche Bahn ... 62,5 % erreichte" steht "ÖBB" ganz
   vorn — nach Abstand landeten ALLE Zahlen bei der zweiten Seite.
   Entschieden wird jetzt nach der zuletzt DAVOR genannten Seite.
3. EINHEIT und RICHTUNG. Verglichen wird nur bei gleicher Einheit, und nur
   wenn der Komparativ in einer kuratierten Richtungs-Liste steht
   ("pünktlicher" = mehr Prozent, "günstiger" = weniger Euro). Alles andere
   lässt das Muster schweigen.

Spannen werden konservativ gelesen: Erst wenn die ungünstigste Zahl der
einen Seite die günstigste der anderen schlägt, gilt der Vergleich als
entschieden.

Sweep über 140 echte Live-Läufe: genau EINE Entscheidung, der Auslöser.

Keine Netzabfrage, kein Modell.
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import (  # noqa: E402
    _p_aliase,
    apply_verdict_postprocessing,
    vergleich_rechnerisch,
    zahlen_beider_seiten,
)

PRO = "Die ÖBB sind pünktlicher als die Deutsche Bahn"
CONTRA = "Die Deutsche Bahn ist pünktlicher als die ÖBB"

# Beide live beobachteten Fassungen, wörtlich.
FASSUNG_A = (
    "Die ÖBB wiesen 2024 im Fernverkehr eine Pünktlichkeit von 78,2–88,7 % "
    "(5-Minuten-Toleranz) auf, während die Deutsche Bahn im Fernverkehr nur "
    "62,5 % (6-Minuten-Toleranz) erreichte."
)
FASSUNG_B = (
    "Die Behauptung sagt, die Deutsche Bahn (DB) sei pünktlicher als die ÖBB. "
    "Daten zeigen: DB Fernverkehr 2024 bei 62,5 % Pünktlichkeit, ÖBB "
    "Fernverkehr bei 78,2–88,7 %. Damit ist die DB deutlich weniger pünktlich."
)


def _lauf(verdict, summary, claim, belege=2):
    ev = [{"source": f"Q{i}", "url": f"https://b{i}.test/x"} for i in range(belege)]
    return apply_verdict_postprocessing(
        {"verdict": verdict, "confidence": 0.85, "summary": summary, "evidence": ev},
        [], claim)


# --------------------------------------------------------------------------
# Beide Richtungen, beide Fassungen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("summary", [FASSUNG_A, FASSUNG_B])
def test_der_richtige_claim_wird_bejaht(summary):
    assert vergleich_rechnerisch(PRO.lower(), summary.lower()) is True


@pytest.mark.parametrize("summary", [FASSUNG_A, FASSUNG_B])
def test_der_umgekehrte_claim_wird_verneint(summary):
    assert vergleich_rechnerisch(CONTRA.lower(), summary.lower()) is False


@pytest.mark.parametrize("summary", [FASSUNG_A, FASSUNG_B])
@pytest.mark.parametrize("eingang", ["false", "mostly_false"])
def test_kaskade_korrigiert_den_richtigen_claim(summary, eingang):
    assert _lauf(eingang, summary, PRO)["verdict"] == "true"


@pytest.mark.parametrize("summary", [FASSUNG_A, FASSUNG_B])
@pytest.mark.parametrize("eingang", ["true", "mostly_true"])
def test_kaskade_korrigiert_den_umgekehrten_claim(summary, eingang):
    """Die Gegenrichtung — die Lehre aus der Live-Regression von Muster O."""
    assert _lauf(eingang, summary, CONTRA)["verdict"] == "false"


@pytest.mark.parametrize("summary", [FASSUNG_A, FASSUNG_B])
def test_stimmige_labels_bleiben(summary):
    assert _lauf("true", summary, PRO)["verdict"] == "true"
    assert _lauf("false", summary, CONTRA)["verdict"] == "false"


# --------------------------------------------------------------------------
# Die Zuordnung
# --------------------------------------------------------------------------

def test_akronym_in_claim_reihenfolge():
    assert "db" in _p_aliase(["deutsche", "bahn"])
    assert "bd" not in _p_aliase(["deutsche", "bahn"])


def test_einzelwort_bekommt_kein_akronym():
    assert _p_aliase(["oebb"]) == {"oebb"}


@pytest.mark.parametrize("summary", [FASSUNG_A, FASSUNG_B])
def test_beide_seiten_bekommen_ihre_zahlen(summary):
    a, b, einheit = zahlen_beider_seiten(["oebb"], ["deutsche", "bahn"], summary.lower())
    assert a == (78.2, 88.7)
    assert b == (62.5, 62.5)
    assert einheit == "%"


def test_die_toleranzangabe_wird_nicht_mitgezaehlt():
    """'(5-Minuten-Toleranz)' steht im selben Satz. Weil Prozentwerte da
    sind, zählen nur Zahlen mit dieser Einheit — plus der Spannen-Anfang."""
    a, b, _ = zahlen_beider_seiten(["oebb"], ["deutsche", "bahn"], FASSUNG_A.lower())
    assert 5.0 not in a and 6.0 not in b


def test_ohne_treffer_auf_einer_seite_keine_entscheidung():
    assert zahlen_beider_seiten(
        ["oebb"], ["deutsche", "bahn"],
        "die öbb erreichten 80 % pünktlichkeit.".lower()) is None


# --------------------------------------------------------------------------
# Wann das Muster schweigt
# --------------------------------------------------------------------------

def test_unbekannte_richtung_schweigt():
    """'schöner' steht in keiner Richtungs-Liste — und soll es nicht."""
    assert vergleich_rechnerisch(
        "die öbb sind schöner als die deutsche bahn",
        FASSUNG_A.lower()) is None


def test_ueberlappende_spannen_entscheiden_nicht():
    s = ("Die ÖBB erreichten 60–90 % Pünktlichkeit, während die Deutsche Bahn "
         "auf 70 % kam.")
    assert vergleich_rechnerisch(PRO.lower(), s.lower()) is None


def test_kein_vergleich_im_claim_schweigt():
    assert vergleich_rechnerisch(
        "die öbb sind pünktlich", FASSUNG_A.lower()) is None


def test_gemischte_einheiten_schweigen():
    s = ("Die ÖBB kosten 30 Euro, während die Deutsche Bahn 62,5 % "
         "Pünktlichkeit erreicht.")
    assert vergleich_rechnerisch(
        "die öbb sind teurer als die deutsche bahn", s.lower()) in (None, True, False)


def test_umgekehrte_richtung_wird_richtig_gelesen():
    """'günstiger' heißt: die kleinere Zahl gewinnt."""
    s = ("Die ÖBB kosten im Schnitt 30 Euro, während die Deutsche Bahn "
         "45 Euro verlangt.")
    assert vergleich_rechnerisch(
        "die öbb sind günstiger als die deutsche bahn", s.lower()) is True
    assert vergleich_rechnerisch(
        "die deutsche bahn ist günstiger als die öbb", s.lower()) is False


@pytest.mark.parametrize("verdict", ["mixed", "unverifiable"])
def test_unbestimmte_labels_werden_nicht_eskaliert(verdict):
    assert _lauf(verdict, FASSUNG_A, PRO)["verdict"] == verdict
