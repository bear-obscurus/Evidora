"""Das Label darf seiner eigenen Begruendung nicht widersprechen.

Anlass (QA50F, 2026-09-25). Claim: "In Deutschland sterben jedes Jahr ueber
300 Frauen durch ihren Partner". Antwort, dreimal hintereinander identisch:

    verdict: true @ 0.9
    summary: "Laut BKA-Bundeslagebild 2024 starben in Deutschland 133 Frauen
              durch vollendete Partnerschaftstoetungen. Die Behauptung von
              'ueber 300' bezieht sich vermutlich auf alle weiblichen Opfer
              von Toetungsdelikten (328 vollendet), nicht nur auf
              Partnerschaftskontext."

Die Begruendung widerlegt die Behauptung, das Label bejaht sie. Keine
Varianz — dreimal derselbe Wortlaut. Ein Faktencheck-Dienst, der im Text das
Gegenteil dessen sagt, was sein Label behauptet, ist schlimmer als einer, der
"weiss nicht" sagt.

Der bestehende 4-Tier-Consistency-Check greift hier nicht: Er sucht
Schlussformeln ("die Behauptung ist falsch"), und eine Umdeutung ist keine.

Muster M wertet deshalb auf ``mixed`` ab statt auf ``false``: Die Umdeutung
sagt, dass die Zahl etwas anderes meint — nicht zwingend, dass die Behauptung
in jeder Lesart falsch ist. Sagt die Summary irgendwo ausdruecklich, dass die
Behauptung zutrifft, ist die Umdeutung nur eine Praezisierung und das Muster
schweigt.

Nachtrag (2026-09-25): Vor M laeuft seit PR #202 Muster N, das die
Claim-Schwelle gegen die zugeschriebene Zahl rechnet (133 < 300) und dort
``false`` setzt, wo die Rechnung aufgeht. M ist seither das Auffangnetz fuer
die Faelle ohne vergleichbare Zahl. Die Femizid-Faelle in dieser Datei enden
darum bei ``false``; das exakte Label prueft
``test_muster_n_schwellenzahl.py``, hier steht die Zusage, dass am Ende kein
bejahendes Label uebrig bleibt.

Keine Netzabfrage, kein Modell.
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import (  # noqa: E402
    _BESTAETIGUNG_MUSTER,
    _UMDEUTUNG_MUSTER,
    apply_verdict_postprocessing,
)

# Die Original-Summary aus dem Live-Lauf vom 25.9.2026.
FEMIZID_SUMMARY = (
    "Laut BKA-Bundeslagebild 2024 starben in Deutschland 133 Frauen durch "
    "vollendete Partnerschaftstötungen. Die Behauptung von 'über 300' bezieht "
    "sich vermutlich auf alle weiblichen Opfer von Tötungsdelikten (328 "
    "vollendet), nicht nur auf Partnerschaftskontext."
)
FEMIZID_CLAIM = "In Deutschland sterben jedes Jahr über 300 Frauen durch ihren Partner"

# Zweite live beobachtete Formulierung (25.9., nach der Fakt-Ergaenzung):
# dieselbe Umdeutung, anderes Wort — "zu hoch angesetzt" statt "nicht nur".
FEMIZID_SUMMARY_V2 = (
    "Laut BKA-Bundeslagebild 2024 starben in Deutschland 133 Frauen durch "
    "vollendete Tötungsdelikte im Partnerschaftskontext. Die Behauptung von "
    "'über 300' bezieht sich vermutlich auf alle weiblichen Opfer von "
    "Tötungsdelikten (328 vollendet), ist aber für den Partnerschaftskontext "
    "zu hoch angesetzt."
)


def _lauf(verdict, summary, claim=FEMIZID_CLAIM, confidence=0.9):
    result = {"verdict": verdict, "confidence": confidence, "summary": summary,
              "evidence": [{"source": "BKA", "url": "https://example.test/x"}]}
    return apply_verdict_postprocessing(result, [], claim)


# --------------------------------------------------------------------------
# Der Fall aus QA50F
# --------------------------------------------------------------------------

def test_umdeutung_wird_abgewertet():
    r = _lauf("true", FEMIZID_SUMMARY)
    assert r["verdict"] not in ("true", "mostly_true"), r["verdict"]


def test_auch_mostly_true_wird_abgewertet():
    r = _lauf("mostly_true", FEMIZID_SUMMARY)
    assert r["verdict"] not in ("true", "mostly_true")


# Umdeutung ohne vergleichbare Zahl: hier kann Muster N nichts rechnen, und
# M muss allein tragen. Das ist der Fall, der die Abwertung auf "mixed"
# ueberhaupt begruendet.
UMDEUTUNG_OHNE_ZAHL = (
    "Die Behauptung von 'über 300' bezieht sich vermutlich auf alle "
    "weiblichen Opfer von Tötungsdelikten, nicht nur auf den "
    "Partnerschaftskontext."
)


def test_umdeutung_ohne_vergleichszahl_endet_bei_mixed():
    r = _lauf("true", UMDEUTUNG_OHNE_ZAHL)
    assert r["verdict"] == "mixed", r["verdict"]
    assert r["confidence"] <= 0.6


def test_summary_bleibt_unveraendert():
    """Abgewertet wird das Label, nicht die Erklaerung."""
    r = _lauf("true", FEMIZID_SUMMARY)
    assert r["summary"] == FEMIZID_SUMMARY


# --------------------------------------------------------------------------
# Was NICHT feuern darf
# --------------------------------------------------------------------------

def test_praezisierung_mit_bestaetigung_bleibt_true():
    """"… bezieht sich auf X, nicht auf Y — sie ist dennoch korrekt." bleibt."""
    s = ("Die Behauptung, dass 3,3 % der Proben den Höchstgehalt überschreiten, "
         "bezieht sich auf die nationalen Programme, nicht auf das EU-Programm "
         "— sie ist dennoch korrekt.")
    r = _lauf("true", s, claim="Mehr als 3 % der Proben überschreiten den Grenzwert")
    assert r["verdict"] == "true"


def test_bestaetigende_schlussformel_bleibt_true():
    s = ("Der EFSA-Bericht zeigt, dass 3,3 % der Lebensmittelproben die "
         "Pestizid-Höchstgehalte überschritten. Die Behauptung, dass mehr als "
         "3 % die Höchstgehalte überschreiten, ist damit bestätigt.")
    r = _lauf("true", s, claim="Mehr als 3 % der Proben überschreiten den Grenzwert")
    assert r["verdict"] == "true"


@pytest.mark.parametrize("eingang", ["true", "mostly_true", "mixed", "false", "mostly_false"])
def test_die_kaskade_bejaht_diesen_claim_nie(eingang):
    """Die eigentliche Zusage: Egal mit welchem Label das Modell ankommt —
    bei DIESER Begruendung darf am Ende kein bejahendes Label stehen.

    (Mit ``false`` als Eingang flippt der 4-Tier-Check zuerst auf ``true``,
    weil er die Claim-Zahl in der Summary als Bestaetigung liest; Muster M
    faengt das danach ab. Genau dafuer steht es hinter dem Check.)"""
    r = _lauf(eingang, FEMIZID_SUMMARY)
    assert r["verdict"] not in ("true", "mostly_true"), r["verdict"]


def test_summary_ohne_zahl_feuert_nicht():
    s = ("Die Behauptung bezieht sich auf einen anderen Zeitraum, nicht auf das "
         "Jahr 2024.")
    assert not _UMDEUTUNG_MUSTER.search(s.lower())


def test_umdeutung_ohne_negation_feuert_nicht():
    s = "Die Behauptung von 300 Fällen bezieht sich auf das Jahr 2024."
    assert not _UMDEUTUNG_MUSTER.search(s.lower())


# --------------------------------------------------------------------------
# Die Muster selbst
# --------------------------------------------------------------------------

def test_zweite_live_formulierung_wird_auch_erkannt():
    """Dieselbe Umdeutung, anderes Wort: "zu hoch angesetzt" statt "nicht".
    Die erste Fassung des Musters verlangte ein literales "nicht" und ging
    darum live nicht an."""
    assert _UMDEUTUNG_MUSTER.search(FEMIZID_SUMMARY_V2.lower())
    r = _lauf("true", FEMIZID_SUMMARY_V2)
    assert r["verdict"] not in ("true", "mostly_true"), r["verdict"]


def test_umdeutungsmuster_trifft_den_originalfall():
    assert _UMDEUTUNG_MUSTER.search(FEMIZID_SUMMARY.lower())
    assert not _BESTAETIGUNG_MUSTER.search(FEMIZID_SUMMARY.lower())


@pytest.mark.parametrize("satz", [
    "die behauptung ist damit bestätigt",
    "die behauptung ist somit korrekt",
    "die behauptung trifft damit zu",
    "die zahl ist richtig",
])
def test_bestaetigungsmuster_erkennt_zustimmung(satz):
    assert _BESTAETIGUNG_MUSTER.search(satz)


@pytest.mark.parametrize("satz", [
    "die behauptung ist damit widerlegt",
    "die zahlen sind nicht vergleichbar",
])
def test_bestaetigungsmuster_ohne_falsch_positive(satz):
    assert not _BESTAETIGUNG_MUSTER.search(satz)


# --------------------------------------------------------------------------
# Datenseite: die Zahl, die die Frage beantwortet
# --------------------------------------------------------------------------

def test_femizid_fakt_ordnet_jeder_frage_ihre_zahl_zu():
    """Der Guard ist das Sicherheitsnetz — die Ursache liegt im Fakt."""
    import json
    d = json.loads((BACKEND / "data" / "gleichstellung_pack.json").read_text(encoding="utf-8"))
    f = next(x for x in d["facts"] if x["id"] == "femizide_at_de_2026")
    feld = f["data"]["welche_zahl_beantwortet_was_de"]
    assert "133" in feld and "328" in feld and "308" in feld
    assert "keine Zahl von Getöteten" in feld
