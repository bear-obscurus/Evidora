"""Ein bestimmtes Verdict ohne einen einzigen zitierbaren Beleg.

QA50F-Zensus: 32 % der Claims wurden ohne Evidenz beantwortet. Diese Zahl ist
aber **zu breit** — beim Nachzählen zerfällt sie:

    18/50 Claims ohne Evidenz
       davon 12 mit Verdict „unverifiable"   <- korrekt: nichts zu zitieren
       davon  6 mit BESTIMMTEM Verdict       <- der eigentliche Befund

Ein korrektes „unverifiable" ohne Evidenz ist keine Schwäche, sondern
Ehrlichkeit. Die sechs anderen verletzen die eigene Prompt-Regel („If no
evidence remains after the relevance filter, set verdict to unverifiable") —
und die Gültigkeitsprüfung im Synthesizer fängt sie nicht, weil sie nur
prüft, ob das FELD existiert. `"evidence": []` besteht sie.

Zwei Muster, beide gleich behandelt:

    #3   true@0.85,  14 von 24 Quellen lieferten Treffer, zitiert wurde nichts
    #46  false@0.7,   2 von 17 — die Antwort kam aus dem Vorwissen

**Bewusst keine Abwertung auf „unverifiable".** In fünf der sechs Fälle war
die Antwort richtig, und eine richtige Antwort wegzuwerfen macht den Dienst
schlechter, nicht ehrlicher. Was fehlt, ist nicht das Urteil, sondern der
Beleg — also wird die Selbstsicherheit gekappt und der fehlende Beleg
ausgesprochen. Dieselbe Logik wie beim Degraded-Analysis-Guard.
"""

import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import (  # noqa: E402
    UNBELEGT_CONFIDENCE_CAP,
    apply_unbelegt_cap,
)

BESTIMMT = ("true", "mostly_true", "mixed", "mostly_false", "false")


def _lauf(verdict, confidence, evidence, nuance=None, lang="de"):
    s = {"verdict": verdict, "confidence": confidence, "evidence": evidence}
    if nuance is not None:
        s["nuance"] = nuance
    return apply_unbelegt_cap(s, lang)


# --------------------------------------------------------------------------
# Der Befund
# --------------------------------------------------------------------------

@pytest.mark.parametrize("verdict", BESTIMMT)
def test_bestimmtes_verdict_ohne_evidenz_wird_gekappt(verdict):
    r = _lauf(verdict, 0.85, [])
    assert r["verdict"] == verdict, "Das Urteil bleibt — nur die Sicherheit sinkt"
    assert r["confidence"] == UNBELEGT_CONFIDENCE_CAP
    assert "keine einzelne Quelle als Beleg" in r["nuance"]
    assert r["_ohne_beleg"] is True


def test_der_ausloesende_fall():
    """QA50F #3: 14 von 24 Quellen lieferten Treffer, zitiert wurde nichts."""
    r = _lauf("true", 0.85, [])
    assert (r["verdict"], r["confidence"]) == ("true", 0.5)


def test_urteil_wird_nicht_weggeworfen():
    """Kein Downgrade auf „unverifiable": in fünf der sechs Fälle war die
    Antwort richtig. Eine richtige Antwort wegzuwerfen macht den Dienst
    schlechter, nicht ehrlicher."""
    for v in BESTIMMT:
        assert _lauf(v, 0.9, [])["verdict"] == v


# --------------------------------------------------------------------------
# Was NICHT gekappt werden darf
# --------------------------------------------------------------------------

def test_mit_evidenz_bleibt_alles_unveraendert():
    r = _lauf("true", 0.9, [{"source": "Frontex", "finding": "-12 %"}])
    assert r["confidence"] == 0.9
    assert "_ohne_beleg" not in r
    assert "nuance" not in r


def test_unverifiable_ohne_evidenz_ist_korrekt():
    """Zwölf der achtzehn evidenzlosen Claims waren „unverifiable" — dort ist
    fehlende Evidenz die richtige Antwort, kein Mangel."""
    r = _lauf("unverifiable", 0.15, [])
    assert r["confidence"] == 0.15
    assert "_ohne_beleg" not in r
    assert "nuance" not in r


def test_niedrige_konfidenz_wird_nicht_angehoben():
    """Der Cap ist eine Obergrenze, kein Zielwert."""
    r = _lauf("mixed", 0.3, [])
    assert r["confidence"] == 0.3
    assert r["_ohne_beleg"] is True, "Der Hinweis gehoert trotzdem dran"


def test_bestehende_nuance_bleibt_erhalten():
    r = _lauf("false", 0.7, [], nuance="Die Quelle misst nur Detektionen.")
    assert "Detektionen" in r["nuance"]
    assert "keine einzelne Quelle als Beleg" in r["nuance"]


def test_englischer_hinweis():
    r = _lauf("true", 0.9, [], lang="en")
    assert "No single source could be cited" in r["nuance"]
    assert "keine einzelne" not in r["nuance"]


# --------------------------------------------------------------------------
# Verdrahtung und Prompt
# --------------------------------------------------------------------------

def test_guard_laeuft_nach_dem_fallback_cap():
    """Reihenfolge zählt: der Fallback-Cap setzt womöglich schon auf 0,5 —
    dann darf dieser Guard nicht wieder anheben, und der Hinweis muss
    trotzdem angehängt werden."""
    quelle = (BACKEND / "main.py").read_text(encoding="utf-8")
    assert "apply_unbelegt_cap" in quelle
    assert (quelle.index("apply_analysis_fallback_cap(synthesis, analysis, lang)")
            < quelle.index("apply_unbelegt_cap(synthesis, lang)"))


def test_prompt_verlangt_den_beleg():
    """Der Guard ist die Auffanglinie. Die Ursache gehört in den Prompt —
    sonst produziert das Modell weiter unbelegte Verdicts und der Nutzer
    bekommt statt eines Belegs nur einen Hinweis auf dessen Fehlen."""
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "BELEGPFLICHT" in quelle
    assert "MUSS `evidence` mindestens einen Eintrag enthalten" in quelle
    assert "dann ist \"unverifiable\" die ehrliche Antwort" in quelle


def test_prompt_nennt_die_messung():
    """Eine Regel ohne Anlass wird beim nächsten Aufräumen gestrichen."""
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "QA50F" in quelle and "sechs von fünfzig" in quelle
