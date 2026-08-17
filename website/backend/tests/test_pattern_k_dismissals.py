"""Pattern K: Dismissal-Abdeckung und Kopula-Gate (QA50D #305).

Live-Befund 2026-08-14: Claim „Dass Zugfahren gefährlicher ist als
Autofahren, ist doch SCHWACHSINN" — die Summary belegte ¬P sauber
(0,03 vs. 1,55 Getötete je Mrd. Personenkilometer), Pattern K schwieg,
Verdict blieb 6/6 falsch.

Zwei getrennte Ursachen, beide im Container reproduziert (5 von 8
geprüften Formulierungen scheiterten):

  Gate 1 — fehlende Literale: „schwachsinn", „unfug", „nonsens".
  Gate 2 — das Kopula-Gate erlaubte weder den UNBESTIMMTEN ARTIKEL noch
           gängige Verstärker. Dadurch fiel sogar „ist EIN Schmarrn"
           durch, obwohl „schmarrn" im Literal-Set steht.

Die Erweiterung macht K großzügiger — also in die RISKANTE Richtung.
Deshalb sind die Gegenproben hier wichtiger als die Treffer: Pattern K
darf eine Zurückweisung einer WAHREN Aussage nicht auf true flippen
(genau diese Überkorrektur wurde 2026-07-27 in PR #91 real gefangen).

Dependency-light: reine Kaskaden-Tests, kein Netz/LLM.
"""
import pytest

from services.verdict_postprocess import (
    _antimythos_flip,
    _ANTI_MYTHOS_DISMISSALS,
)

# Summary belegt ¬P über das Antonym (Bahn sicherer als Auto)
SUM_NICHT_P = ("zugfahren ist deutlich sicherer als autofahren. pro "
               "personenkilometer sterben im strassenverkehr rund 52-mal "
               "mehr menschen als auf der schiene.")
# Summary belegt P (Autofahren IST gefährlicher) — Zurückweisung ist falsch
SUM_P = ("autofahren ist deutlich gefährlicher als zugfahren. die "
         "behauptung, autofahren sei gefährlicher als zugfahren, ist "
         "damit bestätigt.")


def _flip(claim, summary):
    return _antimythos_flip(claim, claim.lower(), summary)


@pytest.mark.parametrize("dismissal", [
    # vor dem Fix bereits erkannt
    "ist ja wohl Unsinn", "ist doch Quatsch", "ist Humbug",
    # Gate 1 — Literale fehlten
    "ist doch Schwachsinn", "ist doch Unfug", "ist purer Nonsens",
    # Gate 2 — unbestimmter Artikel
    "ist ein Schmarrn", "ist doch ein Blödsinn", "ist ein Märchen",
    # Gate 2 — Verstärker
    "ist totaler Unsinn", "ist der reinste Unsinn",
    "ist ja wohl ein völliger Unsinn",
])
def test_dismissal_formen_werden_erkannt(dismissal):
    claim = f"Dass Zugfahren gefährlicher ist als Autofahren, {dismissal}."
    assert _flip(claim, SUM_NICHT_P) is True, dismissal


@pytest.mark.parametrize("dismissal", [
    "ist ja wohl Unsinn", "ist doch Schwachsinn", "ist ein Schmarrn",
    "ist totaler Unsinn",
])
def test_zurueckweisung_einer_WAHREN_aussage_kippt_nicht(dismissal):
    """PFLICHT-GEGENPROBE. Die Erweiterung macht K großzügiger — genau
    hier muss sie sich beweisen."""
    claim = f"Dass Autofahren gefährlicher ist als Zugfahren, {dismissal}."
    assert _flip(claim, SUM_P) is False, dismissal


def test_ohne_komparativ_feuert_k_per_design_nicht():
    """Bedingung 2 (Komparativ '<adj>er als <B>') ist der Schutz für
    Claims wie '… ist doch längst widerlegt' ohne Vergleich."""
    claim = "Dass die Masern-Impfung Autismus auslöst, ist doch Schwachsinn."
    assert _flip(claim, "es gibt keinen zusammenhang zwischen impfung "
                        "und autismus.") is False


def test_ohne_dismissal_feuert_k_nicht():
    claim = "Zugfahren ist gefährlicher als Autofahren."
    assert _flip(claim, SUM_NICHT_P) is False


# --- Contract: riskante Literale bleiben DRAUSSEN ---

@pytest.mark.parametrize("wort", ["käse", "kaese", "topfen", "quark"])
def test_lebensmittel_woerter_sind_keine_dismissals(wort):
    """„Käse" und „Topfen" heißen umgangssprachlich auch „Unsinn", sind
    aber zugleich Lebensmittel — sie würden auf Ernährungs-Claims feuern
    (der Cluster-B-Fakt zu Magertopfen ist genau so ein Fall)."""
    assert wort not in _ANTI_MYTHOS_DISMISSALS


def test_neue_literale_sind_vorhanden():
    for w in ("schwachsinn", "unfug", "nonsens"):
        assert w in _ANTI_MYTHOS_DISMISSALS, w
