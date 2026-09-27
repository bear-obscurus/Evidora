"""Muster N: Rechnen statt lesen — der Schwellen-Claim gegen die Zahl,
die die Summary dem Gegenstand des Claims zuschreibt.

Anlass (QA50F, 2026-09-24/25). Derselbe Claim, dreimal live, drei
Formulierungen, immer ``true @ 0.9``:

    Claim:  "In Deutschland sterben jedes Jahr ueber 300 Frauen durch ihren
             Partner"
    Summary: "... starben in Deutschland 133 Frauen durch vollendete
             Toetungsdelikte im Partnerschaftskontext ..."

Muster M (Umdeutung, PR #199/#201) fing die ersten beiden Fassungen, weil
sie ein Widerlegungs-Signal trugen ("nicht nur", "zu hoch angesetzt"). Die
dritte stellte dieselbe Umdeutung rein sachlich fest — "wobei 133 davon im
Partnerschaftskontext lagen" — und trug gar keins. Die Lehre aus #140/#141
gilt auch hier: Formulierungen aufzaehlen skaliert nicht.

Also der Vergleich. Der Claim nennt eine Schwelle (300), die Summary
schreibt dem Gegenstand des Claims eine Zahl zu (133) — 133 < 300, die
Behauptung ist in ihrer eigenen Lesart widerlegt. Muster N laeuft VOR
Muster M, weil "false" praeziser ist als das "mixed", auf das sich M ohne
Zahlenvergleich beschraenken muss.

Die Zuschreibung ist die heikle Stelle, deshalb ist sie streng: mindestens
zwei Claim-Inhaltswoerter im Umfeld von +-70 Zeichen, Jahreszahlen,
Prozente, Geldbetraege, Grenzwerte und Altersangaben ausgeschlossen, und
bei zwei gleich gut verankerten, verschiedenen Zahlen lieber nichts tun.

Keine Netzabfrage, kein Modell.
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import (  # noqa: E402
    _N_SCHWELLE_RE,
    _n_inhaltswoerter,
    apply_verdict_postprocessing,
    zahl_zum_claim_gegenstand,
)

CLAIM = "In Deutschland sterben jedes Jahr über 300 Frauen durch ihren Partner"

# Die drei live beobachteten Fassungen, woertlich.
LIVE_24_09 = (
    "Laut BKA-Bundeslagebild 2024 starben in Deutschland 133 Frauen durch "
    "vollendete Partnerschaftstötungen. Die Behauptung von 'über 300' bezieht "
    "sich vermutlich auf alle weiblichen Opfer von Tötungsdelikten (328 "
    "vollendet), nicht nur auf Partnerschaftskontext."
)
LIVE_25_09_A = (
    "Laut BKA-Bundeslagebild 2024 starben in Deutschland 133 Frauen durch "
    "vollendete Tötungsdelikte im Partnerschaftskontext. Die Behauptung von "
    "'über 300' bezieht sich vermutlich auf alle weiblichen Opfer von "
    "Tötungsdelikten (328 vollendet), ist aber für den Partnerschaftskontext "
    "zu hoch angesetzt."
)
# Diese Fassung traegt KEIN Widerlegungs-Signal — nur Muster N faengt sie.
LIVE_25_09_B = (
    "Laut BKA-Bundeslagebild 2024 starben in Deutschland 133 Frauen durch "
    "Tötungsdelikte im Partnerschaftskontext. Die Behauptung von 'über 300' "
    "bezieht sich vermutlich auf die Gesamtzahl der weiblichen Opfer (328 "
    "vollendete Tötungsdelikte), wobei 133 davon im Partnerschaftskontext "
    "lagen."
)
LIVE = {"24.9.": LIVE_24_09, "25.9. a": LIVE_25_09_A, "25.9. b": LIVE_25_09_B}


def _lauf(verdict, summary, claim=CLAIM, confidence=0.9):
    result = {"verdict": verdict, "confidence": confidence, "summary": summary,
              "evidence": [{"source": "BKA", "url": "https://example.test/x"}]}
    return apply_verdict_postprocessing(result, [], claim)


# --------------------------------------------------------------------------
# Der Fall aus QA50F — alle drei Fassungen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("fassung", sorted(LIVE))
@pytest.mark.parametrize("eingang", ["true", "mostly_true"])
def test_bejahendes_label_wird_korrigiert(fassung, eingang):
    assert _lauf(eingang, LIVE[fassung])["verdict"] == "false"


def test_die_sachliche_dritte_fassung_ist_der_eigentliche_grund():
    """Sie traegt kein Widerlegungs-Signal — Muster M schweigt hier."""
    from services.verdict_postprocess import _UMDEUTUNG_MUSTER
    assert not _UMDEUTUNG_MUSTER.search(LIVE_25_09_B.lower())
    assert _lauf("true", LIVE_25_09_B)["verdict"] == "false"


def test_summary_bleibt_unveraendert():
    """Korrigiert wird das Label, nicht die Erklaerung."""
    assert _lauf("true", LIVE_25_09_B)["summary"] == LIVE_25_09_B


@pytest.mark.parametrize("fassung", sorted(LIVE))
def test_die_zuschreibung_trifft_133(fassung):
    treffer = zahl_zum_claim_gegenstand(CLAIM.lower(), LIVE[fassung].lower(), "300")
    assert treffer is not None, fassung
    wert, woerter = treffer
    assert wert == 133.0
    assert {"partner", "frauen"} <= woerter


# --------------------------------------------------------------------------
# Die andere Richtung: die Rechnung darf auch bestaetigen
# --------------------------------------------------------------------------

BESTAETIGT = (
    "Laut BKA-Bundeslagebild 2024 starben in Deutschland 133 Frauen durch "
    "Tötungsdelikte im Partnerschaftskontext."
)


def test_zutreffende_schwelle_korrigiert_ein_falsches_false():
    c = "In Deutschland sterben jedes Jahr über 100 Frauen durch ihren Partner"
    assert _lauf("false", BESTAETIGT, claim=c)["verdict"] == "true"


def test_zutreffende_schwelle_laesst_true_stehen():
    c = "In Deutschland sterben jedes Jahr über 100 Frauen durch ihren Partner"
    assert _lauf("true", BESTAETIGT, claim=c)["verdict"] == "true"


@pytest.mark.parametrize("claim_teil,erwartet", [
    ("über 100", "true"),          # 133 > 100
    ("über 300", "false"),         # 133 < 300
    ("mehr als 300", "false"),
    ("mindestens 100", "true"),    # Grenze eingeschlossen
    ("mindestens 134", "false"),
    ("höchstens 200", "true"),
    ("höchstens 132", "false"),
    ("unter 300", "true"),         # 133 < 300
    ("unter 100", "false"),
    ("weniger als 100", "false"),
])
def test_vergleichsrichtungen(claim_teil, erwartet):
    c = f"In Deutschland sterben jedes Jahr {claim_teil} Frauen durch ihren Partner"
    eingang = "true" if erwartet == "false" else "false"
    assert _lauf(eingang, BESTAETIGT, claim=c)["verdict"] == erwartet


# --------------------------------------------------------------------------
# Was NICHT feuern darf
# --------------------------------------------------------------------------

def test_prozentzahlen_zaehlen_nicht_als_zuschreibung():
    s = ("In Deutschland sank die Zahl der getöteten Frauen im "
         "Partnerschaftskontext um 8,4 % gegenüber dem Vorjahr.")
    assert zahl_zum_claim_gegenstand(CLAIM.lower(), s.lower(), "300") is None


def test_jahreszahlen_zaehlen_nicht_als_zuschreibung():
    s = ("Laut BKA-Bundeslagebild 2024 liegen für Deutschland keine Zahlen zu "
         "getöteten Frauen im Partnerschaftskontext vor.")
    assert zahl_zum_claim_gegenstand(CLAIM.lower(), s.lower(), "300") is None


def test_geldbetraege_zaehlen_nicht_als_zuschreibung():
    s = ("Deutschland gibt für den Schutz von Frauen vor ihren Partnern "
         "jährlich 120 Mio. Euro aus.")
    assert zahl_zum_claim_gegenstand(CLAIM.lower(), s.lower(), "300") is None


def test_andere_groessenordnung_wird_nicht_verglichen():
    """Faktor > 1000 spricht fuer eine andere Einheit, nicht fuer einen
    Widerspruch — hier Anzeigen statt Getoetete."""
    s = ("In Deutschland erfassten die Behörden 938.000 Fälle von Gewalt "
         "gegen Frauen durch ihren Partner.")
    assert _lauf("true", s)["verdict"] == "true"


def test_mehrdeutige_zuschreibung_wird_nicht_entschieden():
    """Zwei gleich gut verankerte, verschiedene Zahlen — lieber nichts tun."""
    s = ("Getötete Frauen in Deutschland: 133 nach der einen Zählweise, 250 "
         "nach der anderen.")
    assert zahl_zum_claim_gegenstand(CLAIM.lower(), s.lower(), "300") is None


def test_die_bessere_verankerung_gewinnt():
    """Zwei Zahlen, aber eine liegt naeher am Gegenstand des Claims: 133 hat
    'deutschland', 'frauen' und 'partner' im Umfeld, 24 nur zwei davon."""
    s = ("In Deutschland starben 133 Frauen durch ihren Partner, in "
         "Österreich starben 24 Frauen durch ihren Partner.")
    wert, woerter = zahl_zum_claim_gegenstand(CLAIM.lower(), s.lower(), "300")
    assert wert == 133.0 and "deutschland" in woerter


def test_gleiche_zahl_wie_im_claim_entscheidet_nichts():
    """Bekannte Grenze: Nennt die Summary genau die Claim-Zahl, ist sie von
    einem Zitat der Behauptung nicht zu unterscheiden — dann schweigt das
    Muster, statt auf einer eigenen Lesart zu bestehen."""
    s = "In Deutschland starben 300 Frauen durch ihren Partner."
    assert zahl_zum_claim_gegenstand(CLAIM.lower(), s.lower(), "300") is None


def test_claim_ohne_anker_wird_nicht_entschieden():
    assert zahl_zum_claim_gegenstand("es sind über 300", "hier stehen 133 fälle", "300") is None


def test_summary_ohne_zahl_laesst_das_label_stehen():
    s = "Belastbare Zahlen zu Tötungen von Frauen durch Partner fehlen."
    assert _lauf("true", s)["verdict"] == "true"


def test_claim_ohne_schwelle_laesst_das_label_stehen():
    assert _lauf("true", BESTAETIGT,
                 claim="In Deutschland sterben Frauen durch ihren Partner")["verdict"] == "true"


@pytest.mark.parametrize("eingang", ["mixed", "unverifiable"])
def test_unentschiedene_labels_werden_nicht_eskaliert(eingang):
    """Muster N korrigiert einen Widerspruch, es faellt kein neues Urteil."""
    assert _lauf(eingang, LIVE_25_09_B)["verdict"] == eingang


# --------------------------------------------------------------------------
# Die Bausteine
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,op,zahl", [
    ("über 300 Frauen", "über", "300"),
    ("mehr  als 1.500 Fälle", "mehr  als", "1.500"),
    ("weniger als 2,5 Prozent", "weniger als", "2,5"),
    ("mindestens 40 Morde", "mindestens", "40"),
    ("höchstens 12 Betriebe", "höchstens", "12"),
])
def test_schwellenmuster(text, op, zahl):
    m = _N_SCHWELLE_RE.search(text.lower())
    assert m and m.group(1) == op.lower() and m.group(2) == zahl


@pytest.mark.parametrize("text", ["genau 300 Frauen", "rund 300 Frauen", "300 Frauen"])
def test_schwellenmuster_ohne_falsch_positive(text):
    assert not _N_SCHWELLE_RE.search(text.lower())


def test_inhaltswoerter_lassen_funktionswoerter_weg():
    w = _n_inhaltswoerter(CLAIM.lower())
    assert {"deutschland", "sterben", "frauen", "partner"} <= w
    assert "jedes" not in w and "durch" not in w and "ihren" not in w
