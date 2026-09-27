"""Muster O: das Label verneint, was die eigene Begründung bejaht —
bei einem Vergleich OHNE Schwellenzahl.

Anlass (HART40, 27.9.2026):

    Claim:   "Die ÖBB sind pünktlicher als die Deutsche Bahn"
    Label:   mostly_false @ 0.85
    Summary: "Die ÖBB erreichen 2024 im Fernverkehr 78,2–88,7 %
              Pünktlichkeit (5-Minuten-Toleranz), während die Deutsche Bahn
              im Fernverkehr nur 62,5 % (6-Minuten-Toleranz) aufweisen.
              Selbst bei strengerer Definition (5 vs. 6 Minuten) sind die
              ÖBB deutlich pünktlicher."

Muster M greift nicht (keine Umdeutungs-Formel), Muster N auch nicht: Der
Claim nennt KEINE Schwellenzahl, es ist ein reiner Vergleich.

⭐ Der Umweg, den die Messung erzwungen hat: Die erste Fassung verlangte in
der Summary die volle Form "<komparativ> als <B>". Im Sweep über 140 echte
Live-Läufe feuerte sie **null Mal** — auch nicht auf ihrem eigenen Auslöser,
weil der bestätigende Satz mit "sind die ÖBB deutlich pünktlicher." endet;
der Vergleichspartner steht im Satz davor. Gesucht wird jetzt das
Komparativ-WORT DES CLAIMS selbst.

Sweep mit der fertigen Fassung über dieselben 140 Läufe: **2 Treffer.**
Einer ist der Auslöser (wird korrigiert), einer trägt schon ein bejahendes
Label ("Elektrische Zahnbürsten putzen besser als Handzahnbürsten",
true@0.95) und bleibt unangetastet, weil das Muster nur unter
verneinenden Labels arbeitet.

Keine Netzabfrage, kein Modell.
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import (  # noqa: E402
    apply_verdict_postprocessing,
    summary_bestaetigt_vergleich,
    vergleich_aus_claim,
)

CLAIM = "Die ÖBB sind pünktlicher als die Deutsche Bahn"
# Der echte Wortlaut aus dem Live-Lauf vom 27.9.2026.
SUMMARY = (
    "Die ÖBB erreichen 2024 im Fernverkehr 78,2–88,7 % Pünktlichkeit "
    "(5-Minuten-Toleranz), während die Deutsche Bahn im Fernverkehr nur "
    "62,5 % (6-Minuten-Toleranz) aufweisen. Selbst bei strengerer Definition "
    "(5 vs. 6 Minuten) sind die ÖBB deutlich pünktlicher."
)


def _lauf(verdict, summary=SUMMARY, claim=CLAIM, belege=2):
    ev = [{"source": f"Q{i}", "url": f"https://b{i}.test/x"} for i in range(belege)]
    return apply_verdict_postprocessing(
        {"verdict": verdict, "confidence": 0.85, "summary": summary, "evidence": ev},
        [], claim)


# --------------------------------------------------------------------------
# Der Fall
# --------------------------------------------------------------------------

@pytest.mark.parametrize("eingang", ["false", "mostly_false"])
def test_verneinendes_label_wird_korrigiert(eingang):
    assert _lauf(eingang)["verdict"] == "true"


def test_der_bestaetigende_satz_braucht_kein_als():
    """Der Grund für den Umbau: Der Satz endet mit 'pünktlicher.', der
    Vergleichspartner steht im Satz davor."""
    assert "pünktlicher als" not in SUMMARY.split(".")[-2]
    assert summary_bestaetigt_vergleich(CLAIM.lower(), SUMMARY.lower())


def test_summary_bleibt_unveraendert():
    assert _lauf("mostly_false")["summary"] == SUMMARY


def test_bejahendes_label_wird_nicht_angefasst():
    """Das Muster arbeitet nur unter verneinenden Labels."""
    for v in ("true", "mostly_true", "mixed", "unverifiable"):
        assert _lauf(v)["verdict"] == v


# --------------------------------------------------------------------------
# Was NICHT als Bestätigung zählt
# --------------------------------------------------------------------------

@pytest.mark.parametrize("summary", [
    "Die Deutsche Bahn ist pünktlicher als die ÖBB.",          # gedreht
    "Die ÖBB wären pünktlicher als die Deutsche Bahn, wenn man anders misst.",
    "Die ÖBB sind nicht pünktlicher als die Deutsche Bahn.",
    "Die ÖBB könnten pünktlicher sein als die Deutsche Bahn.",
    "Die ÖBB sind teurer als die Deutsche Bahn.",              # anderer Komparativ
    "Die ÖBB sind pünktlich, die Deutsche Bahn ebenfalls.",    # kein Komparativ
])
def test_keine_bestaetigung(summary):
    assert not summary_bestaetigt_vergleich(CLAIM.lower(), summary.lower()), summary


def test_fehlender_vergleichspartner_zaehlt_nicht():
    """Ohne B in der Summary ist es nicht DERSELBE Vergleich."""
    assert not summary_bestaetigt_vergleich(
        CLAIM.lower(), "die öbb sind deutlich pünktlicher geworden.")


@pytest.mark.parametrize("claim", [
    "Die ÖBB sind pünktlich",                       # kein Vergleich
    "Impfungen verursachen Autismus",
    "In Deutschland sterben über 300 Frauen durch ihren Partner",
])
def test_claims_ohne_vergleich_werden_zerlegt_zu_nichts(claim):
    assert vergleich_aus_claim(claim.lower()) is None


# --------------------------------------------------------------------------
# Die Zerlegung
# --------------------------------------------------------------------------

def test_zerlegung_des_ausloesers():
    subjekt, komparativ, partner = vergleich_aus_claim(CLAIM.lower())
    assert "oebb" in subjekt
    assert komparativ == "puenktlicher"
    assert {"deutsche", "bahn"} <= partner


@pytest.mark.parametrize("claim,komparativ", [
    ("Österreich ist reicher als Ungarn", "reicher"),
    ("Windkraft liefert mehr Strom als Photovoltaik", "mehrer"),
    ("Die Schweiz ist teurer als Österreich", "teurer"),
])
def test_weitere_vergleichsformen(claim, komparativ):
    z = vergleich_aus_claim(claim.lower())
    if komparativ == "mehrer":
        assert z is None or z[1] != "mehrer"   # "mehr als" ist kein -er-Komparativ
    else:
        assert z and z[1] == komparativ, (claim, z)


# --------------------------------------------------------------------------
# Über-Trigger: gemessen an 140 echten Live-Läufen
# --------------------------------------------------------------------------

def test_der_zweite_treffer_aus_dem_sweep_bleibt_folgenlos():
    """Der Sweep fand zwei Bestätigungen in 140 Läufen. Die zweite trug
    schon ein bejahendes Label und darf deshalb nichts aendern."""
    claim = "Elektrische Zahnbürsten putzen besser als Handzahnbürsten"
    summary = ("Mehrere systematische Reviews zeigen, dass elektrische "
               "Zahnbürsten Plaque wirksamer entfernen als Handzahnbürsten. "
               "Elektrische Zahnbürsten putzen damit besser.")
    assert summary_bestaetigt_vergleich(claim.lower(), summary.lower())
    r = _lauf("true", summary=summary, claim=claim)
    assert r["verdict"] == "true"


# --------------------------------------------------------------------------
# Der Wiederholungssatz (Live-Regression 27.9.2026)
# --------------------------------------------------------------------------
# Eine Stunde nach dem Deploy live gemessen und zurueckgenommen: Der
# GEGENTEILIGE Claim wurde faelschlich auf true korrigiert.
#
#   Claim:   "Die Deutsche Bahn ist pünktlicher als die ÖBB"   (falsch)
#   Summary: "Die Behauptung sagt, die Deutsche Bahn (DB) sei pünktlicher
#             als die ÖBB. ... Damit ist die DB deutlich weniger pünktlich
#             als die ÖBB."
#
# Der erste Satz REFERIERT den Claim, bevor die Summary ihn widerlegt. Er
# traegt den Komparativ des Claims wörtlich und das Subjekt davor — fuer
# die erste Fassung sah er aus wie eine Bestaetigung.

GEGEN_CLAIM = "Die Deutsche Bahn ist pünktlicher als die ÖBB"
GEGEN_SUMMARY = (
    "Die Behauptung sagt, die Deutsche Bahn (DB) sei pünktlicher als die ÖBB. "
    "Daten zeigen: DB Fernverkehr 2024 bei 62,5 % Pünktlichkeit "
    "(6-Min-Toleranz), ÖBB Fernverkehr bei 78,2–88,7 % (5-Min-Toleranz). "
    "Damit ist die DB deutlich weniger pünktlich als die ÖBB."
)


def test_referat_des_claims_ist_keine_bestaetigung():
    assert not summary_bestaetigt_vergleich(GEGEN_CLAIM.lower(), GEGEN_SUMMARY.lower())


@pytest.mark.parametrize("eingang", ["false", "mostly_false"])
def test_der_gegenteilige_claim_bleibt_verneint(eingang):
    r = _lauf(eingang, summary=GEGEN_SUMMARY, claim=GEGEN_CLAIM)
    assert r["verdict"] == eingang, r["verdict"]


@pytest.mark.parametrize("satz", [
    "Die Behauptung, die ÖBB seien pünktlicher als die Deutsche Bahn, wird geprüft.",
    "Behauptet wird, die ÖBB seien pünktlicher als die Deutsche Bahn.",
    "Die Aussage lautet, die ÖBB seien pünktlicher als die Deutsche Bahn.",
])
def test_referats_formeln_zaehlen_nicht(satz):
    assert not summary_bestaetigt_vergleich(CLAIM.lower(), satz.lower()), satz


def test_der_ausloeser_wird_weiterhin_erkannt():
    """Der Wiederholungs-Guard darf den eigentlichen Fall nicht mitnehmen."""
    assert summary_bestaetigt_vergleich(CLAIM.lower(), SUMMARY.lower())
