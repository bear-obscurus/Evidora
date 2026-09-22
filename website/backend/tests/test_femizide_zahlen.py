"""Tötungsdelikte an Frauen: amtliche Zahlen und saubere Zählweisen.

Anlass (2026-09-22): Der Fakt nannte "AT 2023: 31 vollendete Femizide (BMI)"
und "DE 2024: 155 vollendete Femizide (BKA)". Beide Zahlen stehen so in
keinem amtlichen Bericht. Nachgelesen:

  Österreich, PKS 2024 (Aufbereitung der Nationalen Koordinierungsstelle
  "Gewalt gegen Frauen", Stand Dezember 2025), § 75 StGB:
      285 Opfer gesamt — 98 weiblich, 187 männlich
      weibliche Opfer: 58 Mordversuche, 40 vollendete Morde
      männliche Opfer: 151 Mordversuche, 36 vollendete Morde

  Deutschland, BKA-Bundeslagebild 2024 (21.11.2025):
      859 weibliche Opfer versuchter und vollendeter Tötungsdelikte
      328 davon vollendet, 308 tödlich verletzt
      häusliche Gewalt: 198 vollendete Taten (133 Partnerschaft, 65 Familie)
      Anteil weiblicher Opfer an vollendeten Partnerschaftstötungen 84,7 %

Dazu kam eine Falle, in die auch Medien laufen: Die Zahl 308 steht im
BKA-Bericht an zwei Stellen fuer Verschiedenes — tödlich verletzte Frauen
insgesamt UND weibliche Opfer von Tötungsdelikten bei Partnerschaftsgewalt
inklusive Versuchen. Der Fakt sagt das jetzt dazu.

Keine Netzabfrage in diesen Tests.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"
sys.path.insert(0, str(BACKEND))

from services._struct_marker import has_false_verdict_override  # noqa: E402
from services._topic_match import substring_or_composite_match as trifft  # noqa: E402


def _fakt(datei: str, fakt_id: str) -> dict:
    def such(o):
        if isinstance(o, dict):
            if o.get("id") == fakt_id:
                return o
            for v in o.values():
                if (r := such(v)) is not None:
                    return r
        elif isinstance(o, list):
            for v in o:
                if (r := such(v)) is not None:
                    return r
        return None
    f = such(json.loads((DATA / datei).read_text(encoding="utf-8")))
    assert f is not None, (datei, fakt_id)
    return f


F = _fakt("gleichstellung_pack.json", "femizide_at_de_2026")
# Geprueft wird die Aussage (headline + data); die "Korrigiert"-Notiz nennt
# die alten Falschzahlen absichtlich.
TEXT = F["headline"] + " " + json.dumps(F["data"], ensure_ascii=False)


# --------------------------------------------------------------------------
# Österreich
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["285", "98", "187", "58", "40", "151", "36"])
def test_at_zahlen_stehen_im_fakt(wert):
    assert wert in F["data"]["at_2024_pks"], (wert, F["data"]["at_2024_pks"])


def test_at_trennt_versuch_von_vollendung():
    a = F["data"]["at_2024_pks"]
    assert "Mordversuche" in a and "vollendete Morde" in a


def test_at_beziehungsverhaeltnis_ist_belegt():
    b = F["data"]["at_2024_beziehung"]
    assert "n = 90" in b
    assert "Partnerschaft mit Hausgemeinschaft 33 %" in b
    assert "Ex-Partnerschaft ohne Hausgemeinschaft 8 %" in b


# --------------------------------------------------------------------------
# Deutschland
# --------------------------------------------------------------------------

@pytest.mark.parametrize("wert", ["859", "328", "308", "198", "133", "65", "84,7 %"])
def test_de_zahlen_stehen_im_fakt(wert):
    assert wert in F["data"]["de_2024_bka"], (wert, F["data"]["de_2024_bka"])


def test_de_doppelbedeutung_der_308_ist_erklaert():
    """Genau hier verrutschen Medienzitate."""
    z = F["data"]["zaehlweisen"]
    assert "308" in z and "zwei verschiedene Dinge" in z


# --------------------------------------------------------------------------
# Die alten Zahlen sind raus
# --------------------------------------------------------------------------

@pytest.mark.parametrize("falsch", [
    "31 vollendete Femizide",
    "155 vollendete Femizide",
    "85 % Täter",
    "Lettland 1,8",
    "70-90 % der vollendeten Femizide",
    "2024 vorläufig 36",
])
def test_unbelegte_zahlen_sind_weg(falsch):
    assert falsch not in TEXT, falsch


def test_femizid_ist_als_nicht_amtliche_kategorie_gekennzeichnet():
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "kein eigener Straftatbestand" in k or "keine eigene Straf- oder Polizeikategorie" in F["headline"]
    assert "fehlende bundeseinheitliche Definition" in k


def test_zivilgesellschaftliche_zaehlung_ist_als_solche_ausgewiesen():
    k = F["data"]["kernsatz_fuer_synthesizer"]
    assert "Autonome Österreichische Frauenhäuser" in k
    assert "Medienberichten" in k
    assert "nicht deckungsgleich mit der Polizeistatistik" in k


def test_campbell_studie_mit_doi_und_grenze():
    r = F["data"]["risikofaktor_trennung"]
    assert "10.2105/AJPH.93.7.1089" in r
    assert "nicht automatisch" in r, "Übertragbarkeit muss eingeschränkt sein"


# --------------------------------------------------------------------------
# Marker und Trigger
# --------------------------------------------------------------------------

def test_kein_struktureller_falsch_marker():
    """Der Fakt bejaht Claims ('mehr Frauen als Männer bei vollendeten
    Morden') und verneint andere ('Einzelfälle')."""
    assert not has_false_verdict_override(F["data"]["kernsatz_fuer_synthesizer"])


@pytest.mark.parametrize("phrasing", F["claim_phrasings_handled"])
def test_phrasings_treffen(phrasing):
    assert trifft(F, phrasing.lower()), phrasing


@pytest.mark.parametrize("claim", [
    "Wie viele Femizide gab es 2024 in Österreich?",
    "In Deutschland werden immer mehr Frauen von ihren Partnern getötet",
    "Frauenmorde sind Einzelfälle",
])
def test_batterie(claim):
    assert trifft(F, claim.lower()), claim
