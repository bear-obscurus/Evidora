"""Die WSAVA-Welpen-Leitlinie, die es nicht gibt (12.10.2026).

Aus dem Rückstand toter Belege (#244): `welpe_schlafbedarf_konsens` in
`tierhaltung_pack.json` berief sich auf

    https://wsava.org/global-guidelines/puppy-development-guidelines/

Drei unabhängige Prüfungen, alle mit demselben Ergebnis:

  1. **WSAVAs eigene Leitlinien-Übersicht** (`/global-guidelines/`,
     direkt gelesen) führt 12 Leitlinien — Animal Welfare, Dental,
     Gastrointestinal, Hereditary Disease, Liver, Microchip, Nutrition,
     Pain, Professional Wellness, Renal, Reproduction Control,
     Vaccination. **Keine zu Welpen.**
  2. Die URL antwortet mit Bot- UND Browser-User-Agent 404.
  3. Eine erweiterte Websuche findet kein Dokument dieses Namens;
     Welpen-Themen behandelt die WSAVA in den Impf-, Ernährungs- und
     Owner/Breeder-Leitlinien.

Das Archiv (Wayback-CDX) konnte nichts beitragen: es antwortete auf JEDE
Abfrage mit HTTP 503, auch auf die Kontrolle mit einer existierenden
Leitlinie. Eine Null von einem Instrument, das auch bei Bekanntem nichts
findet, ist kein Befund — deshalb steht sie hier nicht als einer.

⚠️ Das Gewicht lag nicht nur im Link: „WSAVA Puppy Guidelines" stand in
der `headline`, im `source_label`, im `kernsatz_fuer_synthesizer` (dem
Feld, das das Modell liest), in `praktische_empfehlung` und als eigener
Datenfeld-NAME `wsava_puppy_guidelines`. Sechs Stellen — dieselbe
Streuung wie bei den erfundenen Aktenzeichen in #235.

Beim Durchsehen desselben Fakts mitgeprüft:
  * **Roffwarg 1966, Science** — existiert (PMID 17779492, „Ontogenetic
    development of the human sleep-dream cycle"), bleibt, jetzt mit PMID.
  * **„Dumoulin 2017 Sci Rep Hund-spezifisch"** — nicht auflösbar, weder
    über Autor noch über Thema. Entfernt statt geraten.

Die AUSSAGE selbst bleibt: 18-20 Stunden sind beim American Kennel Club
belegt (Seite live geprüft, Zahl im Text). Als gemessene Vergleichsgröße
steht jetzt eine begutachtete Kohorte daneben (Kinsman et al. 2020,
Animals, 2.332 Welpen mit 16 Wochen, Halter-Angaben) — Orientierungswert
und Messung sind damit unterscheidbar.
"""

import json
from pathlib import Path

import pytest

DATA = Path(__file__).resolve().parents[1] / "data"
TEXT = (DATA / "tierhaltung_pack.json").read_text(encoding="utf-8")
FAKT = next(f for f in json.loads(TEXT)["facts"]
            if f.get("topic") == "welpe_schlafbedarf_konsens")
ALLES = json.dumps(FAKT, ensure_ascii=False)


def test_die_erfundene_leitlinie_wird_nicht_mehr_als_quelle_gefuehrt():
    """Der Name darf nur noch dort stehen, wo die Korrektur ihn WIDERLEGT.

    Dieselbe Unterscheidung wie in #235: eine Korrektur muss benennen, was
    falsch war — sonst liest niemand sie als Korrektur. Deshalb nicht „das
    Wort kommt nicht vor", sondern „es kommt nur in Feldern vor, die das
    Korrekturdatum tragen".
    """
    assert "wsava.org" not in ALLES, "die tote URL muss weg sein"
    traeger = {k for k, v in FAKT["data"].items()
               if "KORREKTUR 2026-10-12" in str(v)}
    assert traeger, "ohne Korrekturdatum waere der Name nur stehengeblieben"
    for feld, wert in FAKT["data"].items():
        if feld in traeger:
            continue
        assert "WSAVA" not in str(wert), feld
    for feld in ("headline", "source_label", "source_url", "secondary_url"):
        assert "WSAVA" not in str(FAKT[feld]), feld


def test_der_datenfeld_name_behauptet_sie_auch_nicht_mehr():
    """Der Feldname selbst war eine Quellenangabe — und wird mit in den
    Prompt gereicht."""
    assert "wsava_puppy_guidelines" not in FAKT["data"]
    assert "aktivitaets_faustregel" in FAKT["data"]


@pytest.mark.parametrize("feld", ["headline", "source_label"])
def test_die_zuschreibung_ist_aus_den_kopffeldern_raus(feld):
    assert "WSAVA" not in FAKT[feld]


def test_die_korrektur_steht_im_kernsatz():
    """#235: eine Korrektur, die nur in den Detailfeldern steht, erreicht
    das Modell nicht."""
    k = FAKT["data"]["kernsatz_fuer_synthesizer"]
    assert "KORREKTUR 2026-10-12" in k
    assert "WSAVA" in k, "die Korrektur muss benennen, was falsch war"
    assert "keine Welpen-Leitlinie" in k


def test_die_faustregel_nennt_sich_nicht_mehr_leitlinie():
    f = FAKT["data"]["aktivitaets_faustregel"]
    assert "KEINE WSAVA-Leitlinie" in f
    assert "Faustregel" in f
    assert "Eine veröffentlichende Stelle ist nicht belegt" in f


def test_die_aussage_haengt_jetzt_an_einer_erreichbaren_quelle():
    assert FAKT["source_url"] == (
        "https://www.akc.org/expert-advice/health/how-much-do-puppies-sleep/")
    assert "18-20 Stunden" in FAKT["data"]["kernsatz_fuer_synthesizer"]


def test_die_gemessene_vergleichsgroesse_steht_daneben():
    """Orientierungswert (AKC) und Messung (Kohorte) sind zweierlei — der
    Fakt soll beides unterscheidbar führen."""
    assert FAKT["secondary_url"] == "https://pubmed.ncbi.nlm.nih.gov/32664232/"
    assert "Kinsman" in FAKT["source_label"]
    assert "2.332" in FAKT["source_label"]


def test_das_nicht_aufloesbare_zitat_ist_weg_das_echte_praezisiert():
    assert "Dumoulin" not in ALLES
    assert "PMID 17779492" in FAKT["data"]["schlaf_neurologische_entwicklung"]


def test_keine_behauptung_ueber_hunde_die_nur_fuer_nager_belegt_ist():
    """Roffwarg misst den REM-Anteil bei Säugetier-Babys, nicht die Folgen
    von Schlafentzug bei Hunden. Der Fakt sagt das jetzt."""
    s = FAKT["data"]["schlaf_neurologische_entwicklung"]
    assert "für Hunde nicht eigens belegt" in s


@pytest.mark.parametrize("feld,wert", list(FAKT["data"].items()))
def test_datenfelder_bleiben_unter_der_laengengrenze(feld, wert):
    if feld == "kernsatz_fuer_synthesizer":
        pytest.skip("Kernsatz ist von der 400-Zeichen-Grenze ausgenommen")
    assert len(str(wert)) <= 400, f"{feld}: {len(str(wert))} Zeichen"
