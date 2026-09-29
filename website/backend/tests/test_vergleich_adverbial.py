"""Der Vergleich, bei dem der Gegenstand VOR dem Komparativ steht.

Anlass (29.9.2026, Kontrollmessung nach #233). Live:

    Claim:   "Weibliche Beschäftigte verdienen weniger als männliche"
    Label:   mostly_false @ 0.7
    Summary: "Laut OECD beträgt der Gender Pay Gap 2024 durchschnittlich
              10,07 %. Dies BESTÄTIGT, dass weibliche Beschäftigte im
              Schnitt weniger verdienen als männliche."

Das Label widerspricht der eigenen Begründung — genau die Klasse, für die
Muster O da ist. Es schwieg, weil #227 nur die ATTRIBUTIVE Stellung kannte:

    attributiv   "mehr MÄNNER als Frauen"
                 -> der Gegenstand steht zwischen Komparativ und "als"
    adverbial    "weibliche Beschäftigte VERDIENEN weniger als …"
                 -> dort steht nichts, der Gegenstand steht davor

Bei der adverbialen Form lieferte ``_o_seiten`` eine leere vordere Seite,
``vergleich_aus_claim`` gab ``None`` zurück, und O sah den Claim nie.

Im 3.730-Claim-Korpus stehen **43 attributive gegen 25 adverbiale** Formen.

## Zwei Dinge, die der adverbiale Zweig hereinlässt und die draußen bleiben

SCHWELLEN-CLAIMS. „Wer weniger als 8 Gläser trinkt", „Mehr als 2 Eier pro
Woche schadet" laufen strukturell durch denselben Zweig, sind aber keine
Vergleiche zweier Gegenstände, sondern Schwellen — die gehören Muster N,
das vor O läuft. Steht hinter „als" eine Zahl, gibt die Zerlegung ``None``
zurück.

FÜRWÖRTER. Ohne die Schwellen-Regel zerlegte „Wer weniger als 8 Gläser
trinkt" zu Subjekt ``{wer}`` — O hätte auf ein Fürwort hin entschieden.
Ein Vergleich braucht zwei benannte Seiten; besteht die vordere nur aus
Fürwörtern, schweigt die Zerlegung.

## Die Grenze: nur die BESTÄTIGENDE Richtung

Für die adverbiale Form ist die Umkehr-Richtung mit dieser Methode nicht
unterscheidbar. In einer gedrehten Summary („männliche Beschäftigte
verdienen weniger als weibliche") stehen Verb und Kopfnomen auf BEIDEN
Seiten, also treffen Bestätigung UND Drehung — und die Doppeltreffer-Wache
aus #227 hält die Kaskade still. Das ist richtig so: lieber keine
Entscheidung als eine geratene.

Zwei meiner acht Richtungs-Erwartungen waren deshalb falsch gesetzt; sie
stehen unten als Grenze, nicht als Fehler.

Über-Trigger-Sweep über 210 echte Live-Läufe (QA50F, QA50G, HART40,
HART40-B, HART30-C): **0 Label-Unterschiede**. Die Sonde wurde am Auslöser
geprüft (mostly_false -> true), damit ihre Null etwas beweist.

Keine Netzabfrage, kein Modell.
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._schreibweise import normalisiere  # noqa: E402
from services.verdict_postprocess import (  # noqa: E402
    _O_PRONOMEN,
    _o_komparativ,
    _o_seiten,
    apply_verdict_postprocessing,
    summary_bestaetigt_vergleich,
    summary_dreht_vergleich,
    vergleich_aus_claim,
    vergleich_negiert,
)

CLAIM = "Weibliche Beschäftigte verdienen weniger als männliche"
BESTAETIGT = (
    "Laut OECD beträgt der Gender Pay Gap 2024 durchschnittlich 10,07 %. "
    "Dies bestätigt, dass weibliche Beschäftigte im Schnitt weniger "
    "verdienen als männliche."
)
GEDREHT = (
    "Laut der Auswertung verdienen männliche Beschäftigte im Schnitt "
    "weniger als weibliche. Der Abstand beträgt 3 Prozentpunkte."
)
EV = [{"source": "OECD", "url": "https://example.test/a"},
      {"source": "Eurostat", "url": "https://example.test/b"}]


def _lauf(verdict, claim, summary):
    return apply_verdict_postprocessing(
        {"verdict": verdict, "confidence": 0.85, "summary": summary,
         "evidence": list(EV)}, [], claim)


def _seiten(text):
    n = normalisiere(text.lower())
    k, m = _o_komparativ(n)
    return _o_seiten(n, k, m.start())


# ---------------------------------------------------------------------------
# 1. Die beiden Stellungen
# ---------------------------------------------------------------------------

def test_attributiv_gegenstand_steht_zwischen_komparativ_und_als():
    vorn, hinten = _seiten("In Österreich wurden 2024 mehr Männer als Frauen ermordet")
    assert "maenner" in vorn
    assert "frauen" in hinten


def test_adverbial_gegenstand_steht_davor():
    vorn, hinten = _seiten(CLAIM)
    assert "weibliche" in vorn
    assert "maennliche" in hinten


def test_er_als_bleibt_unveraendert():
    vorn, hinten = _seiten("Die ÖBB sind pünktlicher als die Deutsche Bahn")
    assert "oebb" in vorn
    assert "deutsche" in hinten


@pytest.mark.parametrize("claim,erwartet", [
    (CLAIM, ({"weibliche", "beschaeftigte", "verdienen"}, "weniger", {"maennliche"})),
    ("Frauen verdienen 18 Prozent weniger als Männer",
     ({"frauen", "verdienen", "prozent"}, "weniger", {"maenner"})),
    ("In Österreich wurden 2024 mehr Männer als Frauen ermordet",
     ({"maenner"}, "mehr", {"frauen", "ermordet"})),
])
def test_zerlegung(claim, erwartet):
    assert vergleich_aus_claim(claim.lower()) == erwartet


# ---------------------------------------------------------------------------
# 2. Was der adverbiale Zweig NICHT hereinlassen darf
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("claim", [
    "Wer weniger als 8 Gläser trinkt dehydriert",
    "Wer weniger als 8 Stunden schläft schadet seinem Körper",
    "Mehr als 2 Eier pro Woche schadet",
    "Mehr als 3 Prozent der Proben überschreiten den Grenzwert",
])
def test_schwellen_claims_gehoeren_muster_n(claim):
    """Hinter "als" steht eine Zahl — das ist eine Schwelle, kein Vergleich
    zweier Gegenstaende."""
    assert vergleich_aus_claim(claim.lower()) is None


def test_fuerwort_allein_ist_keine_seite():
    assert "wer" in _O_PRONOMEN
    assert vergleich_aus_claim("wer mehr als andere arbeitet verdient mehr") is None


def test_ohne_die_schwellen_regel_zerlegte_es_auf_ein_fuerwort():
    """Selbstkontrolle der Begruendung: Die Zerlegung scheitert an der Zahl,
    und ohne sie stuende dort tatsaechlich nur ein Fuerwort."""
    vorn, hinten = _seiten("Wer weniger als 8 Gläser trinkt dehydriert")
    assert vorn.strip() == "wer"
    assert hinten.strip().startswith("8")


# ---------------------------------------------------------------------------
# 3. Der gemessene Fall
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("eingang", ["mostly_false", "false"])
def test_ausloeser_wird_korrigiert(eingang):
    assert _lauf(eingang, CLAIM, BESTAETIGT)["verdict"] == "true"


def test_summary_bleibt_unveraendert():
    assert _lauf("mostly_false", CLAIM, BESTAETIGT)["summary"] == BESTAETIGT


def test_verneinter_claim_dreht_mit():
    verneint = "Weibliche Beschäftigte verdienen nicht weniger als männliche"
    assert vergleich_negiert(verneint.lower())
    assert _lauf("true", verneint, BESTAETIGT)["verdict"] == "false"


def test_die_verneinung_landet_nicht_im_subjekt():
    """Bei der adverbialen Form ist die vordere Seite der halbe Satz — das
    "nicht" steht mitten darin und darf kein Operand werden."""
    subjekt, _k, _p = vergleich_aus_claim(
        "Weibliche Beschäftigte verdienen nicht weniger als männliche".lower())
    assert "nicht" not in subjekt


# ---------------------------------------------------------------------------
# 4. Die Grenze, bewusst offen
# ---------------------------------------------------------------------------

def test_gedrehte_adverbiale_summary_bleibt_unentschieden():
    """Verb und Kopfnomen stehen in der gedrehten Fassung auf BEIDEN Seiten,
    also treffen Bestaetigung UND Drehung — die Doppeltreffer-Wache aus #227
    haelt die Kaskade still. Lieber keine Entscheidung als eine geratene.

    Wer die Richtung unterscheidbar macht, dreht diese Zeile um."""
    assert summary_bestaetigt_vergleich(CLAIM.lower(), GEDREHT)
    assert summary_dreht_vergleich(CLAIM.lower(), GEDREHT)
    for eingang in ("true", "false", "mostly_true", "mostly_false"):
        assert _lauf(eingang, CLAIM, GEDREHT)["verdict"] == eingang


def test_bei_der_attributiven_form_traegt_die_drehung_weiter():
    """Gegenprobe: Dort ist die Umkehr sehr wohl unterscheidbar — die
    Grenze oben gilt nur fuer die adverbiale Stellung."""
    mord = ("In Österreich gab es 2024 bei vollendeten Morden 40 weibliche "
            "und 36 männliche Opfer – also mehr Frauen als Männer.")
    claim = "In Österreich wurden 2024 mehr Männer als Frauen ermordet"
    assert not summary_bestaetigt_vergleich(claim.lower(), mord)
    assert summary_dreht_vergleich(claim.lower(), mord)
    assert _lauf("true", claim, mord)["verdict"] == "false"


# ---------------------------------------------------------------------------
# 5. Die Kontrollen aus #217/#223/#224 halten
# ---------------------------------------------------------------------------

OEBB = (
    "Die ÖBB-Fernverkehr-Pünktlichkeit lag 2024 bei 78,2–88,7 %, während die "
    "Deutsche Bahn nur 62,5 % erreichte. Selbst bei strengerer Definition ist "
    "die ÖBB deutlich pünktlicher als die DB."
)


@pytest.mark.parametrize("claim,eingang,soll", [
    ("Die ÖBB sind pünktlicher als die Deutsche Bahn", "mostly_false", "true"),
    ("Die Deutsche Bahn ist pünktlicher als die ÖBB", "true", "false"),
    ("Die ÖBB sind nicht pünktlicher als die Deutsche Bahn", "true", "false"),
    ("Die Deutsche Bahn ist nicht pünktlicher als die ÖBB", "false", "true"),
])
def test_oebb_kontrollen(claim, eingang, soll):
    assert _lauf(eingang, claim, OEBB)["verdict"] == soll
