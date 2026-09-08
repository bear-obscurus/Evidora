"""Die Zahl von damals stand im Datensatz — sie kam nur nie an.

QA50F-Befund 4: „Die Demokratie in Polen hat sich seit 2023 verbessert" →
`unverifiable@0.15`, mit einer korrekten und entlarvenden Begründung:

    „Die V-Dem-Daten für Polen zeigen Werte für 2025, aber keine direkten
     Vergleichsdaten zu 2023."

In `data/vdem_indicators.json` steht für Polen die volle Reihe (2023: 0.457,
2024: 0.613, 2025: 0.645). Der Konnektor rendert seit jeher nur das neueste
Jahr.

KORREKTUR zu meiner ersten Begründung: den Ungarn-Fall („sank von 0,78
(2010) auf 0,32 (2025)") habe ich für eine erfundene Zahl gehalten, weil
`vdem_indicators.json` erst 2019 beginnt. Falsch — `data/demokratie_pack.json`
enthält „HU 2010 0.78 -> 2023 0.32" aus dem V-Dem Democracy Report 2024. Das
Modell hat eine legitime zweite Quelle zitiert. Der Hinweis auf ein fehlendes
Bezugsjahr bleibt trotzdem richtig, sagt aber bewusst nur etwas über DIESE
Reihe.
"""

import asyncio
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._zeitreihe import (  # noqa: E402
    MAX_JAHRE_IN_REIHE, bezugsjahr, verlangt_verlauf, verlauf_text,
)
from services.vdem import search_vdem  # noqa: E402


def _vdem(claim):
    return asyncio.run(search_vdem({"claim": claim})).get("results") or []


def _namen(claim):
    return [r["indicator_name"] for r in _vdem(claim)]


# --------------------------------------------------------------------------
# Wann eine Reihe verlangt wird
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim", [
    "Die Demokratie in Polen hat sich seit 2023 verbessert",
    "Die Pressefreiheit ist gestiegen",
    "Der Rechtsstaat in Ungarn ist seither verfallen",
    "Wie ist der Trend bei der Wahldemokratie",
    "Die Werte sind rückläufig",
    "Die Demokratie in Polen im Jahr 2021",
])
def test_richtungs_und_jahres_claims_verlangen_die_reihe(claim):
    assert verlangt_verlauf(claim)


@pytest.mark.parametrize("claim", [
    "Polen ist eine Demokratie",
    "Ungarn hat freie Wahlen",
    "",
])
def test_zustands_claims_verlangen_keine_reihe(claim):
    assert not verlangt_verlauf(claim)


def test_ein_jahr_allein_reicht_als_ausloeser():
    """Ohne Richtungswort liefert der Konnektor sonst 2025, während der
    Claim nach 2021 fragt — und das fällt niemandem auf."""
    assert verlangt_verlauf("Die Demokratie in Polen 2021")


def test_jahresmuster_faengt_keine_beliebigen_zahlen():
    """Vierstellig, an Wortgrenzen — sonst sind Beträge und Hausnummern
    plötzlich Jahre."""
    assert not verlangt_verlauf("Der Betrag von 12500 Euro")
    assert not verlangt_verlauf("Das Gesetz mit der Nummer 20255")


def test_bezugsjahr_ist_das_frueheste():
    """„zwischen 2019 und 2023" fragt ab 2019; das spätere Jahr steht
    ohnehin in der Reihe."""
    assert bezugsjahr("zwischen 2019 und 2023") == "2019"
    assert bezugsjahr("seit 2023 verbessert") == "2023"
    assert bezugsjahr("ohne Jahr") is None


# --------------------------------------------------------------------------
# Was die Reihe sagt
# --------------------------------------------------------------------------

def test_reihe_nennt_endpunkte_und_differenz():
    t = verlauf_text({"2023": 0.457, "2024": 0.613, "2025": 0.645}, bezug="2023")
    assert "2023: 0.46" in t and "2025: 0.65" in t
    assert "+0.19" in t, t


def test_fehlendes_bezugsjahr_wird_ausgesprochen():
    """Still das früheste Jahr einsetzen hiesse, eine andere Frage zu
    beantworten als die gestellte."""
    t = verlauf_text({"2019": 0.36, "2025": 0.32}, bezug="2010")
    assert "2010 nicht in dieser Reihe" in t
    assert "sie beginnt 2019" in t


def test_hinweis_spricht_nur_ueber_diese_reihe():
    """Er darf nicht klingen wie „das Jahr gibt es bei uns nicht": eine
    andere Quelle kann es sehr wohl haben — `demokratie_pack.json` hat für
    Ungarn 2010 einen V-Dem-Wert. Ein Hinweis, der das bestreitet, würde
    einen gültigen Beleg entwerten."""
    t = verlauf_text({"2019": 0.36, "2025": 0.32}, bezug="2010")
    for zu_breit in ("unseren Daten", "nicht verfuegbar", "gibt es nicht",
                     "keine Daten"):
        assert zu_breit not in t, f"zu breite Aussage: {zu_breit} — {t}"


def test_ein_einzelner_wert_ergibt_keine_reihe():
    assert verlauf_text({"2025": 0.65}, bezug="2023") == ""
    assert verlauf_text({}, bezug=None) == ""


def test_reihe_ist_gedeckelt():
    lang = {str(j): 0.5 for j in range(1990, 2026)}
    t = verlauf_text(lang, bezug=None)
    assert t.count("/") == MAX_JAHRE_IN_REIHE - 1, t


def test_kaputte_jahre_werden_uebersprungen():
    t = verlauf_text({"2019": 0.4, "keins": 0.9, "2025": 0.6}, bezug=None)
    assert "keins" not in t and "2019" in t and "2025" in t


def test_reihe_bewertet_nicht():
    """Politik-Guardrail: wir zitieren Werte, die Bewertung bleibt bei der
    Quelle. „Differenz +0.19", nicht „deutliche Verbesserung"."""
    t = verlauf_text({"2023": 0.457, "2025": 0.645}, bezug="2023")
    for wort in ("verbesser", "verschlechter", "besser", "schlechter",
                 "gut", "schlecht", "deutlich"):
        assert wort not in t.lower(), f"Wertung im Text: {wort} — {t}"


# --------------------------------------------------------------------------
# Der Konnektor
# --------------------------------------------------------------------------

def test_polen_bekommt_die_reihe_in_den_indicator_name():
    namen = _namen("Die Demokratie in Polen hat sich seit 2023 verbessert")
    assert namen, "V-Dem liefert nichts"
    erst = namen[0]
    assert "2023: 0.46" in erst and "2025: 0.65" in erst, erst
    assert "+0.19" in erst, erst


def test_ungarn_2010_bekommt_den_hinweis_statt_einer_zahl():
    namen = _namen("Die Demokratie in Ungarn hat sich seit 2010 verschlechtert")
    assert namen
    assert "2010 nicht in dieser Reihe" in namen[0], namen[0]


def test_zustands_claim_behaelt_die_alte_form():
    """Regression: ohne Zeitbezug bleibt der knappe Einzelwert."""
    namen = _namen("Polen ist eine Demokratie")
    assert namen
    assert "Reihe" not in namen[0] and "Differenz" not in namen[0], namen[0]
    assert "PL 2025:" in namen[0], namen[0]


def test_indicator_name_bleibt_unter_der_prompt_kappung():
    """Der indicator_name geht bis 400 Zeichen ungekürzt in den Prompt
    (`synthesizer.MAX_STR`). Darüber schneidet die Kappung die Reihe ab —
    und der abgeschnittene Rest sähe aus wie eine vollständige Angabe."""
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "MAX_STR = 400" in quelle, "Kappungs-Grenze hat sich geaendert"
    for claim in ("Die Demokratie in Polen hat sich seit 2023 verbessert",
                  "Die Demokratie in Ungarn hat sich seit 2010 verschlechtert",
                  "Wie war der Trend der Wahldemokratie in Deutschland"):
        for n in _namen(claim):
            assert len(n) <= 400, f"{len(n)} Zeichen: {n}"


def test_skalen_richtung_bleibt_erhalten():
    """#165 hängt die Skalen-Richtung an denselben Text. Beides muss
    nebeneinander stehen — eine Reihe ohne Richtung ist wieder invertierbar."""
    namen = _namen("Die Demokratie in Polen hat sich seit 2023 verbessert")
    assert "HOEHERER Wert = staerkere Demokratie" in namen[0], namen[0]
