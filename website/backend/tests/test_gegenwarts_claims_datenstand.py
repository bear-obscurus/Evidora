"""„heute 51 Prozent" ist eine eigene Tatsachenbehauptung.

QA50E-Befund 3: „Laut Eurobarometer vertrauen **heute** 51 Prozent der EU"
bekam `true@0.9`. Die Zahl stimmt — aus dem Standard Eurobarometer 102 vom
**Herbst 2024**. Heute ist September 2026.

**Das System wusste es und sagte es sogar.** Die Nuance im Live-Lauf lautete
wörtlich: „Die Behauptung bezieht sich auf den Stand Herbst 2024. Aktuellere
Daten (2025/2026) liegen nicht vor." Nur das Verdict sagte es nicht. Es ist
also kein Retrieval-Problem — der Datenstand kam an — sondern eine Lücke
zwischen dem, was das Modell weiss, und dem, was es entscheidet.

Ein Gegenwarts-Wort behauptet zweierlei: dass die Zahl stimmt UND dass sie
jetzt gilt. Ein zwei Jahre alter Messwert belegt nur das Erste.

GEMESSEN, BEVOR GEBAUT
======================
Sechs echte Läufe gegen die Live-Instanz am 2026-09-07, **zwei davon falsch**:

    Eurobarometer 51 % (Herbst 2024)   true@0.9    <- Befund
    CPI Österreich 71 Punkte (2024)    true@0.95   <- derselbe Fehler
    IDEA Dänemark 84 % (Wahl 2022)     unverifiable@0.15  (korrekt erkannt)
    EZB-Leitzins 2,40 % (2026)         true@0.88   (korrekt)
    Freedom House Ungarn 65 (FIW 2026) true@0.9    (korrekt)
    Kopftuchverbot (Gesetz, kein Wert) true@0.95   (korrekt)

ZWEI ENTWÜRFE, DIE DIE MESSUNG VERWORFEN HAT
============================================
1. **Jüngstes Jahr über alle Quellen.** Wirkungslos: `raw_sources` enthält
   immer Wikipedia und GDELT, und die tragen das laufende Jahr. Über alle
   sechs Läufe war das Maximum ausnahmslos 2026 — die Regel hätte nie
   gegriffen.

2. **Jüngstes Jahr aus Summary UND Nuance.** Auch wirkungslos, und zwar aus
   einem hinterhältigen Grund: die Nuance zum Eurobarometer-Fall sagte
   „Aktuellere Daten (**2025/2026**) liegen nicht vor". Dort steht 2026 als
   VERNEINUNG. Wer beide Felder zusammenwirft, liest daraus einen frischen
   Datenstand.

Es zählt deshalb nur die **Summary** — dort steht der Beleg, nicht seine
Abwesenheit.

WARUM EIN MESSWERT VERLANGT WIRD
================================
„Heute gilt an Österreichs Volksschulen ein Kopftuchverbot" ist ein
fortdauernder Rechtszustand, kein Messpunkt: ein Gesetz gilt heute weiter.
Ohne diese Bedingung würde der Guard wahre Claims abwerten — auch das ist
gemessen, nicht vermutet.
"""

import datetime as dt
import re
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import apply_verdict_postprocessing as anwenden  # noqa: E402

# Wortlaut aus den echten Läufen vom 2026-09-07.
S_EUROBAROMETER = ("Laut Standard Eurobarometer 102 (Herbst 2024) vertrauen "
                   "51 % der EU-Bürger:innen der EU 'eher' – der höchste Wert "
                   "seit 2007. Die Stichprobe umfasste ~26.500 Befragte.")
S_CPI = ("Aktuell liegt Österreich im Korruptionswahrnehmungsindex (CPI) von "
         "Transparency International bei 71 von 100 Punkten (2024).")
S_EZB = ("Der EZB-Leitzins (Hauptrefinanzierungssatz) liegt aktuell (Stand "
         "2026-09-06) bei 2,40 %. Belegt durch EZB-Daten (2025-04-23 und "
         "2026-06-17).")
S_FIW = ("Freedom House bewertet Ungarn im Freedom in the World 2026 (für das "
         "Jahr 2025) mit 65 von 100 Punkten und stuft das Land als 'Partly "
         "Free' ein.")
S_KOPFTUCH = ("Seit 1. September 2026 gilt in Österreich ein Kopftuchverbot "
              "für Schülerinnen bis zum 14. Geburtstag.")


def _lauf(claim, verdict="true", confidence=0.9, summary="", nuance=None):
    ergebnis = {"verdict": verdict, "confidence": confidence, "summary": summary}
    if nuance is not None:
        ergebnis["nuance"] = nuance
    return anwenden(ergebnis, [], claim)


# --------------------------------------------------------------------------
# Der Befund
# --------------------------------------------------------------------------

def test_eurobarometer_der_ausloesende_fall():
    r = _lauf("Laut Eurobarometer vertrauen heute 51 Prozent der EU",
              summary=S_EUROBAROMETER)
    assert r["verdict"] == "mostly_true"
    assert r["confidence"] <= 0.75
    assert "2024" in r["nuance"] and "ZEITBEZUG" in r["nuance"]


def test_cpi_derselbe_fehler():
    r = _lauf("Aktuell liegt Österreich im Korruptionsindex bei 71 Punkten",
              confidence=0.95, summary=S_CPI)
    assert r["verdict"] == "mostly_true"


@pytest.mark.parametrize("wort", [
    "heute", "aktuell", "derzeit", "momentan", "zurzeit", "gegenwärtig",
    "inzwischen", "mittlerweile", "jetzt",
])
def test_alle_gegenwarts_woerter_greifen(wort):
    r = _lauf(f"Laut Eurobarometer vertrauen {wort} 51 Prozent der EU",
              summary=S_EUROBAROMETER)
    assert r["verdict"] == "mostly_true", wort


def test_die_zahl_wird_nicht_als_falsch_erklaert():
    """Die Zahl IST korrekt — nur ihr Zeitbezug nicht. „false" wäre genauso
    verkehrt wie „true", und „mixed" würde den Beleg entwerten."""
    r = _lauf("Laut Eurobarometer vertrauen heute 51 Prozent der EU",
              summary=S_EUROBAROMETER)
    assert r["verdict"] == "mostly_true"
    assert r["verdict"] not in ("false", "mostly_false", "mixed", "unverifiable")


def test_bestehende_nuance_bleibt_erhalten():
    """Der Hinweis wird angehängt, nicht ersetzt — sonst geht die Begründung
    des Modells verloren."""
    r = _lauf("Laut Eurobarometer vertrauen heute 51 Prozent der EU",
              summary=S_EUROBAROMETER,
              nuance="Die Erhebung erfolgte in 27 Mitgliedstaaten.")
    assert "27 Mitgliedstaaten" in r["nuance"]
    assert "ZEITBEZUG" in r["nuance"]


# --------------------------------------------------------------------------
# Die Gegenprobe — was NICHT abgewertet werden darf
# --------------------------------------------------------------------------

def test_frische_daten_bleiben_true():
    r = _lauf("Der EZB-Leitzins liegt aktuell bei 2,40 Prozent",
              confidence=0.88, summary=S_EZB)
    assert r["verdict"] == "true" and r["confidence"] == 0.88


def test_aktuelle_ausgabe_bleibt_true():
    r = _lauf("Derzeit hat Ungarn 65 von 100 Punkten bei Freedom House",
              summary=S_FIW)
    assert r["verdict"] == "true"


def test_fortdauernder_rechtszustand_bleibt_true():
    """Ein Gesetz ist kein Messpunkt. Ohne die Messwert-Bedingung würde der
    Guard hier einen wahren Claim abwerten."""
    r = _lauf("Heute gilt an Österreichs Volksschulen ein Kopftuchverbot",
              confidence=0.95, summary=S_KOPFTUCH)
    assert r["verdict"] == "true"


def test_ohne_gegenwarts_wort_keine_abwertung():
    """„2024 vertrauten 51 %" ist schlicht wahr — der Claim behauptet gar
    nichts über heute."""
    r = _lauf("Laut Eurobarometer vertrauten 2024 51 Prozent der EU",
              summary=S_EUROBAROMETER)
    assert r["verdict"] == "true"


def test_nur_true_wird_abgewertet():
    """Der Guard schiebt nicht an anderen Verdicts herum — ein „mixed" hat
    seine eigene Begründung."""
    for v in ("mostly_true", "mixed", "mostly_false", "false", "unverifiable"):
        r = _lauf("Laut Eurobarometer vertrauen heute 51 Prozent der EU",
                  verdict=v, summary=S_EUROBAROMETER)
        assert r["verdict"] == v, v


def test_ein_jahr_alt_reicht_nicht():
    """Jahresindizes erscheinen mit einem Jahr Verzug. „Aktuell" auf den
    zuletzt veröffentlichten Jahrgang ist fair — sonst schlägt der Guard bei
    jedem Jahresindex an und wird abgeschaltet (#128)."""
    vorjahr = dt.date.today().year - 1
    r = _lauf("Aktuell liegt Österreich im Korruptionsindex bei 71 Punkten",
              summary=f"Österreich erreicht {vorjahr} 71 von 100 Punkten.")
    assert r["verdict"] == "true"


def test_summary_ohne_jahreszahl_greift_nicht():
    """Ohne Datenstand keine Aussage über den Datenstand — raten wäre
    schlimmer als nichts tun."""
    r = _lauf("Laut Eurobarometer vertrauen heute 51 Prozent der EU",
              summary="Eine knappe Mehrheit der Befragten vertraut der EU.")
    assert r["verdict"] == "true"


def test_zukunftsjahr_in_der_summary_verfaelscht_nicht():
    """Eine Prognose für 2027 ist kein Beleg für heute — Jahre nach dem
    laufenden werden ignoriert, sonst maskiert eine Vorausschau den alten
    Messwert."""
    kommend = dt.date.today().year + 1
    r = _lauf("Laut Eurobarometer vertrauen heute 51 Prozent der EU",
              summary=f"Eurobarometer 102 (Herbst 2024): 51 %. "
                      f"Für {kommend} wird ein Anstieg erwartet.")
    assert r["verdict"] == "mostly_true"


# --------------------------------------------------------------------------
# Die beiden verworfenen Entwürfe — als Regressionsschutz
# --------------------------------------------------------------------------

def test_nuance_wird_nicht_als_datenstand_gelesen():
    """Entwurf 2 scheiterte hieran: „Aktuellere Daten (2025/2026) liegen nicht
    vor" nennt 2026 als VERNEINUNG. Wer die Nuance mitliest, hält den Fall für
    frisch und greift nie."""
    r = _lauf("Laut Eurobarometer vertrauen heute 51 Prozent der EU",
              summary=S_EUROBAROMETER,
              nuance="Aktuellere Daten (2025/2026) liegen nicht vor.")
    assert r["verdict"] == "mostly_true", (
        "Die Nuance darf den Datenstand nicht überschreiben")


def test_quellen_liste_wird_nicht_als_datenstand_gelesen():
    """Entwurf 1 scheiterte hieran: `raw_sources` enthält immer Wikipedia und
    GDELT mit dem laufenden Jahr. Über sechs echte Läufe war das Maximum
    ausnahmslos das aktuelle Jahr."""
    quellen = [{"source": "Wikipedia", "results": [
        {"display_value": f"Stand {dt.date.today().year}: Artikel aktualisiert"}]}]
    ergebnis = anwenden({"verdict": "true", "confidence": 0.9,
                         "summary": S_EUROBAROMETER}, quellen,
                        "Laut Eurobarometer vertrauen heute 51 Prozent der EU")
    assert ergebnis["verdict"] == "mostly_true"


# --------------------------------------------------------------------------
# Die Prompt-Schicht
# --------------------------------------------------------------------------

def test_prompt_traegt_die_regel_und_die_gegenprobe():
    """Defense-in-Depth: die Prompt-Regel ist die semantische Schicht (das
    Modell weiss, welcher Wert die Zahl trägt), der Guard die deterministische.
    Beide müssen dasselbe sagen, sonst korrigiert der Guard das Modell in eine
    Richtung, die der Prompt gar nicht kennt."""
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    assert "GEGENWARTS-CLAIMS" in quelle
    assert "HÖCHSTENS \"mostly_true\"" in quelle
    assert "GEGENPROBE" in quelle
    assert "Kopftuchverbot" in quelle, "Die Ausnahme muss im Prompt stehen"
