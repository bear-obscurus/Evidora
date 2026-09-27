"""Vergleich in der Form "mehr X als Y" — und drei Fehler, die dabei auffielen.

Anlass (HART30-C Nr. 29, 27.9.2026):

    Claim:   "In Österreich wurden 2024 mehr Männer als Frauen ermordet"
    Label:   true @ 0.85
    Summary: "In Österreich gab es 2024 bei vollendeten Morden 40 weibliche
              und 36 männliche Opfer – also mehr FRAUEN als MÄNNER. Bei Mord
              und Mordversuch zusammen waren 98 Frauen und 187 Männer
              betroffen."

Die Begründung sagt wörtlich das Gegenteil des Labels. Weder Muster O noch
Muster P sahen das: "mehr" endet nicht auf "-er", und der Komparativ-Ausdruck
suchte genau das. Beim Nachbauen fielen drei Dinge auf, die alle einzeln
geprüft werden:

1. DIE SEITEN LIEGEN ANDERS. Bei "-er als" steht der Gegenstand DAVOR ("die
   ÖBB sind pünktlicher als die DB"), bei "mehr/weniger" liegen beide Seiten
   DAHINTER ("mehr Männer als Frauen"). Ohne diese Unterscheidung wäre das
   Subjekt der halbe Satz davor ("in österreich wurden 2024").

2. DIE ZAHLEN-ZUORDNUNG STAND VERKEHRT. Muster P ordnet jede Zahl der
   zuletzt DAVOR genannten Seite zu, weil deutsche Sätze den Träger vor
   seine Zahl stellen. Für "98 Frauen und 187 Männer" gilt das nicht: Dort
   folgt der Träger seiner Zahl. Gemessen ergab die alte Regel

       Männer -> 98,  Frauen -> 187

   also genau verkehrt — der Satz davor endete mit "... als Männer". Steht
   ein Operand UNMITTELBAR hinter der Zahl, gehört sie jetzt ihm. Das
   Fenster ist absichtlich winzig: Ein erster Versuch mit 30 Zeichen ab dem
   ANFANG der Zahl reichte in "78,2–88,7 %, während die Deutsche Bahn" über
   die Satzgrenze und gab der ÖBB-Zahl die Deutsche Bahn als Träger.

3. DER SATZ-SPLITTER WAR BLIND. Die Kaskade gab Muster O die Summary
   kleingeschrieben. ``teile_in_einheiten`` trennt hinter einem Punkt aber
   nur, wenn danach ein Großbuchstabe folgt — sonst wäre jede
   Gliederungszahl ("seit 1. Jänner") eine Satzgrenze. Ergebnis: Die GANZE
   Summary war ein Satz, und alle Satz-Wachen (Hedge, Referat, Verneinung)
   wirkten nur noch global. Nachweisbarer Fehltreffer: "Deutsche Bahn" aus
   Satz 1 galt als Subjekt des Komparativs in Satz 2.

Muster P rechnet bei "mehr/weniger" BEWUSST NICHT. Der gemessene Fall zeigt,
warum: Dieselbe Summary nennt zwei Messgrößen mit gegenteiliger Antwort — 40
zu 36 bei vollendeten Morden, 98 zu 187 bei Mord und Mordversuch. P erwischt
nur die zweite, weil "40 weibliche / 36 männliche" die Operandenwörter nicht
trägt; die Rechnung wäre hier zufällig richtig oder zufällig falsch.
Entschieden wird deshalb literal, über den Satz, den die Summary selbst
schreibt — und "ermordet" meint vollendete Morde, also 40 zu 36.

Über-Trigger-Sweep über 210 echte Live-Läufe (QA50F, QA50G, HART40, HART40-B,
HART30-C): genau EIN Label-Unterschied, der Auslöser. Die Sonde wurde gegen
den Auslöser selbst geprüft, damit ein blindes Instrument nicht als Beweis
durchgeht.

Keine Netzabfrage, kein Modell.
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._satzgrenzen import teile_in_einheiten  # noqa: E402
from services.verdict_postprocess import (  # noqa: E402
    _o_komparativ,
    _o_seiten,
    apply_verdict_postprocessing,
    summary_bestaetigt_vergleich,
    summary_dreht_vergleich,
    vergleich_aus_claim,
    vergleich_negiert,
    vergleich_rechnerisch,
    zahlen_beider_seiten,
)

# Die Original-Summary aus dem Live-Lauf vom 27.9.2026.
MORD = (
    "In Österreich gab es 2024 bei vollendeten Morden 40 weibliche und 36 "
    "männliche Opfer – also mehr Frauen als Männer. Bei Mord und Mordversuch "
    "zusammen waren 98 Frauen und 187 Männer betroffen."
)
MORD_CLAIM = "In Österreich wurden 2024 mehr Männer als Frauen ermordet"

# Der ÖBB-Fall aus HART40 — die Gegenprobe in der anderen Vergleichsform.
OEBB = (
    "Die ÖBB-Fernverkehr-Pünktlichkeit lag 2024 bei 78,2–88,7 %, während die "
    "Deutsche Bahn nur 62,5 % erreichte. Selbst bei strengerer Definition ist "
    "die ÖBB deutlich pünktlicher als die DB."
)

EV = [{"source": "BKA", "url": "https://example.test/a"},
      {"source": "Statistik", "url": "https://example.test/b"}]


def _lauf(verdict, claim, summary, confidence=0.85):
    result = {"verdict": verdict, "confidence": confidence, "summary": summary,
              "evidence": list(EV)}
    return apply_verdict_postprocessing(result, [], claim)


# --------------------------------------------------------------------------
# 1. "mehr" wird als Komparativ erkannt, und die Seiten liegen richtig
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim,erwartet", [
    ("die öbb sind pünktlicher als die deutsche bahn", "puenktlicher"),
    ("in österreich wurden 2024 mehr männer als frauen ermordet", "mehr"),
    ("es gibt weniger wölfe als 2020", "weniger"),
    ("die leerstandsabgabe beträgt 215 euro", None),
])
def test_komparativ_wird_erkannt(claim, erwartet):
    assert _o_komparativ(claim)[0] == erwartet


def test_mehr_claim_zerlegt_die_seiten_richtig():
    """Der Gegenstand steht ZWISCHEN "mehr" und "als", nicht davor."""
    subjekt, komparativ, partner = vergleich_aus_claim(MORD_CLAIM.lower())
    assert komparativ == "mehr"
    assert subjekt == {"maenner"}
    assert "frauen" in partner
    # Der halbe Satz davor gehoert NICHT zum Subjekt.
    assert "oesterreich" not in subjekt and "2024" not in subjekt


def test_er_als_claim_bleibt_unveraendert():
    """Die andere Vergleichsform darf sich durch die Erweiterung nicht ändern."""
    subjekt, komparativ, partner = vergleich_aus_claim(
        "die öbb sind pünktlicher als die deutsche bahn")
    assert komparativ == "puenktlicher"
    assert subjekt == {"oebb"}
    assert partner == {"deutsche", "bahn"}


def test_mehr_ohne_als_ist_kein_vergleich():
    """"Es gibt mehr Wölfe" vergleicht nichts — das Muster muss schweigen."""
    assert vergleich_aus_claim("in österreich gibt es mehr wölfe") is None


@pytest.mark.parametrize("komparativ,text,vorn_soll,hinten_soll", [
    ("mehr", "es gab mehr maenner als frauen", "maenner", "frauen"),
    ("puenktlicher", "die oebb sind puenktlicher als die db", "oebb", "db"),
])
def test_seiten_helfer(komparativ, text, vorn_soll, hinten_soll):
    vorn, hinten = _o_seiten(text, komparativ, text.find(komparativ))
    assert vorn_soll in vorn and vorn_soll not in hinten
    assert hinten_soll in hinten and hinten_soll not in vorn


# --------------------------------------------------------------------------
# 2. Die Zahlen-Zuordnung: Träger hinter der Zahl
# --------------------------------------------------------------------------

def test_nachgestellter_traeger_bekommt_seine_zahl():
    """"98 Frauen und 187 Männer" — vor dem Fix war es genau verkehrt."""
    a, b, _einheit = zahlen_beider_seiten(["maenner"], ["frauen"], MORD.lower())
    assert a == (187.0, 187.0), f"Männer bekamen {a}, erwartet 187"
    assert b == (98.0, 98.0), f"Frauen bekamen {b}, erwartet 98"


def test_vorangestellter_traeger_bleibt_richtig():
    """Die Gegenprobe: Hier steht der Träger VOR seiner Zahl. Ein zu weites
    Vorwärts-Fenster gab 78,2 an die Deutsche Bahn."""
    a, b, einheit = zahlen_beider_seiten(["oebb"], ["deutsche", "bahn"],
                                         OEBB.lower())
    assert a == (78.2, 88.7)
    assert b == (62.5, 62.5)
    assert einheit == "%"


# --------------------------------------------------------------------------
# 3. Der Satz-Splitter braucht die Großschreibung
# --------------------------------------------------------------------------

def test_splitter_braucht_grossschreibung():
    """Der Beweis für die Ursache — kleingeschrieben wird nicht getrennt."""
    assert len(teile_in_einheiten(OEBB)) >= 2
    assert len(teile_in_einheiten(OEBB.lower())) == 1


def test_subjekt_aus_dem_nachbarsatz_zaehlt_nicht():
    """Fehltreffer vor dem Fix: "Deutsche Bahn" steht in Satz 1, der
    Komparativ in Satz 2 — und O las das als Bestätigung."""
    claim = "die deutsche bahn ist pünktlicher als die öbb"
    assert not summary_bestaetigt_vergleich(claim, OEBB)


def test_o_bekommt_die_summary_in_originalschreibung():
    """Regressionsschutz gegen ein zurückgedrehtes .lower() in der Kaskade:
    Kleingeschrieben feuert der Fehltreffer wieder."""
    claim = "die deutsche bahn ist pünktlicher als die öbb"
    assert summary_bestaetigt_vergleich(claim, OEBB.lower()), (
        "Erwartet wird der ALTE Fehltreffer auf kleingeschriebenem Text — "
        "wenn er ausbleibt, prüft dieser Test nichts mehr."
    )


# --------------------------------------------------------------------------
# Die Gegenrichtung selbst
# --------------------------------------------------------------------------

def test_gedrehter_vergleich_wird_erkannt():
    assert summary_dreht_vergleich(MORD_CLAIM.lower(), MORD)


def test_gedreht_und_bestaetigt_schliessen_sich_aus():
    assert not summary_bestaetigt_vergleich(MORD_CLAIM.lower(), MORD)


def test_bestaetigung_gilt_nicht_als_drehung():
    claim = "die öbb sind pünktlicher als die deutsche bahn"
    assert summary_bestaetigt_vergleich(claim, OEBB)
    assert not summary_dreht_vergleich(claim, OEBB)


@pytest.mark.parametrize("eingang", ["true", "mostly_true"])
def test_ausloeser_wird_korrigiert(eingang):
    """"ermordet" meint vollendete Morde: 40 Frauen zu 36 Männern."""
    assert _lauf(eingang, MORD_CLAIM, MORD)["verdict"] == "false"


def test_gegenclaim_wird_bestaetigt():
    claim = "In Österreich wurden 2024 mehr Frauen als Männer ermordet"
    assert _lauf("false", claim, MORD)["verdict"] == "true"


def test_verneinter_claim_dreht_mit():
    """"NICHT mehr Männer als Frauen" — die Drehung bestätigt ihn."""
    claim = "In Österreich wurden 2024 nicht mehr Männer als Frauen ermordet"
    assert vergleich_negiert(claim.lower())
    assert _lauf("false", claim, MORD)["verdict"] == "true"


def test_summary_bleibt_unveraendert():
    assert _lauf("true", MORD_CLAIM, MORD)["summary"] == MORD


# --------------------------------------------------------------------------
# Was NICHT feuern darf
# --------------------------------------------------------------------------

def test_muster_p_rechnet_bei_mehr_nicht():
    """Bewusste Sperre: Die Summary nennt zwei Messgrößen mit gegenteiliger
    Antwort. Eine Rechnung wäre hier zufällig richtig."""
    assert vergleich_rechnerisch(MORD_CLAIM.lower(), MORD.lower()) is None


def test_zwei_gegenteilige_saetze_lassen_das_muster_schweigen():
    """Bejaht ein Satz den Vergleich und dreht ein anderer ihn um, greift
    die Kaskade nicht ein."""
    doppelt = ("Bei vollendeten Morden gab es mehr Frauen als Männer. "
               "Bei Mord und Mordversuch gab es mehr Männer als Frauen.")
    assert summary_bestaetigt_vergleich(MORD_CLAIM.lower(), doppelt)
    assert summary_dreht_vergleich(MORD_CLAIM.lower(), doppelt)
    for eingang in ("true", "false", "mostly_true", "mostly_false"):
        assert _lauf(eingang, MORD_CLAIM, doppelt)["verdict"] == eingang


def test_referat_des_claims_zaehlt_nicht():
    """Ein Satz, der den Claim nur wiedergibt, ist keine Aussage."""
    s = ("Die Behauptung sagt, es seien mehr Frauen als Männer ermordet "
         "worden. Belastbare Zahlen dazu liegen nicht vor.")
    assert not summary_dreht_vergleich(MORD_CLAIM.lower(), s)


def test_konjunktiv_zaehlt_nicht():
    s = "Möglicherweise wurden mehr Frauen als Männer ermordet."
    assert not summary_dreht_vergleich(MORD_CLAIM.lower(), s)


def test_verneinter_summary_satz_zaehlt_nicht():
    s = "Es wurden nicht mehr Frauen als Männer ermordet."
    assert not summary_dreht_vergleich(MORD_CLAIM.lower(), s)


def test_nur_eine_seite_ist_keine_drehung():
    """Steht nur der Partner auf der Gewinnerseite und das Subjekt nirgends,
    ist es ein anderer Satz — kein Widerspruch."""
    s = "Bei vollendeten Morden gab es mehr Frauen als Verletzte."
    assert not summary_dreht_vergleich(MORD_CLAIM.lower(), s)


@pytest.mark.parametrize("eingang", ["unverifiable", "mixed"])
def test_unentschiedene_label_bleiben_unberuehrt(eingang):
    assert _lauf(eingang, MORD_CLAIM, MORD)["verdict"] == eingang
