"""Pattern A: Fremd-Attribution des Superlativs generisch erkennen (QA50D #342).

Live-Befund 2026-08-14 (Prod `a9f454e`, aufgedeckt durch Cluster B): Claim
„Die meisten tödlichen Verkehrsunfälle in Österreich passieren auf der
Autobahn" ist FALSCH — nur 25 von 351 Getöteten (7,1 %) entfielen 2024 auf
Autobahnen. Das LLM lieferte korrekt ``false``; Pattern A flippte es auf
``true``, weil die Superlativ-Phrase „die meisten" in der Summary vorkam.

Wurzel, im deployten Container isoliert nachgewiesen:

  „Die meisten Getöteten entfielen auf Landesstraßen B"  -> refutes = False
  dieselbe Aussage mit „Deutschland" statt der Straßenart -> refutes = True

``_summary_refutes_superlative`` prüfte Fremd-Attribution ausschließlich
gegen eine Whitelist aus Ländern und Bundesländern. Straßenarten,
Altersgruppen, Berufsgruppen, Medien, Produkte und Kategorien waren
unsichtbar. Verschärfend: ein ``if not claim_countries: return False`` ganz
oben beendete die Funktion bei JEDEM Claim ohne Land — also bei genau den
Claims, für die die Whitelist ohnehin nichts taugt.

Das ist strukturell dieselbe Überanpassung wie bei Pattern G vor G2: das
Muster wurde in der Geografie-Domäne gebaut und kannte nur sie.

Fehlerkosten-Asymmetrie, die das Design rechtfertigt: ein True dieser
Funktion verhindert lediglich den Confirm-Override — das rohe LLM-Label
bleibt stehen. Ein False-Negative kippt dagegen ein korrektes ``false`` auf
``true``. Im Zweifel also refutieren.

Dependency-light: reine Kaskaden-Tests, kein Netz/LLM.
"""
import pytest

from services.verdict_postprocess import (
    apply_verdict_postprocessing,
    _summary_refutes_superlative,
    _attr_stem_match,
)

# Wörtliche Live-Summary aus dem Cluster-B-Verifikationslauf
LIVE342 = ("2024 starben in Österreich 351 Menschen im Straßenverkehr, davon "
           "25 auf Autobahnen (7,1 %) und 32 inklusive Schnellstraßen "
           "(9,1 %). Die meisten Getöteten entfielen auf Landesstraßen "
           "B/ehemalige Bundesstraßen mit 142 Fällen (40,5 %).")
CLAIM342 = ("Die meisten tödlichen Verkehrsunfälle in Österreich passieren "
            "auf der Autobahn.")


def _refutes(claim, summary):
    return _summary_refutes_superlative(claim.lower(), summary.lower())


def _run(claim, verdict, summary, confidence=0.9):
    return apply_verdict_postprocessing(
        {"verdict": verdict, "confidence": confidence, "summary": summary},
        [], claim)


# --- Der Live-Fall ---

def test_live_342_strassenart_wird_als_fremdattribution_erkannt():
    assert _refutes(CLAIM342, LIVE342) is True


def test_live_342_end_to_end_bleibt_false():
    """Ohne den Fix flippte Pattern A dieses korrekte false auf true."""
    assert _run(CLAIM342, "false", LIVE342)["verdict"] == "false"


def test_live_342_gegenprobe_mit_land_funktionierte_schon_vorher():
    """Belegt, dass nur die ENTITAETSART fehlte, nicht die Logik."""
    mit_land = LIVE342.replace("Landesstraßen B/ehemalige Bundesstraßen",
                               "Deutschland")
    assert _refutes(CLAIM342, mit_land) is True


# --- Claims OHNE Land (frueher sofortiger return False) ---

def test_claim_ohne_land_wird_jetzt_geprueft():
    assert _refutes(
        "Die Krone bekommt die meisten Inserate von der öffentlichen Hand.",
        "Die meisten Inserate gingen an Heute, die Krone liegt auf Rang zwei."
    ) is True


@pytest.mark.parametrize("claim,summary", [
    ("Aldi hat die meisten Filialen in Österreich.",
     "Die meisten Filialen betreibt Spar mit rund 1.500 Standorten."),
    ("Die meisten Studierenden sind an der Universität Wien inskribiert.",
     "Die meisten Studierenden entfallen auf Fachhochschulen."),
])
def test_weitere_entitaetsarten(claim, summary):
    assert _refutes(claim, summary) is True


# --- GEGENRICHTUNG: Pattern A muss weiter bestaetigen koennen ---

def test_superlativ_dem_claim_subjekt_zugeschrieben():
    assert _refutes(
        "Die Krone bekommt die meisten Inserate von der öffentlichen Hand.",
        "Die Krone erhält mit 22,4 Mio. Euro die meisten Inserate."
    ) is False


def test_nachfolger_im_fenster_refutiert_nicht():
    """Attributions-GEBUNDEN statt fenster-basiert: ein blosses
    70-Zeichen-Fenster haette hier faelschlich refutiert."""
    assert _refutes(
        "Die Krone bekommt die meisten Inserate von der öffentlichen Hand.",
        "Die Krone erhält die meisten Inserate, gefolgt von Heute und oe24."
    ) is False


def test_plural_stamm_gilt_als_dasselbe_subjekt():
    assert _refutes(
        "Auf der Autobahn passieren die meisten tödlichen Unfälle.",
        "Die meisten Getöteten entfielen auf Autobahnen mit 142 Fällen."
    ) is False


def test_kurzer_entitaetsname_exakt_gleich():
    """'wien' == 'wien' hat nur 4 Zeichen — muss trotzdem als
    Claim-Subjekt gelten, sonst blockiert der Guard einen korrekten
    Confirm."""
    assert _refutes(
        "In Wien ist die höchste Arbeitslosenquote Österreichs.",
        "Die höchste Quote findet sich in Wien mit 17,7 Prozent."
    ) is False


def test_end_to_end_confirm_bleibt_erhalten():
    """Der Original-Zweck von Pattern A darf nicht kaputtgehen."""
    r = _run("Die Krone bekommt die meisten Inserate von der öffentlichen Hand.",
             "false",
             "Die Krone erhält mit 22,4 Mio. Euro die meisten Inserate der "
             "Bundesregierung.")
    assert r["verdict"] == "true", r


# --- Regressionen der bestehenden Zweige ---

def test_regression_land_zweig():
    assert _refutes("Österreich hat die niedrigste Wohneigentumsquote in der EU.",
                    "Die niedrigste Quote hat Deutschland mit 46 %, nicht Österreich."
                    ) is True


def test_regression_negations_zweig():
    assert _refutes("Deutschland hat die niedrigste Mordrate in Europa.",
                    "Deutschland hat nicht die niedrigste Mordrate; Italien "
                    "liegt darunter.") is True


def test_rekord_jahr_ohne_attributionsmuster():
    assert _refutes("Wien hatte 2024 das wärmste Jahr.",
                    "2024 war in Wien mit 13,0 Grad das wärmste Jahr seit "
                    "Messbeginn.") is False


# --- Der Stamm-Vergleich isoliert ---

@pytest.mark.parametrize("a,b,erwartet", [
    ("autobahn", "autobahnen", True),
    ("wien", "wien", True),
    ("krone", "kronen", True),
    ("auto", "autor", False),
    ("landesstraßen", "autobahn", False),
    ("heute", "krone", False),
])
def test_attr_stem_match(a, b, erwartet):
    assert _attr_stem_match(a, b) is erwartet
