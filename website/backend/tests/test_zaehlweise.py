"""Zaehlweisen-Guard: ein Vergleich ohne genannte Zaehlweise hat keine Seite.

Gemessen am 12.10.2026, zwei unabhaengige Live-Faelle:

    "In Österreich werden mehr Frauen als Männer ermordet"    -> true @ 0.85
    "Österreich 2024: mehr männliche als weibliche Mordopfer"  -> true @ 0.85

    "Mauna Kea ist höher als der Everest"     -> mostly_true @ 0.85
    "Der Everest ist höher als der Mauna Kea" -> true @ 0.85

Beide Richtungen desselben Vergleichs bejaht, beide mit 0,85. Hoechstens
eine Seite kann stimmen. Beide kuratierten Fakten sagen SELBST, dass die
Antwort von der Zaehlweise abhaengt — nur das Label sagt es nicht.

Was hier NICHT getestet wird, weil es nicht gebaut ist: eine Rechnung, die
die Richtung bestimmt. PR #227 hat sie fuer "mehr X als Y" bewusst
stillgelegt, und eine Nachmessung am 12.10.2026 bestaetigte den Grund —
satzweise gerechnet sagt die Arithmetik fuer beide GEGENSAETZLICHEN
Behauptungen "Claim falsch", weil sie jeweils die andere Zaehlweise
greift. Zwei gegensaetzliche Behauptungen koennen nicht beide falsch sein.

Dependency-light: Trigger + Postprocess, kein Netz, kein LLM.
"""
import glob
import json
import os
import re

import pytest

from services._struct_marker import render_data_with_marker
from services._topic_match import substring_or_composite_match
from services._zaehlweise import (
    FELD,
    ZAEHLWEISE_PREFIX,
    claim_nennt_zaehlweise,
    deklarierte_namen,
    name_ist_zulaessig,
    namen_aus_text,
    prefix_fuer,
    unterscheidende_woerter,
)
from services.verdict_postprocess import (
    _ZW_KONFIDENZ_DECKEL,
    _claim_ist_vergleichsfrage,
    apply_verdict_postprocessing,
)

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "data")

#: Die Fakten, die eine Zaehlweisen-Abhaengigkeit deklarieren. Gepinnt,
#: damit ein stiller Verlust der Deklaration auffaellt — ohne sie schweigt
#: der Guard, und das sieht von aussen aus wie "funktioniert".
MARKIERT = (
    ("gleichstellung_pack.json", "femizide_at_de_2026"),
    ("geographie_pack.json", "everest_hoechster_2026"),
)


def _fakt(datei, kennung):
    with open(os.path.join(DATA, datei), encoding="utf-8") as fh:
        d = json.load(fh)
    for f in d.get("facts") or []:
        if (f.get("id") or f.get("topic")) == kennung:
            return f
    raise AssertionError(f"{kennung} nicht in {datei}")


def _quellen(datei, kennung):
    """``source_results`` mit dem ECHTEN gerenderten display_value.

    Nicht mit einem handgeschriebenen Marker-String: Der Rueckweg
    ``namen_aus_text`` muss gegen das pruefen, was der Pack wirklich
    rendert ([[lessons_learned]]: Fixtures sind keine echten Summaries).
    """
    fakt = _fakt(datei, kennung)
    display = render_data_with_marker(fakt.get("data") or {})
    return [{"results": [{"display_value": display}]}]


def _lauf(claim, verdict, konfidenz, quellen, summary="Platzhalter."):
    r = {"verdict": verdict, "confidence": konfidenz,
         "summary": summary, "nuance": ""}
    apply_verdict_postprocessing(r, quellen, claim)
    return r


# --- Die gemessenen Ausloeser ------------------------------------------

@pytest.mark.parametrize("claim", [
    "In Österreich werden mehr Frauen als Männer ermordet",
    "Österreich 2024: mehr männliche als weibliche Mordopfer",
])
def test_beide_richtungen_werden_mixed(claim):
    """Der Kern: nicht mehr beide Richtungen mit 0,85 bejahen."""
    r = _lauf(claim, "true", 0.85, _quellen(*MARKIERT[0]))
    assert r["verdict"] == "mixed", claim
    assert r["confidence"] <= 0.6


@pytest.mark.parametrize("claim,verdict", [
    ("Mauna Kea ist höher als der Everest", "mostly_true"),
    ("Der Everest ist höher als der Mauna Kea", "true"),
    ("Mount Everest ist der höchste Berg der Welt", "true"),
])
def test_everest_beide_richtungen_werden_mixed(claim, verdict):
    r = _lauf(claim, verdict, 0.85, _quellen(*MARKIERT[1]))
    assert r["verdict"] == "mixed", claim
    assert r["confidence"] <= 0.6


def test_nuance_nennt_die_zaehlweisen():
    """Abwerten ohne Begruendung waere nur eine andere Unschaerfe."""
    r = _lauf("In Österreich werden mehr Frauen als Männer ermordet",
              "true", 0.85, _quellen(*MARKIERT[0]))
    for name in deklarierte_namen(_fakt(*MARKIERT[0])["data"]):
        assert name in r["nuance"], name


# --- Wo der Guard schweigen muss ---------------------------------------

@pytest.mark.parametrize("claim", [
    "Bei vollendeten Morden sind in Österreich mehr Frauen als Männer Opfer",
    "Mehr Frauen als Männer sind Opfer von Mord und Mordversuch",
    "Mehr Frauen als Männer sind Opfer von Mordversuchen",
])
def test_claim_nennt_zaehlweise_label_bleibt(claim):
    """Nennt die Behauptung die Zaehlweise, ist sie eindeutig."""
    r = _lauf(claim, "true", 0.85, _quellen(*MARKIERT[0]))
    assert r["verdict"] == "true", claim
    assert r["confidence"] == 0.85


@pytest.mark.parametrize("claim", [
    "Über dem Meeresspiegel ist der Everest höher als der Mauna Kea",
    "Vom Meeresgrund aus gemessen ist Mauna Kea höher als der Everest",
])
def test_everest_claim_nennt_zaehlweise(claim):
    r = _lauf(claim, "true", 0.9, _quellen(*MARKIERT[1]))
    assert r["verdict"] == "true", claim


@pytest.mark.parametrize("claim", [
    # Kein Vergleich, kein Superlativ — der Fakt antwortet eindeutig.
    "Die Zahl der Femizide steigt",
    "Femizide sind Einzelfälle",
    "In Deutschland wird fast jeden Tag eine Frau getötet",
    # Der Ausloeser aus dem Sweep: "werden MEISTENS" ist kein Superlativ.
    "Frauen werden meistens von ihren Partnern getötet",
])
def test_ohne_vergleich_kein_eingriff(claim):
    r = _lauf(claim, "mostly_false", 0.85, _quellen(*MARKIERT[0]))
    assert r["verdict"] == "mostly_false", claim
    assert r["confidence"] == 0.85


def test_wie_hoch_ist_der_everest_bleibt_eindeutig():
    """Eine Mengenfrage hat EINE Antwort — der Guard darf sie nicht
    abwerten. Deshalb greift er nur bei Vergleich oder Superlativ."""
    r = _lauf("Wie hoch ist der Mount Everest?", "true", 0.9,
              _quellen(*MARKIERT[1]))
    assert r["verdict"] == "true"
    assert r["confidence"] == 0.9


def test_unverifiable_bleibt_unberuehrt():
    """Der Guard wertet AB, er wertet nicht auf."""
    r = _lauf("In Österreich werden mehr Frauen als Männer ermordet",
              "unverifiable", 0.1, _quellen(*MARKIERT[0]))
    assert r["verdict"] == "unverifiable"
    assert r["confidence"] == 0.1


def test_selbst_gewaehltes_mixed_wird_gedeckelt():
    """Live gemessen am 12.10.2026, direkt nach dem Deploy von #254.

    Die Prompt-Regel wirkt: auf drei der vier Ausloeser antwortete das
    Modell von SELBST mit "mixed" — und behielt dabei 0,9. Der Guard sah
    das nicht an, weil "mixed" nicht in _BESTIMMTE_LABELS steht. Damit
    bekam dieselbe Erkenntnislage 0,6 oder 0,9, je nachdem wer sie zog.

    Das Label bleibt dabei unberuehrt; nur Konfidenz und Nuance kommen
    dazu.
    """
    r = _lauf("Mauna Kea ist höher als der Everest", "mixed", 0.9,
              _quellen(*MARKIERT[1]))
    assert r["verdict"] == "mixed"
    assert r["confidence"] == _ZW_KONFIDENZ_DECKEL
    assert "was gezaehlt wird" in r["nuance"]


def test_der_deckel_hebt_eine_niedrigere_konfidenz_nicht():
    """``min``, nicht ``=``: ein duenn belegtes mixed bleibt duenn."""
    r = _lauf("Mauna Kea ist höher als der Everest", "mixed", 0.3,
              _quellen(*MARKIERT[1]))
    assert r["confidence"] == 0.3


def test_prompt_regel_und_guard_nennen_dieselbe_zahl():
    """Sonst zieht das Modell auf 0,6 und der Guard deckelt auf 0,5 —
    oder umgekehrt, und niemand merkt es."""
    pfad = os.path.join(os.path.dirname(DATA), "services", "synthesizer.py")
    with open(pfad, encoding="utf-8") as fh:
        quelle = fh.read()
    stelle = quelle.find(ZAEHLWEISE_PREFIX)
    assert re.search(r"0[,.]50.{0,3}0[,.]60", quelle[stelle:stelle + 2000]), (
        "Prompt-Regel nennt keine Konfidenz-Spanne")
    assert _ZW_KONFIDENZ_DECKEL == 0.6, (
        "Deckel und die in der Prompt-Regel genannte Obergrenze "
        "muessen zusammenpassen")


def test_ohne_marker_kein_eingriff():
    """Giftprobe: dieselbe Behauptung, Quellen ohne Marker."""
    r = _lauf("In Österreich werden mehr Frauen als Männer ermordet",
              "true", 0.85, [{"results": [{"display_value":
                                           "Irgendeine Quelle ohne Marker."}]}])
    assert r["verdict"] == "true"
    assert r["confidence"] == 0.85


def test_giftprobe_marker_entfernt_laesst_den_test_fallen():
    """Der Guard haengt wirklich am Marker, nicht am Claim-Text.

    Ohne diese Probe koennte der Guard auf dem Claim-Wortlaut feuern und
    die Tests oben waeren gruen, obwohl die Deklaration gar nicht gelesen
    wird ([[lessons_learned]]: eine gruene Suite beweist nichts ueber
    ungetesteten Code).
    """
    fakt = _fakt(*MARKIERT[0])
    ohne = {k: v for k, v in (fakt.get("data") or {}).items() if k != FELD}
    display = render_data_with_marker(ohne)
    assert ZAEHLWEISE_PREFIX not in display
    r = _lauf("In Österreich werden mehr Frauen als Männer ermordet",
              "true", 0.85, [{"results": [{"display_value": display}]}])
    assert r["verdict"] == "true"


# --- Das Modul selbst --------------------------------------------------

def test_unterscheidende_woerter_lassen_gemeinsame_staemme_weg():
    """"mord" aus "Mord und Mordversuch" unterscheidet NICHT gegen "morde"
    aus "vollendete Morde" — sonst nennt "Mordopfer" eine Zaehlweise und
    der Guard schweigt auf seinem eigenen Ausloeser. Dieselbe Lehre wie
    PR #229: Singular und Plural sind nicht dasselbe Token."""
    w = unterscheidende_woerter(
        ["vollendete Morde", "Mord und Mordversuch", "Partnerschaftskontext"])
    assert "mord" not in w["Mord und Mordversuch"]
    assert "morde" not in w["vollendete Morde"]
    assert w["Mord und Mordversuch"] == ("mordversuch",)
    assert w["vollendete Morde"] == ("vollendete",)


def test_wort_muss_am_wortanfang_stehen():
    """"morde" in "erMORDEt" ist kein Hinweis auf eine Zaehlweise.

    Die Namen hier sind ABSICHTLICH nicht die echten: Bei der
    Deklaration des Femizid-Fakts raeumt die Stamm-Gruppierung "morde"
    schon weg (es hat "mord" als Geschwister), und dann traegt die
    Wortanfang-Bindung nichts mehr. Eine Giftprobe am 12.10.2026 zeigte
    genau das — die Bindung liess sich entfernen, ohne dass ein Test
    fiel. Zwei Namen ohne gemeinsamen Stamm stellen sie frei.

    Damit ist die Bindung nicht etwa unnoetig: Sie schuetzt jede
    kuenftige Deklaration, in der ein kurzes Wort ohne Geschwister
    unterscheidet. Nur traegt sie in den heutigen Daten nichts — das
    steht hier, damit niemand sie fuer gemessen haelt.
    """
    namen = ["vollendete Morde", "Partnerschaftskontext"]
    assert "morde" in unterscheidende_woerter(namen)["vollendete Morde"]
    assert claim_nennt_zaehlweise("Frauen werden ermordet", namen) is None
    # Am Wortanfang eines Kompositums trifft es weiter — das ist gewollt.
    assert claim_nennt_zaehlweise("Mordesfaelle in Wien", namen) \
        == "vollendete Morde"


def test_echte_namen_brauchen_die_bindung_nicht():
    """Gegenprobe zum Test oben, auf den ECHTEN Deklarationen."""
    namen = deklarierte_namen(_fakt(*MARKIERT[0])["data"])
    assert claim_nennt_zaehlweise("Frauen werden ermordet", namen) is None
    assert claim_nennt_zaehlweise("weibliche Mordopfer", namen) is None


def test_endung_darf_abweichen():
    """"bei vollendeten Morden" muss "vollendete Morde" finden."""
    namen = ["vollendete Morde", "Mord und Mordversuch"]
    assert claim_nennt_zaehlweise(
        "Bei vollendeten Morden ueberwiegen Frauen", namen) == "vollendete Morde"


def test_eine_zaehlweise_allein_ist_keine_mehrdeutigkeit():
    assert deklarierte_namen({FELD: ["nur eine"]}) == ()
    assert deklarierte_namen({FELD: "ein String"}) == ()
    assert deklarierte_namen({}) == ()


def test_prefix_und_rueckweg_sind_umkehrbar():
    namen = ("vollendete Morde", "Mord und Mordversuch", "Partnerschaftskontext")
    assert namen_aus_text(prefix_fuer(namen)) == namen
    assert namen_aus_text("Vorne was | " + prefix_fuer(namen) + " hinten") == namen
    assert namen_aus_text("kein Marker hier") == ()


# --- Gates ueber ALLE Fakten -------------------------------------------

def _alle_fakten():
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        try:
            with open(p, encoding="utf-8") as fh:
                d = json.load(fh)
        except Exception:
            continue
        if not isinstance(d, dict):
            continue
        for f in d.get("facts") or []:
            if isinstance(f, dict):
                yield os.path.basename(p), (f.get("id") or f.get("topic")), f


def test_jeder_deklarierte_name_uebersteht_den_rueckweg():
    """Ein Name mit Klammer oder " / " zerschneidet den Rueckweg still.

    Gate statt Konvention: ``namen_aus_text`` liest aus einer Klammer und
    trennt an " / ". Ein Name, der eines davon enthaelt, kommt anders
    zurueck als er hineinging — und der Guard wertet dann mit falschen
    Namen in der Nuance ab, oder gar nicht.
    """
    gepruefte = 0
    for datei, kennung, f in _alle_fakten():
        namen = deklarierte_namen(f.get("data") or {})
        if not namen:
            continue
        gepruefte += 1
        for n in namen:
            assert name_ist_zulaessig(n), f"{datei}::{kennung}: {n!r}"
        assert namen_aus_text(prefix_fuer(namen)) == namen, f"{datei}::{kennung}"
    assert gepruefte == len(MARKIERT), (
        f"{gepruefte} Fakten deklarieren Zaehlweisen, erwartet "
        f"{len(MARKIERT)} — neue Deklaration gehoert in MARKIERT")


def test_jeder_name_hat_ein_unterscheidendes_wort():
    """Ein Name ohne eigenes Wort ist nicht erkennbar.

    Nennt die Behauptung ihn, merkt der Guard es nicht und wertet eine
    eindeutige Aussage ab. Das Gate rechnet es aus den Namen aus, statt
    es zuzusichern ([[lessons_learned]]: die Zusage im Datenfeld ist
    ungepruefte Schuld, bis ein Gate sie prueft).
    """
    for datei, kennung, f in _alle_fakten():
        namen = deklarierte_namen(f.get("data") or {})
        if not namen:
            continue
        for name, woerter in unterscheidende_woerter(namen).items():
            assert woerter, f"{datei}::{kennung}: {name!r} ohne eigenes Wort"


def test_ueber_trigger_sweep_ueber_alle_phrasings():
    """Kein FREMDER Fakt wird abgewertet.

    Gemessen am 12.10.2026 ueber alle 2.814 dokumentierten
    Formulierungen: 93 davon sind Vergleiche oder Superlative, und auf
    KEINER wuerde ein markierter Fakt eine fremde Antwort abwerten. Der
    Sweep laeuft hier als Gate mit, nicht als Notiz — die Zahl aendert
    sich mit jeder neuen Formulierung.

    ⚠️ Seit #288 bekommt ``_claim_ist_vergleichsfrage`` die Formulierung
    in IHRER Schreibung — sonst prueft dieses Gate den neuen
    Orthografie-Pfad gar nicht, weil der auf ein kleingeschriebenes
    Original nicht anspringt. Eine gruene Suite haette hier nichts
    bewiesen.
    """
    fakten = [(k, _fakt(d, k)) for d, k in MARKIERT]
    fremde = []
    for datei, kennung, f in _alle_fakten():
        for ph in f.get("claim_phrasings_handled") or []:
            if not isinstance(ph, str) or not ph.strip():
                continue
            ist_vgl = _claim_ist_vergleichsfrage(ph.lower(), ph)
            for k, mf in fakten:
                if kennung == k or not ist_vgl:
                    continue
                if not substring_or_composite_match(mf, ph.lower()):
                    continue
                namen = deklarierte_namen(mf.get("data") or {})
                if not claim_nennt_zaehlweise(ph, namen):
                    fremde.append((k, datei, kennung, ph))
    assert not fremde, f"{len(fremde)} fremde Abwertungen: {fremde[:5]}"


def test_superlativ_braucht_eine_linke_wortgrenze():
    """"frauen WERDEN MEISTENS" ist kein Superlativ.

    Der Fehler war vorbestehend und traf auch die drei aelteren
    Fundstellen von ``_SUPERLATIVE_PHRASE``, die SUMMARIES lesen: das
    "den" der Regex kam aus "werden". Gefunden im Sweep oben.
    """
    assert not _claim_ist_vergleichsfrage(
        "frauen werden meistens von ihren partnern getötet")
    assert not _claim_ist_vergleichsfrage(
        "die zahlen werden größtenteils bestätigt")
    # Die echten Formen muessen weiter treffen.
    assert _claim_ist_vergleichsfrage("wien hat den höchsten wert")
    assert _claim_ist_vergleichsfrage("das ist die größte gruppe")


def test_prompt_regel_steht_in_beiden_sprachen():
    """Verteidigung in der Tiefe: das Modell soll beide Zahlenpaare
    nennen, nicht nur das Label stimmen. Ohne Prompt-Regel bleibt die
    Summary einseitig und die Nuance widerspricht ihr."""
    pfad = os.path.join(os.path.dirname(DATA), "services", "synthesizer.py")
    with open(pfad, encoding="utf-8") as fh:
        quelle = fh.read()
    assert quelle.count(ZAEHLWEISE_PREFIX) >= 2, (
        "Prompt-Regel fehlt in einer der beiden Sprachfassungen")
    for sprachmarke in ("ENTSCHEIDUNGSREGEL", "DECISION RULE"):
        stelle = quelle.find(ZAEHLWEISE_PREFIX)
        assert re.search(
            re.escape(ZAEHLWEISE_PREFIX) + r".{0,2000}" + sprachmarke,
            quelle[stelle:], re.S), sprachmarke
