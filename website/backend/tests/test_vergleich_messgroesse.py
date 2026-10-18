"""Das Label sagte true, die Begründung sagte das Gegenteil — und der Guard bejahte.

Gemessen am 18.10.2026 gegen prod, direkt nach dem Deploy von #260:

    Claim:   "Die USA haben mehr Atomwaffen als Russland"   -> true @ 0,85
    Summary: "Laut SIPRI Yearbook 2026 besitzt Russland 5.420 Atomwaffen,
              die USA 5.042. Russland hat also mehr Atomwaffen als die USA."

Die Zahlen waren richtig (sie kamen gerade aus #260), das Label war falsch
— und Muster O hat es nicht übersehen, sondern **bejaht**.

## Warum

``_o_seiten`` liefert im attributiven Zweig den Zwischen-Slot als Seite 0.
Bei "mehr Männer als Frauen" ist das die verglichene Seite. Bei "mehr
Atomwaffen als Russland" ist es die **Messgrösse** — und damit haben der
Claim und seine Umkehrung DIESELBE Seite 0:

    Claim:   mehr [Atomwaffen] als [Russland]
    Summary: mehr [Atomwaffen] als [die USA]

``summary_bestaetigt_vergleich`` prüfte nur Seite 0 gegen das
Claim-Subjekt und verlangte den Partner bloss *irgendwo* in der Summary.
"russland" stand dort — also galt der Vergleich als bestätigt. Der
Vergleich war strukturell richtungsblind.

## Im Korpus kein Randfall

Zensus über alle 2.814 dokumentierten Formulierungen: 41 tragen
"mehr/weniger … als", und rund ein Drittel der zerlegbaren verliert die
echte Seite an die Messgrösse:

    "Dresden hatte mehr TOTE als Auschwitz"              -> Dresden fehlt
    "Der Verkehr verursacht mehr EMISSIONEN als …"       -> Verkehr fehlt
    "Bitcoin verbraucht mehr STROM als Industrieländer"  -> Bitcoin fehlt
    "Der Mittelstand zahlt mehr STEUERN als die Reichen" -> Mittelstand fehlt

## Der Fix, und was er nicht ist

Die Zerlegung wird NICHT reparariert. Zu unterscheiden, ob das Wort
zwischen Komparativ und "als" die Seite oder die Messgrösse ist, braucht
Grammatik: "In Österreich **werden** mehr Frauen als Männer ermordet" ist
Passiv und die Seite steht dazwischen, "Die USA **haben** mehr Atomwaffen
als Russland" ist Aktiv und die Seite steht davor. Beide haben ein
finites Verb vor dem Komparativ. Jede Regel, die ich dafür fand, zerbrach
an einem Fall des Zensus.

Stattdessen zwei literale Schärfungen, die ohne diese Unterscheidung
auskommen:

1. Die Bestätigung verlangt bei "mehr/weniger" den Partner auf der
   GEGENSEITE, nicht mehr irgendwo. Reine Verschärfung — sie kann
   Bejahungen nur wegnehmen, nie hinzufügen.
2. Die Drehung erkennt den Fall mit gleicher Messgrösse: steht der
   Claim-Partner nicht hinter "als", sondern VOR dem Komparativ, sind die
   Seiten getauscht.

## Gemessen

A/B über alle 16 Vergleichs-Claims mit echter Summary aus QA50F und
QA50H: **0 Verhaltens-Änderungen**. Der Gewinn liegt auf dem
Atomwaffen-Fall und auf einer Grenze, die PR #234 selbst offengelassen
hatte — deren Test endete mit "Wer die Richtung unterscheidbar macht,
dreht diese Zeile um", und genau das ist jetzt geschehen.

Dependency-light: reine Textregeln, kein Netz, kein Modell.
"""
import pytest

from services.verdict_postprocess import (
    _o_seiten,
    apply_verdict_postprocessing,
    summary_bestaetigt_vergleich,
    summary_dreht_vergleich,
    vergleich_aus_claim,
)

# Der echte Wortlaut vom 18.10.2026.
CLAIM = "Die USA haben mehr Atomwaffen als Russland"
SUMMARY = ("Laut SIPRI Yearbook 2026 (Stand Januar 2026) besitzt Russland "
           "5.420 Atomwaffen, die USA 5.042. Russland hat also mehr "
           "Atomwaffen als die USA.")
BESTAETIGEND = "Die USA haben mehr Atomwaffen als Russland."


def _lauf(verdict, claim, summary, konfidenz=0.85):
    r = {"verdict": verdict, "confidence": konfidenz,
         "summary": summary, "nuance": ""}
    apply_verdict_postprocessing(r, [], claim)
    return r


# --- Der gemessene Fall -----------------------------------------------

def test_die_bejahung_greift_nicht_mehr():
    """Der Kern: der Guard darf eine umgekehrte Begründung nicht bejahen."""
    assert not summary_bestaetigt_vergleich(CLAIM.lower(), SUMMARY)


def test_die_drehung_wird_erkannt():
    assert summary_dreht_vergleich(CLAIM.lower(), SUMMARY)


def test_das_label_wird_korrigiert():
    """Vorher true@0.85 mit einer Begründung, die das Gegenteil sagte."""
    assert _lauf("true", CLAIM, SUMMARY)["verdict"] == "false"
    assert _lauf("mostly_true", CLAIM, SUMMARY)["verdict"] == "false"


def test_ein_verneinendes_label_bleibt():
    """Es gibt nichts zu korrigieren, wenn das Label schon stimmt."""
    for eingang in ("false", "mostly_false"):
        assert _lauf(eingang, CLAIM, SUMMARY)["verdict"] == eingang


def test_eine_echt_bestaetigende_summary_bejaht_weiter():
    """Die Verschärfung darf die richtige Richtung nicht mitnehmen."""
    assert summary_bestaetigt_vergleich(CLAIM.lower(), BESTAETIGEND)
    assert not summary_dreht_vergleich(CLAIM.lower(), BESTAETIGEND)
    assert _lauf("false", CLAIM, BESTAETIGEND)["verdict"] == "true"


# --- Die Ursache, als Eigenschaft festgehalten ------------------------

def test_die_zerlegung_nimmt_die_messgroesse_als_seite_null():
    """Nicht reparariert, sondern festgehalten.

    Solange das hier gilt, braucht der Guard die Schärfungen. Wer die
    Zerlegung eines Tages grammatisch löst, wird diesen Test umdrehen —
    und darf dann prüfen, ob die Schärfungen noch nötig sind.
    """
    subjekt, komparativ, partner = vergleich_aus_claim(CLAIM.lower())
    assert komparativ == "mehr"
    assert subjekt == {"atomwaffen"}, "die Messgrösse, nicht die USA"
    assert partner == {"russland"}
    assert "usa" not in subjekt, (
        "Wenn die USA jetzt im Subjekt stehen, ist die Zerlegung "
        "repariert — dann gehört dieser Test umgedreht")


@pytest.mark.parametrize("claim,messgroesse", [
    ("Dresden hatte mehr Tote als Auschwitz", "tote"),
    ("Der Verkehr verursacht mehr Emissionen als die Landwirtschaft",
     "emissionen"),
    ("Bitcoin verbraucht mehr Strom als Industrieländer", "strom"),
    ("Der Mittelstand zahlt mehr Steuern als die Reichen", "steuern"),
])
def test_der_zensus_fall_fuer_fall(claim, messgroesse):
    """Aus dem Zensus über die 2.814 dokumentierten Formulierungen.

    Alle vier verlieren ihre echte Seite an die Messgrösse — deshalb ist
    der Fehler kein Randfall, sondern rund ein Drittel der Form.
    """
    subjekt, _k, _p = vergleich_aus_claim(claim.lower())
    assert messgroesse in subjekt


def test_claim_und_umkehrung_haben_dieselbe_seite_null():
    """Der strukturelle Grund in einer Zeile.

    Ohne diesen Test liest sich die Verschärfung wie Willkür.
    """
    a = "die usa haben mehr atomwaffen als russland"
    b = "russland hat also mehr atomwaffen als die usa"
    # Die Stelle ausrechnen, nicht raten — eine erste Fassung dieses Tests
    # hatte 20 und 18 hartkodiert und schnitt mitten in "atomwaffen".
    vorn_a, hinten_a = _o_seiten(a, "mehr", a.index("mehr"))
    vorn_b, hinten_b = _o_seiten(b, "mehr", b.index("mehr"))
    assert "atomwaffen" in vorn_a and "atomwaffen" in vorn_b
    assert hinten_a.strip() != hinten_b.strip(), (
        "Die Gegenseiten unterscheiden sich — DAS ist die Information, "
        "die die alte Regel nicht benutzt hat")


# --- Wo nichts geändert werden durfte ---------------------------------

OEBB_CLAIM = "Die ÖBB sind pünktlicher als die Deutsche Bahn"
#: Der echte Wortlaut aus dem Live-Lauf vom 27.9.2026 (siehe #227). Der
#: bestätigende Satz endet mit "pünktlicher." — der Vergleichspartner steht
#: im Satz DAVOR. Eine Fassung, die ihn im selben Satz verlangt, feuerte
#: auf 140 Live-Läufen nie. Deshalb gilt die Verschärfung NUR für
#: "mehr/weniger".
OEBB_SUMMARY = (
    "Die ÖBB erreichen 2024 im Fernverkehr 78,2–88,7 % Pünktlichkeit "
    "(5-Minuten-Toleranz), während die Deutsche Bahn im Fernverkehr nur "
    "62,5 % (6-Minuten-Toleranz) aufweisen. Selbst bei strengerer Definition "
    "(5 vs. 6 Minuten) sind die ÖBB deutlich pünktlicher."
)


def test_der_er_als_zweig_ist_unberuehrt():
    assert summary_bestaetigt_vergleich(OEBB_CLAIM.lower(), OEBB_SUMMARY)
    assert _lauf("mostly_false", OEBB_CLAIM, OEBB_SUMMARY)["verdict"] == "true"


def test_die_verschaerfung_gilt_nur_fuer_mehr_und_weniger():
    """Gegenprobe zum Test oben: Wer sie auf '-er als' ausdehnt, bricht
    den Auslöser von #227."""
    import inspect
    quelle = inspect.getsource(summary_bestaetigt_vergleich)
    assert 'komparativ in ("mehr", "weniger")' in quelle, (
        "Die Verschärfung ist nicht mehr auf mehr/weniger beschränkt")


FEMIZID_CLAIM = "In Österreich werden mehr Frauen als Männer ermordet"
FEMIZID_JA = "Bei vollendeten Morden waren also mehr Frauen als Männer Opfer."
FEMIZID_GEDREHT = ("Bei vollendeten Morden gab es 40 weibliche und 36 "
                   "männliche Opfer — also mehr Männer als Frauen.")


def test_femizid_bestaetigung_haelt():
    """Hier IST der Zwischen-Slot die verglichene Seite. Die
    Verschärfung darf diese Form nicht treffen."""
    assert summary_bestaetigt_vergleich(FEMIZID_CLAIM.lower(), FEMIZID_JA)


def test_femizid_drehung_haelt():
    assert summary_dreht_vergleich(FEMIZID_CLAIM.lower(), FEMIZID_GEDREHT)


# --- Giftproben an den beiden Schärfungen -----------------------------

def test_ohne_die_gegenseiten_pruefung_wuerde_bejaht():
    """Die Probe am Instrument: baut man die alte, einseitige Regel nach,
    bejaht sie den gemessenen Fall."""
    import re

    from services._satzgrenzen import teile_in_einheiten
    from services._schreibweise import normalisiere
    from services.verdict_postprocess import (
        _O_HEDGE, _O_NEGATION, _O_WIEDERHOLUNG)

    subjekt, komparativ, partner = vergleich_aus_claim(CLAIM.lower())
    einseitig = False
    for satz in teile_in_einheiten(SUMMARY):
        satz_n = normalisiere(satz)
        stelle = satz_n.find(komparativ)
        if stelle < 0:
            continue
        if any(h in satz_n for h in _O_HEDGE):
            continue
        if any(w in satz_n for w in _O_WIEDERHOLUNG):
            continue
        if any(n in re.findall(r"[a-zäöüßa-z]+", satz_n[:stelle])
               for n in _O_NEGATION):
            continue
        seiten = _o_seiten(satz_n, komparativ, stelle)
        if seiten and any(w in seiten[0] for w in subjekt):
            einseitig = True
    assert einseitig, (
        "Die alte Regel bejaht den Fall nicht mehr — dann prüft dieser "
        "Test nicht länger, dass die Verschärfung das Entscheidende tut")
    assert not summary_bestaetigt_vergleich(CLAIM.lower(), SUMMARY)
