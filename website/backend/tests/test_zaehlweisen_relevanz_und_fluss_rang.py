"""Der Waechter wertete ab, obwohl das Verdict gar nicht auf seiner Quelle
ruhte — und die Gesamtlaenge wurde zur Rangliste im Land.

Beide Befunde live am 28.10.2026 gegen prod (`45025d2`) gemessen, nachdem
#286 die beiden Geographie-Abfragen zum Laufen gebracht hatte. Die
Abfragen liefern seitdem: "Österreich" 1 Zeile, "Deutschland" 1, "Nepal"
1, "Deutschland"/"Italien" 3 Fluss-Zeilen. Falsch sind jetzt die
ANTWORTEN.

## Befund 1 — eine abgerufene Quelle, die nichts beitrug, entschied mit

    "Welcher ist der höchste Berg Österreichs?"   mixed @ 0,6
    "Wie heißt der höchste Berg in Deutschland?"  mixed @ 0,6

Beide Antworten waren RICHTIG (Großglockner 3.798 m, Zugspitze 2.962 m,
beide aus Wikidata) und benannten die Zaehlweise von selbst: „Andere
Definitionen (z. B. vom Meeresboden) sind für Österreich nicht
relevant." Der Zaehlweisen-Guard haengte trotzdem „Die Behauptung nennt
keine davon" an und deckelte auf 0,6 — er sah den Everest/Mauna-Kea-Fakt,
der zum THEMA passt, zur Frage aber nichts beitraegt. Unter den Belegen
stand er nicht.

⚠️ **Die naheliegende Regel wurde gemessen und VERWORFEN.** „Die Summary
nennt eine Zaehlweise, also ist sie eindeutig" — alle vier
Original-Ausloeser nennen in ihrer Summary ebenfalls eine:

    "Die Behauptung bezieht sich auf vollendete Morde."
    "Die Behauptung bezieht sich auf die Höhe über dem Meeresspiegel."

Und genau das ist der Fall, den der Guard fangen soll: Das Modell waehlt
eine Lesart, die die Behauptung nie genannt hat. Die Summary trennt die
Faelle nicht.

Was sie trennt, ist die BELEGLAGE — 6 von 6 richtig:

    Berg Österreich / Deutschland   Beleg: nur Wikidata      -> schweigen
    Mauna Kea / Everest, beide      Beleg: Britannica-URL    -> feuern
    Mordopfer, beide Richtungen     Beleg: Konsens-URL       -> feuern

Entschieden wird nur, wenn es etwas zu vergleichen gibt (die Zeile hat
eine URL, und es gibt Belege). Sonst feuert der Guard wie bisher: Ein
nicht entscheidbarer Fall darf keinen Waechter stilllegen. Genau darauf
ruhen die 33 bestehenden Tests in ``test_zaehlweise.py`` — ihre Zeilen
haben keine URL.

Die Reichweite ist gemessen: **genau zwei Fakten** im ganzen Bestand
deklarieren Zaehlweisen (``everest_hoechster_2026``,
``femizide_at_de_2026``). Beide sind oben in beiden Richtungen live
geprueft.

## Befund 2 — die Gesamtlaenge wurde zur Rangliste im Land

    "Wie lang ist der längste Fluss Deutschlands?"   true @ 0,85
    "Der längste Fluss Deutschlands ist die Donau mit einer Gesamtlänge
     von 2.850 km, wobei die Strecke innerhalb Deutschlands 647 km
     beträgt."

Das ist falsch: Der Rhein fuehrt 865 seiner 1.233 km durch Deutschland.
Die Antwort nennt die 647 km selbst und stellt die Donau trotzdem an die
Spitze.

``_format_row`` haengt an jede Fluss-Zeile „GESAMTlaenge des Flusses,
NICHT die Strecke im angefragten Land". Live BEFOLGT das Modell den
Hinweis — in einer Formulierung von zwei:

    "Welcher ist der längste Fluss Österreichs?"  unverifiable @ 0,1
    "Die Wikidata-Einträge beziehen sich auf die Gesamtlänge der Flüsse,
     nicht auf die Strecke innerhalb Österreichs."

Dieselben drei Zeilen, derselbe Hinweis, zwei Antworten. Ein Hinweis in
den Daten kann eine Zuordnung nicht erzwingen (#285), also tut es der
Waechter.

Keine Abfrage kann das ersetzen: Direkt gegen WDQS geprueft hat die Donau
**genau eine** Laengenangabe (2.850 km), ohne Land-Qualifikator.

Gemessen ueber fuenf Formulierungen aendert sich **eine** — die falsche:

    "Der längste Fluss Deutschlands ist die Donau"   false @0,85  bleibt
    "Wie lang ist der längste Fluss Deutschlands?"   true  @0,85  -> unverifiable
    "Die Donau ist der längste Fluss in Deutschland" false @0,85  bleibt
    "Welcher ist der längste Fluss Österreichs?"     unverif.     bleibt
    "Der längste Fluss Österreichs ist die Donau"    true  @0,85  bleibt

Die drei unberuehrten haben Wikipedia mitbelegt und antworten richtig
(„nicht der längste Fluss in Deutschland … der Rhein mit 1.233 km, davon
865 km in Deutschland"). Der Waechter greift nur, wenn das Verdict
AUSSCHLIESSLICH auf Gesamtlaengen-Zeilen ruht.

Label ``unverifiable`` wie beim Prognose-Guard: Das ist keine geratene
Richtung, sondern die Beleglage — und es ist dieselbe Antwort, die das
Modell in der oesterreichischen Formulierung von selbst gab. Ein
gedeckeltes „true" haette die falsche Rangliste in der Schlagzeile
stehen gelassen.

Dependency-light: Postprocess + der echte Renderer, kein Netz.
"""
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services.verdict_postprocess import (  # noqa: E402
    GESAMTLAENGE_MARKER,
    _BESTIMMTE_LABELS,
    _ZW_KONFIDENZ_DECKEL,
    _quelle_traegt_das_verdict,
    _url_kern,
    apply_verdict_postprocessing,
)
from services._struct_marker import render_data_with_marker  # noqa: E402
from services.wikidata import _format_row  # noqa: E402

DATA = os.path.join(BACKEND, "data")

HINWEIS_ZW = "Die Antwort haengt davon ab, was gezaehlt wird"
HINWEIS_FL = "nur die GESAMTlaenge"

def _dv(datei: str, kennung: str) -> str:
    """Der ECHT gerenderte display_value eines Fakts.

    Nicht handgeschrieben: Mein erster Entwurf baute den Marker-String
    selbst nach, und ``namen_aus_text`` las ihn nicht — sieben Tests
    gruen, die nichts bewiesen, weil der Guard nie feuerte. Dieselbe
    Falle wie [[lessons_learned]] „Fixtures sind keine echten
    Summaries", hier auf der Quellen-Seite.
    """
    with open(os.path.join(DATA, datei), encoding="utf-8") as fh:
        d = json.load(fh)
    for f in d.get("facts") or []:
        if (f.get("id") or f.get("topic")) == kennung:
            return render_data_with_marker(f.get("data") or {})
    raise AssertionError(f"{kennung} nicht in {datei}")


EVEREST_URL = "https://www.britannica.com/place/Mount-Everest"


def _quelle(datei="geographie_pack.json", kennung="everest_hoechster_2026",
            url=EVEREST_URL, label="Geographie-Mythen"):
    return {"source": label,
            "results": [{"display_value": _dv(datei, kennung), "url": url}]}


def _lauf(claim, quellen, evidence, verdict="true", konfidenz=0.85,
          summary="Platzhalter.", nuance=""):
    r = {"verdict": verdict, "confidence": konfidenz, "summary": summary,
         "nuance": nuance, "evidence": evidence}
    apply_verdict_postprocessing(r, quellen, claim)
    return r


BERG_CLAIM = "Welcher ist der höchste Berg Österreichs?"
WIKIDATA_BELEG = [{"source": "Wikidata",
                   "finding": "Großglockner (3.798 m) ist der höchste "
                              "Punkt von Österreich.",
                   "url": "https://www.wikidata.org/wiki/Q3388"}]


# ------------------------------------------------- Befund 1: die Beleglage

def test_der_berg_behaelt_sein_label():
    """Der live gemessene Fall: Everest-Fakt abgerufen, aber nicht belegt."""
    r = _lauf(BERG_CLAIM, [_quelle()], WIKIDATA_BELEG)
    assert r["verdict"] == "true"
    assert r["confidence"] == 0.85
    assert HINWEIS_ZW not in (r.get("nuance") or "")


def test_belegte_quelle_feuert_weiter():
    """Die vier Original-Ausloeser: die deklarierende Quelle IST belegt."""
    belege = [{"source": "USGS + Britannica", "finding": "…",
               "url": EVEREST_URL}]
    r = _lauf("Ist der Mauna Kea höher als der Mount Everest?",
              [_quelle()], belege)
    assert r["verdict"] == "mixed"
    assert r["confidence"] == _ZW_KONFIDENZ_DECKEL
    assert HINWEIS_ZW in r["nuance"]


def test_quellenbezeichnung_statt_url_zaehlt_auch():
    """Das Modell kuerzt die Bezeichnung; ein Treffer am markanten Anfang
    reicht. Allein traegt dieses Kriterium nicht — 2 der 4 Ausloeser
    verfehlt es —, als Ergaenzung senkt es das Risiko eines
    stillgelegten Waechters."""
    belege = [{"source": "Geographie-Mythen (NASA + UNESCO)",
               "finding": "…", "url": "https://example.org/anders"}]
    r = _lauf("Ist der Mauna Kea höher als der Mount Everest?",
              [_quelle()], belege)
    assert r["verdict"] == "mixed"


@pytest.mark.parametrize("quellen,belege,warum", [
    ([_quelle(url="")], WIKIDATA_BELEG, "Zeile ohne URL"),
    ([_quelle()], [], "keine Belege"),
    ([_quelle()], None, "Belege fehlen ganz"),
])
def test_nicht_entscheidbar_feuert_wie_bisher(quellen, belege, warum):
    """Darauf ruhen die 33 bestehenden Tests — ihre Zeilen haben keine
    URL. Ein nicht entscheidbarer Fall darf keinen Waechter stilllegen."""
    r = _lauf("Ist der Mauna Kea höher als der Mount Everest?",
              quellen, belege)
    assert r["verdict"] == "mixed", warum
    assert HINWEIS_ZW in r["nuance"], warum


def test_zweite_deklaration_wird_nicht_uebersehen():
    """Die erste Deklaration ist unbelegt, die zweite belegt — dann feuert
    der Guard mit der zweiten. Vorher brach die Suche bei der ersten ab."""
    zweite = _quelle("gleichstellung_pack.json", "femizide_at_de_2026",
                     url="https://www.coordination-vaw.gv.at/x",
                     label="Gleichstellung-Konsens")
    belege = [{"source": "BKA", "finding": "…",
               "url": "https://www.coordination-vaw.gv.at/x"}]
    r = _lauf("In Deutschland werden mehr Frauen als Männer ermordet",
              [_quelle(), zweite], belege)
    assert r["verdict"] == "mixed"
    assert "vollendete Morde" in r["nuance"]


def test_die_verworfene_regel_wuerde_den_guard_stilllegen():
    """Giftprobe gegen die gemessene Fehl-Hypothese: Die Summary nennt
    eine Zaehlweise — der Guard muss trotzdem feuern, denn genau diese
    Wahl hat die Behauptung nie getroffen."""
    belege = [{"source": "Konsens", "finding": "…", "url": EVEREST_URL}]
    r = _lauf("Ist der Mauna Kea höher als der Mount Everest?",
              [_quelle()], belege,
              summary="Die Behauptung bezieht sich auf die Höhe über dem "
                      "Meeresspiegel, die Standarddefinition.")
    assert r["verdict"] == "mixed"
    assert HINWEIS_ZW in r["nuance"]


@pytest.mark.parametrize("a,b", [
    ("https://www.example.org/x", "http://example.org/x/"),
    ("HTTPS://Example.org/X", "example.org/X"),
])
def test_url_kern_vergleicht_schreibvarianten(a, b):
    assert _url_kern(a) == _url_kern(b)


def test_url_kern_unterscheidet_verschiedene_seiten():
    assert _url_kern("https://a.org/x") != _url_kern("https://a.org/y")


def test_entscheidbarkeit_ist_dreiwertig():
    """``None`` heisst „nicht entscheidbar" und ist nicht ``False``."""
    _ev = _dv("geographie_pack.json", "everest_hoechster_2026")
    zeile = {"display_value": _ev, "url": EVEREST_URL}
    assert _quelle_traegt_das_verdict(zeile, "x", []) is None
    assert _quelle_traegt_das_verdict({"display_value": _ev},
                                      "x", WIKIDATA_BELEG) is None
    assert _quelle_traegt_das_verdict(zeile, "x", WIKIDATA_BELEG) is False
    assert _quelle_traegt_das_verdict(
        zeile, "x", [{"url": EVEREST_URL}]) is True


# ------------------------------------------------ Befund 2: der Fluss-Rang

FLUSS_CLAIM = "Wie lang ist der längste Fluss Deutschlands?"
FLUSS_SUMMARY = ("Der längste Fluss Deutschlands ist die Donau mit einer "
                 "Gesamtlänge von 2.850 km, wobei die Strecke innerhalb "
                 "Deutschlands 647 km beträgt.")


def _fluss_quellen():
    zeilen = []
    for label, qid, laenge in (("Donau", "Q1653", "2850"),
                               ("Rhein", "Q584", "1232.7"),
                               ("Elbe", "Q1644", "1094")):
        text, _, _ = _format_row("geographie_fluss",
                                 {"riverLabel": {"value": label},
                                  "length": {"value": laenge}})
        zeilen.append({"display_value": text,
                       "url": f"https://www.wikidata.org/wiki/{qid}"})
    return [{"source": "Wikidata SPARQL", "results": zeilen}]


def _fluss_belege(qids=("Q1653", "Q584", "Q1644")):
    return [{"source": "Wikidata (CC0)", "finding": "…",
             "url": f"https://www.wikidata.org/wiki/{q}"} for q in qids]


def test_der_marker_kommt_aus_dem_echten_renderer():
    """Die Konstante im Waechter gegen die AUSGABE des Renderers — nicht
    gegen einen handgeschriebenen String."""
    text, _, _ = _format_row("geographie_fluss",
                             {"riverLabel": {"value": "Donau"},
                              "length": {"value": "2850"}})
    assert GESAMTLAENGE_MARKER in text, text


def test_die_falsche_rangliste_wird_unverifiable():
    r = _lauf(FLUSS_CLAIM, _fluss_quellen(), _fluss_belege(),
              summary=FLUSS_SUMMARY)
    assert r["verdict"] == "unverifiable"
    assert r["confidence"] == 0.10
    assert HINWEIS_FL in r["nuance"]


def test_der_hinweis_nennt_die_zahlen_die_es_entscheiden():
    r = _lauf(FLUSS_CLAIM, _fluss_quellen(), _fluss_belege(),
              summary=FLUSS_SUMMARY)
    assert "865" in r["nuance"] and "1.233" in r["nuance"]
    assert "647" in r["nuance"] and "2.850" in r["nuance"]


def test_ein_weiterer_beleg_laesst_das_verdict_stehen():
    """Live: Wo Wikipedia mitbelegt, antwortete der Dienst richtig."""
    belege = _fluss_belege(("Q1653",)) + [
        {"source": "Wikipedia (DE)", "finding": "Der Rhein führt 865 km "
         "durch Deutschland.", "url": "https://de.wikipedia.org/wiki/Rhein"}]
    r = _lauf("Der längste Fluss Österreichs ist die Donau",
              _fluss_quellen(), belege)
    assert r["verdict"] == "true"
    assert r["confidence"] == 0.85


def test_ohne_belege_kein_eingriff():
    r = _lauf(FLUSS_CLAIM, _fluss_quellen(), [], summary=FLUSS_SUMMARY)
    assert r["verdict"] == "true"


def test_unverifiable_bleibt_unberuehrt():
    """Die oesterreichische Formulierung lag live schon richtig — der
    Waechter darf ihre Konfidenz nicht anheben."""
    r = _lauf("Welcher ist der längste Fluss Österreichs?",
              _fluss_quellen(), _fluss_belege(),
              verdict="unverifiable", konfidenz=0.1,
              summary="Die bereitgestellten Quellen enthalten keine "
                      "Informationen zur Länge der Flüsse in Österreich.")
    assert r["verdict"] == "unverifiable"
    assert r["confidence"] == 0.1


@pytest.mark.parametrize("verdict", _BESTIMMTE_LABELS)
def test_jedes_bestimmte_label_wird_gefangen(verdict):
    """Auch ein ``false`` ruht dann auf Zeilen, die in KEINER Richtung
    eine Rangfolge im Land hergeben."""
    r = _lauf(FLUSS_CLAIM, _fluss_quellen(), _fluss_belege(),
              verdict=verdict, summary=FLUSS_SUMMARY)
    assert r["verdict"] == "unverifiable", verdict


def test_ohne_fluss_zeilen_kein_eingriff():
    """Giftprobe: Marker entfernt, sonst alles gleich."""
    quellen = _fluss_quellen()
    for z in quellen[0]["results"]:
        z["display_value"] = z["display_value"].replace(
            GESAMTLAENGE_MARKER, "Laenge")
    r = _lauf(FLUSS_CLAIM, quellen, _fluss_belege(), summary=FLUSS_SUMMARY)
    assert r["verdict"] == "true"
    assert HINWEIS_FL not in (r.get("nuance") or "")
