"""Ein Länder-Verzeichnis für alle Konnektoren — und die Vergleichsart dazu.

Anlass, 2026-09-07: „Wie ist die Pressefreiheit in Suedafrika laut RSF?" kam
als `unverifiable@0.1` zurück. RSF **wurde abgefragt** und lieferte nichts —
nicht wegen der Schreibweise, sondern weil `südafrika` in `rsf.COUNTRY_MAP`
fehlte, obwohl `ZAF` im Datensatz steht:

    rsf: Karte 44 Länder, Datensatz 180   ->   136 unerreichbar

**Das Schwierige war nicht die Tabelle, sondern die Vergleichsart.** Von 44
auf 183 Länder heisst Kollisionen, und die bisherige Suche prüfte
`name in claim` in Einfüge-Reihenfolge:

    "mali"     steckt in  "so-MALI-a"
    "niger"    steckt in  "NIGER-ia"
    "oman"     steckt in  "r-OMAN-ia"
    "russland" steckt in  "weiss-RUSSLAND"
    "guinea"   steckt in  "papua-neu-GUINEA", "GUINEA-bissau"

Drei Regeln zusammen lösen das, einzeln keine — und **Regel 3 ist die, die man
vergisst**: ohne sie liefert „Nigeria" *beide* Codes, weil „niger" dieselbe
Stelle gleich nochmal trifft.

Die Flexions-Regel ist gemessen, nicht ausgedacht. Ein erster Entwurf prüfte
nur den Wortanfang — dann traf „benin" den „Beninger Weg" und „guinea" das
englische „guineafowl". Beide Wortenden zu verlangen ging auch nicht:
„Österreichs Pressefreiheit" verlor den Genitiv. Erlaubt ist deshalb hinter
dem Alias ein Genitiv-s oder, bei „-isch"-Aliassen, bis zu drei Buchstaben
Adjektiv-Endung — eine grammatische Regel, keine Wortliste (Lehre aus #141).
"""

import json
import re
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._laender import ALIASSE, aus_analyse, finde  # noqa: E402
from services._schreibweise import normalisiere  # noqa: E402
from services.rsf import _find_countries, _load_rsf_data  # noqa: E402


def _rsf_laender():
    return _load_rsf_data()[0]


# --------------------------------------------------------------------------
# Der Befund
# --------------------------------------------------------------------------

def test_jedes_land_im_rsf_datensatz_ist_erreichbar():
    """Vorher 44 von 180. Ein durchgefallenes Land heisst nicht „keine Daten",
    sondern „Antwort für ein anderes Land" — der DACH-Default springt ein."""
    bc = _rsf_laender()
    assert len(bc) >= 180, f"Datensatz geschrumpft: {len(bc)}"
    fehlt = [c for c in bc
             if _find_countries({"claim": f"Pressefreiheit in {bc[c]['country_en']}"})[:1] != [c]]
    assert not fehlt, f"{len(fehlt)} unerreichbar: {fehlt[:10]}"


def test_suedafrika_der_ausloesende_fall():
    for schreibweise in ("Südafrika", "Suedafrika", "South Africa"):
        assert _find_countries(
            {"claim": f"Wie ist die Pressefreiheit in {schreibweise}?"}) == ["ZAF"]


def test_jeder_alias_trifft_sein_land():
    """623 Aliasse, jeder muss den Code liefern, unter dem er steht."""
    fehl = [(iso, a) for iso, al in ALIASSE.items() for a in al
            if finde(f"Bericht ueber {a} heute", frozenset({iso}))[:1] != [iso]]
    assert not fehl, fehl[:8]


# --------------------------------------------------------------------------
# Die drei Regeln — jede einzeln festgenagelt
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,erwartet", [
    ("Somalia", ["SOM"]),          # "mali" darf nicht mitgehen
    ("Mali", ["MLI"]),
    ("Somalia und Mali", ["SOM", "MLI"]),
    ("Romania", ["ROU"]),          # "oman" steckt in r-OMAN-ia
    ("Oman", ["OMN"]),
    ("Weissrussland", ["BLR"]),    # "russland" steckt darin
    ("Russland", ["RUS"]),
])
def test_regel_1_wortgrenze(text, erwartet):
    assert finde(f"Ein Bericht ueber {text} von heute") == erwartet


def test_regel_2_laengste_zuerst():
    """„weissrussland" muss vor „russland" geprüft werden."""
    reihenfolge = [a for a, _ in
                   __import__("services._laender", fromlist=["x"])._suchreihenfolge()]
    laengen = [len(a) for a in reihenfolge]
    assert laengen == sorted(laengen, reverse=True)


@pytest.mark.parametrize("text,erwartet", [
    ("Nigeria", ["NGA"]),          # ohne Regel 3 kaeme NER mit
    ("Niger", ["NER"]),
    ("Niger und Nigeria", ["NER", "NGA"]),
    ("Nigeria und Niger", ["NGA", "NER"]),   # Reihenfolge = Text, nicht Alias-Laenge
    ("Guinea-Bissau", ["GNB"]),
    ("Papua-Neuguinea", ["PNG"]),
    ("Äquatorialguinea", ["GNQ"]),
    ("Guinea", ["GIN"]),
])
def test_regel_3_fundstelle_verbrauchen(text, erwartet):
    """Ohne das Ausixen der Fundstelle trifft ein kürzerer Alias dieselbe
    Stelle noch einmal — „Nigeria" lieferte dann NGA *und* NER."""
    assert finde(f"Ein Bericht ueber {text} von heute") == erwartet


# --------------------------------------------------------------------------
# Flexion gegen Fehltreffer — der Entwurf davor lag in beide Richtungen falsch
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,erwartet", [
    ("das Vereinigte Königreich", "GBR"),       # flektiertes VORDERES Wort
    ("die Vereinigten Staaten", "USA"),
    ("in der Europäischen Union", "EUR"),
    ("die russischen Medien", "RUS"),
    ("Österreichs Pressefreiheit", "AUT"),      # Genitiv-s
    ("Oesterreichs Pressefreiheit", "AUT"),
    ("Deutschlands Presse", "DEU"),
    ("die russische Föderation", "RUS"),        # Adjektiv-Endung
    ("kenianische Medien", "KEN"),
    ("namibische Zeitungen", "NAM"),
])
def test_flexion_wird_erkannt(text, erwartet):
    assert finde(text)[:1] == [erwartet], text


@pytest.mark.parametrize("text", [
    "Der Beninger Weg in Wien",        # "benin" + "ger"
    "Guineafowl ist das englische Wort fuer Perlhuhn",
    "Die Romanverfilmung war gut",
    "Ein Tonabnehmer für den Plattenspieler",
    "Der Malermeister kam am Montag",
    "Die Chance auf Regen ist gering",
    "Kristalle wachsen in Salzlösungen",
    "Wie backe ich einen Marmorkuchen?",
    "Der FC Bayern hat gestern gewonnen",
])
def test_kein_fehltreffer_in_gewoehnlichen_woertern(text):
    """Gemessen, nicht ausgedacht: die ersten beiden Fälle hat ein früherer
    Entwurf tatsächlich getroffen."""
    assert finde(text) == [], text


# --------------------------------------------------------------------------
# Sauberkeit der Tabelle
# --------------------------------------------------------------------------

def test_iso3_codes_sind_wohlgeformt():
    for iso in ALIASSE:
        assert len(iso) == 3 and iso.isupper() and iso.isalpha(), iso


# „usa" stand in vier der alten Karten, „uk" in `wgi` — beide sind die
# häufigste Schreibweise überhaupt. Unter der Wortgrenzen-Prüfung sind sie
# eindeutig („usa" trifft „usability" nicht). Ein Blanko-Minimum von zwei
# Zeichen wäre dagegen gefährlich: „at" (Österreich) stand in einer der 24
# Karten und steckt in „at the". „eu" ist bewusst nicht dabei — das Token hat
# schon einmal über-getriggert (#110).
KURZ_ERLAUBT = {"usa", "uk"}


def test_keine_leeren_oder_zu_kurzen_aliasse():
    """Ein Alias unter vier Zeichen würde quer durch den Korpus matchen —
    ausser den zwei gemessenen Ausnahmen."""
    for iso, al in ALIASSE.items():
        assert al, iso
        for a in al:
            assert len(a) >= 4 or a in KURZ_ERLAUBT, (iso, a)
            assert a == a.lower().strip(), (iso, a)


def test_kurze_aliasse_bleiben_die_gemessene_ausnahme():
    """Wer eine dritte Kurzform aufnimmt, soll das bewusst tun."""
    kurz = {a for al in ALIASSE.values() for a in al if len(a) < 4}
    assert kurz == KURZ_ERLAUBT, kurz


@pytest.mark.parametrize("text,erwartet", [
    ("Die USA und Kanada", ["USA", "CAN"]),
    ("Korruption in den USA", ["USA"]),
    ("UK-Politik nach dem Brexit", ["GBR"]),
    ("usability testing", []),
    ("Die Ursache war unklar", []),
    ("Ein Ukulele-Konzert", []),
])
def test_kurzformen_sind_unter_wortgrenzen_eindeutig(text, erwartet):
    assert finde(text) == erwartet, text


def test_kein_alias_gehoert_zwei_laendern():
    """Ausser den bewusst doppelt gefuehrten Codes desselben Landes."""
    von = {}
    for iso, al in ALIASSE.items():
        for a in al:
            von.setdefault(normalisiere(a), set()).add(iso)
    mehrdeutig = {a: sorted(i) for a, i in von.items() if len(i) > 1}
    assert all(set(i) == {"RKS", "XKX"} for i in mehrdeutig.values()), mehrdeutig


def test_doppelte_codes_werden_von_erlaubt_aufgeloest():
    """RKS und XKX sind beide Kosovo — die Datensätze im Projekt führen
    unterschiedliche Kürzel. Ein Konnektor muss den treffen, den SEINE Daten
    haben; ohne Einschränkung gewinnt der ISO-3166-Code."""
    assert finde("Pressefreiheit im Kosovo") == ["XKX"]
    assert finde("Pressefreiheit im Kosovo", frozenset({"RKS"})) == ["RKS"]
    assert finde("Pressefreiheit im Kosovo", frozenset({"XKX"})) == ["XKX"]


def test_erlaubt_verhindert_zusagen_ohne_daten():
    """„Europäische Union" steht im gemeinsamen Verzeichnis (OECD führt
    EU-Aggregate), RSF hat dazu nichts. Der Konnektor darf sie nicht melden —
    sonst bekommt der Nutzer „keine Daten" statt „nicht zuständig"."""
    assert "EUR" not in _rsf_laender()
    assert finde("Pressefreiheit in der Europäischen Union") == ["EUR"]
    assert _find_countries(
        {"claim": "Pressefreiheit in der Europäischen Union"}) == []


def test_max_n_wird_eingehalten():
    text = "Deutschland, Frankreich, Italien, Spanien und Polen"
    assert len(finde(text, max_n=3)) == 3
    assert len(finde(text, max_n=5)) == 5


def test_aus_analyse_nimmt_ner_zuerst():
    treffer = aus_analyse({"ner_entities": {"countries": ["Kenia"]},
                           "claim": "Wie frei ist die Presse dort?"})
    assert treffer == ["KEN"]


def test_leerer_text():
    assert finde("") == [] and finde(None or "") == []
    assert aus_analyse({}) == []


# --------------------------------------------------------------------------
# Der Datenfehler, der dabei auffiel
# --------------------------------------------------------------------------

def test_keine_ersatzzeichen_in_den_laendernamen():
    """`CIV` und `TUR` trugen ein U+FFFD: der Scraper hat die Quelle falsch
    dekodiert, und der Schaden war in der Datei festgeschrieben. Ohne den
    englischen Namen ist ein Land über genau diesen Weg nicht erreichbar."""
    roh = (BACKEND / "data" / "rsf.json").read_text(encoding="utf-8")
    assert "�" not in roh, "Ersatzzeichen in data/rsf.json"
    bc = _rsf_laender()
    assert bc["CIV"]["country_en"] == "Côte d'Ivoire"
    assert bc["TUR"]["country_en"] == "Türkiye"
