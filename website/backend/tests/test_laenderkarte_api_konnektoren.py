"""`transparency`, `wgi` und `idea` erreichten ein Fünftel ihrer Datensätze.

Fortsetzung von #154, wo `rsf` an das gemeinsame Länder-Verzeichnis kam. Die
drei hier sind **Live-API-Konnektoren** — sie halten keine Datei, sondern
fragen bei jedem Claim ab. Damit ist die Länderkarte das **einzige Tor**: was
dort fehlt, ist nicht „langsam" oder „ungenau", sondern unerreichbar.

Gemessen gegen das echte Universum der jeweiligen Quelle (OWID-CSV für CPI und
Wahlbeteiligung, Weltbank-Länderliste für WGI):

    transparency    40/181   22 %   ->   180/181   99 %
    idea            44/172   25 %   ->   171/172   99 %
    wgi             44/217   20 %   ->   216/217   99 %

Das Verzeichnis aus #154 trug davon schon 94/97/81 % — es fehlten nur 41
Länder, überwiegend Inselstaaten und abhängige Gebiete, die die Weltbank als
eigene Economies führt.

ZWEI BEFUNDE BEIM UMBAU
-----------------------
1. **`usa` und `uk` fielen durch die Vier-Zeichen-Regel.** Das
   Regressions-Gate hat es sofort gemeldet: die häufigste Schreibweise
   überhaupt wäre stillschweigend verschwunden. Beide sind unter der
   Wortgrenzen-Prüfung eindeutig und stehen jetzt als **gemessene** Ausnahme
   drin — nicht als aufgeweichte Regel. Ein Blanko-Minimum von zwei Zeichen
   hätte „at" (Österreich, aus einer der 24 Karten) zurückgeholt, und das
   steckt in „at the".

2. **Blosses „Kongo" fand keines der beiden Kongos.** In #154 hatte ich den
   Alias bewusst weggelassen, weil es zwei Staaten gibt — und damit die
   häufigste deutsche Schreibweise gekostet. Jetzt zeigt er auf die DR Kongo
   (rund zwanzigmal so gross); die Ausgabe nennt das Land beim vollen Namen,
   die Wahl bleibt sichtbar und korrigierbar. Nichts zu liefern ist keine
   neutrale Option, sondern auch eine Entscheidung.

`erlaubt=` schränkt auf die Codes ein, zu denen der geladene Datensatz Werte
hat. Bei `wgi` bleibt es bewusst weg: die Weltbank wird pro Land abgefragt,
es gibt kein vorab geladenes Universum.
"""

import importlib
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._laender import ALIASSE, finde  # noqa: E402

KONNEKTOREN = ("transparency", "idea", "wgi")


def _modul(name):
    return importlib.import_module(f"services.{name}")


# --------------------------------------------------------------------------
# Der Befund
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", KONNEKTOREN)
def test_karte_kommt_aus_dem_gemeinsamen_verzeichnis(name):
    """Vorher 40/44/44 Länder je Konnektor, jede Karte von Hand gepflegt."""
    karte = _modul(name).COUNTRY_MAP
    assert len(set(karte.values())) == len(ALIASSE) >= 224, name
    assert karte["südafrika"] == "ZAF"


@pytest.mark.parametrize("name,laender", [
    ("transparency", ("Südafrika", "Kamerun", "Argentinien", "Australien",
                      "Bangladesch", "Suedafrika")),
    ("idea", ("Namibia", "Uruguay", "Mongolei", "Senegal", "Jamaika")),
    ("wgi", ("Ruanda", "Bolivien", "Nepal", "Sambia", "Katar")),
])
def test_laender_die_vorher_unerreichbar_waren(name, laender):
    finden = _modul(name)._find_countries
    for land in laender:
        treffer = finden({"claim": f"Wie steht {land} im Vergleich da?"})
        assert treffer and treffer[0] == ALIASSE_CODE(land), (name, land, treffer)


def ALIASSE_CODE(land):
    """ISO3 zum deutschen Namen — über dasselbe Verzeichnis, das geprüft wird.
    Bewusst kein zweites Mapping im Test: eine Zahl in zwei Kopien driftet."""
    treffer = finde(f"Bericht ueber {land} heute")
    assert treffer, land
    return treffer[0]


# --------------------------------------------------------------------------
# Das Regressions-Gate, das die usa/uk-Lücke gefangen hat
# --------------------------------------------------------------------------

ALTE_ALIASSE = {
    "transparency": ("österreich", "deutschland", "schweiz", "frankreich",
                     "italien", "spanien", "usa", "russland", "china",
                     "ungarn", "polen", "dänemark", "türkei"),
    "idea": ("österreich", "deutschland", "schweiz", "schweden", "usa",
             "frankreich", "belgien", "australien", "griechenland"),
    "wgi": ("österreich", "deutschland", "schweiz", "usa", "uk",
            "grossbritannien", "russland", "china", "singapur"),
}


@pytest.mark.parametrize("name", KONNEKTOREN)
def test_kein_alias_der_alten_karte_verloren(name):
    """Der Kern des Gates. Genau diese Prüfung hat gemeldet, dass „usa" und
    „uk" durch die Vier-Zeichen-Regel gefallen waren."""
    finden = _modul(name)._find_countries
    verloren = [a for a in ALTE_ALIASSE[name]
                if not finden({"claim": f"Bericht ueber {a} heute"})]
    assert not verloren, f"{name}: {verloren}"


@pytest.mark.parametrize("kurz,erwartet", [("usa", "USA"), ("uk", "GBR")])
def test_kurzformen_treffen_weiter(kurz, erwartet):
    for name in KONNEKTOREN:
        finden = _modul(name)._find_countries
        assert finden({"claim": f"Korruption in {kurz} 2026"})[:1] == [erwartet], name


# --------------------------------------------------------------------------
# Der Kongo-Fall
# --------------------------------------------------------------------------

def test_blosses_kongo_liefert_die_dr_kongo():
    """In #154 lieferte „Kongo" NICHTS, weil der Alias wegen der Zweideutigkeit
    fehlte — und kostete damit die häufigste deutsche Schreibweise. Nichts zu
    liefern ist keine neutrale Option."""
    assert finde("Korruption im Kongo") == ["COD"]
    assert finde("die Demokratische Republik Kongo") == ["COD"]
    assert finde("die Republik Kongo") == ["COG"]
    assert finde("Kongo-Brazzaville") == ["COG"]


# --------------------------------------------------------------------------
# `erlaubt` — nur zusagen, was der Datensatz hergibt
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", ("transparency", "idea"))
def test_erlaubt_wird_am_aufrufort_gesetzt(name):
    """Beide laden die Daten VOR der Länder-Erkennung — dann muss die
    Einschränkung auch gesetzt werden, sonst meldet der Konnektor ein Land,
    zu dem er nichts hat."""
    quelle = (BACKEND / "services" / f"{name}.py").read_text(encoding="utf-8")
    assert "_find_countries(analysis, erlaubt=frozenset(data))" in quelle


def test_wgi_bleibt_bewusst_ohne_erlaubt():
    """Die Weltbank wird PRO LAND abgefragt; ein vorab geladenes Universum
    gibt es nicht. Der Grund steht als Kommentar am Aufrufort, damit ihn
    niemand versehentlich „nachrüstet"."""
    quelle = (BACKEND / "services" / "wgi.py").read_text(encoding="utf-8")
    assert "Kein `erlaubt`: die Weltbank wird PRO LAND abgefragt" in quelle
    assert "_find_countries(analysis)\n" in quelle


@pytest.mark.parametrize("name", KONNEKTOREN)
def test_erlaubt_filtert_wirklich(name):
    finden = _modul(name)._find_countries
    assert finden({"claim": "Wie steht Kamerun da?"}, erlaubt=frozenset({"AUT"})) == []
    assert finden({"claim": "Wie steht Kamerun da?"},
                  erlaubt=frozenset({"CMR", "AUT"})) == ["CMR"]


# --------------------------------------------------------------------------
# Über-Trigger
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim", (
    "Wie backe ich einen Marmorkuchen?",
    "Der FC Bayern hat gestern gewonnen",
    "Kristalle wachsen in Salzlösungen",
    "Der Beninger Weg in Wien",
    "Ein Tonabnehmer für den Plattenspieler",
    "Die Grenadine im Cocktail",
    "Der Marinesoldat war müde",
))
def test_kein_land_wo_keines_steht(claim):
    """224 Länder statt 44 heisst mehr Gelegenheit für Fehltreffer. Gemessen
    über alle drei: 0 neue Über-Trigger."""
    assert finde(claim) == [], claim
    for name in KONNEKTOREN:
        assert _modul(name)._find_countries({"claim": claim}) == [], (name, claim)
