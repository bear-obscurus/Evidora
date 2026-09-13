"""Ein genannter Ort ohne Daten bekommt nichts — kein Ersatzland.

Anlass (QA50F-Nachgang, 2026-09-13): „In Nordkorea gibt es keine freien
Wahlen" bekam von Freedom House „Austria: 94/100 (Free)". Das Verdict stimmte,
die Summary nannte „Freedom House bewertet Nordkorea mit 0/100" — eine Zahl,
die in keiner unserer Dateien steht. Der Konnektor hatte Oesterreich geliefert.

Nachgemessen ueber alle Laender-Konnektoren: derselbe Fehler in zwoelf, in
drei Varianten.

    DACH/Oesterreich statt des Landes   freedom_house, polity5, vdem, cat,
                                        wid, rsf, transparency, idea, wgi
    Top/Bottom-Uebersicht statt Land    mipex (Nordkorea -> Schweden),
                                        bti (Laos -> Taiwan)
    EU-Durchschnitt statt Land          easie

Zwei Faelle waren schlimmer als Oesterreich: `wid` machte aus Nordkorea
SUEDkorea („korea" steckt in „nordkorea"), und `vdem` hat Werte fuer 179
Laender, erkannte aber nur 32 — Eritrea, Laos und Nordkorea bekamen
Oesterreich, obwohl ihre Zahlen im Datensatz stehen.

Die gemeinsame Ursache war EINE Regel: „kein bekanntes Land erkannt ->
Default". Richtig ist: nur ein Claim OHNE Ortsangabe bekommt den Default.

Beim Bauen fiel ein zweiter Fehler in `_laender.finde` selbst auf: mit
`erlaubt` wurde ein gesperrter Alias uebersprungen, ohne seine Fundstelle zu
verbrauchen. „North Korea" ohne PRK lieferte dann KOR. 29 solcher Lecks.

Kein Test hier geht ins Netz: `wgi`, `transparency` und `idea` werden auf
Ebene der Laender-Auswahl geprueft, nicht ueber ihren Abruf.
"""

import asyncio
import importlib
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services import _laender  # noqa: E402
from services._laender import REGIONEN, finde, zustaendigkeit  # noqa: E402

# Konnektor -> (Suchfunktion, Satz mit Ort, Satz ohne Ort)
STATISCH = {
    "freedom_house": ("search_freedom_house",
                      "In {} gibt es keine freien Wahlen",
                      "Es gibt keine freien Wahlen"),
    "polity5": ("search_polity5",
                "Der Polity-Score von {} ist niedrig",
                "Der Polity-Score ist niedrig"),
    "vdem": ("search_vdem",
             "Die Demokratie in {} ist schwach",
             "Die Demokratie ist schwach"),
    "climate_action_tracker": ("search_cat",
                               "Der Climate Action Tracker bewertet {} als unzureichend",
                               "Der Climate Action Tracker bewertet die Klimapolitik als unzureichend"),
    "easie": ("search_easie",
              "Die inklusive Bildung in {} ist schlecht",
              "Die inklusive Bildung ist schlecht"),
    "wid": ("search_wid",
            "Laut World Inequality Database ist die Ungleichheit in {} hoch",
            "Laut World Inequality Database ist die Ungleichheit hoch"),
    "mipex": ("search_mipex",
              "Laut MIPEX ist die Integrationspolitik in {} schwach",
              "Laut MIPEX ist die Integrationspolitik schwach"),
    "bti": ("search_bti",
            "Laut BTI ist die Transformation in {} schwach",
            "Laut BTI ist die Transformation schwach"),
    "rsf": ("search_rsf",
            "Die Pressefreiheit in {} ist gut",
            "Die Pressefreiheit ist gut"),
}


def _suche(mod, claim):
    fn = getattr(importlib.import_module(f"services.{mod}"), STATISCH[mod][0])
    return (asyncio.run(fn({"claim": claim, "original_claim": claim})) or {}).get("results") or []


def _laender_von(res):
    return [str(r.get("country") or "") for r in res]


# --------------------------------------------------------------------------
# Die Regel, an jedem statischen Konnektor
# --------------------------------------------------------------------------

@pytest.mark.parametrize("mod", sorted(STATISCH))
def test_genannter_ort_ohne_daten_bekommt_nichts(mod):
    """Tuvalu steht in keinem der neun Datensaetze. Vorher bekam es AT, AT/DE/CH,
    Schweden, Taiwan oder den EU-Durchschnitt."""
    res = _suche(mod, STATISCH[mod][1].format("Tuvalu"))
    assert res == [], f"{mod}: Ersatzland statt nichts: {_laender_von(res)}"


@pytest.mark.parametrize("mod,land,erwartet", [
    ("freedom_house", "Ungarn", "HU"),
    ("polity5", "Ungarn", "HU"),
    ("vdem", "Ungarn", "HU"),
    ("climate_action_tracker", "Deutschland", "DE"),
    ("easie", "Ungarn", "HU"),
    ("wid", "Ungarn", "HU"),
    ("mipex", "Ungarn", "HU"),
    ("bti", "Ungarn", "HU"),
    ("rsf", "Deutschland", "DEU"),
])
def test_erfasstes_land_bleibt_erreichbar(mod, land, erwartet):
    res = _suche(mod, STATISCH[mod][1].format(land))
    assert res and _laender_von(res)[0] == erwartet, (mod, _laender_von(res))


@pytest.mark.parametrize("mod,erwartet", [
    ("freedom_house", ["AT"]),
    ("polity5", ["AT"]),
    ("vdem", ["AT"]),
    ("climate_action_tracker", ["AT", "DE", "CH"]),
    ("easie", ["EU"]),
    ("wid", ["AT"]),
    ("rsf", ["AUT", "DEU"]),
])
def test_ohne_ortsangabe_bleibt_der_default(mod, erwartet):
    """Die Gegenprobe: der Default ist fuer Claims ohne Ort gedacht und bleibt."""
    res = _suche(mod, STATISCH[mod][2])
    assert _laender_von(res)[:len(erwartet)] == erwartet, (mod, _laender_von(res))


@pytest.mark.parametrize("mod", ["mipex", "bti"])
def test_ohne_ortsangabe_bleibt_die_uebersicht(mod):
    assert _suche(mod, STATISCH[mod][2]), mod


# --------------------------------------------------------------------------
# Die ausloesenden und die schlimmeren Faelle
# --------------------------------------------------------------------------

def test_nordkorea_bekommt_von_freedom_house_kein_oesterreich():
    assert _suche("freedom_house", "In Nordkorea gibt es keine freien Wahlen") == []


def test_wid_macht_aus_nordkorea_kein_suedkorea():
    res = _suche("wid", "Laut World Inequality Database ist die Ungleichheit in Nordkorea hoch")
    assert "KR" not in _laender_von(res), _laender_von(res)
    assert res == []


def test_wid_markenname_ist_kein_claim_ueber_die_welt():
    """„World Inequality Database" enthaelt „world" — ohne Bereinigung waere
    jeder WID-Claim ein Claim ueber die Welt."""
    res = _suche("wid", STATISCH["wid"][2])
    assert "WLD" not in _laender_von(res), _laender_von(res)


def test_wid_weltweit_bleibt_die_welt():
    """WID fuehrt Welt- und Europa-Werte — dort ist die Region ein gueltiges Ziel."""
    res = _suche("wid", "Laut World Inequality Database ist die Ungleichheit weltweit hoch")
    assert res and _laender_von(res)[0] == "WLD", _laender_von(res)


def test_vdem_erreicht_die_laender_ausserhalb_der_alias_liste():
    """179 Laender mit Werten, 32 in der Alias-Liste. Eritrea war unerreichbar."""
    res = _suche("vdem", "Die Demokratie in Eritrea ist schwach")
    assert res, "Eritrea liefert nichts"
    assert all(r["country"] == "ERI" for r in res), _laender_von(res)


def test_vdem_beschriftet_nicht_mit_fremden_kuerzeln():
    """iso3[:2] machte aus Chile „CH" (Schweiz), aus der Ukraine „UK"."""
    for land, falsch, richtig in (("Chile", "CH", "CHL"), ("Ukraine", "UK", "UKR"),
                                  ("Benin", "BE", "BEN")):
        res = _suche("vdem", f"Die Demokratie in {land} ist schwach")
        assert res, land
        name = res[0]["indicator_name"]
        assert f" {richtig} " in name and f" {falsch} " not in name, name


def test_vdem_indikator_ohne_das_land_faellt_weg():
    """Ein Land im Datensatz, das in EINEM Indikator fehlt, bekam frueher dessen
    Zeile mit Oesterreichs Wert."""
    from services.vdem import _select_primary_country
    assert _select_primary_country(["ERI"], {"AUT": {"2025": 0.8}}) is None


def test_easie_eu_bekommt_den_eu_durchschnitt():
    for claim in ("Die inklusive Bildung in der EU ist schlecht",
                  "Die inklusive Bildung in Europa ist schlecht"):
        res = _suche("easie", claim)
        assert _laender_von(res) == ["EU"], (claim, _laender_von(res))


def test_bti_bewertet_oesterreich_nicht_und_liefert_nichts():
    """BTI deckt keine OECD-Staaten ab. Vorher kam Taiwan + Eritrea."""
    assert _suche("bti", "Laut BTI ist die Transformation in Österreich schwach") == []


def test_cat_ungarn_bekommt_kein_dach():
    """CAT bewertet die EU als Ganzes, Ungarn nicht einzeln. Vorher: AT/DE/CH."""
    assert _suche("climate_action_tracker",
                  "Der Climate Action Tracker bewertet Ungarn als unzureichend") == []


# --------------------------------------------------------------------------
# Der Helfer
# --------------------------------------------------------------------------

def test_zustaendigkeit_mit_daten():
    assert zustaendigkeit({"claim": "Wahlen in Ungarn"}, frozenset({"HUN"})) == (["HUN"], [])


def test_zustaendigkeit_ohne_daten():
    assert zustaendigkeit({"claim": "Wahlen in Tuvalu"}, frozenset({"HUN"})) == ([], ["TUV"])


def test_zustaendigkeit_ohne_ortsangabe():
    assert zustaendigkeit({"claim": "Wahlen sind wichtig"}, frozenset({"HUN"})) == ([], [])


def test_ein_beantwortbarer_ort_genuegt():
    """Nennt der Claim einen Ort mit und einen ohne Daten, wird der erste
    beantwortet — der zweite ist kein Grund zu schweigen."""
    mit, ohne = zustaendigkeit({"claim": "Ungarn und Tuvalu"}, frozenset({"HUN"}))
    assert mit == ["HUN"] and ohne == []


def test_entities_und_ner_zaehlen_mit():
    mit, _ = zustaendigkeit({"claim": "Wie steht es dort?", "entities": ["Ungarn"]},
                            frozenset({"HUN"}))
    assert mit == ["HUN"]
    mit, _ = zustaendigkeit({"claim": "Wie steht es dort?",
                             "ner_entities": {"countries": ["Ungarn"]}}, frozenset({"HUN"}))
    assert mit == ["HUN"]


def test_entities_lassen_sich_abschalten():
    """Die flache Entity-Liste des Analyzers kann halluzinierte Eintraege
    enthalten (siehe unhcr.py). Konnektoren, die sie bisher nicht lasen,
    lesen sie auch jetzt nicht."""
    analyse = {"claim": "Wie steht es um die Pressefreiheit?", "entities": ["Tuvalu"]}
    assert zustaendigkeit(analyse, frozenset({"HUN"})) == ([], ["TUV"])
    assert zustaendigkeit(analyse, frozenset({"HUN"}), entities=False) == ([], [])


@pytest.mark.parametrize("mod", ["rsf", "transparency", "idea", "wgi"])
def test_aus_analyse_konnektoren_lesen_keine_entities(mod):
    """Vor der Regel lasen sie ueber `aus_analyse` nur NER und Claim."""
    quelle = (BACKEND / "services" / f"{mod}.py").read_text(encoding="utf-8")
    aufrufe = [z for z in quelle.splitlines() if "_LAENDER.zustaendigkeit(" in z]
    assert aufrufe and all("entities=False" in z for z in aufrufe), (mod, aufrufe)


def test_bereinigen_wirkt_auf_jeden_text():
    mit, ohne = zustaendigkeit(
        {"claim": "world inequality database", "entities": ["World Inequality Database"]},
        frozenset({"WLD"}), bereinigen=lambda t: t.lower().replace("world inequality database", " "))
    assert (mit, ohne) == ([], [])


def test_ohne_erlaubt_gibt_es_kein_ohne_daten():
    assert zustaendigkeit({"claim": "Wahlen in Tuvalu"}, None) == (["TUV"], [])


def test_kosovo_bleibt_ueber_beide_codes_erreichbar():
    for code in ("RKS", "XKX"):
        assert zustaendigkeit({"claim": "Pressefreiheit im Kosovo"}, frozenset({code})) == ([code], [])


# --------------------------------------------------------------------------
# Das Leck in `finde`
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,erlaubt", [
    ("North Korea", {"KOR"}),
    ("DVR Korea", {"KOR"}),
    ("South Sudan", {"SDN"}),
    ("Guinea-Bissau", {"GIN"}),
    ("Papua New Guinea", {"GIN"}),
    ("Republic of China", {"CHN"}),
    ("Demokratische Republik Kongo", {"COG"}),
])
def test_gesperrter_langer_alias_verbraucht_seine_fundstelle(text, erlaubt):
    """Vorher traf der kuerzere Alias eines ANDEREN Landes in der Fundstelle
    des gesperrten laengeren — 29 solcher Lecks ueber die ganze Tabelle."""
    assert finde(text, frozenset(erlaubt)) == [], (text, finde(text, frozenset(erlaubt)))


def test_kurzer_alias_trifft_weiterhin_allein():
    assert finde("Südkorea", frozenset({"KOR"})) == ["KOR"]
    assert finde("Korea", frozenset({"KOR"})) == ["KOR"]
    assert finde("Sudan", frozenset({"SDN"})) == ["SDN"]


# --------------------------------------------------------------------------
# Die Live-Konnektoren: Auswahl, nicht Abruf
# --------------------------------------------------------------------------

def test_wgi_schickt_keine_regionen_an_die_weltbank():
    """„EU" und „weltweit" gingen als EUR/WLD an die API und kamen als
    „Invalid value" zurueck. Jetzt sind sie „nicht zustaendig"."""
    erlaubt = frozenset(_laender.ALIASSE) - REGIONEN
    for claim, region in (("Governance in der EU", "EUR"), ("Governance weltweit", "WLD")):
        assert zustaendigkeit({"claim": claim}, erlaubt) == ([], [region]), claim


@pytest.mark.parametrize("mod", ["transparency", "idea", "wgi", "rsf"])
def test_live_und_cache_konnektoren_nutzen_die_regel(mod):
    """Eine gruene Suite beweist nichts ueber ungerufenen Code."""
    quelle = (BACKEND / "services" / f"{mod}.py").read_text(encoding="utf-8")
    assert "_LAENDER.zustaendigkeit(" in quelle, mod


@pytest.mark.parametrize("mod", ["freedom_house", "polity5", "vdem"])
def test_kein_oesterreich_rueckfall_mehr_im_code(mod):
    quelle = (BACKEND / "services" / f"{mod}.py").read_text(encoding="utf-8")
    assert 'return "AUT"' not in quelle, mod
