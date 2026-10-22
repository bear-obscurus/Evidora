"""Derselbe Marker, andere Wortwahl — und 47 Prozent der Werte falsch.

Anlass war eine Kadenz-Frage: ``KADENZ_MAX_AGE`` kannte kein
``halbjaehrlich``, und ``climate_action_tracker.json`` nannte diesen Takt
nur in der Prosa. Beim Nachsehen stand im Fakt:

    audit_flag: STATIC-SNAPSHOT — Werte sind Recherche-Approximationen aus
    oeffentlichen CAT-Country-Pages (Stand November 2024). Vor produktivem
    Live-Einsatz: pro Country-Page gegenpruefen.

Niemand hat gegengeprueft. Es ist dieselbe Klasse wie MIPEX (#266, 52 %
falsch) und BTI (#268, 80 %) — **nur in anderer Wortwahl**, weshalb das
Gate aus #268 sie nicht fand: Es suchte woertlich nach "AUDIT NEEDED".
Meine Aussage dort, der letzte Marker sei abgearbeitet, galt also nur fuer
diese Zeichenkette.

Ein Zensus ueber alle Datendateien fand genau vier Dateien mit solchen
Markern; drei davon (bti, mipex, vdem) sind erledigte Aufzeichnungen, die
ihren frueheren Zustand beschreiben. CAT war der letzte unerledigte.

## Das Ergebnis

    75 von 159 Werten abweichend (47 %)
    nur AUS, SWE und TUR fehlerfrei
    51 Abweichungen um eine Stufe, 20 um zwei, 4 um drei
    schlimmster Fall USA: Overall 'Critically insufficient' statt
    'Insufficient' — zwei Stufen, und die Ziel-Bewertung drei

Dazu war der Stand zwei Jahre alt: ``assessment_date`` stand bei allen 41
Laendern auf ``2024-11``, waehrend CAT im Oktober 2026 aktualisiert hat.

## Drei Fehlversuche beim Lesen, und was sie gelehrt haben

Die Ratings stehen als TEXT im Tile, nicht nur in der Farbe — eine
Farb-Zuordnung haette eine Methodik-Tabelle gebraucht und waere beim
naechsten Design-Wechsel gerissen.

**Erster Versuch:** Labels von der DEUTSCHEN Seite abgeleitet ("National
target against ..."). Ergebnis: Bei 38 von 40 Laendern wurden nur zwei der
vier Ratings gelesen — die meisten Seiten schreiben "NDC target".

**Zweiter Versuch:** beide Varianten aufgezaehlt. 17 Laender blieben
lueckenhaft, weil die Schweiz "NDC target (domestic)" und "NDC target
(full)" schreibt.

**Dritter Versuch:** nicht mehr den Label-Anfang pruefen, sondern den
unterscheidenden Teil — WOGEGEN gemessen wird ("against modelled domestic
pathways", "against fair share"). Varianten aufzaehlen scheitert hier wie
in #140/#141. Danach 39 von 41 vollstaendig.

Dabei kam eine SECHSTE Kategorie zum Vorschein, die unsere
Methodik-Tabelle nicht fuehrte: Norwegens Ziel-Rating lautet "1.5°C global
least cost".

## Die EU-Ebene

Sieben EU-Mitglieder (AUT, ESP, FRA, ITA, NLD, POL, SWE) zeigen auf
DIESELBE Seite — die EU-Seite. CAT bewertet die EU als Einheit; nur
Deutschland hat eine eigene Laenderseite. Der vorige Stand trug fuer diese
sieben trotzdem UNTERSCHIEDLICHE Werte, obwohl alle aus derselben Quelle
stammen muessten.

Aufgefallen ist das an einer Zahl: 35 eindeutige URLs bei 41 Laendern. Die
Werte sind jetzt fuer alle sieben gleich und als ``bewertungs_ebene:
EU-Aggregat`` markiert — und der Marker wird GERENDERT, nicht nur
gespeichert. Genau diese Haelfte war der Befund in #273.

## Zwei eigene Verluste beim Neuschreiben

``tools/feldzugriffe_pruefen.py`` aus #270 hat gemeldet, dass der Service
``source_url`` und ``note`` liest, die ich beim Neuschreiben der Datei
verloren hatte. ``note`` war ein PER-LAND-Feld mit drei Eintraegen, und
einer davon erklaert den Ukraine-Befund: "Bewertung erschwert durch
Kriegssituation seit 2022". Beide wiederhergestellt.

Das Gate, das ich zwei Tage vorher gegen fremde Umbenennungen gebaut
hatte, hat meine eigene gefangen.

Dependency-light: nur Daten, Service und Werkzeug, kein Netz.
"""
import asyncio
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)
sys.path.insert(0, os.path.join(BACKEND, "tools"))

from data_freshness_check import KADENZ_MAX_AGE  # noqa: E402
from cat_aus_laenderseiten import (  # noqa: E402
    ANFANGS_FELDER,
    GEGEN_FELDER,
    KATEGORIEN,
    lies_seite,
)
from services.climate_action_tracker import search_cat  # noqa: E402

#: Die sieben EU-Mitglieder ohne eigene CAT-Laenderseite.
EU_AGGREGAT = ("AUT", "ESP", "FRA", "ITA", "NLD", "POL", "SWE")

#: Am 22.10.2026 von den Laenderseiten gelesen. Stichprobe, die die
#: schwersten Abweichungen des alten Standes festhaelt.
GELESEN = {
    "USA": {"overall_rating": "Critically insufficient",
            "targets_rating": "Critically insufficient"},
    "DEU": {"overall_rating": "Insufficient",
            "current_policies_rating": "Almost sufficient"},
    "NOR": {"targets_rating": "1.5°C global least cost"},
    "PHL": {"targets_rating": "1.5°C global least cost"},
}

#: Werte des alten Standes, die nachweislich falsch waren.
WIDERLEGT = {
    "USA": ("overall_rating", "Insufficient"),
    "VNM": ("fair_share_rating", "Almost sufficient"),
    "KAZ": ("targets_rating", "Critically insufficient"),
}


def _daten():
    with open(os.path.join(DATA, "climate_action_tracker.json"),
              encoding="utf-8") as fh:
        return json.load(fh)


def _ergebnis(claim):
    a = {"claim": claim, "original_claim": claim, "queries": [claim]}
    return asyncio.run(search_cat(a)).get("results") or []


# --- Die Kadenz-Entscheidung -----------------------------------------

def test_halbjaehrlich_steht_im_vokabular():
    """240 Tage, interpoliert zwischen quartalsweise (150) und jaehrlich
    (400). Das Muster der bestehenden Werte: Je laenger der Takt, desto
    kleiner der Sicherheitsfaktor."""
    assert KADENZ_MAX_AGE["halbjaehrlich"] == 240
    assert KADENZ_MAX_AGE["quartalsweise"] < 240 < KADENZ_MAX_AGE["jaehrlich"]


def test_die_schwelle_folgt_dem_muster_der_nachbarn():
    """Ein Wert, der aus der Reihe faellt, waere geraten statt
    interpoliert."""
    zyklen = {"woechentlich": 7, "monatlich": 30, "quartalsweise": 91,
              "halbjaehrlich": 182, "jaehrlich": 365}
    faktoren = {k: KADENZ_MAX_AGE[k] / v for k, v in zyklen.items()}
    # monoton fallend: laengerer Takt -> kleinerer Faktor
    reihe = [faktoren[k] for k in ("woechentlich", "monatlich",
                                   "quartalsweise", "halbjaehrlich",
                                   "jaehrlich")]
    assert reihe == sorted(reihe, reverse=True), faktoren


@pytest.mark.parametrize("datei,takt", [
    ("climate_action_tracker", "halbjaehrlich"),
    ("at_factbook", "jaehrlich"),
    ("pks", "jaehrlich"),
])
def test_die_drei_dateien_erklaeren_ihren_takt(datei, takt):
    """Alle drei nannten ihren Takt nur in der Prosa — der Waechter mass
    gegen den 120-Tage-Standard."""
    with open(os.path.join(DATA, f"{datei}.json"), encoding="utf-8") as fh:
        d = json.load(fh)
    assert d["refresh_kadenz"] == takt
    assert d["refresh_kadenz"] in KADENZ_MAX_AGE
    assert len(d["kadenz_begruendung"]) > 80


# --- Die gelesenen Werte ---------------------------------------------

@pytest.mark.parametrize("iso,felder", sorted(GELESEN.items()))
def test_die_gelesenen_werte_stehen_in_der_datei(iso, felder):
    rec = _daten()["scores"][iso]
    for feld, wert in felder.items():
        assert rec[feld] == wert, f"{iso}.{feld}"


@pytest.mark.parametrize("iso,paar", sorted(WIDERLEGT.items()))
def test_kein_widerlegter_wert_steht_noch_da(iso, paar):
    """Gegenprobe gegen einen halben Refresh."""
    feld, alt = paar
    assert _daten()["scores"][iso].get(feld) != alt


def test_der_marker_ist_abgearbeitet_nicht_geloescht():
    d = _daten()
    aktiv = [k for k, v in d.items()
             if k not in ("audit_flag", "marker_hinweis")
             and "STATIC-SNAPSHOT" in json.dumps(v, ensure_ascii=False)]
    assert not aktiv, aktiv
    assert d["audit_flag"].startswith("GEPRUEFT 2026-10-22")
    assert "47 %" in d["audit_flag"]
    assert "cat_aus_laenderseiten.py" in d["audit_flag"]


def test_die_marker_klasse_ist_benannt():
    """Dass derselbe Marker in anderer Wortwahl durch das Gate aus #268
    fiel, gehoert festgehalten — sonst sucht der naechste wieder nach
    einer Zeichenkette."""
    h = _daten()["marker_hinweis"]
    assert "#266" in h and "#268" in h
    assert "AUDIT NEEDED" in h
    assert "andere" in h.lower()


# --- Das Datum ist ein ABRUF-Datum -----------------------------------

def test_das_datum_heisst_wie_es_gemeint_ist():
    """CAT druckt kein maschinenlesbares Bewertungsdatum. Das frueher
    gefuehrte ``assessment_date`` gab ein Abruf- als Bewertungsdatum
    aus."""
    d = _daten()
    assert d["ratings_gelesen_am"] == "2026-10-22"

    # Geprueft werden SCHLUESSEL, nicht der Volltext: `datums_hinweis`
    # nennt den alten Feldnamen absichtlich, damit der naechste die
    # Umbenennung nachvollziehen kann. Eine erste Fassung suchte im
    # serialisierten JSON und schlug an genau dieser Erklaerung fehl —
    # dieselbe Falle wie in #267, #270 und #272.
    def _schluessel(x):
        if isinstance(x, dict):
            for k, v in x.items():
                yield k
                yield from _schluessel(v)
        elif isinstance(x, list):
            for v in x:
                yield from _schluessel(v)

    assert "assessment_date" not in set(_schluessel(d))
    assert "Abrufdatum" in d["datums_hinweis"]


def test_der_service_erfindet_kein_jahr():
    """Drei Literale: "2024", "2024-11" und report_year=2024 (#74/#133)."""
    with open(os.path.join(BACKEND, "services",
                           "climate_action_tracker.py"), encoding="utf-8") as fh:
        q = fh.read()
    for literal in ('or "2024"', 'or "2024-11"', 'report_year", 2024'):
        assert literal not in q, literal
    assert 'data.get("ratings_gelesen_am")' in q


def test_die_ausgabe_nennt_den_abruf_nicht_eine_bewertung():
    res = _ergebnis("Wie bewertet CAT Deutschland?")
    assert res
    name = res[0]["indicator_name"]
    assert "Ratings gelesen 2026-10-22" in name
    assert "Stand 2024" not in name


# --- Die EU-Ebene ----------------------------------------------------

@pytest.mark.parametrize("iso", EU_AGGREGAT)
def test_die_eu_mitglieder_sind_als_aggregat_markiert(iso):
    rec = _daten()["scores"][iso]
    assert rec["bewertungs_ebene"] == "EU-Aggregat"
    assert "EU als EINE Einheit" in rec["ebenen_hinweis"]


def test_die_eu_mitglieder_tragen_dieselben_werte():
    """Der vorige Stand trug fuer die sieben UNTERSCHIEDLICHE Werte,
    obwohl alle auf dieselbe Seite zeigen. Aufgefallen an 35 eindeutigen
    URLs bei 41 Laendern."""
    sc = _daten()["scores"]
    ratings = {json.dumps({k: v for k, v in sc[i].items()
                           if k.endswith("_rating")}, sort_keys=True)
               for i in EU_AGGREGAT}
    assert len(ratings) == 1, ratings
    urls = {sc[i]["url"] for i in EU_AGGREGAT}
    assert len(urls) == 1 and urls.pop().endswith("/eu/")


def test_deutschland_hat_eine_eigene_bewertung():
    """Gegenprobe: Nicht jedes EU-Land haengt am Aggregat."""
    de = _daten()["scores"]["DEU"]
    assert "bewertungs_ebene" not in de
    assert de["url"].endswith("/germany/")


def test_der_ebenen_marker_wird_gerendert():
    """Ein Marker in den Daten, den kein Renderer liest, ist nicht im
    Produkt — der Befund aus #273."""
    at = _ergebnis("Wie bewertet CAT Österreich?")
    de = _ergebnis("Wie bewertet CAT Deutschland?")
    assert at and de
    assert "EU-Aggregat" in at[0]["indicator_name"]
    assert "EU-Aggregat" not in de[0]["indicator_name"]


# --- Was NICHT gelesen werden konnte ---------------------------------

def test_die_ukraine_traegt_keine_erfundenen_werte():
    """Lieber keine Werte als die Approximationen von 2024."""
    ukr = _daten()["scores"]["UKR"]
    assert not [k for k in ukr if k.endswith("_rating")]
    assert "Kriegssituation" in ukr["nicht_gelesen"]
    assert ukr["url"].endswith("/ukraine/")


def test_die_per_land_notizen_sind_erhalten():
    """Mein Neuschreiben hatte sie verloren; das Gate aus #270 hat es
    gemeldet, weil der Service ``rec["note"]`` liest."""
    sc = _daten()["scores"]
    mit = [i for i in sc if sc[i].get("note")]
    assert sorted(mit) == ["AUT", "FRA", "UKR"], mit


def test_die_quellen_url_ist_erhalten():
    """Ebenfalls beim Neuschreiben verloren und vom Gate gemeldet."""
    assert _daten()["source_url"].startswith(
        "https://climateactiontracker.org")


# --- Das Werkzeug ----------------------------------------------------

def test_die_zuordnung_prueft_den_unterscheidenden_teil():
    """Varianten aufzaehlen scheiterte zweimal (#140/#141). Stabil ist,
    WOGEGEN gemessen wird."""
    marker = [m for m, _f in GEGEN_FELDER]
    assert "against fair share" in marker
    assert "against modelled domestic pathways" in marker


def test_policies_wird_vor_dem_ziel_rating_geprueft():
    """"Policies and action" wird ebenfalls gegen die domestic pathways
    gemessen — ohne diese Reihenfolge landet sie im Ziel-Feld."""
    anfaenge = [a for a, _f in ANFANGS_FELDER]
    assert "policies and action against modelled domestic pathways" in anfaenge
    assert anfaenge.index("overall rating") == 0


def test_die_sechste_kategorie_ist_erfasst():
    """Norwegens Ziel-Rating lautet "1.5°C global least cost" — in
    unserer Methodik-Tabelle fehlte die Stufe."""
    assert "1.5°C global least cost" in KATEGORIEN
    assert "1.5C_global_least_cost" in _daten()["rating_scale"]


def test_die_laengste_kategorie_gewinnt():
    """"Insufficient" steckt in "Highly insufficient" und "Critically
    insufficient" — dieselbe Teilstring-Falle wie #231."""
    i_lang = KATEGORIEN.index("Critically insufficient")
    i_kurz = KATEGORIEN.index("Insufficient")
    assert i_lang < i_kurz, KATEGORIEN


def test_fremde_tiles_belegen_kein_rating_feld():
    """"Net zero target: Poor" und "Land use: Sink" haben eigene Skalen.

    Mit den heutigen Woertern faellt das schon auf, weil "Poor" und "Sink"
    nicht im Kategorie-Vokabular stehen — die Giftprobe zu FREMDE_TILES
    HIELT deshalb zunaechst. Der Fall, in dem die Sperre wirklich traegt,
    ist ein fremdes Tile MIT einer CAT-Kategorie: "Climate finance against
    fair share Insufficient" wuerde ueber den Marker "against fair share"
    das Ziel-Feld belegen. Deshalb steht er hier mit.
    """
    html = (
        '<div class="ratings-matrix__tile--ORANGE"><dt>Overall rating</dt>'
        ' Insufficient</div>'
        '<div class="ratings-matrix__tile--RED"><dt>Net zero target</dt>'
        ' Poor</div>'
        '<div class="ratings-matrix__tile--GREEN"><dt>Land use &amp;'
        ' forestry</dt> Sink</div>'
        '<div class="ratings-matrix__tile--ORANGE"><dt>Climate finance'
        ' against fair share</dt> Critically insufficient</div>')
    gelesen = lies_seite(html)
    assert gelesen == {"overall_rating": "Insufficient"}, gelesen
    assert "fair_share_rating" not in gelesen, (
        "ein Climate-finance-Tile hat das Ziel-Feld belegt")


def test_eine_seite_ohne_overall_rating_gilt_als_unlesbar(tmp_path):
    """Sie als "keine Abweichung" zu zaehlen waere der stille Fehlschluss
    (gleiche Pruefung wie in tools/mipex_aus_archiv.py).

    Geprueft wird ueber ``main``, nicht nur ueber ``lies_seite``: Die
    Pruefung steht im Aufrufer, und eine erste Fassung dieses Tests konnte
    sie deshalb nicht verlieren — die Giftprobe HIELT.
    """
    import cat_aus_laenderseiten as werkzeug
    assert lies_seite("<html><body>Fehlerseite</body></html>") == {}

    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "XXX.html").write_text("<html>Fehlerseite</html>", encoding="utf-8")
    fakt = tmp_path / "cat.json"
    fakt.write_text(json.dumps({"scores": {"XXX": {
        "iso3": "XXX", "country_en": "Test",
        "overall_rating": "Insufficient",
        "url": "https://example.test/countries/xxx/"}}}), encoding="utf-8")
    rc = werkzeug.main(["--cache", str(cache), "--cat-json", str(fakt),
                        "--leise"])
    assert rc in werkzeug.BEFUND_EXITS, (
        "eine unlesbare Seite muss einen Befund-Exit geben, nicht 0")


def test_das_werkzeug_meldet_befunde_ueber_den_exit_code():
    with open(os.path.join(BACKEND, "tools", "cat_aus_laenderseiten.py"),
              encoding="utf-8") as fh:
        q = fh.read()
    assert "BEFUND_EXITS" in q
    assert "return 1 if (ab_gesamt or unlesbar) else 0" in q


def test_das_schema_nennt_seine_vereinfachung():
    """CAT publiziert bis zu sechs Ratings, wir fuehren vier. Das ist eine
    Vereinfachung, kein Lesefehler — und es gehoert dazugesagt."""
    h = _daten()["schema_hinweis"]
    assert "VIER" in h and "SECHS" in h
    assert "against fair share" in h
