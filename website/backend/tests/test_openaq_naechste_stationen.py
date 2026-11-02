"""`order_by=distance` gibt es bei OpenAQ nicht mehr — und der 401 hat es
sieben Wochen verdeckt.

Nachdem #296 das versprengte Zeichen aus dem Schluessel entfernt hatte,
kam HTTP 422 zum Vorschein. Die API sagt genau, warum:

    {'type': 'enum', 'loc': ('query', 'order_by'),
     'msg': "Input should be 'id'", 'input': 'distance'}

Gemessen am 2026-11-02 gegen die echte API: Dieselbe Abfrage OHNE
``order_by``/``sort_order`` liefert HTTP 200 und **50 Locations**
(``meta.found=">50"``) im 25-km-Radius um Wien, **alle 50 mit
Koordinaten**.

⭐ Ein Ausfall verdeckt den naechsten (#237). Der 401 war die Reparatur
von #296; dahinter lag ein zweiter Befund, der erst sichtbar wurde, als
die Authentifizierung wieder durchlief.

## Warum die Sortierung bei UNS passieren muss

Sortieren nach Naehe war der ZWECK des Parameters. Ohne ihn liefert die
API eine Liste in API-Reihenfolge — und das alte ``results[:5]`` haette
dann fuenf beliebige Stationen genommen, nicht die naechsten. Deshalb:

* ``LOCATIONS_ROHLIMIT = 50`` statt ``limit=5`` — erst holen, dann
  sortieren, dann kuerzen.
* ``_naechste_fuenf`` sortiert nach quadriertem Gradabstand mit
  Breitengrad-Korrektur. Keine Geodaesie: Fuer einen Vergleich innerhalb
  von 50 km genuegt das, und ein Vergleich ist alles, was die
  Sortierung braucht.

Gegen die echten 50 Wiener Stationen ergibt das Stephansplatz
(48.20815 gegen unsere 48.2082), dann Taborstrasse, dann Krafftgasse —
die tatsaechlich naechsten.

Dependency-light: nur die Sortierfunktion und die Query-Form, kein Netz.
"""
import inspect
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services.openaq import (  # noqa: E402
    LOCATIONS_ROHLIMIT,
    _fetch_locations_near,
    _naechste_fuenf,
)

WIEN = (48.2082, 16.3738)


def _ort(name, lat=None, lon=None):
    if lat is None:
        return {"name": name}
    return {"name": name, "coordinates": {"latitude": lat, "longitude": lon}}


# --------------------------------------------------- die Sortierung selbst

def test_die_naechste_station_kommt_zuerst():
    roh = [_ort("fern", 48.9, 16.9), _ort("nah", 48.21, 16.38),
           _ort("mittel", 48.4, 16.5)]
    assert [e["name"] for e in _naechste_fuenf(roh, *WIEN)] == \
        ["nah", "mittel", "fern"]


def test_die_echten_wiener_stationen_in_der_richtigen_reihenfolge():
    """Werte aus der echten Antwort vom 2026-11-02."""
    roh = [
        _ort("Krafftgasse", 48.22243365718118, 16.373341834689935),
        _ort("Wien Taborstrasse", 48.21806464890474, 16.381947015788832),
        _ort("Stephansplatz", 48.208149999712305, 16.373253999919783),
        _ort("Taborstraße", 48.216738999711964, 16.38091800012245),
        _ort("Wien Stephansplatz", 48.20890608925811, 16.373862178231754),
    ]
    assert [e["name"] for e in _naechste_fuenf(roh, *WIEN)][:2] == \
        ["Stephansplatz", "Wien Stephansplatz"]


def test_station_ohne_koordinaten_landet_hinten():
    """Sie darf die Sortierung nicht sprengen — und nicht vor einer
    Station mit bekannter Lage stehen."""
    roh = [_ort("ohne"), _ort("nah", 48.21, 16.38)]
    assert [e["name"] for e in _naechste_fuenf(roh, *WIEN)] == ["nah", "ohne"]


@pytest.mark.parametrize("koord", [
    {"latitude": None, "longitude": 16.4},
    {"latitude": "48.2", "longitude": "16.4"},
    {},
])
def test_kaputte_koordinaten_stuerzen_nicht_ab(koord):
    roh = [{"name": "kaputt", "coordinates": koord},
           _ort("nah", 48.21, 16.38)]
    assert [e["name"] for e in _naechste_fuenf(roh, *WIEN)][0] == "nah"


def test_hoechstens_fuenf():
    roh = [_ort(f"s{i}", 48.2 + i / 100, 16.4) for i in range(12)]
    assert len(_naechste_fuenf(roh, *WIEN)) == 5


def test_leere_liste():
    assert _naechste_fuenf([], *WIEN) == []


def test_die_breitengrad_korrektur_traegt():
    """Auf 48° Breite ist ein Laengengrad nur rund zwei Drittel so lang
    wie ein Breitengrad. Ohne die Korrektur waere die Reihenfolge hier
    verkehrt."""
    # 0,30° in der Laenge ~ 22 km; 0,25° in der Breite ~ 28 km.
    roh = [_ort("laenge", 48.2082, 16.3738 + 0.30),
           _ort("breite", 48.2082 + 0.25, 16.3738)]
    assert [e["name"] for e in _naechste_fuenf(roh, *WIEN)] == \
        ["laenge", "breite"]


# ------------------------------------------------------- die Query-Form

def test_order_by_ist_raus():
    """DER Befund: ``order_by=distance`` erzeugte HTTP 422, die API
    akzeptiert dort nur noch ``id``."""
    quelle = inspect.getsource(_fetch_locations_near)
    assert "order_by" not in quelle.split('"""')[2], quelle
    assert "sort_order" not in quelle.split('"""')[2]


def test_die_api_meldung_steht_im_docstring():
    """Damit der naechste Durchgang nicht raet, was 422 bedeutete."""
    doc = _fetch_locations_near.__doc__ or ""
    assert "order_by" in doc
    assert "Input should be 'id'" in doc
    assert "422" in doc


def test_das_rohlimit_ist_groesser_als_die_ausgabe():
    """Sonst waehlt die Kuerzung auf fuenf aus einer UNSORTIERTEN Liste —
    genau der Fehler, den das weggefallene ``order_by`` hinterlassen
    haette."""
    assert LOCATIONS_ROHLIMIT > 5
    assert LOCATIONS_ROHLIMIT >= 50, "88 Stationen liegen im Wiener Radius"


def test_die_abfrage_nutzt_das_rohlimit():
    quelle = inspect.getsource(_fetch_locations_near)
    assert "LOCATIONS_ROHLIMIT" in quelle
    assert '"limit": "5"' not in quelle
