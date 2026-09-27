"""UNHCR lieferte für JEDEN Länder-Claim keine Bevölkerungszahlen.

Entdeckt am 2026-09-13 bei einem lokalen Aufruf von `search_unhcr` mit
„Laut UNHCR leben in Österreich viele Fluechtlinge":

    UNHCR population request failed: unsupported operand type(s) for +=:
    'int' and 'str'

Die Live-API mischt die Typen innerhalb der Zählfelder. Typ-Zensus über
coa=AUT/DEU/TUR/CHE und coo=SYR/UKR (yearFrom 2019 / yearTo 2024):

    refugees, asylum_seekers   immer int
    idps                       "0" für AUT/DEU/TUR/CHE, int für SYR/UKR
    returned_refugees          5000 und "0" im Wechsel, je nach Jahr
    stateless, ooc, hst        teils "0"
    oip                        "-"

Die globale Abfrage ohne Land kam dagegen nur mit ints — deshalb fiel der
Fehler nur bei Länder-Claims auf. Das alte `item.get("idps", 0) or 0` half
nicht: der String "0" ist truthy, `0 += "0"` wirft, und das except fing die
GANZE Population-Abfrage ab. Übrig blieben nur die Asylanträge.

Jetzt: Zahl-Strings werden umgewandelt, Nicht-Zahlen („-", "") werden
übersprungen und in EINER Warnung pro Abfrage gemeldet. Kein Netz im Test.
"""

import logging
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services import unhcr  # noqa: E402

CLAIM = "Laut UNHCR leben in Österreich viele Fluechtlinge"

# Nachgebaut aus der Live-Antwort für coa=AUT (2026-09-13), plus zwei
# künstliche Nicht-Zahlen („-" und "") und ein Zahl-String in einem Feld,
# das live ein int ist — damit auch die Summe über mehrere Zeilen geprüft ist.
POPULATION = {
    "page": 1, "maxPages": 1, "total": [],
    "items": [
        {"year": 2023, "coa_name": "Austria", "coo_name": "-",
         "refugees": 257811, "asylum_seekers": 38039,
         "returned_refugees": 5000, "idps": "0", "returned_idps": "0",
         "stateless": 3194, "ooc": "0", "oip": "-", "hst": "0"},
        {"year": 2024, "coa_name": "Austria", "coo_name": "-",
         "refugees": 100000, "asylum_seekers": "2000",
         "returned_refugees": "0", "idps": "0", "oip": "-"},
        {"year": 2024, "coa_name": "Austria", "coo_name": "-",
         "refugees": "-", "asylum_seekers": 500,
         "returned_refugees": "0", "idps": "", "oip": "-"},
    ],
}

ANTRAEGE = {
    "page": 1, "maxPages": 1,
    "items": [
        {"year": 2024, "coa_name": "Austria", "applied": "25322"},
        {"year": 2024, "coa_name": "Austria", "applied": "-"},
        {"year": 2023, "coa_name": "Austria", "applied": 59215},
    ],
}


class _Antwort:
    def __init__(self, nutzlast, status=200):
        self._n, self.status_code = nutzlast, status

    def json(self):
        return self._n


class _Klient:
    def __init__(self, population, antraege):
        self._population, self._antraege = population, antraege
        self.aufrufe: list[tuple[str, dict]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def get(self, url, params=None, **_):
        self.aufrufe.append((url, params or {}))
        if "/asylum-applications/" in url:
            return _Antwort(self._antraege)
        return _Antwort(self._population)


@pytest.fixture
def klient(monkeypatch):
    k = _Klient(POPULATION, ANTRAEGE)
    monkeypatch.setattr(unhcr, "polite_client", lambda **_: k)
    return k


def _nach_indikator(ergebnis, indikator):
    return {r["year"]: r for r in ergebnis["results"]
            if r["indicator"] == indikator}


# --------------------------------------------------------------------------
# Umwandlung einzelner Werte
# --------------------------------------------------------------------------

@pytest.mark.parametrize("roh, erwartet", [
    (135951, 135951),
    ("0", 0),
    ("25322", 25322),
    (" 7 ", 7),
    ("5000.0", 5000),
    (12.0, 12),
    (None, 0),          # fehlender Key / null: wie vorher `or 0`
])
def test_zahlen_und_zahl_strings_werden_umgewandelt(roh, erwartet):
    assert unhcr._to_count(roh) == erwartet


@pytest.mark.parametrize("roh", ["-", "", "n/a", True, [], float("nan")])
def test_nicht_zahlen_sind_none(roh):
    assert unhcr._to_count(roh) is None


# --------------------------------------------------------------------------
# Der Kern: search_unhcr mit String-Werten in der Antwort
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_population_ueberlebt_string_werte(klient, caplog):
    """Vorher: TypeError beim ersten "0", die Population-Ergebnisse fehlten
    komplett. Jetzt kommen beide Jahre mit korrekten Summen an."""
    with caplog.at_level(logging.WARNING, logger="evidora"):
        ergebnis = await unhcr.search_unhcr({"claim": CLAIM,
                                             "original_claim": CLAIM})

    assert "population request failed" not in caplog.text, caplog.text

    pop = _nach_indikator(ergebnis, "UNHCR Refugee Population")
    assert set(pop) == {2023, 2024}, ergebnis["results"]
    assert pop[2023]["refugees"] == 257811
    assert pop[2023]["asylum_seekers"] == 38039
    # 2024: zwei Zeilen; "2000" zählt, refugees="-" wird übersprungen
    assert pop[2024]["refugees"] == 100000
    assert pop[2024]["asylum_seekers"] == 2500


@pytest.mark.asyncio
async def test_uebersprungene_werte_in_einer_warnung(klient, caplog):
    """Übersprungen heißt nicht still: eine Zeile pro Abfrage, mit Jahr,
    Feld und Rohwert. `oip` wird nicht summiert und taucht nicht auf."""
    with caplog.at_level(logging.WARNING, logger="evidora"):
        await unhcr.search_unhcr({"claim": CLAIM, "original_claim": CLAIM})

    pop_warnungen = [r.getMessage() for r in caplog.records
                     if r.getMessage().startswith("UNHCR population:")]
    assert len(pop_warnungen) == 1, caplog.text
    zeile = pop_warnungen[0]
    assert "2 non-numeric value(s) skipped" in zeile
    assert "2024/refugees='-'" in zeile
    assert "2024/idps=''" in zeile
    assert "oip" not in zeile


@pytest.mark.asyncio
async def test_asylantraege_ueberleben_string_werte(klient, caplog):
    """Dasselbe Summen-Muster steckte in `applied`. Live derzeit int — aber
    dieselbe API, dasselbe Risiko."""
    with caplog.at_level(logging.WARNING, logger="evidora"):
        ergebnis = await unhcr.search_unhcr({"claim": CLAIM,
                                             "original_claim": CLAIM})

    assert "asylum applications request failed" not in caplog.text
    antraege = _nach_indikator(ergebnis, "UNHCR Asylum Applications")
    assert antraege[2024]["applications"] == 25322
    assert antraege[2023]["applications"] == 59215
    assert "2024/applied='-'" in caplog.text


@pytest.mark.asyncio
async def test_ohne_strings_keine_warnung(monkeypatch, caplog):
    """Die globale Live-Antwort kommt nur mit ints — die darf keine
    Warnung auslösen, sonst wird die Zeile zum Rauschen."""
    nur_ints = {"items": [{"year": 2024, "refugees": 42700000,
                           "asylum_seekers": 8000000, "idps": 68300000}]}
    k = _Klient(nur_ints, {"items": []})
    monkeypatch.setattr(unhcr, "polite_client", lambda **_: k)
    with caplog.at_level(logging.WARNING, logger="evidora"):
        ergebnis = await unhcr.search_unhcr({"claim": "Weltweit gibt es "
                                             "viele Flüchtlinge"})
    assert not caplog.records, caplog.text
    pop = _nach_indikator(ergebnis, "UNHCR Refugee Population")
    assert pop[2024]["refugees"] == 42700000
