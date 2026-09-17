"""Zwei Kopien derselben Zahl: demokratie_pack gegen den Transparency-Connector (CPI).

Befund (2026-09-13, Produktion): Zum Claim „Die Korruption in der EU ist laut
Korruptionswahrnehmungsindex gering" standen zwei Österreich-Werte im selben
Prompt — der Connector mit „CPI Austria (2024): 67/100", das Pack mit
„CPI 2024 … AT 71/100 (Rang 20)".

Der Connector hatte recht. 71/100 auf Rang 20 ist der **CPI 2023**; beim
Umetikettieren auf 2024 blieb die Zahl stehen. Die Zeitreihe im selben Fakt
schrieb es sogar hin: „2023 71 (20), 2024 71 (20)". Und nicht nur AT — die
Top-10-Liste mischte Ausgaben (NZ 87, FI 84, UK 73), RUS 26 (2023) stand neben
Rang 154 (2024), Nordkorea stand 2024 nicht unter den letzten drei.

Folge in QA50F #38 („Österreich liegt im Korruptionsindex auf Platz 20",
erwartet `true`): das Urteil `true` (0.95) zitierte wörtlich die Pack-Zahl.
Nach CPI 2024 ist AT auf Rang 25 — Rang 20 war der Stand CPI 2023.

Anders als bei Freedom House liegt die Quelle des Connectors nicht im Repo:
`services/transparency.py` lädt die OWID-CSV zur Laufzeit, und die CI geht
nicht ins Netz. Deshalb liegt ein Auszug (2019–2024, alle Staaten, Zeilen
unverändert) in `tests/fixtures/owid_cpi_2019_2024.csv`. Der Test lässt den
**echten** Connector-Code über diesen Auszug laufen und hält das Pack dagegen.

Grenze: Der Auszug ist selbst eine Kopie. Übernimmt OWID den CPI 2025, zeigt
der Connector in Produktion 2025 und dieser Test bleibt grün. Beim Refresh
gehören Auszug und Pack in denselben PR — dann greift der Test wieder.

Ränge: OWID führt keine. Transparency International vergibt geteilte Ränge
(gleicher Score = gleicher Rang); so aus dem Auszug berechnet stimmen sie für
AT 2019–2024 mit TI überein. OWID hat 179 der 180 Staaten, der fehlende liegt
zwischen Rang 25 und 154 (RUS: TI 154, Auszug 153) — Ränge werden daher nur
bis 25 geprüft.
"""

import asyncio
import importlib
import json
import re
import sys
from pathlib import Path

import httpx
import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

AUSZUG = BACKEND / "tests" / "fixtures" / "owid_cpi_2019_2024.csv"
PACK_ROH = (BACKEND / "data" / "demokratie_pack.json").read_text(encoding="utf-8")
FAKT = next(f for f in json.loads(PACK_ROH)["facts"]
            if f.get("topic") == "korruption_index_konsens")
FAKT_TEXT = json.dumps(FAKT, ensure_ascii=False)
KERNSATZ = FAKT["data"]["kernsatz_fuer_synthesizer"]

KURZ_ZU_ISO3 = {"DK": "DNK", "FI": "FIN", "SG": "SGP", "NZ": "NZL", "CH": "CHE",
                "LU": "LUX", "NO": "NOR", "SE": "SWE", "NL": "NLD", "AU": "AUS",
                "IE": "IRL", "IS": "ISL", "CA": "CAN", "DE": "DEU", "UK": "GBR",
                "AT": "AUT", "RUS": "RUS"}
KURZ = "|".join(KURZ_ZU_ISO3)
RANG_GEPRUEFT_BIS = 25


@pytest.fixture(scope="module")
def connector():
    """Der echte Connector, gefüttert mit dem Auszug statt mit dem Netz."""
    tr = importlib.import_module("services.transparency")
    vorher = (tr._cpi_cache, tr._cpi_cache_time)
    tr._cpi_cache, tr._cpi_cache_time = None, 0.0
    text = AUSZUG.read_text(encoding="utf-8")

    def antwort(request):
        assert str(request.url) == tr.CPI_CSV_URL
        return httpx.Response(200, text=text)

    async def laden():
        async with httpx.AsyncClient(transport=httpx.MockTransport(antwort)) as c:
            return await tr.fetch_cpi(c)

    daten = asyncio.run(laden())
    yield tr, daten
    tr._cpi_cache, tr._cpi_cache_time = vorher


def _jahr(daten) -> int:
    return max(daten["AUT"])


def _score(daten, iso3: str, jahr: int) -> int:
    return round(daten[iso3][jahr]["score"])


def _rang(daten, iso3: str, jahr: int) -> int:
    """Geteilter Rang wie bei TI: 1 + Zahl der Staaten mit höherem Score."""
    eigen = daten[iso3][jahr]["score"]
    return 1 + sum(1 for j in daten.values() if jahr in j and j[jahr]["score"] > eigen)


# --------------------------------------------------------------------------
# Der Connector: genau die Zeile, die in Produktion als Beleg stand
# --------------------------------------------------------------------------

async def test_connector_zeile_fuer_oesterreich(connector):
    tr, daten = connector
    r = await tr.search_transparency(
        {"claim": "Die Korruption in Österreich ist laut Korruptionswahrnehmungsindex gering"})
    zeilen = [x["indicator_name"] for x in r["results"] if x.get("country") == "AUT"]
    assert zeilen, r["results"]
    assert zeilen[0].startswith(
        f"CPI Austria ({_jahr(daten)}): {_score(daten, 'AUT', _jahr(daten))}/100"), zeilen


def test_pack_nennt_dieselbe_ausgabe_wie_der_connector(connector):
    _, daten = connector
    jahr = _jahr(daten)
    assert FAKT["year"] == jahr
    assert f"CPI {jahr}" in FAKT["scope"]
    assert f"CPI {jahr}" in FAKT["headline"]


# --------------------------------------------------------------------------
# Österreich: jede Stelle, die Wert und Rang des aktuellen Jahres nennt
# --------------------------------------------------------------------------

def _at_nennungen(text: str) -> list[tuple[int, int]]:
    """(Score, Rang) in den drei Schreibweisen, die das Pack verwendet."""
    paare = [(int(s), int(r)) for s, r in re.findall(
        r"\bAT(?:-CPI)? (\d{1,3})/100 \(Rang (\d{1,3})\b", text)]
    paare += [(int(s), int(r)) for r, s in re.findall(
        r"\b(\d{1,3})\. AT (\d{1,3})\b", text)]
    paare += [(int(s), int(r)) for r, s in re.findall(
        r"\bAT (\d{1,3})\. \((\d{1,3})\)", text)]
    return paare


def test_at_wert_und_rang_an_jeder_stelle(connector):
    _, daten = connector
    jahr = _jahr(daten)
    soll = (_score(daten, "AUT", jahr), _rang(daten, "AUT", jahr))
    genannt = _at_nennungen(FAKT_TEXT)
    # scope, headline, Kernsatz (Liste + DACH), Top-Liste, context_note
    assert len(genannt) >= 6, f"zu wenige AT-Nennungen gefunden: {genannt}"
    falsch = [p for p in genannt if p != soll]
    assert not falsch, f"Pack (Score, Rang) {falsch}, Connector {soll}"


def _zeitreihe(text: str) -> dict[int, tuple[int, int]]:
    return {int(j): (int(s), int(r)) for j, s, r in re.findall(
        r"\b(20[12]\d) (\d{1,3})(?:/100| P\.)? \((?:Rang )?(\d{1,3})\)", text)}


@pytest.mark.parametrize("stelle", ("kernsatz", "at_verschlechterung_zeitreihe"))
def test_zeitreihe_aus_dem_auszug(connector, stelle):
    _, daten = connector
    text = (KERNSATZ.split("(2) ")[1].split("Hauptursachen")[0]
            if stelle == "kernsatz" else FAKT["data"][stelle])
    reihe = _zeitreihe(text)
    assert 2019 in reihe and _jahr(daten) in reihe, reihe
    for jahr, (score, rang) in reihe.items():
        soll = (_score(daten, "AUT", jahr), _rang(daten, "AUT", jahr))
        assert (score, rang) == soll, f"AT {jahr}: Pack {(score, rang)}, Auszug {soll}"


def test_verschlechterung_seit_2019(connector):
    _, daten = connector
    delta = _score(daten, "AUT", 2019) - _score(daten, "AUT", _jahr(daten))
    genannt = [int(d) for d in re.findall(
        r"-(\d{1,2}) P\.(?: seit 2019| 2019-2024)", FAKT_TEXT)]
    assert len(genannt) >= 3, genannt
    assert set(genannt) == {delta}, f"Pack {genannt}, Auszug -{delta}"


def test_rang_20_ist_der_cpi_2023_stand(connector):
    """Die Verwechslung selbst: 71/Rang 20 gehört zu 2023, nicht zu 2024."""
    _, daten = connector
    assert (_score(daten, "AUT", 2023), _rang(daten, "AUT", 2023)) == (71, 20)
    assert (_score(daten, "AUT", 2024), _rang(daten, "AUT", 2024)) != (71, 20)
    for alt in ("AT 71/100 (Rang 20", "20. AT 71", "2024 71 (20)"):
        assert alt not in FAKT_TEXT, f"{alt!r} steht noch im Pack"


# --------------------------------------------------------------------------
# Die anderen Länder im selben Fakt
# --------------------------------------------------------------------------

def _laender_nennungen() -> list[tuple[str, int, int]]:
    """(Kürzel, Score, Rang) aus Ranglisten, Headline und DACH-Vergleich."""
    treffer = []
    for text in (KERNSATZ, FAKT["data"]["cpi_2024_top"]):
        treffer += [(k, int(s), int(r)) for r, k, s in re.findall(
            rf"\b(\d{{1,3}})\. ({KURZ}) (\d{{1,3}})\b", text)]
    # Nur im DACH-Abschnitt: sonst liest "154. RUS 22. (2) AT-…" die
    # Abschnittsnummer (2) als Score.
    dach = KERNSATZ.split("(4) DACH-VERGLEICH")[1].split("(5) ")[0]
    treffer += [(k, int(s), int(r)) for k, r, s in re.findall(
        rf"\b({KURZ}) (\d{{1,3}})\. \((\d{{1,3}})(?: P\.)?\)", dach)]
    treffer += [(k, int(s), int(r)) for k, s, r in re.findall(
        rf"\b({KURZ}) (\d{{1,3}})(?:/100)? \((?:Rang )?(\d{{1,3}})\.?[,)]",
        FAKT["headline"])]
    return treffer


def test_ranglisten_werte_aus_dem_auszug(connector):
    _, daten = connector
    jahr = _jahr(daten)
    treffer = _laender_nennungen()
    assert len(treffer) >= 20, treffer
    falsch = []
    for kurz, score, rang in treffer:
        iso3 = KURZ_ZU_ISO3[kurz]
        if score != _score(daten, iso3, jahr):
            falsch.append((kurz, "Score", score, _score(daten, iso3, jahr)))
        if rang <= RANG_GEPRUEFT_BIS and rang != _rang(daten, iso3, jahr):
            falsch.append((kurz, "Rang", rang, _rang(daten, iso3, jahr)))
    assert not falsch, f"(Land, Feld, Pack, Auszug): {falsch}"


def test_schlusslicht_aus_dem_auszug(connector):
    _, daten = connector
    jahr = _jahr(daten)
    m = re.search(r"Süd-Sudan \((\d+) P\.\), Somalia \((\d+)\) und Venezuela \((\d+)\)",
                  KERNSATZ)
    assert m, "Schlusslicht-Satz nicht gefunden"
    genannt = dict(zip(("SSD", "SOM", "VEN"), map(int, m.groups())))
    assert genannt == {c: _score(daten, c, jahr) for c in genannt}
    letzte = sorted((j[jahr]["score"], c) for c, j in daten.items() if jahr in j)[:3]
    assert {c for _, c in letzte} == set(genannt)
