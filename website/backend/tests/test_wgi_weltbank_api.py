"""WGI lieferte vier Monate lang für JEDES Land null Ergebnisse.

Entdeckt am 2026-09-07 bei der Live-Kontrolle zu #155: „Wie steht Ruanda beim
WGI Rechtsstaatlichkeit?" bekam `unverifiable@0.1`. Der Verdacht lag auf der
gerade erweiterten Länderkarte — der war falsch. Das Prod-Log zeigte:

    GET .../country/RWA;EUU/indicator/RL.EST?source=75&... "HTTP/1.1 200 OK"
    WGI: 0 results for indicators=['RL.EST'] countries=['RWA']

Das Land kam korrekt an. Die Weltbank antwortete mit **HTTP 200** — und einem
Fehler-Objekt im Rumpf:

    [{"message":[{"id":"120","key":"Invalid value",
                  "value":"The provided parameter value is not valid"}]}]

Zwei Umstellungen auf Seiten der Quelle, beide unbemerkt:

    source=75  ist die ESG-Sammlung, nicht WGI      ->  source=3
    RL.EST     existiert dort nicht mehr             ->  GOV_WGI_RL.EST

**Warum es niemand gemerkt hat, ist der eigentliche Befund.** `raise_for_status`
greift bei HTTP 200 nicht. Der Code prüfte danach nur `len(data) < 2` und cachte
still eine leere Liste. „Null Ergebnisse" ist von „diese Quelle hat zu diesem
Land nichts" nicht zu unterscheiden — und genau so sah es 24 Stunden lang im
Cache aus, jeden Tag neu, seit dem 17.05.2026.

Eine grüne Suite hat das nicht gefangen und konnte es nicht: kein Test spricht
mit der echten API. Was hilft, ist, den Ausfall SICHTBAR zu machen — der
Konnektor unterscheidet jetzt zwischen „nichts gefunden" und „Abfrage kaputt"
und schreibt im zweiten Fall eine Warnung.
"""

import logging
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services import wgi  # noqa: E402

FEHLER_ANTWORT = [{"message": [{"id": "120", "key": "Invalid value",
                                "value": "The provided parameter value is not valid"}]}]
GUTE_ANTWORT = [
    {"page": 1, "pages": 1, "per_page": 200, "total": 2, "sourceid": "3"},
    [{"indicator": {"id": "GOV_WGI_RL.EST", "value": "Rule of Law"},
      "countryiso3code": "AUT", "date": "2024", "value": 1.6777399},
     {"indicator": {"id": "GOV_WGI_RL.EST", "value": "Rule of Law"},
      "countryiso3code": "AUT", "date": "2023", "value": 1.66}],
]


class _Antwort:
    def __init__(self, nutzlast, status=200):
        self._n, self.status_code = nutzlast, status

    def json(self):
        return self._n

    def raise_for_status(self):
        """HTTP 200 — genau darum hat der Fehler vier Monate ueberlebt."""


class _Klient:
    def __init__(self, nutzlast):
        self._n = nutzlast
        self.aufrufe: list[tuple[str, dict]] = []

    async def get(self, url, params=None, **_):
        self.aufrufe.append((url, params or {}))
        return _Antwort(self._n)


@pytest.fixture(autouse=True)
def _leerer_cache():
    wgi._cache.clear()
    yield
    wgi._cache.clear()


# --------------------------------------------------------------------------
# Die beiden Umstellungen der Quelle
# --------------------------------------------------------------------------

def test_quelle_ist_die_wgi_datenbank():
    """source=75 war die ESG-Sammlung. Die Weltbank quittiert sie fuer diese
    Indikatoren mit „Invalid value"."""
    assert wgi.SOURCE_ID == "3"


def test_api_id_traegt_das_praefix():
    assert wgi.API_ID_PRAEFIX == "GOV_WGI_"
    assert set(wgi.WGI_INDICATORS) == {"VA.EST", "PV.EST", "GE.EST",
                                       "RQ.EST", "RL.EST", "CC.EST"}


@pytest.mark.asyncio
async def test_abfrage_baut_die_neue_id():
    k = _Klient(GUTE_ANTWORT)
    await wgi._fetch_indicator(k, "AUT", "RL.EST", "2020:2024")
    url, params = k.aufrufe[0]
    assert url.endswith("/indicator/GOV_WGI_RL.EST"), url
    assert params["source"] == "3"


# --------------------------------------------------------------------------
# Der Kern: Fehler-Objekt bei HTTP 200
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fehlerobjekt_wird_als_fehler_erkannt(caplog):
    """Vorher: still `[]` zurueck und 24 h gecacht. Jetzt eine Warnung, die
    „kaputte Abfrage" von „nichts gefunden" unterscheidet."""
    k = _Klient(FEHLER_ANTWORT)
    with caplog.at_level(logging.WARNING, logger="evidora"):
        ergebnis = await wgi._fetch_indicator(k, "RWA", "RL.EST", "2020:2024")
    assert ergebnis == []
    text = caplog.text
    assert "API-FEHLER" in text, text
    assert "Invalid value" in text
    assert "kaputte Abfrage" in text


@pytest.mark.asyncio
async def test_fehler_wird_nicht_gecacht():
    """Ein gecachter Fehler haelt den Ausfall 24 Stunden am Leben. Ein leeres
    ECHTES Ergebnis darf dagegen gecacht werden."""
    k = _Klient(FEHLER_ANTWORT)
    await wgi._fetch_indicator(k, "RWA", "RL.EST", "2020:2024")
    assert not wgi._cache, wgi._cache

    wgi._cache.clear()
    k2 = _Klient([{"total": 0}, []])
    await wgi._fetch_indicator(k2, "RWA", "RL.EST", "2020:2024")
    assert wgi._cache, "echtes Leer-Ergebnis sollte gecacht werden"


@pytest.mark.asyncio
async def test_gute_antwort_kommt_durch(caplog):
    k = _Klient(GUTE_ANTWORT)
    with caplog.at_level(logging.WARNING, logger="evidora"):
        ergebnis = await wgi._fetch_indicator(k, "AUT", "RL.EST", "2020:2024")
    assert len(ergebnis) == 2
    assert ergebnis[0]["value"] == pytest.approx(1.6777399)
    assert "API-FEHLER" not in caplog.text


# --------------------------------------------------------------------------
# Das EU-Aggregat gibt es in dieser Quelle nicht
# --------------------------------------------------------------------------

def test_kein_aggregat_wird_mitabgefragt():
    """Gegen EUU, OED, WLD, ECS, HIC und EMU geprueft — alle liefern in
    source=3 total=0. Ein angehaengtes Aggregat kostet Antwortzeit und liefert
    nie einen Wert."""
    quelle = (BACKEND / "services" / "wgi.py").read_text(encoding="utf-8")
    assert 'country_str = ";".join(countries)' in quelle
    assert '";".join([*countries, EU_AGGREGATE])' not in quelle


def test_vergleichssatz_vertraegt_fehlenden_eu_wert():
    """Der Satz haengt nur an, wenn ein Wert da ist — sonst stuende dort eine
    Zahl, die niemand geliefert hat."""
    ohne = wgi._build_description("RL.EST", "Austria", "2024", 1.68, None)
    assert "EU-Aggregat" not in ohne
    mit = wgi._build_description("RL.EST", "Austria", "2024", 1.68, 0.9)
    assert "EU-Aggregat" in mit
