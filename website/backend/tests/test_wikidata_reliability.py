"""Wikidata-SPARQL-Zuverlässigkeit (QA50B #25-Rest, 2026-07-12).

Live-Befund: Orbán-Claim korrekt false@0.7 NUR wenn WDQS antwortete
(2/4 Läufen); Wurzel war ein doppelter Fehler: (a) kein Retry,
(b) Fehler/Timeout wurde wie '0 Treffer' behandelt und 1 h NEGATIV
GECACHT — ein transienter Timeout vergiftete alle Folge-Claims zur
selben Entität. Hier gepinnt: Fehler ≠ leer, Retry, Last-Good-Fallback.

Dependency-light: _run_sparql/polite_client gemockt, kein Netz.
"""
import asyncio
from contextlib import asynccontextmanager

import pytest

import services.wikidata as wd

CLAIM = "Viktor Orbán ist Ministerpräsident von Ungarn"
ROW = {"person": {"value": "http://www.wikidata.org/entity/Q57641"},
       "personLabel": {"value": "Viktor Orbán"},
       "positionLabel": {"value": "Ministerpräsident Ungarns"},
       "start": {"value": "2010-05-29T00:00:00Z"},
       "end": {"value": "2026-05-09T00:00:00Z"}}


@asynccontextmanager
async def _dummy_client(timeout=None):
    yield object()


def _analysis():
    return {"claim": CLAIM, "original_claim": CLAIM,
            "entities": ["Viktor Orbán"]}


def _setup(monkeypatch, responses):
    """responses: Liste von Rückgaben pro _run_sparql-Aufruf
    (None = Fehler/Timeout, [] = echte 0 Treffer, [ROW] = Erfolg)."""
    calls = {"n": 0}

    async def fake_run(client, query):
        i = min(calls["n"], len(responses) - 1)
        calls["n"] += 1
        return responses[i]

    monkeypatch.setattr(wd, "_run_sparql", fake_run)
    monkeypatch.setattr(wd, "polite_client", _dummy_client)
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    wd._CACHE.clear()
    wd._LAST_GOOD.clear()
    return calls


async def _no_sleep(_s):
    return None


def test_retry_liefert_nach_transientem_fehler(monkeypatch):
    calls = _setup(monkeypatch, [None, [ROW]])
    out = asyncio.run(wd.search_wikidata(_analysis()))
    assert out["results"], out
    assert calls["n"] == 2  # 1 Fehler + 1 Retry-Erfolg


def test_fehler_wird_nicht_negativ_gecacht(monkeypatch):
    """DER Kern-Bug: nach einem Fehler-Lauf muss der nächste Aufruf
    WIEDER SPARQL versuchen — vorher servierte der 1-h-Negativ-Cache
    still leer."""
    calls = _setup(monkeypatch, [None, None, [ROW]])
    out1 = asyncio.run(wd.search_wikidata(_analysis()))
    assert not out1["results"] and calls["n"] == 2  # beide Versuche fehl
    out2 = asyncio.run(wd.search_wikidata(_analysis()))
    assert out2["results"], "Fehler wurde negativ gecacht!"
    assert calls["n"] == 3


def test_last_good_fallback_bei_fehler(monkeypatch):
    calls = _setup(monkeypatch, [[ROW], None, None])
    out1 = asyncio.run(wd.search_wikidata(_analysis()))
    assert out1["results"]
    wd._CACHE.clear()  # TTL abgelaufen simulieren — Last-Good bleibt
    out2 = asyncio.run(wd.search_wikidata(_analysis()))
    assert out2["results"] == out1["results"], "Last-Good griff nicht"


def test_echte_null_treffer_bleiben_negativ_gecacht(monkeypatch):
    """rows == [] ist ein legitimes Ergebnis — der Negativ-Cache für
    echte 0-Treffer muss erhalten bleiben (Wiederholungs-Schutz)."""
    calls = _setup(monkeypatch, [[]])
    asyncio.run(wd.search_wikidata(_analysis()))
    n_after_first = calls["n"]
    asyncio.run(wd.search_wikidata(_analysis()))
    assert calls["n"] == n_after_first  # zweiter Aufruf aus dem Cache


def test_templates_decken_mul_labels():
    """Wurzelbefund #25: Wikidata migriert sprachübergreifend identische
    Labels ins 'mul'-Pseudo-Label und löscht de/en — reines @de fand
    z. B. Orbán (Q57641) NIE mehr (echte 0 Treffer → legitimer
    Negativ-Cache → Quelle dauerhaft tot). Jedes Template muss
    de+mul+en abfragen."""
    import os
    svc = open(os.path.join(os.path.dirname(__file__), "..",
                            "services", "wikidata.py"),
               encoding="utf-8").read()
    assert 'rdfs:label "{name}"@de.' not in svc
    assert svc.count('@mul') >= 10


def test_meloni_regel_aktives_spitzenamt_unterdrueckt_nebenamt_marker(monkeypatch):
    """QA50C #7: beendetes 8-Tage-Interims-Ministeramt feuerte STRUKT-
    Marker gegen die AKTIVE Ministerpräsidentin. Ohne Amts-Substantiv im
    Claim + aktivem Spitzenamt fliegen beendete Nebenämter raus."""
    rows = [
        {"person": {"value": "http://www.wikidata.org/entity/Q118625"},
         "personLabel": {"value": "Giorgia Meloni"},
         "positionLabel": {"value": "Italian minister of Tourism"},
         "start": {"value": "2026-03-26T00:00:00Z"},
         "end": {"value": "2026-04-03T00:00:00Z"}},
        {"person": {"value": "http://www.wikidata.org/entity/Q118625"},
         "personLabel": {"value": "Giorgia Meloni"},
         "positionLabel": {"value": "Italienischer Ministerpräsident"},
         "start": {"value": "2022-10-22T00:00:00Z"}},
    ]
    _setup(monkeypatch, [rows])
    out = asyncio.run(wd.search_wikidata(
        {"claim": "Giorgia Meloni regiert Italien noch immer",
         "original_claim": "Giorgia Meloni regiert Italien noch immer",
         "entities": ["Giorgia Meloni"]}))
    disp = " ".join(r.get("display_value", "") for r in out["results"])
    assert "STRUKTURELL" not in disp, disp
    assert "Ministerpräsident" in disp
    # Orbán-Klasse unberührt: ALLE Ämter beendet → Marker bleibt
    rows_ended = [dict(rows[0]),
                  {**rows[1], "end": {"value": "2026-05-09T00:00:00Z"}}]
    _setup(monkeypatch, [rows_ended])
    out2 = asyncio.run(wd.search_wikidata(
        {"claim": "Giorgia Meloni regiert Italien noch immer",
         "original_claim": "Giorgia Meloni regiert Italien noch immer",
         "entities": ["Giorgia Meloni"]}))
    disp2 = " ".join(r.get("display_value", "") for r in out2["results"])
    assert "STRUKTURELL" in disp2, disp2
