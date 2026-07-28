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


# --- QA100 2026-07-28: die beiden defekten Templates ---

def _template(name):
    for t in wd._TEMPLATES:
        if t["name"] == name:
            return t
    raise AssertionError(f"Template {name} fehlt")


def test_bevoelkerung_template_akzeptiert_auch_siedlungen():
    """QA100 #90: Der Typfilter war auf wd:Q6256 (Land) beschränkt, wurde
    aber für Städte aufgerufen — 'Wien' lieferte HTTP 200 mit 0 Treffern,
    jeder Städte-Einwohner-Claim blieb datenlos. Live gegen WDQS
    verifiziert: mit Q486972 (Siedlung) treffen Wien 2.028.289 und
    Hamburg 1.910.160, Länder bleiben unverändert."""
    sparql = _template("land_bevoelkerung")["sparql"]
    assert "wd:Q486972" in sparql, "Siedlungs-Typ fehlt — Städte fallen durch"
    assert "wd:Q6256" in sparql, "Länder-Typ darf nicht verloren gehen"


def test_bevoelkerung_template_entschaerft_label_mehrdeutigkeit():
    """Zwei live reproduzierte Störer: ohne Jahres-Filter gewann Wiens
    historischer Höchststand von 1910 (2.083.630), ohne Sortierung nach
    Größe ein gleichnamiges Dorf mit 885 Einwohnern. DISTINCT verhindert
    zusätzlich die Dreifach-Zeile aus den de/mul/en-Labels."""
    sparql = _template("land_bevoelkerung")["sparql"]
    assert "SELECT DISTINCT" in sparql
    assert "YEAR(?date) >= 2015" in sparql
    assert "ORDER BY DESC(?population)" in sparql


def test_organisation_gruendung_ohne_generische_trigger():
    """QA100: 'existiert' und 'gibt es' feuerten auf beliebige Claims
    ('In Österreich GIBT ES mehr Rinder als Schweine') und schickten dann
    Entitäten wie 'Wien' in eine Query, die bei mehrdeutigen Labels live
    ~52 s braucht — der Service retryt nur 12 s + 8 s, daher im Prod-Log
    deterministisch 'SPARQL-Fehler … kein Last-Good — leer'."""
    triggers = _template("organisation_gruendung")["triggers"]
    for verboten in ("existiert", "gibt es"):
        assert verboten not in triggers, (
            f"{verboten!r} ist als Trigger zu generisch — es zieht "
            f"beliebige Claims in eine teure Organisations-Query.")
    # Die fachlich gemeinten Trigger müssen erhalten bleiben
    assert "gegründet" in triggers and "gründung" in triggers


def test_organisation_gruendung_feuert_nicht_auf_gibt_es_claims():
    """Funktionale Gegenprobe zur Trigger-Liste."""
    claim = "In Österreich gibt es mehr Rinder als Schweine"
    triggers = _template("organisation_gruendung")["triggers"]
    assert not any(t in claim.lower() for t in triggers), claim


# --- QA100 #90: Multi-Entitäts-Abfrage bei Vergleichs-Claims ---

_POP_CLAIM = "Wien hat mehr Einwohner als Hamburg"


def _pop_row(label, pop, date="2025-01-01T00:00:00Z"):
    return {"country": {"value": f"http://www.wikidata.org/entity/Q{abs(hash(label)) % 9999}"},
            "countryLabel": {"value": label},
            "population": {"value": str(pop)},
            "date": {"value": date}}


def _pop_analysis(entities):
    return {"claim": _POP_CLAIM, "original_claim": _POP_CLAIM,
            "entities": entities}


def test_vergleichs_claim_fragt_beide_entitaeten_ab(monkeypatch):
    """QA100 #90: `search_wikidata` verarbeitete genau EINE Entität, deshalb
    lieferte 'Wien hat mehr Einwohner als Hamburg' nur die Wien-Zahl — der
    Synthesizer sah keinen Vergleichswert und gab `unverifiable` aus."""
    calls = _setup(monkeypatch, [[_pop_row("Wien", 2028289)],
                                 [_pop_row("Hamburg", 1910160)]])
    out = asyncio.run(wd.search_wikidata(_pop_analysis(["Wien", "Hamburg"])))
    assert calls["n"] == 2, "beide Entitäten müssen abgefragt werden"
    blob = " ".join(r.get("display_value", "") for r in out["results"])
    assert "Wien" in blob and "Hamburg" in blob, out["results"]


def test_zweite_entitaet_nur_bei_vergleichs_signal(monkeypatch):
    """Ohne Vergleichs-Wendung wird nicht spekulativ eine zweite Entität
    abgefragt — das wäre Latenz für eine Frage, die niemand gestellt hat."""
    claim = "Wie viele Einwohner hat Wien?"
    calls = _setup(monkeypatch, [[_pop_row("Wien", 2028289)]])
    asyncio.run(wd.search_wikidata(
        {"claim": claim, "original_claim": claim,
         "entities": ["Wien", "Hamburg"]}))
    assert calls["n"] == 1, "ohne Vergleich nur eine Abfrage"


def test_multi_entity_nur_fuer_vergleichs_templates():
    """Bei politiker_amtszeit wäre eine zweite Entität kein Vergleich,
    sondern Rauschen."""
    claim = "Ist Rutte länger im Amt als Macron?"
    got = wd._entities_for_claim(
        claim, {"entities": ["Rutte", "Macron"]}, "politiker_amtszeit")
    assert got == ["Rutte"], got
    got2 = wd._entities_for_claim(
        claim, {"entities": ["Rutte", "Macron"]}, "land_bevoelkerung")
    assert got2 == ["Rutte", "Macron"], got2


def test_fehler_bei_zweiter_entitaet_liefert_erste_trotzdem(monkeypatch):
    """Ein halber Vergleich ist besser als gar keine Daten — die zweite
    Entität darf den Claim nicht mit in den Abgrund ziehen."""
    calls = _setup(monkeypatch, [[_pop_row("Wien", 2028289)], None])
    out = asyncio.run(wd.search_wikidata(_pop_analysis(["Wien", "Hamburg"])))
    assert calls["n"] >= 2
    assert out["results"], "Wien-Treffer muss erhalten bleiben"
    assert "Wien" in out["results"][0].get("display_value", "")


def test_einzel_entitaet_pfad_unveraendert(monkeypatch):
    """Der Einzel-Pfad muss byte-gleich weiterlaufen — er trägt die
    gesamte Retry-/Last-Good-/Meloni-Logik."""
    calls = _setup(monkeypatch, [[ROW]])
    out = asyncio.run(wd.search_wikidata(_analysis()))
    assert calls["n"] == 1
    assert out["results"]
