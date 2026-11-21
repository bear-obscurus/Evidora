"""Die Analyse-Stufe kennt das heutige Datum, und fremde Jahre erreichen die Dienste nicht.

Messung 5.11.2026: 20 von 98 Analysen erfanden Jahreszahlen (2023/2024), weil
der Analyse-Prompt kein Datum nannte. #316 setzte das Datum in die
Nutzernachricht. Relative Angaben stimmten dann ("letztes Jahr" -> 2025), aber
das Modell haengte "2026" an Gegenwarts-Claims (27 bis 30 von 98, 21.11.2026).
Deshalb gibt es jetzt beides als ein Paket:

- das Datum in der NUTZERNACHRICHT, nicht im SYSTEM_PROMPT (der bleibt Tag
  fuer Tag Byte-gleich, Prompt-Cache);
- den Jahres-Nachfilter (services/_jahresfilter.py) nach der ersten Antwort
  und nach der JSON-Wiederholung, mit demselben Tag wie die Nachricht.

Modellfrei: chat_completion wird ersetzt, kein Netz. Die Claims sind
ausgedacht.
"""
import asyncio
import datetime
import json
import logging
import os
import re
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

import services.claim_analyzer as ca  # noqa: E402

TAG = datetime.date(2026, 11, 20)


def _antwort(fc=(), ent=(), pm=()):
    return json.dumps({"claim": "x", "category": "other", "entities": list(ent),
                       "pubmed_queries": list(pm), "factcheck_queries": list(fc)})


ANTWORT = _antwort()


def _aufrufe(monkeypatch, antworten, tag=TAG):
    gesehen = []

    async def falsch(messages, **kw):
        gesehen.append([dict(m) for m in messages])
        return antworten.pop(0)

    monkeypatch.setattr(ca, "chat_completion", falsch)
    monkeypatch.setattr(ca, "_heute", lambda: tag, raising=False)
    return gesehen


def _jahre(text):
    return set(re.findall(r"(?<!\d)(?:19|20)\d\d(?!\d)", text))


# --- das Datum in der Nutzernachricht ---------------------------------------------

def test_datum_steht_in_der_nutzernachricht(monkeypatch):
    gesehen = _aufrufe(monkeypatch, [ANTWORT])
    asyncio.run(ca.analyze_claim("Die Inflation ist hoch"))
    nachricht = [m for m in gesehen[0] if m["role"] == "user"][0]["content"]
    assert "20.11.2026" in nachricht, nachricht


def test_system_prompt_traegt_kein_datum(monkeypatch):
    """Kein Jahr und kein Datum im System-Prompt, damit er cachebar bleibt."""
    gesehen = _aufrufe(monkeypatch, [ANTWORT])
    asyncio.run(ca.analyze_claim("Die Inflation ist hoch"))
    system = [m for m in gesehen[0] if m["role"] == "system"][0]["content"]
    assert system == ca.SYSTEM_PROMPT
    assert "20.11.2026" not in system
    assert "2026" not in _jahre(system), _jahre(system)


def test_system_prompt_haengt_nicht_vom_tag_ab(monkeypatch):
    """Zwei verschiedene Tage → derselbe System-Prompt, andere Nutzernachricht."""
    gesehen = _aufrufe(monkeypatch, [ANTWORT, ANTWORT])
    asyncio.run(ca.analyze_claim("Die Inflation ist hoch"))
    monkeypatch.setattr(ca, "_heute", lambda: datetime.date(2027, 1, 3))
    asyncio.run(ca.analyze_claim("Die Inflation ist hoch"))
    sys1, sys2 = (g[0]["content"] for g in gesehen)
    nutz1, nutz2 = (g[1]["content"] for g in gesehen)
    assert sys1 == sys2
    assert nutz1 != nutz2
    assert "03.01.2027" in nutz2


def test_claim_bleibt_in_den_tags_und_steht_hinter_dem_datum(monkeypatch):
    gesehen = _aufrufe(monkeypatch, [ANTWORT])
    asyncio.run(ca.analyze_claim("Heuer gab es mehr Asylanträge"))
    nachricht = gesehen[0][1]["content"]
    assert nachricht.count("<claim>Heuer gab es mehr Asylanträge</claim>") == 1
    assert nachricht.index("20.11.2026") < nachricht.index("<claim>")


def test_json_wiederholung_kennt_das_datum_auch(monkeypatch):
    """Auch der zweite Versuch nach unlesbarem JSON bekommt das Datum."""
    gesehen = _aufrufe(monkeypatch, ["kein json", ANTWORT])
    asyncio.run(ca.analyze_claim("Die Inflation ist hoch"))
    assert len(gesehen) == 2
    erste_nutz = gesehen[1][1]
    assert erste_nutz["role"] == "user" and "20.11.2026" in erste_nutz["content"]
    assert gesehen[1][0]["content"] == ca.SYSTEM_PROMPT


def test_heute_liefert_ein_datum():
    assert isinstance(ca._heute(), datetime.date)


# --- fremde Jahre erreichen die Dienste nicht ---------------------------------------

def test_erfundenes_jahr_erreicht_die_dienste_nicht(monkeypatch):
    """Der Fall aus #316: '2026' an einem Gegenwarts-Claim."""
    _aufrufe(monkeypatch, [_antwort(fc=["Inflation Österreich 2026", "Teuerung 2023 2024"],
                                    ent=["Inflation", "2026"], pm=["inflation Austria 2026"])])
    a = asyncio.run(ca.analyze_claim("Die Inflation in Österreich ist hoch"))
    assert a["factcheck_queries"] == ["Inflation Österreich", "Teuerung"]
    assert a["pubmed_queries"] == ["inflation Austria"]
    assert a["entities"] == ["Inflation"]


def test_jahr_aus_dem_claim_und_relatives_jahr_bleiben(monkeypatch):
    _aufrufe(monkeypatch, [_antwort(fc=["Asylanträge 2026 2025 Vergleich"]),
                           _antwort(fc=["MIPEX 2020 Österreich"])])
    a = asyncio.run(ca.analyze_claim("Heuer gab es mehr Asylanträge als letztes Jahr"))
    assert a["factcheck_queries"] == ["Asylanträge 2026 2025 Vergleich"]
    b = asyncio.run(ca.analyze_claim("Österreich lag im MIPEX 2020 auf Rang 20"))
    assert b["factcheck_queries"] == ["MIPEX 2020 Österreich"]


def test_der_filter_rechnet_mit_demselben_tag_wie_die_nachricht(monkeypatch):
    """Am 3.1.2027 ist 'heuer' 2027: Das Modell bekommt dieses Datum, und der
    Filter laesst 2027 stehen und nimmt 2026 heraus."""
    gesehen = _aufrufe(monkeypatch, [_antwort(fc=["Asylanträge 2027", "Asylanträge 2026"])],
                       tag=datetime.date(2027, 1, 3))
    a = asyncio.run(ca.analyze_claim("Heuer gab es mehr Asylanträge"))
    assert "03.01.2027" in gesehen[0][1]["content"]
    assert a["factcheck_queries"] == ["Asylanträge 2027", "Asylanträge"]


def test_auch_die_json_wiederholung_wird_gefiltert(monkeypatch):
    _aufrufe(monkeypatch, ["kein json", _antwort(fc=["Inflation 2026"], ent=["2024"])])
    a = asyncio.run(ca.analyze_claim("Die Inflation ist hoch"))
    assert a["factcheck_queries"] == ["Inflation"]
    assert a["entities"] == []


def test_notfall_antwort_behaelt_den_claim_als_suchbegriff(monkeypatch):
    _aufrufe(monkeypatch, ["kein json", "immer noch kein json"])
    a = asyncio.run(ca.analyze_claim("Die Arbeitslosigkeit sank 2025"))
    assert a.get("_fallback") is True
    assert a["factcheck_queries"] == ["Die Arbeitslosigkeit sank 2025"]


def test_ein_filterfehler_kippt_die_analyse_nicht(monkeypatch, caplog):
    """Scheitert der Filter, bleibt die Analyse ungefiltert. Ins Protokoll
    kommt nur der Name der Ausnahme, kein Claim-Text."""
    def kaputt(*a, **kw):
        raise ValueError("Die Inflation in Österreich ist hoch")

    _aufrufe(monkeypatch, [_antwort(fc=["Inflation 2026"])])
    monkeypatch.setattr(ca, "jahre_nachfiltern", kaputt, raising=False)
    with caplog.at_level(logging.ERROR, logger="evidora"):
        a = asyncio.run(ca.analyze_claim("Die Inflation in Österreich ist hoch"))
    assert a["factcheck_queries"] == ["Inflation 2026"]
    fehler = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("ValueError" in f for f in fehler), fehler
    assert not any("Inflation" in f for f in fehler), fehler
