"""Die Analyse-Stufe kennt das heutige Datum — in der Nutzernachricht.

Messung 5.11.2026: 22 von 98 Analysen erfanden Jahreszahlen (2023/2024),
vor allem in den factcheck_queries, die 17 Dienste nutzen. Ursache: Der
Analyse-Prompt nannte kein heutiges Datum, das Modell setzte sein
Trainings-"Jetzt" ein.

Das Datum gehoert in die NUTZERNACHRICHT, nicht in den SYSTEM_PROMPT:
Der System-Prompt soll Tag fuer Tag Byte-gleich bleiben (Prompt-Cache).

Modellfrei: chat_completion wird ersetzt, kein Netz.
"""
import asyncio
import datetime
import json
import os
import re
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

import services.claim_analyzer as ca  # noqa: E402

TAG = datetime.date(2026, 11, 20)
ANTWORT = json.dumps({"claim": "x", "category": "other", "entities": [],
                      "pubmed_queries": [], "factcheck_queries": []})


def _aufrufe(monkeypatch, antworten):
    gesehen = []

    async def falsch(messages, **kw):
        gesehen.append([dict(m) for m in messages])
        return antworten.pop(0)

    monkeypatch.setattr(ca, "chat_completion", falsch)
    monkeypatch.setattr(ca, "_heute", lambda: TAG, raising=False)
    return gesehen


def _jahre(text):
    return set(re.findall(r"(?<!\d)(?:19|20)\d\d(?!\d)", text))


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
