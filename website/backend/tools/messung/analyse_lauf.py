#!/usr/bin/env python3
"""NUR die Analyse-Stufe (LLM) fuer eine Liste von Eingaben — im Wartungs-Container.

Laeuft im Einmal-Container ``wartung`` (224 MB), NIE im Prod-Container und
ohne NER. Der Aufruf mit flock steht in CLAUDE.local.md:

    scp tools/messung/analyse_lauf.py eingaben.json <ssh>:/tmp/
    ssh <ssh> 'cd /opt/Evidora/website && flock -w 600 -E 75 /home/burrito/evidora-logs/.wartung.lock \\
      docker compose --profile wartung run --rm -T -v /tmp/analyse_lauf.py:/tmp/analyse_lauf.py:ro \\
      -v /tmp/eingaben.json:/tmp/eingaben.json:ro wartung python3 /tmp/analyse_lauf.py' > analyse.jsonl

``eingaben.json`` wie bei prod_pruefen.py (``id``/``nr`` und ``claim``/``text``).
Eine JSON-Zeile je Eingabe auf stdout: ``{"id", "analysis", "fehler", "dauer_s",
"vor_filter"}``. ``vor_filter`` sind die drei Suchfelder, wie sie VOR dem
Jahres-Nachfilter waren (``None``, wenn der Filter nicht lief, etwa bei der
Notfall-Antwort oder auf einem Stand ohne Filter). Prod aendert das nicht:
Nur hier, im Wartungs-Container, liegt eine Huelle um den Filter.
Parallel ``ANALYSE_PARALLEL`` (Standard 4), bei Fehler ein zweiter Versuch
nach 10 s. Der Wartungs-Container schreibt nicht ins Datei-Log; die Aufrufe
verfaelschen den Cache-Anteil also nicht.
"""

from __future__ import annotations

import asyncio
import contextvars
import copy
import json
import os
import sys
import time

PARALLEL = int(os.environ.get("ANALYSE_PARALLEL", "4"))
FILTER_FELDER = ("factcheck_queries", "pubmed_queries", "entities")
_VOR_FILTER: contextvars.ContextVar = contextvars.ContextVar("vor_filter", default=None)


def filter_mitschneiden(modul, name: str = "jahre_nachfiltern") -> bool:
    """Legt eine Huelle um ``modul.<name>``, die die drei Felder vor dem Filter
    festhaelt. Jede Eingabe laeuft in einer eigenen asyncio-Task und damit in
    einem eigenen Kontext; parallele Analysen mischen sich nicht."""
    original = getattr(modul, name, None)
    if original is None:
        return False

    def huelle(claim_text, analysis, *args, **kwargs):
        ziel = _VOR_FILTER.get()
        if ziel is not None and isinstance(analysis, dict):
            ziel.clear()
            ziel.update({f: copy.deepcopy(analysis.get(f)) for f in FILTER_FELDER})
        return original(claim_text, analysis, *args, **kwargs)

    setattr(modul, name, huelle)
    return True


def normalisiere(eingaben: list) -> list[dict]:
    aus = []
    for i, e in enumerate(eingaben):
        claim = e if isinstance(e, str) else (e.get("claim") or e.get("text"))
        kennung = str(i + 1) if isinstance(e, str) else str(e.get("id", e.get("nr", i + 1)))
        if not claim:
            raise ValueError(f"Eintrag {i} ohne claim/text")
        aus.append({"id": kennung, "claim": claim})
    return aus


async def _eine(sem, e, analyze_claim, ausgabe):
    async with sem:
        t0, a, err = time.time(), None, None
        vor: dict = {}
        _VOR_FILTER.set(vor)
        for versuch in (1, 2):
            vor.clear()
            try:
                a, err = await analyze_claim(e["claim"]), None
                break
            except Exception as ex:  # noqa: BLE001 — Fehler gehoeren in die Ausgabe
                err = repr(ex)[:300]
                if versuch == 1:
                    await asyncio.sleep(10)
        ausgabe(json.dumps({"id": e["id"], "analysis": a, "fehler": err,
                            "dauer_s": round(time.time() - t0, 1),
                            "vor_filter": dict(vor) or None}, ensure_ascii=False))


async def lauf(eingaben, analyze_claim, ausgabe=lambda z: print(z, flush=True)):
    sem = asyncio.Semaphore(PARALLEL)
    await asyncio.gather(*(_eine(sem, e, analyze_claim, ausgabe) for e in eingaben))


def main() -> int:
    pfad = sys.argv[1] if len(sys.argv) > 1 else "/tmp/eingaben.json"
    sys.path.insert(0, "/app")
    import services.claim_analyzer as ca  # erst hier: Container-Pfad
    if not filter_mitschneiden(ca):
        print("Hinweis: dieser Stand hat keinen Jahres-Nachfilter, vor_filter bleibt leer",
              file=sys.stderr)
    analyze_claim = ca.analyze_claim
    with open(pfad, encoding="utf-8") as f:
        eingaben = normalisiere(json.load(f))
    asyncio.run(lauf(eingaben, analyze_claim))
    return 0


if __name__ == "__main__":
    sys.exit(main())
