#!/usr/bin/env python3
"""NUR die Analyse-Stufe (LLM) fuer eine Liste von Eingaben — im Wartungs-Container.

Laeuft im Einmal-Container ``wartung`` (224 MB), NIE im Prod-Container und
ohne NER. Der Aufruf mit flock steht in CLAUDE.local.md:

    scp tools/messung/analyse_lauf.py eingaben.json <ssh>:/tmp/
    ssh <ssh> 'cd /opt/Evidora/website && flock -w 600 -E 75 /home/burrito/evidora-logs/.wartung.lock \\
      docker compose --profile wartung run --rm -T -v /tmp/analyse_lauf.py:/tmp/analyse_lauf.py:ro \\
      -v /tmp/eingaben.json:/tmp/eingaben.json:ro wartung python3 /tmp/analyse_lauf.py' > analyse.jsonl

``eingaben.json`` wie bei prod_pruefen.py (``id``/``nr`` und ``claim``/``text``).
Eine JSON-Zeile je Eingabe auf stdout: ``{"id", "analysis", "fehler", "dauer_s"}``.
Parallel ``ANALYSE_PARALLEL`` (Standard 4), bei Fehler ein zweiter Versuch
nach 10 s. Der Wartungs-Container schreibt nicht ins Datei-Log; die Aufrufe
verfaelschen den Cache-Anteil also nicht.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

PARALLEL = int(os.environ.get("ANALYSE_PARALLEL", "4"))


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
        for versuch in (1, 2):
            try:
                a, err = await analyze_claim(e["claim"]), None
                break
            except Exception as ex:  # noqa: BLE001 — Fehler gehoeren in die Ausgabe
                err = repr(ex)[:300]
                if versuch == 1:
                    await asyncio.sleep(10)
        ausgabe(json.dumps({"id": e["id"], "analysis": a, "fehler": err,
                            "dauer_s": round(time.time() - t0, 1)}, ensure_ascii=False))


async def lauf(eingaben, analyze_claim, ausgabe=lambda z: print(z, flush=True)):
    sem = asyncio.Semaphore(PARALLEL)
    await asyncio.gather(*(_eine(sem, e, analyze_claim, ausgabe) for e in eingaben))


def main() -> int:
    pfad = sys.argv[1] if len(sys.argv) > 1 else "/tmp/eingaben.json"
    sys.path.insert(0, "/app")
    from services.claim_analyzer import analyze_claim  # erst hier: Container-Pfad
    with open(pfad, encoding="utf-8") as f:
        eingaben = normalisiere(json.load(f))
    asyncio.run(lauf(eingaben, analyze_claim))
    return 0


if __name__ == "__main__":
    sys.exit(main())
