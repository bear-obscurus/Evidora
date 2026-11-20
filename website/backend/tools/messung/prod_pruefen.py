#!/usr/bin/env python3
"""Pruefungen gegen Prod per ssh + Test-Key — fuer Messungen, nicht fuer den Betrieb.

Bis 20.11.2026 schrieb Claude dieses Skript fuer jede Messung neu
(115 Prod-Pruefungen in einer einzigen Sitzung, mehrere Varianten). Jetzt
gibt es eine getestete Fassung.

Aufruf (lokal, auf dem Mac):

    EVIDORA_SSH=<user>@<host> python3 tools/messung/prod_pruefen.py eingaben.json ergebnis.jsonl

``eingaben.json``: Liste von ``{"id": ..., "claim": ...}``; ``nr`` statt
``id`` und ``text`` statt ``claim`` gehen auch.

Zusagen:
- Der Test-Key wird NUR auf dem Server in eine Variable gelesen. Er steht
  nie lokal, nie in einer Ausgabe und nie in der lokalen Kommandozeile.
- Der Claim geht per stdin an curl (``-d @-``), nicht in die Kommandozeile.
- Fortsetzbar: ids, die schon in ``ergebnis.jsonl`` stehen, werden
  uebersprungen.
- Am Ende wird das Zeitfenster in UTC ausgegeben und in
  ``<ergebnis>.zeitfenster`` angehaengt. Pruefungen gegen Prod verfaelschen
  Betriebsmessungen (Cache-Anteil, Schattenprotokoll); das Fenster muss
  dort ausgeschlossen werden.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time

FERN = (
    "K=$(grep ^EVIDORA_TEST_API_KEY /opt/Evidora/website/.env | cut -d= -f2-); "
    'curl -sN -m {timeout} -H "Content-Type: application/json" '
    '-H "X-Evidora-Test-Key: $K" -d @- http://127.0.0.1:8000/api/check'
)

_DATA = re.compile(r"^data: (\{.*\})\s*$", re.M)


def normalisiere(eingaben: list) -> list[dict]:
    """Beliebige Eingabeliste -> [{"id": str, "claim": str}] in Reihenfolge."""
    aus = []
    for i, e in enumerate(eingaben):
        if isinstance(e, str):
            aus.append({"id": str(i + 1), "claim": e})
            continue
        claim = e.get("claim") or e.get("text")
        if not claim:
            raise ValueError(f"Eintrag {i} ohne claim/text: {e!r}")
        kennung = e.get("id", e.get("nr", i + 1))
        aus.append({"id": str(kennung), "claim": claim})
    ids = [e["id"] for e in aus]
    if len(set(ids)) != len(ids):
        raise ValueError("ids sind nicht eindeutig")
    return aus


def werte_sse_aus(roh: str) -> dict:
    """SSE-Antwort von /api/check -> das Wesentliche. Kaputte Zeilen werden uebersprungen."""
    ergebnis, cache, fehler, schritte = {}, False, None, []
    for m in _DATA.finditer(roh or ""):
        try:
            d = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        if not isinstance(d, dict):
            continue
        if "step" in d:
            schritte.append(d["step"])
            if d["step"] == "cache_hit":
                cache = True
        if "detail" in d:
            fehler = d["detail"]
        if "verdict" in d:
            ergebnis = d
    quellen = ergebnis.get("source_coverage") or ergebnis.get("sources") or []
    return {
        "verdict": ergebnis.get("verdict"),
        "confidence": ergebnis.get("confidence"),
        "cache": cache,
        "fehler": fehler,
        "schritte": schritte,
        "quellen": [q.get("source") or q.get("name") for q in quellen if isinstance(q, dict)]
                   if isinstance(quellen, list) else quellen,
        "evidence": [{"url": e.get("url"), "source": e.get("source"), "titel": (e.get("title") or "")[:160]}
                     for e in (ergebnis.get("evidence") or []) if isinstance(e, dict)],
        "summary": ergebnis.get("summary") or "",
        "nuance": ergebnis.get("nuance") or "",
    }


def schon_erledigt(pfad: str) -> set[str]:
    if not os.path.exists(pfad):
        return set()
    ids = set()
    with open(pfad, encoding="utf-8") as f:
        for zeile in f:
            try:
                ids.add(str(json.loads(zeile)["id"]))
            except (json.JSONDecodeError, KeyError):
                continue
    return ids


def pruefe(ssh: str, claim: str, timeout: int = 300, lauf=subprocess.run) -> str:
    """Eine Pruefung. Der Claim reist per stdin, der Key bleibt auf dem Server."""
    try:
        r = lauf(["ssh", "-o", "ConnectTimeout=20", ssh, FERN.format(timeout=timeout)],
                 input=json.dumps({"claim": claim}, ensure_ascii=False),
                 capture_output=True, text=True, timeout=timeout + 30)
        return r.stdout
    except subprocess.TimeoutExpired:
        return ""


def main(argv: list[str] | None = None, lauf=subprocess.run) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("eingaben")
    ap.add_argument("ausgabe")
    ap.add_argument("--ssh", default=os.environ.get("EVIDORA_SSH"),
                    help="user@host (Standard: $EVIDORA_SSH, siehe CLAUDE.local.md)")
    ap.add_argument("--timeout", type=int, default=300)
    a = ap.parse_args(argv)
    if not a.ssh:
        print("Kein Ziel: --ssh oder EVIDORA_SSH setzen (siehe CLAUDE.local.md).", file=sys.stderr)
        return 2
    with open(a.eingaben, encoding="utf-8") as f:
        eingaben = normalisiere(json.load(f))
    erledigt = schon_erledigt(a.ausgabe)
    offen = [e for e in eingaben if e["id"] not in erledigt]
    print(f"{len(offen)} offen, {len(erledigt)} schon erledigt", flush=True)
    if not offen:
        return 0                     # nichts gegen Prod geschickt -> kein Zeitfenster
    beginn = dt.datetime.now(dt.timezone.utc)
    with open(a.ausgabe, "a", encoding="utf-8") as out:
        for e in offen:
            t0 = time.time()
            r = werte_sse_aus(pruefe(a.ssh, e["claim"], a.timeout, lauf=lauf))
            r.update({"id": e["id"], "claim": e["claim"], "dauer_s": round(time.time() - t0, 1),
                      "zeit_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")})
            out.write(json.dumps(r, ensure_ascii=False) + "\n")
            out.flush()
            print(f"{e['id']:<8} {str(r['verdict']):13s} {r['confidence']} ev={len(r['evidence'])} "
                  f"{'CACHE ' if r['cache'] else ''}{r['dauer_s']}s | {e['claim'][:60]}", flush=True)
    ende = dt.datetime.now(dt.timezone.utc)
    fenster = f"{beginn:%Y-%m-%d %H:%M:%S}–{ende:%H:%M:%S} UTC: {len(offen)} Pruefungen ({os.path.basename(a.eingaben)})"
    with open(a.ausgabe + ".zeitfenster", "a", encoding="utf-8") as f:
        f.write(fenster + "\n")
    print("Zeitfenster:", fenster, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
