#!/usr/bin/env python3
"""Wertet ein Log von latenz_probe.sh aus — je Zeitfenster und Endpunkt.

    python3 tools/messung/latenz_auswerten.py probe.log "vorher=18:38:00-18:39:49" "embedding=18:40:35-18:50:34"

Ohne Fenster wird das ganze Log ausgewertet. Je Fenster und Endpunkt:
Zahl der Proben, OK, Fehlschlaege (Code != 200), Median, p95, Maximum und
die Zahl ueber der Schwelle (``--schwelle``, Standard 0,5 s). Dazu die erste
und letzte fehlgeschlagene Probe, also das Ausfallfenster.

Entstanden aus dem 11.11.2026: Dort zaehlte ein Ad-hoc-Filter die falschen
Spalten und lieferte eine unbrauchbare Ausreisserliste. Die Spalten stehen
jetzt einmal fest und sind getestet.
"""

from __future__ import annotations

import argparse
import statistics
import sys


def lies(zeilen) -> list[dict]:
    """``HH:MM:SS legal CODE T health CODE T`` -> Zeilen; alles andere wird uebersprungen."""
    aus = []
    for z in zeilen:
        t = z.split()
        if len(t) != 7 or t[1] != "legal" or t[4] != "health":
            continue
        try:
            aus.append({"zeit": t[0], "legal": (t[2], float(t[3])), "health": (t[5], float(t[6]))})
        except ValueError:
            continue
    return aus


def statistik(zeilen: list[dict], endpunkt: str, schwelle: float = 0.5) -> dict:
    ok = [z[endpunkt][1] for z in zeilen if z[endpunkt][0] == "200"]
    fehl = [z["zeit"] for z in zeilen if z[endpunkt][0] != "200"]
    erg = {"n": len(zeilen), "ok": len(ok), "fehl": len(fehl),
           "ausfall_von": fehl[0] if fehl else None, "ausfall_bis": fehl[-1] if fehl else None}
    if ok:
        s = sorted(ok)
        erg.update(median=statistics.median(s), p95=s[int(0.95 * (len(s) - 1))], max=s[-1],
                   ueber=sum(v > schwelle for v in s))
    return erg


def im_fenster(zeilen: list[dict], von: str, bis: str) -> list[dict]:
    return [z for z in zeilen if von <= z["zeit"] < bis]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("log")
    ap.add_argument("fenster", nargs="*", help='name=HH:MM:SS-HH:MM:SS')
    ap.add_argument("--schwelle", type=float, default=0.5)
    a = ap.parse_args(argv)
    with open(a.log, encoding="utf-8") as f:
        zeilen = lies(f)
    if not zeilen:
        print("keine auswertbaren Zeilen")
        return 1
    angaben = a.fenster or [f"gesamt={zeilen[0]['zeit']}-99:99:99"]
    fenster = []
    for f_ in angaben:
        name, _, spanne = f_.partition("=")
        von, _, bis = spanne.partition("-")
        fenster.append((name, von, bis))
    for name, von, bis in fenster:
        teil = im_fenster(zeilen, von, bis)
        for ep in ("legal", "health"):
            s = statistik(teil, ep, a.schwelle)
            if s.get("ok"):
                print(f"{name:<24} {ep:<6} n={s['n']:4d} ok={s['ok']:4d} fehl={s['fehl']:3d} | "
                      f"Median {s['median']:.3f}s p95 {s['p95']:.3f}s max {s['max']:.3f}s | "
                      f">{a.schwelle}s: {s['ueber']}"
                      + (f" | Ausfall {s['ausfall_von']}–{s['ausfall_bis']}" if s["fehl"] else ""))
            else:
                print(f"{name:<24} {ep:<6} n={s['n']:4d} — keine erfolgreiche Probe")
    return 0


if __name__ == "__main__":
    sys.exit(main())
