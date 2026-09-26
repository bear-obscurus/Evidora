"""Mess-Werkzeug fuer den englischen Trigger-Pass (services/_englisch.py).

Ruft die ECHTE ``find_matching_items`` auf — aus einem waehlbaren Backend-
Verzeichnis. Dieselbe Messung laeuft so gegen einen Worktree von ``main``
(Status quo) und gegen den Branch; der Vergleich der beiden Roh-Dateien ist
die Gegenprobe.

    # 1. Roh-Treffer erzeugen (je Backend einmal)
    python tools/englisch_messung.py --backend /pfad/zu/main/website/backend \\
        --out /tmp/main.json
    python tools/englisch_messung.py --out /tmp/branch.json

    # 2. Bericht: Baseline, Rueckgewinnung, Ueber-Trigger, Muss-Treffer
    python tools/englisch_messung.py --bericht /tmp/main.json /tmp/branch.json

    # 3. Instrument gegenpruefen: ein absichtlich vergiftetes Glossar
    #    ("the" -> "oesterreich") MUSS im Ueber-Trigger-Sweep anschlagen —
    #    und mit aufgezwungen offenem Gate auch bei den DEUTSCHEN Claims
    python tools/englisch_messung.py --gegenprobe --out /tmp/gift.json
    python tools/englisch_messung.py --ohne-gate --out /tmp/offen.json
    python tools/englisch_messung.py --bericht /tmp/main.json /tmp/gift.json

Population (wie #205): Fakten MIT Trigger-Feldern (trigger_keywords /
trigger_composite / trigger_all). Fakten ohne Trigger-Felder (at_factbook,
dach_factbook) laufen NICHT durch den Matcher — sie haben keinen.

Claims:
  de           die 2.603 dokumentierten claim_phrasings_handled
  en           624 englische Claims aus tools/englisch_korpus.json
  themenfremd  40 englische Claims ohne Bezug zu einem Fakt
  live         der Live-Claim aus der Produktions-Messung

Keine Netzabfrage, kein Modell.
"""

from __future__ import annotations

import argparse
import glob
import importlib
import json
import os
import sys
from collections import Counter

HIER = os.path.dirname(os.path.abspath(__file__))
BACKEND_DEFAULT = os.path.dirname(HIER)
KORPUS_DEFAULT = os.path.join(HIER, "englisch_korpus.json")
TRIGGER_FELDER = ("trigger_keywords", "trigger_composite", "trigger_all")


def population(backend: str) -> list[tuple[str, str, str]]:
    """(Pfad, items_key, Dateiname) fuer jede data-Datei mit Trigger-Fakten."""
    out = []
    for p in sorted(glob.glob(os.path.join(backend, "data", "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if isinstance(v, list) and any(
                    isinstance(it, dict) and any(it.get(t) for t in TRIGGER_FELDER)
                    for it in v):
                out.append((p, k, os.path.basename(p)))
    return out


def fakt_id(it: dict) -> str:
    return it.get("id") or it.get("topic") or ""


def claims(backend: str, korpus_pfad: str) -> list[dict]:
    """Alle Mess-Claims mit ihrem Soll-Fakt."""
    out = []
    for p, k, datei in population(backend):
        with open(p, encoding="utf-8") as fh:
            items = json.load(fh)[k]
        for it in items:
            if not isinstance(it, dict) or not any(it.get(t) for t in TRIGGER_FELDER):
                continue
            for ph in it.get("claim_phrasings_handled") or []:
                out.append({"art": "de", "datei": datei, "id": fakt_id(it),
                            "text": ph})
    with open(korpus_pfad, encoding="utf-8") as fh:
        korpus = json.load(fh)
    for c in korpus["claims"]:
        out.append({"art": "en", "datei": c["datei"], "id": c["id"],
                    "split": c["split"], "text": c["en"], "de": c["de"]})
    for t in korpus["themenfremd"]:
        out.append({"art": "themenfremd", "text": t})
    for l in korpus["live"]:
        datei, fid = l["erwartet_fakt"]
        out.append({"art": "live", "datei": datei, "id": fid, "text": l["en"]})
    return out


def vergifte_glossar(mod) -> None:
    """Gegenprobe: Ein Glossar-Eintrag, der auf JEDEM englischen Claim
    zuendet ("the") und einen Begriff glossiert, der in 114 Composites steht.
    Der Sweep muss das als Ueber-Trigger melden — sonst ist er blind."""
    mod.GLOSSAR = mod.GLOSSAR + (("österreich", ("the",)),
                                 ("gefährlich", ("is",)))
    mod._index.cache_clear()
    mod._fassung.cache_clear()


def roh_messung(backend: str, korpus_pfad: str, gegenprobe: bool = False,
                gate_streng: bool = False, ohne_gate: bool = False) -> dict:
    sys.path.insert(0, backend)
    for name in list(sys.modules):
        if name == "services" or name.startswith("services."):
            del sys.modules[name]
    tm = importlib.import_module("services._topic_match")
    if gegenprobe:
        vergifte_glossar(importlib.import_module("services._englisch"))
    if gate_streng:
        # Nur das Funktionswort-Gate, ohne die Stichwort-Erweiterung
        importlib.import_module("services._englisch").VOKABEL_EVIDENZ_MIND = 10 ** 6
    if ohne_gate:
        # Gegenprobe fuer den DEUTSCHEN Sweep: Gate aufgezwungen, jeder Claim
        # laeuft durch den englischen Pass. „die" -> „sterben", „war" ->
        # „krieg" — der Sweep MUSS jetzt auch bei deutschen Claims anschlagen.
        en = importlib.import_module("services._englisch")
        en.englisch_gate = lambda claim: True
        en._fassung.cache_clear()
    pop = population(backend)
    ergebnis = []
    for c in claims(backend, korpus_pfad):
        treffer = {}
        for p, k, datei in pop:
            m = tm.find_matching_items(p, k, claim_lc=c["text"].lower(),
                                       full_claim=c["text"], descriptor_fn=None)
            if m:
                treffer[datei] = [[fakt_id(x), bool((x.get("data") or {})
                                   .get("_matched_exact", True))] for x in m]
        ergebnis.append({**c, "treffer": treffer})
    return {"backend": backend, "gegenprobe": gegenprobe,
            "gate_streng": gate_streng, "ohne_gate": ohne_gate,
            "claims": ergebnis}


# ---------------------------------------------------------------------------
# Bericht
# ---------------------------------------------------------------------------

def _eigen(c: dict) -> tuple[bool, bool]:
    """(trifft eigenen Fakt, davon exakt)"""
    for fid, exakt in c["treffer"].get(c.get("datei"), []):
        if fid == c["id"]:
            return True, exakt
    return False, False


def _fremd(c: dict) -> set[tuple[str, str]]:
    """Dienst-fremde Treffer: Fakten in einer ANDEREN data-Datei."""
    return {(d, fid) for d, lst in c["treffer"].items()
            if d != c.get("datei") for fid, _ in lst}


def bericht(alt: dict, neu: dict) -> dict:
    a, n = alt["claims"], neu["claims"]
    assert len(a) == len(n) and all(x["text"] == y["text"] for x, y in zip(a, n))
    r: dict = {}

    # 1. Baseline + Muss-Treffer (deutsche Phrasings)
    de = [(x, y) for x, y in zip(a, n) if x["art"] == "de"]
    r["de_phrasings"] = len(de)
    r["de_exakt_alt"] = sum(_eigen(x) == (True, True) for x, _ in de)
    r["de_exakt_neu"] = sum(_eigen(y) == (True, True) for _, y in de)
    r["muss_treffer_verloren"] = [y["text"] for x, y in de
                                  if _eigen(x) == (True, True)
                                  and _eigen(y) != (True, True)]

    # 2. Rueckgewinnung (englischer Korpus)
    for split in ("dev", "test", "alle"):
        en = [(x, y) for x, y in zip(a, n) if x["art"] == "en"
              and (split == "alle" or x["split"] == split)]
        verloren = [(x, y) for x, y in en if not _eigen(x)[0]]
        zurueck = [(x, y) for x, y in verloren if _eigen(y)[0]]
        r[f"en_{split}"] = {
            "claims": len(en),
            "trifft_alt": sum(_eigen(x)[0] for x, _ in en),
            "trifft_neu": sum(_eigen(y)[0] for _, y in en),
            "verloren_alt": len(verloren),
            "zurueck": len(zurueck),
            "davon_exakt_markiert": sum(_eigen(y)[1] for _, y in zurueck),
            "eigener_fakt_weg": sum(_eigen(x)[0] and not _eigen(y)[0]
                                    for x, y in en),
        }

    # 3. Ueber-Trigger-Sweep: NEUE dienst-fremde Treffer gegen den Status quo
    geprueft = Counter()
    for art in ("de", "en", "themenfremd", "live"):
        paare = [(x, y) for x, y in zip(a, n) if x["art"] == art]
        neu_fremd = []
        status_quo = 0
        for x, y in paare:
            f_alt, f_neu = _fremd(x), _fremd(y)
            status_quo += len(f_alt)
            for d, fid in sorted(f_neu - f_alt):
                neu_fremd.append((y["text"], d, fid))
        r[f"ueber_{art}"] = {"claims": len(paare), "status_quo_fremd": status_quo,
                             "neu_fremd": len(neu_fremd), "beispiele": neu_fremd}
        geprueft[art] = len(paare)

    # 3b. Deckungsgleich mit Deutsch? Ein neuer englischer Fremdtreffer, den
    # das deutsche Quell-Phrasing desselben Fakts im Status quo AUCH hat, ist
    # kein Ueber-Trigger des englischen Passes, sondern derselbe Querbezug
    # zwischen zwei Packs (Cookie-Banner in datenschutz UND cybersecurity).
    de_fremd = {x["text"]: _fremd(x) for x in a if x["art"] == "de"}
    gleich, echt = [], []
    for x, y in zip(a, n):
        if x["art"] != "en":
            continue
        for d, fid in sorted(_fremd(y) - _fremd(x)):
            (gleich if (d, fid) in de_fremd.get(x["de"], set()) else echt).append(
                (y["text"], d, fid))
    r["ueber_en_deckungsgleich_mit_deutsch"] = len(gleich)
    r["ueber_en_echt_neu"] = len(echt)
    r["ueber_en_echt_neu_beispiele"] = echt
    return r


def drucke(r: dict) -> None:
    p = lambda z, n: f"{z / n:.1%}" if n else "-"  # noqa: E731
    print("BASELINE (deutsche Phrasings, eigener Fakt exakt)")
    print(f"  alt {r['de_exakt_alt']}/{r['de_phrasings']} "
          f"({p(r['de_exakt_alt'], r['de_phrasings'])})   "
          f"neu {r['de_exakt_neu']}/{r['de_phrasings']}")
    print(f"  Muss-Treffer verloren: {len(r['muss_treffer_verloren'])}")
    print("\nRUECKGEWINNUNG (englischer Korpus, eigener Fakt in irgendeinem Pass)")
    for s in ("dev", "test", "alle"):
        e = r[f"en_{s}"]
        print(f"  {s:5s} trifft {e['trifft_alt']:3d} -> {e['trifft_neu']:3d} "
              f"von {e['claims']}   zurueck {e['zurueck']}/{e['verloren_alt']} "
              f"({p(e['zurueck'], e['verloren_alt'])})   "
              f"eigener Fakt weg: {e['eigener_fakt_weg']}")
    print("\nUEBER-TRIGGER (neue dienst-fremde Treffer gegen den Status quo)")
    for art in ("de", "en", "themenfremd", "live"):
        u = r[f"ueber_{art}"]
        print(f"  {art:11s} {u['claims']:5d} Claims   Status quo fremd "
              f"{u['status_quo_fremd']:5d}   NEU {u['neu_fremd']}")
        if art != "en":
            for t, d, fid in u["beispiele"][:60]:
                print(f"      + {d}:{fid}  <- {t}")
    print(f"\n  davon EN deckungsgleich mit dem deutschen Quell-Phrasing: "
          f"{r['ueber_en_deckungsgleich_mit_deutsch']}")
    print(f"  davon EN echt neu (das deutsche Phrasing trifft den Fakt nicht): "
          f"{r['ueber_en_echt_neu']}")
    for t, d, fid in r["ueber_en_echt_neu_beispiele"]:
        print(f"      + {d}:{fid}  <- {t}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", default=BACKEND_DEFAULT)
    ap.add_argument("--korpus", default=KORPUS_DEFAULT)
    ap.add_argument("--out")
    ap.add_argument("--gegenprobe", action="store_true")
    ap.add_argument("--ohne-gate", action="store_true",
                    help="Gegenprobe: Sprach-Gate aufgezwungen offen")
    ap.add_argument("--gate-streng", action="store_true",
                    help="nur Funktionswort-Gate (ohne Stichwort-Erweiterung)")
    ap.add_argument("--bericht", nargs=2, metavar=("ALT", "NEU"))
    ap.add_argument("--json", action="store_true",
                    help="Bericht als JSON statt Text")
    args = ap.parse_args()
    if args.bericht:
        with open(args.bericht[0], encoding="utf-8") as fh:
            alt = json.load(fh)
        with open(args.bericht[1], encoding="utf-8") as fh:
            neu = json.load(fh)
        r = bericht(alt, neu)
        if args.json:
            print(json.dumps(r, ensure_ascii=False, indent=1))
        else:
            drucke(r)
        return
    roh = roh_messung(os.path.abspath(args.backend), args.korpus,
                      args.gegenprobe, args.gate_streng, args.ohne_gate)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(roh, fh, ensure_ascii=False)
    print(f"{len(roh['claims'])} Claims gemessen -> {args.out}")


if __name__ == "__main__":
    main()
