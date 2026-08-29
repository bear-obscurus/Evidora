#!/usr/bin/env python3
"""Retrieval-Zensus: WARUM ist ein Claim gescheitert?

Warum es das gibt
-----------------
Zwei grosse QA-Laeufe sagen unabhaengig dasselbe: der dominante Fehlermodus
ist nicht, dass Evidora falsch SCHLIESST, sondern dass es nichts zu
schliessen hat. QA100: `evidence_n=0` bei 42 von 100 Claims. QA50D: 14 von
50 deterministische Abdeckungsluecken. Gleichzeitig feuerte die L4-Override-
Kaskade auf 155 Durchlaeufe nur 3 Mal (G2/H/I/J/K kein einziges Mal) —
dorthin ist trotzdem der Grossteil der Arbeit geflossen.

Bevor daraus eine Roadmap wird, muss die Verteilung GEMESSEN sein. Dieses
Projekt hat mehrfach teuer gelernt, dass plausible Ursachen-Hypothesen
falsch sind (die „Rauschen verdraengt Inhalt"-Hypothese war es; die
Ueber-Trigger-Quoten waren ein Artefakt des eigenen Test-Suffixes).

Das Werkzeug kostet KEIN Guthaben: es wertet die bereits vorhandenen
QA-Rohdaten aus. Kein LLM-Call, kein Netz.

Die vier Klassen
----------------
  A  NICHTS ABGERUFEN      keine einzige Quelle lieferte einen Treffer
  B  KEINE EVIDENZ, FALSCH  Treffer da, evidence_n=0, Verdict falsch
  E  KEINE EVIDENZ, RICHTIG Treffer da, evidence_n=0, Verdict trotzdem
                            korrekt — also aus dem Vorwissen des Modells
                            statt aus Quellen
  C  EVIDENZ + VERDICT OK
  D  EVIDENZ, VERDICT FALSCH

**Nur D ist ein Befund der Verdict-Logik.** A und B sind Retrieval- bzw.
Abdeckungs-Befunde und brauchen Fakten oder bessere Queries, keine weiteren
Override-Muster.

**E ist die unbequeme Klasse.** Ein Faktencheck-Dienst, der die richtige
Antwort ohne zitierbare Evidenz gibt, hat nicht recht — er hat Glueck
gehabt. Diese Faelle zaehlen in jeder PASS-Quote als Erfolg und verdecken
dieselbe Abdeckungsluecke wie B. Sie gehoeren deshalb ausgewiesen, nicht
weggerechnet.

Mit ``--triggers`` kommt die entscheidende Zusatz-Information: welche
Quellen haetten laut ihrem Trigger-Gate feuern MUESSEN, tauchen aber nicht
unter den Treffern auf. Das trennt „kein Trigger" (Abdeckungs-/Trigger-
Luecke) von „Trigger feuerte, Quelle lieferte 0" — und genau in der zweiten
Gruppe wuerde sich ein Sprachproblem zeigen (deutscher Claim gegen
englischsprachigen Volltext-Index).

⚠️ Suffix-Warnung: Die QA-Laeufe haben den Claims ein Cache-Bust-Suffix
angehaengt (`claim_sent`), und in `(Pruefsatz-NNN)` steckt z. B. `efsa`.
Der Zensus rechnet deshalb standardmaessig mit dem SAUBEREN `claim` und
weist die Differenz zu `claim_sent` getrennt aus (--suffix-diff).

Aufruf
------
  python3 tools/retrieval_census.py --claims qa50d_claims.json \\
                                    --results qa50d_main.jsonl
  ... [--triggers] [--suffix-diff] [--json bericht.json]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter

# tools/ liegt unter backend/ — services/ ist eine Ebene hoeher.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Klassen-Kuerzel -> Klartext
KLASSEN = {
    "A": "NICHTS ABGERUFEN (keine Quelle lieferte Treffer)",
    "B": "KEINE EVIDENZ + Verdict FALSCH",
    "E": "KEINE EVIDENZ, Verdict richtig (aus Vorwissen, nicht aus Quellen)",
    "C": "EVIDENZ + Verdict richtig",
    "D": "EVIDENZ, Verdict FALSCH  <- Verdict-Logik",
}
KLASSEN_ORDER = ("A", "B", "E", "C", "D")
RETRIEVAL_KLASSEN = ("A", "B", "E")


# ---------------------------------------------------------------------------
# Klassifikation (rein, ohne I/O — das ist der testbare Kern)
# ---------------------------------------------------------------------------

def verdict_ok(verdict: str | None, expected: str | None,
               acceptable: list | None) -> bool:
    """Verdict gilt als korrekt, wenn es `expected` trifft ODER in
    `acceptable` steht (die QA-Definitionen fuehren beides)."""
    if not verdict:
        return False
    if expected and verdict == expected:
        return True
    return verdict in (acceptable or [])


def classify(result: dict, expected: str | None,
             acceptable: list | None) -> str:
    """Ein Ergebnis-Record in A/B/C/D einordnen.

    Defensiv: fehlende Zaehler gelten als 0 — ein unvollstaendiger Record
    darf den Zensus nicht abbrechen, sondern soll konservativ als
    Retrieval-Problem erscheinen (das ist die Richtung, die zu weiterem
    Hinsehen fuehrt, nicht die, die etwas verschweigt).
    """
    sources_n = result.get("sources_n")
    if not isinstance(sources_n, int):
        sources_n = len(result.get("sources") or [])
    evidence_n = result.get("evidence_n")
    if not isinstance(evidence_n, int):
        evidence_n = 0

    ok = verdict_ok(result.get("verdict"), expected, acceptable)
    if sources_n <= 0:
        return "A"
    if evidence_n <= 0:
        return "B" if not ok else "E"
    return "C" if ok else "D"


def join_runs(claims: list[dict], results: list[dict]) -> list[dict]:
    """Claims und Ergebnisse ueber die id zusammenfuehren.

    Ergebnisse ohne passenden Claim (und umgekehrt) werden gemeldet statt
    still verworfen — eine stille Luecke wuerde die Quoten verfaelschen.
    """
    by_id = {c.get("id"): c for c in claims}
    joined, verwaist = [], []
    for r in results:
        c = by_id.get(r.get("id"))
        if c is None:
            verwaist.append(r.get("id"))
            continue
        joined.append({
            "id": r.get("id"),
            "klasse_qa": c.get("klasse", ""),
            "claim": c.get("claim", ""),
            "claim_sent": r.get("claim_sent", ""),
            "expected": c.get("expected"),
            "acceptable": c.get("acceptable") or [],
            "verdict": r.get("verdict"),
            "confidence": r.get("confidence"),
            "sources": r.get("sources") or [],
            "sources_n": r.get("sources_n", len(r.get("sources") or [])),
            "evidence_n": r.get("evidence_n", 0),
            "zensus": classify(r, c.get("expected"), c.get("acceptable")),
        })
    fehlend = sorted(set(by_id) - {r.get("id") for r in results})
    return joined, verwaist, fehlend


# ---------------------------------------------------------------------------
# Trigger-Simulation (optional, offline)
# ---------------------------------------------------------------------------

_DISPATCH_RE = re.compile(
    r"if\s+(claim_mentions_\w+_cached)\(claim\):\s*\n"
    r"\s*tasks\.append\(cached\(\s*\"([^\"]+)\"",
    re.MULTILINE,
)


def parse_dispatches(main_py: str) -> list[tuple[str, str]]:
    """(Gate-Funktionsname, Quellen-Label) aus main.py — per Regex ueber das
    feste Dispatch-Muster. Bewusst kein Import von main.py: das laedt SpaCy
    und sentence-transformers, was hier nichts beitraegt."""
    return _DISPATCH_RE.findall(main_py)


# Beide Import-Stile: einzeilig und geklammert-mehrzeilig.
_IMPORT_RE = re.compile(
    r"^from\s+services\.(\w+)\s+import\s+(\([^)]*\)|[^\n]*)", re.MULTILINE,
)
# Proxy fuer die Gesamtzahl der Dispatch-Punkte — zur ehrlichen
# Abdeckungs-Angabe, damit eine Teilabdeckung nicht als Vollbild durchgeht.
_QUERIED_RE = re.compile(r"queried_names\.append\(")


def parse_gate_modules(main_py: str) -> dict[str, str]:
    """Gate-Funktionsname -> Modulname, direkt aus den Import-Zeilen von
    main.py gelesen.

    Warum nicht raten: Modulnamen folgen dem Gate-Namen NICHT verlaesslich
    (`claim_mentions_cat_cached` sitzt nicht in `services/cat.py`). Geraten
    hatte 12 von 137 Gates verfehlt — und ein Zensus mit stiller
    Teilabdeckung produziert eine huebsche, falsche Zahl.
    """
    mapping: dict[str, str] = {}
    for modul, namen in _IMPORT_RE.findall(main_py):
        for name in namen.replace("(", " ").replace(")", " ").split(","):
            name = name.strip()
            if name.startswith("claim_mentions_"):
                mapping[name] = modul
    return mapping


def load_gates(dispatches: list[tuple[str, str]],
               gate_modules: dict[str, str]) -> tuple[dict, list[str]]:
    """Gate-Funktionen importieren. Gibt (name -> callable, Fehlschlaege).

    Fehlschlaege werden GEMELDET, nicht verschwiegen — ein Zensus, der
    stillschweigend einen Teil der Gates ueberspringt, produziert eine
    huebsche und falsche Zahl.
    """
    import importlib

    gates, fehler = {}, []
    for gate_name, _label in dispatches:
        modul = gate_modules.get(gate_name)
        if not modul:
            fehler.append(f"{gate_name} (kein Import gefunden)")
            continue
        try:
            m = importlib.import_module(f"services.{modul}")
            fn = getattr(m, gate_name, None)
        except Exception as e:  # noqa: BLE001
            fehler.append(f"{gate_name} ({type(e).__name__})")
            continue
        if callable(fn):
            gates[gate_name] = fn
        else:
            fehler.append(f"{gate_name} (nicht in services.{modul})")
    return gates, fehler


def simulate_triggers(claim: str, gates: dict) -> set[str]:
    """Welche Gates sagen fuer diesen Claim JA? Fehler eines einzelnen Gates
    duerfen den Lauf nicht kippen."""
    treffer = set()
    for name, fn in gates.items():
        try:
            if fn(claim):
                treffer.add(name)
        except Exception:
            continue
    return treffer


# ---------------------------------------------------------------------------
# Bericht
# ---------------------------------------------------------------------------

def quellen_statistik(joined: list[dict]) -> Counter:
    """Wie oft liefert jede Quelle ueber den Korpus Treffer? Das ist der
    billige Ueber-Trigger-Detektor aus den QA-Lehren — Zielgroesse fuer
    einen Themen-Pack ist einstellig."""
    c = Counter()
    for row in joined:
        for s in row["sources"]:
            c[s] += 1
    return c


def _kurz(quelle: str, n: int = 46) -> str:
    return quelle if len(quelle) <= n else quelle[:n - 1] + "…"


def bericht(joined, verwaist, fehlend, trigger_info=None, out=sys.stdout):
    n = len(joined)
    p = lambda *a: print(*a, file=out)
    p(f"\n{'=' * 72}\nRETRIEVAL-ZENSUS — {n} Claims\n{'=' * 72}")
    if verwaist:
        p(f"⚠️  {len(verwaist)} Ergebnis(se) ohne passenden Claim: {verwaist}")
    if fehlend:
        p(f"⚠️  {len(fehlend)} Claim(s) ohne Ergebnis: {fehlend}")

    zaehler = Counter(r["zensus"] for r in joined)
    p("\nKLASSEN-BILANZ")
    for k in KLASSEN_ORDER:
        anz = zaehler.get(k, 0)
        p(f"  {k}  {anz:3d}  ({anz / n * 100:5.1f} %)  {KLASSEN[k]}")

    falsch = zaehler.get("B", 0) + zaehler.get("D", 0) + zaehler.get("A", 0)
    ohne_ev = sum(zaehler.get(k, 0) for k in RETRIEVAL_KLASSEN)
    p(f"\n  Falsche Verdicts gesamt: {falsch} ({falsch / n * 100:.1f} %)")
    if falsch:
        p(f"    davon Retrieval-Ursache (A+B): {zaehler.get('A', 0) + zaehler.get('B', 0)}"
          f"  ({(zaehler.get('A', 0) + zaehler.get('B', 0)) / falsch * 100:.0f} % aller Fehler)")
        p(f"    davon Verdict-Logik  (D):      {zaehler.get('D', 0)}"
          f"  ({zaehler.get('D', 0) / falsch * 100:.0f} % aller Fehler)")
    p(f"  Ohne jede Evidenz beantwortet (A+B+E): {ohne_ev} ({ohne_ev / n * 100:.1f} %)"
      f"  — davon {zaehler.get('E', 0)} zufaellig richtig")

    for k in ("A", "B", "E", "D"):
        rows = [r for r in joined if r["zensus"] == k]
        if not rows:
            continue
        p(f"\n{'-' * 72}\nKLASSE {k} — {KLASSEN[k]}\n{'-' * 72}")
        for r in rows:
            p(f"  #{r['id']}  {r['verdict']}@{r['confidence']} "
              f"(erwartet {r['expected']})  quellen={r['sources_n']} "
              f"evidenz={r['evidence_n']}")
            p(f"      {r['claim'][:88]}")
            if k in ("B", "E") and r["sources"]:
                p(f"      Treffer von: {', '.join(_kurz(s, 34) for s in r['sources'][:4])}")

    stat = quellen_statistik(joined)
    if stat:
        p(f"\n{'-' * 72}\nQUELLEN-TREFFERQUOTE (Ueber-Trigger-Detektor)\n{'-' * 72}")
        for quelle, anz in stat.most_common(15):
            p(f"  {anz:3d}/{n}  ({anz / n * 100:5.1f} %)  {_kurz(quelle)}")

    if trigger_info:
        p(f"\n{'-' * 72}\nTRIGGER FEUERTE, QUELLE LIEFERTE NICHTS\n{'-' * 72}")
        p(f"  Gates importiert: {trigger_info['gates_ok']}"
          f" / {trigger_info['gates_total']} erkannte Dispatch-Punkte"
          f" (von {trigger_info['dispatch_punkte']} im Code)")
        if trigger_info["gates_fehler"]:
            p(f"  ⚠️  nicht importierbar: "
              f"{', '.join(trigger_info['gates_fehler'][:8])}"
              f"{' …' if len(trigger_info['gates_fehler']) > 8 else ''}")
        p("  (hohe Werte hier = Trigger ok, aber die Quelle antwortet nicht"
          " —\n   genau dort saesse ein Sprach-/Query-Problem)")
        for quelle, anz in trigger_info["stumm"].most_common(15):
            p(f"  {anz:3d}/{n}  ({anz / n * 100:5.1f} %)  {_kurz(quelle)}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--claims", required=True, help="claims.json (mit expected/acceptable)")
    ap.add_argument("--results", required=True, help="results.jsonl aus dem QA-Lauf")
    ap.add_argument("--triggers", action="store_true",
                    help="Trigger-Gates offline simulieren (importiert services/)")
    ap.add_argument("--suffix-diff", action="store_true",
                    help="zusaetzlich mit claim_sent (inkl. Cache-Bust-Suffix) rechnen")
    ap.add_argument("--main-py", default="main.py")
    ap.add_argument("--json", help="Bericht zusaetzlich als JSON schreiben")
    args = ap.parse_args(argv)

    with open(args.claims, encoding="utf-8") as f:
        claims_doc = json.load(f)
    claims = claims_doc.get("claims", claims_doc) if isinstance(claims_doc, dict) else claims_doc
    with open(args.results, encoding="utf-8") as f:
        results = [json.loads(line) for line in f if line.strip()]

    joined, verwaist, fehlend = join_runs(claims, results)
    if not joined:
        print("Keine gemeinsamen ids — passen claims und results zusammen?",
              file=sys.stderr)
        return 2

    trigger_info = None
    if args.triggers:
        with open(args.main_py, encoding="utf-8") as f:
            main_src = f.read()
        dispatches = parse_dispatches(main_src)
        gates, fehler = load_gates(dispatches, parse_gate_modules(main_src))
        gate_to_label = dict(dispatches)
        stumm = Counter()
        for row in joined:
            feuernd = simulate_triggers(row["claim"], gates)
            geliefert = " | ".join(row["sources"]).lower()
            for g in feuernd:
                label = gate_to_label.get(g, g)
                if label.lower() not in geliefert:
                    stumm[label] += 1
            row["trigger_n"] = len(feuernd)
        trigger_info = {
            "gates_ok": len(gates), "gates_total": len(dispatches),
            "dispatch_punkte": len(_QUERIED_RE.findall(main_src)),
            "gates_fehler": fehler, "stumm": stumm,
        }

    if args.suffix_diff:
        with open(args.main_py, encoding="utf-8") as f:
            main_src = f.read()
        dispatches = parse_dispatches(main_src)
        gates, _ = load_gates(dispatches, parse_gate_modules(main_src))
        rein = sum(len(simulate_triggers(r["claim"], gates)) for r in joined)
        mit = sum(len(simulate_triggers(r["claim_sent"] or r["claim"], gates))
                  for r in joined)
        print(f"\nSUFFIX-EFFEKT: Trigger-Treffer sauber {rein} vs. "
              f"mit Cache-Bust-Suffix {mit} "
              f"(+{mit - rein}, {(mit - rein) / max(rein, 1) * 100:.1f} %)")

    bericht(joined, verwaist, fehlend, trigger_info)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({
                "n": len(joined),
                "klassen": Counter(r["zensus"] for r in joined),
                "claims": joined,
                "quellen": quellen_statistik(joined),
                "stumme_quellen": dict(trigger_info["stumm"]) if trigger_info else {},
            }, f, ensure_ascii=False, indent=1, default=dict)
        print(f"\nJSON geschrieben: {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
