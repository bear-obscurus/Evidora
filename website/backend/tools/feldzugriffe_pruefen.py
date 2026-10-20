#!/usr/bin/env python3
"""Prueft, ob jedes Feld, das ein Service aus seiner JSON liest, dort steht.

Anlass (QA50I, 20.10.2026). Der Claim "WCAG 2.2 hat 87 Erfolgskriterien"
bekam ``false@0.8`` mit der Begruendung "WCAG 2.2 enthaelt 62
Erfolgskriterien". 62 ist frei erfunden. Die Ursache stand in
``services/wcag22.py``:

    total = key_facts.get("total_criteria", "?")

In PR #267 wurde ``total_criteria`` in ``gelistete_kriterien`` und
``aktive_kriterien`` aufgeteilt — diese Service-Zeile blieb stehen. Der
Service lieferte seither woertlich "? Success Criteria", und das Modell
hat die Luecke gefuellt.

Der Test aus #267 pruefte, dass das Feld in den DATEN weg ist. Dass der
SERVICE es nicht mehr liest, pruefte nichts. Genau diese Einseitigkeit
schliesst dieses Werkzeug: Es liest beide Seiten und vergleicht sie.

WIE ES ARBEITET
Keine Textsuche, sondern eine Taint-Analyse auf dem AST. Ausgehend vom
Loader eines Services (``_load_data()``, ``load_json_mtime_aware(...)``,
``json.load(...)``) wird verfolgt, welche Namen die geladenen Daten
tragen. Fuer jeden ``.get("feld")``-Zugriff auf einen solchen Namen wird
geprueft, ob ``feld`` in der JSON des Services als Schluessel vorkommt —
auf irgendeiner Ebene.

WARUM NAMENSBASIERT UND NICHT FLUSS-SENSITIV
Die Services geben das geladene Dict als Parameter ``data`` weiter
(``_build_result_row(iso3, rec, display_countries, scores, data)``). Eine
fluss-sensitive Analyse ueber Funktionsgrenzen waere genauer, aber
aufwendiger als der Befund wert ist; die Konvention ``data`` ist in
diesem Code durchgaengig. Parameter mit diesem Namen gelten deshalb als
geladene Daten, wenn das Modul einen Loader hat.

Exit-Codes: 0 = kein Feld fehlt, 1 = mindestens eines fehlt,
2 = Aufruf-/Lesefehler.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import sys

#: Welche Exit-Codes einen BEFUND bedeuten (Gate aus #259).
BEFUND_EXITS: tuple[int, ...] = (1,)

#: Namen, die auf einen Loader hindeuten — als GANZER Aufrufname, nicht
#: als Teilstring. ``json.load`` liest eine Datei, ``json.loads`` eine
#: Zeichenkette: In ``datacommons.py`` ist das der Rumpf einer
#: HTTP-Antwort, und deren Felder stehen in keiner JSON des Projekts. Eine
#: erste Fassung dieses Werkzeugs pruefte mit ``in`` und meldete deshalb
#: ``dataFeedElement`` als fehlendes Datenfeld — dieselbe Teilstring-Falle
#: wie "wie viel" in "wie viele" (#231).
LOADER_NAMEN = frozenset({
    "_load_data", "load_json_mtime_aware", "_load_static", "_laden",
    "json.load",
    # Die Pack-Services laden ueber diese zwei Helfer und bekommen die
    # LISTE der Fakten zurueck, nicht das Dict. Ohne sie blieben 60
    # Services ungeprueft — und genau dort liegen die meisten Felder.
    "load_items", "find_matching_items",
})

#: Parameternamen, die in diesem Code das geladene Dict tragen.
DATEN_PARAMETER = {"data", "daten"}

#: KEINE Ausnahmeliste. Eine erste Fassung fuehrte hier vier Feldnamen
#: ("results", "source", "type", "error") als "keine Datenfelder". Das war
#: Schuld mit Zinsen (#230): Die Liste haette jeden kuenftigen Fehlalarm
#: aufgenommen, statt seine Ursache zu benennen. Die Ursache ist in
#: `parst_zeichenketten` benannt, und die vier Namen stehen alle in den
#: Daten — sie waren nie ein Problem.
NICHT_DATENFELDER: frozenset[str] = frozenset()


def json_schluessel(pfad: str) -> set[str]:
    """Alle Schluessel der JSON, auf jeder Ebene."""
    with open(pfad, encoding="utf-8") as fh:
        wurzel = json.load(fh)
    aus: set[str] = set()

    def lauf(x):
        if isinstance(x, dict):
            for k, v in x.items():
                aus.add(k)
                lauf(v)
        elif isinstance(x, list):
            for v in x:
                lauf(v)

    lauf(wurzel)
    return aus


def _ist_loader(knoten: ast.AST, zusatz: frozenset[str] = frozenset()) -> bool:
    if not isinstance(knoten, ast.Call):
        return False
    name = ast.unparse(knoten.func)
    return name in LOADER_NAMEN or name in zusatz


def weiterleitende_funktionen(baum: ast.AST) -> frozenset[str]:
    """Modul-Funktionen, die geladene Daten DURCHREICHEN.

    Die Pack-Services schreiben nicht ``x = load_items(...)``, sondern

        def _claim_matches_facts(...):
            return find_matching_items(STATIC_JSON_PATH, "facts", ...)

    und spaeter ``matches = _claim_matches_facts(...)``. Ohne diesen
    Schritt waere der Taint an der Funktionsgrenze zu Ende, und 60 der 89
    Services blieben ungeprueft. Zwei Runden, damit auch eine zweite
    Weiterleitungsebene traegt.
    """
    weiter: set[str] = set()
    for _runde in range(3):
        vorher = len(weiter)
        for fn in ast.walk(baum):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for knoten in ast.walk(fn):
                if isinstance(knoten, ast.Return) and knoten.value is not None \
                        and _ist_loader(_entpacke(knoten.value),
                                        frozenset(weiter)):
                    weiter.add(fn.name)
        if len(weiter) == vorher:
            break
    return frozenset(weiter)


def _entpacke(knoten: ast.AST) -> ast.AST:
    """``x or {}`` und ``(x or {})`` auf ``x`` zurueckfuehren."""
    while isinstance(knoten, ast.BoolOp) and isinstance(knoten.op, ast.Or):
        knoten = knoten.values[0]
    return knoten


def getroffene_namen(baum: ast.AST) -> set[str]:
    """Die Namen, die geladene Daten tragen.

    Mehrere Durchlaeufe, weil eine Zuweisung vor ihrer Quelle stehen kann
    (Funktionsreihenfolge im Modul ist nicht die Ausfuehrungsreihenfolge).
    """
    weiter = weiterleitende_funktionen(baum)
    hat_loader = any(_ist_loader(n, weiter) for n in ast.walk(baum))
    namen: set[str] = set()
    if hat_loader:
        for fn in ast.walk(baum):
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for arg in list(fn.args.args) + list(fn.args.kwonlyargs):
                    if arg.arg in DATEN_PARAMETER:
                        namen.add(arg.arg)

    for _runde in range(6):
        vorher = len(namen)
        for knoten in ast.walk(baum):
            if not isinstance(knoten, (ast.Assign, ast.AnnAssign)):
                continue
            wert = knoten.value
            if wert is None:
                continue
            ziele = (knoten.targets if isinstance(knoten, ast.Assign)
                     else [knoten.target])
            wert = _entpacke(wert)
            quelle_getroffen = False
            if _ist_loader(wert, weiter):
                quelle_getroffen = True
            elif isinstance(wert, ast.Call) and isinstance(wert.func, ast.Attribute) \
                    and wert.func.attr == "get":
                basis = _entpacke(wert.func.value)
                quelle_getroffen = (isinstance(basis, ast.Name)
                                    and basis.id in namen)
            elif isinstance(wert, ast.Subscript):
                basis = _entpacke(wert.value)
                quelle_getroffen = (isinstance(basis, ast.Name)
                                    and basis.id in namen)
            if quelle_getroffen:
                for z in ziele:
                    if isinstance(z, ast.Name):
                        namen.add(z.id)
        # `for fact in <getroffen>:` — die Pack-Services lesen ihre Felder
        # von den ELEMENTEN der geladenen Liste, nicht von ihr selbst.
        for knoten in ast.walk(baum):
            if not isinstance(knoten, (ast.For, ast.AsyncFor)):
                continue
            quelle = _entpacke(knoten.iter)
            if isinstance(quelle, ast.Call) and isinstance(quelle.func, ast.Attribute) \
                    and quelle.func.attr == "get":
                quelle = _entpacke(quelle.func.value)
            if isinstance(quelle, ast.Subscript):
                quelle = _entpacke(quelle.value)
            if isinstance(quelle, ast.Name) and quelle.id in namen:
                if isinstance(knoten.target, ast.Name):
                    namen.add(knoten.target.id)
        # Und `[... for fact in <getroffen>]`
        for knoten in ast.walk(baum):
            if not isinstance(knoten, ast.comprehension):
                continue
            quelle = _entpacke(knoten.iter)
            if isinstance(quelle, ast.Name) and quelle.id in namen \
                    and isinstance(knoten.target, ast.Name):
                namen.add(knoten.target.id)
        if len(namen) == vorher:
            break
    return namen


def gelesene_felder(baum: ast.AST, namen: set[str]) -> list[tuple[str, int]]:
    """``(feldname, zeile)`` fuer jeden ``.get("feld")`` auf geladene Daten."""
    aus: list[tuple[str, int]] = []
    for knoten in ast.walk(baum):
        if not isinstance(knoten, ast.Call):
            continue
        if not (isinstance(knoten.func, ast.Attribute)
                and knoten.func.attr == "get"):
            continue
        basis = _entpacke(knoten.func.value)
        if not (isinstance(basis, ast.Name) and basis.id in namen):
            continue
        if not knoten.args:
            continue
        erstes = knoten.args[0]
        if isinstance(erstes, ast.Constant) and isinstance(erstes.value, str):
            aus.append((erstes.value, knoten.lineno))
    return aus


def json_dateien(baum: ast.AST) -> list[str]:
    """Die ``*.json``-Literale des Moduls."""
    aus = []
    for knoten in ast.walk(baum):
        if isinstance(knoten, ast.Constant) and isinstance(knoten.value, str) \
                and knoten.value.endswith(".json") and "/" not in knoten.value:
            if knoten.value not in aus:
                aus.append(knoten.value)
    return aus


def parst_zeichenketten(baum: ast.AST) -> bool:
    """Parst das Modul JSON aus einer ZEICHENKETTE, nicht aus einer Datei?

    ``json.loads`` heisst: Die Daten kommen aus dem Netz. Dann kann ein
    Name im Modul zwei Gestalten tragen — in ``datacommons.py`` heisst die
    Variable ``entry`` sowohl ein schema.org-ClaimReview aus der API als
    auch ein Eintrag des eigenen Caches, und die Felder unterscheiden
    sich. Eine automatische Zuordnung ist dort unsicher, also wird das
    Modul nicht geprueft.

    Gemessen trifft diese Regel GENAU EIN Modul (datacommons.py), dessen
    Cache es ausserdem selbst erzeugt — eine Drift zwischen Erzeuger und
    Leser ist dort nicht die Fehlerklasse, die dieses Werkzeug sucht.
    Die Regel ist damit praeziser als eine Ausnahmeliste und nennt ihren
    Grund.
    """
    for knoten in ast.walk(baum):
        if isinstance(knoten, ast.Call) and \
                ast.unparse(knoten.func) == "json.loads":
            return True
    return False


def pruefe_service(pfad: str, datenordner: str) -> list[tuple[str, str, int]]:
    """``(json, feld, zeile)`` fuer jedes gelesene Feld, das fehlt."""
    with open(pfad, encoding="utf-8") as fh:
        quelle = fh.read()
    baum = ast.parse(quelle)
    dateien = json_dateien(baum)
    if not dateien or parst_zeichenketten(baum):
        return []
    namen = getroffene_namen(baum)
    if not namen:
        return []
    felder = gelesene_felder(baum, namen)
    if not felder:
        return []

    vorhanden: set[str] = set()
    gelesen_aus: list[str] = []
    for name in dateien:
        p = os.path.join(datenordner, name)
        if os.path.exists(p):
            try:
                vorhanden |= json_schluessel(p)
                gelesen_aus.append(name)
            except (ValueError, OSError):
                continue
    if not gelesen_aus:
        return []

    fehlend = []
    for feld, zeile in sorted(set(felder)):
        if feld in NICHT_DATENFELDER or feld in vorhanden:
            continue
        fehlend.append((", ".join(gelesen_aus), feld, zeile))
    return fehlend


def main(argv=None) -> int:
    hier = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--services", default=os.path.join(hier, "services"))
    ap.add_argument("--data", default=os.path.join(hier, "data"))
    ap.add_argument("--leise", action="store_true")
    args = ap.parse_args(argv)

    if not os.path.isdir(args.services) or not os.path.isdir(args.data):
        print("services/ oder data/ fehlt", file=sys.stderr)
        return 2

    gepruefte = 0
    befunde: list[tuple[str, str, str, int]] = []
    for name in sorted(os.listdir(args.services)):
        if not name.endswith(".py") or name.startswith("_"):
            continue
        pfad = os.path.join(args.services, name)
        try:
            fehlend = pruefe_service(pfad, args.data)
        except SyntaxError as e:
            print(f"  {name}: parst nicht — {e}", file=sys.stderr)
            return 2
        gepruefte += 1
        for datei, feld, zeile in fehlend:
            befunde.append((name, datei, feld, zeile))

    print(f"{gepruefte} Services geprueft, {len(befunde)} gelesene Felder "
          f"fehlen in ihrer JSON")
    if befunde and not args.leise:
        print()
        for service, datei, feld, zeile in befunde:
            print(f"  {service}:{zeile}  liest {feld!r} — nicht in {datei}")
    return 1 if befunde else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
