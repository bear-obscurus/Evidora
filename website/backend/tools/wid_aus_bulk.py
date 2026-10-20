#!/usr/bin/env python3
"""Liest die WID-Ungleichheits-Anteile aus dem amtlichen Bulk-Download.

Anlass (20.10.2026). ``data/wid.json`` fuehrte Werte aus dem World
Inequality Report 2022 mit Datenjahr 2022. Der WIR 2026 ist am 10.12.2025
erschienen; in PR #267 wurde der Rueckstand benannt und der Vollrefresh
als offen vermerkt. Dies ist der Vollrefresh.

WOHER DIE ZAHLEN KOMMEN
``https://wid.world/bulk_download/`` liefert pro Land eine CSV mit allen
Reihen (``WID_data_<code>.csv``, 20-55 MB je Datei). Gelesen werden genau
zwei Variablen und drei Perzentile:

    sptincj992   Anteil am Pretax National Income, equal-split adults
    shwealj992   Anteil am Netto-Haushaltsvermoegen, equal-split adults

    p99p100      Top 1 %
    p90p100      Top 10 %
    p0p50        Untere 50 %

ACHTUNG BEI DEN VARIABLENNAMEN: Die gaengige Schreibweise in Aufsaetzen
ist ``sptinc992j``, im Bulk-Download heisst dieselbe Reihe aber
``sptincj992`` — Alters- und Aufteilungscode sind vertauscht. Eine erste
Fassung dieses Werkzeugs filterte auf die Aufsatz-Form und fand NULL
Zeilen in einer 21-MB-Datei. Null Treffer ist kein Ergebnis, sondern ein
Verdacht.

DIE GEBIETSCODES DER AGGREGATE
``QE`` ist Europa, ``WO`` die Welt — belegt aus ``WID_countries.csv``
(Zeilen ``QE-MER``/``QE-PPP`` und ``WO-MER``/``WO-PPP``). Laut README
steht der nackte Code fuer die Kaufkraftparitaeten-Fassung, die der
Standard ist. Nicht geraten: Die Plausibilitaet der Werte (Europa Top-10 %
rund 36 %, Welt rund 53 %) stuetzt die Zuordnung, belegt sie aber nicht.

SPEICHER
Die Dateien werden ZEILENWEISE gelesen und sofort gefiltert, nie als
Ganzes gehalten — zusammen sind es ueber 800 MB (vgl. #263, wo ein
``f.read()`` auf 108 MB den Wartungs-Container riss).

Verwendung:
  python3 tools/wid_aus_bulk.py --cache <ordner> [--jahr 2023] [--json out.json]

Exit-Codes: 0 = keine Abweichung zu data/wid.json, 1 = Abweichungen oder
unvollstaendige Abdeckung, 2 = Aufruf-/Netzfehler.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

#: Welche Exit-Codes einen BEFUND bedeuten (Gate aus #259).
BEFUND_EXITS: tuple[int, ...] = (1,)

BASIS = "https://wid.world/bulk_download"

#: Unser ISO3-Schluessel -> WID-Gebietscode.
GEBIETE = {
    "AUS": "AU", "AUT": "AT", "BEL": "BE", "BRA": "BR", "CAN": "CA",
    "CHE": "CH", "CHN": "CN", "CZE": "CZ", "DEU": "DE", "DNK": "DK",
    "ESP": "ES", "EUR": "QE", "FIN": "FI", "FRA": "FR", "GBR": "GB",
    "GRC": "GR", "HUN": "HU", "IND": "IN", "IRL": "IE", "ITA": "IT",
    "JPN": "JP", "KOR": "KR", "MEX": "MX", "NLD": "NL", "NOR": "NO",
    "POL": "PL", "PRT": "PT", "ROU": "RO", "RUS": "RU", "SWE": "SE",
    "TUR": "TR", "USA": "US", "WLD": "WO", "ZAF": "ZA",
}

#: (Variable, Perzentil) -> Feldname in ``data/wid.json``.
REIHEN = {
    ("sptincj992", "p99p100"): "income_top1",
    ("sptincj992", "p90p100"): "income_top10",
    ("sptincj992", "p0p50"): "income_bottom50",
    ("shwealj992", "p99p100"): "wealth_top1",
    ("shwealj992", "p90p100"): "wealth_top10",
    ("shwealj992", "p0p50"): "wealth_bottom50",
}

VARIABLEN = {v for v, _p in REIHEN}
PERZENTILE = {p for _v, p in REIHEN}


def hole_gefiltert(code: str, ziel: str) -> int:
    """Laedt ``WID_data_<code>.csv`` und schreibt nur die Zeilen nach
    ``ziel``, die eine der sechs Reihen tragen.

    Zeilenweise — die Datei wird nie ganz gehalten.
    """
    url = f"{BASIS}/WID_data_{code}.csv"
    behalten = 0
    with urllib.request.urlopen(url, timeout=300) as antwort, \
            open(ziel, "w", encoding="utf-8") as aus:
        for rohzeile in antwort:
            zeile = rohzeile.decode("utf-8", "replace")
            teile = zeile.split(";")
            if len(teile) < 5:
                continue
            if teile[1] in VARIABLEN and teile[2] in PERZENTILE:
                aus.write(zeile)
                behalten += 1
    return behalten


def lies_gefiltert(pfad: str) -> dict[int, dict[str, float]]:
    """``{jahr: {feld: anteil_in_prozent}}`` aus einer gefilterten Datei.

    WID fuehrt Anteile als Bruch (0.089). Gerechnet wird in Prozent mit
    einer Dezimalstelle, wie ``data/wid.json`` sie fuehrt.
    """
    aus: dict[int, dict[str, float]] = {}
    with open(pfad, encoding="utf-8") as fh:
        for zeile in fh:
            teile = zeile.rstrip("\n").split(";")
            if len(teile) < 5:
                continue
            feld = REIHEN.get((teile[1], teile[2]))
            if feld is None or not teile[4].strip():
                continue
            try:
                jahr = int(teile[3])
                wert = float(teile[4])
            except ValueError:
                continue
            aus.setdefault(jahr, {})[feld] = round(wert * 100, 1)
    return aus


def jahr_mit_voller_abdeckung(nach_jahr: dict[int, dict[str, float]]) -> int | None:
    """Das jüngste Jahr, in dem ALLE sechs Reihen stehen.

    Ein Jahr mit fuenf von sechs Reihen taugt nicht: Dann stuende ein
    Einkommensanteil von 2024 neben einem Vermoegensanteil von 2021, und
    die Datei behauptete ein gemeinsames Datenjahr, das es nicht gibt.
    """
    felder = set(REIHEN.values())
    for jahr in sorted(nach_jahr, reverse=True):
        if felder <= set(nach_jahr[jahr]):
            return jahr
    return None


def main(argv=None) -> int:
    hier = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cache", required=True,
                    help="Ordner fuer die gefilterten CSVs (wird angelegt)")
    ap.add_argument("--wid-json", default=os.path.join(hier, "data", "wid.json"))
    ap.add_argument("--jahr", type=int,
                    help="Datenjahr erzwingen statt es zu bestimmen")
    ap.add_argument("--json", help="gelesene Werte zusaetzlich hierhin schreiben")
    ap.add_argument("--neu-laden", action="store_true",
                    help="auch dann laden, wenn eine gefilterte Datei vorliegt")
    args = ap.parse_args(argv)

    os.makedirs(args.cache, exist_ok=True)
    with open(args.wid_json, encoding="utf-8") as fh:
        fakt = json.load(fh)
    shares = fakt["shares"]

    fehlend = sorted(set(shares) - set(GEBIETE))
    if fehlend:
        print(f"Kein Gebietscode fuer: {', '.join(fehlend)}", file=sys.stderr)
        return 2

    gelesen: dict[str, dict[int, dict[str, float]]] = {}
    for iso in sorted(shares):
        code = GEBIETE[iso]
        pfad = os.path.join(args.cache, f"{code}.csv")
        if args.neu_laden or not os.path.exists(pfad):
            try:
                n = hole_gefiltert(code, pfad)
            except (urllib.error.URLError, OSError) as e:
                print(f"  {iso} ({code}): Abruf fehlgeschlagen — {e}",
                      file=sys.stderr)
                return 2
            print(f"  {iso} ({code}): {n} Zeilen behalten", flush=True)
        gelesen[iso] = lies_gefiltert(pfad)

    je_iso_jahr = {iso: jahr_mit_voller_abdeckung(j)
                   for iso, j in gelesen.items()}
    ohne = sorted(i for i, j in je_iso_jahr.items() if j is None)
    vorhanden = [j for j in je_iso_jahr.values() if j is not None]
    gemeinsam = min(vorhanden) if vorhanden else None

    print(f"\n{len(gelesen)} Gebiete gelesen")
    if ohne:
        print(f"  OHNE vollstaendiges Jahr: {', '.join(ohne)}")
    print(f"  juengstes Jahr je Gebiet: "
          f"{min(vorhanden)}..{max(vorhanden)}" if vorhanden else "  (keines)")
    knapp = sorted((j, i) for i, j in je_iso_jahr.items() if j is not None)[:6]
    print(f"  die aeltesten: {', '.join(f'{i} {j}' for j, i in knapp)}")

    jahr = args.jahr or gemeinsam
    if jahr is None:
        return 1
    print(f"\n  gewaehltes Datenjahr: {jahr}"
          + ("  (erzwungen)" if args.jahr else "  (juengstes gemeinsames)"))

    amtlich: dict[str, dict[str, float]] = {}
    luecken: list[str] = []
    for iso, nach_jahr in sorted(gelesen.items()):
        werte = nach_jahr.get(jahr) or {}
        if set(REIHEN.values()) <= set(werte):
            amtlich[iso] = {f: werte[f] for f in sorted(set(REIHEN.values()))}
        else:
            luecken.append(f"{iso} (fehlt: "
                           f"{','.join(sorted(set(REIHEN.values()) - set(werte)))})")
    if luecken:
        print(f"  UNVOLLSTAENDIG in {jahr}: {'; '.join(luecken)}")

    ab_gesamt = gepr_gesamt = 0
    print()
    for iso in sorted(amtlich):
        ab = [(f, amtlich[iso][f], shares[iso][f])
              for f in sorted(amtlich[iso])
              if f in shares[iso]
              and abs(float(shares[iso][f]) - amtlich[iso][f]) > 0.05]
        gepr_gesamt += len([f for f in amtlich[iso] if f in shares[iso]])
        ab_gesamt += len(ab)
        if ab:
            print(f"  {iso}: {len(ab)} abweichend")
            for f, amt, drin in ab:
                print(f"      {f:<17} amtlich {amt:>6}  im Fakt {drin:>6}  "
                      f"(Delta {float(drin) - amt:+.1f})")
    quote = ab_gesamt * 100 // gepr_gesamt if gepr_gesamt else 0
    print(f"\n  GESAMT: {ab_gesamt} Abweichungen von {gepr_gesamt} Werten "
          f"({quote} %)")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump({"jahr": jahr, "shares": amtlich}, fh,
                      ensure_ascii=False, indent=1, sort_keys=True)
        print(f"  geschrieben: {args.json}")
    return 1 if (ab_gesamt or luecken or ohne) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
