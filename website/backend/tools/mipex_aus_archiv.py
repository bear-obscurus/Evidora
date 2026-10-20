#!/usr/bin/env python3
"""Liest die publizierten MIPEX-2020-Werte aus archivierten Laenderseiten.

Anlass (19./20.10.2026). ``data/mipex.json`` trug seit seiner Anlage den
Vermerk

    audit_flag: AUDIT NEEDED — MIPEX-2020-Werte sind LLM-Approximationen
    aus dem oeffentlich publizierten Ranking. Vor produktivem Einsatz mit
    offiziellem Excel-Export gegenchecken.

Niemand hat gegengecheckt, und die Werte wurden seit Mai 2026 live als
MIPEX-Punktzahlen serviert. Nachgemessen gegen die archivierten
Laenderseiten: **77 von 144 Werten wichen ab, 53 %**. Nur Kanada und
Deutschland waren fehlerfrei. Die groessten Abweichungen:

    Ungarn   Bildung               publiziert   0  im Fakt  39
    Ungarn   Anti-Diskriminierung  publiziert  96  im Fakt  50
    Griech.  Arbeitsmarkt          publiziert  61  im Fakt  25
    Irland   Arbeitsmarkt          publiziert  22  im Fakt  56
    Austral. Gesundheit            publiziert  79  im Fakt  47

EINE KORREKTUR AN MEINER EIGENEN ERSTEN MESSUNG
Nach drei Laendern (AT, DE, SE) hatte ich berichtet, die Fehler haeuften
sich bei Oesterreich. Das war falsch: Ich hatte zufaellig zwei der nur
zwei sauberen Laender in der Stichprobe. Erst der Lauf ueber alle
verfuegbaren Laender zeigte die 53 %.

WARUM DAS ARCHIV UND NICHT DIE QUELLE
Die heutigen mipex.eu-Laenderseiten zeigen fuer die Politikbereiche nur
noch Einstufungen ("halfway favourable"), keine Zahlen. Ein Scores-Export
ist auf /download nicht zu finden; dort liegen nur Methodik-Dateien.
Die archivierten Seiten der MIPEX-2020-Zeit liefern die Werte dagegen
maschinenlesbar als ``data-score``-Attribute — das ist die einzige
belegbare Quelle fuer die Bereichszahlen, die wir gefunden haben.

Verwendung:
  python3 tools/mipex_aus_archiv.py --verzeichnis <pfad-mit-html> [--json out.json]

Exit-Codes: 0 = alle gelesen, 1 = mindestens eine Seite unlesbar,
2 = Aufruf-/Lesefehler.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

#: Welche Exit-Codes einen BEFUND bedeuten. Dieses Werkzeug laeuft nicht
#: im Cron; die Zusage steht trotzdem hier, damit das Gate aus #259 sie
#: findet, falls es eines Tages dazukommt.
BEFUND_EXITS: tuple[int, ...] = (1,)

#: Der Bereichsname im HTML -> der Schluessel in ``data/mipex.json``.
BEREICHE = {
    "labour-market-mobility": "labour_market_mobility",
    "family-reunion": "family_reunion",
    "education": "education",
    "health": "health",
    "political-participation": "political_participation",
    "permanent-residence": "permanent_residence",
    "access-to-nationality": "access_to_nationality",
    "anti-discrimination": "anti_discrimination",
}

_SCORE_RE = re.compile(
    r'country-strand-([a-z-]+)[^"]*"\s+data-score="(\d+)"')
_OVERALL_RE = re.compile(r"MIPEX Score \(with Health\):\s*(\d+)")
_TAGS_RE = re.compile(r"<[^>]+>")


def lies_seite(pfad: str) -> dict[str, int]:
    """Die publizierten Werte einer archivierten Laenderseite.

    ``overall`` kommt aus dem Text ("MIPEX Score (with Health): 46"), die
    Bereiche aus den ``data-score``-Attributen. Fehlt eines, fehlt es im
    Ergebnis — geraten wird nichts.
    """
    with open(pfad, encoding="utf-8", errors="replace") as fh:
        rohtext = fh.read()
    aus: dict[str, int] = {}
    m = _OVERALL_RE.search(_TAGS_RE.sub(" ", rohtext))
    if m:
        aus["overall"] = int(m.group(1))
    for name, wert in _SCORE_RE.findall(rohtext):
        if name in BEREICHE:
            aus[BEREICHE[name]] = int(wert)
    return aus


def vergleiche(archiv: dict[str, int],
               fakt: dict[str, int]) -> list[tuple[str, int, int]]:
    """``(Feld, publiziert, im Fakt)`` fuer jede Abweichung."""
    return [(k, v, fakt[k]) for k, v in sorted(archiv.items())
            if k in fakt and fakt[k] != v]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--verzeichnis", required=True,
                    help="Ordner mit <ISO3>.html-Dateien")
    ap.add_argument("--mipex-json", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "mipex.json"))
    ap.add_argument("--json", help="gelesene Werte zusaetzlich hierhin schreiben")
    args = ap.parse_args(argv)

    if not os.path.isdir(args.verzeichnis):
        print(f"Verzeichnis fehlt: {args.verzeichnis}", file=sys.stderr)
        return 2
    with open(args.mipex_json, encoding="utf-8") as fh:
        fakt_scores = json.load(fh)["scores"]

    gelesen: dict[str, dict[str, int]] = {}
    unlesbar: list[str] = []
    for name in sorted(os.listdir(args.verzeichnis)):
        if not name.endswith(".html"):
            continue
        iso = name[:-5].upper()
        if iso not in fakt_scores:
            continue
        werte = lies_seite(os.path.join(args.verzeichnis, name))
        # Eine Seite ohne Bereichswerte ist keine Laenderseite, sondern
        # eine Fehler- oder Weiterleitungsseite. Sie als "keine
        # Abweichung" zu zaehlen waere der stille Fehlschluss.
        if len(werte) < 5:
            unlesbar.append(iso)
            continue
        gelesen[iso] = werte

    ab_gesamt = gepr_gesamt = 0
    print(f"{len(gelesen)} Laenderseiten gelesen, {len(unlesbar)} unlesbar"
          + (f" ({', '.join(unlesbar)})" if unlesbar else ""))
    print()
    for iso in sorted(gelesen):
        ab = vergleiche(gelesen[iso], fakt_scores[iso])
        gepr = len([k for k in gelesen[iso] if k in fakt_scores[iso]])
        ab_gesamt += len(ab)
        gepr_gesamt += gepr
        if ab:
            print(f"  {iso}: {len(ab)}/{gepr} abweichend")
            for k, publiziert, im_fakt in ab:
                print(f"      {k:<26} publiziert {publiziert:>3}  "
                      f"im Fakt {im_fakt:>3}  (Delta {im_fakt - publiziert:+d})")
    sauber = [i for i in sorted(gelesen) if not vergleiche(gelesen[i], fakt_scores[i])]
    print(f"\n  ohne Abweichung: {', '.join(sauber) or '(keines)'}")
    quote = ab_gesamt * 100 // gepr_gesamt if gepr_gesamt else 0
    print(f"  GESAMT: {ab_gesamt} Abweichungen von {gepr_gesamt} Werten ({quote} %)")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(gelesen, fh, ensure_ascii=False, indent=1, sort_keys=True)
        print(f"\n  geschrieben: {args.json}")
    return 1 if unlesbar else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
