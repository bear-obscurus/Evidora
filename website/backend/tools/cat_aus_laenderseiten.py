#!/usr/bin/env python3
"""Liest die CAT-Ratings aus den Laenderseiten von climateactiontracker.org.

Anlass (22.10.2026). ``data/climate_action_tracker.json`` trug den Vermerk

    audit_flag: STATIC-SNAPSHOT — Werte sind Recherche-Approximationen aus
    oeffentlichen CAT-Country-Pages (Stand November 2024). Vor produktivem
    Live-Einsatz: pro Country-Page gegenpruefen.

Niemand hat gegengeprueft, und die Werte wurden seit Mai 2026 live
serviert. Es ist dieselbe Marker-Klasse wie bei MIPEX (#266, 52 % der
Werte falsch) und BTI (#268, 80 %) — nur in anderer Wortwahl, weshalb das
Gate aus #268 sie nicht gefunden hat: Es suchte woertlich nach
"AUDIT NEEDED".

Dazu ist der Stand zwei Jahre alt: ``assessment_date`` steht bei allen 41
Laendern auf ``2024-11``, waehrend CAT im Oktober 2026 aktualisiert hat.

WIE DIE SEITE GELESEN WIRD
Die Ratings stehen als Text IM Tile, nicht nur in der Farbe:

    <div class="ratings-matrix__overall ratings-matrix__tile--ORANGE">
      <dt>Overall rating</dt> ... Insufficient ...

Gelesen wird deshalb das LABEL, nicht die Farbe. Eine Farb-Zuordnung
haette eine Methodik-Tabelle gebraucht und waere beim naechsten
Design-Wechsel gerissen; der Text traegt die Aussage selbst.

Verwendung:
  python3 tools/cat_aus_laenderseiten.py --cache <ordner> [--json out.json]

Exit-Codes: 0 = keine Abweichung, 1 = Abweichungen oder unlesbare Seiten,
2 = Aufruf-/Lesefehler.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request

#: Welche Exit-Codes einen BEFUND bedeuten (Gate aus #259).
BEFUND_EXITS: tuple[int, ...] = (1,)

#: Die CAT-Kategorien, laengste zuerst — sonst trifft "Insufficient" auch
#: in "Highly insufficient" und "Critically insufficient" (dieselbe
#: Teilstring-Falle wie "wie viel" in "wie viele", #231).
#:
#: Aus allen 41 Laenderseiten gezaehlt, nicht angenommen. Dabei kam eine
#: SECHSTE Kategorie zum Vorschein, die in der Methodik-Tabelle unserer
#: JSON fehlt: Norwegens Ziel-Rating lautet "1.5°C global least cost".
KATEGORIEN = (
    "Critically insufficient",
    "Highly insufficient",
    "Almost sufficient",
    "1.5°C global least cost",
    "1.5C global least cost",
    "1.5°C compatible",
    "1.5C compatible",
    "Insufficient",
)

#: Die Zuordnung laeuft NICHT ueber den Label-Anfang, sondern ueber den
#: unterscheidenden Teil — und das ist eine Lehre aus zwei Fehlversuchen.
#:
#: Erste Fassung: Labels von der DEUTSCHEN Seite abgeleitet ("National
#: target against ..."). Ergebnis: Bei 38 von 40 Laendern wurden nur zwei
#: der vier Ratings gelesen, denn die meisten Seiten schreiben "NDC
#: target".
#:
#: Zweite Fassung: beide Varianten aufgezaehlt. Ergebnis: 17 Laender
#: blieben lueckenhaft, weil die Schweiz und andere "NDC target
#: (domestic)" und "NDC target (full)" schreiben.
#:
#: Varianten aufzaehlen scheitert hier wie in #140/#141. Stabil ist, WOGEGEN
#: gemessen wird: "against modelled domestic pathways" und "against fair
#: share". Die Reihenfolge der Pruefung ist dabei entscheidend — "Policies
#: and action" wird ebenfalls gegen die domestic pathways gemessen und muss
#: deshalb VOR dem Ziel-Rating geprueft werden.
#: Gemessen ueber alle 41 Seiten (Haeufigkeit in Klammern):
#:
#:   Overall rating                                            (80)
#:   Policies and action against modelled domestic pathways    (46)
#:   NDC target against modelled domestic pathways             (46)
#:   NDC target against fair share                             (46)
#:   Policies and action against fair share                    (32)
#:   Conditional NDC target against modelled domestic pathways (26)
#:   Unconditional NDC target against fair share               (24)
#:   NDC target (domestic)/(full) against ...                   (4)
#:   National target against ...                                (2)
#:   Policies & actions / Internationally supported target / Fair
#:   share target                                     (Iran-Schema)
#:
#: WIR FUEHREN VIER FELDER, CAT PUBLIZIERT BIS ZU SECHS RATINGS. "Policies
#: and action against fair share" und die Conditional/Unconditional-
#: Aufspaltung haben bei uns keinen Platz. Das ist eine Vereinfachung, die
#: in den Daten steht (schema_hinweis) — nicht ein Lesefehler.
ANFANGS_FELDER = (
    ("overall rating", "overall_rating"),
    # "Policies and action" kommt in ZWEI Fassungen vor (gegen domestic
    # pathways und gegen fair share). Die ERSTE im Dokument gewinnt, und das
    # ist die gegen die domestic pathways — die Bedeutung unseres Feldes.
    ("policies and action against modelled domestic pathways",
     "current_policies_rating"),
    ("policies & actions", "current_policies_rating"),
    ("policies and action", "current_policies_rating"),
)
GEGEN_FELDER = (
    ("against fair share", "fair_share_rating"),
    ("fair share target", "fair_share_rating"),
    ("against modelled domestic pathways", "targets_rating"),
    ("internationally supported target", "targets_rating"),
)
#: Tiles, die KEIN CAT-Rating der fuenfstufigen Skala tragen. Sie haben
#: eigene Skalen ("Poor"/"Average"/"Acceptable", "Source"/"Sink") und
#: wuerden sonst ein Feld belegen, das sie nicht meinen.
FREMDE_TILES = ("climate finance", "net zero target", "land use")

_TILE_RE = re.compile(
    r'ratings-matrix__tile--[A-Z_]+"[^>]*>(.{0,400}?)</div>', re.S)
_TAG_RE = re.compile(r"<[^>]+>")


def _text(roh: str) -> str:
    return " ".join(_TAG_RE.sub(" ", roh).replace("&amp;", "&").split())


def _kategorie(text: str) -> str | None:
    """Die CAT-Kategorie in diesem Tile-Text, oder None.

    Die laengste passende gewinnt: "Insufficient" steckt in "Highly
    insufficient" und "Critically insufficient" — dieselbe
    Teilstring-Falle wie "wie viel" in "wie viele" (#231).
    """
    tief = text.lower()
    for k in KATEGORIEN:
        if k.lower() in tief:
            return k
    return None


def lies_seite(roh_html: str) -> dict[str, str]:
    """``{feldname: kategorie}`` aus einer Laenderseite.

    Geraten wird nichts: Fehlt ein Tile oder laesst sich seine Kategorie
    nicht lesen, fehlt das Feld im Ergebnis.
    """
    aus: dict[str, str] = {}
    for m in _TILE_RE.finditer(roh_html):
        text = _text(m.group(1))
        tief = text.lower()
        if any(tief.startswith(f) for f in FREMDE_TILES):
            continue
        feld = None
        rest = text
        for anfang, kandidat in ANFANGS_FELDER:
            if tief.startswith(anfang):
                feld, rest = kandidat, text[len(anfang):]
                break
        if feld is None:
            for marker, kandidat in GEGEN_FELDER:
                i = tief.find(marker)
                if i >= 0:
                    feld, rest = kandidat, text[i + len(marker):]
                    break
        if feld is None or feld in aus:
            continue
        kat = _kategorie(rest)
        if kat:
            aus[feld] = kat
    return aus


def hole(url: str, ziel: str) -> str:
    """Laedt die Seite, wenn sie nicht im Cache liegt, und gibt sie zurueck."""
    if os.path.exists(ziel):
        with open(ziel, encoding="utf-8", errors="replace") as fh:
            return fh.read()
    anfrage = urllib.request.Request(
        url, headers={"User-Agent": "Evidora-Datenpruefung/1.0"})
    with urllib.request.urlopen(anfrage, timeout=60) as antwort:
        roh = antwort.read().decode("utf-8", "replace")
    with open(ziel, "w", encoding="utf-8") as fh:
        fh.write(roh)
    return roh


def vergleiche(amtlich: dict[str, str],
               fakt: dict) -> list[tuple[str, str, str]]:
    """``(Feld, auf der Seite, im Fakt)`` fuer jede Abweichung.

    Gross-/Kleinschreibung zaehlt nicht: CAT schreibt im Tile "Almost
    Sufficient", in der Methodik "Almost sufficient".
    """
    ab = []
    for feld, wert in sorted(amtlich.items()):
        drin = fakt.get(feld)
        if drin is None:
            continue
        if str(drin).strip().lower() != wert.strip().lower():
            ab.append((feld, wert, str(drin)))
    return ab


def main(argv=None) -> int:
    hier = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cache", required=True,
                    help="Ordner fuer die geladenen Seiten (wird angelegt)")
    ap.add_argument("--cat-json", default=os.path.join(
        hier, "data", "climate_action_tracker.json"))
    ap.add_argument("--json", help="gelesene Werte zusaetzlich hierhin schreiben")
    ap.add_argument("--leise", action="store_true")
    args = ap.parse_args(argv)

    os.makedirs(args.cache, exist_ok=True)
    with open(args.cat_json, encoding="utf-8") as fh:
        daten = json.load(fh)
    scores = daten["scores"]

    gelesen: dict[str, dict[str, str]] = {}
    unlesbar: list[str] = []
    for iso in sorted(scores):
        url = (scores[iso].get("url") or "").strip()
        if not url:
            unlesbar.append(f"{iso} (keine URL)")
            continue
        ziel = os.path.join(args.cache, f"{iso}.html")
        try:
            roh = hole(url, ziel)
        except (urllib.error.URLError, OSError) as e:
            unlesbar.append(f"{iso} ({e})")
            continue
        werte = lies_seite(roh)
        # Eine Seite ohne Overall-Rating ist keine Laenderseite, sondern eine
        # Fehler- oder Weiterleitungsseite. Sie als "keine Abweichung" zu
        # zaehlen waere der stille Fehlschluss (gleiche Pruefung wie in
        # tools/mipex_aus_archiv.py).
        if "overall_rating" not in werte:
            unlesbar.append(f"{iso} (kein Overall-Rating)")
            continue
        gelesen[iso] = werte
        if not args.leise:
            print(f"  {iso}: {len(werte)} Ratings gelesen", flush=True)

    print(f"\n{len(gelesen)} Laenderseiten gelesen, {len(unlesbar)} unlesbar"
          + (f" ({', '.join(unlesbar)})" if unlesbar else ""))

    ab_gesamt = gepr_gesamt = 0
    je_feld: dict[str, int] = {}
    print()
    for iso in sorted(gelesen):
        ab = vergleiche(gelesen[iso], scores[iso])
        gepr_gesamt += len([f for f in gelesen[iso] if f in scores[iso]])
        ab_gesamt += len(ab)
        for feld, _a, _b in ab:
            je_feld[feld] = je_feld.get(feld, 0) + 1
        if ab and not args.leise:
            print(f"  {iso}: {len(ab)} abweichend")
            for feld, seite, drin in ab:
                print(f"      {feld:<26} Seite {seite:<26} im Fakt {drin}")

    sauber = [i for i in sorted(gelesen) if not vergleiche(gelesen[i], scores[i])]
    print(f"\n  ohne Abweichung: {', '.join(sauber) or '(keines)'}")
    print("  je Feld: " + (", ".join(f"{f} {n}" for f, n in sorted(je_feld.items()))
                           or "(keine)"))
    quote = ab_gesamt * 100 // gepr_gesamt if gepr_gesamt else 0
    print(f"  GESAMT: {ab_gesamt} Abweichungen von {gepr_gesamt} Werten ({quote} %)")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(gelesen, fh, ensure_ascii=False, indent=1, sort_keys=True)
        print(f"  geschrieben: {args.json}")
    return 1 if (ab_gesamt or unlesbar) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
