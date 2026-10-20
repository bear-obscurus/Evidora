#!/usr/bin/env python3
"""Liest die offiziellen BTI-Scores aus dem XLSX-Export der Bertelsmann Stiftung.

Anlass (20.10.2026). Die Datei hiess damals ``data/bti_2024.json`` und
trug seit ihrer Anlage den Vermerk

    audit_flag: AUDIT NEEDED — BTI-2024-Werte sind LLM-Approximationen
    aus dem oeffentlich kommunizierten Ranking (137 Laender, drei
    Dimensionen 1-10). Vor produktivem Einsatz mit offiziellem
    Excel-Export aus https://bti-project.org/en/downloads gegenchecken.

Niemand hat gegengecheckt. Es ist derselbe Markertext wie bei MIPEX, und
dort waren nach dem Nachrechnen 52 % der Bereichswerte falsch (PR #266).

Dieses Werkzeug macht die im Vermerk verlangte Gegenprobe wiederholbar.
Es liest das XLSX mit der Standardbibliothek — kein openpyxl, kein pandas —
damit es ohne neue Abhaengigkeit ueberall laeuft.

SPALTEN IM EXPORT
Beide Ausgaben (2024 und 2026) haben denselben Aufbau. Die Kopfzeile ist
Zeile 1, die Daten beginnen in Zeile 2:

    A    Land (englischer Name)
    C    Ranking Status Index
    D    S   | Status Index          (Mittel aus Democracy und Economy)
    E    SI  | Democracy Status
    AC   SII | Economy Status
    AZ   G   | Governance Index
    DG   Kategorie-Label Democracy   ("Hard-line autocracy", ...)
    DJ   Kategorie-Label Economy     ("Rudimentary", ...)
    DM   Kategorie-Label Governance  ("Weak", ...)

Die Spalten DB/DE/DH/DK fuehren dieselben Zahlen auf zwei Stellen gerundet;
verglichen wird gegen die gerundete Fassung, weil ``data/bti.json``
zwei Stellen fuehrt.

Verwendung:
  python3 tools/bti_aus_excel.py --xlsx <pfad> [--json out.json] [--leise]

Exit-Codes: 0 = keine Abweichung, 1 = Abweichungen gefunden,
2 = Aufruf-/Lesefehler.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
import xml.etree.ElementTree as ET
import zipfile

#: Welche Exit-Codes einen BEFUND bedeuten (Gate aus #259).
BEFUND_EXITS: tuple[int, ...] = (1,)

_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

#: Spaltenbuchstabe -> Feldname in ``data/bti.json``.
#: Bewusst die gerundeten Kategorie-Spalten, nicht die vollen Brueche.
SPALTEN_ZAHL = {
    "C": "rank",
    "DB": "status_index",
    "DE": "political",
    "DH": "economic",
    "DK": "governance",
    # Die Kategorie-NUMMER je Dimension (1 = beste Stufe). Ueber sie
    # laeuft der Label-Vergleich, nicht ueber den englischen Text:
    # BTI schreibt im Plural ("Hard-line autocracies"), und eine erste
    # Fassung dieses Werkzeugs uebersetzte geratene Singular-Formen —
    # sie meldete daraufhin 53 von 53 Labels als falsch, obwohl 43
    # stimmten. Die Nummer ist eindeutig, der Text nicht.
    "DF": "_kat_political",
    "DI": "_kat_economic",
    "DL": "_kat_governance",
}
SPALTEN_LABEL = {
    "DG": "_bti_political",
    "DJ": "_bti_economic",
    "DM": "_bti_governance",
}

#: BTI's eigene Ordinalskala, aus dem Export abgelesen (Kategorie-Nummer
#: -> englisches Label), und die deutsche Fassung dazu.
#:
#: BTI nimmt die Einstufung selbst vor — wir zitieren sie und uebersetzen
#: den Namen; wir klassifizieren nicht selbst (Politik-Tabu). Deshalb
#: steht das englische Original neben der Uebersetzung.
#:
#: WICHTIG: Die Stufen 3 und 4 der Demokratie-Skala UEBERLAPPEN im Score
#: (hochdefekte Demokratien 4,75-5,95 gegen moderate Autokratien
#: 4,03-5,48). Die Einstufung ist also KEINE reine Schwellenregel — BTI
#: zieht die Demokratie/Autokratie-Bewertung hinzu. Wer die Kategorie aus
#: dem Score herleitet, bekommt sie in diesem Band falsch.
SKALA = {
    "political": {
        1: ("Democracies in consolidation", "Demokratie in Konsolidierung"),
        2: ("Defective democracies", "Defekte Demokratie"),
        3: ("Highly defective democracies", "Hochdefekte Demokratie"),
        4: ("Moderate autocracies", "Moderate Autokratie"),
        5: ("Hard-line autocracies", "Harte Autokratie"),
    },
    "economic": {
        1: ("Highly advanced", "Weit entwickelte Marktwirtschaft"),
        2: ("Advanced", "Entwickelte Marktwirtschaft"),
        3: ("Limited", "Marktwirtschaft mit Funktionsdefiziten"),
        4: ("Very limited", "Marktwirtschaft mit schweren Funktionsdefiziten"),
        5: ("Rudimentary", "Rudimentäre Marktwirtschaft"),
    },
    "governance": {
        1: ("Very good", "Sehr gut"),
        2: ("Good", "Gut"),
        3: ("Moderate", "Moderat"),
        4: ("Weak", "Schwach"),
        5: ("Failed", "Versagend"),
    },
}


def _spalte_zu_index(ref: str) -> int:
    """'A' -> 0, 'AC' -> 28, 'DM' -> 116."""
    n = 0
    for z in ref:
        n = n * 26 + (ord(z) - 64)
    return n - 1


def _zelltext(c, shared: list[str]) -> str:
    """Der Wert einer Zelle als Text — leer, wenn sie leer ist."""
    typ = c.get("t")
    if typ == "s":
        v = c.find(_NS + "v")
        if v is None or not (v.text or "").strip():
            return ""
        return shared[int(v.text)]
    if typ == "inlineStr":
        is_ = c.find(_NS + "is")
        return "".join(t.text or "" for t in is_.iter(_NS + "t")) if is_ is not None else ""
    v = c.find(_NS + "v")
    return (v.text or "") if v is not None else ""


def lies_blatt(pfad: str) -> dict[str, dict]:
    """``{englischer Landesname: {feld: wert}}`` aus dem XLSX.

    Geraten wird nichts: Fehlt eine Zelle, fehlt das Feld im Ergebnis.
    """
    mit_ref = {**SPALTEN_ZAHL, **SPALTEN_LABEL}
    gesucht = {_spalte_zu_index(s): f for s, f in mit_ref.items()}
    zahlen = {_spalte_zu_index(s) for s in SPALTEN_ZAHL}

    with zipfile.ZipFile(pfad) as z:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in z.namelist():
            wurzel = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in wurzel.findall(_NS + "si"):
                shared.append("".join(t.text or "" for t in si.iter(_NS + "t")))
        blatt = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))

    aus: dict[str, dict] = {}
    for row in blatt.iter(_NS + "row"):
        if row.get("r") == "1":          # Kopfzeile
            continue
        zeile: dict[int, str] = {}
        for c in row.findall(_NS + "c"):
            ref = c.get("r") or ""
            m = re.match(r"([A-Z]+)", ref)
            if not m:
                continue
            j = _spalte_zu_index(m.group(1))
            if j == 0 or j in gesucht:
                zeile[j] = _zelltext(c, shared)
        land = (zeile.get(0) or "").strip()
        if not land:
            continue
        werte: dict[str, object] = {}
        for j, feld in gesucht.items():
            roh = (zeile.get(j) or "").strip()
            if not roh:
                continue
            if j in zahlen:
                try:
                    f = float(roh)
                except ValueError:
                    continue
                werte[feld] = int(f) if feld == "rank" else round(f, 2)
            else:
                werte[feld] = roh
        if werte:
            aus[land] = werte
    return aus


def _schluessel(s: str) -> str:
    """Vergleichsform eines Landesnamens.

    NFC ist hier nicht kosmetisch: Der Export schreibt ``Türkiye`` als
    ``u`` + kombinierendes Trema (NFD, U+0075 U+0308), unsere Alias-Liste
    als ``ü`` (NFC, U+00FC). Optisch identisch, byteweise verschieden —
    ohne Normalisierung fiel die Tuerkei lautlos aus der Zuordnung, und
    ein Claim, der ``Türkiye`` so schreibt, traf den Fakt nicht.
    Dieselbe Fehlerform wie bei den Phrasings in #143/#144.
    """
    return unicodedata.normalize("NFC", s).strip().lower()


def namen_zu_iso(bti_namen, display_names, aliases) -> dict[str, str]:
    """Englischer BTI-Name -> ISO3, ueber die Namen der JSON.

    Erst der Anzeigename, dann die Alias-Liste. Wer nicht zugeordnet
    werden kann, fehlt — eine Zuordnung zu erfinden waere schlimmer als
    eine Luecke.
    """
    nach_display = {_schluessel(v): k for k, v in display_names.items()}
    nach_alias: dict[str, str] = {}
    for iso, liste in aliases.items():
        for a in liste:
            nach_alias.setdefault(_schluessel(a), iso)
    zuordnung: dict[str, str] = {}
    for name in bti_namen:
        k = _schluessel(name)
        iso = nach_display.get(k) or nach_alias.get(k)
        if iso:
            zuordnung[name] = iso
    return zuordnung


#: Die Zahlenfelder, die beide Seiten fuehren.
ZAHLEN_FELDER = ("rank", "status_index", "political", "economic", "governance")


def label_soll(dimension: str, kategorie) -> tuple[str, str]:
    """``(englisch, deutsch)`` fuer eine Kategorie-Nummer."""
    return SKALA[dimension][int(float(kategorie))]


def vergleiche(amtlich: dict, fakt: dict) -> list[tuple[str, object, object]]:
    """``(Feld, amtlich, im Fakt)`` fuer jede Abweichung.

    Zahlen mit Toleranz 0.005 (beide Seiten fuehren zwei Stellen). Die
    Einstufungen werden ueber BTI's Kategorie-NUMMER aufgeloest, nicht
    ueber den englischen Text.
    """
    ab: list[tuple[str, object, object]] = []
    for feld in ZAHLEN_FELDER:
        if feld not in amtlich or feld not in fakt:
            continue
        wert, drin = amtlich[feld], fakt[feld]
        if feld == "rank":
            if int(drin) != int(wert):
                ab.append((feld, wert, drin))
        elif abs(float(drin) - float(wert)) > 0.005:
            ab.append((feld, wert, drin))

    for dim in ("political", "economic", "governance"):
        kat = amtlich.get(f"_kat_{dim}")
        feld = f"{dim}_status"
        if kat is None or feld not in fakt:
            continue
        englisch, deutsch = label_soll(dim, kat)
        if str(fakt[feld]).strip() != deutsch:
            ab.append((feld, f"{englisch} / {deutsch}", fakt[feld]))
    return ab


def main(argv=None) -> int:
    hier = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--xlsx", required=True, help="BTI_<jahr>_Scores.xlsx")
    ap.add_argument("--bti-json", default=os.path.join(hier, "data", "bti.json"))
    ap.add_argument("--json", help="gelesene Werte zusaetzlich hierhin schreiben")
    ap.add_argument("--leise", action="store_true",
                    help="nur die Summe, nicht jede Abweichung")
    args = ap.parse_args(argv)

    if not os.path.isfile(args.xlsx):
        print(f"XLSX fehlt: {args.xlsx}", file=sys.stderr)
        return 2
    with open(args.bti_json, encoding="utf-8") as fh:
        daten = json.load(fh)
    fakt = daten["scores"]

    blatt = lies_blatt(args.xlsx)
    zuordnung = namen_zu_iso(blatt, daten["country_display_names"],
                             daten["country_aliases"])
    amtlich = {zuordnung[n]: w for n, w in blatt.items() if n in zuordnung}

    fehlt = sorted(set(fakt) - set(amtlich))
    print(f"{len(blatt)} Laender im Export, {len(amtlich)} davon im Fakt "
          f"zugeordnet, {len(fehlt)} nicht gefunden"
          + (f" ({', '.join(fehlt)})" if fehlt else ""))
    print()

    ab_gesamt = gepr_gesamt = 0
    je_feld: dict[str, int] = {}
    schlimmste: list[tuple[float, str, str, object, object]] = []
    for iso in sorted(amtlich):
        ab = vergleiche(amtlich[iso], fakt[iso])
        gepr = len([f for f in ZAHLEN_FELDER
                    if f in amtlich[iso] and f in fakt[iso]])
        gepr += len([d for d in ("political", "economic", "governance")
                     if f"_kat_{d}" in amtlich[iso] and f"{d}_status" in fakt[iso]])
        ab_gesamt += len(ab)
        gepr_gesamt += gepr
        for feld, soll, ist in ab:
            je_feld[feld] = je_feld.get(feld, 0) + 1
            if feld in ZAHLEN_FELDER:
                schlimmste.append((abs(float(ist) - float(soll)), iso, feld, soll, ist))
        if ab and not args.leise:
            print(f"  {iso}: {len(ab)}/{gepr} abweichend")
            for feld, soll, ist in ab:
                print(f"      {feld:<17} amtlich {str(soll):>34}  "
                      f"im Fakt {str(ist):>34}")

    sauber = [i for i in sorted(amtlich) if not vergleiche(amtlich[i], fakt[i])]
    print(f"\n  ohne Abweichung: {', '.join(sauber) or '(keines)'}")
    print("  je Feld: " + ", ".join(f"{f} {n}" for f, n in sorted(je_feld.items())))
    quote = ab_gesamt * 100 // gepr_gesamt if gepr_gesamt else 0
    print(f"  GESAMT: {ab_gesamt} Abweichungen von {gepr_gesamt} Werten ({quote} %)")

    if schlimmste:
        print("\n  groesste Zahlen-Abweichungen:")
        for delta, iso, feld, soll, ist in sorted(schlimmste, reverse=True)[:12]:
            print(f"      {iso} {feld:<14} amtlich {soll:>7}  im Fakt {ist:>7}  "
                  f"(Delta {float(ist) - float(soll):+.2f})")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(amtlich, fh, ensure_ascii=False, indent=1, sort_keys=True)
        print(f"\n  geschrieben: {args.json}")
    return 1 if ab_gesamt else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
