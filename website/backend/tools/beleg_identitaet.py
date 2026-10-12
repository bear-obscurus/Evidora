"""Zeigt ein zitierter Beleg auf die Arbeit, die das Label nennt?

Der Link-Waechter (`check_urls.py`) prueft ERREICHBARKEIT. Am 12.10.2026
kam die Luecke heraus: neun Belege lebten (HTTP 200, echte begutachtete
Arbeiten) und zeigten trotzdem auf etwas anderes als das, was im
`source_label` des Fakts steht. Fuenf davon mit derselben Signatur —
richtige Zeitschrift, richtiges Jahr, falscher Artikel. Ein Beleg beim
Muskelkater-Fakt war eine Anzeigenseite aus *California and Western
Medicine* von 1946.

Dieses Werkzeug prueft die zweite Haelfte: IDENTITAET. Jeder aufloesbare
Literatur-Link (DOI, PubMed, PMC) wird gegen Crossref bzw. die
NCBI-E-utilities aufgeloest; stimmt kein Nachname der gefundenen Arbeit
mit dem `source_label` ueberein, ist das ein Befund.

Was es NICHT kann, und das ist wichtig: es sagt nicht, ob die Arbeit die
AUSSAGE traegt. Es sagt nur, ob Link und Label dieselbe Arbeit meinen.
Fehlalarme sind eingebaut — ein Label, das Institutionen statt Autoren
nennt ("AHA + ESC + USPSTF"), hat keinen Nachnamen zum Vergleichen. Die
Ausgabe ist deshalb eine Liste zum Ansehen, keine Fehlerliste.

Nutzung (Netz noetig, daher kein CI-Test):
  docker compose --profile wartung run --rm ...   # noch nicht verdrahtet
  python3 tools/beleg_identitaet.py --data-dir data

Exit: 2 = Befunde, 0 = keine. (Wie quellen_lebenszeichen seit #236.)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

KOPF = {"User-Agent": "Evidora/1.0 (+https://evidora.eu; Beleg-Identitaetspruefung)"}
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
PAUSE_S = 1.1          # NCBI bittet um hoechstens 3 Anfragen/Sekunde
PUBMED_RE = re.compile(r"pubmed\.ncbi\.nlm\.nih\.gov/(\d+)")
PMC_RE = re.compile(r"articles/PMC(\d+)")
DOI_RE = re.compile(r"(10\.\d{4,9}/[^\s\"'<>?]+)")


def kennung(url: str):
    """(art, id) fuer aufloesbare Literatur-Links, sonst None.

    Die DOI wird aus JEDEM Pfad gezogen, nicht nur aus `/doi/`: Springer
    legt sie unter `/article/`, LWW unter `/abstract/`. Die erste Fassung
    kannte nur `/doi/` und uebersah genau den Springer-Link, der auf eine
    fremde Arbeit zeigte.
    """
    m = PUBMED_RE.search(url)
    if m:
        return "pubmed", m.group(1)
    m = PMC_RE.search(url)
    if m:
        return "pmc", m.group(1)
    m = DOI_RE.search(url)
    if m:
        return "doi", m.group(1).rstrip(").,;")
    return None


def _json(url: str):
    with urllib.request.urlopen(
            urllib.request.Request(url, headers=KOPF), timeout=40) as r:
        return json.load(r)


def aufloesen(art: str, kenn: str):
    """(Nachnamen, Jahr, Titel, Zeitschrift) oder None."""
    if art == "doi":
        it = _json("https://api.crossref.org/works/"
                   + urllib.parse.quote(kenn, safe="/"))["message"]
        return ([a.get("family", "") for a in (it.get("author") or [])],
                (it.get("issued", {}).get("date-parts") or [[None]])[0][0],
                (it.get("title") or ["?"])[0],
                (it.get("container-title") or ["?"])[0])
    db = "pmc" if art == "pmc" else "pubmed"
    d = _json(f"{EUTILS}/esummary.fcgi?db={db}&retmode=json&id={kenn}")["result"][kenn]
    jahr = re.search(r"\d{4}", d.get("pubdate", "") or "")
    return ([a["name"].split()[0] for a in d.get("authors", [])],
            int(jahr.group()) if jahr else None,
            d.get("title", "?"),
            d.get("source") or d.get("fulljournalname") or "?")


def fakten(datei: str):
    with open(datei, encoding="utf-8") as f:
        d = json.load(f)
    roh = d.get("facts") if isinstance(d, dict) else d
    return [x for x in (roh or []) if isinstance(x, dict)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--pause", type=float, default=PAUSE_S)
    a = ap.parse_args()

    ziele = []
    for name in sorted(os.listdir(a.data_dir)):
        if not name.endswith(".json") or name.startswith((".", "_")):
            continue
        try:
            fs = fakten(os.path.join(a.data_dir, name))
        except Exception:
            continue
        for f in fs:
            for feld in ("source_url", "secondary_url"):
                k = kennung(f.get(feld) or "")
                if k:
                    ziele.append((name, f.get("id") or f.get("topic"), feld,
                                  k, f.get("source_label", "") or ""))

    print(f"=== Beleg-Identitaet: {len(ziele)} aufloesbare Literatur-Links ===\n")
    befunde, offen = [], []
    for datei, fid, feld, (art, kenn), label in ziele:
        try:
            autoren, jahr, titel, zs = aufloesen(art, kenn)
        except Exception as e:
            offen.append(f"{datei}/{fid}/{feld}: {type(e).__name__}")
            time.sleep(a.pause)
            continue
        lab = label.lower()
        if not any(x and len(x) > 1 and x.lower() in lab for x in autoren[:6]):
            befunde.append((datei, fid, feld, label,
                            f"{', '.join(autoren[:2]) or '?'} ({jahr}) "
                            f"{zs[:30]} — {titel[:70]}"))
        time.sleep(a.pause)

    for datei, fid, feld, label, gefunden in befunde:
        print(f"  ANSEHEN  {datei} / {fid} / {feld}")
        print(f"           Label:    {label[:96]}")
        print(f"           Verlinkt: {gefunden}")
    if offen:
        print(f"\n  {len(offen)} nicht aufloesbar (kein Befund, nur ungeprueft):")
        for o in offen:
            print(f"    {o}")
    print(f"\n{len(ziele) - len(offen)} geprueft, {len(befunde)} zum Ansehen, "
          f"{len(offen)} offen.")
    print("Ein Treffer ist KEIN Fehler: Labels, die Institutionen statt "
          "Autoren nennen, haben nichts zum Vergleichen.")
    return 2 if befunde else 0


if __name__ == "__main__":
    sys.exit(main())
