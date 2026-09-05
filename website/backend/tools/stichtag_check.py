#!/usr/bin/env python3
"""Stichtags-Wächter: findet Fakten, deren eingebauter Stichtag abgelaufen ist.

Warum es das gibt (Lehrgeld 2026-09-05): Der Kopftuch-Fakt trug den Satz
„ist aber erst ab 1.9.2026 sanktionswirksam — derzeit TEILS-TEILS
(Empfehlung: Verdict mixed, Konfidenz 0.6)". Am 5.9.2026 lieferte die
Pipeline damit `mostly_false@0.9` auf einen ZUTREFFENDEN Claim, mit der
Begründung „bis 31.8.2026 gilt es daher noch nicht" — vier Tage nach dem
Stichtag. Das war keine veraltete Zahl, sondern eine aktive Fehlanweisung.

Warum der bestehende Freshness-Cron das nicht fängt: `data_freshness_check`
prüft das **Datei-Alter** (`fetched_at_iso`). Eine frisch committete Datei
gilt dort als gesund — auch wenn ein Datum IM Text längst abgelaufen ist.
Genau diese Lücke schließt dieser Job. Und der ⏰-Marker im Memory reichte
nicht: er wird nur gelesen, wenn jemand danach sucht.

Drei Schweregrade:

  KRITISCH  Ein in die Zukunft gerichteter SCHALTER ist umgelegt. Der Fakt
            beschreibt den Zustand VOR dem Stichtag als den aktuellen
            („erst ab X", „bis X gilt noch nicht", „für den Zeitraum bis X").
            Solche Sätze werden am Stichtag falsch, ohne dass etwas
            ausschlägt — sie instruieren die Pipeline aktiv falsch.

  WARNUNG   Ein Zeitfenster ist zugegangen („kostenfrei bis 30.6.2026").
            Die Aussage ist nicht zwingend falsch, aber sie beschreibt
            etwas Vergangenes im Präsens-Kontext.

  INFO      Vintage-Marker („Stand Februar 2020") älter als --max-vintage-
            months. Kein Fehler, aber ein Refresh-Kandidat.

Aufruf:
  python3 tools/stichtag_check.py [--strict] [--max-vintage-months N]
                                  [--alert-webhook URL] [--json out.json]

Cron (auf prod), über run_evidora_tool.sh im Backend-Container:
  0 6 * * 1 /opt/Evidora/website/run_evidora_tool.sh stichtag_check.py \
            --strict >> /home/burrito/evidora-logs/stichtag_check.log 2>&1

Exit-Codes: 0 = sauber, 1 = Funde bei --strict, 2 = Aufruf-/Lesefehler.
"""

import argparse
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Container-Schlüssel, unter denen Fakten in den data/*.json liegen.
LIST_KEYS = ("facts", "rulings", "items", "entries", "topics")

_D = r"(\d{1,2})\.\s?(\d{1,2})\.\s?(20\d{2})"

# KRITISCH — der Satz stellt den Zustand VOR dem Stichtag als aktuell dar.
KRITISCH_PATTERNS = [
    re.compile(r"erst\s+ab\s+" + _D, re.I),
    re.compile(r"bis\s+" + _D + r"[^.]{0,40}\bgilt\s+(?:es\s+)?(?:daher\s+)?noch\s+nicht", re.I),
    re.compile(r"für\s+den\s+Zeitraum\s+bis\s+" + _D, re.I),
    re.compile(r"noch\s+nicht\s+(?:rechts|sanktions)wirksam[^.]{0,60}?" + _D, re.I),
    re.compile(_D + r"[^.]{0,30}\bnoch\s+nicht\s+in\s+Kraft", re.I),
]

# WARNUNG — ein Zeitfenster ist zugegangen.
WARNUNG_PATTERNS = [
    re.compile(r"(?:gratis|kostenfrei|kostenlos|befristet|gültig|läuft)[^.]{0,60}?bis\s+" + _D, re.I),
    re.compile(r"bis\s+" + _D + r"\s+(?:gratis|kostenfrei|kostenlos|befristet)", re.I),
]

# INFO — Vintage-Marker.
_MONATE = {"jänner": 1, "januar": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4,
           "mai": 5, "juni": 6, "juli": 7, "august": 8, "september": 9,
           "oktober": 10, "november": 11, "dezember": 12}
# \b vor "Stand": sonst matcht „HöchstSTAND Oktober 2022" — eine
# historische Tatsache, kein Vintage-Marker (real aufgetreten).
VINTAGE_RE = re.compile(
    r"\bStand\s+(" + "|".join(_MONATE) + r")\s+(20\d{2})", re.I)


# Ein Zeitfenster, das der Satz SELBST als beendet beschreibt, ist kein
# Fund — sonst meldet der Waechter korrekte Vergangenheits-Aussagen
# („das Programm WAR befristet und ist mit 30.6.2026 ausgelaufen") und
# trainiert damit an, ihn zu ignorieren. Gilt nur fuer WARNUNG: ein
# KRITISCH-Satz behauptet ja gerade, der Stichtag stehe noch bevor.
_ENDE_MARKER = re.compile(
    r"\bausgelaufen\b|\bendete\b|\bbeendet\b|\blief\b[^.]{0,30}\baus\b|"
    r"\bwar\b|\bwaren\b|\bnicht mehr\b|\bbis einschliesslich\b", re.I)


def _nennt_das_ende(satz: str) -> bool:
    return bool(_ENDE_MARKER.search(satz))


def _als_datum(m) -> date | None:
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except (ValueError, IndexError):
        return None


# Satzgrenze: ein Punkt, dem KEINE Ziffer vorausgeht. Sonst zerreisst
# „31.8.2026" den Satz mitten im Datum — dieselbe Falle wie die
# Tausenderpunkte in den Summary-Regexen (lessons_learned).
_SATZENDE = re.compile(r"(?<!\d)\.(?=\s|$)")


def _satz_um(text: str, pos: int, breite: int = 220) -> str:
    """Der Satz um eine Fundstelle — damit ein Mensch urteilen kann,
    statt nur eine Regex-Meldung zu sehen."""
    start = 0
    for m in _SATZENDE.finditer(text, 0, pos):
        start = m.end()
    m = _SATZENDE.search(text, pos)
    ende = m.end() if m else min(len(text), pos + breite)
    return " ".join(text[start:ende].split())[:240]


def pruefe_eintrag(eintrag: dict, heute: date, max_vintage_months: int) -> list[dict]:
    """Alle Stichtags-Funde eines Fakts, mit Schweregrad und Belegsatz."""
    blob = json.dumps(eintrag, ensure_ascii=False)
    fid = eintrag.get("id") or eintrag.get("topic") or "?"
    funde: list[dict] = []

    for stufe, muster in (("KRITISCH", KRITISCH_PATTERNS),
                          ("WARNUNG", WARNUNG_PATTERNS)):
        for pat in muster:
            for m in pat.finditer(blob):
                datum = _als_datum(m)
                if datum is None or datum > heute:
                    continue                     # Zukunft ist in Ordnung
                if stufe == "WARNUNG" and _nennt_das_ende(_satz_um(blob, m.start())):
                    continue                     # korrekt als beendet beschrieben
                funde.append({
                    "stufe": stufe, "id": fid, "datum": datum.isoformat(),
                    "tage_her": (heute - datum).days,
                    "fund": " ".join(m.group(0).split())[:90],
                    "satz": _satz_um(blob, m.start()),
                })

    grenze_monate = heute.year * 12 + heute.month - max_vintage_months
    for m in VINTAGE_RE.finditer(blob):
        monat = _MONATE[m.group(1).lower()]
        jahr = int(m.group(2))
        if jahr * 12 + monat <= grenze_monate:
            funde.append({
                "stufe": "INFO", "id": fid, "datum": f"{jahr}-{monat:02d}",
                "tage_her": (heute.year * 12 + heute.month) - (jahr * 12 + monat),
                "fund": m.group(0), "satz": _satz_um(blob, m.start()),
            })
    return funde


def scanne(data_dir: Path, heute: date, max_vintage_months: int) -> list[dict]:
    alle: list[dict] = []
    for pfad in sorted(data_dir.glob("*.json")):
        try:
            d = json.loads(pfad.read_text(encoding="utf-8"))
        except Exception:
            continue                              # generierte Caches u. Ä.
        if not isinstance(d, dict):
            continue
        for key in LIST_KEYS:
            for eintrag in (d.get(key) or []):
                if not isinstance(eintrag, dict):
                    continue
                for f in pruefe_eintrag(eintrag, heute, max_vintage_months):
                    f["pack"] = pfad.name
                    alle.append(f)
    rang = {"KRITISCH": 0, "WARNUNG": 1, "INFO": 2}
    return sorted(alle, key=lambda f: (rang[f["stufe"]], -f["tage_her"]))


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--strict", action="store_true",
                    help="Exit 1, wenn KRITISCH- oder WARNUNG-Funde vorliegen")
    ap.add_argument("--max-vintage-months", type=int, default=24)
    ap.add_argument("--alert-webhook", default=os.getenv("EVIDORA_ALERT_WEBHOOK", ""))
    ap.add_argument("--json", help="Funde zusätzlich als JSON schreiben")
    ap.add_argument("--data-dir", default=str(DATA_DIR))
    args = ap.parse_args(argv)

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        print(f"data-dir nicht gefunden: {data_dir}", file=sys.stderr)
        return 2

    heute = date.today()
    funde = scanne(data_dir, heute, args.max_vintage_months)
    nach_stufe = {s: [f for f in funde if f["stufe"] == s]
                  for s in ("KRITISCH", "WARNUNG", "INFO")}

    print(f"Stichtags-Check {heute.isoformat()} — {data_dir}")
    for stufe in ("KRITISCH", "WARNUNG", "INFO"):
        treffer = nach_stufe[stufe]
        if not treffer:
            print(f"\nOK — keine {stufe}-Funde.")
            continue
        print(f"\n{stufe}: {len(treffer)} Fund(e)")
        for f in treffer:
            print(f"  {f['datum']} (+{f['tage_her']} "
                  f"{'Monate' if stufe == 'INFO' else 'Tage'})  "
                  f"{f['id'][:38]}  [{f['pack']}]")
            print(f"      Fund: {f['fund']}")
            print(f"      Satz: {f['satz'][:170]}")

    blockend = nach_stufe["KRITISCH"] + nach_stufe["WARNUNG"]
    if blockend and args.alert_webhook:
        try:
            import httpx
            zeilen = [f"{f['stufe']}: {f['id']} ({f['datum']}, "
                      f"+{f['tage_her']} d) — {f['fund']}" for f in blockend]
            httpx.post(
                args.alert_webhook,
                content=(f"stichtag_check {heute.isoformat()}\n"
                         + "\n".join(zeilen))[:3800],
                headers={"Title": "Evidora Stichtags-ALARM",
                         "Priority": "high", "Tags": "hourglass"},
                timeout=15,
            )
            print(f"\n  alert webhook posted to {args.alert_webhook}")
        except Exception as e:
            print(f"\n  alert webhook failed: {e}")

    if args.json:
        Path(args.json).write_text(
            json.dumps(funde, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"\nJSON geschrieben: {args.json}")

    if blockend and args.strict:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
