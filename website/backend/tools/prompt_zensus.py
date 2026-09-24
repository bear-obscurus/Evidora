#!/usr/bin/env python3
"""Prompt-Zensus: Kommt die entscheidende Zahl ueberhaupt im Prompt an?

Der Synthesizer kuerzt jedes Prompt-Feld auf ``MAX_STR`` = 400 Zeichen und
waehlt dafuer die claim-relevantesten Saetze (``_claim_centered_truncate``).
Ein Fakt mit 6.000 Zeichen liefert dem Modell also rund 6 % seines Inhalts —
welche 6 %, entscheidet der Claim. Bis jetzt war das unbeobachtet: Ein Fakt
konnte die richtige Zahl enthalten, ein Test konnte das gruen bestaetigen,
und das Modell sah sie trotzdem nie.

Der Zensus misst genau diese Stufe — deterministisch, ohne LLM-Aufruf:

    display_value = "<headline>. <data-Felder>"   (wie in den Pack-Services)
    description   = data["context"] + " " + context_notes
    -> _prompt_claim_terms(claim) -> _claim_centered_truncate(..., 400)
    -> kommt jeder Muss-Treffer der Batterie im Ergebnis vor?

Aufruf:

    python3 tools/prompt_zensus.py                  # Batterie pruefen
    python3 tools/prompt_zensus.py --zeige-prompt   # ankommenden Text zeigen
    python3 tools/prompt_zensus.py --alt            # mit der alten Satztrennung
                                                    # (Vorher/Nachher-Vergleich)

Exit-Code 1, sobald ein Muss-Treffer fehlt — damit taugt der Zensus als Gate.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services import synthesizer as syn  # noqa: E402
from services._struct_marker import render_data_with_marker  # noqa: E402

DATA = BACKEND / "data"
BATTERIE = Path(__file__).resolve().parent / "prompt_zensus_batterie.json"


def _fakt(datei: str, fakt_id: str) -> dict:
    d = json.loads((DATA / datei).read_text(encoding="utf-8"))

    def such(o):
        if isinstance(o, dict):
            if o.get("id") == fakt_id:
                return o
            for v in o.values():
                if (r := such(v)) is not None:
                    return r
        elif isinstance(o, list):
            for v in o:
                if (r := such(v)) is not None:
                    return r
        return None

    f = such(d)
    if f is None:
        raise SystemExit(f"Fakt nicht gefunden: {datei} :: {fakt_id}")
    return f


def felder(fakt: dict) -> dict[str, str]:
    """display_value und description — so wie die Pack-Services sie bauen."""
    d = fakt.get("data") or {}
    notizen = " | ".join(fakt.get("context_notes") or [])
    return {
        "display_value": f"{fakt.get('headline', '?')}. {render_data_with_marker(d)}",
        "description": (d.get("context", "") + " " + notizen).strip(),
    }


def ankommend(text: str, claim: str, budget: int) -> str:
    terme = syn._prompt_claim_terms({}, claim)
    if len(text) <= budget:
        return text
    return syn._claim_centered_truncate(text, terme, budget)


def _alte_satztrennung() -> None:
    """Die Trennung vor dem Fix — fuer den Vorher/Nachher-Vergleich."""
    import services._satzgrenzen as sg

    sg.teile_in_einheiten = lambda s: [  # type: ignore[assignment]
        seg.strip() for seg in re.split(r"(?<=[.!?])\s+|\n+", s) if seg and seg.strip()
    ]


def pruefe(eintrag: dict, max_str: int | None = None) -> dict:
    """Misst mit derselben Politik wie der Synthesizer: display_value bekommt
    PROMPT_MAX_DISPLAY (erstes Ergebnis je Quelle), description PROMPT_MAX_STR."""
    fakt = _fakt(eintrag["datei"], eintrag["fakt"])
    texte = felder(fakt)
    budgets = {"display_value": max_str or syn.PROMPT_MAX_DISPLAY,
               "description": syn.PROMPT_MAX_STR}
    zusammen = {k: ankommend(v, eintrag["claim"], budgets[k])
                for k, v in texte.items() if v}
    alles = " ".join(zusammen.values())
    # Ein Muss-Eintrag darf eine Liste von Schreibvarianten sein (any-of):
    # entscheidend ist die Information, nicht ihr Wortlaut.
    def _da(m) -> bool:
        return any(a in alles for a in (m if isinstance(m, list) else [m]))

    fehlt = [m for m in eintrag["muss"] if not _da(m)]
    return {
        "claim": eintrag["claim"],
        "fakt": eintrag["fakt"],
        "voll": len(texte["display_value"]),
        "an": len(zusammen.get("display_value", "")),
        "fehlt": fehlt,
        "text": zusammen,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--batterie", default=str(BATTERIE))
    p.add_argument("--max-str", type=int, default=None,
                   help="Budget fuer display_value (Vorgabe: synthesizer.PROMPT_MAX_DISPLAY)")
    p.add_argument("--zeige-prompt", action="store_true")
    p.add_argument("--alt", action="store_true", help="mit der alten Satztrennung messen")
    p.add_argument("--luecken", action="store_true",
                   help="statt der Pflicht-Claims die bekannten Luecken messen "
                        "(zeigt, was ein groesseres Budget brauchen wuerde)")
    a = p.parse_args()

    if a.alt:
        _alte_satztrennung()

    roh = json.loads(Path(a.batterie).read_text(encoding="utf-8"))
    batterie = roh["bekannte_luecken"] if a.luecken else roh["claims"]
    ergebnisse = [pruefe(e, a.max_str) for e in batterie]

    print(f"{'Fakt':38s} {'voll':>6s} {'an':>5s} {'%':>5s}  Claim")
    print("-" * 110)
    for r in ergebnisse:
        quote = 100.0 * r["an"] / max(1, r["voll"])
        marke = "OK " if not r["fehlt"] else "!! "
        print(f"{marke}{r['fakt'][:35]:35s} {r['voll']:6d} {r['an']:5d} {quote:4.0f}%  {r['claim'][:60]}")
        if r["fehlt"]:
            print(f"    fehlt im Prompt: {r['fehlt']}")
        if a.zeige_prompt:
            for k, v in r["text"].items():
                print(f"    [{k}] {v}")

    schlecht = [r for r in ergebnisse if r["fehlt"]]
    gesamt_voll = sum(r["voll"] for r in ergebnisse)
    gesamt_an = sum(r["an"] for r in ergebnisse)
    print("-" * 110)
    print(f"{len(ergebnisse) - len(schlecht)}/{len(ergebnisse)} Claims vollstaendig | "
          f"Durchsatz {100.0 * gesamt_an / max(1, gesamt_voll):.1f} % der Fakt-Zeichen "
          f"({gesamt_an} von {gesamt_voll})")
    # Beim Luecken-Lauf ist ein Fehlschlag der erwartete Zustand, kein Fehler.
    return 0 if a.luecken else (1 if schlecht else 0)


if __name__ == "__main__":
    raise SystemExit(main())
