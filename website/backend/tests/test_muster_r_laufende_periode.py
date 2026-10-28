"""Muster R — die Begründung widerspricht SICH SELBST.

Nach #284 blieb die Hälfte von QA50I #24 offen. Live reproduzierbar, in der
AUSSAGE-Form:

    Claim:   "Die Grippewelle in Deutschland ist dieses Jahr besonders stark"
    Summary: "Die AKTUELLE Grippewelle 2026/27 … ist die schwächste der
              letzten vier ABGESCHLOSSENEN Saisons"

Was läuft, kann nicht das Extrem einer Menge abgeschlossener Perioden sein.
In den Daten steht der Superlativ über die abgeschlossene Saison 2025/26 —
das Modell hat ihn auf die laufende übertragen (Meloni-Drift-Klasse, #122).

**#284 hat das Wort schon in die Daten geschrieben** („die SCHWÄCHSTE der
vier ABGESCHLOSSENEN Saisonen"). Gemessen half das nur bei der FRAGE-Form:

    "Ist die Grippewelle … besonders stark?"  → korrekt getrennt
    "Die Grippewelle … ist … besonders stark" → Drift bleibt

Das Modell übernimmt das Wort und hängt den Superlativ trotzdem ans falsche
Subjekt. Ein Wort in den Daten kann eine Zuordnung nicht erzwingen; das muss
der Wächter tun. Deshalb Muster R.

## Drei Bedingungen, alle nötig

1. ein Superlativ (Artikel + Wort auf *-ste*),
2. der die Menge im **Genitiv** nennt, und diese Menge ist ausdrücklich
   abgeschlossen („der letzten vier abgeschlossenen Saisons"),
3. und dessen Subjekt im **selben Teilsatz** als laufend bezeichnet ist.

## Zwei Fassungen vorher, beide gemessen und verworfen

**Erste Fassung — Wortliste.** Sie kannte „schwächste/stärkste/höchste/…"
und verfehlte zwei von vier Zielfällen: „die **kürzeste** der vergangenen
Perioden" (Wort fehlte) und „**das** wärmste der vergangenen Jahre" (nur
„die" als Artikel). Dieselbe Falle wie #140/#141 — jetzt steht dort jeder
Superlativ auf *-ste*.

**Zweite Fassung — ohne Partitiv.** Sie schlug an einer Gegenprobe an:

    "Die aktuelle Lage ist die beste Grundlage FÜR die vergangenen Jahre
     der Reform."

Superlativ und abgeschlossene Menge im selben Teilsatz, aber in keinem
Rang-Verhältnis. Erst die Genitiv-Bedingung („die beste **der** …") trennt
das. 10 von 10 Proben erst mit dem Partitiv.

**Und ein Fehltreffer aus dem Satz-Splitter.** Die allererste Fassung nahm
das „laufende" aus dem FOLGESATZ als Subjekt:

    "… die schwächste der vier abgeschlossenen Saisonen seit 2022/23.
     Die laufende Saison 2026/27 zeigt aktuell …"

``teile_in_einheiten`` trennt hinter „2022/23." nicht (Ziffer vor dem
Punkt — bewusst, siehe ``_satzgrenzen``), also stand beides in einer
Einheit. Muster R schneidet deshalb selbst bis zur nächsten
Teilsatz-Grenze zurück. Dieselbe Klasse wie #227.

## Was der Wächter tut

**Kein Label-Wechsel.** Welche Hälfte des Widerspruchs stimmt, ist von hier
aus nicht zu entscheiden. Gesenkt wird die Konfidenz auf
``_ZW_KONFIDENZ_DECKEL`` (0,6) — dieselbe Stufe wie Muster M und der
Zählweisen-Guard, die aus demselben Grund abwerten — plus ein Hinweis, der
den Widerspruch benennt.

Gemessen: **0 Treffer auf 98 QA50I-Summaries**, 10 von 10 Proben richtig.

Dependency-light: nur verdict_postprocess, kein Netz.
"""
import json
import os
import pathlib
import sys

import pytest

BACKEND = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND))

from services.verdict_postprocess import (  # noqa: E402
    _BESTIMMTE_LABELS,
    _ZW_KONFIDENZ_DECKEL,
    apply_verdict_postprocessing,
    superlativ_am_laufenden_subjekt as finde,
)

#: Der live gemessene Satz, Wort fuer Wort.
LIVE = ("Die aktuelle Grippewelle 2026/27 in Deutschland liegt mit 6.854 "
        "ARE-Fällen je 100.000 (KW 41/2026) 10 % unter dem Vorjahreswert "
        "(7.584) und ist die schwächste der letzten vier abgeschlossenen "
        "Saisons (Höchstwert 2025/26: 8.661 je 100.000).")

#: Dieselbe Datenlage, korrekt zugeordnet — so antwortet die FRAGE-Form.
KORREKT = ("Die Grippewelle 2025/26 in Deutschland war mit einem "
           "GrippeWeb-Höchstwert von 8.661 je 100.000 die schwächste der "
           "vier abgeschlossenen Saisonen seit 2022/23. Die laufende "
           "Saison 2026/27 zeigt aktuell 10 % niedrigere Werte als im "
           "Vorjahr.")


def test_der_live_fall_wird_erkannt():
    assert finde(LIVE)


def test_die_korrekte_zuordnung_bleibt_unberuehrt():
    """Der Satz-Splitter trennt hinter '2022/23.' nicht — hier haette die
    erste Fassung einen Fehltreffer gebaut."""
    assert not finde(KORREKT)


@pytest.mark.parametrize("summary", [
    "Die laufende Saison ist die schwächste der letzten vier Saisonen.",
    "Die aktuelle Legislaturperiode ist die kürzeste der vergangenen "
    "Perioden.",
    "Das laufende Jahr ist das wärmste der vergangenen Jahre.",
    "Die derzeitige Welle ist die stärkste der bisherigen Saisonen.",
])
def test_der_widerspruch_wird_in_jeder_form_erkannt(summary):
    """Wortliste und Artikel waren die Luecken der ersten Fassung."""
    assert finde(summary), summary


@pytest.mark.parametrize("summary", [
    # Kein laufendes Subjekt — eine abgeschlossene Periode DARF ein Extrem
    # einer Menge abgeschlossener Perioden sein.
    "2024 war das wärmste der vergangenen Jahre.",
    "Die Saison 2025/26 war die schwächste der vier abgeschlossenen "
    "Saisonen.",
    # Ein Vergleich, kein Superlativ — das ist Muster O/P.
    "Die aktuelle Welle ist schwächer als die letzten vier Saisonen.",
    # Ein Superlativ OHNE abgeschlossene Menge: "bisher" ist zulaessig.
    "Die aktuelle Saison zeigt bisher die niedrigsten Werte.",
    # Superlativ und Menge im selben Teilsatz, aber kein Rang-Verhaeltnis.
    "Die aktuelle Lage ist die beste Grundlage für die vergangenen Jahre "
    "der Reform.",
    # Die Menge ist selbst laufend — kein Widerspruch.
    "Die aktuelle Saison ist die schwächste der vier laufenden Saisonen.",
])
def test_keine_fehltreffer(summary):
    assert not finde(summary), summary


def test_leere_eingabe():
    assert finde("") == ""
    assert finde(None) == ""


# ------------------------------------------------------------ Ende zu Ende

def _lauf(summary, verdict="mostly_false", confidence=0.7, belege=4):
    res = {
        "verdict": verdict, "confidence": confidence,
        "summary": summary, "nuance": "",
        "evidence": [{"source": "RKI", "finding": f"Befund {i}",
                      "strength": "strong"} for i in range(belege)],
    }
    return apply_verdict_postprocessing(
        res, [], "Die Grippewelle in Deutschland ist dieses Jahr "
                 "besonders stark")


def test_die_konfidenz_wird_gesenkt():
    out = _lauf(LIVE)
    assert out["confidence"] == _ZW_KONFIDENZ_DECKEL


def test_das_label_bleibt_unberuehrt():
    """Welche Haelfte des Widerspruchs stimmt, ist hier nicht entscheidbar
    — ein Label-Wechsel waere geraten."""
    for verdict in _BESTIMMTE_LABELS:
        out = _lauf(LIVE, verdict=verdict)
        assert out["verdict"] == verdict, verdict


def test_der_hinweis_benennt_den_widerspruch():
    out = _lauf(LIVE)
    assert "laufende Periode" in out["nuance"]
    assert "ABGESCHLOSSENEN" in out["nuance"]


def test_eine_schon_niedrige_konfidenz_wird_nicht_angehoben():
    out = _lauf(LIVE, confidence=0.4)
    assert out["confidence"] == 0.4


def test_bei_korrekter_zuordnung_kein_eingriff():
    out = _lauf(KORREKT, confidence=0.85)
    assert out["confidence"] == 0.85
    assert "laufende Periode" not in (out.get("nuance") or "")


# -------------------------------------------------------- der Korpus-Beleg

@pytest.mark.skipif(
    not (pathlib.Path(os.path.expanduser("~/Documents/Evidora Dokumente"))
         / "qa50i" / "qa50i_results.jsonl").exists(),
    reason="QA50I-Ergebnisse liegen ausserhalb des Repos")
def test_kein_ueber_trigger_auf_den_qa_summaries():
    """0 von 98 — gemessen am 27.10.2026.

    Die Datei liegt im Dokumente-Ordner, nicht im Repo; in der CI wird
    diese Pruefung uebersprungen und der Grund steht dran (#283).
    """
    pfad = (pathlib.Path(os.path.expanduser("~/Documents/Evidora Dokumente"))
            / "qa50i" / "qa50i_results.jsonl")
    treffer = []
    with open(pfad, encoding="utf-8") as fh:
        for zeile in fh:
            r = json.loads(zeile)
            text = f"{r.get('summary', '')} {r.get('nuance') or ''}"
            if finde(text):
                treffer.append(r.get("id"))
    assert not treffer, treffer
