"""Zwei Arbeitslosenquoten, und der Claim sagt nicht, welche gemeint ist.

Befund aus der QA-Batterie vom 2026-09-06 (Claim 19). „Die Arbeitslosigkeit in
Österreich sinkt 2026" bekam **`true@0.85`**, begründet mit Eurostat-ILO-
Monatswerten (5,8 % im Juni) — während die nationale AMS-Quote laut
WIFO/IHS-Prognose STEIGT (7,4 → 7,5 %).

**Beide Reihen sind korrekt.** Sie messen Verschiedenes: AMS zählt registrierte
Arbeitslose nach nationaler Berechnung, Eurostat das ILO-Konzept aus der
Arbeitskräfteerhebung. Die Differenz liegt in Österreich bei rund
2 Prozentpunkten, und die beiden können gleichzeitig in verschiedene
Richtungen zeigen.

Die Ursache war aber **kein Abwägungsfehler des Synthesizers, sondern
Retrieval** — wieder einmal:

* `wifo_ihs` trägt die nationale Prognose, aber sein Trigger kannte nur
  BIP/Rezession/Konjunktur. Mit einem Arbeitslosen-Claim war der Fakt **gar
  nicht erreichbar**.
* `ams_wifo` hat eine dritte Composite-Gruppe als Spezifitäts-Gate
  (`quote`, `rate`, `2024`, `wie hoch`, …). Sie lässt Frageformen durch, aber
  **keine Richtungs-Claims** („sinkt") und keine Jahre ab 2025 — also genau
  die Claim-Form, die ein Faktencheck am häufigsten sieht. Ergebnis: **0
  Treffer**.
* Der Fakt, der die Differenz erklärt (`ams_vs_eurostat_diff_konsens`),
  verlangte, dass der Nutzer „AMS" **und** „Eurostat"/„Methodik" selbst
  nennt. Wer fragt, ob die Arbeitslosigkeit sinkt, weiß aber genau das nicht.

Beide kuratierten Fakten existierten also und kamen nie im Prompt an; der
Synthesizer antwortete allein aus den Live-Konnektoren und hatte keine
Möglichkeit zu wissen, dass es eine zweite Quote gibt.
"""

import asyncio
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
AMS = json.loads((BACKEND / "data" / "ams_wifo.json").read_text(encoding="utf-8"))
MAX_STR = 400

from services.ams_wifo import search_ams_wifo  # noqa: E402
from services.wifo_ihs import (  # noqa: E402
    claim_mentions_wifo_ihs_cached,
    search_wifo_ihs,
)

RICHTUNGS_CLAIM = "Die Arbeitslosigkeit in Österreich sinkt 2026"


def _ams_topics(claim):
    return [x.get("topic") for x in asyncio.run(
        search_ams_wifo({"claim": claim, "original_claim": claim}))["results"]]


def _wifo(claim):
    return asyncio.run(search_wifo_ihs({"claim": claim,
                                        "original_claim": claim}))["results"][0]


# --------------------------------------------------------------------------
# Die Prognose muss bei Arbeitslosen-Claims überhaupt erreichbar sein
# --------------------------------------------------------------------------

def test_wifo_ihs_triggert_bei_arbeitslosen_claims():
    """Der Fakt trägt die nationale Prognose — vorher war er mit einem
    Arbeitslosen-Claim nicht erreichbar."""
    for claim in (RICHTUNGS_CLAIM,
                  "Steigt die Arbeitslosigkeit in Oesterreich?",
                  "Die Arbeitslosenquote in Österreich ist gesunken"):
        assert claim_mentions_wifo_ihs_cached(claim), claim


def test_wifo_ihs_triggert_weiterhin_bei_konjunktur():
    for claim in ("Österreich steckt in der Rezession",
                  "Wie entwickelt sich die Konjunktur in Österreich?"):
        assert claim_mentions_wifo_ihs_cached(claim), claim


def test_kein_ueber_trigger_bei_wifo_ihs():
    for claim in ("Die Arbeitslosigkeit in Deutschland steigt",
                  "Wie viele Einwohner hat Wien?"):
        assert not claim_mentions_wifo_ihs_cached(claim), claim


# --------------------------------------------------------------------------
# Die Messgrößen-Warnung
# --------------------------------------------------------------------------

def test_warnung_nennt_beide_quoten_und_die_divergenz():
    text = _wifo(RICHTUNGS_CLAIM)["display_value"]
    assert "MESSGRÖSSE" in text
    assert "AMS-Quote" in text and "Eurostat/ILO" in text
    assert "GLEICHZEITIG in die andere" in text, (
        "die entscheidende Aussage: beide Reihen können gegenläufig sein")


def test_warnung_haengt_nur_bei_arbeitslosen_claims_an():
    """Bei jedem Konjunktur-Claim mitzulaufen würde das Prompt-Budget
    sprengen — und die Warnung wäre als letzter Satz das Erste, was die
    Kürzung frisst."""
    assert "MESSGRÖSSE" not in _wifo("Österreich steckt in der Rezession")["display_value"]


def test_beide_zweige_bleiben_unter_dem_prompt_budget():
    """Der Vertrag aus #131 hat den ersten Entwurf sofort gefangen — er war
    618 Zeichen lang."""
    for claim in (RICHTUNGS_CLAIM, "Österreich steckt in der Rezession",
                  "Steigt die Arbeitslosigkeit in Oesterreich?"):
        dv = _wifo(claim)["display_value"]
        assert len(dv) <= MAX_STR, f"{claim}: {len(dv)} Zeichen"


def test_richtung_der_nationalen_quote_bleibt_sichtbar():
    text = _wifo(RICHTUNGS_CLAIM)["display_value"]
    assert "steigt" in text and "7,5" in text and "7,4" in text


# --------------------------------------------------------------------------
# Das Spezifitäts-Gate in ams_wifo
# --------------------------------------------------------------------------

def test_richtungs_claims_passieren_das_gate():
    """Vorher: 0 Treffer, weil die dritte Composite-Gruppe nur
    Frageformen und die Jahre 2023/2024 kannte."""
    for claim in (RICHTUNGS_CLAIM,
                  "Die Arbeitslosigkeit in Österreich ist gestiegen",
                  "Die Arbeitslosenquote in Oesterreich geht zurück"):
        assert _ams_topics(claim), claim


def test_der_erklaerende_fakt_faehrt_mit():
    """`ams_vs_eurostat_diff_konsens` verlangte, dass der Nutzer die beiden
    Messgrößen selbst nennt. Wer fragt, ob die Arbeitslosigkeit sinkt, weiß
    genau das nicht."""
    for claim in (RICHTUNGS_CLAIM,
                  "Wie hoch ist die Arbeitslosenquote in Oesterreich?"):
        assert "ams_vs_eurostat_diff_konsens" in _ams_topics(claim), claim


def test_der_erklaerende_fakt_nennt_beide_konzepte():
    f = next(x for x in AMS["facts"] if x["topic"] == "ams_vs_eurostat_diff_konsens")
    kopf = f["headline"]
    assert "AMS" in kopf and "Eurostat" in kopf
    assert "KORREKT" in kopf, "beide Werte sind richtig — das ist der Kern"
    assert "ILO" in kopf


def test_kein_ueber_trigger_bei_ams_wifo():
    for claim in ("Die Arbeitslosigkeit in Deutschland steigt",
                  "Die Inflation in Österreich steigt",
                  "Wie viele Einwohner hat Wien?"):
        assert not _ams_topics(claim), claim
