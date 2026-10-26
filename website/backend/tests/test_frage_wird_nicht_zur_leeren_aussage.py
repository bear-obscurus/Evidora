"""Die Frage wurde zu einer leeren Aussage — und die war nicht prüfbar.

Nach #280 erreichte "Wie viele WCAG-Erfolgskriterien gibt es?" den Dienst,
und die Antwort blieb trotzdem falsch:

    verdict: unverifiable @ 0.1
    summary: "Die bereitgestellten Quellen enthalten keine Information
              zur Anzahl der WCAG-Erfolgskriterien."

## Vier Messpunkte schließen Retrieval und Budget aus

    Source 14 (WCAG 2.2) returned 1 results          (Prod-Log)
    Reranked WCAG 2.2: kept 1/1, top score 0.321     (Prod-Log)
    "86" an Position 62 von 366 im display_value, Budget 1200
    keine "Fan-out budget"-Zeile — nichts gekürzt

Die Zahl lag dem Modell also vor.

## Was es wirklich war

Dieselbe Behauptung als AUSSAGE funktioniert:

    "WCAG 2.2 hat 87 Erfolgskriterien"   → mostly_true @ 0.8
        "WCAG 2.2 listet 87 Erfolgskriterien auf, wobei SC 4.1.1 als
         obsolet gilt … sodass effektiv 86 aktive bestehen."
    "WCAG 2.2 hat 120 Erfolgskriterien"  → false @ 0.8

Das Modell liest die Zeile also. Der Unterschied steckt in der
Analyse-Stufe: Sie formt aus der FRAGE eine Behauptung, und dabei entstand

    "WCAG 2.2 hat eine bestimmte Anzahl von Erfolgskriterien"

Das ist keine Behauptung, sondern eine Leerform. Der Synthesizer meldet
völlig korrekt, dass er sie nicht prüfen kann.

⚠️ **Meine erste Vermutung war falsch** — ich hatte die FORM der
WCAG-Zeile verdächtigt (zwei Zahlen in einem Nebensatz). Dass die
Aussage-Variante aus derselben Zeile die richtige Antwort holt, widerlegt
das.

## Und die Leerform allein genügt auch nicht

    "Wie hoch ist die Arbeitslosenquote in Österreich?"
        analysis.claim: "Die Arbeitslosenquote in Österreich beträgt
                         einen bestimmten Wert"      → true @ 0.95 ✓
    "Wie viele Lehrer in Österreich?"
        analysis.claim: "Die Anzahl der Lehrer in Österreich"
                                                      → true @ 0.7 ✓

Dieselbe Leerform, und die Antwort stimmt. Es sind also ZWEI Ursachen, die
sich verstärken: die leere Behauptung und eine Zahl, die in der Zeile nicht
die offensichtliche Antwort ist. Deshalb beide behoben:

1. **Analyse-Stufe**: Platzhalter-Prädikate ("hat eine bestimmte Anzahl",
   "beträgt einen bestimmten Wert") sind ausdrücklich verboten. Statt
   dessen die gesuchte Größe als Nominalphrase — genau die Form, die das
   Modell beim Lehrer-Fall von selbst gewählt hat und die dort funktioniert.
2. **WCAG-Überblick-Zeile**: Der ``indicator_name`` hieß nur "WCAG 2.2 —
   Überblick" und trug das Thema, nicht die Antwort. Jetzt steht die Zahl
   darin. Lehre aus #192: den fertigen Satz liefern.

Der #87-Pfad bleibt: Ohne Zahlen in den Daten steht auch im Titel keine.

Dependency-light: Dienst und Prompt-Text, kein Netz.
"""
import asyncio
import copy
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services.claim_analyzer import SYSTEM_PROMPT  # noqa: E402
from services.synthesizer import prompt_budget  # noqa: E402
from services.wcag22 import (  # noqa: E402
    _build_overview_result,
    _load_static_json,
    search_wcag22,
)


def _daten():
    d = _load_static_json()
    assert d, "wcag22.json nicht geladen"
    return d


# ------------------------------------------------- die Zeile traegt die Antwort

def test_der_titel_nennt_beide_zahlen():
    row = _build_overview_result(_daten())
    titel = row["indicator_name"]
    assert "86" in titel, titel
    assert "87" in titel, titel
    assert "Erfolgskriterien" in titel, titel


def test_der_titel_steht_im_prompt_budget():
    """Der ``indicator_name`` ist das kuerzeste Feld — die Zahl muss ganz
    sicher hineinpassen, auch wenn die Zeile nicht Rang 0 ist."""
    row = _build_overview_result(_daten())
    titel = row["indicator_name"]
    budget = prompt_budget("indicator_name", 3)
    assert len(titel) <= budget, (len(titel), budget)
    assert titel.index("86") < budget


def test_ohne_zahlen_steht_keine_zahl_im_titel():
    """Der #87-Pfad: lieber keine Angabe als eine erfundene.

    Giftprobe — die Zahlen werden aus einer KOPIE der Daten entfernt.
    """
    d = copy.deepcopy(_daten())
    d.setdefault("key_facts", {}).pop("aktive_kriterien", None)
    d["key_facts"].pop("gelistete_kriterien", None)
    row = _build_overview_result(d)
    titel = row["indicator_name"]
    assert "86" not in titel and "87" not in titel, titel
    assert "nicht verfuegbar" in titel.lower(), titel
    # Und auch im display_value darf keine Zahl auftauchen.
    assert "nicht verfuegbar" in row["display_value"].lower()


def test_die_zahl_bleibt_auch_im_display_value():
    """Der Titel ergaenzt die Zeile, er ersetzt sie nicht."""
    row = _build_overview_result(_daten())
    assert "86" in row["display_value"]
    assert "87" in row["display_value"]


@pytest.mark.parametrize("claim", [
    "Wie viele WCAG-Erfolgskriterien gibt es?",
    "Wie viele Erfolgskriterien hat WCAG 2.2?",
    "WCAG 2.2 hat 87 Erfolgskriterien",
    "How many WCAG success criteria are there?",
])
def test_die_zahl_erreicht_den_prompt(claim):
    r = asyncio.run(search_wcag22({"claim": claim}))
    rows = r.get("results") or []
    assert rows, claim
    sichtbar = " ".join(
        f"{row.get('indicator_name', '')} {row.get('display_value', '')}"
        for rang, row in enumerate(rows)
        if rang == 0 or "overview" in str(row.get("indicator", ""))
    )
    assert "86" in sichtbar and "87" in sichtbar, sichtbar[:200]


# --------------------------------------------- die Regel in der Analyse-Stufe

def test_die_analyse_verbietet_platzhalter_praedikate():
    """Die drei gemessenen Leerformen stehen woertlich im Verbot.

    Sonst waere die Regel eine Absichtserklaerung: Das Modell hat genau
    diese Wendungen erzeugt.
    """
    for leerform in ("hat eine bestimmte Anzahl",
                     "betraegt einen bestimmten Wert"):
        assert leerform in SYSTEM_PROMPT, leerform


def test_die_analyse_nennt_die_gewuenschte_form():
    """Nominalphrase statt Leer-Aussage, mit Beispiel."""
    assert "Nominalphrase" in SYSTEM_PROMPT
    assert "Die Anzahl der Erfolgskriterien in WCAG 2.2" in SYSTEM_PROMPT


def test_die_analyse_behaelt_pruefbare_fragen_als_aussage():
    """"Hat WCAG 2.2 wirklich 87 Kriterien?" ist eine pruefbare Aussage —
    die darf die Regel nicht zur Nominalphrase verflachen."""
    assert "WCAG 2.2 hat 87 Erfolgskriterien" in SYSTEM_PROMPT
    assert "pruefbare Zahl" in SYSTEM_PROMPT


def test_fragen_gelten_als_gueltige_eingabe():
    """Dieselbe Linie wie #232: die Form der Eingabe ist kein Grund fuer
    ein Verdict."""
    assert "FRAGEN sind gueltige Eingaben" in SYSTEM_PROMPT


def test_die_regel_steht_im_regel_block():
    """Nicht irgendwo im Prompt, sondern dort, wo die Analyse-Regeln
    stehen — sonst liest das Modell sie nach den Query-Regeln."""
    regeln = SYSTEM_PROMPT.index("Regeln:")
    pubmed = SYSTEM_PROMPT.index("PubMed-Query-Regeln")
    frage = SYSTEM_PROMPT.index("FRAGEN sind gueltige Eingaben")
    assert regeln < frage < pubmed


def test_der_prompt_bleibt_gueltiges_json_schema():
    """Die Ergaenzung darf das JSON-Beispiel nicht beschaedigt haben."""
    anfang = SYSTEM_PROMPT.index("{")
    ende = SYSTEM_PROMPT.index("}", anfang) + 1
    schema = json.loads(SYSTEM_PROMPT[anfang:ende]
                        .replace("true/false", "true")
                        .replace("0.0-1.0", "0.5"))
    assert "claim" in schema and "category" in schema
