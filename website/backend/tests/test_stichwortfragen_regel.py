"""Die Prompt-Regel gegen „unverifiable, weil die Eingabe unvollständig sei".

Anlass (29.9.2026, Live-Messung nach #230/#231). Der Claim

    "Wie viele Lehrer in Österreich"

kam dreimal als ``unverifiable @ 0.1`` mit 0 Belegen zurück:

    "Die Behauptung ist unvollständig formuliert und enthält keine konkrete
     Frage oder Zahl."

Das Backend-Log zeigte dabei, dass die Quelle geliefert hatte
(``Source 4 (IQS Bildung AT) returned 1 results``) — die Zahl 132.000
Lehrkräfte stand im Prompt. Abgelehnt wurde nicht die Datenlage, sondern
die FORM der Eingabe.

Zwei Dinge wurden vorher gemessen und ausgeschlossen:

* Es ist kein allgemeines Fragment-Problem. Eine Batterie aus 12
  dokumentierten Kurz-Phrasings lieferte 11 saubere Antworten, darunter
  strukturgleiche Fälle wie „Wie viele offene Stellen in Österreich"
  (true@0.7) und „Wie viele Beschäftigte in Österreich 2024" (true@0.7).
* Es lag auch nicht an zu viel Evidenz. In #231 zog ein zu breites Token
  („wie viel" steckt in „wie viele") zusätzlich den Ausgaben-Fakt heran;
  nach der Verengung lieferte die Quelle nur noch einen Fakt — und die
  Antwort blieb `unverifiable`.

Auffällig blieb die Willkür: „Wie viele Lehrer in **AT**" wurde mit
`true@0.7` und der Zahl beantwortet, „Wie viele Lehrer in **Österreich**"
nicht. Gleiche Form, gleiche Daten, gegenteiliges Ergebnis.

Der Prompt verbot bis dahin nur, den QUELLEN pauschal fehlende Angaben zu
unterstellen („generische Aussagen wie 'die Quellen liefern keine konkreten
Angaben' sind unzulässig, wenn die Quellen sehr wohl relevante Werte
enthalten"). Die Eingabe als unvollständig abzulehnen war nicht gedeckt.

Diese Suite prüft die Regel als TEXT. Ob sie wirkt, kann nur die
Live-Messung zeigen; ihre Basislinie steht in der PR.

Keine Netzabfrage, kein Modell.
"""

import re
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

PROMPT = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
REGEL = PROMPT[PROMPT.index("- STICHWORT-FRAGEN"):]
REGEL = REGEL[:REGEL.index('- WENN du "unverifiable" wählst')]


def test_die_regel_steht_im_prompt():
    assert "STICHWORT-FRAGEN sind gültige Eingaben" in PROMPT


def test_sie_verbietet_die_begruendung_an_der_form():
    """Der Kern: Das Verdict haengt an der Datenlage, nicht an der
    Formulierung der Eingabe."""
    assert "unvollständig formuliert" in REGEL
    assert "keine konkrete Frage oder Zahl" in REGEL
    assert "Die Form der Eingabe ist kein Grund für ein Verdict" in REGEL


def test_sie_nennt_den_gemessenen_fall():
    assert "Wie viele Lehrer in Österreich" in REGEL
    assert "132.000" in REGEL


def test_sie_laesst_unverifiable_ausdruecklich_zu():
    """Sonst waere sie eine Einladung, alles zu beantworten. Der letzte
    Punkt der Regel haelt den Fall offen, in dem die Quellen die Frage
    NICHT beantworten."""
    assert "Nur wenn die Quellen die implizierte Frage NICHT beantworten" in REGEL
    assert "fehlende ANGABE" in REGEL


def test_sie_bleibt_eng():
    """Fuenf Zeilen. Eine Regel, die laenger wird als der Fall, den sie
    loest, faengt an, andere Regeln auszuhebeln."""
    zeilen = [z for z in REGEL.strip().split("\n") if z.strip()]
    assert len(zeilen) <= 6, len(zeilen)


@pytest.mark.parametrize("regel", [
    'generische Aussagen wie "die Quellen liefern keine konkreten Angaben" sind unzulässig',
    "PHILOSOPHISCHE Behauptungen",
    "NORMATIV-MORALISCHE Behauptungen",
])
def test_die_bestehenden_unverifiable_regeln_sind_unangetastet(regel):
    assert regel in PROMPT


def test_sie_steht_bei_den_falschen_unverifiable_verdicts():
    """Platzierung ist Teil der Wirkung — die Regel gehoert in den Block
    ueber falsche unverifiable-Verdicts, nicht irgendwohin."""
    block = PROMPT.index('BEISPIELE für falsche "unverifiable"-Verdicts')
    stichwort = PROMPT.index("- STICHWORT-FRAGEN")
    nuance = PROMPT.index('- WENN du "unverifiable" wählst')
    assert block < stichwort < nuance
