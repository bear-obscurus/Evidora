"""Die Prompt-Regel gegen „unverifiable trotz vorhandener Werte".

Anlass (27.9.2026). Nach #220/#221 standen für „Wie hoch sind die
Sparzinsen in Österreich?" BEIDE Werte im Prompt — 0,43 % täglich fällig,
2,10 % gebunden — und die Antwort lautete trotzdem:

    unverifiable @ 0.1
    "Die EZB-Daten liefern konkrete Werte für täglich fällige Einlagen
     (0,43 %) und gebundene Einlagen (2,10 %), aber keine pauschale
     Antwort auf die allgemeine Frage."

Der erste Versuch schrieb die Anweisung in den TITEL eines Datenpunkts
(„bei allgemeiner Frage BEIDE nennen"). Das wirkte nicht: Dort liest das
Modell sie als Beschreibung der Quelle, nicht als Regel für sich selbst.
Eine Anweisung an das Modell gehört in den System-Prompt.

Diese Suite prüft die Regel als TEXT — ob sie wirkt, kann nur die
Live-Messung zeigen, und die Basislinie dafür ist in der PR dokumentiert.
Geprüft wird hier, dass die Regel da ist, eng bleibt und die
Kategorienfehler-Regeln ausdrücklich nicht aushebelt.

Keine Netzabfrage, kein Modell.
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.synthesizer import SYSTEM_PROMPTS  # noqa: E402

PROMPT = "\n".join(str(v) for v in SYSTEM_PROMPTS.values())


def test_die_regel_steht_im_prompt():
    assert "MEHRERE MESSGRÖSSEN ZU DERSELBEN FRAGE" in PROMPT


def test_sie_verbietet_unverifiable_nur_bei_vorhandenen_werten():
    assert '"unverifiable" ist in diesem Fall NICHT zulässig' in PROMPT
    assert "NUR, wenn die Werte in den vorliegenden Quellen stehen" in PROMPT
    assert 'Fehlen sie, bleibt "unverifiable" richtig' in PROMPT


def test_sie_nennt_ein_konkretes_beispiel():
    """Ohne Beispiel bleibt so eine Regel abstrakt — der Fall, für den sie
    gebaut wurde, steht drin."""
    assert "0,43" in PROMPT and "2,10" in PROMPT


def test_sie_hebelt_die_kategorienfehler_regeln_nicht_aus():
    """Die gefährliche Richtung: eine Regel, die `unverifiable`
    zurückdrängt, darf normative und theologische Claims nicht mitreißen."""
    assert "Kategorienfehler-Regeln unten NICHT aus" in PROMPT
    assert "Normative, theologische, ästhetische und Prognose-Claims bleiben" in PROMPT


@pytest.mark.parametrize("regel", [
    "THEOLOGISCH-WELTANSCHAULICHE",
    "PHILOSOPHISCHE",
    "ÄSTHETISCHE/GESCHMACKLICHE",
    "NORMATIV-MORALISCHE",
])
def test_die_bestehenden_unverifiable_regeln_sind_unangetastet(regel):
    assert regel in PROMPT


def test_die_regel_steht_vor_den_kategorienfehlern():
    """Reihenfolge zählt: Erst die Ausnahme, dann die Regeln, auf die sie
    sich bezieht — sonst liest das Modell die Abgrenzung vor dem Fall."""
    assert PROMPT.index("MEHRERE MESSGRÖSSEN") < PROMPT.index("Kategorienfehler-Detection")
