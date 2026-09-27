"""Welche ``context_notes`` in den Synthesizer-Prompt duerfen.

Anlass (2026-09-23, PR #191): Der Claim "Die Leerstandsabgabe in Vorarlberg
betraegt 3.000 Euro im Jahr" bekam **mostly_true@0.85** mit der Begruendung
"laut RIS-Daten 1.000 bis 3.000 Euro pro Jahr" — eine Spanne, die am Vortag
als unbelegt aus dem Fakt entfernt worden war. Sie stand nur noch in der
Wartungs-Notiz "Korrigiert 2026-09-23: Der Fakt nannte …".

Der Weg dorthin ist mechanisch: Die Pack-Services haengen alle
``context_notes`` an die ``description``, und die claim-zentrierte Kuerzung
im Synthesizer legt ihr Fenster um die Claim-Begriffe — also genau um die
zitierte falsche Zahl, waehrend der Rahmen ("Der Fakt nannte", "war
unbelegt") wegfaellt.

#191 hat das per Guard verboten (``tests/test_korrekturnotiz_ohne_alte_
zahlen.py``: eine Korrektur-Notiz darf die widerlegte Zahl nicht nennen).
Hier wird es mechanisch unmoeglich: Wartungs-Notizen erreichen den Prompt
gar nicht mehr.

ABSICHTLICH ENG
===============
Gemessen ueber alle kuratierten Pakete: 1.718 ``context_notes``, davon
**10** in der Form ``Korrigiert JJJJ-MM-TT:``. Die uebrigen tragen Inhalt
("Wichtige Differenzierung: …", "Datenstand: …", "Zensus 2022 hat die
Bevoelkerungs-Schaetzung um ~1,4 Mio nach unten korrigiert") und bleiben
drin — pauschal alle Notizen zu streichen wuerde Substanz kosten und
ausserdem die Eingabe des Rerankers aendern, dessen Schwellen daran
haengen.

Erkannt wird nur das datierte Praefix. Ein Wort wie "korrigiert" mitten im
Text ist Inhalt, kein Wartungsvermerk.
"""

from __future__ import annotations

import re

__all__ = ["ist_wartungsnotiz", "prompt_notizen", "WARTUNGS_PRAEFIX"]

# "Korrigiert 2026-09-23: …" — Datum verpflichtend, damit ein beilaeufiges
# "korrigiert" im Fliesstext nicht faelschlich als Wartungsvermerk gilt.
WARTUNGS_PRAEFIX = re.compile(r"^\s*Korrigiert \d{4}-\d{2}-\d{2}\s*:")


def ist_wartungsnotiz(notiz: str) -> bool:
    """True, wenn die Notiz ein datierter Korrektur-Vermerk ist."""
    return bool(isinstance(notiz, str) and WARTUNGS_PRAEFIX.match(notiz))


def prompt_notizen(notizen) -> list[str]:
    """Die Notizen, die in den Prompt duerfen — ohne Wartungs-Vermerke."""
    if not notizen:
        return []
    return [n for n in notizen if isinstance(n, str) and not ist_wartungsnotiz(n)]
