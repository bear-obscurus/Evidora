"""Flexionstolerantes Trigger-Matching fuer mehrwortige Begriffe.

QA50F-Befund 2 (2026-09-07): „In Nordkorea gibt es keine **freien** Wahlen"
feuert Freedom House nicht — der Trigger heisst `"freie wahlen"`. Mit
„freie Wahlen" feuert er. Lokal reproduziert:

    False  In Nordkorea gibt es keine freien Wahlen
    True   In Nordkorea gibt es keine freie Wahlen

WAS SUBSTRING-MATCHING SCHON KANN — UND WAS NICHT
==================================================
Gemessen ueber alle Trigger-Praedikate:

    Genitiv-s   ("oesterreich" -> "oesterreichs")   731/731  = 100 %
    Plural -en  ("wahl" -> "wahlen")               1597/1598 =  99 %
    Adjektiv-Endung im MEHRWORT-Begriff             108/492  =  21 %

Die ersten beiden sind kein Problem: eine Endung waechst HINTEN an, und
`"wahl" in "wahlen"` ist wahr. Der Ausfall ist ein **Infix** — bei
„freie wahlen" flektiert das VORDERE Wort, und damit reisst der Substring.

384 Ausfaelle ueber 241 mehrwortige deutsche Trigger. (Ehrlich: die Sonde
erzeugt alle vier Endungen, auch grammatisch falsche wie „algorithmisches
Diskriminierung" — realistisch ist rund die Haelfte.)

DIE REGEL
=========
Jedes Wort ausser dem letzten darf bis zu zwei zusaetzliche Buchstaben
tragen. Das deckt die deutschen Adjektiv-Endungen ab (-n, -r, -s, -m, -en,
-er, -es, -em), ohne die Begriffe selbst aufzuzaehlen — die Lehre aus den
Frontex-Flexionsformen (#141): wer Varianten auflistet, vergisst welche.

Das LETZTE Wort bleibt bewusst offen (Substring): dort waechst die Endung
ohnehin nach hinten, und eine Begrenzung wuerde „wahlen" gegen „wahl"
kaputtmachen.

WARUM NUR MEHRWORT-BEGRIFFE
===========================
Ein einzelnes Wort braucht die Regel nicht — bei ihm waechst jede Endung
hinten an und der Substring haelt. Die Einschraenkung ist zugleich der
Kostendeckel: der Regex-Pfad laeuft nur fuer die paar hundert mehrwortigen
Trigger, und auch dort erst, wenn der schnelle Substring-Vergleich scheitert.
"""

from __future__ import annotations

import re
from functools import lru_cache

from services._schreibweise import normalisiere

# Bis zu zwei Buchstaben je Wort. Drei waeren zu viel: „freie" duerfte dann
# „freiheit" werden und „grosse" „grossen…" ueber Wortgrenzen hinweg.
MAX_ENDUNG = 2
_WORTZEICHEN = "a-zäöüß"


@lru_cache(maxsize=8192)
def _muster(term_n: str) -> re.Pattern[str]:
    """Regex fuer einen bereits normalisierten Mehrwort-Begriff."""
    worte = term_n.split(" ")
    teile = [re.escape(w) + rf"[{_WORTZEICHEN}]{{0,{MAX_ENDUNG}}}"
             for w in worte[:-1]]
    teile.append(re.escape(worte[-1]))      # letztes Wort offen lassen
    return re.compile(r"(?<![" + _WORTZEICHEN + r"])" + r"\s+".join(teile))


def trifft(claim_n: str, term: str) -> bool:
    """Steckt ``term`` im bereits normalisierten ``claim_n``?

    ``claim_n`` muss durch ``normalisiere`` gelaufen sein — der Aufrufer
    macht das einmal je Claim, nicht einmal je Token.
    """
    if not term:
        return False
    term_n = normalisiere(term)
    if not term_n:
        return False
    # Schneller Normalfall zuerst: er deckt Einwort-Begriffe komplett ab und
    # die meisten Mehrwort-Treffer auch.
    if term_n in claim_n:
        return True
    if " " not in term_n.strip():
        return False
    return bool(_muster(term_n.strip()).search(claim_n))
