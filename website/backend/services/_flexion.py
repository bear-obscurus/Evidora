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

WORTGRENZEN (2026-09-26)
========================
Kurze Trigger-Tokens stecken in fremden Woertern. Gemessen ueber die 2.603
dokumentierten ``claim_phrasings_handled`` (Fakten MIT Trigger-Feldern) plus
1.163 Stress-Test-Claims, echte ``find_matching_items`` gegen main:

    "tum"   TU Muenchen   <- TUMor, WachsTUM, EigenTUM, DaTUM   17 + 7 Claims
    "ass"   Aspirin       <- wASSer, TrinkwASSer                 4 + 5
    "de "   ETER Dtschl.  <- StudierenDE_                        3 + 0
    "neutral" in BEIDEN   <- klimaNEUTRAL                        1 + 3
      Composite-Gruppen      (die Regel war damit ein Stichwort)

Die Abhilfe ist im Datenbestand laengst angelegt: `" eter "`, `" eu "`,
`" at "` — das Leerzeichen am Rand markiert die Wortgrenze. Es trug aber nur
MITTEN im Satz. Vor dem ersten Wort steht kein Leerzeichen, hinter dem
letzten keins, und vor einem Satzzeichen auch nicht:

    "KI ersetzt 47 % der Jobs"                   " ki "  trifft nicht
    "Treat-to-Target ist Standard-Strategie bei RA"  " ra "  trifft nicht

Sieben dokumentierte Phrasings trafen deshalb ihren Fakt nicht. Und Binden
hatte einen Preis: wer `"ki"` zu `" ki "` band, verlor jeden Claim, der mit
„KI" beginnt.

``trifft_mit_wortgrenze`` prueft ein Token mit Rand-Leerzeichen ZUSAETZLICH
gegen ``wortgrenzen_fassung`` — den Claim mit Leerzeichen an beiden Raendern
und statt jedes Satzzeichens. Nur das ROHE Token zaehlt: `"eu-"`
normalisiert zwar zu `"eu "`, ist aber als Praefix gemeint („EU-Beitritt");
als Wortende behandelt traefe es zusaetzlich „neu." am Satzende. Tokens ohne
Rand-Leerzeichen verhalten sich exakt wie vorher.

Ohne jede Daten-Aenderung gemessen: 0 Muss-Treffer verloren, 7 gewonnen,
2 zusaetzliche dienst-fremde Paare — beide durch schon vorhandene gebundene
Tokens am Claim-Ende (`" at "`, `" eu "`); mitten im Satz („… in der EU")
trafen dieselben Saetze schon vorher.

Grenze: Der Regex-Pfad oben streift die Rand-Leerzeichen eines Mehrwort-
Begriffs ab. Die Wortgrenze VORN erzwingt er selbst, die HINTEN nicht —
`"studentenzahl de "` traefe weiter „Studentenzahl DER Uni Wien". Hinten
binden geht nur mit Einwort-Tokens.
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


# Satzzeichen, die ein Wort beenden. Bindestrich, Schraegstrich usw. macht
# ``normalisiere`` bereits zu Leerzeichen.
_SATZZEICHEN = re.compile(r"[.,;:!?()\[\]{}\"'„“”‚‘’«»…]")


# Gecacht wie ``normalisiere``: der Matcher ruft das je Fakt, also fuer
# denselben Claim hunderte Male — ungecacht kostete das +2,4 ms je Claim.
@lru_cache(maxsize=4096)
def wortgrenzen_fassung(claim_n: str) -> str:
    """Der normalisierte Claim mit Leerzeichen an beiden Raendern und statt
    jedes Satzzeichens — die Vergleichsfassung fuer gebundene Tokens.

    >>> wortgrenzen_fassung("ersetzt durch ki.")
    ' ersetzt durch ki '
    """
    return " " + " ".join(_SATZZEICHEN.sub(" ", claim_n).split()) + " "


def ist_gebunden(term) -> bool:
    """Traegt das ROHE Token ein Leerzeichen am Rand?"""
    return isinstance(term, str) and (term[:1] == " " or term[-1:] == " ")


def trifft_mit_wortgrenze(claim_n: str, claim_w: str, term) -> bool:
    """``trifft`` — und fuer gebundene Tokens zaehlen auch Claim-Rand und
    Satzzeichen als Wortgrenze.

    ``claim_w`` ist ``wortgrenzen_fassung(claim_n)``; beides berechnet der
    Aufrufer einmal je Claim, nicht einmal je Token.
    """
    # Der schnelle Pfad zuerst und ohne Zusatz-Aufrufe: das laeuft fuer jedes
    # der rund 19.000 Trigger-Tokens je Claim.
    if trifft(claim_n, term):
        return True
    return (isinstance(term, str) and (term[:1] == " " or term[-1:] == " ")
            and trifft(claim_w, term))
