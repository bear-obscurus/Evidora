"""Schreibweisen-Normalisierung fuer die Trigger-Erkennung.

Gemessen am 2026-09-06 ueber 85 Services und 2.656 dokumentierte
``claim_phrasings_handled``: **371 (14 %) trafen nicht mehr**, sobald man sie
in einer anderen, voellig ueblichen Schreibweise eingibt.

    Bindestrich -> Leerzeichen    75 % Trefferquote   198 Ausfaelle
    Umlaut -> ae/oe/ue            88 %                127 Ausfaelle
    Leerzeichen -> Bindestrich    92 %                 66 Ausfaelle

Echte Beispiele aus der Messung: „Oesterreich", „Linkshaender", „kuerzere
Lebenserwartung", „Massen Ueberwachung", „Soros NGOs". Fuer einen
oesterreichischen Dienst ist die ASCII-Umschrift keine Randerscheinung —
ohne Umlaut-Tastatur oder aus Suchmaschinen-Gewohnheit tippen Leute das
staendig.

Bisher wurde das von Hand aufgezaehlt, wo jemand daran dachte:

    "fpö", "fpoe", "spö", "spoe", "övp", "oevp", ...
    "geldwäsche", "geldwaesche", ...

32 solcher Doppel-Eintraege allein in ``_topic_match.py``, und nur 2 von 140
Services normalisierten ueberhaupt. Das ist dasselbe Muster wie die
aufgezaehlten Flexionsformen bei Frontex (PR #141): Wer Varianten aufzaehlt,
vergisst welche — und es ist erfahrungsgemaess die haeufigste.

BEWUSST NICHT normalisiert: Trennzeichen ganz zu entfernen
(„sglt-2-hemmer" -> „sglt2hemmer") wuerde zwar noch mehr Schreibweisen
zusammenfuehren, erzeugt aber Treffer ueber Wortgrenzen hinweg: der Trigger
„at pflege" wuerde als „atpflege" in „…haT PFLEGEbedarf…" stecken. Der
Gewinn rechtfertigt das Risiko nicht.
"""

from __future__ import annotations

import re
from functools import lru_cache

# ß -> ss ist bewusst dabei: „Straße"/„Strasse" ist in AT/CH-Texten
# alltaeglich. Kollisionen wie „Buße"/„Busse" sind dadurch moeglich und
# werden vom Ueber-Trigger-Sweep abgesichert, nicht wegdefiniert.
_UMLAUTE = str.maketrans({
    "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
    "Ä": "ae", "Ö": "oe", "Ü": "ue",
    "á": "a", "à": "a", "â": "a", "é": "e", "è": "e", "ê": "e",
    "í": "i", "ì": "i", "ó": "o", "ò": "o", "ô": "o", "ú": "u", "ù": "u",
    "ç": "c", "ñ": "n",
})

# Trennzeichen, die Nutzer beliebig gegeneinander tauschen.
_TRENNER = re.compile(r"[-‐‑‒–—―_/·•]+")
_MEHRFACH_LEER = re.compile(r"\s+")


# Der Cache lohnt, weil die Trigger-Tokens statisch sind und bei JEDEM Claim
# erneut normalisiert wuerden: ohne ihn kostet der groesste Pack 0,205 ms
# statt 0,027 ms — hochgerechnet auf 85 Static-Services rund 17 ms je Claim.
# Absolut harmlos, aber unnoetig: die Menge der Tokens ist klein und fest.
@lru_cache(maxsize=16384)
def normalisiere(text: str) -> str:
    """Bringt Claim und Trigger auf dieselbe Schreibweise.

    Kleinschreibung, Umlaut-Faltung, alle Trennzeichen zu einem Leerzeichen,
    Mehrfach-Leerzeichen zusammengefasst. Idempotent.

    >>> normalisiere("Brustkrebs-Früherkennung Österreich")
    'brustkrebs frueherkennung oesterreich'
    >>> normalisiere("Brustkrebs Frueherkennung Oesterreich")
    'brustkrebs frueherkennung oesterreich'
    >>> normalisiere(" ETER ")          # Rand-Leerzeichen bleiben erhalten
    ' eter '
    """
    if not text:
        return ""
    t = text.lower().translate(_UMLAUTE)
    t = _TRENNER.sub(" ", t)
    # NICHT strippen: Rand-Leerzeichen sind in Triggern Absicht. `" eter "`
    # schuetzt das Kuerzel ETER vor Treffern in "KilomETER"; ein strip()
    # macht daraus `"eter"` und genau dieser Fehler ist beim Bauen
    # passiert — der Ueber-Trigger-Sweep hat ihn gefangen.
    return _MEHRFACH_LEER.sub(" ", t)


def enthaelt(claim: str, begriff: str) -> bool:
    """Steckt ``begriff`` im ``claim`` — schreibweisen-unabhaengig?

    Ersetzt das Idiom ``begriff.lower() in claim_lc`` in der Trigger-Logik.
    """
    if not begriff:
        return False
    return normalisiere(begriff) in normalisiere(claim)
