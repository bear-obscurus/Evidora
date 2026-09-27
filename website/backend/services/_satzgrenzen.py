"""Satzgrenzen, die deutsche Abkuerzungen und Gliederungszahlen aushalten.

Anlass (2026-09-24, Prompt-Zensus): Der Synthesizer kuerzt jedes Prompt-Feld
auf 400 Zeichen und waehlt dafuer die claim-relevantesten Saetze
(``_claim_centered_truncate``). Getrennt wurde bislang mit

    re.split(r"(?<=[.!?])\\s+|\\n+", s)

Fuer diesen Ausdruck endet ein Satz hinter jedem Punkt mit Leerzeichen — also
auch hinter "LGBl.", "Nr.", "Abs.", "Mio." und hinter jeder Ordnungszahl
("seit 1. Jaenner 2023"). Gemessen am Leerstandsabgaben-Fakt: der Claim
"Wie hoch ist die Leerstandsabgabe in Tirol?" bekam drei Fragmente der Form

    "Tirol bis 2025: Tirol, TFLAG (LGBl. […]"

in den Prompt — 20 Zeichen Satzanfang, keine einzige Zahl, und das dreimal.
Das Budget war weg, die Antwort "unverifiable".

Zusaetzlich trennen wir am Feld-Separator " | ", den
``render_data_with_marker`` zwischen die data-Felder setzt. Damit ist die
kleinste Einheit im Zweifel ein ganzes Datenfeld statt eines Satzfragments.

Rein string-basiert, kein Modell, deterministisch (CI-tauglich).
"""

import re

__all__ = ["teile_in_einheiten", "ABKUERZUNGEN"]

# Abkuerzungen, hinter denen KEIN Satz endet. Kleinschreibung, ohne Punkt.
# Die Einzelbuchstaben decken "z. B.", "d. h.", "u. a.", "i. d. R." ab.
ABKUERZUNGEN = frozenset({
    # Rechtsquellen und Fundstellen
    "lgbl", "bgbl", "abl", "nr", "abs", "lit", "art", "ziff", "zif", "rz",
    "ff", "hrsg", "bd", "aufl", "kap", "vgl", "idgf", "idf", "iv", "vo",
    # Groessen und Mengen
    "mio", "mrd", "bzw", "ca", "inkl", "exkl", "zzgl", "max", "min", "etc",
    "evtl", "ggf", "bspw", "usw", "sog", "insb", "jhd", "jh", "tsd", "vh",
    # Titel und Verweise
    "dr", "prof", "dipl", "ing", "mag", "univ", "st", "tab", "abb", "s",
    "seite", "nr", "pkt", "zb", "dh", "ua", "idr",
    # Einzelbuchstaben aus mehrteiligen Abkuerzungen
    "z", "b", "d", "h", "u", "a", "i", "e", "v", "o", "m", "g", "t",
})

# Kandidaten: Satzzeichen vor Leerraum, oder ein Zeilenumbruch, oder der
# Feld-Separator " | " aus render_data_with_marker.
_KANDIDAT = re.compile(r"[.!?]+(?=\s)|\n+|\s\|\s")


def _token_vor(text: str, i: int) -> str:
    """Das Wort unmittelbar vor Position ``i`` (ohne Punkt)."""
    j = i - 1
    while j >= 0 and (text[j].isalnum() or text[j] in "§"):
        j -= 1
    return text[j + 1:i]


def _ist_satzende(text: str, start: int, ende: int) -> bool:
    """Ist der Treffer von ``start`` bis ``ende`` eine echte Grenze?"""
    stueck = text[start:ende]
    if "|" in stueck or "\n" in stueck:
        return True                      # Feldgrenze bzw. Zeilenumbruch
    if stueck.strip(".") != "":
        return True                      # "!" oder "?" — praktisch immer Ende
    token = _token_vor(text, start)
    if not token:
        return True
    if token.isdigit() and len(token) <= 2:
        return False                     # "seit 1. Jaenner", "2. Abschnitt"
    if token.lower() in ABKUERZUNGEN:
        return False                     # "LGBl. Nr.", "Abs. 3", "Mio. Euro"
    if len(token) == 1 and token.isupper():
        return False                     # Initiale: "M. Segu"
    k = ende
    while k < len(text) and text[k].isspace():
        k += 1
    if k < len(text) and (text[k].islower() or text[k].isdigit()):
        return False                     # ein neuer Satz faengt nicht klein an
    return True


def teile_in_einheiten(text: str) -> list[str]:
    """Zerlege ``text`` in Saetze bzw. Datenfelder.

    Leere Teile fallen weg; Satzzeichen bleiben am Satz.
    """
    if not text:
        return []
    teile: list[str] = []
    letzte = 0
    for m in _KANDIDAT.finditer(text):
        if not _ist_satzende(text, m.start(), m.end()):
            continue
        stueck = text[letzte:m.end()].strip()
        # Der Feld-Separator selbst gehoert in keinen Satz.
        stueck = stueck.rstrip("|").strip()
        if stueck:
            teile.append(stueck)
        letzte = m.end()
    rest = text[letzte:].strip().lstrip("|").strip()
    if rest:
        teile.append(rest)
    return teile
