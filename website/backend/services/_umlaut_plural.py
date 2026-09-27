"""Der Umlaut-Plural: „Aufsichtsräte" zu „aufsichtsrat".

Anlass (28.9.2026, aus dem #228-Sweep). Der Claim

    "Männliche Führungskräfte dominieren die Aufsichtsräte"

erreichte ``frauenquote_wirksamkeit_2026`` nicht, obwohl der Fakt das Token
``aufsichtsrat`` fuehrt: Normalisiert heisst der Plural "aufsichtsraete",
und darin steckt "aufsichtsrat" nicht. Derselbe Bruch trifft jeden
Umlaut-Plural — Löhne, Städte, Bundesländer, Häuser, Bücher, Ärzte, Wälder.

## Warum nicht das Token umlauten

Der erste Versuch ging von der Token-Seite: den letzten Stammvokal
umlauten und gegen einen Korpus aus 3.779 echten Claims pruefen. Ergebnis
225 Paare — aber als Obermenge aus drei Sorten:

    bundesland -> bundeslaender     echter Plural
    gefahr     -> gefaehrlich       Ableitung, anderes Wort
    wahr       -> waehrend          Zufall ("während" hat nichts mit wahr zu tun)

Und der GEMESSENE Fall fehlte, weil "Aufsichtsräte" im Korpus nicht
vorkommt. Ein Instrument, das den Anlass nicht findet, taugt nicht.

## Was stattdessen

Nicht das Token umlauten, sondern das Claim-Wort ENTUMLAUTEN. Das braucht
kein Lexikon und deckt die ganze Klasse mit einer Transformation:

    "aufsichtsraete" -> "aufsichtsrate" == "aufsichtsrat" + "e"

Damit die Zufallstreffer draussen bleiben, muss der Rest hinter dem Token
eine echte PLURAL-ENDUNG sein und das Wort auf ``token + endung`` ENDEN:

    aufsichtsrate  == aufsichtsrat + "e"     ja
    bundeslander   == bundesland   + "er"    ja
    fuhrungskrafte endet auf kraft + "e"     ja  (Kompositum)
    wahrend        == wahr + "end"           nein, "end" ist keine Endung
    wahrung        == wahr + "ung"           nein
    osterreich     == oster + "reich"        nein
    betragt        == betrag + "t"           nein ("beträgt" ist ein Verb)
    betrage        == betrag + "e"           ja  (Beträge, echter Plural)
    schuler        == schule + "r"           nein ("Schüler" ist kein Plural
                                             von "Schule")

Verlangt wird das Wortende, nicht ein Praefix: sonst waere "wahrend" wieder
drin, weil es mit "wahre" beginnt.

Rein string-basiert, kein Modell, deterministisch.
"""

import re
from functools import lru_cache

__all__ = ["entumlaute", "plural_woerter", "plural_trifft", "ENDUNGEN"]

# Plural- und Dativ-Plural-Endungen. Das leere Wort deckt die Faelle ohne
# Endung ab ("Väter" zu "Vater").
ENDUNGEN: tuple[str, ...] = ("", "e", "er", "en", "ern")

# Normalisierter Text traegt die Umlaute schon gefaltet (ä -> ae). "aeu"
# zuerst, sonst wird aus "haeuser" ein "haeser".
_FALTUNG = (("aeu", "au"), ("ae", "a"), ("oe", "o"), ("ue", "u"))

_WORT_RE = re.compile(r"[a-z]+")

# Kuerzer als das ergibt zu viel Rauschen: "muss" fiele sonst auf "müssen".
MINDESTLAENGE = 5


def entumlaute(text: str) -> str:
    """Faltet die schon normalisierten Umlaute auf ihren Grundvokal zurueck.

    >>> entumlaute("aufsichtsraete")
    'aufsichtsrate'
    >>> entumlaute("haeuser")
    'hauser'
    """
    for umlaut, grund in _FALTUNG:
        text = text.replace(umlaut, grund)
    return text


@lru_cache(maxsize=4096)
def plural_woerter(claim_n: str) -> tuple[str, ...]:
    """Die entumlauteten Einzelwoerter des Claims — einmal pro Claim.

    Nur Woerter, die ueberhaupt einen Umlaut trugen, kommen mit: fuer alle
    anderen hat der exakte Pass schon entschieden.

    Gecacht, weil ``substring_or_composite_match`` pro Claim fuer jeden der
    ueber 600 Fakten einmal aufgerufen wird.

    >>> plural_woerter("wie viele aufsichtsraete gibt es in wien")
    ('aufsichtsrate',)
    """
    raus = []
    for wort in _WORT_RE.findall(claim_n):
        gefaltet = entumlaute(wort)
        if gefaltet != wort and gefaltet not in raus:
            raus.append(gefaltet)
    return tuple(raus)


def plural_trifft(woerter: tuple[str, ...], tok: str) -> bool:
    """Endet ein entumlautetes Claim-Wort auf ``tok`` plus Plural-Endung?

    >>> plural_trifft(("aufsichtsrate",), "aufsichtsrat")
    True
    >>> plural_trifft(("fuhrungskrafte",), "kraft")   # Kompositum, Wortende
    True
    >>> plural_trifft(("wahrend", "wahrung"), "wahr")
    False
    >>> plural_trifft(("betragt",), "betrag")
    False
    """
    if len(tok) < MINDESTLAENGE or " " in tok:
        return False
    for wort in woerter:
        for endung in ENDUNGEN:
            if endung and not wort.endswith(endung):
                continue
            if wort.endswith(tok + endung) and len(wort) > len(endung):
                return True
    return False
