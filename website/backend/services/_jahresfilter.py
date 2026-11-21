"""Jahres-Nachfilter fuer die Analyse-Stufe: reine Logik, kein Modell, kein Netz.

Ohne Datum schrieb das Analyse-Modell sein Trainings-"Jetzt" (2023/2024) in
die Suchbegriffe: 20 von 98 Analysen nach Regel E (Messung 5.11.2026). Mit
Datum in der Nutzernachricht (#316) haengte es stattdessen "2026" an
Gegenwarts-Claims (27 bis 30 von 98, 21.11.2026); eine Regelzeile im Prompt
half nicht. Darum raeumt der Code nach. Eine Jahreszahl in
``factcheck_queries``, ``pubmed_queries`` oder ``entities`` bleibt nur, wenn
sie im Claim steht oder aus ihm folgt (Regel E, Evidora Dokumente/
analyse_datum_2026-11-20/Plan.md):

- E.1  woertlich im Claim (auch ``2026/27``; ``2.000`` zaehlt als 2000)
- E.2  aus einem relativen Ausdruck: heute/heuer/aktuell -> J, letztes Jahr
       -> J-1, vor N Jahren -> J-N, in den letzten N Jahren -> J-N .. J;
       die laufende Saison (Winter, Saison, Grippe + J) -> J-1 .. J+1
- E.3  begrenzt durch ein Jahr im Claim (seit X, vor X, bis X, zwischen X
       und Z, X-er) oder durch ein benanntes Ereignis (seit Corona -> 2020 .. J)
- E.4  fester Name (Pariser Abkommen -> 2015)

Was weder im Claim steht noch folgt, faellt weg, samt Datum, Monat und
Bindewort davor ("im Jahr 2026"). Von einem Bereich, der nur teilweise folgt
("2020-2026" bei "seit 2023"), bleibt das erlaubte Jahr. Eine Zahl mit Einheit
("2000 Euro") ist keine Jahreszahl und bleibt. ``claim`` und die Schalter fasst
der Filter nicht an. Er schreibt nichts ins Protokoll.
"""

from __future__ import annotations

import datetime
import re

FELDER = ("factcheck_queries", "pubmed_queries", "entities")
UNTEN, OBEN = 1900, 2099

_JAHR = r"(?:19|20)\d\d"


def _falte(text: str) -> str:
    """Kleinschreibung, Umlaute als ae/oe/ue, ss: eine Form fuer beide Schreibweisen."""
    s = (text or "").casefold()
    for a, b in (("ä", "ae"), ("ö", "oe"), ("ü", "ue")):
        s = s.replace(a, b)
    return s


def _ohne_tausenderpunkt(text: str) -> str:
    """``2.000`` -> ``2000``, damit ein Betrag im Claim als woertlich gilt."""
    return re.sub(r"(?<![\d.])(\d{1,3})((?:[.  '’]\d{3})+)(?!\d|[.]\d)",
                  lambda m: m.group(1) + re.sub(r"\D", "", m.group(2)), text)


# --- E.2: relative Ausdruecke (auf gefaltetem Text) ---------------------------

_ZAHLWORT = {
    "ein": 1, "einem": 1, "einen": 1, "eins": 1, "zwei": 2, "drei": 3, "vier": 4,
    "fuenf": 5, "sechs": 6, "sieben": 7, "acht": 8, "neun": 9, "zehn": 10,
    "elf": 11, "zwoelf": 12, "fuenfzehn": 15, "zwanzig": 20,
    "a": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "twelve": 12, "fifteen": 15,
    "twenty": 20,
}
_N = r"(\d{1,2}|" + "|".join(sorted(_ZAHLWORT, key=len, reverse=True)) + r")"

_JETZT = re.compile(
    r"\b(?:heute|heutig\w*|heutzutage|heuer|aktuell\w*|derzeit\w*|zurzeit|zur zeit"
    r"|momentan\w*|jetzt\w*|gerade|inzwischen|mittlerweile|gegenwaertig\w*"
    r"|diesjaehrig\w*|gestern|vorgestern|morgen|uebermorgen"
    r"|dies(?:es|em|en|er|e)?\s+(?:jahr(?:es)?|woche|monat(?:s)?|saison"
    r"|winter\w*|sommer\w*|herbst\w*|fruehling\w*|fruehjahr\w*)"
    r"|laufend(?:es|en|em)\s+jahr(?:es)?"
    r"|today|yesterday|tomorrow|currently|nowadays|now|at present"
    r"|this\s+(?:year|week|month|season|winter|summer|autumn|fall|spring))\b")
_LETZTES = re.compile(
    r"\b(?:(?:letzt|vorig|vergangen)(?:es|en|em|er)\s+jahr(?:es)?|vorjahr\w*"
    r"|vorjaehrig\w*|letztjaehrig\w*|last\s+year|previous\s+year)\b")
_NAECHSTES = re.compile(
    r"\b(?:(?:naechst|kommend)(?:es|en|em|er)\s+jahr(?:es)?|next\s+year)\b")
_VOR_N = re.compile(rf"\b(?:vor\s+{_N}\s+jahr(?:en)?|{_N}\s+years?\s+ago)\b")
_LETZTE_N = re.compile(
    rf"\b(?:(?:letzt|vergangen|zurueckliegend)en\s+{_N}\s+jahr(?:en)?"
    rf"|seit\s+{_N}\s+jahr(?:en)?|(?:last|past)\s+{_N}\s+years)\b")
_SAISON = re.compile(r"winter|saison|grippe|influenza|season")


def _zahl(wort: str) -> int:
    return int(wort) if wort.isdigit() else _ZAHLWORT[wort]


def _relativ(f: str, j: int) -> set[int]:
    anker: set[int] = set()
    if _JETZT.search(f):
        anker.add(j)
    if _LETZTES.search(f):
        anker.add(j - 1)
    if _NAECHSTES.search(f):
        anker.add(j + 1)
    for m in _VOR_N.finditer(f):
        anker.add(j - _zahl(m.group(1) or m.group(2)))
    for m in _LETZTE_N.finditer(f):
        anker.update(range(j - _zahl(next(g for g in m.groups() if g)), j + 1))
    # Die laufende Saison umfasst zwei Kalenderjahre ("diesen Winter",
    # "Grippewelle dieses Jahr" -> J-1 .. J+1). Nur um J, nicht um "letztes
    # Jahr": So liest Regel E.2 und so stehen #24/#79 in der Handeinordnung.
    if j in anker and _SAISON.search(f):
        anker |= {j - 1, j + 1}
    return anker


# --- E.3 und E.4: Begrenzung durch ein Jahr, benannte Ereignisse ---------------

_BEREICH_IM_CLAIM = re.compile(
    rf"\b(?:zwischen\s+({_JAHR})\s+und\s+({_JAHR})|between\s+({_JAHR})\s+and\s+({_JAHR})"
    rf"|(?:von\s+|from\s+)?({_JAHR})\s*(?:bis|to|–|—|-)\s*({_JAHR}))\b")
_AB = re.compile(
    rf"\b(?:seit|ab|since|from)\s+(?:(?:dem\s+)?jahre?\s+|anfang\s+|mitte\s+|ende\s+)?({_JAHR})\b")
_VOR = re.compile(rf"\b(?:vor|before)\s+(?:dem\s+jahre?\s+)?({_JAHR})\b")
_BIS = re.compile(rf"\b(?:bis|until|till|by)\s+(?:(?:zum\s+)?jahre?\s+|ende\s+)?({_JAHR})\b")
_JAHRZEHNT = re.compile(r"\b((?:19|20)\d0)(?:-?ern?|s|'s)\b")
_JAHRZEHNT_KURZ = re.compile(r"\b(00|10|20|30|40|50|60|70|80|90)(?:-?ern?|s)\b")

# (Muster, Jahre bei blosser Nennung, Beginn fuer "seit ..."): genau die
# Beispiele aus Regel E.3/E.4, mehr nicht.
_EREIGNISSE = (
    (re.compile(r"\b(?:corona\w*|covid\w*|sars-cov-2|pandemie\w*)"), (2019, 2020, 2023), 2020),
    (re.compile(r"\b(?:ukraine-?krieg\w*|krieg\w*\s+(?:in|gegen)\s+(?:der\s+|die\s+)?ukraine"
                r"|angriffskrieg\w*|invasion\s+(?:der\s+|in\s+die\s+)?ukraine|war\s+in\s+ukraine)"),
     (2022,), 2022),
    (re.compile(r"\bbrexit\w*"), (2016, 2020), 2016),
    (re.compile(r"\b(?:euro-?einfuehrung\w*|einfuehrung\s+des\s+euro\w*)"), (1999, 2002), 1999),
    (re.compile(r"\b(?:pariser\s+(?:klima-?)?(?:abkommen\w*|uebereinkommen\w*)"
                r"|pariser\s+klimaziel\w*|paris\s+(?:climate\s+)?agreement)"), (2015,), 2015),
)
_SEIT_DAVOR = re.compile(r"\b(?:seit|since)\b[^.;!?]{0,30}$")


def _begrenzt(f: str, j: int) -> set[int]:
    jahre: set[int] = set()
    rest = f
    for m in _BEREICH_IM_CLAIM.finditer(f):
        a, b = sorted(int(x) for x in m.groups() if x)
        jahre.update(range(a, b + 1))
        rest = rest.replace(m.group(0), " ")
    for m in _AB.finditer(rest):
        x = int(m.group(1))
        jahre.update(range(x, max(x, j) + 1))
    for m in _VOR.finditer(rest):
        jahre.update(range(UNTEN, int(m.group(1))))
    for m in _BIS.finditer(rest):
        jahre.update(range(UNTEN, int(m.group(1)) + 1))
    for m in _JAHRZEHNT.finditer(f):
        x = int(m.group(1))
        jahre.update(range(x, x + 10))
    for m in _JAHRZEHNT_KURZ.finditer(f):
        k = int(m.group(1))
        for jh in ((2000,) if k <= 10 else (1900, 2000) if k == 20 else (1900,)):
            jahre.update(range(jh + k, jh + k + 10))
    for muster, genannt, beginn in _EREIGNISSE:
        for m in muster.finditer(f):
            jahre.update(genannt)
            if _SEIT_DAVOR.search(f[:m.start()]):
                jahre.update(range(beginn, j + 1))
    return jahre


def erlaubte_jahre(claim_text: str, heute: datetime.date) -> set[int]:
    """Alle Jahre, die im Claim stehen oder aus ihm folgen (Regel E.1 bis E.4)."""
    text = _ohne_tausenderpunkt(claim_text or "")
    jahre = {int(x) for x in re.findall(rf"(?<!\d)({_JAHR})(?!\d)", text)}
    for a, b in re.findall(rf"(?<!\d)({_JAHR})\s*[/–—-]\s*(\d\d)(?!\d)", text):
        if int(b) == (int(a) + 1) % 100:
            jahre.add(int(a) + 1)
    f = _falte(text)
    jahre |= _relativ(f, heute.year)
    jahre |= _begrenzt(f, heute.year)
    return {x for x in jahre if UNTEN <= x <= OBEN}


# --- der Filter ------------------------------------------------------------------

_MONAT = (r"(?:j[äa]e?nner|januar|feber|februar|m[äa]e?rz|april|mai|juni|juli|august"
          r"|september|oktober|november|dezember|january|february|march|may|june|july"
          r"|october|december|jan|feb|m[äa]r|apr|jun|jul|aug|sept?|okt|oct|nov|dez|dec)\.?")
_AUSDRUCK = re.compile(
    rf"""(?<![\d.,])
    (?:
        (?P<iso>(?P<iso_j>{_JAHR})-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01]))
      | (?P<vorsatz>(?:\d{{1,2}}\.\s?)?(?:\d{{1,2}}\.\s?|{_MONAT}\s+))?
        (?P<j1>{_JAHR})
        (?: \s?[/–—-]\s?(?P<j2>{_JAHR}|\d\d)(?!\d) | \s+(?:bis|to)\s+(?P<j3>{_JAHR}) )?
        (?:ern?|s|'s)?
    )
    (?![\d\w])""",
    re.VERBOSE | re.IGNORECASE)
_EINHEIT_DANACH = re.compile(
    r"\s?(?:[€$£%]|m²|(?:euro|eur|cent|dollar|usd|chf|pfund|prozent|percent|km|kilometer|meter"
    r"|m|kg|tonnen|t|mio|millionen|mrd|milliarden|stunden|minuten|tage|menschen|personen"
    r"|einwohner|leute|people|persons|f[äa]e?lle|cases|tote|deaths|opfer|st[üu]e?ck|mal|ha"
    r"|hektar|mw|gw|twh|kwh)\b)", re.IGNORECASE)
# "€ 1950" ist ein Betrag. In "220 Euro 2024" gehoert "Euro" zur 220: Steht
# vor der Waehrung eine Zahl, ist die folgende Zahl wieder ein Jahr.
_WAEHRUNG_DAVOR = re.compile(r"(\d[\d.,]*\s?)?(?:[€$£]|\b(?:eur|euro|usd|chf)\b)\s?$", re.IGNORECASE)
_BINDEWORT_DAVOR = re.compile(
    r"(?:(?<!\w)(?:im|in|seit|ab|bis|vor|nach|von|vom|zum|zur|f[üu]e?r|als|und|oder|vs\.?"
    r"|versus|gegen|gegen[üu]e?ber|zwischen|jahr|jahre|jahres|jahren|dem|den|des|der|das"
    r"|year|years|the|since|from|to|until|between|and|or|of|for)\s*)+$",
    re.IGNORECASE)
# "2024 vs 2025" ohne 2024 soll "2025" ergeben, nicht "vs 2025".
_BINDEWORT_DANACH = re.compile(
    r"\s*(?:vs\.?|versus|und|oder|gegen[üu]e?ber|bis|and|or|to)(?=\s|$)", re.IGNORECASE)


def _jahre_im_ausdruck(m: re.Match) -> tuple[list[int], int]:
    """Jahre eines Treffers und sein Ende. ``2026-12`` ist kein Bereich: Zwei
    Ziffern gelten nur als Jahr, wenn sie das Folgejahr sind (``2025/26``)."""
    if m.group("iso"):
        return [int(m.group("iso_j"))], m.end()
    j1 = int(m.group("j1"))
    j2, j3 = m.group("j2"), m.group("j3")
    if j3:
        return [j1, int(j3)], m.end()
    if j2 and len(j2) == 4:
        return [j1, int(j2)], m.end()
    if j2:
        if int(j2) == (j1 + 1) % 100:
            return [j1, j1 + 1], m.end()
        return [j1], m.end("j1")
    return [j1], m.end()


def _saeubern(s: str) -> str:
    s = re.sub(r"\(\s*\)|\[\s*\]", " ", s)
    s = re.sub(r"(^|\s)[-–—/](?=\w)", r"\1", s)
    s = re.sub(r"\s+([,;:.!?)\]])", r"\1", s)
    s = re.sub(r"([,;:/–—-])(?:\s*[,;:/–—-])+", r"\1", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip(" ,;:/–—-")


def _eintrag_filtern(text: str, erlaubt: set[int], entfernt: list[int]) -> str:
    """Ein Suchbegriff oder eine Entitaet. Haengt die entfernten Jahre an ``entfernt``."""
    stuecke, pos = [], 0
    for m in _AUSDRUCK.finditer(text):
        jahre, ende = _jahre_im_ausdruck(m)
        fremd = [j for j in jahre if j not in erlaubt]
        if not fremd:
            continue
        waehrung = _WAEHRUNG_DAVOR.search(text, 0, m.start())
        if _EINHEIT_DANACH.match(text, ende) or (waehrung and not waehrung.group(1)):
            continue  # ein Betrag oder eine Menge, keine Jahreszahl
        bleibt = [str(j) for j in jahre if j in erlaubt]
        anfang = m.start()
        if not bleibt:
            davor = _BINDEWORT_DAVOR.search(text[pos:anfang])
            if davor:
                anfang = pos + davor.start()
            else:
                danach = _BINDEWORT_DANACH.match(text, ende)
                if danach:
                    ende = danach.end()
        stuecke.append(text[pos:anfang])
        stuecke.append(" ".join(bleibt))
        pos = ende
        entfernt.extend(fremd)
    if not stuecke:
        return text
    stuecke.append(text[pos:])
    return _saeubern("".join(stuecke))


def jahre_nachfiltern(claim_text: str, analysis: dict, heute: datetime.date) -> list[int]:
    """Entfernt fremde Jahreszahlen aus den drei Suchfeldern, in place.

    Gibt die entfernten Jahre zurueck (fuer Tests und das Messwerkzeug). Ein
    Eintrag, der danach leer ist oder einem frueheren gleicht, faellt weg.
    """
    if not isinstance(analysis, dict):
        return []
    erlaubt = erlaubte_jahre(claim_text, heute)
    alle: list[int] = []
    for feld in FELDER:
        werte = analysis.get(feld)
        if isinstance(werte, str):
            analysis[feld] = _eintrag_filtern(werte, erlaubt, alle)
            continue
        if not isinstance(werte, list):
            continue
        neu, gesehen = [], set()
        for w in werte:
            if not isinstance(w, str):
                neu.append(w)
                continue
            entfernt: list[int] = []
            gefiltert = _eintrag_filtern(w, erlaubt, entfernt)
            if entfernt:
                alle.extend(entfernt)
                if not re.search(r"\w", gefiltert) or gefiltert.casefold() in gesehen:
                    continue
            gesehen.add(gefiltert.casefold())
            neu.append(gefiltert)
        analysis[feld] = neu
    return alle
