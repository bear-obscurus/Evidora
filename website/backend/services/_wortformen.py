"""Attestierte Wortformen: dieselbe Sache, anderes Wort.

Anlass (27.9.2026, Live-Messung zu PR #227). Der Claim

    "Österreich 2024: mehr männliche als weibliche Mordopfer"

bekam ``unverifiable @ 0.1`` mit der Begruendung, es gebe "keine
geschlechtsspezifische Aufschluesselung" — waehrend drei andere
Formulierungen derselben Frage den Femizid-Fakt treffen. Ursache: Die
Trigger-Gruppen kennen die Personen-Substantive ("frauen", "maenner"),
nicht die Adjektive ("weibliche", "maennliche"). Komposita sind NICHT das
Problem, das wurde gemessen — "mord" trifft weiterhin in "mordopfer".

Das ist keine Flexion, sondern ein anderes Lexem: "weiblich" kommt von
"Weib", "Frau" ist etymologisch unverwandt. Kein Stemmer verbindet die
beiden, und deshalb greift die Regel "Flexionsformen aufzaehlen scheitert
— Wortstamm pruefen" hier nicht. Aufzaehlen ist genau richtig, nur nicht
in 664 Fakten einzeln.

## Warum nur Geschlecht, obwohl der Sweep mehr fand

Der Sweep ueber alle 664 Trigger-Fakten leitete Paare aus den Daten ab:
Ein Paar gilt als attestiert, wenn irgendein Fakt beide Woerter in
DERSELBEN Trigger-Gruppe fuehrt. So fanden sich 29 Paare mit gemeinsamem
Stamm, davon 18 ohne Teilzeichenketten-Beziehung, und mechanische Sonden
wiesen 33 davon als "bezahlt" aus (die Sonde verliert ihren Fakt).

Eine erste Fassung setzte alle 14 Klassen symmetrisch um. Der
Ueber-Trigger-Sweep wies sie ab: 332 -> 350 Fremdtreffer-Paare, und die 18
neuen waren zum Grossteil **Stichwort-Degeneration** — ein Claim-Wort
erfuellte plotzlich ZWEI AND-Gruppen desselben Fakts:

  * ``women_bulky_muscles_2026``: Gruppe c/0 fuehrt frau/frauen, c/1 fuehrt
    maennlich (im Sinne "maennlich wirkende Muskeln"). Mit maenner in c/1
    genuegte "Frauen verdienen weniger als Männer", und ein Fitness-Mythos
    beantwortete Gender-Pay- und Femizid-Claims.
  * ``psychotherapie_wirkt_nicht_mythos``: Mit der Klasse
    wirksam/wirkt/wirkung zog der Fakt "ECT ist bei therapieresistenter
    Depression wirksam" und "Semaglutid ist wirksamste pharmakologische
    Adipositas-Therapie" an — Pharmakologie in einem Psychotherapie-Fakt.

Die 33 "bezahlten" Luecken stammten aus mechanisch ersetzten Sonden ("die
mrna impfung ist gefahr"), also aus schwachem Beleg. Der gemessene Schaden
war dagegen konkret. Geblieben ist deshalb nur die Klasse mit echter
Messung dahinter — [[lessons_learned]]: vor der Radikalloesung zaehlen.

## Drei bewusste Verengungen

1. NUR Geschlecht. Die anderen 13 Klassen sind im Sweep-Protokoll
   dokumentiert und bewusst nicht umgesetzt.
2. NUR die Richtung NOMEN -> ADJEKTIV. Umgekehrt ("maennlich" in einer
   Gruppe erlaubt auch "maenner") war die Ursache des
   women_bulky-Fehltreffers.
3. NUR Composite- und trigger_all-Gruppen, NICHT ``trigger_keywords``.
   Keywords sind der bedingungslose Eingang eines Fakts; sie zu weiten
   traegt ein anderes Risiko als eine Gruppe, die ohnehin nur ein
   AND-Glied von mehreren ist.

Das Paar ist in den Daten attestiert: ``gender_pay_gap_2026`` fuehrt in
Gruppe c/1 frauen, maenner, maennlich UND weiblich; ``women_bulky_muscles_2026``
in c/0 frau, frauen, weibliche.

Ergaenzt wird nur der Wortstamm ohne Endung ("weiblich"), weil der
Substring-Matcher damit auch "weibliche", "weiblicher" und "weiblichen"
findet.

Rein string-basiert, kein Modell, deterministisch.
"""

from functools import lru_cache

from services._schreibweise import normalisiere

__all__ = ["mit_wortformen", "REGELN"]

# (Ausloeser-Nomen, ergaenzte Adjektiv-Staemme). Richtung ist Absicht.
REGELN: tuple[tuple[frozenset[str], tuple[str, ...]], ...] = (
    (frozenset({"frau", "frauen"}), ("weiblich",)),
    (frozenset({"mann", "maenner"}), ("maennlich",)),
)


@lru_cache(maxsize=8192)
def _erweitert(gruppe: tuple[str, ...]) -> tuple[str, ...]:
    woerter = {normalisiere(t) for t in gruppe if isinstance(t, str)}
    zusatz: list[str] = []
    for ausloeser, formen in REGELN:
        if not (woerter & ausloeser):
            continue
        for form in formen:
            # Steht die Form schon da — auch als laengere Schreibung
            # ("weibliche") —, ist nichts zu tun.
            if any(w.startswith(form) for w in woerter) or form in zusatz:
                continue
            zusatz.append(form)
    if not zusatz:
        return gruppe
    return gruppe + tuple(zusatz)


def mit_wortformen(gruppe):
    """Die Alternations-Gruppe plus die attestierten Formen ihrer Mitglieder.

    Ohne Treffer in einer Regel wird die Gruppe unveraendert
    zurueckgegeben — kein Objekt-Neubau fuer die 99 %, die nichts angeht.

    >>> mit_wortformen(("frauen", "gender"))
    ('frauen', 'gender', 'weiblich')
    >>> mit_wortformen(("frauen", "maenner"))
    ('frauen', 'maenner', 'weiblich', 'maennlich')
    >>> mit_wortformen(("frau", "weibliche"))      # schon abgedeckt
    ('frau', 'weibliche')
    >>> mit_wortformen(("maennlich", "muskel"))    # Richtung: nur Nomen -> Adjektiv
    ('maennlich', 'muskel')
    >>> mit_wortformen(("leerstand", "abgabe"))
    ('leerstand', 'abgabe')
    """
    try:
        schluessel = tuple(gruppe)
    except TypeError:
        return gruppe
    return _erweitert(schluessel)
