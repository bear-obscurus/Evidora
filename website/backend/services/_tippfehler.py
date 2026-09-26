"""Tippfehler-toleranter Trigger-Match — die letzte Instanz vor dem Aufgeben.

Gemessen am 26.9.2026 ueber alle 2.603 dokumentierten ``claim_phrasings_
handled`` von Fakten MIT Trigger-Feldern: 97,7 % treffen ihren Fakt. Mit
EINEM Tippfehler bleiben davon

    ein Zeichen weggelassen      56,6 %   (1.102 Treffer verloren)
    zwei Zeichen vertauscht      57,6 %   (1.079 verloren)
    ein Zeichen doppelt          66,3 %   (856 verloren)

Ein Drittel bis zwei Fuenftel der Retrieval-Leistung haengt also an der
Rechtschreibung. Fuer **49 der 63** Trigger-Services ist das ohne Netz:
sie haben den Cosine-Backup seit #41 abgeschaltet (``descriptor_fn=None``),
weil er bei Multi-Topic-Packs themenfremde Claims zog. Live sichtbar in der
QA50F: „Wie hoc ist die Leertsandsabgabe in Tirol?" → ``unverifiable@0.15``
nach 5,2 s, die Pipeline lief nie.

Dieses Modul schliesst die Luecke, ohne den #41-Fehler zu wiederholen: Es
bleibt **literal**. Getestet wird nicht Bedeutungs-Aehnlichkeit, sondern
Schreibweisen-Naehe zu einem Trigger-Token — Damerau-Levenshtein-Abstand 1,
also genau ein weggelassenes, eingefuegtes, ersetztes oder mit dem Nachbarn
vertauschtes Zeichen.

Drei Baender halten das eng:

  * **Mindestlaenge 6** auf beiden Seiten. Bei kurzen Woertern ist Abstand 1
    Alltag, nicht Tippfehler: „wahlen"/„zahlen", „miete"/„mietet", „euro"/
    „euros". Sechs Zeichen ist die Grenze, ab der ein Nachbar im Deutschen
    selten ein anderes echtes Wort ist.
  * **Die ersten 3 Zeichen muessen stimmen.** Tippfehler sitzen fast nie im
    Wortanfang, und die gefaehrlichen Paare unterscheiden sich genau dort
    („Wahlen"/„Zahlen", „Bericht"/„Gericht"). Kostet die Faelle mit Fehler
    im Anlaut — bewusst.
  * **Nur Einwort-Tokens.** Mehrwort-Trigger („freie wahlen") bleiben exakt;
    dort traegt bereits ``services/_flexion.py``.

Und es laeuft nur, wenn der exakte Pass NICHTS gefunden hat: kein
Zusatzaufwand im Normalfall, und ein Fakt, der exakt ankert, gewinnt immer
gegen einen, der nur schreibweisen-nah ist. Der Treffer wird als
``_matched_exact=False`` markiert — ein tippfehler-toleranter Treffer ist
ein schwaches Signal und darf kein „strukturell falsch" behaupten.

ECHTE WOERTER (2026-09-26)
==========================
Abstand 1 misst Schreibweisen-Naehe, nicht Tippfehler. Neben einem Trigger-
Token liegt oft ein anderes echtes Wort: „fuehrt" neben „fuehlt", „aktion"
neben „aktien", „schweiz" neben „schwein". Gefunden bei #207: „Kuenstliche
Intelligenz fuehrt zu Massenarbeitslosigkeit" traf den KI-Bewusstseins-Fakt.

Gemessen ueber die 2.603 Phrasings plus 1.163 Stress-Claims: Der tolerante
Pass lieferte 20 Treffer, keinen im eigenen Pack, und in ALLEN 20 war das
tragende Claim-Wort ein echtes Wort. Zwei Sorten:

    anderes Wort      fuehrt, aktion, schweiz (2x), dieser           5
    Form des Tokens   schadet, vernichtet, entgiftet, normal,       15
                      tertiaer, beziehen, fehlen, kleinen, ...

Die Formen sind so gut oder so schlecht wie ein exakter Treffer auf die
Grundform — „Hund … Schaden" traefe den Haftungs-Fakt genauso. Die anderen
Woerter hat kein Trigger-Autor je gemeint.

Deshalb traegt ein schreibweisen-nahes Wort nicht mehr, wenn es ein
**anderes echtes Wort** ist: Es steht in der Prosa der Trigger-Packs
(``echtwoerter()``) UND ist keine Form desselben Worts — Flexionsendung
(schaden/schadet, ablehnen/ablehnten), Umlaut-Umschrift (schaden/schaeden,
auslander/auslaender), Fugen-s (vermoegen(s)steuer), t/z vor i
(potenzial/potential). Ein Pauschalverbot echter Woerter waere zu breit:
Formen tragen in den Composites mit („erkenne" fuer „erkennen" in der einen
Gruppe, der Tippfehler in der anderen) — es kostete 9 der 2.884
Rueckgewinnungen der #205-Sonde, die Regel keine.

Mit der fertigen Fassung:

    Korpus                     20 -> 15 tolerante Treffer: genau die 5
                               anderen Woerter weg, 0 neu
    #205-Sonde                 2.884 -> 2.884 Rueckgewinnungen
    erschoepfend (jede Position ab Zeichen 4 jedes Worts >= 6, 160.223
    Tippfehler-Claims ohne exakten Treffer):
        weggelassen -3, vertauscht -3, doppelt 0, Nachbartaste -13
        (von 110.759 Rueckgewinnungen)

Die 19 sind Tippfehler, die selbst ein echtes Wort ergeben — „schlaeft" ->
„schlaegt", „Gender" -> „Genfer", „Welpen" -> „Wellen". Das ist der Preis.

BEWUSST NICHT: ie/ei als Form (entschieden/entscheiden, 3 der 19) — die
Klasse ist mehrdeutig, unter den eigenen Tokens steht „partei"/„partie".
Englische Schreibungen (organization, vatican) gehoeren in den Glossar-Pass
(#206). Und das Vokabular kennt nur, was die Prosa benutzt: „Partie" steht
dort einmal, unter der Schwelle — das Paar bleibt offen.
"""

import glob
import json
import os
import re
from collections import Counter
from functools import lru_cache

from services._flexion import trifft_mit_wortgrenze, wortgrenzen_fassung
from services._schreibweise import normalisiere

MIND_LAENGE = 6
PRAEFIX_LAENGE = 3
MIND_HAEUFIGKEIT = 2

_WORT_RE = re.compile(r"[a-zäöüß]+")

_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
_TRIGGER_FELDER = ("trigger_keywords", "trigger_composite", "trigger_all")
# Nutzersprache bzw. Wortfragmente, keine Prosa — siehe echtwoerter().
_KEINE_PROSA = frozenset(_TRIGGER_FELDER + ("claim_phrasings_handled",))

# Endungen, in denen sich zwei Formen DESSELBEN Worts unterscheiden
# (schaden/schadet, normal/normale, tertiaer/tertiaere, ablehnen/ablehnten,
# fehlen/fehlend). Verglichen wird der Rest hinter dem gemeinsamen Anfang.
_ENDUNGEN = frozenset({"", "e", "n", "en", "er", "es", "em", "m", "r", "s",
                       "st", "t", "et", "te", "ten", "d"})


def abstand_hoechstens_eins(a: str, b: str) -> bool:
    """Damerau-Levenshtein-Abstand ≤ 1 (ein Zeichen weg, dazu, ersetzt oder
    mit dem Nachbarn vertauscht). Frueher Ausstieg statt Matrix — die
    Funktion laeuft pro Claim ueber tausende Token-Paare."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la == lb:
        unterschiede = [i for i in range(la) if a[i] != b[i]]
        if len(unterschiede) == 1:
            return True                                  # ein Zeichen ersetzt
        if len(unterschiede) == 2:
            i, j = unterschiede
            return j == i + 1 and a[i] == b[j] and a[j] == b[i]   # vertauscht
        return False
    # Laengen unterscheiden sich um 1: ein Zeichen weggelassen bzw. dazu
    kurz, lang = (a, b) if la < lb else (b, a)
    i = 0
    while i < len(kurz) and kurz[i] == lang[i]:
        i += 1
    return kurz[i:] == lang[i + 1:]


def _kandidaten(claim_n: str) -> list[str]:
    """Woerter des normalisierten Claims, die fuer einen toleranten
    Vergleich lang genug sind."""
    return [w for w in _WORT_RE.findall(claim_n) if len(w) >= MIND_LAENGE]


def _prosa(obj):
    """Alle Strings eines Packs ausser Trigger-Feldern, Phrasings und URLs."""
    if isinstance(obj, str):
        if not obj.startswith("http"):
            yield obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if k not in _KEINE_PROSA:
                yield from _prosa(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _prosa(v)


def _hat_trigger_fakten(daten) -> bool:
    return isinstance(daten, dict) and any(
        isinstance(v, list) and any(
            isinstance(it, dict) and any(it.get(f) for f in _TRIGGER_FELDER)
            for it in v)
        for v in daten.values())


@lru_cache(maxsize=1)
def echtwoerter() -> frozenset[str]:
    """Woerter, die in der kuratierten Prosa der Trigger-Packs mindestens
    ``MIND_HAEUFIGKEIT``-mal vorkommen — normalisiert, ab ``MIND_LAENGE``.

    Quelle sind nur Dateien MIT Trigger-Fakten: Rohdaten-Archive wie
    ``euvsdisinfo_db.json`` (zwei Drittel aller Woerter in data/, englisch,
    abgeschnitten: „manipulati", „ukrainie") sind keine Referenz dafuer, was
    ein echtes Wort ist. Trigger und Phrasings zaehlen nicht: das eine sind
    Wortstaemme („allergi"), das andere Nutzersprache. Die Schwelle 2 haelt
    Einzelstuecke draussen — ein Tippfehler in der Prosa („Gepant") soll
    nicht zum echten Wort werden.

    Gebaut beim ersten Bedarf, einmal je Prozess: im Normalfall nie, denn es
    braucht einen schreibweisen-nahen Treffer, der keine Formvariante ist.
    """
    zaehler = Counter()
    for pfad in sorted(glob.glob(os.path.join(_DATA_DIR, "*.json"))):
        try:
            with open(pfad, encoding="utf-8") as fh:
                roh = fh.read()
            if '"trigger_' not in roh:
                continue                      # Archive gar nicht erst parsen
            daten = json.loads(roh)
        except (OSError, ValueError):
            continue
        if not _hat_trigger_fakten(daten):
            continue
        for s in _prosa(daten):
            # __wrapped__: die Prosa ist Einmal-Text und soll den Cache der
            # Trigger-Tokens nicht verdraengen.
            zaehler.update(w for w in _WORT_RE.findall(normalisiere.__wrapped__(s))
                           if len(w) >= MIND_LAENGE)
    return frozenset(w for w, n in zaehler.items() if n >= MIND_HAEUFIGKEIT)


def _gleiche_endung(a: str, b: str) -> bool:
    """Unterscheiden sich a und b nur in einer Flexionsendung?"""
    i = 0
    while i < min(len(a), len(b)) and a[i] == b[i]:
        i += 1
    return a[i:] in _ENDUNGEN and b[i:] in _ENDUNGEN


def _umlaut_umschrift(a: str, b: str) -> bool:
    """Unterscheiden sich a und b nur in einem „e" hinter a/o/u? Das ist
    der Umlaut in ASCII-Umschrift — als Flexion (schaden/schaeden) oder als
    fehlender Umlaut (auslander/auslaender), nie ein anderes Wort."""
    if abs(len(a) - len(b)) != 1:
        return False
    kurz, lang = (a, b) if len(a) < len(b) else (b, a)
    i = 0
    while i < len(kurz) and kurz[i] == lang[i]:
        i += 1
    return (i > 0 and lang[i] == "e" and lang[i - 1] in "aou"
            and kurz[i:] == lang[i + 1:])


def _fugen_s(a: str, b: str) -> bool:
    """Ein „s" in der Kompositionsfuge: vermoegen(s)steuer, schaden(s)ersatz.
    Beide Glieder mindestens 5 bzw. 4 Zeichen — sonst waere auch
    „bremen"/„bremsen" eine Fuge."""
    if abs(len(a) - len(b)) != 1:
        return False
    kurz, lang = (a, b) if len(a) < len(b) else (b, a)
    i = 0
    while i < len(kurz) and kurz[i] == lang[i]:
        i += 1
    return (lang[i] == "s" and kurz[i:] == lang[i + 1:]
            and i >= 5 and len(kurz) - i >= 4)


def _t_z_vor_i(a: str, b: str) -> bool:
    """Die zwei amtlichen Schreibungen potenzial/potential,
    essenziell/essentiell: nur t gegen z, und nur vor „i"."""
    if len(a) != len(b):
        return False
    diff = [i for i in range(len(a)) if a[i] != b[i]]
    return (len(diff) == 1 and {a[diff[0]], b[diff[0]]} == {"t", "z"}
            and a[diff[0] + 1:diff[0] + 2] == "i")


def ist_formvariante(tok_n: str, w: str) -> bool:
    """Ist ``w`` eine andere Form oder Schreibung desselben Worts wie
    ``tok_n``? Flexion, Umlaut-Umschrift, Fugen-s, t/z vor i."""
    return (_gleiche_endung(tok_n, w) or _umlaut_umschrift(tok_n, w)
            or _fugen_s(tok_n, w) or _t_z_vor_i(tok_n, w))


def ist_anderes_wort(tok_n: str, w: str) -> bool:
    """Ist ``w`` kein Tippfehler von ``tok_n``, sondern ein anderes echtes
    Wort („fuehrt" neben „fuehlt", „aktion" neben „aktien")?

    Formvarianten zuerst: sie sind billig und brauchen das Vokabular nie."""
    return not ist_formvariante(tok_n, w) and w in echtwoerter()


def token_ist_schreibnah(tok: str, woerter: list[str]) -> bool:
    """Steht im Claim ein Wort, das sich von ``tok`` nur in einem Zeichen
    unterscheidet — bei gleichem Wortanfang — und das kein anderes echtes
    Wort ist?"""
    tok_n = normalisiere(tok)
    if len(tok_n) < MIND_LAENGE or " " in tok_n or "-" in tok_n:
        return False
    praefix = tok_n[:PRAEFIX_LAENGE]
    for w in woerter:
        if w[:PRAEFIX_LAENGE] != praefix:
            continue
        if abstand_hoechstens_eins(tok_n, w) and not ist_anderes_wort(tok_n, w):
            return True
    return False


def tippfehler_match(item: dict, claim_lc: str) -> bool:
    """Wie ``substring_or_composite_match``, aber schreibweisen-tolerant.

    Dieselbe Logik — ``trigger_keywords`` als any-of, ``trigger_composite``
    und ``trigger_all`` als AND-of-OR. Nur der Einzelvergleich ist tolerant.
    """
    claim_n = normalisiere(claim_lc)
    woerter = _kandidaten(claim_n)
    if not woerter:
        return False
    claim_w = wortgrenzen_fassung(claim_n)

    def trifft(tok) -> bool:
        """Exakt ODER schreibweisen-nah. Das ODER ist entscheidend: Ein
        Composite verlangt mehrere Bedingungen, und die kurzen darunter
        ("tirol", 5 Zeichen) koennen per Konstruktion nie tolerant treffen.
        Wer im tolerant-Pass ALLE Vergleiche tolerant macht, verliert sie —
        und damit genau den Fall, der das Modul ausgeloest hat."""
        if not isinstance(tok, str):
            return False
        return (trifft_mit_wortgrenze(claim_n, claim_w, tok)
                or token_ist_schreibnah(tok, woerter))

    for kw in item.get("trigger_keywords") or ():
        if trifft(kw):
            return True
    composite = item.get("trigger_composite") or []
    if composite and all(
        isinstance(alt, (list, tuple)) and any(trifft(tok) for tok in alt)
        for alt in composite
    ):
        return True
    for rule in item.get("trigger_all") or ():
        if rule and all(
            isinstance(alt, (list, tuple)) and any(trifft(tok) for tok in alt)
            for alt in rule
        ):
            return True
    return False
