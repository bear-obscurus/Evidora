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
"""

import re

from services._flexion import trifft as _flexion_trifft
from services._schreibweise import normalisiere

MIND_LAENGE = 6
PRAEFIX_LAENGE = 3

_WORT_RE = re.compile(r"[a-zäöüß]+")


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


def token_ist_schreibnah(tok: str, woerter: list[str]) -> bool:
    """Steht im Claim ein Wort, das sich von ``tok`` nur in einem Zeichen
    unterscheidet — bei gleichem Wortanfang?"""
    tok_n = normalisiere(tok)
    if len(tok_n) < MIND_LAENGE or " " in tok_n or "-" in tok_n:
        return False
    praefix = tok_n[:PRAEFIX_LAENGE]
    for w in woerter:
        if w[:PRAEFIX_LAENGE] != praefix:
            continue
        if abstand_hoechstens_eins(tok_n, w):
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

    def trifft(tok) -> bool:
        """Exakt ODER schreibweisen-nah. Das ODER ist entscheidend: Ein
        Composite verlangt mehrere Bedingungen, und die kurzen darunter
        ("tirol", 5 Zeichen) koennen per Konstruktion nie tolerant treffen.
        Wer im tolerant-Pass ALLE Vergleiche tolerant macht, verliert sie —
        und damit genau den Fall, der das Modul ausgeloest hat."""
        if not isinstance(tok, str):
            return False
        return _flexion_trifft(claim_n, tok) or token_ist_schreibnah(tok, woerter)

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
