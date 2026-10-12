"""Zaehlweisen-Guard: ein Vergleich ohne genannte Zaehlweise hat keine Seite.

Anlass (12.10.2026, QA50H #51/#52 und eine zweite Live-Probe). Der Dienst
bejahte BEIDE Richtungen desselben Vergleichs mit hoher Konfidenz:

    "In Österreich werden mehr Frauen als Männer ermordet"   -> true @ 0.85
    "Österreich 2024: mehr männliche als weibliche Mordopfer" -> true @ 0.85

    "Mauna Kea ist höher als der Everest"   -> mostly_true @ 0.85
    "Der Everest ist höher als der Mauna Kea" -> true @ 0.85

Hoechstens eine Seite kann stimmen — oder, richtig gelesen, keine: Die
Behauptung sagt nicht, WAS gezaehlt wird. Die Polizeiliche
Kriminalstatistik 2024 nennt 40 vollendete Morde an Frauen gegen 36 an
Maennern, bei Mord UND Mordversuch 98 Frauen gegen 187 Maenner. Beide
Zahlen sind amtlich, und der kuratierte Fakt sagt es selbst: "Drei
Zaehlweisen, die oft vermischt werden — beim Zitieren immer dazusagen,
welche gemeint ist." Der Everest-Fakt genauso: "Pauschal ohne
Spezifizierung: alle 3 Antworten richtig."

## Warum nicht rechnen

Naheliegend waere, die Zahlen beider Seiten zu vergleichen — Muster P tut
das. Fuer "mehr X als Y" hat PR #227 die Rechnung BEWUSST stillgelegt, mit
genau diesem Fall als Begruendung ("zufaellig richtig oder zufaellig
falsch"). Das wurde am 12.10.2026 nachgemessen und bestaetigt: Satzweise
ueber die echten Antworten gerechnet, sagt die Arithmetik fuer #51 UND
fuer die Gegenbehauptung #52 jeweils "Claim falsch" — sie greift einfach
die andere Zaehlweise als das Modell. Zwei gegensaetzliche Behauptungen
koennen nicht beide falsch sein.

Auch das Ueberlappungs-Signal aus ``zahlen_beider_seiten`` traegt nicht:
Von drei ueberlappenden Spannen in 116 Live-Antworten waren zwei reines
Zuordnungs-Rauschen auf RICHTIGEN Verdicts (ein RSF-Rang landete neben
einem RSF-Score, eine Prozentzahl auf beiden Seiten).

Also nicht die Richtung erraten, sondern das sagen, was bekannt ist: Die
Frage hat ohne Zaehlweise keine Antwort. Das Label dafuer ist ``mixed``.

## Warum die Daten nur Klartext-Namen deklarieren

Der Guard muss zwei Dinge wissen: DASS der Vergleich zaehlweisenabhaengig
ist, und ob die Behauptung die Zaehlweise selbst nennt. Fuer das Zweite
braucht er Woerter — und eine Liste von Such-Staemmen neben den Namen
waere eine zweite Kopie derselben Sache, die driftet
([[lessons_learned]]: eine Zahl in zwei Kopien driftet unbemerkt).

Stattdessen werden die unterscheidenden Woerter aus den Namen ABGELEITET:
Ein Wort unterscheidet, wenn es in genau EINEM der deklarierten Namen
vorkommt. Aus

    ["vollendete Morde", "Mord und Mordversuch", "Partnerschaftskontext"]

folgt mechanisch, dass "vollendete", "mordversuch" und
"partnerschaftskontext" unterscheiden, "mord" aber nicht — es steht in
zwei Namen. Genau richtig: "mord" steht ohnehin in jeder Behauptung zum
Thema und wuerde den Guard immer stilllegen.
"""

from __future__ import annotations

import re
from functools import lru_cache

from services._schreibweise import normalisiere

#: Prefix im ``display_value``, parallel zu ``STRUKTURELL FALSCH:``. Der
#: Synthesizer-Prompt und ``verdict_postprocess`` erkennen es daran.
ZAEHLWEISE_PREFIX = "ZAEHLWEISE ENTSCHEIDET"

#: Der ``data``-Schluessel, unter dem ein Fakt seine Zaehlweisen deklariert.
FELD = "zaehlweisen_namen"

#: Trennzeichen zwischen den Namen im Prefix. Ein Schraegstrich mit
#: Leerzeichen, weil Namen selbst Kommas und "und" tragen ("Mord und
#: Mordversuch") und ein Komma sie zerschneiden wuerde.
_TRENNER = " / "

_PREFIX_RE = re.compile(
    re.escape(ZAEHLWEISE_PREFIX) + r"\s*\(([^)]*)\)\s*:",
    re.IGNORECASE,
)

#: Funktionswoerter, die in einem Zaehlweisen-Namen nichts unterscheiden.
_NICHT_UNTERSCHEIDEND = frozenset({
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einer",
    "und", "oder", "von", "vom", "zur", "zum", "ueber", "unter", "mit",
    "ohne", "bei", "im", "in", "am", "an", "auf", "fuer", "als", "pro",
    "je", "nach", "aus", "inkl", "einschliesslich", "nur", "alle",
    "anzahl", "zahl", "hoehe", "anteil", "quote",
})

#: Kuerzere Woerter taugen nicht zum Unterscheiden — sie stecken als
#: Teilzeichenkette in fremden Woertern. Dieselbe Schwelle wie
#: ``_flexion.MIN_FLEKTIERBAR``, aus demselben Grund.
MIN_UNTERSCHEIDEND = 4


def _woerter(name: str) -> list[str]:
    return [w for w in re.findall(r"[a-zäöüßa-z]+", normalisiere(name))
            if len(w) >= MIN_UNTERSCHEIDEND and w not in _NICHT_UNTERSCHEIDEND]


def _gleicher_stamm(a: str, b: str) -> bool:
    """Sind das zwei Formen desselben Wortes?

    Gemessen am 12.10.2026: Die erste Fassung zaehlte, in wie vielen Namen
    ein Wort WOERTLICH vorkommt — und hielt "mord" (aus "Mord und
    Mordversuch") fuer unterscheidend gegen "morde" (aus "vollendete
    Morde"), weil die Zeichenketten verschieden sind. Damit nannte
    "mehr männliche als weibliche MORDOPFER" angeblich eine Zaehlweise,
    und der Guard schwieg auf seinem eigenen Ausloeser. Dieselbe Lehre wie
    PR #229: Singular und Plural sind nicht dasselbe Token.

    Dieselbe Toleranz wie ``_flexion.MAX_ENDUNG``, aus demselben Grund.
    """
    kurz, lang = sorted((a, b), key=len)
    return lang.startswith(kurz) and len(lang) - len(kurz) <= 2


def deklarierte_namen(d) -> tuple[str, ...]:
    """Die im Fakt deklarierten Zaehlweisen-Namen, oder ``()``.

    Erwartet wird im ``data``-Dict eine Liste von Klartext-Namen unter
    ``zaehlweisen_namen``. Unter zwei Namen ist nichts zu entscheiden — eine
    einzige Zaehlweise ist keine Mehrdeutigkeit.

    Der Schluessel heisst nicht ``zaehlweisen``, weil
    ``gleichstellung_pack.json`` den Namen schon fuer einen Prosa-Absatz
    belegt ("Warum Zahlen auseinandergehen: …"). Zwei Bedeutungen unter
    einem Schluessel waeren genau die Doppeldeutigkeit, gegen die dieser
    Guard gebaut ist.
    """
    if not isinstance(d, dict):
        return ()
    roh = d.get(FELD)
    if not isinstance(roh, (list, tuple)):
        return ()
    namen = tuple(str(n).strip() for n in roh if str(n).strip())
    return namen if len(namen) >= 2 else ()


def unterscheidende_woerter(namen) -> dict[str, tuple[str, ...]]:
    """Je Name die Woerter, die NUR in ihm vorkommen.

    Ein Wort in mehreren Namen unterscheidet nicht: "mord" steht in
    "vollendete Morde" und in "Mord und Mordversuch" und taugt deshalb
    nicht als Hinweis, welche der beiden gemeint ist.
    """
    je_name = {n: set(_woerter(n)) for n in namen}
    aus: dict[str, tuple[str, ...]] = {}
    for name, woerter in je_name.items():
        fremd = [w for andere, ws in je_name.items() if andere != name
                 for w in ws]
        aus[name] = tuple(sorted(
            w for w in woerter
            if not any(_gleicher_stamm(w, f) for f in fremd)))
    return aus


@lru_cache(maxsize=2048)
def _anfangs_muster(wort: str) -> re.Pattern[str]:
    return re.compile(r"(?<![a-zäöüß])" + re.escape(wort)
                      + r"[a-zäöüß]{0,2}")


def _trifft_am_wortanfang(claim_n: str, wort: str) -> bool:
    """Steckt ``wort`` im Claim, und beginnt es dort ein Wort?

    ``_flexion.trifft`` laesst Einwort-Begriffe bewusst ungebunden, damit
    "mord" in "Mordopfer" trifft — das ist fuer Trigger richtig. Hier
    nicht: Ohne vordere Grenze fand "morde" sich in "erMORDEt" wieder und
    legte den Guard auf seinem eigenen Ausloeser still. Die HINTERE Grenze
    bleibt offen (zwei Zeichen Endung), damit "vollendete" auch
    "vollendeten" findet und der Kopf eines Kompositums weiter trifft.
    """
    return bool(_anfangs_muster(wort).search(claim_n))


def claim_nennt_zaehlweise(claim: str, namen) -> str | None:
    """Welchen der Namen nennt die Behauptung selbst? Sonst ``None``.

    Gesucht wird mit ``_flexion.trifft``, damit "bei vollendeten Morden"
    den Namen "vollendete Morde" findet — die Endung unterscheidet sich.
    """
    if not claim or not namen:
        return None
    claim_n = normalisiere(claim.lower())
    for name, woerter in unterscheidende_woerter(namen).items():
        if any(_trifft_am_wortanfang(claim_n, w) for w in woerter):
            return name
    return None


def prefix_fuer(namen) -> str:
    """Der ``display_value``-Prefix fuer diese Namen, oder ``""``."""
    if not namen:
        return ""
    return f"{ZAEHLWEISE_PREFIX} ({_TRENNER.join(namen)}):"


def namen_aus_text(text: str) -> tuple[str, ...]:
    """Die Namen aus einem ``display_value`` zurueckholen, oder ``()``.

    Die Gegenrichtung zu :func:`prefix_fuer`. ``verdict_postprocess`` sieht
    nur die gerenderten Quellen-Texte, nicht mehr das ``data``-Dict.
    """
    if not text:
        return ()
    m = _PREFIX_RE.search(text)
    if not m:
        return ()
    namen = tuple(n.strip() for n in m.group(1).split(_TRENNER) if n.strip())
    return namen if len(namen) >= 2 else ()


def name_ist_zulaessig(name: str) -> bool:
    """Taugt der Name fuer Prefix UND Rueckweg?

    Klammern wuerden ``_PREFIX_RE`` beenden, der Trenner wuerde einen
    Namen in zwei zerlegen. Ein Gate prueft das ueber alle Fakten, damit
    ein neuer Name den Rueckweg nicht still zerschneidet.
    """
    return bool(name.strip()) and not any(
        z in name for z in ("(", ")", _TRENNER))
