"""Zeitreihen fuer Richtungs-Claims — die Zahl von damals gehoert dazu.

QA50F-Befund 4 (2026-09-07): „Die Demokratie in Polen hat sich seit 2023
verbessert" bekam `unverifiable@0.15`. Die Begruendung des Modells war
korrekt und entlarvend:

    „Die V-Dem-Daten fuer Polen zeigen Werte fuer 2025, aber keine direkten
     Vergleichsdaten zu 2023, um eine Verbesserung seit diesem Jahr zu
     belegen."

Das Modell hatte recht: geliefert wurde EIN Jahr. In ``vdem_indicators.json``
steht fuer Polen aber die volle Reihe — 2023: 0.457, 2024: 0.613, 2025:
0.645. Der Konnektor rendert seit jeher nur ``_latest_year``. Die Daten waren
da, die Antwort war unmoeglich.

EINE KORREKTUR ZU MEINER EIGENEN BEGRUENDUNG
===========================================
Beim Bauen hielt ich einen zweiten Fall fuer denselben Fehler: „Die Demokratie
in Ungarn hat sich seit 2010 verschlechtert" bekam `true@0.95` mit „sank von
0,78 (2010) auf 0,32 (2025)" — und 0,78 steht nicht in
``vdem_indicators.json``, dessen Reihe 2019 beginnt. Ich habe daraus eine
erfundene Zahl gemacht. **Das war falsch.**

Nachgesehen: ``data/demokratie_pack.json`` enthaelt „HU 2010 0.78 -> 2023
0.32" aus dem V-Dem Democracy Report 2024. Das Modell hat eine legitime
zweite Quelle von uns zitiert und mit dem Konnektor kombiniert — genau das,
was es tun soll. Der Beleg war echt, mein Vorwurf nicht.

Was bleibt, ist der Polen-Fall: dort fehlte die Vergleichszahl wirklich, in
allen Quellen. Und der Hinweis auf ein fehlendes Bezugsjahr bleibt richtig —
er sagt etwas ueber DIESE Reihe, nicht ueber die Datenlage insgesamt. Genau
deshalb ist er so formuliert.

WO DIE REIHE HINGEHOERT
=======================
In den ``indicator_name``. Der geht bis 400 Zeichen ungekuerzt in den Prompt
(#131), der ``display_value`` nicht zwangslaeufig — dieselbe Ueberlegung wie
bei der Skalen-Richtung (siehe ``_skala.py``).

ZITIEREN, NICHT BEWERTEN
========================
Der Text nennt Zahlen und ihre Differenz, kein Urteil: „Differenz +0.19",
nicht „deutliche Verbesserung". Was ein hoeherer Wert bedeutet, sagt bereits
der Skalen-Hinweis. Die Politik-Guardrails verlangen, dass wir Werte
zitieren und die Bewertung der Quelle ueberlassen — eine Subtraktion zweier
zitierter Zahlen bleibt Zitat.
"""

from __future__ import annotations

import re

from services._schreibweise import normalisiere

# Ein Jahr im Claim. Bewusst eng: vierstellig, 1900-2099, an Wortgrenzen —
# sonst faengt das Muster Hausnummern, Betraege und Aktenzeichen.
_JAHR = re.compile(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)")

# Wortstaemme fuer Veraenderung. Staemme, keine Formen: wer Flexionen
# aufzaehlt, vergisst welche (Lehre aus den Frontex-Formen, #141).
_VERAENDERUNG = (
    "verbesser", "verschlechter", "gestiegen", "steig", "gesunken", "sink",
    "zugenommen", "zunahm", "zunahme", "abgenommen", "abnahm", "abnahme",
    "gewachsen", "wachs", "schrumpf", "rueckgang", "rueckläufig", "ruecklaeufig",
    "entwickl", "trend", "verlauf", "fortschritt", "erhol", "verfall",
    "erodier", "abbau", "seither", "frueher", "damals", "inzwischen",
    "mittlerweile", "nach wie vor", "weiterhin",
)

# Praepositionen, die eine Zeitspanne aufmachen. „seit" allein reicht nicht
# als Beweis (es gibt „seit Jahren"), aber zusammen mit dem Jahres-Muster
# oder einem Veraenderungs-Stamm ist die Absicht eindeutig.
_SPANNE = ("seit", "gegenueber", "im vergleich zu", "verglichen mit", "bis")

MAX_JAHRE_IN_REIHE = 8


def verlangt_verlauf(claim: str) -> bool:
    """True, wenn der Claim eine Aussage ueber Zeit macht.

    Zwei unabhaengige Ausloeser, beide bewusst:

    1. Ein Jahr im Claim. Auch ohne Richtungswort ist die Reihe dann noetig —
       ``_latest_year`` liefert sonst 2025, waehrend der Claim nach 2021
       fragt, und das faellt niemandem auf.
    2. Ein Veraenderungs-Stamm. „verbessert", „gestiegen", „Trend" fragen
       nach einer Differenz, und eine Differenz braucht zwei Zahlen.

    >>> verlangt_verlauf("Die Demokratie in Polen hat sich seit 2023 verbessert")
    True
    >>> verlangt_verlauf("Polen ist eine Demokratie")
    False
    """
    if not claim:
        return False
    if _JAHR.search(claim):
        return True
    n = normalisiere(claim)
    return any(stamm in n for stamm in _VERAENDERUNG)


def bezugsjahr(claim: str) -> str | None:
    """Das FRUEHESTE im Claim genannte Jahr, als String.

    „seit 2023 verbessert" fragt nach 2023 als Ausgangspunkt. Nennt der Claim
    mehrere Jahre („zwischen 2019 und 2023"), ist das fruehere der Bezug —
    das spaetere ist der Zielpunkt und steht ohnehin in der Reihe.
    """
    treffer = _JAHR.findall(claim or "")
    return min(treffer) if treffer else None


def _zahl(wert: float, nachkomma: int) -> str:
    return f"{wert:.{nachkomma}f}"


def verlauf_text(reihe: dict, *, bezug: str | None = None,
                 nachkomma: int = 2) -> str:
    """Ein Satzteil mit Reihe, Endpunkten und Differenz.

    >>> verlauf_text({"2023": 0.457, "2024": 0.613, "2025": 0.645},
    ...              bezug="2023")
    '2023: 0.46 -> 2025: 0.65, Differenz +0.19 (Reihe 2023-2025: 0.46/0.61/0.65)'

    Fehlt das Bezugsjahr in der Reihe, wird das ausgesprochen statt still
    ersetzt — sonst beantwortet die Zeile eine andere Frage als die gestellte.
    Der Hinweis gilt bewusst nur DIESER Reihe: eine andere Quelle kann das
    Jahr haben, und das soll er nicht bestreiten.

    >>> verlauf_text({"2019": 0.41, "2025": 0.32}, bezug="2010")
    '2010 nicht in dieser Reihe; sie beginnt 2019. 2019: 0.41 -> 2025: 0.32, Differenz -0.09 (Reihe 2019-2025: 0.41/0.32)'
    """
    paare = []
    for jahr, wert in (reihe or {}).items():
        try:
            paare.append((int(jahr), float(wert)))
        except (TypeError, ValueError):
            continue
    if len(paare) < 2:
        return ""
    paare.sort()
    paare = paare[-MAX_JAHRE_IN_REIHE:]

    vorspann = ""
    start = paare[0]
    if bezug:
        passend = [p for p in paare if str(p[0]) == bezug]
        if passend:
            start = passend[0]
        else:
            # „in dieser Reihe", nicht „in unseren Daten": eine andere
            # Quelle kann das Jahr sehr wohl haben (siehe Korrektur oben).
            vorspann = (f"{bezug} nicht in dieser Reihe; "
                        f"sie beginnt {paare[0][0]}. ")
    ende = paare[-1]
    if start[0] == ende[0]:
        return ""

    diff = ende[1] - start[1]
    werte = "/".join(_zahl(w, nachkomma) for _, w in paare)
    return (
        f"{vorspann}"
        f"{start[0]}: {_zahl(start[1], nachkomma)} -> "
        f"{ende[0]}: {_zahl(ende[1], nachkomma)}, "
        f"Differenz {diff:+.{nachkomma}f} "
        f"(Reihe {paare[0][0]}-{paare[-1][0]}: {werte})"
    )
