"""Skalen-Richtung in Worten — gegen die Inversions-Falle bei Vergleichen.

QA50F-Befund 1 (2026-09-07): „Die Pressefreiheit in Kamerun ist schlechter
als in Suedafrika" bekam `mostly_false@0.85` — obwohl die eigene Summary die
Zahlen korrekt nannte: Kamerun 40,9/100 (Rang 133), Suedafrika 78,0/100
(Rang 21). Kamerun IST schlechter, der Claim ist wahr. **Das Verdict
widersprach seiner eigenen Begruendung.**

GEMESSEN, NICHT VERMUTET
========================
Spiegel-Paare gegen die Live-Instanz, bei denen genau eine Richtung wahr sein
kann:

    Kamerun schlechter als Suedafrika (Presse)   WAHR   -> mostly_false@0.85  X
    Suedafrika schlechter als Kamerun (Presse)   FALSCH -> false@0.95         ok
    Kamerun korrupter als Oesterreich            WAHR   -> true@0.80          ok
    Oesterreich korrupter als Kamerun            FALSCH -> false@0.80         ok

Beide Presse-Richtungen bekamen ein Nein. Logisch unmoeglich — und der
Unterschied zum funktionierenden CPI-Paar liegt in EINEM Feld:

    CPI:  "CPI Cameroon (2024): 26/100 — hoch wahrgenommene Korruption"
    RSF:  "RSF Pressefreiheit Cameroon (2026): 40.9/100 — Schwierig
           (Rang 133/180)"

CPI uebersetzt die Zahl in Worte; die Richtung steht da. RSF nennt eine
Kategorie („Schwierig") und einen RANG — und der Rang laeuft zahlenmaessig
GEGEN den Score: das schlechtere Land hat die groessere Rangzahl. Zwei Zahlen
in derselben Zeile, die in entgegengesetzte Richtungen zeigen.

Nachgemessen ueber neun Index-Quellen: **acht nennen ihre Richtung nicht**,
nur `transparency` tut es — und `transparency` ist die einzige, die das
Spiegel-Paar besteht. Vier der acht melden zusaetzlich einen Rang.

WAS DIESER HELFER TUT
=====================
Er haengt an den ``indicator_name`` einen kurzen Satz, der die Richtung
ausspricht. Bewusst dort und nicht im ``display_value``: die Headline geht bis
400 Zeichen ungekuerzt in den Prompt (#131), der display_value nicht
zwangslaeufig.

Bewusst KEIN Freitext pro Konnektor: eine Formulierung, die ueberall gleich
aussieht, ist fuer das Modell ein wiedererkennbares Muster. Neun verschiedene
Umschreibungen waeren neun Gelegenheiten, es anders zu lesen.
"""

from __future__ import annotations


def richtung(hoch_bedeutet: str, *, spanne: str = "",
             mit_rang: bool = False) -> str:
    """Skalen-Hinweis fuer den ``indicator_name``.

    >>> richtung("mehr Pressefreiheit", spanne="0-100", mit_rang=True)
    ' [Skala 0-100, HOEHERER Wert = mehr Pressefreiheit; NIEDRIGERE Rangzahl = besser]'

    Args:
        hoch_bedeutet: was ein HOHER Wert inhaltlich heisst — in Worten, nicht
            als Symbol. „mehr Pressefreiheit", nicht „besser".
        spanne: die Wertespanne, wenn sie nicht offensichtlich ist.
        mit_rang: setzen, wenn dieselbe Zeile auch einen Rang nennt. Genau
            diese Kombination hat die Inversion ausgeloest.
    """
    if not hoch_bedeutet:
        raise ValueError("hoch_bedeutet darf nicht leer sein — ohne Inhalt "
                         "ist der Hinweis wertlos")
    teile = []
    if spanne:
        teile.append(f"Skala {spanne}")
    teile.append(f"HOEHERER Wert = {hoch_bedeutet}")
    if mit_rang:
        teile.append("NIEDRIGERE Rangzahl = besser")
    return " [" + ", ".join(teile) + "]"
