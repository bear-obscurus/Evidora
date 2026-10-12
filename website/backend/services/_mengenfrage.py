"""Eine Gruppe, die nach einer GRÖSSE fragt, muss auch auf die Frageform hören.

QA50H (12.10.2026): „Wie viele Sonderschüler gibt es in Österreich?" blieb
`unverifiable@0.1` mit 0 Belegen. Der Fakt `nbb_2024_spf_bestand_at` trägt
die Zahl, aber seine Mengen-Gruppe führt

    ["quote", "anteil", "prozent", "anzahl", "bestand", …]

und damit kein einziges Wort, das ein Mensch tatsächlich tippt, wenn er
nach einer Anzahl fragt. Die Notiz dazu stammt vom 28.09.; #231 hat nur
„wie viel gibt" für einen ANDEREN Fakt verengt.

Zensus über alle Fakten: 71 tragen eine Mengen-Gruppe, **50 davon ohne
jede Frageform**, verteilt auf 25 Dateien.

WARUM HIER UND NICHT IN DEN DATEN. Die Alternative wäre, in 50 Gruppen
„wie viele" und „wie viel" einzutragen. Dagegen sprechen zwei Dinge:

  1. Die Lehre aus #231 — „wie viel" ist eine Teilzeichenkette von „wie
     viele". Wer beide in 50 Listen pflegt, baut 50 Gelegenheiten für
     genau die Verwechslung ein, die dort schon einmal dazu führte, dass
     jede Kopfzahl-Frage den Ausgaben-Fakt mitzog.
  2. Eine Regel an einem Ort ist eine Regel; dieselbe Regel in 50 Kopien
     driftet (vgl. „eine Zahl in zwei Kopien driftet unbemerkt").

Die AND-Struktur der Composite-Trigger trägt das Risiko: die Frageform
befriedigt nur die MENGEN-Gruppe. Die übrigen Gruppen (Thema, Land) müssen
weiterhin treffen — „Wie viele Menschen leben in Wien?" erreicht den
Sonderschul-Fakt nicht, weil dessen Themen-Gruppe nicht anschlägt.

Gebaut wie `_wortformen.mit_wortformen` (#228): die Gruppe wird für den
Vergleich erweitert, die Daten bleiben unberührt.
"""

from __future__ import annotations

from services._schreibweise import normalisiere

# Woran man eine Mengen-Gruppe erkennt: sie fragt nach einer GRÖSSE.
# Bewusst eng — „zahl" allein wäre zu breit („Zahlungsverkehr").
MENGEN_MARKER = frozenset({
    "anzahl", "anteil", "quote", "prozent", "bestand", "hoehe", "summe",
    "menge", "rate", "pro kopf",
})

# Was ein Mensch tippt. Beide Formen, Zusammen- und Getrenntschreibung.
FRAGEFORMEN: tuple[str, ...] = ("wie viele", "wie viel", "wieviele", "wieviel")


# Eine Gruppe ist erst dann eine MENGEN-Gruppe, wenn sie ueberwiegend nach
# einer Groesse fragt — nicht schon, wenn sie eine erwaehnt.
#
# Gemessen: mit "enthaelt EINEN Marker" kamen sechs neue Fremdtreffer, zwei
# davon falsch. `frauen_in_politik_2026` hat 20 Token, von denen zwei
# ("anteil", "quote") Mengen-Marker sind — die Gruppe fragt nach
# Repraesentation, nicht nach einer Zahl. Und `partg_par3_rahmen_pro_
# wahlberechtigtem` galt nur wegen "pro kopf" als Mengen-Gruppe, obwohl
# die Gruppe Bezugsgroessen auflistet. In beiden Faellen war die
# Mengen-Gruppe das Einzige, was einen zu weiten Themen-Treffer noch
# aufhielt.
# 0,25 gemessen, nicht geraten. Die Schwellen-Reihe (Fremdtreffer /
# synthetische Zielsonden gegen die Regel-aus-Fassung):
#   0,00  +6 / +24      0,10  +2 / +13
#   0,15  +1 /  +7      0,25  +0 /  +5
# Die synthetischen Sonden suggerierten, man muesse +6 Fremdtreffer fuer
# den Gewinn bezahlen. Sie waren aber aus rohen Trigger-Tokens gebaut
# ("Wie viele sonderpaedagog gibt es") — keine Frage, die jemand stellt.
# An ECHTEN Fragen gemessen ("Wie viele Sonderschueler gibt es in
# Oesterreich?", "Wie viele Rinder gibt es in Oesterreich?", "Wie viele
# Frauen wurden 2024 in Oesterreich ermordet?") trifft jede davon bei
# JEDER Schwelle — also auch bei 0,25, wo kein einziger Fremdtreffer
# hinzukommt. Die Kosten waren eine Taeuschung der Messgroesse.
MINDESTANTEIL = 0.25


def ist_mengengruppe(alt) -> bool:
    tokens = [str(t) for t in alt]
    if not tokens:
        return False
    marker = sum(1 for t in tokens if normalisiere(t) in MENGEN_MARKER)
    return marker > 0 and marker / len(tokens) >= MINDESTANTEIL


def mit_mengenfrage(alt):
    """Erweitert eine Mengen-Gruppe um die Frageformen, sonst unveraendert."""
    if not ist_mengengruppe(alt):
        return alt
    return tuple(alt) + FRAGEFORMEN
