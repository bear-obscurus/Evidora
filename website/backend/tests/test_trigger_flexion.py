"""Flexion im Trigger: „freie Wahlen" trifft „freien Wahlen" nicht.

QA50F-Befund 2: „In Nordkorea gibt es keine **freien** Wahlen" feuert Freedom
House nicht. Der Trigger heisst `"freie wahlen"`.

    False  In Nordkorea gibt es keine freien Wahlen
    True   In Nordkorea gibt es keine freie Wahlen

GEMESSEN — UND DIE MESSUNG HAT ZWEI HYPOTHESEN VERWORFEN
=========================================================
Erster Verdacht war „Flexion allgemein". Über alle Trigger-Prädikate:

    Genitiv-s   („oesterreich" -> „oesterreichs")   731/731  = 100 %
    Plural -en  („wahl" -> „wahlen")               1597/1598 =  99 %
    Adjektiv-Endung im MEHRWORT-Begriff             108/492  =  21 %

Genitiv und Plural sind **kein** Problem: die Endung wächst HINTEN an, und
`"wahl" in "wahlen"` ist wahr. Substring-Matching kann das von selbst.

Der Ausfall ist ein **Infix**: bei „freie wahlen" flektiert das VORDERE Wort,
und damit reisst der Substring. Das ist die ganze Fehlerklasse.

Zweiter verworfener Verdacht: die Pack-Trigger in den JSON-Dateien seien
mitbetroffen — sie sind es, aber sie tauchten in der ersten Messung gar nicht
auf, weil die nur Python-Literale las. Nachgemessen: 228 deutsche
Mehrwort-Trigger in `data/*.json`, **0 von 912** flektierten Varianten
trafen. Nach dem Umbau: 912/912.

DIE REGEL, NICHT DIE LISTE
==========================
Jedes Wort ausser dem letzten darf bis zu zwei zusätzliche Buchstaben tragen.
Das deckt die deutschen Adjektiv-Endungen ab, ohne sie aufzuzählen — die
Lehre aus den Frontex-Flexionsformen (#141). Das letzte Wort bleibt offen,
dort wächst die Endung ohnehin nach hinten.

BEWUSST NICHT UMGEBAUT
======================
17 Dienste normalisieren ihren Claim nicht (`bis`, `arxiv`, `sipri`, …). Für
sie wäre der Umbau asymmetrisch — genau die Falle, die in #147 47 Services
still kaputtgemacht hat. Sie brauchen zuerst die Normalisierung; die
verbleibenden Ausfälle in der Messung stammen alle von dort.
"""

import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._flexion import MAX_ENDUNG, trifft  # noqa: E402
from services._schreibweise import normalisiere  # noqa: E402


def _t(claim, term):
    return trifft(normalisiere(claim), term)


# --------------------------------------------------------------------------
# Der auslösende Fall
# --------------------------------------------------------------------------

def test_der_ausloesende_fall():
    from services.freedom_house import claim_mentions_freedom_house_cached as FH
    assert FH("In Nordkorea gibt es keine freien Wahlen")
    assert FH("In Nordkorea gibt es keine freie Wahlen")


def test_frontex_flexionsformen():
    """Dieselbe Klasse an der Quelle, die #141 schon einmal betraf."""
    from services.frontex import claim_mentions_frontex_cached as FX
    for c in ("Die irregulaeren Grenzuebertritte sind gestiegen",
              "irregulaere Grenzuebertritte", "irregulärer Grenzübertritte"):
        assert FX(c), c


@pytest.mark.parametrize("claim,term", [
    ("in nordkorea gibt es keine freien wahlen", "freie wahlen"),
    ("bei den freier wahlen", "freie wahlen"),
    ("die politischen rechte in ungarn", "politische rechte"),
    ("mit kuenstlicher intelligenz", "kuenstliche intelligenz"),
    ("an den irregulaeren grenzuebertritten", "irregulaere grenzuebertritte"),
    ("die grossen mauer", "grosse mauer"),
])
def test_flektiertes_adjektiv_trifft(claim, term):
    assert _t(claim, term), (claim, term)


# --------------------------------------------------------------------------
# Was Substring-Matching schon konnte — und weiter können muss
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim,term", [
    ("oesterreichs wirtschaft", "oesterreich"),      # Genitiv-s
    ("die wahlen 2026", "wahl"),                     # Plural, waechst hinten
    ("die wahlbeteiligung sank", "wahlbeteiligung"),  # Wort im Satz
    ("freie wahlen", "freie wahlen"),                # unveraendert
])
def test_endungen_die_hinten_wachsen_trafen_schon_immer(claim, term):
    assert _t(claim, term), (claim, term)


# --------------------------------------------------------------------------
# Die Grenzen der Regel
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim,term", [
    ("die freiheit der wahlen", "freie wahlen"),
    ("auf freiem markt", "freie wahlen"),
    ("politik und rechte", "politische rechte"),
    ("grosse mauern und kleine zaeune", "kleine mauer"),
])
def test_kein_treffer_wo_keiner_hingehoert(claim, term):
    assert not _t(claim, term), (claim, term)


def test_endung_ist_begrenzt():
    """Zwei Buchstaben, nicht beliebig viele. Ohne Deckel wuerde „freie" zu
    „freiheit" und der Trigger fraesse fremde Woerter."""
    assert MAX_ENDUNG == 2
    assert not _t("die freiheitliche wahlen", "freie wahlen")


def test_einwort_begriffe_gehen_den_schnellen_weg():
    """Ein einzelnes Wort braucht die Regel nicht — dort waechst jede Endung
    hinten an. Der Regex-Pfad bleibt den Mehrwort-Begriffen vorbehalten, das
    ist der Kostendeckel."""
    assert _t("die wahlen", "wahl")
    assert not _t("die wahlen", "waehler")


def test_leere_eingaben():
    assert not trifft("irgendein claim", "")
    assert not trifft("", "freie wahlen")


# --------------------------------------------------------------------------
# Reichweite: eine Stelle deckt 62 Pack-Dienste ab
# --------------------------------------------------------------------------

def test_gemeinsamer_matcher_nutzt_die_regel():
    """`find_matching_items` -> `substring_or_composite_match` -> `trifft`.
    Eine Aenderung dort erreicht alle Pack-Dienste auf einmal."""
    quelle = (BACKEND / "services" / "_topic_match.py").read_text(encoding="utf-8")
    assert "_flexion_trifft" in quelle
    from services._topic_match import substring_or_composite_match
    item = {"trigger_keywords": ["freie wahlen"]}
    assert substring_or_composite_match(item, "es gab keine freien wahlen")
    assert not substring_or_composite_market_dummy(item)


def substring_or_composite_market_dummy(item):
    """Gegenprobe-Helfer: derselbe Trigger darf bei fremdem Text schweigen."""
    from services._topic_match import substring_or_composite_match
    return substring_or_composite_match(item, "ein rezept fuer marmorkuchen")


def test_composite_regeln_gelten_weiter():
    """Die AND-of-OR-Struktur darf durch den flexionstoleranten Vergleich
    nicht aufweichen."""
    from services._topic_match import substring_or_composite_match
    item = {"trigger_composite": [["freie wahlen"], ["ungarn"]]}
    assert substring_or_composite_match(item, "freien wahlen in ungarn")
    assert not substring_or_composite_match(item, "freien wahlen in polen")


# --------------------------------------------------------------------------
# Was bewusst offen bleibt
# --------------------------------------------------------------------------

def test_dienste_ohne_normalisierung_bleiben_unberuehrt():
    """17 Dienste normalisieren ihren Claim nicht. Sie umzubauen waere
    asymmetrisch — die Falle aus #147. Ein Test haelt fest, dass sie bewusst
    aussen vor sind, damit es niemand fuer ein Versehen haelt."""
    import re
    ohne = []
    for p in sorted((BACKEND / "services").glob("*.py")):
        if p.stem.startswith("_"):
            continue
        q = p.read_text(encoding="utf-8")
        if "claim_mentions" not in q and "claim_wants" not in q:
            continue
        if "_flexion_trifft" in q:
            continue
        if re.search(r"any\(\w+ in claim", q):
            ohne.append(p.stem)
    assert "bis" in ohne, "bis normalisiert nicht und darf nicht umgebaut sein"
    for d in ohne:
        q = (BACKEND / "services" / f"{d}.py").read_text(encoding="utf-8")
        assert not re.search(r"normalisiere\(\s*claim", q), (
            f"{d} normalisiert doch — dann gehoert es in den Umbau")
