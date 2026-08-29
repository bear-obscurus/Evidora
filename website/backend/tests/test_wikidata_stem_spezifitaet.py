"""Positions-Filter nimmt den SPEZIFISCHSTEN Amts-Stem (Meloni-Drift 2026-08-29).

Befund: „Giorgia Meloni ist Italiens Ministerpraesidentin." lieferte live
`mostly_false@0.85`, obwohl die Summary den Claim BESTAETIGTE. Log:

    STRUKTURELL FALSCH override: LLM returned 'true' @ 0.9 despite
    STRUKTURELL FALSCH marker in sources (ratio 3/9 = 33%).
    Enforcing 'mostly_false' @ 0.85.

Das LLM lag also richtig; der L2-Override kippte es.

Wurzel (live gegen WDQS reproduziert): Der Claim trifft DREI Stems —
'ministerpräsident', 'minister' und 'präsident', weil alle drei Substrings
des Wortes „Ministerpraesidentin" sind. `_row_position_matches_stems`
verodert sie. Damit passierte auch die Row 'Italian minister of Tourism'
(Melonis beendetes 8-Tage-Interims-Ministerium, Ende 03.04.2026) den
Filter — und brachte ihren STRUKTURELL-FALSCH-Marker mit. Von 5 gelieferten
Rows waren DREI dieses beendete Ministerium.

Warum die Meloni-Regel (PR #88) nicht griff: ihr Gate ist
`not _claim_position_stems(claim)` — sie ist nur fuer Claims OHNE
Amts-Substantiv gebaut („X regiert noch"). Hier NENNT der Claim ein Amt,
also schaltete sie sich ab. Der Fall faellt exakt zwischen die beiden
Guards. Das ist die dokumentierte Ueberanpassung: ein Muster wird in der
Formulierung gebaut, in der es entdeckt wurde, und faellt in der anderen um.

Fix: `_most_specific_stems` verwirft jeden Stem, der echter Substring eines
anderen getroffenen Stems ist. Der Claim nennt das spezifischste Amt;
danach ist zu filtern. Faellt dabei alles weg, greift der bisherige breite
Filter als Fallback — kein Claim verliert seine Quelle.

Dependency-light: reine Mengen-/String-Logik, kein Netz, kein Modell.
"""

import pytest

from services.wikidata import (
    _claim_position_stems,
    _most_specific_stems,
    _row_position_matches_stems,
)


def _row(pos_label):
    return {"positionLabel": {"value": pos_label}}


# Genau die Labels, die WDQS am 2026-08-29 fuer Meloni lieferte.
MELONI_ROWS = [
    _row("Italian minister of Tourism"),
    _row("Italian minister of Tourism"),
    _row("Italian minister of Tourism"),
    _row("Italienischer Ministerpräsident"),
    _row("Italienischer Ministerpräsident"),
]
MELONI_CLAIM = "Giorgia Meloni ist Italiens Ministerpräsidentin."


# ---------------------------------------------------------------------------
# Der Kern: Substring-Stems werden verworfen
# ---------------------------------------------------------------------------

def test_ministerpraesident_schlaegt_minister_und_praesident():
    stems = _claim_position_stems(MELONI_CLAIM)
    assert stems == {"minister", "ministerpräsident", "präsident"}, \
        "Vorbedingung: der Claim trifft alle drei Stems"
    assert _most_specific_stems(stems) == {"ministerpräsident"}


def test_einzelner_stem_bleibt_unveraendert():
    """Trump-Kontrolle: 'Präsident der USA' trifft nur einen Stem — die
    Verengung darf dort nichts tun."""
    stems = _claim_position_stems("Donald Trump ist Präsident der USA.")
    assert stems == {"präsident"}
    assert _most_specific_stems(stems) == {"präsident"}


def test_disjunkte_stems_bleiben_beide():
    """Nur echte Substrings fliegen raus, nicht bloss verwandte Begriffe."""
    assert _most_specific_stems({"kanzler", "gouverneur"}) == {"kanzler", "gouverneur"}


def test_leere_menge_ist_stabil():
    assert _most_specific_stems(set()) == set()


# ---------------------------------------------------------------------------
# Wirkung auf die echten Meloni-Rows
# ---------------------------------------------------------------------------

def test_breite_stems_lassen_das_beendete_ministerium_durch():
    """Der Defekt, schriftlich: mit der ODER-Menge passiert das
    Tourismus-Ministerium den Filter."""
    stems = _claim_position_stems(MELONI_CLAIM)
    durch = [r for r in MELONI_ROWS if _row_position_matches_stems(r, stems)]
    assert len(durch) == 5
    assert any("Tourism" in r["positionLabel"]["value"] for r in durch)


def test_enge_stems_halten_das_beendete_ministerium_raus():
    eng = _most_specific_stems(_claim_position_stems(MELONI_CLAIM))
    durch = [r for r in MELONI_ROWS if _row_position_matches_stems(r, eng)]
    assert len(durch) == 2
    assert all("Ministerpräsident" in r["positionLabel"]["value"] for r in durch)
    assert not any("Tourism" in r["positionLabel"]["value"] for r in durch)


# ---------------------------------------------------------------------------
# Gegenrichtung: der Fallback darf keine Quelle verstummen lassen
# ---------------------------------------------------------------------------

def test_fallback_wenn_der_enge_filter_leer_laeuft():
    """Orbán-Konstellation: unter den gelieferten Rows ist gar kein
    Ministerpraesidenten-Amt. Dann darf die Verengung NICHT dazu fuehren,
    dass nichts uebrig bleibt — der breite Filter greift als Fallback,
    genau wie vor dem Fix."""
    rows = [_row("Mitglied der ungarischen Nationalversammlung"),
            _row("Vorsitzender des Fidesz")]
    stems = _claim_position_stems("Viktor Orbán ist Ungarns Ministerpräsident.")
    eng = _most_specific_stems(stems)
    assert [r for r in rows if _row_position_matches_stems(r, eng)] == []
    assert [r for r in rows if _row_position_matches_stems(r, stems)] == []
    # beide leer -> die aufrufende Stelle laesst `rows` unveraendert,
    # das Verhalten ist identisch zum Stand vor dem Fix.


def test_ministerclaim_ohne_spezifischeres_amt_bleibt_breit():
    """Wer ausdruecklich nach einem Ministerium fragt, soll es auch
    bekommen — hier darf die Verengung nichts wegnehmen."""
    stems = _claim_position_stems("Wer ist Tourismusminister in Italien?")
    assert "minister" in stems
    eng = _most_specific_stems(stems)
    assert "minister" in eng
    rows = [_row("Italian minister of Tourism")]
    assert [r for r in rows if _row_position_matches_stems(r, eng)] == rows


@pytest.mark.parametrize("claim,erwartet", [
    ("Karl Nehammer ist Bundeskanzler.", {"bundeskanzler"}),
    ("Wer ist Kanzler in Österreich?", {"kanzler"}),
    ("Ist er noch Ministerpräsident?", {"ministerpräsident"}),
])
def test_weitere_verengungen(claim, erwartet):
    assert _most_specific_stems(_claim_position_stems(claim)) == erwartet
