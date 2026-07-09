"""Tests für den 🔵-Opus-kuratierten Holocaust-Leugnungs-Fakt
(50-Claim-Test 2026-07-09, Befund #19).

Befund: „Der Holocaust hat so nie stattgefunden" → false@0.7. Label richtig,
aber 0.7 inakzeptabel niedrig für den bestbelegten Genozid; „Konsens"-Framing
zu weich; Verbotsgesetz-Einordnung fehlte. Es gab KEINEN Leugnungs-Fakt im
Pack (nur holocaust_wissen_2026 = anderes Thema, verlangt Wissens-Wörter).

Neuer Fakt: VERDICT-DIREKTIVE false@0.95 + unabhängige Beweisstränge +
§ 3h VerbotsG (deskriptiv) + Differenzierungs-Klausel für legitime
Opferzahl-Präzisierung. Trigger von Opus validiert (Leugnung feuert,
legitime Geschichtswissenschaft nicht).
"""

import json
import os

import pytest

from services._struct_marker import has_false_verdict_override
from services._topic_match import substring_or_composite_match

_DATA = os.path.join(os.path.dirname(__file__), "..", "data", "geschichte_pack.json")


def _fact(fid):
    d = json.load(open(_DATA, encoding="utf-8"))
    for f in d["facts"]:
        if f["id"] == fid:
            return f
    raise AssertionError(f"{fid} fehlt in geschichte_pack.json")


_DENIAL = _fact("holocaust_leugnung_2026")
_WISSEN = _fact("holocaust_wissen_2026")


@pytest.mark.parametrize("claim", [
    "Der Holocaust hat so nie stattgefunden.",
    "Der Holocaust ist erfunden",
    "Die Auschwitz-Lüge ist bewiesen",
    "Es gab keine Gaskammern",
    "Die 6 Millionen sind eine Lüge",
    "Der Holocaust ist maßlos übertrieben",
    "Die Zahlen zum Holocaust sind reine Propaganda",
    "In Wahrheit starben viel weniger als sechs Millionen Juden",
    "Treblinka ist ein Schwindel",
    "Die Schoah hat nie stattgefunden",
])
def test_denial_phrasings_fire(claim):
    assert substring_or_composite_match(_DENIAL, claim.lower()), claim


@pytest.mark.parametrize("claim", [
    # Legitime historische Aussagen/Fragen dürfen NICHT als Leugnung matchen
    "Wie viele Menschen starben im Holocaust?",
    "Die frühere sowjetische Auschwitz-Zahl von 4 Millionen wurde auf etwa 1,1 Millionen korrigiert",
    "Der Holocaust wird von manchen Gruppen geleugnet",
    "Im Holocaust wurden sechs Millionen Juden ermordet",
    "Das Wannsee-Protokoll dokumentiert die Planung des Völkermords",
])
def test_legitimate_history_does_not_fire(claim):
    assert not substring_or_composite_match(_DENIAL, claim.lower()), claim


def test_denial_claim_does_not_hit_wissen_fact():
    """Der Leugnungs-Claim darf nicht auf den (thematisch anderen)
    holocaust_wissen-Fakt laufen — der behandelt 'Deutsche wussten nichts'."""
    assert not substring_or_composite_match(
        _WISSEN, "der holocaust hat so nie stattgefunden")


def test_kernsatz_directive_complete():
    ks = _DENIAL["data"]["kernsatz_fuer_synthesizer"]
    assert "false bei 0.95" in ks
    assert "KEINE falsche Ausgewogenheit" in ks
    assert "§ 3h Verbotsgesetz" in ks                 # AT-Rechtshinweis
    assert "§ 130" in ks                              # DE-Pendant
    assert "DIFFERENZIERUNG" in ks                    # Schutz legitimer Forschung
    assert "1,1 Millionen" in ks                      # Auschwitz-Forschungsstand
    # kein weiches Konsens-Framing als alleinige Begründung
    assert "unabhängige beweisstränge" in ks.lower().replace("voneinander ", "")


def test_kernsatz_activates_struct_override():
    """Der kernsatz muss den STRUKTURELL-FALSCH-Override aktivieren
    (deterministischer Pfad zusätzlich zur In-Text-Direktive)."""
    assert has_false_verdict_override(_DENIAL["data"]["kernsatz_fuer_synthesizer"])


def test_sources_are_verified_urls():
    assert "ushmm.org" in _DENIAL["source_url"]
    assert "ris.bka.gv.at" in _DENIAL["secondary_url"]   # Verbotsgesetz im RIS
