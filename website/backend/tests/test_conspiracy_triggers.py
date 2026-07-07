"""Golden-Tests für Schattenregierungs-Paraphrasen + Antisemitismus-Konfidenz
(Verdict-Feedback-Audit 2026-07-07).

Befund 1 (Schattenregierung): Der Claim „Es gibt eine Regierung die im
Hintergrund die Fäden zieht" feuerte den vorhandenen deep_state-Fakt NICHT
(dessen Trigger kannten nur „Schattenregierung/Deep State", nicht die
deutsche „Fäden ziehen"-Paraphrase) → „nicht überprüfbar" statt größtenteils
falsch. Neue trigger_all-Regel: Macht-Subjekt + Verdeckt-Marker + Steuer-Verb
— eng genug, um „nach der OP die Fäden ziehen" NICHT zu treffen.

Befund 2 (Antisemitismus): „Juden sind schuld an der Weltverschwörung" lief
korrekt auf false, aber nur @ 0.7 — dem kernsatz fehlte eine explizite
Konfidenz-Direktive. Jetzt: false @ 0.9, keine falsche Ausgewogenheit.

Dependency-light: liest nur die Pack-JSONs + Trigger-Helper, kein Netz/LLM.
"""

import json
import os

import pytest

from services._topic_match import substring_or_composite_match

_DATA = os.path.join(os.path.dirname(__file__), "..", "data")


def _fact(pack: str, fact_id: str) -> dict:
    d = json.load(open(os.path.join(_DATA, pack), encoding="utf-8"))
    facts = d.get("facts", d if isinstance(d, list) else [])
    for f in facts:
        if f.get("id") == fact_id:
            return f
    raise AssertionError(f"Fakt {fact_id} nicht in {pack}")


_DEEP_STATE = _fact("verschwoerungen_pack.json", "deep_state_konspiration_2026")
_ANTISEM = _fact("religionsgemeinschaften_pack.json", "antisemitismus_verschwoerung_2026")


@pytest.mark.parametrize("claim", [
    "Es gibt eine Regierung die im Hintergrund die Fäden zieht",
    "Eine geheime Elite steuert im Verborgenen die Politik",
    "Mächtige Hintermänner lenken im Geheimen die Regierung",
    "Es gibt einen Deep State",
    "Eine Schattenregierung zieht die Fäden",
])
def test_shadow_government_paraphrases_fire(claim):
    assert substring_or_composite_match(_DEEP_STATE, claim.lower()), \
        f"deep_state-Fakt sollte feuern: {claim}"


@pytest.mark.parametrize("claim", [
    "Nach der Operation müssen die Fäden gezogen werden",
    "Der Schneider zieht den Faden durch die Nadel",
    "Die Regierung arbeitet im Hintergrund an einem neuen Gesetz",
    "Im Hintergrund läuft leise Musik",
])
def test_shadow_government_false_positives_dont_fire(claim):
    assert not substring_or_composite_match(_DEEP_STATE, claim.lower()), \
        f"deep_state-Fakt darf NICHT feuern (kein Verschwörungs-Claim): {claim}"


def test_antisemitism_kernsatz_has_high_confidence_directive():
    ks = _ANTISEM["data"]["kernsatz_fuer_synthesizer"]
    assert "false bei 0.9" in ks, "explizite false@0.9-Direktive fehlt"
    assert "Protokolle der Weisen" in ks, "historische Fälschungs-Einordnung fehlt"
    assert "teils-teils" in ks.lower(), "Warnung gegen falsche Ausgewogenheit fehlt"


def test_antisemitism_world_conspiracy_claim_still_fires():
    # trigger_composite: (juden|…) UND (…weltverschwör…|verschwör|…)
    assert substring_or_composite_match(
        _ANTISEM, "juden sind schuld an der weltverschwörung"), \
        "Antisemitismus-Fakt muss auf den Weltverschwörungs-Claim feuern"
