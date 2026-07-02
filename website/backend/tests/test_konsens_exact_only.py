"""Regressions-Netz für den Exakt-Only-Umbau der Multi-Topic-Konsens-Packs
(#41 Cosine-Rauschen, 2026-07-02).

Hintergrund: Diese 11 Packs feuerten via Cosine-Backup (Threshold 0.45 auf
generische Deskriptoren) auf themenfremde Claims — ein SIPRI-Militär-Claim
zog 9+ Packs über geteiltes "Österreich + Ausgaben"-Vokabular. Messung
zeigte: KEINE Cosine-Schwelle trennt (Off-Topic scorte teils HÖHER als
legitime Claims). Fix: Cosine-Backup deaktiviert (descriptor_fn=None),
davor die exakte Trigger-Abdeckung der eigenen claim_phrasings_handled
von 74 % auf 100 % geschlossen (179 trigger_all-Regeln + 13 Keywords +
3 Hand-Regeln).

Diese Tests pinnen beides:
  1. Die Phrasings-Battery bleibt 100 % — wer eine neue Phrasing in ein
     Pack-JSON einträgt, muss auch den Trigger dafür liefern.
  2. Der SIPRI-Referenz-Claim matcht keinen der 11 Packs exakt.
Kein Modell, kein Netzwerk — reine Substring-/Composite-Logik, <1 s.
"""

import importlib
import json

import pytest

from services._topic_match import substring_or_composite_match

KONSENS_PACKS = [
    "sozialstaat_pack", "mobilitaet_pack", "arbeitsmarkt_pack",
    "datenschutz_pack", "landwirtschaft_pack", "welthandel_pack",
    "wohnen_pack", "oeif_zara", "rechnungshof_parteienfin",
    "wirtschaftspolitik_pack", "internationale_quellen",
]

SIPRI_NOISE_CLAIM = (
    "Laut SIPRI-Jahrbuch 2024 liegt Österreich bei den "
    "Pro-Kopf-Rüstungsausgaben über dem NATO-Durchschnitt."
)


def _facts(pack_name):
    mod = importlib.import_module(f"services.{pack_name}")
    with open(mod.STATIC_JSON_PATH, encoding="utf-8") as fh:
        return json.load(fh).get("facts", [])


@pytest.mark.parametrize("pack", KONSENS_PACKS)
def test_phrasings_battery_full_exact_coverage(pack):
    """Jede claim_phrasing des Packs muss EXAKT (Substring/Composite)
    matchen — sonst verliert sie durch den Backup-Disable ihren Treffer."""
    misses = []
    for f in _facts(pack):
        for ph in (f.get("claim_phrasings_handled") or []):
            if not substring_or_composite_match(f, ph.lower()):
                misses.append((f.get("id"), ph))
    assert not misses, (
        f"{pack}: {len(misses)} Phrasings ohne exakten Trigger — "
        f"trigger_keywords/trigger_all ergänzen: {misses[:5]}"
    )


@pytest.mark.parametrize("pack", KONSENS_PACKS)
def test_sipri_noise_claim_matches_nothing(pack):
    """Der #41-Referenz-Claim (Militär/SIPRI) darf keinen Fact der
    Multi-Topic-Packs exakt treffen."""
    cl = SIPRI_NOISE_CLAIM.lower()
    hits = [f.get("id") for f in _facts(pack)
            if substring_or_composite_match(f, cl)]
    assert not hits, f"{pack}: SIPRI-Claim matcht {hits}"


@pytest.mark.parametrize("pack", KONSENS_PACKS)
def test_cosine_backup_disabled(pack):
    """Die Service-Gates dürfen den Cosine-Backup nicht mehr nutzen —
    descriptor_fn=None ist der Kontrakt dieses Umbaus."""
    mod = importlib.import_module(f"services.{pack}")
    src = open(mod.__file__, encoding="utf-8").read()
    assert "descriptor_fn=None" in src, (
        f"{pack}: Cosine-Backup wieder aktiv? descriptor_fn=None fehlt."
    )
    assert "descriptor_fn=_descriptor" not in src
