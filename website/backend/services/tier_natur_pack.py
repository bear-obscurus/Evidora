"""Tier-/Natur-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
Tier- und Natur-Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- esoterik_pack.py (Pseudowissenschaft)
- geschichte_pack.py (historische Mythen)
- verschwoerungen_pack.py (Verschwörungs-Narrative)
- tech_ki_pack.py (Tech-/KI-Mythen)
- gesundheits_autoritaeten_pack.py (Gesundheits-Authority-Konsens)

Dieser Pack fokussiert auf populäre Tier- und Natur-Mythen, wo PubMed/
Wissenschafts-DBs zwar Studien liefern, aber kuratierte authoritative
Anker (Smithsonian, AMNH, Britannica, Snopes, IUCN) die Verdict-
Konsistenz bei populär-falschen Aussagen ('Hai-Angriffe sind häufig',
'Eskimos haben 100 Wörter für Schnee', 'Lemming-Massensuizid') deutlich
verbessern.

Quellen-Mix:
  - Smithsonian National Zoo + Smithsonian Magazine
  - AMNH (American Museum of Natural History)
  - Britannica + Britannica Mythbusters
  - Snopes (Hoax-Verifikation)
  - International Shark Attack File (Florida Museum)
  - Alaska Native Language Center (ANLC)
  - Alaska Department of Fish & Game
  - Burke Museum (Univ. Washington) — Spider Myths
  - Forest Preserve District of Will County
  - VCA Animal Hospitals (veterinär-medizinisch)
  - peer-reviewed Studien (Neitz 1989 Hunde-Farben, Pullum 1991 Eskimo-
    Hoax, etc.)

Topics:
  - goldfish_gedaechtnis_mythos (3-Sekunden-Memory FALSE)
  - stier_rote_farbe_mythos (Stier reagiert auf Rot FALSE)
  - fledermaus_blind_mythos (Fledermaus blind FALSE)
  - hund_schwarz_weiss_mythos (Hund sieht s/w FALSE)
  - spinnen_im_schlaf_mythos (8 Spinnen/Jahr Hoax 1993)
  - lemming_massensuizid_mythos (Disney-Faked-Footage 1958)
  - hai_angriff_haeufigkeit_mythos (extrem selten)
  - eskimo_schnee_woerter_mythos (Boas-/Whorf-Hoax)
  - strauss_kopf_sand_mythos (Plinius-Mythos)
  - elefant_maus_mythos (allgemeine Vorsicht, keine Phobie)

Politische Sensibilität: niedrig. Pack ist kompatibel mit allen
politischen Positionen — biologische / linguistische Fakten.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "tier_natur_pack.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_tier_natur_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_tier_natur(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    parts: list[str] = []
    for key, val in d.items():
        if key == "context":
            continue
        if isinstance(val, str) and val.strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)


async def search_tier_natur(analysis: dict) -> dict:
    empty = {
        "source": "Tier-/Natur-Mythen (Smithsonian + AMNH + Britannica + Snopes)",
        "type": "natural_history_consensus",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_facts(claim.lower(), full_claim=claim)
    if not matches:
        return empty

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        url = fact.get("source_url", "")
        secondary = fact.get("secondary_url", "")
        label = fact.get("source_label", "Smithsonian / AMNH / Britannica / Snopes")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "tier_natur_konsens_fact",
            "country": "—",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Tier-/Natur-Mythen (Smithsonian + AMNH + Britannica + Snopes)",
        "type": "natural_history_consensus",
        "results": results,
    }
