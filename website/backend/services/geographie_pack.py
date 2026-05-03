"""Geographie-/Reise-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
geografischen + Reise-Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- tier_natur_pack.py (Eskimo-Schnee-Wörter dort, NICHT hier — Überlappung
  vermeiden)
- esoterik_pack.py (Pseudo-Heilung)

Topics (7):
  - bermudadreieck_mythos (Lloyd's of London + USCG: nicht statistisch
    auffällig; Kusche 1975 Dekonstruktion)
  - chinesische_mauer_vom_mond_mythos (NASA + Yang Liwei 2003 + Chiao
    2004: nicht mit bloßem Auge)
  - everest_hoechster_berg_mythos (TRUE-mit-Kontext: nach Definition
    unterschiedlich — Mauna Kea + Chimborazo)
  - toilette_coriolis_mythos (Coriolis im Waschbecken-Maßstab vernach-
    lässigbar)
  - sahara_groesste_wueste_mythos (FALSE: Antarktis größere Wüste nach
    Niederschlags-Definition)
  - australien_giftigste_tiere_mythos (TRUE-mit-Kontext: viele giftig,
    aber Tier-Tote in USA + Indien höher)
  - vatikan_kleinster_staat_konsens (TRUE: 0,49 km², kleinster Staat)

Quellen-Mix:
  - NASA für Astronauten-Beobachtungen (Mauer)
  - Lloyd's of London + US Coast Guard (Bermuda)
  - Britannica + National Geographic (Definitionen)
  - CIA World Factbook (Vatikan-Daten)
  - UNESCO (Wüsten-Definition)
  - WHO (Schlangenbiss-Statistiken)
  - peer-reviewed Studien (Trefethen MIT 1962 Coriolis)

Politische Sensibilität: niedrig. Pack ist primär unterhaltsam-bildend.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "geographie_pack.json",
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


def claim_mentions_geographie_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_geographie(client=None):
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


async def search_geographie(analysis: dict) -> dict:
    empty = {
        "source": "Geographie-/Reise-Mythen (NASA + Lloyd's + USCG + NatGeo + CIA Factbook + UNESCO)",
        "type": "geography_consensus",
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
        label = fact.get("source_label", "NASA / Lloyd's / USCG / NatGeo / CIA Factbook / UNESCO")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "geographie_konsens_fact",
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
        "source": "Geographie-/Reise-Mythen (NASA + Lloyd's + USCG + NatGeo + CIA Factbook + UNESCO)",
        "type": "geography_consensus",
        "results": results,
    }
