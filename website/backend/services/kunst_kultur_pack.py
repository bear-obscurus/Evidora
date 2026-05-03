"""Kunst-/Kultur-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
Geschichts- und Kultur-Halbwahrheiten in Schul-/Bildungs-Kontext.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- geschichts_pack.py (politische / militärische Mythen, NS, Antike-Politik)
- verschwoerungen_pack.py (Mond-Landung, JFK etc.)

Topics (8):
  - pyramiden_sklaven_mythos (Mark Lehner Harvard, FALSE)
  - mozart_armer_tod_mythos (Otto Erich Deutsch, FALSE)
  - napoleon_klein_mythos (Autopsie 1821, FALSE)
  - wikinger_hoerner_mythos (Gjermundbu-Helm, FALSE/Wagner-Doepler)
  - kolumbus_erde_rund_mythos (Eratosthenes, FALSE/Stephen Jay Gould)
  - salem_hexen_konsens (Univ. Virginia Archive, TRUE-mit-Zahlen)
  - great_wall_china_alter_konsens (UNESCO, MIXED — Ming primär)
  - shakespeare_autorenschaft_konsens (Folger Library + Shapiro, TRUE)

Quellen-Mix: Smithsonian, Mark Lehner (Harvard/AERA), Otto Erich Deutsch
(Mozart documentary biography), Roderick Floud (Anthropometric History),
Else Roesdahl (Wikinger-Forschung), Stephen Jay Gould ('The Late
Birth of a Flat Earth'), University of Virginia Salem Witch Trials
Archive, UNESCO World Heritage, Folger Shakespeare Library, James
Shapiro 'Contested Will' (Columbia Univ.).

Politische Sensibilität: niedrig. Hohe Lehrer-Relevanz (Geschichts-
Lehrplan AT/DE), Schul-Mythen-Korrektur.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "kunst_kultur_pack.json",
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


def claim_mentions_kunst_kultur_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_kunst_kultur(client=None):
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


async def search_kunst_kultur(analysis: dict) -> dict:
    empty = {
        "source": "Kunst-/Kultur-Mythen (Smithsonian + Harvard + Folger + UNESCO)",
        "type": "culture_consensus",
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
        label = fact.get("source_label", "Smithsonian / Harvard / Folger / UNESCO")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "kunst_kultur_konsens_fact",
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
        "source": "Kunst-/Kultur-Mythen (Smithsonian + Harvard + Folger + UNESCO)",
        "type": "culture_consensus",
        "results": results,
    }
