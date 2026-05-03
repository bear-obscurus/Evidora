"""Internationale Quellen — Pew Research + WMO + IMF + WTO.

Static-First-Aggregate-Service für Statements aus den vier wichtigsten
internationalen Daten-Quellen, die KEIN offen zugängliches API für die
spezifischen Aggregat-Statements bieten.

Komplementaer zu:
- eurobarometer (EU-Bürger-Stimmung)
- ecb (EU-Geldpolitik Live-API)
- worldbank (Live-API für Länder-Indikatoren)
- imf (...neue Quelle, nicht zu verwechseln)

Topics (4):
  - pew_demokratie_zufriedenheit (Pew Global Democracy Survey 2024)
  - wmo_klimazustand (WMO State of the Global Climate 2024)
  - imf_weltwirtschaft_outlook (IMF World Economic Outlook 2024)
  - wto_welthandel_konsens (WTO Trade Forecast + Statistical Review 2024)

Quellen-Mix: Pew Research Center, World Meteorological Organization,
International Monetary Fund, World Trade Organization. Alle vier sind
internationale + UN-/multilaterale Institutionen mit anerkannter
methodischer Konsistenz.

Politische Sensibilität: niedrig-mittel. Themen sind politisch relevant
(Demokratie, Klima, Wirtschaft, Handel), aber Daten + Methodik sind
maßgebend.

Erweitert das Quellen-Inventar um globale (nicht-EU-zentrierte)
Perspektive.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "internationale_quellen.json",
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


def claim_mentions_internationale_quellen_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_internationale_quellen(client=None):
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


async def search_internationale_quellen(analysis: dict) -> dict:
    empty = {
        "source": "Internationale Quellen (Pew + WMO + IMF + WTO)",
        "type": "international_organisations",
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
        label = fact.get("source_label", "Pew / WMO / IMF / WTO")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "internationale_quellen_fact",
            "country": "GLOBAL",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Internationale Quellen (Pew + WMO + IMF + WTO)",
        "type": "international_organisations",
        "results": results,
    }
