"""DESTATIS — Statistisches Bundesamt Deutschland — Static-Aggregate-Service
für DE-Baseline-Indikatoren (Bevölkerung, BIP, Inflation, Arbeitslosigkeit,
Geburten, Lebenserwartung).

Komplementaer zu:
- eurostat.py (EU-Live-API, deckt DE über EU-Aggregate ab)
- dach_factbook.py (DACH-spezifische Debatten-Topics)
- statistik_at (AT-Pendant für AT-Indikatoren)

DESTATIS-Service liefert deutschland-spezifische Konsens-Daten mit
deutscher Original-Sprache, exakten Veröffentlichungs-Pressemitteilungen
und nuancierter Lesart (z.B. ILO vs BA-Arbeitslosenquote, real vs nominal
BIP, Kohorten- vs Periode-Geburtenziffer).

Topics:
  - destatis_bevoelkerung
  - destatis_inflation
  - destatis_geburtenrate
  - destatis_arbeitslosigkeit
  - destatis_bip_wachstum
  - destatis_lebenserwartung

Aktualisierung: jährlich, mit Verweis auf DESTATIS-Pressemitteilungen.
Static-First — kein Live-API-Aufruf, hot-reload via mtime-aware-Cache.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "destatis.json",
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


def claim_mentions_destatis_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_destatis(client=None):
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


async def search_destatis(analysis: dict) -> dict:
    empty = {
        "source": "DESTATIS — Statistisches Bundesamt Deutschland",
        "type": "official_data",
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
        label = fact.get("source_label", "DESTATIS — Statistisches Bundesamt")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "destatis_de_fact",
            "country": "DE",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "DESTATIS — Statistisches Bundesamt Deutschland",
        "type": "official_data",
        "results": results,
    }
