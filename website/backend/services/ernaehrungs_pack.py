"""Ernährungs-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
Ernährungs- und Lebensmittel-Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- gesundheits_autoritaeten_pack.py (NIH/CDC/BfR-Authority-Konsens für
  spezifische Lebensmittel-Risiken — Acrylamid, Mikroplastik, Glyphosat,
  Aspartam, BPA, rotes Fleisch)
- esoterik_pack.py (Pseudo-Heilung)

Während gesundheits_autoritaeten_pack die HOCH-AUTORITATIVEN Behörden-
Stellungnahmen zu spezifischen Stoff-Risiken liefert, fokussiert
ernaehrungs_pack auf populäre Ernährungs-MYTHEN — meistens harmlose
'Hausmythen' wie 5-Sekunden-Regel, Spinat-Eisen-Mythos, Eier-
Cholesterin-Übertreibung, Detox-Säfte, Mikrowellen-Schaden.

Quellen-Mix:
  - DGE (Deutsche Gesellschaft für Ernährung)
  - Cochrane (PubMed) für Meta-Analysen
  - Mayo Clinic + Harvard T.H. Chan School of Public Health
  - NHS (UK) für Detox-Position
  - BfR + AGES für Lebensmittel-Sicherheit
  - EFSA für EU-Lebensmittel-Sicherheits-Position
  - peer-reviewed Studien (Killer 2014 Kaffee-Hydration, Pinnock 1990
    Milch-Schleim, Miranda & Schaffner 2016 5-Sek-Regel)

Topics:
  - fuenf_sekunden_regel_mythos
  - spinat_eisen_mythos
  - eier_cholesterin_mythos (MIXED — spiegelt aktuelle Meta-Analysen-
    Uneinigkeit)
  - karotten_sehkraft_mythos (TRUE-mit-Kontext, RAF-Propaganda 1940)
  - detox_saefte_mythos
  - mikrowelle_naehrstoffe_mythos
  - brauner_zucker_mythos
  - bio_pestizidfrei_mythos (FALSE — Bio erlaubt 'natürliche' Pestizide)
  - kaffee_dehydration_mythos
  - milch_schleim_mythos

Politische Sensibilität: niedrig. Pack ist methodisch unproblematisch,
deckt populäre Halbwahrheiten ab, die täglich relevant sind.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "ernaehrungs_pack.json",
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


def claim_mentions_ernaehrung_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_ernaehrung(client=None):
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


async def search_ernaehrung(analysis: dict) -> dict:
    empty = {
        "source": "Ernährungs-Mythen (DGE + Cochrane + Mayo + Harvard Chan + NHS + EFSA)",
        "type": "nutrition_consensus",
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
        label = fact.get("source_label", "DGE / Cochrane / Mayo / Harvard / NHS / EFSA")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "ernaehrung_konsens_fact",
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
        "source": "Ernährungs-Mythen (DGE + Cochrane + Mayo + Harvard Chan + NHS + EFSA)",
        "type": "nutrition_consensus",
        "results": results,
    }
