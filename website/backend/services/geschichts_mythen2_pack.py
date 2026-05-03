"""Geschichts-Mythen-2-Pack — kuratierte Konsens-Daten zu klassischen
Schul-/Bildungs-Geschichts-Halbwahrheiten (Wissenschafts-Geschichte +
Religions-/Geistes-Geschichte).

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- geschichts_pack.py (politische / militärische Mythen, NS-Zeit, Antike-
  Politik) — DAS ist Pack v1
- kunst_kultur_pack.py (Pyramiden-Sklaven, Mozart-Armer-Tod, etc.)
- verschwoerungen_pack.py (Mond-Landung, JFK)

Topics (8):
  - buddha_hotei_mythos (FALSE — Verwechslung Siddhartha Gautama vs.
    Hotei/Budai 10. Jh. China)
  - mittelalter_dunkel_mythos (FALSE — Aufklärungs-Erfindung;
    Hexenverfolgung 1580-1640 = Frühe Neuzeit)
  - galileo_folter_mythos (FALSE — Hausarrest, keine Folter,
    'E pur si muove' apokryph)
  - newton_apfel_mythos (FALSE — Apfel fiel NICHT auf Kopf;
    Stukeley 1726/1752)
  - marie_antoinette_kuchen_mythos (FALSE — Rousseau Confessions
    Buch VI, geschrieben 1765-67 als M.A. 9-12 J.)
  - einstein_schulversager_mythos (FALSE — Schweizer Notensystem-
    Verwechslung 6 = sehr gut)
  - edison_gluehbirne_mythos (FALSE/MOSTLY_FALSE — Joseph Swan UK 1878
    + Heinrich Goebel 1854 + 20 weitere Vorgänger)
  - napoleon_sphinx_nase_mythos (FALSE — Frederic Norden 1755
    Zeichnungen zeigen bereits keine Nase, 43 Jahre vor Napoleon)

Quellen-Mix: Stanford Encyclopedia of Philosophy, Britannica, Smithsonian
Magazine + National Museum of American History, Royal Society Newton
Project, Princeton Einstein Papers Project, Antonia Fraser 'Marie
Antoinette: The Journey', Walter Isaacson 'Einstein', Maurice
Finocchiaro 'The Galileo Affair', Robert Friedel 'Edison's Electric
Light', Wolfgang Behringer + Brian Levack (Hexen-Forschung), Mark
Lehner Harvard (Ägyptologie), Vatican Archives 1992 Galileo-Reha.

Politische Sensibilität: niedrig. Hohe Lehrer-Relevanz für Geschichts-
Lehrplan AT/DE (Wissenschafts-Geschichte, Aufklärung, Geistes-
Geschichte) — Schul-Mythen-Korrektur.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "geschichts_mythen2_pack.json",
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


def claim_mentions_geschichts_mythen2_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_geschichts_mythen2(client=None):
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


async def search_geschichts_mythen2(analysis: dict) -> dict:
    empty = {
        "source": "Geschichts-Mythen-2 (Stanford SEP + Britannica + Smithsonian + Royal Society + Princeton)",
        "type": "history2_consensus",
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
        label = fact.get("source_label", "Stanford SEP / Britannica / Smithsonian / Royal Society")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "geschichts_mythen2_konsens_fact",
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
        "source": "Geschichts-Mythen-2 (Stanford SEP + Britannica + Smithsonian + Royal Society + Princeton)",
        "type": "history2_consensus",
        "results": results,
    }
