"""Verschwoerungstheorien-Pack — kuratierte Konsens-Daten zu zeitgenoessischen
Verschwoerungs-Narrativen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- esoterik_pack.py (Pseudowissenschaft: Heilsteine, Astrologie, Reiki, ...)
- geschichte_pack.py (historische Mythen inkl. klassischer Verschwoerungs-
  Geschichte wie Mondlandung, 9/11, AIDS-CIA)

Dieser Pack fokussiert auf **zeitgenoessische / aktuelle** Verschwoerungs-
theorien, zu denen behoerdliche oder wissenschaftliche Konsens-Quellen
existieren (Verfassungsschutz, BVerfG, ADL, IKG Wien, DOEW, IHRA).

Politische Guardrails (project_political_guardrails.md):
- Wir bewerten spezifische faktische Aussagen gegen den dokumentierten
  Konsens.
- Wir machen KEINE eigenen Einstufungen — wir zitieren Einstufungen, die
  bereits behoerdlich oder akademisch dokumentiert sind.
- Wir bewerten weder Personen noch politische Bewegungen pauschal.
- Bei besonders heiklen Themen (Soros: Antisemitismus-Kontext;
  Reichsbuerger: Verfassungsschutz-Einstufung) explizite Distanzierungs-
  Klauseln im Topic-Inhalt.

Aktueller Stand (Probe, 2026-05-02): 2 Topics
- reichsbuerger_brd_gmbh_mythos: BVerfG + 2+4-Vertrag + BfV (DE) + DSN (AT)
- soros_eu_steuerung_mythos: ADL + IKG Wien + DOEW + BfV + IHRA + EUvsDisinfo

Wenn der Probe-Pack im Live-Test methodisch sauber funktioniert, kann er
um weitere zeitgenoessische Verschwoerungs-Themen erweitert werden:
QAnon, Great Reset, Chemtrails, 5G-Krebs, Bill-Gates-Microchips,
Plandemic-Corona, Bilderberger.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "verschwoerungen_pack.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    """Descriptor for the cosine-similarity backup trigger."""
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_verschwoerungen_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_verschwoerungen(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Concat all string-valued data fields except 'context' (kept separate
    in description) into a compact display value."""
    parts: list[str] = []
    for key, val in d.items():
        if key == "context":
            continue
        if isinstance(val, str) and val.strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)


async def search_verschwoerungen(analysis: dict) -> dict:
    empty = {
        "source": "Verschwoerungen-Faktencheck (BVerfG + Verfassungsschutz + ADL/IKG/DÖW + IHRA)",
        "type": "consensus_fact_check",
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
        label = fact.get("source_label", "BVerfG / Verfassungsschutz / ADL / IKG / DÖW / IHRA")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "verschwoerungen_konsens_fact",
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
        "source": "Verschwoerungen-Faktencheck (BVerfG + Verfassungsschutz + ADL/IKG/DÖW + IHRA)",
        "type": "consensus_fact_check",
        "results": results,
    }
