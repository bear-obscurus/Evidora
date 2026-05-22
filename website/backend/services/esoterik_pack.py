"""Esoterik / Pseudowissenschaft — kuratierte Skeptiker-Konsens-Daten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.
Deckt Themen ab, zu denen mainstream-medizinische Datenbanken (PubMed,
Cochrane) entweder keine Studien zurückliefern (Heilsteine, Aura) oder
einzelne thematisch unrelevante Studien liefern (Reinkarnation), so dass
der Synthesizer defensiv-konservativ auf ``unverifiable`` schließt.

Quellen-Mix: GWUP-Skeptiker-Konsens, Cochrane-Reviews (wo vorhanden),
NHMRC, BfArM, Stiftung Warentest, klassische Falsifizierungs-Studien
(Carlson 1985 Astrologie, Knipschild 1988 Iridologie, Cordi 2014
Mondphasen, Betz 1990 Wünschelrute).

Topics:
  - heilsteine_kristalle, astrologie_sternzeichen, bach_blueten,
    reiki_energieheilung, aura_diagnose, chakren_anatomie,
    mondphasen_schlaf, wuenschelrute_radiaesthesie,
    geistheilung_fernheilung, schuessler_salze, bioresonanz_therapie,
    hellsehen_wahrsagen, reinkarnation_rueckfuehrung, iris_diagnose,
    wasserbelebung_verwirbelung
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "esoterik_pack.json",
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


def claim_mentions_esoterik_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_esoterik(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py). 16 facts
    haben seit 2026-05-22 kernsätze (Agent-A-Audit).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_esoterik(analysis: dict) -> dict:
    empty = {
        "source": "Esoterik-Faktencheck (GWUP + Cochrane + Skeptiker-Konsens)",
        "type": "skeptic_consensus",
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
        label = fact.get("source_label", "GWUP / Cochrane / Skeptiker-Konsens")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        # Generic display builder: headline + concatenated evidence-fields.
        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        # The 'context' field is the narrative the synthesizer should weight;
        # context_notes carry the methodological caveats.
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            # Single indicator → matched by reranker authoritative-whitelist;
            # the per-topic detail goes into the 'topic' field.
            "indicator": "esoterik_skeptic_fact",
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
        "source": "Esoterik-Faktencheck (GWUP + Cochrane + Skeptiker-Konsens)",
        "type": "skeptic_consensus",
        "results": results,
    }
