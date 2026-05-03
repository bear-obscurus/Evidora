"""Sport-/Fitness-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
Trainings- und Fitness-Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- esoterik_pack.py (Schwitzen-Detox dort, NICHT hier — Überlappung
  vermieden)
- ernaehrungs_pack.py (Lebensmittel-Mythen, nicht Sport-spezifisch)
- gesundheits_autoritaeten_pack.py (Behörden-Stoff-Risiken)

Topics (10):
  - spot_fat_reduction_mythos (gezielte Fett-Verbrennung FALSE)
  - muscle_confusion_mythos (P90X-Marketing FALSE, Progressive Overload)
  - knuckle_cracking_arthritis_mythos (FALSE, Donald Unger 60 J)
  - no_pain_no_gain_mythos (FALSE, Anstrengung ≠ Schmerz)
  - lactic_acid_soreness_mythos (FALSE, DOMS via Mikroriss-Theorie)
  - women_bulky_muscles_mythos (FALSE, Testosteron-Niveau)
  - stretching_injury_mythos (FALSE/MIXED, dynamic warm-up besser)
  - fat_burning_zone_mythos (MIXED, hohe Intensität verbrennt mehr)
  - abs_kitchen_konsens (TRUE-mit-Kontext: Ernährung primär)
  - high_protein_kidneys_mythos (FALSE bei Gesunden)

Quellen-Mix: ACSM (American College of Sports Medicine), Cochrane,
AHA, Mayo Clinic, ISSN (International Society of Sports Nutrition),
NSCA, BJSM (British Journal of Sports Medicine), peer-reviewed Studien
(Schoenfeld 2010 Hypertrophie, Newham 1983 DOMS, Antonio 2017 Protein,
Wewege 2022 HIIT-Meta-Analyse, Cochrane Review Stretching 2007,
Donald Unger 60-Jahre-Selbstexperiment, Roberts 2020 Sex-Dimorphism
in Hypertrophie).

Politische Sensibilität: niedrig. Pack ist primär präventiv-medizinisch,
hohe Lehrer-Relevanz (Sport-Lehrer-Mythen, Fitness-Trainer-Marketing).
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "sport_fitness_pack.json",
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


def claim_mentions_sport_fitness_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_sport_fitness(client=None):
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


async def search_sport_fitness(analysis: dict) -> dict:
    empty = {
        "source": "Sport-/Fitness-Mythen (ACSM + Cochrane + AHA + ISSN + NSCA)",
        "type": "sports_consensus",
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
        label = fact.get("source_label", "ACSM / Cochrane / AHA / ISSN / NSCA")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "sport_fitness_konsens_fact",
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
        "source": "Sport-/Fitness-Mythen (ACSM + Cochrane + AHA + ISSN + NSCA)",
        "type": "sports_consensus",
        "results": results,
    }
