"""Gesundheits-Autoritäten-Pack — kuratierte Konsens-Daten zu klassischen
Gesundheits-, Lebensmittel- und Pharma-Mythen aus den weltweit
maßgebenden Health-Behörden.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- esoterik_pack.py (Pseudowissenschaft)
- geschichte_pack.py (historische Mythen)
- verschwoerungen_pack.py (Verschwörungs-Narrative)
- tech_ki_pack.py (Tech-/KI-Mythen)

Dieser Pack fokussiert auf Gesundheits-Themen, wo NIH (NIDA, NCI, NCCIH),
CDC und BfR (DE) klare Konsens-Positionen haben — komplementär zu
PubMed/Cochrane/EMA, die für Aussagen ohne klare Behörden-Stellungnahme
weiter zuständig sind.

Quellen-Mix:
  - CDC (Centers for Disease Control, USA) — Vaccine-Safety, Fluoride
  - NIH (National Institutes of Health, USA) — NIDA Cannabis, NCI Cancer,
    NCCIH Supplements
  - BfR (Bundesinstitut für Risikobewertung, DE) — Lebensmittel-Risiken,
    Acrylamid, Mikroplastik, Glyphosat, Aspartam, BPA
  - WHO IARC (Krebsforschungs-Agentur) — bei Bewertungs-Uneinigkeit
    explizit benannt (Glyphosat, Aspartam)
  - EFSA + JECFA — bei EU-Lebensmittel-Themen mitzitiert

Topics:
  - vaccines_autism_konsens (CDC: keine Verbindung)
  - fluoride_trinkwasser_konsens (CDC + ADA + WHO: sicher + wirksam)
  - cannabis_gesundheit_konsens (NIDA: dokumentierte Effekte, kein Gateway)
  - red_meat_cancer_konsens (IARC + NCI: Group 1/2A)
  - acrylamid_lebensmittel_konsens (BfR: Vorsorge-Minimierung)
  - mikroplastik_lebensmittel_konsens (BfR: keine etablierten Schäden, aber Datenlücken)
  - glyphosat_krebs_konsens (MIXED: IARC 2A vs EFSA/BfR/EPA)
  - aspartam_krebs_konsens (MIXED: IARC 2B vs JECFA/BfR/EFSA)
  - bpa_babyflasche_konsens (EU-Verbot 2011, TDI 2023 abgesenkt)
  - vitaminpraeparate_krankheitspraevention_konsens (NIH + USPSTF: kein Routine-Vorteil)

Methodische Disziplin: bei Bewertungs-Uneinigkeit zwischen Behörden
(Glyphosat, Aspartam) wird DIE UNEINIGKEIT selbst als Fakt zitiert, nicht
eine Position als Wahrheit ausgewählt — der Synthesizer soll dem Nutzer
die regulatorische Realität präsentieren.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "gesundheits_autoritaeten_pack.json",
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


def claim_mentions_gesundheits_autoritaeten_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_gesundheits_autoritaeten(client=None):
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


async def search_gesundheits_autoritaeten(analysis: dict) -> dict:
    empty = {
        "source": "Gesundheits-Autoritäten (NIH + CDC + BfR + WHO IARC)",
        "type": "health_authority_consensus",
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
        label = fact.get("source_label", "NIH / CDC / BfR / WHO IARC")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "gesundheits_autoritaeten_fact",
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
        "source": "Gesundheits-Autoritäten (NIH + CDC + BfR + WHO IARC)",
        "type": "health_authority_consensus",
        "results": results,
    }
