"""VfGH (Verfassungsgerichtshof) + VwGH (Verwaltungsgerichtshof) —
kuratierte Sammlung der Schlüsselerkenntnisse, die im Boulevard regelmäßig
verfälscht zitiert werden (Ehe für alle, Sterbehilfe, COVID-Lockdown-
Verordnungen, Impfpflicht, ORF-Finanzierung, Asyl-Drittstaat-Verfahren,
Bundespräsidenten-Wahlaufhebung 2016 etc.).

Datenquelle: Static-curated JSON in data/at_courts.json. Volltexte lägen
im RIS, aber für die populärsten Boulevard-Verfälschungen reicht eine
kuratierte Sammlung mit "kerninhalt" + "boulevard_falschmeldung" als
Counter-Frame — analog zum eu_courts.py-Pattern.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "at_courts.json",
)


def _descriptor(r: dict) -> tuple[dict, str]:
    court = r.get("court", "")
    name = r.get("case_name", "")
    kern = r.get("kerninhalt", "")[:240]
    return (r, f"{court}-Erkenntnis: {name}. {kern}")


def _claim_matches_rulings(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "rulings",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_at_courts_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_rulings(claim.lower(), full_claim=claim))


async def fetch_at_courts(client=None):
    return load_items(STATIC_JSON_PATH, "rulings")


async def search_at_courts(analysis: dict) -> dict:
    empty = {
        "source": "VfGH + VwGH Schlüsselerkenntnisse",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_rulings(claim.lower(), full_claim=claim)
    if not matches:
        return empty

    results: list[dict] = []
    for r in matches:
        court = r.get("court", "?")
        case = r.get("case_number", "")
        name = r.get("case_name", "")
        year = r.get("year", "")
        kern = r.get("kerninhalt", "")
        falsch = r.get("boulevard_falschmeldung", "")

        headline = f"{court}-Erkenntnis {case} ({year}) — {name}"
        description_parts = [f"Tatsächlicher Inhalt: {kern}"]
        if falsch:
            description_parts.append(
                "Häufige Boulevard-/Telegram-Verfälschung: „"
                + falsch
                + "“ — diese Lesart wird vom Erkenntnis NICHT gestützt."
            )

        results.append({
            "indicator_name": headline,
            "indicator": "at_courts_ruling",
            "court": court,
            "case_number": case,
            "year": str(year),
            "topic": r.get("topic", ""),
            "display_value": headline,
            "description": " ".join(description_parts),
            "url": r.get("url", ""),
            "source": ("VfGH (Verfassungsgerichtshof)" if court == "VfGH"
                       else "VwGH (Verwaltungsgerichtshof)" if court == "VwGH"
                       else court),
        })

    return {
        "source": "VfGH + VwGH Schlüsselerkenntnisse",
        "type": "official_data",
        "results": results,
    }
