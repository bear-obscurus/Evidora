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

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "at_courts.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "rulings" not in data:
        logger.warning("at_courts.json missing 'rulings' key")
        return None
    return data


def _ruling_matches(ruling: dict, claim_lc: str) -> bool:
    for kw in ruling.get("trigger_keywords") or ():
        if kw.lower() in claim_lc:
            return True
    composite = ruling.get("trigger_composite") or []
    if composite and all(
        isinstance(alt, (list, tuple)) and any(tok in claim_lc for tok in alt)
        for alt in composite
    ):
        return True
    return False


def _claim_matches_rulings(claim_lc: str) -> list[dict]:
    data = _load_static_json()
    if not data:
        return []
    return [r for r in data.get("rulings") or [] if _ruling_matches(r, claim_lc)]


def claim_mentions_at_courts_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_rulings(claim.lower()))


async def fetch_at_courts(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("rulings") or []


async def search_at_courts(analysis: dict) -> dict:
    empty = {
        "source": "VfGH + VwGH Schlüsselerkenntnisse",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_rulings(claim.lower())
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
