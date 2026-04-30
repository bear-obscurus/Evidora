"""EuGH (CURIA) + EGMR (HUDOC) — kuratierte Sammlung der Schlüsselurteile,
die im Boulevard und in FPÖ-nahen Medien regelmäßig verfälscht zitiert werden.

Datenquelle: Static-curated JSON in data/eu_courts.json (15+ Schlüsselurteile,
ergänzbar). CURIA hat keine offizielle JSON-API; HUDOC zwar REST, aber Volltext-
Urteile auf Englisch sind für den Reranker schwer zu matchen. Die meisten
Boulevard-Verfälschungen betreffen ohnehin nur eine kleine Menge wiederkehrender
Schlüsselurteile (Schrems II, NS/Dublin, Transitzonen Ungarn, Push-back Melilla,
Klimaseniorinnen, Lautsi-Kruzifix etc.).

Pattern: Trigger-Match (Substring + Composite), authoritativer Result-Builder
mit "kerninhalt" + "boulevard_falschmeldung" als Counter-Frame, Reranker-
Whitelist via 'eu_courts_ruling' indicator.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "eu_courts.json",
)


def _descriptor(r: dict) -> tuple[dict, str]:
    """Pair a ruling with a kurze Topic-Repräsentation für Cosine-Match."""
    court = r.get("court", "")
    name = r.get("case_name", "")
    kern = r.get("kerninhalt", "")[:240]
    return (r, f"{court}-Urteil: {name}. {kern}")


def _claim_matches_rulings(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "rulings",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_eu_courts_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_rulings(claim.lower(), full_claim=claim))


async def fetch_eu_courts(client=None):
    return load_items(STATIC_JSON_PATH, "rulings")


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------
async def search_eu_courts(analysis: dict) -> dict:
    empty = {
        "source": "EuGH + EGMR Schlüsselurteile",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    cl = claim.lower()
    matches = _claim_matches_rulings(cl, full_claim=claim)
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

        headline = f"{court}-Urteil {case} ({year}) — {name}"
        description_parts = [f"Tatsächlicher Inhalt: {kern}"]
        if falsch:
            description_parts.append(
                "Häufige Boulevard-/Telegram-Verfälschung: „"
                + falsch
                + "“ — diese Lesart wird vom Urteil NICHT gestützt."
            )

        results.append({
            "indicator_name": headline,
            "indicator": "eu_courts_ruling",
            "court": court,
            "case_number": case,
            "year": str(year),
            "celex": r.get("celex", ""),
            "topic": r.get("topic", ""),
            "display_value": headline,
            "description": " ".join(description_parts),
            "url": r.get("url", ""),
            "source": ("Curia EuGH" if court == "EuGH"
                       else "HUDOC EGMR" if court == "EGMR" else court),
        })

    return {
        "source": "EuGH + EGMR Schlüsselurteile",
        "type": "official_data",
        "results": results,
    }
