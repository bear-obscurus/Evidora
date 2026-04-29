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

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "eu_courts.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "rulings" not in data:
        logger.warning("eu_courts.json missing 'rulings' key")
        return None
    return data


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _ruling_matches(ruling: dict, claim_lc: str) -> bool:
    """Match if ANY of:
      - any substring in `trigger_keywords` is present, OR
      - all alternation-lists in `trigger_composite` fire
        (interpretation: trigger_composite is ONE rule, each element
         is an OR-list of synonyms; all elements must fire (AND)).
    """
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


def claim_mentions_eu_courts_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_rulings(claim.lower()))


# ---------------------------------------------------------------------------
# Prefetch (no-op — static JSON, but uniform interface)
# ---------------------------------------------------------------------------
async def fetch_eu_courts(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("rulings") or []


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------
async def search_eu_courts(analysis: dict) -> dict:
    empty = {
        "source": "EuGH + EGMR Schlüsselurteile",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    cl = claim.lower()
    matches = _claim_matches_rulings(cl)
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
