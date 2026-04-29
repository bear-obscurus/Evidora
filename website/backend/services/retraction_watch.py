"""Retraction Watch — kuratierte Sammlung zurückgezogener wissenschaftlicher
Studien für Faktencheck-Use-Cases.

Datenquelle:
- Static-curated JSON in ``data/retraction_watch.json`` (~5–20 wichtigste
  Retractions wie Wakefield 2010, Surgisphere 2020, Hydroxychloroquin-
  Raoult 2024, Schön-Physik-Skandal etc.)
- Vollständige Datenbank wäre ~50.000 Einträge via Crossref-Labs-API
  (https://api.labs.crossref.org/data/retractionwatch) — bei Bedarf
  später als zweiter Pfad ergänzbar.

Use-Case:
Wenn ein Claim eine wissenschaftliche Studie zitiert ("Wakefield-
Studie", "Lancet 1998", "Hydroxychloroquin wirkt gegen COVID"), prüfen
wir gegen die Liste der zurückgezogenen Studien und liefern den
Retraction-Befund + wissenschaftlichen Konsens.

Architektur folgt at_factbook-Pattern:
- Static-First JSON
- Substring + Composite Trigger
- Topic-spezifische Result-Builder
- Reranker-Whitelist
"""

import json
import logging
import os

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "retraction_watch.json",
)

_cache: dict | None = None


# ---------------------------------------------------------------------------
# Static load
# ---------------------------------------------------------------------------
def _load_static_json() -> dict | None:
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(STATIC_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "retractions" not in data:
            logger.warning("retraction_watch.json missing 'retractions' key")
            return None
        _cache = data
        logger.info(
            f"Retraction Watch loaded: {len(data['retractions'])} curated retractions"
        )
        return _cache
    except FileNotFoundError:
        logger.warning(f"retraction_watch.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"retraction_watch.json load failed: {e}")
        return None


async def fetch_retraction_watch(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("retractions") or []


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_matches_retractions(claim: str) -> list[dict]:
    """Returns list of retraction records that match the claim."""
    if not claim:
        return []
    cl = claim.lower()
    data = _load_static_json()
    if not data:
        return []
    retractions = data.get("retractions") or []
    matches: list[dict] = []
    for r in retractions:
        # Match against topic_keywords (primary), then authors and journal
        keywords = r.get("topic_keywords") or []
        if any(kw.lower() in cl for kw in keywords):
            matches.append(r)
            continue
        # Composite-Match: zwei oder mehr Kern-Begriffe aus topic_keywords
        # einzeln getrennt vorhanden — fängt Phrasings wie "Schokolade hilft
        # beim Abnehmen" (statt "Schokolade abnehmen") ab.
        single_words = set()
        for kw in keywords:
            for w in kw.lower().split():
                if len(w) >= 5:
                    single_words.add(w)
        if len(single_words) >= 2:
            hits = sum(1 for w in single_words if w in cl)
            if hits >= 2:
                matches.append(r)
                continue
        # Match Author-Name + Year
        authors = r.get("authors") or []
        for author in authors:
            # Just the surname for matching
            surname = author.split()[0].lower() if author else ""
            if len(surname) >= 4 and surname in cl:
                # Plus optional year-match
                pub_year = r.get("publication_year")
                if pub_year and str(pub_year) in cl:
                    matches.append(r)
                    break
    return matches


def claim_mentions_retraction_watch_cached(claim: str) -> bool:
    """Synchronous gate for the request hot path."""
    return bool(_claim_matches_retractions(claim))


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _build_retraction_results(matches: list[dict]) -> list[dict]:
    results: list[dict] = []
    for r in matches:
        title = r.get("original_title", "Unbekannte Studie")
        retraction_year = r.get("retraction_year", "?")
        publication_year = r.get("publication_year", "?")
        journal = r.get("journal", "?")
        authors = r.get("authors") or []
        author_str = ", ".join(authors[:3])
        if len(authors) > 3:
            author_str += " et al."
        reason = r.get("retraction_reason", "")
        consensus = r.get("scientific_consensus", "")
        misinformation = r.get("common_misinformation", "")

        headline = (
            f"ZURÜCKGEZOGENE STUDIE (Retraction Watch): "
            f"'{title[:120]}' "
            f"(Autoren: {author_str}, {journal}, publiziert {publication_year}, "
            f"zurückgezogen {retraction_year}). "
            f"Rückzugs-Grund: {reason} "
            f"VERDICT-EMPFEHLUNG: Behauptungen, die auf dieser Studie basieren, "
            f"sind 'false' mit Confidence 0.95."
        )

        description = (
            f"{misinformation} "
            f"Wissenschaftlicher Konsens: {consensus}"
        )

        results.append({
            "indicator_name": f"Retraction Watch: {title[:80]}",
            "indicator": "retraction_watch_classic",
            "country": "WLD",
            "country_name": "Welt (Wissenschaft)",
            "year": str(retraction_year),
            "display_value": headline,
            "description": description,
            "url": r.get("source_url",
                          "https://retractionwatch.com/the-retraction-watch-leaderboard/"),
            "source": "Retraction Watch (kuratiert) / Crossref Labs",
        })

    return results


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_retraction_watch(analysis: dict) -> dict:
    empty = {
        "source": "Retraction Watch",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}"
    matches = _claim_matches_retractions(matchable)
    if not matches:
        return empty

    return {
        "source": "Retraction Watch",
        "type": "official_data",
        "results": _build_retraction_results(matches),
    }
