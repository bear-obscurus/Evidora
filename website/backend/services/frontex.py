"""Frontex — EU-Grenzschutz-Statistiken (irreguläre Grenzübertritte).

Datenquelle: Frontex Migratory Map + Pressemeldungen. Kuratiertes JSON
in ``data/frontex.json``, weil die Live-API zwar existiert (data.europa.eu)
aber das Frontex-Dashboard primär als HTML-Karte ausliefert. Die Pressemeldungen
liefern die wichtigsten Aggregate (Total, Routen-Aufschlüsselung, Top-Herkunfts-
länder).

Use-Case:
- "irreguläre Grenzübertritte 2025 sind um X gesunken/gestiegen"
- "Westbalkan-Route geschlossen / aktiv"
- "Mittelmeer-Tote 2025"
- "Frontex-Statistik" / "EU-Außengrenzen"
"""

import json
import logging
import os

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "frontex.json",
)

_cache: dict | None = None


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_FRONTEX_TERMS = (
    "frontex", "grenzschutz eu", "eu-grenzschutz",
    "eu-außengrenzen", "eu außengrenzen",
    "irreguläre grenzübertritte", "irregular border crossings",
    "illegale grenzübertritte",
    "westbalkan-route", "westbalkanroute", "balkan-route",
    "mittelmeer-route", "mittelmeerroute",
    "zentrales mittelmeer", "östliches mittelmeer", "westliches mittelmeer",
    "kanaren-route", "westafrika-route", "westafrikaroute",
    "ärmelkanal migration", "channel crossings",
    "mittelmeer tote", "tote mittelmeer", "tote im mittelmeer",
    "im mittelmeer gestorben", "menschen gestorben mittelmeer",
    "ertrunken mittelmeer", "mittelmeer ertrunken",
    "fluchtroute eu", "fluchtrouten",
)


def _claim_mentions_frontex(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _FRONTEX_TERMS)
    if has_term:
        return True
    # Composite: 'grenzübertritt' + EU-Bezug
    has_grenzubert = any(t in claim_lc for t in (
        "grenzübertritt", "grenzübertrit", "border crossing",
        "border-crossing",
    ))
    has_eu = any(t in claim_lc for t in (
        "eu", "europa", "europäische union", "european union",
    ))
    if has_grenzubert and has_eu:
        return True
    # Composite: Mittelmeer + Tote/Tod
    has_mittelmeer = "mittelmeer" in claim_lc
    has_tot = any(t in claim_lc for t in (
        "tote", "gestorben", "tod ", "tods", "ertrunken",
    ))
    if has_mittelmeer and has_tot:
        return True
    return False


def claim_mentions_frontex_cached(claim: str) -> bool:
    return _claim_mentions_frontex((claim or "").lower())


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
        if not isinstance(data, dict) or "data" not in data:
            logger.warning("frontex.json missing 'data' key")
            return None
        _cache = data
        logger.info("Frontex data loaded: 1 dataset")
        return _cache
    except FileNotFoundError:
        logger.warning(f"frontex.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"frontex.json load failed: {e}")
        return None


async def fetch_frontex(client=None):
    data = _load_static_json()
    if not data:
        return []
    return [data] if data else []


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _de_int(v):
    if v is None:
        return "?"
    try:
        return f"{int(v):,}".replace(",", ".")
    except Exception:
        return str(v)


def _build_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "Frontex"

    results: list[dict] = []

    headline = (
        f"Frontex 2025: rund {_de_int(data.get('irregular_crossings_eu_2025_total_approx'))} "
        f"Detektionen irregulärer Grenzübertritte EU-weit "
        f"({data.get('rueckgang_2025_yoy_pct')} % gegenüber 2024). "
        f"2024: {_de_int(data.get('irregular_crossings_eu_2024_total'))} "
        f"({data.get('rueckgang_2024_yoy_pct')} % yoy, "
        f"niedrigster Stand seit {data.get('irregular_crossings_eu_2024_lowest_since')}). "
        f"Mittelmeer-Tote 2025: mind. {_de_int(data.get('todesfaelle_mittelmeer_2025_min'))}."
    )

    description_parts: list[str] = []
    routen = data.get("routen_2025") or []
    if routen:
        routen_str = " · ".join(
            f"{r['name']}: {r['trend_2025']}"
            for r in routen
        )
        description_parts.append(f"Routen 2025: {routen_str}")
    if data.get("top_3_herkunftslaender_2025"):
        description_parts.append(
            "Top-3 Herkunftsländer 2025: "
            + ", ".join(data["top_3_herkunftslaender_2025"])
        )
    for caveat in data.get("wichtige_caveats") or []:
        description_parts.append(caveat)

    main = {
        "indicator_name": "Frontex EU-Grenzübertritte 2024-2025",
        "indicator": "frontex_main",
        "country": "EU",
        "country_name": "Europäische Union",
        "year": "2025",
        "value": data.get("irregular_crossings_eu_2025_total_approx"),
        "display_value": headline,
        "description": " ".join(description_parts),
        "url": src,
        "source": label,
    }
    results.append(main)

    # Spezial-Eintrag wenn Claim eine spezifische Route nennt.
    # Mapping: Trigger-Stichwort → Routen-Substring, der eindeutig zuordnet.
    route_triggers = [
        ("westbalkan", "westbalkan"),
        ("balkan", "westbalkan"),
        ("westafrika", "westafrikanisch"),
        ("kanaren", "westafrikanisch"),
        ("zentrales mittelmeer", "zentrales mittelmeer"),
        ("östliches mittelmeer", "östliches mittelmeer"),
        ("oestliches mittelmeer", "östliches mittelmeer"),
        ("kreta", "östliches mittelmeer"),
        ("westliches mittelmeer", "westliches mittelmeer"),
        ("ärmelkanal", "ärmelkanal"),
        ("aermelkanal", "ärmelkanal"),
        ("channel", "ärmelkanal"),
    ]
    for trigger_kw, route_substr in route_triggers:
        if trigger_kw in claim_lc:
            for r in routen:
                if route_substr in r["name"].lower():
                    results.insert(0, {
                        "indicator_name": f"Frontex {r['name']} 2025",
                        "indicator": "frontex_route",
                        "country": "EU", "country_name": "Europäische Union",
                        "year": "2025",
                        "display_value": (
                            f"Frontex {r['name']} 2025: {r['trend_2025']}. "
                            f"Wichtigste Herkunftsorte: {r['wichtigster_herkunftshafen']}."
                        ),
                        "description": (
                            "Frontex-Routen-Aufschlüsselung 2025. "
                            "WICHTIG: Detektionen sind keine Personen — eine "
                            "Person kann mehrfach gezählt werden."
                        ),
                        "url": src, "source": label,
                    })
                    break
            break

    return results


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_frontex(analysis: dict) -> dict:
    empty = {
        "source": "Frontex",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_frontex(matchable):
        return empty

    data = _load_static_json()
    if not data:
        return empty

    return {
        "source": "Frontex",
        "type": "official_data",
        "results": _build_results(data, matchable),
    }
