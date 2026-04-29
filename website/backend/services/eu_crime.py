"""Eurostat Crime + BKA-DE PKS + BMI-AT PKS — kuratierte Sammlung der
wichtigsten Kriminalitätsstatistik-Eckwerte für DACH und EU-Vergleich.

Datenquelle: Static-curated JSON in data/eu_crime.json. Eurostat-Endpoints
(crim_off_*, crim_hom_*) wären als zweiter Live-Pfad ergänzbar; für die
Top-Boulevard-Themen (Migrantenkriminalität AT/DE, Tötungsdelikt-Vergleich,
'Kriminalitätsexplosion') reicht eine kuratierte Sammlung mit den jährlich
aktualisierten Eckwerten.

Pattern: Trigger-Match + topic-spezifischer Result-Builder mit einem
expliziten 'context_warnings'-Block für die häufige Boulevard-
Verzerrung 'Roh-Anteil ohne Kontextualisierung'.
"""

import logging
import os

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "eu_crime.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("eu_crime.json missing 'facts' key")
        return None
    return data


def _fact_matches(fact: dict, claim_lc: str) -> bool:
    for kw in fact.get("trigger_keywords") or ():
        if kw.lower() in claim_lc:
            return True
    composite = fact.get("trigger_composite") or []
    if composite and all(
        isinstance(alt, (list, tuple)) and any(tok in claim_lc for tok in alt)
        for alt in composite
    ):
        return True
    return False


def _claim_matches_facts(claim_lc: str) -> list[dict]:
    data = _load_static_json()
    if not data:
        return []
    return [f for f in data.get("facts") or [] if _fact_matches(f, claim_lc)]


def claim_mentions_eu_crime_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower()))


async def fetch_eu_crime(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


async def search_eu_crime(analysis: dict) -> dict:
    empty = {
        "source": "Eurostat Crime + DACH PKS",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    cl = claim.lower()
    matches = _claim_matches_facts(cl)
    if not matches:
        return empty

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        headline = fact.get("headline", "?")
        data = fact.get("data") or {}
        context_notes = fact.get("context_notes") or []
        url = fact.get("source_url", "")
        label = fact.get("source_label", "Eurostat / BKA / BMI")

        # Build a structured display
        if topic == "homicide_eu_compare":
            display = (
                f"Tötungsdelikte pro 100.000 Einwohner 2024: "
                f"AT = {data.get('homicide_at_per_100k')}, "
                f"DE = {data.get('homicide_de_per_100k')}, "
                f"EU-Schnitt = {data.get('homicide_eu_avg_per_100k')}. "
                f"Österreich liegt unter dem EU-Schnitt."
            )
            description = data.get("trend_at", "") + " " + data.get("context", "")
        elif topic == "migrant_crime_at":
            display = (
                f"PKS Österreich 2024: Nicht-AT-Tatverdächtige = "
                f"{data.get('anteil_nicht_at_tatverdaechtige_pct')} % "
                f"(Roh-Anteil); Wohnbevölkerungsanteil Nicht-AT = "
                f"{data.get('anteil_nicht_at_wohnbevoelkerung_pct')} %. "
                f"Roh-Faktor 2,0 — strukturkontrolliert (Alter, Geschlecht, "
                f"Sozioökonomie) deutlich kleiner."
            )
            warnings = data.get("context_warnings") or []
            description = " | ".join(warnings)
        elif topic == "migrant_crime_de":
            display = (
                f"BKA-PKS Deutschland 2024: Nicht-DE-Tatverdächtige = "
                f"{data.get('anteil_nicht_de_tatverdaechtige_pct')} %; "
                f"Wohnbevölkerungsanteil Nicht-DE = "
                f"{data.get('anteil_nicht_de_wohnbevoelkerung_pct')} %. "
                f"Sub-Kategorie 'Zuwanderer im engeren Sinne': "
                f"{data.get('anteil_zuwanderer_im_engeren_sinne_pct')} %."
            )
            warnings = data.get("context_warnings") or []
            description = " | ".join(warnings)
        elif topic == "crime_trend_at":
            display = (
                f"Gesamtkriminalität Österreich: 2010 = "
                f"{data.get('angezeigte_straftaten_2010'):,}".replace(",", ".") + ", "
                f"2024 = "
                f"{data.get('angezeigte_straftaten_2024'):,}".replace(",", ".") + " "
                f"angezeigte Straftaten. Trend leicht rückläufig — "
                f"keine 'Kriminalitätsexplosion' erkennbar."
            )
            description = (
                data.get("trend_text", "")
                + f" Aufklärungsquote 2024: {data.get('aufklaerungsquote_pct_2024')} % "
                + f"(2014: {data.get('aufklaerungsquote_pct_2014')} %)."
            )
        else:
            display = headline
            description = ""

        if context_notes:
            description = (description + " ").strip() + " | " + " | ".join(context_notes)

        results.append({
            "indicator_name": headline,
            "indicator": "eu_crime_fact",
            "country": "AT/DE/EU",
            "year": str(fact.get("year", "")),
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "Eurostat Crime + DACH PKS",
        "type": "official_data",
        "results": results,
    }
