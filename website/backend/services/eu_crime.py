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

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "eu_crime.json",
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


def claim_mentions_eu_crime_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_eu_crime(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


async def search_eu_crime(analysis: dict) -> dict:
    empty = {
        "source": "Eurostat Crime + DACH PKS",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    cl = claim.lower()
    matches = _claim_matches_facts(cl, full_claim=claim)
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
