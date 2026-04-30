"""OeNB Wohnimmobilienpreis-Index + Statistik Austria HVPI/Wohnen +
Eurostat EU-SILC — Wohnungsmarkt-Eckwerte gegen die häufigsten Boulevard-
Mythen ('Wohnen wird unleistbar', 'Mieten explodieren', 'Eigentum
unerreichbar')."""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "housing_at.json",
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


def claim_mentions_housing_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_housing(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


async def search_housing(analysis: dict) -> dict:
    empty = {
        "source": "Wohnen Österreich (OeNB + EU-SILC)",
        "type": "official_data",
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
        label = fact.get("source_label", "OeNB / Statistik Austria / Eurostat")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        if topic == "wohnpreise_at":
            display = (
                f"OeNB Wohnimmobilienpreis-Index Österreich (2010=100): "
                f"2010={d.get('wohnimmo_index_at_2010_basis')}, "
                f"2015={d.get('wohnimmo_index_at_2015')}, "
                f"2020={d.get('wohnimmo_index_at_2020')}, "
                f"2022 (Peak)={d.get('wohnimmo_index_at_2022_peak')}, "
                f"2024={d.get('wohnimmo_index_at_2024')} (+107 % seit 2010). "
                f"Eigentumswohnung Wien {d.get('preis_pro_m2_eigentumswohnung_wien_2024'):,} €/m², ".replace(",", ".")
                + f"AT-Schnitt {d.get('preis_pro_m2_eigentumswohnung_at_2024'):,} €/m². ".replace(",", ".")
                + f"Miete Wien Neuvermietung {d.get('miete_pro_m2_wien_neuvermietung_2024')} €/m², "
                f"Altmietverhältnis Schnitt {d.get('miete_pro_m2_wien_altmietverhaeltnis_durchschnitt_2024')} €/m²."
            )
            description = d.get("trend_text", "") + " " + d.get("context", "") + " " + notes_joined
        elif topic == "wohnkostenbelastung":
            display = (
                f"Wohnkostenbelastung Österreich 2024: "
                f"{d.get('anteil_wohnkosten_at_2024_pct')} % des verfügbaren "
                f"Haushaltseinkommens (EU-Schnitt {d.get('anteil_wohnkosten_eu_avg_2024_pct')} %). "
                f"Bei niedrigen Einkommen (<60 % Median): "
                f"{d.get('anteil_wohnkosten_at_unter_60_einkommensmedian_pct')} %. "
                f"Wohnkostenüberlastung (>40 % Einkommen): "
                f"{d.get('wohnkostenueberlastung_at_pct')} % der Haushalte "
                f"(EU-Schnitt {d.get('wohnkostenueberlastung_eu_avg_pct')} %)."
            )
            description = d.get("context", "") + " " + notes_joined
        else:
            display = fact.get("headline", "?")
            description = notes_joined

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "housing_at_fact",
            "country": "AT",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "Wohnen Österreich (OeNB + EU-SILC)",
        "type": "official_data",
        "results": results,
    }
