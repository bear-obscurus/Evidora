"""OeNB Wohnimmobilienpreis-Index + Statistik Austria HVPI/Wohnen +
Eurostat EU-SILC — Wohnungsmarkt-Eckwerte gegen die häufigsten Boulevard-
Mythen ('Wohnen wird unleistbar', 'Mieten explodieren', 'Eigentum
unerreichbar')."""

import logging
import os

from services._static_cache import load_json_mtime_aware
from services._reranker_backup import best_matches as _backup_best_matches

logger = logging.getLogger("evidora")


def _fact_with_descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "housing_at.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("housing_at.json missing 'facts' key")
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


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    data = _load_static_json()
    if not data:
        return []
    facts = data.get("facts") or []
    matches = [f for f in facts if _fact_matches(f, claim_lc)]
    if matches:
        return matches
    if not full_claim:
        return []
    items = [_fact_with_descriptor(f) for f in facts]
    return _backup_best_matches(full_claim, items, threshold=0.45, top_n=3)


def claim_mentions_housing_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_housing(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


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
