"""Energy-Charts (Fraunhofer ISE) + APG + ENTSO-E — Stromproduktions- und
Handelsbilanz-Eckwerte DACH gegen die häufigsten Klima-/Energie-Boulevard-
Mythen.

Datenquelle: Static-curated JSON in data/energy_charts.json. Live-API-
Pfad (api.energy-charts.info, transparency.entsoe.eu, apg.at/transparency)
wäre für Echtzeit-Last + Strompreis ergänzbar; für die Top-Mythen-Counter
(DE-Atomstrom-aus-Frankreich, EE-Anteil 'nur 10 %', Dunkelflaute legt Netz
lahm, AT-Stromsaldo) reicht eine kuratierte jährliche Aktualisierung.

Pattern: Trigger-Match (Substring + Composite) → topic-spezifische
Result-Builder mit kontextualisierter Boulevard-Counter-Erklärung.
"""

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
    "energy_charts.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("energy_charts.json missing 'facts' key")
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
    return _backup_best_matches(full_claim, items, threshold=0.62, top_n=3)


def claim_mentions_energy_charts_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_energy_charts(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


async def search_energy_charts(analysis: dict) -> dict:
    empty = {
        "source": "Energy-Charts (Fraunhofer) + APG",
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
        headline = fact.get("headline", "?")
        d = fact.get("data") or {}
        url = fact.get("source_url", "")
        label = fact.get("source_label", "Energy-Charts")
        notes = fact.get("context_notes") or []

        if topic == "de_atomausstieg_atomstrom_import":
            display = (
                f"DE Stromsaldo 2024: {d.get('de_stromsaldo_2024_twh')} TWh "
                f"(Netto-Importeur). Import netto aus Frankreich: "
                f"{d.get('de_nettoimport_aus_frankreich_2024_twh')} TWh "
                f"(FR-Strom 2024 zu {d.get('fr_kernkraft_anteil_2024_pct')} % "
                f"aus Kernenergie). DE-EE-Anteil 2024: "
                f"{d.get('de_erneuerbare_anteil_2024_pct')} %."
            )
            description = d.get("context_kontextualisiert", "")
        elif topic == "ee_anteil_de":
            display = (
                f"DE Stromproduktion 2024: Erneuerbarer Anteil = "
                f"{d.get('ee_anteil_brutto_pct_2024')} % (brutto), "
                f"Wind {d.get('wind_an_land_anteil_pct_2024')} %, "
                f"Solar {d.get('solar_anteil_pct_2024')} %, "
                f"Biomasse {d.get('biomasse_anteil_pct_2024')} %, "
                f"Wasserkraft {d.get('wasser_anteil_pct_2024')} %."
            )
            description = d.get("trend_text", "")
        elif topic == "dunkelflaute_de":
            display = (
                f"DE 2024: {d.get('anzahl_dunkelflauten_2024')} Dunkelflauten "
                f"(längste {d.get('laengste_dunkelflaute_h')} h), "
                f"keine großflächigen Blackouts. SAIDI = "
                f"{d.get('stromnetz_blackouts_2024_minuten_pro_kunde')} Min/Kunde "
                f"(EU-Rang {d.get('stromnetz_saidi_de_eu_rang')})."
            )
            description = d.get("context", "")
        elif topic == "at_strom_eckdaten":
            display = (
                f"AT Stromsaldo 2024: {d.get('at_stromsaldo_2024_twh')} TWh "
                f"(Netto-Importeur). Erneuerbarer Anteil "
                f"{d.get('at_erneuerbare_anteil_pct_2024')} % "
                f"(Wasser {d.get('at_wasserkraft_anteil_pct_2024')} %, "
                f"Wind {d.get('at_wind_anteil_pct_2024')} %, "
                f"Solar {d.get('at_solar_anteil_pct_2024')} %)."
            )
            description = d.get("context", "")
        else:
            display = headline
            description = ""

        if notes:
            description = (description + " ").strip() + " | " + " | ".join(notes)

        results.append({
            "indicator_name": headline,
            "indicator": "energy_charts_fact",
            "country": "DE/AT/EU",
            "year": str(fact.get("year", "")),
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "Energy-Charts (Fraunhofer) + APG",
        "type": "official_data",
        "results": results,
    }
