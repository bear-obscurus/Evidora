"""ÖBB Pünktlichkeit + Verkehrs-CO2 Umweltbundesamt + KlimaTicket-Stats —
Verkehrs-Eckwerte gegen Boulevard-Mythen ('ÖBB unzuverlässig',
'Klimakleber sollen Lkw blockieren', 'KlimaTicket ist gescheitert')."""

import logging
import os

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "transport_at.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("transport_at.json missing 'facts' key")
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


def claim_mentions_transport_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower()))


async def fetch_transport(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


async def search_transport(analysis: dict) -> dict:
    empty = {
        "source": "Verkehr Österreich (ÖBB + UBA + KlimaTicket)",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_facts(claim.lower())
    if not matches:
        return empty

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        url = fact.get("source_url", "")
        label = fact.get("source_label", "ÖBB / UBA / Eurostat")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        if topic == "oebb_puenktlichkeit":
            display = (
                f"ÖBB-Pünktlichkeit 2024 (≤5 Min Verspätung): "
                f"Nahverkehr {d.get('puenktlichkeit_oebb_nahverkehr_2024_pct')} %, "
                f"Fernverkehr {d.get('puenktlichkeit_oebb_fernverkehr_2024_pct')} %. "
                f"DACH-Vergleich Fernverkehr: SBB "
                f"{d.get('puenktlichkeit_sbb_fernverkehr_2024_pct')} %, DB nur "
                f"{d.get('puenktlichkeit_db_fernverkehr_2024_pct')} %. "
                f"Fahrgäste 2024: {d.get('fahrgaeste_oebb_2024_mio')} Mio "
                f"(2014: {d.get('fahrgaeste_oebb_2014_mio')} Mio, +11 %)."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "verkehr_co2":
            display = (
                f"Verkehrs-CO2 Österreich 2024: "
                f"{d.get('co2_verkehr_at_2024_mio_t')} Mio. t "
                f"(1990: {d.get('co2_verkehr_at_1990_mio_t')} Mio. t; "
                f"2005-Peak: {d.get('co2_verkehr_at_2005_peak_mio_t')} Mio. t). "
                f"Verkehr = {d.get('anteil_verkehr_an_thg_at_pct_2024')} % der "
                f"AT-Treibhausgase. Pkw verursachen "
                f"{d.get('anteil_pkw_an_verkehrs_co2_pct_2024')} % der "
                f"Verkehrs-CO2, Lkw {d.get('anteil_lkw_an_verkehrs_co2_pct_2024')} %, "
                f"Flugverkehr {d.get('anteil_flugverkehr_at_emissionen_pct_2024')} %. "
                f"Modal-Split AT: {d.get('modal_split_pkw_at_pct')} % Pkw, "
                f"{d.get('modal_split_oeffi_at_pct')} % ÖV, "
                f"{d.get('modal_split_rad_fuss_at_pct')} % Rad/Fuß."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "klimaticket":
            display = (
                f"KlimaTicket Österreich: "
                f"{d.get('klimaticket_inhaber_2024'):,} Inhaber:innen 2024 ".replace(",", ".")
                + f"(Start 10/2021: {d.get('klimaticket_inhaber_2022_start'):,}, ".replace(",", ".")
                + f"+83 %). Vollpreis {d.get('klimaticket_kosten_2024_eur'):,} €/Jahr; ".replace(",", ".")
                + f"Jugend/Senior {d.get('klimaticket_kosten_jugend_2024_eur'):,} €. ".replace(",", ".")
                + f"Geschätzter Modal-Shift "
                f"{d.get('modal_shift_pkw_zu_oeffi_pct_geschaetzt')} % der Inhaber:innen "
                f"vom Auto zum ÖV; CO2-Einsparung "
                f"~{d.get('co2_einsparung_geschaetzt_2024_kt')} kt/Jahr (~1 % "
                f"der Verkehrs-CO2)."
            )
            description = d.get("context", "") + " " + notes_joined
        else:
            display = fact.get("headline", "?")
            description = notes_joined

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "transport_at_fact",
            "country": "AT",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "Verkehr Österreich (ÖBB + UBA + KlimaTicket)",
        "type": "official_data",
        "results": results,
    }
