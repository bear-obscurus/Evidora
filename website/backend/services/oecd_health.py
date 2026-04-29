"""OECD Health Statistics + Eurostat hlth_* — Gesundheitssystem-Eckwerte
DACH gegen die häufigsten Boulevard-Mythen ('Gesundheitssystem
kollabiert', 'Lebenserwartung sinkt', 'zu wenig Spitalsbetten').
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
    "oecd_health.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("oecd_health.json missing 'facts' key")
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


def claim_mentions_oecd_health_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_oecd_health(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


async def search_oecd_health(analysis: dict) -> dict:
    empty = {
        "source": "OECD Health (DACH)",
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
        label = fact.get("source_label", "OECD")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        if topic == "lebenserwartung":
            display = (
                f"Lebenserwartung bei Geburt 2024: AT = "
                f"{d.get('lebenserwartung_at_2024')} Jahre, DE = "
                f"{d.get('lebenserwartung_de_2024')}, CH = "
                f"{d.get('lebenserwartung_ch_2024')} (EU-Schnitt: "
                f"{d.get('lebenserwartung_eu_avg_2024')}). "
                f"AT-Trend 2000–2024: +{d.get('lebenserwartung_at_2024') - d.get('lebenserwartung_at_2000'):.1f} Jahre. "
                f"Frauen {d.get('lebenserwartung_at_frauen_2024')} / "
                f"Männer {d.get('lebenserwartung_at_maenner_2024')}."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "spitalsbetten":
            display = (
                f"Akutbetten je 1.000 Einwohner 2024: AT = "
                f"{d.get('akutbetten_pro_1000_at')}, DE = "
                f"{d.get('akutbetten_pro_1000_de')}, CH = "
                f"{d.get('akutbetten_pro_1000_ch')} "
                f"(EU-Schnitt: {d.get('akutbetten_pro_1000_eu_avg')}). "
                f"AT-Ärzte / 1.000: {d.get('anzahl_aerzte_pro_1000_at')} "
                f"(EU-Spitze; EU-Schnitt {d.get('anzahl_aerzte_pro_1000_eu_avg')}). "
                f"AT-Pflegekräfte / 1.000: {d.get('anzahl_pflegekraefte_pro_1000_at')} "
                f"(unter EU-Schnitt {d.get('anzahl_pflegekraefte_pro_1000_eu_avg')})."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "gesundheitsausgaben":
            display = (
                f"Gesundheitsausgaben 2024 (% des BIP): AT = "
                f"{d.get('gesundheitsausgaben_at_pct_bip_2024')} %, DE = "
                f"{d.get('gesundheitsausgaben_de_pct_bip_2024')} %, CH = "
                f"{d.get('gesundheitsausgaben_ch_pct_bip_2024')} % "
                f"(EU-Schnitt: {d.get('gesundheitsausgaben_eu_avg_pct_bip_2024')} %). "
                f"Pro Kopf: AT {d.get('gesundheitsausgaben_at_pro_kopf_eur_2024'):,} € ".replace(",", ".")
                + f"(EU-Schnitt {d.get('gesundheitsausgaben_eu_avg_pro_kopf_eur_2024'):,} €). ".replace(",", ".")
                + f"Öffentlicher Anteil AT: "
                f"{d.get('anteil_oeffentlich_at_pct_2024')} % "
                f"(EU-Schnitt {d.get('anteil_oeffentlich_eu_avg_pct_2024')} %)."
            )
            description = d.get("context", "") + " " + notes_joined
        else:
            display = fact.get("headline", "?")
            description = notes_joined

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "oecd_health_fact",
            "country": "AT/DE/CH/EU",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "OECD Health (DACH)",
        "type": "official_data",
        "results": results,
    }
