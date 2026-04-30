"""OECD Health Statistics + Eurostat hlth_* — Gesundheitssystem-Eckwerte
DACH gegen die häufigsten Boulevard-Mythen ('Gesundheitssystem
kollabiert', 'Lebenserwartung sinkt', 'zu wenig Spitalsbetten').
"""

import logging
import os

from services._topic_match import (
    find_matching_items,
    load_items,
)

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "oecd_health.json",
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


def claim_mentions_oecd_health_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_oecd_health(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


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
        elif topic == "kinder_adipositas":
            display = (
                f"Kinder + Jugendliche Übergewicht/Adipositas (DACH-Vergleich): "
                f"AT (6–15 J.) = {d.get('uebergewicht_at_kinder_6_15_pct_2024')} % "
                f"übergewichtig, davon {d.get('adipositas_at_kinder_6_15_pct_2024')} % "
                f"adipös. Trend AT 2008→2024: "
                f"{d.get('uebergewicht_at_kinder_2014_pct')} % → "
                f"{d.get('uebergewicht_at_kinder_6_15_pct_2024')} %. "
                f"DE (KIGGS, 3–17 J.): "
                f"{d.get('uebergewicht_de_kinder_3_17_pct_2024')} % / "
                f"{d.get('adipositas_de_kinder_3_17_pct_2024')} %. "
                f"OECD-Schnitt Übergewicht: "
                f"{d.get('uebergewicht_oecd_avg_kinder_pct_2024')} %."
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
