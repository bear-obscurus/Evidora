"""RKI SurvStat 2.0 — Robert Koch-Institut Surveillance-Eckwerte für die
häufigsten Boulevard-Themen rund um meldepflichtige Krankheiten in DE.

Datenquelle: Static-curated JSON in data/rki_surveillance.json. Live-Pfad
über survstat.rki.de wäre via SOAP-Endpoint möglich, ist aber komplex
und nur quartalsweise notwendig — für die wichtigsten Use-Cases (Masern-
Welle 2024, TB-Migration-Mythos, COVID-vs.-Grippe-Winter 2024/25)
reicht eine kuratierte Sammlung mit jährlicher Aktualisierung.

Pattern: Trigger-Match → Topic-spezifischer Result-Builder mit
Strukturkontext (Inzidenz-Vergleich historisch, Migrations-Anteil
mit Erklärung, Peak-Vergleich mit Vor-Pandemie-Niveau).
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
    "rki_surveillance.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("rki_surveillance.json missing 'facts' key")
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


def claim_mentions_rki_surveillance_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_rki_surveillance(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


async def search_rki_surveillance(analysis: dict) -> dict:
    empty = {
        "source": "RKI SurvStat (Surveillance)",
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
        label = fact.get("source_label", "RKI SurvStat")
        notes = fact.get("context_notes") or []

        if topic == "rki_masern":
            display = (
                f"Masern in DE: 2023 = {d.get('rki_masern_faelle_2023')} Fälle, "
                f"2024 = {d.get('rki_masern_faelle_2024')} Fälle "
                f"(+706 % vs. 2023), 2025 Q1 = "
                f"{d.get('rki_masern_faelle_2025_stand_q1')} Fälle. "
                f"Zweitimpfquote 24 Mon. = "
                f"{d.get('impfquote_de_masern_kinder_24m_pct_2024')} % "
                f"(WHO-Herdimmunität: "
                f"{d.get('impfquote_who_herdimmunitaet_pct')} %)."
            )
            description = d.get("context", "") + " " + d.get("context_quelle", "")
        elif topic == "rki_tuberkulose":
            display = (
                f"TB in DE 2024: {d.get('rki_tb_faelle_2024')} Fälle "
                f"(Inzidenz {d.get('rki_tb_inzidenz_pro_100k_2024')}/100 k). "
                f"Zum Vergleich: 1980 = {d.get('rki_tb_inzidenz_pro_100k_1980')}/100 k, "
                f"1995 = {d.get('rki_tb_inzidenz_pro_100k_1995')}/100 k. "
                f"Anteil im Ausland Geborener: "
                f"{d.get('anteil_im_ausland_geboren_pct_2024')} % "
                f"— wenig Übertragung in DE, viele Fälle bei Einreise diagnostiziert."
            )
            description = d.get("context", "") + " " + d.get("context_quelle", "")
        elif topic == "rki_atemwegsinfekte":
            display = (
                f"Atemwegsinfekt-Welle Winter 2024/25 (DE): "
                f"Peak ARI in KW 5/2025 = "
                f"{d.get('rki_ari_inzidenz_peak_woche_5_2025_pro_100k'):,}/100 k".replace(",", ".")
                + f" (typischer Vor-Pandemie-Peak ~"
                f"{d.get('rki_ari_inzidenz_typischer_winterpeak_pro_100k'):,}/100 k".replace(",", ".")
                + f"). Influenza dominant ({d.get('rki_influenza_anteil_an_ari_peak_pct')} %), "
                f"COVID nur {d.get('rki_covid_anteil_an_ari_peak_pct')} %, "
                f"RSV {d.get('rki_rsv_anteil_an_ari_peak_pct')} %."
            )
            description = d.get("context", "") + " " + d.get("context_quelle", "")
        else:
            display = fact.get("headline", "?")
            description = ""

        if notes:
            description = (description + " ").strip() + " | " + " | ".join(notes)

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "rki_surveillance_fact",
            "country": "DE",
            "year": str(fact.get("year", "")),
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "RKI SurvStat (Surveillance)",
        "type": "official_data",
        "results": results,
    }
