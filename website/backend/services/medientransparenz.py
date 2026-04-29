"""MedienTransparenz.gv.at (RTR / KommAustria) — Inserate-Meldungen
der öffentlichen Hand in Österreich nach §§ 2 + 4 MedKF-TG.

Datenquelle: Static-curated JSON in data/medientransparenz.json — basierend
auf den jährlichen RTR-Veröffentlichungen. Live-CSV-Pfad
(rtr.at/medien/was_wir_tun/medientransparenz) wäre quartalsweise refreshbar,
aber für die Boulevard-Use-Cases ('wer kriegt am meisten Inserate?',
'Inseratenaffäre Kurz') reicht eine jährliche Aktualisierung.

Pattern: Trigger-Match → Topic-spezifische Result-Builder mit den
Top-Empfänger / Top-Auftraggeber-Listen + WKStA-Kontext.
"""

import logging
import os

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "medientransparenz.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("medientransparenz.json missing 'facts' key")
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


def claim_mentions_medientransparenz_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower()))


async def fetch_medientransparenz(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


async def search_medientransparenz(analysis: dict) -> dict:
    empty = {
        "source": "MedienTransparenz (RTR/KommAustria)",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_facts(claim.lower())
    if not matches:
        return empty

    def _de(v):
        try:
            return f"{int(round(float(v))):,}".replace(",", ".")
        except Exception:
            return str(v)

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        url = fact.get("source_url", "")
        label = fact.get("source_label", "RTR / KommAustria")
        notes = fact.get("context_notes") or []

        if topic == "medientransparenz_overview":
            top10 = d.get("top_10_empfaenger_2024") or []
            top10_str = " · ".join(
                f"#{e['rang']} {e['medium']} ({e['betrag_mio_eur']} Mio.)"
                for e in top10[:5]
            )
            display = (
                f"Inserate der öffentlichen Hand in Österreich 2024: "
                f"Gesamtvolumen {d.get('gesamtvolumen_2024_mio_eur')} Mio. € "
                f"(2017: {d.get('gesamtvolumen_2017_mio_eur')} Mio. €). "
                f"Top-5: {top10_str}."
            )
            description = " | ".join(notes)
        elif topic == "medientransparenz_kurz_aera":
            display = (
                f"BKA-Inserate 2017 = "
                f"{d.get('bundeskanzleramt_inserate_2017_mio_eur')} Mio. → "
                f"2021 = {d.get('bundeskanzleramt_inserate_2021_mio_eur')} Mio. (+509 %); "
                f"BMF 2019 = {d.get('bmf_inserate_2019_mio_eur')} Mio. → "
                f"2021 = {d.get('bmf_inserate_2021_mio_eur')} Mio. (+89 %); "
                f"BMLRT 2019 = {d.get('bmlrt_inserate_2019_mio_eur')} Mio. → "
                f"2021 = {d.get('bmlrt_inserate_2021_mio_eur')} Mio. (+264 %). "
                f"WKStA-Ermittlungen / Anklage gegen Kurz seit Herbst 2024 — "
                f"Verfahren laufend, Unschuldsvermutung gilt."
            )
            description = (d.get("kontext_pilnacek_wkstaat_ermittlung", "")
                           + " | " + " | ".join(notes))
        else:
            display = fact.get("headline", "?")
            description = " | ".join(notes)

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "medientransparenz_fact",
            "country": "AT",
            "year": str(fact.get("year", "")),
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "MedienTransparenz (RTR/KommAustria)",
        "type": "official_data",
        "results": results,
    }
