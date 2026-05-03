"""Energie-/Klima-Politik-Pack — kuratierte Konsens-Daten zu klassischen
Energie- und Klimaschutz-politischen Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- copernicus.py / berkeley_earth.py / nasa_giss.py (Klima-Daten)
- skeptical_science.py (Klimaskeptiker-Argumente)
- energy_charts.py (Stromhandel + EE-Anteil DE/EU)
- eea.py (EU-Umweltagentur)

Während die anderen Klima-Quellen primär WISSENSCHAFTLICHE Klimadaten
liefern (Temperaturen, CO2-Konzentrationen), fokussiert der Energie-/
Klima-Politik-Pack auf populäre POLITISCHE Halbwahrheiten — Atomkraft-
Diskurs, China-Whataboutism, E-Auto-Skepsis, Wärmepumpen-Mythen,
Solar-Recycling, Windkraft-Vögel, Versorgungssicherheits-Argumente,
Diesel-Skandal-Aufarbeitung.

Quellen-Mix:
  - IPCC AR6 (2022) WG III für Lifecycle-Emissionen
  - IEA (International Energy Agency) für globale Energie-Bilanzen
  - Fraunhofer ISE für DE-Wärmepumpen + Energy-Charts-Daten
  - JRC (Joint Research Centre der EU) für Lifecycle-Analysen
  - ICCT (International Council on Clean Transportation) für E-Auto +
    Diesel-Lifecycle
  - UBA AT/DE (Umweltbundesamt) für Schadstoff- + Klimaschutz-Daten
  - EEA (European Environment Agency)
  - NABU/IFAB für Vogelschlag-Studien
  - Climate Watch + Carbon Brief für Vergleichs-Statistiken

Topics:
  - atomkraft_co2_konsens (CO2-Bilanz Atomkraft niedrig)
  - china_kohle_whataboutism_mythos (Whataboutism-Logik widerlegt)
  - e_auto_co2_lifecycle_konsens (E-Auto besser als Verbrenner in EU)
  - waermepumpe_winter_konsens (funktionieren bei Frost)
  - solar_recycling_konsens (95 % recyclebar)
  - windkraft_voegel_konsens (Vergleichsmortalität deutlich)
  - erneuerbare_versorgungssicher_mythos (DE-SAIDI bleibt niedrig)
  - diesel_emissionen_konsens (MIXED — sauberer aber nicht 'sauber')

Politische Sensibilität: mittel. Themen sind politisch aufgeladen,
aber faktisch gut belegbar. Pack zitiert Zahlen, nimmt keine politischen
Wertungen vor. Bei MIXED-Topics (Diesel, Windkraft-Vögel) wird die
Differenzierung explizit aufgezeigt.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "energie_klima_pack.json",
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


def claim_mentions_energie_klima_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_energie_klima(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    parts: list[str] = []
    for key, val in d.items():
        if key == "context":
            continue
        if isinstance(val, str) and val.strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)


async def search_energie_klima(analysis: dict) -> dict:
    empty = {
        "source": "Energie/Klima-Politik (IPCC + IEA + Fraunhofer + UBA + JRC + EEA)",
        "type": "energy_climate_consensus",
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
        secondary = fact.get("secondary_url", "")
        label = fact.get("source_label", "IPCC / IEA / Fraunhofer / UBA / JRC / EEA")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "energie_klima_konsens_fact",
            "country": "—",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Energie/Klima-Politik (IPCC + IEA + Fraunhofer + UBA + JRC + EEA)",
        "type": "energy_climate_consensus",
        "results": results,
    }
