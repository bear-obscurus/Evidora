"""Geld/Finanzen-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
Finanz- und Geld-Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- ecb (Live-API)
- oenb (AT)
- destatis (DE-Inflation, BIP)
- esoterik_pack (Pseudo-Heilung)

Topics (7):
  - aktien_gluecksspiel_mythos (FALSE — Index-ETF langfristig positiv)
  - inflation_geheim_konsens (transparente Methodik DESTATIS+Eurostat)
  - zinseszins_konsens (mathematisches Phänomen, real)
  - riester_rente_konsens (MIXED — lohnt für Geringverdiener mit Kindern)
  - kalte_progression_konsens (real, in DE seit 2022 jährlich kompensiert)
  - krypto_pyramide_mythos (Bitcoin ≠ Ponzi; spezifische Projekte schon)
  - negativzins_volkstheorie_mythos (2014-2022, seit 2022 passé)

Quellen-Mix: EZB, Bundesbank, OeNB, DESTATIS, Stiftung Warentest,
Verbraucherzentrale, Deutsches Aktieninstitut (DAI), ifo, DIW,
BaFin, BMF, BIS (Bank for International Settlements).

Politische Sensibilität: niedrig-mittel. Pack zitiert Statistik +
Verbraucherschutz-Empfehlungen, nimmt keine politischen Wertungen vor.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "finanzen_pack.json",
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


def claim_mentions_finanzen_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_finanzen(client=None):
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


async def search_finanzen(analysis: dict) -> dict:
    empty = {
        "source": "Finanzen-Mythen (EZB + Bundesbank + DAI + Stiftung Warentest + BaFin)",
        "type": "finance_consensus",
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
        label = fact.get("source_label", "EZB / Bundesbank / DAI / Stiftung Warentest / BaFin")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "finanzen_konsens_fact",
            "country": "DE/AT/EU",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Finanzen-Mythen (EZB + Bundesbank + DAI + Stiftung Warentest + BaFin)",
        "type": "finance_consensus",
        "results": results,
    }
