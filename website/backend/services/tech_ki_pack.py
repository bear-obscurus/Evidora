"""Tech-/KI-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
Tech-und-KI-Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- esoterik_pack.py (Pseudowissenschaft)
- geschichte_pack.py (historische Mythen)
- verschwoerungen_pack.py (zeitgenoessische Verschwoerungs-Narrative)

Dieser Pack fokussiert auf Tech-/KI-Mythen, wo die existierenden
Wissenschafts-DBs (PubMed, OpenAlex, SemanticScholar) zwar oft eine
gute Coverage liefern, aber ein kuratierter Authoritative-Anker die
Verdict-Konsistenz und -Robustheit verbessert. Im Erkundungs-Stress-
Test 2026-05-02 lagen die existierenden Quellen schon bei 19/20 strict;
der Pack bringt vor allem Konsistenz und einen klaren Anker bei
kontroversen Einzelfaellen (Apple vs Android, AGI-Timeline).

Quellen-Mix: NIST CVE-Datenbank + NIST PQC-Standards (FIPS 203/204/205),
EFF, ACM/FAccT (Bender et al. 2021 Stochastic Parrots), Chainalysis
Crypto Crime Report, Sweeney 1997 / Narayanan 2008 / de Montjoye 2013
(Re-Identifikation), LeCun-Position (LLMs/Bewusstsein), Cambridge CBECI
(Bitcoin-Stromverbrauch) + IEA Electricity Report.

Topics:
  - ki_bewusstsein_mythos
  - bitcoin_anonymitaet_mythos
  - bitcoin_stromverbrauch_konsens
  - quantencomputer_verschluesselung_mythos
  - anonymisierung_re_identifikation_mythos
  - vpn_anonymitaet_mythos
  - apple_android_sicherheit_mythos
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "tech_ki_pack.json",
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


def claim_mentions_tech_ki_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_tech_ki(client=None):
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


async def search_tech_ki(analysis: dict) -> dict:
    empty = {
        "source": "Tech-/KI-Faktencheck (NIST + EFF + ACM + Tech-Konsens)",
        "type": "tech_consensus",
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
        label = fact.get("source_label", "NIST / EFF / ACM / Tech-Forschungs-Konsens")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "tech_ki_konsens_fact",
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
        "source": "Tech-/KI-Faktencheck (NIST + EFF + ACM + Tech-Konsens)",
        "type": "tech_consensus",
        "results": results,
    }
