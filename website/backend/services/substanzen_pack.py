"""Substanzen-Pack — kuratierte Konsens-Daten zu Sucht-/Drogen-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: medizinisch-statistische Sucht-/Drogen-Konsens-
Aussagen (peer-reviewed, NIDA/EMCDDA/EFSA/Cochrane-gestützt). Bewusst
RAUS: Drogen-Politik-Debatten (Strafrecht vs. Gesundheits-Modell),
spezifische Politiker-Aussagen, Cannabis-Legalisierungs-Wertungen.

Topics (10):
  - cannabis_jugend_hirnschaden_mythos (NUANCED — Meier 2012 PNAS,
    Mokrysz 2016 Reanalyse, Lorenzetti 2019)
  - mikrodosing_lsd_psilocybin_mythos (FALSE — Szigeti 2021 eLife
    Self-Blinding RCT, Polito 2021)
  - energy_drinks_herz_mythos (NUANCED — EFSA 2015, Higgins 2010,
    AAP gegen Pediatrie-Konsum)
  - alkohol_rotwein_herz_mythos (FALSE — WHO 2023, GBD 2016 Lancet
    2018, Stockwell 2016 Sick-Quitter-Bias)
  - vape_e_zigarette_sicherheit_mythos (NUANCED — PHE 2018, EVALI
    2019 CDC, Cochrane Hartmann-Boyce 2022)
  - kratom_natuerliche_alternative_mythos (FALSE — FDA 2018, NIDA,
    μ-Opioid-Rezeptor-Agonist, Hepatotoxizität)
  - kokain_filter_kaffee_mythos (FALSE — Pharmakokinetik, SAMHSA
    Drug-Testing-Guidelines)
  - cbd_wundermittel_mythos (FALSE/MIXED — FDA Epidiolex spezifisch,
    Cochrane Mücke 2018 schwach, Bonn-Miller 2017 Qualitätsprobleme)
  - cannabis_legalisierung_konsum_mythos (FALSE — Sevigny 2024 Meta,
    NIDA Monitoring the Future, Hasin 2017)
  - entgiftungs_diaeten_drogen_mythos (FALSE — NCCIH, BfR;
    Pharmakokinetik nicht beschleunigbar)

Quellen-Mix: NIDA (US National Institute on Drug Abuse), EMCDDA
(European Monitoring Centre Drugs Drug Addiction), EFSA, FDA, CDC,
WHO, Cochrane Reviews, AHA, AAP, NCCIH, BfR, RKI, NICE Guidelines,
Lancet GBD 2016 + 2018, NEJM, JAMA, peer-reviewed Studien (Meier
PNAS, Szigeti eLife, Higgins Mayo Clinic, Stockwell J Stud Alcohol,
Sevigny meta-analyse, Bonn-Miller JAMA, Veltri review, Wong Drug
Alcohol Depend, Wang JAMA Pediatrics).

Politische Sensibilität: niedrig bis mittel (Cannabis-Legalisierung
+ Alkohol-Position politisch heikel, aber wir bleiben streng bei
peer-reviewed Daten). Hohe Lehrer-Relevanz für Sucht-Prävention
in Schulen AT/DE.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "substanzen_pack.json",
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


def claim_mentions_substanzen_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_substanzen(client=None):
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


async def search_substanzen(analysis: dict) -> dict:
    empty = {
        "source": "Substanzen-Konsens (NIDA + EMCDDA + EFSA + WHO + Cochrane + FDA)",
        "type": "substance_consensus",
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
        label = fact.get("source_label", "NIDA / EMCDDA / EFSA / WHO / Cochrane / FDA")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "substanzen_konsens_fact",
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
        "source": "Substanzen-Konsens (NIDA + EMCDDA + EFSA + WHO + Cochrane + FDA)",
        "type": "substance_consensus",
        "results": results,
    }
