"""Tierhaltung-Pack — kuratierte Konsens-Daten zu Haustier-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: veterinärmedizinisch-statistische Tierhaltung-
Konsens-Aussagen (ÖTK/Bundestierärztekammer/WSAVA/AAFCO/FAO/EFSA-
gestützt).

Topics (10):
  - hund_schokolade_toxizitaet_konsens (TRUE — Theobromin toxisch,
    LD ~100-250 mg/kg, dunkle Schokolade gefährlicher)
  - katze_milch_mythos (FALSE — adulte Katzen meist laktose-
    intolerant, Kuhmilch nicht artgerecht)
  - veganer_hund_diet_kontroverse (NUANCED — Hund mit Supplementen
    möglich BVA 2023; Katze als obligate Carnivore NICHT)
  - welpe_schlafbedarf_konsens (TRUE — 18-20h normal, WSAVA Puppy
    Guidelines)
  - wildschwein_tollwut_kontroverse (FALSE für DE/AT — seit 2008
    tollwutfrei, FLI; relevante Risiken sind ASP + Aujeszky)
  - barf_rohfutter_kontroverse_mythos (NUANCED — Salmonellen-Risiko
    21-25% kommerzielles Roh-Fleisch, WSAVA + FDA + AAFCO Vorsicht
    bei Familien mit vulnerablen Mitgliedern)
  - katzen_neunte_leben_mythos (FALSE — Folklore, biologisch keine
    Basis)
  - hund_ein_jahr_sieben_mensch_jahre_mythos (FALSE — Wang 2020
    Cell Sys: nicht-linear, AVMA Größen-Tabelle)
  - katzen_landung_fuesse_konsens (TRUE-mit-Nuance — Drehreflex real,
    aber High-Rise-Syndrome Whitney 1987 paradoxer Höhepunkt 2-7 Stock)
  - pferdefleisch_konsum_eu_konsens (TRUE — legal in EU, FAO
    nährwertreich, kulturell variabel)

Quellen-Mix: ÖTK Österreichische Tierärztekammer, Bundestierärzte-
kammer DE, WSAVA Global Nutrition Committee + Puppy Development
Guidelines, AAFCO Association of American Feed Control Officials,
FAO, EFSA, ASPCA Animal Poison Control, AVMA, Merck Veterinary
Manual, FLI Friedrich-Loeffler-Institut, AGES Österreich, RKI,
EU-Verordnungen 853/2004 + 1099/2009, peer-reviewed Studien (Whitney
1987 JAVMA, Wang 2020 Cell Systems, Knight 2022 PLoS ONE, Lefebvre
2008 J Am Vet Med Assoc, Pedrinelli 2017 J Anim Physiol Anim Nutr,
Vnuk 2004 J Feline Med Surg, BVA Position 2023).

Politische Sensibilität: niedrig — meist veterinärmedizinisch
unkontrovers. Bei BARF + veganer Tierfütterung leicht polarisiert,
wir bleiben bei differenzierter wissenschaftlicher Position.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "tierhaltung_pack.json",
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


def claim_mentions_tierhaltung_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_tierhaltung(client=None):
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


async def search_tierhaltung(analysis: dict) -> dict:
    empty = {
        "source": "Tierhaltung-Konsens (ÖTK + Bundestierärztekammer + WSAVA + AAFCO + FAO + EFSA + ASPCA)",
        "type": "pet_care_consensus",
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
        label = fact.get("source_label", "ÖTK / Bundestierärztekammer / WSAVA / AAFCO / FAO / EFSA")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "tierhaltung_konsens_fact",
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
        "source": "Tierhaltung-Konsens (ÖTK + Bundestierärztekammer + WSAVA + AAFCO + FAO + EFSA + ASPCA)",
        "type": "pet_care_consensus",
        "results": results,
    }
