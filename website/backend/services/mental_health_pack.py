"""Mental-Health-Pack — kuratierte Konsens-Daten zu klassischen
Psychiatrie-/Psychotherapie-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: medizinisch-statistische Psychiatrie-Konsens-
Aussagen (peer-reviewed, DGPPN/NICE/Cochrane/APA-gestützt). Bewusst
RAUS: politisch polarisierte Mental-Health-Themen (Trans-Gesundheit,
Geschlechtsidentitäts-Therapie, Trauma-Anerkennung Soldaten/Migranten
— diese fallen unter Political Guardrails).

Topics (10):
  - depression_charakterschwaeche_mythos (FALSE — DGPPN, NIMH, WHO,
    biopsychosoziale Ätiologie + 40% Heritabilität)
  - antidepressiva_abhaengigkeit_mythos (FALSE/MIXED — NICE NG222,
    DSM-5-Suchtkriterien NICHT erfüllt; Discontinuation aber real)
  - psychotherapie_wirkt_nicht_mythos (FALSE — Cuijpers 2020 World
    Psychiatry n=365 RCTs, Effektstärke d=0,7-0,9)
  - werther_effekt_papageno_mythos (FALSE/NUANCED — WHO Mediendienst
    2017, Niederkrotenthaler 2010 BJP)
  - borderline_klischee_mythos (FALSE — DGPPN S2-Leitlinie 2022, DSM-
    5-TR/ICD-11; Geschlechts-Verhältnis nicht 75% w sondern ~1:1)
  - adhs_ueberdiagnose_mythos (FALSE — Polanczyk 2014 Int J Epidemiol
    Meta n=175 Studien, 30 Jahre konstant; Faraone 2015)
  - psychopharmaka_persoenlichkeit_mythos (FALSE — Tang 2009 Arch Gen
    Psychiatry, Big-Five-Stabilität; Symptom- nicht Trait-Veränderung)
  - traum_unterbewusst_freud_mythos (FALSE — Freud nicht falsifizier-
    bar; Stickgold 2013 Nat Rev, REM-Konsolidierung)
  - selbstmord_kuendigung_mythos (FALSE und gefährlich — Robins 1959,
    Pompili 2010 Crisis: 70-80% kommunizieren Vorfeld-Absicht)
  - essstoerung_willens_mythos (FALSE — DGPPN S3 2018, Bulik 2019/21
    GWAS, Heritabilität 50-70%, Anorexia höchste psychiatrische
    Mortalität SMR ~5-6)

Quellen-Mix: DGPPN S3-Leitlinien, NIMH (US National Institute of
Mental Health), WHO ICD-11/Mediendienst, Cochrane Common Mental
Disorders Group, APA DSM-5-TR + Division 12, NICE Guidelines,
Niederkrotenthaler Wiener Werkstätte für Suizidforschung, Linehan-
Institute (DBT), Faraone World Federation of ADHD, Bulik Lancet
Psychiatry GWAS, Cuijpers World Psychiatry Meta-Analysen, Tang Arch
Gen Psychiatry, Stickgold Nature Reviews, Hobson Activation-Synthesis,
Pompili Crisis, Arcelus Arch Gen Psychiatry Mortality Meta.

Politische Sensibilität: niedrig bis mittel (Borderline + ADHS in
Pop-Diskurs polarisiert, aber wir bleiben streng bei meta-analytischer
Evidenz). Hohe Lehrer-Relevanz für Schul-/Erziehungs-Kontext (ADHS,
Depression bei Jugendlichen, Suizidprävention, Essstörung).
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "mental_health_pack.json",
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


def claim_mentions_mental_health_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_mental_health(client=None):
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


async def search_mental_health(analysis: dict) -> dict:
    empty = {
        "source": "Mental-Health-Konsens (DGPPN + NIMH + Cochrane + APA + NICE + WHO)",
        "type": "mental_health_consensus",
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
        label = fact.get("source_label", "DGPPN / NIMH / Cochrane / APA / NICE / WHO")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "mental_health_konsens_fact",
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
        "source": "Mental-Health-Konsens (DGPPN + NIMH + Cochrane + APA + NICE + WHO)",
        "type": "mental_health_consensus",
        "results": results,
    }
