"""Inklusion-Pack — kuratierte Konsens-Daten zu Behinderung, Autismus,
ADHS, Down-Syndrom, Inklusion + Hochsensibilität/Hochbegabung-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Mythen + Halbwahrheiten
rund um Behinderung, Autismus-/ADHS-/Down-Syndrom-Realität, Inklusion
in Schule + Pflegeheim, Hochsensibilität-/Hochbegabung-Faktoide,
Barrierefreiheit-Empirie + UN-BRK-Verpflichtungen.

Topics (12):
  - autismus_mythen_konsens (DSM-5 + ICD-11 + CDC; Wakefield 1998
    Lancet zurückgezogen 2010 wegen Datenfälschung; MMR-Autismus-
    Mythos in Cochrane-Reviews + Madsen 2002 NEJM widerlegt)
  - adhs_mythen_konsens (DSM-5 5+ Symptome ≥6 Monate; Cochrane MPH
    Reviews 2018+2024; Polanczyk 2014 IJE Meta-Analyse)
  - down_syndrom_realitaet_konsens (Lebenserwartung 1980 ~25 J →
    2024 ~60 J; DKHWB-Studien; Bittles/Bower 2007 Am J Med Genet)
  - iq_foerderung_mythen_konsens (Flynn-Effekt 1987 +3 IQ-Punkte/
    Dekade; Trahan 2014 Psychol Bull; Heckman 2007+2014 NBER —
    frühkindliche Förderung 7-10 % ROI)
  - inklusions_quoten_effekte_konsens (Hattie 2009 Visible Learning;
    UN-BRK 2006 Art 24; Schweden-Modell + Finnland-Modell)
  - pflegeheim_realitaet_at_konsens (Statistik Austria 2024:
    ~75.000 Pflegeheim-Bewohner:innen; 24h-Betreuung ~70.000
    Pflegerinnen; AT GuKG-Novelle 2022)
  - icf_vs_medizinisches_modell_konsens (WHO ICF 2001 + UN-BRK 2006
    Art 1 — bio-psycho-soziales Modell George Engel 1977 statt
    Defizit-orientiertes medizinisches Modell)
  - un_brk_konvention_konsens (CRPD 2006 + AT-Ratifikation BGBl III
    155/2008; AT-Staatenbericht 2023; UN-Ausschuss-Empfehlungen)
  - autismus_therapie_wirkung_konsens (Lovaas 1987 ABA + Early Start
    Denver Model Dawson 2010 Pediatrics + TEACCH; ASAN 2021
    kritisch zu klassischer ABA — Selbstvertretungs-Position)
  - hochsensibilitaet_hochbegabung_mythen_konsens (Aron 1996 HSP
    + Greven 2019 Neurosci Biobehav Rev Meta-Analyse; HSP ist
    Persönlichkeits-Trait, KEINE Diagnose im DSM-5/ICD-11)
  - barrierefreiheit_empirie_at_konsens (AT BGStG 2006 BGBl I
    82/2005 + EU EAA 2019/882 + WCAG 2.1; Wiener-Linien-
    Barrierefreiheits-Statistik)
  - inklusion_schule_vs_sonderschule_konsens (Statistik Austria
    Schulstatistik 2024; BMBWF Inklusions-Politik; Hattie 2009+
    2023 — d=0.40 für Inklusions-Settings vs. Sonderbeschulung)

Quellen-Mix: American Psychiatric Association DSM-5 + WHO ICD-11 +
WHO ICF 2001 + UN-Behindertenrechtskonvention CRPD 2006 + CDC ASD/
ADHD-Statistik + DKHWB Down-Syndrom-Studien + Cochrane Reviews +
Hattie 2009/2023 'Visible Learning' Meta-Analyse + James Flynn 1987
+ Trahan 2014 Psychol Bull + James Heckman 2007 NBER + Elaine Aron
1996 + Greven 2019 Neurosci Biobehav Rev + Lovaas 1987 + Dawson
2010 Pediatrics + TEACCH-Programme + ASAN Autistic Self Advocacy
Network + Statistik Austria + BMSGPK + AT BGStG + AT GuKG + EU
European Accessibility Act + W3C WCAG + DPI Disabled Peoples
International + ÖAR Österreichischer Behindertenrat + peer-reviewed.

Politische Sensibilität: HOCH — Pack hält strikt die 4 Tabus aus
project_political_guardrails.md ein. Pack adressiert AUSSCHLIESSLICH
empirisch falsifizierbare Mythen + Halbwahrheiten, NICHT normative
Werte-Fragen ('Inklusion ist gerecht', 'Sonderschulen sind besser').
Pack respektiert Selbstvertretungs-Verbände (ASAN, ATEMPO, ÖAR) +
deren Position zu klassischer ABA-Therapie. Pack erkennt empirische
Forschungs-Streitigkeiten explizit an wo sie existieren. Pack
distanziert sich strikt von 'Behinderung-als-Krankheit'-Diskursen
zugunsten des bio-psycho-sozialen Modells (ICF + UN-BRK).
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "inklusion_pack.json",
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


def claim_mentions_inklusion_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_inklusion(client=None):
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


async def search_inklusion(analysis: dict) -> dict:
    empty = {
        "source": "Inklusion-Konsens (DSM-5 + ICD-11 + UN-BRK 2006 + WHO ICF 2001 + CDC + DKHWB + Cochrane + Hattie + Flynn + Heckman + Aron + Lovaas + TEACCH + Statistik Austria + BMSGPK + AT BGStG + EU EAA + ASAN + ÖAR)",
        "type": "inclusion_consensus",
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
        label = fact.get("source_label",
                         "DSM-5 + ICD-11 + UN-BRK 2006 + WHO ICF 2001 + CDC + DKHWB + Cochrane + Hattie + Flynn + Heckman + Aron + Lovaas + TEACCH + Statistik Austria + BMSGPK + AT BGStG + EU EAA + ASAN + ÖAR")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "inklusion_konsens_fact",
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
        "source": "Inklusion-Konsens (DSM-5 + ICD-11 + UN-BRK 2006 + WHO ICF 2001 + CDC + DKHWB + Cochrane + Hattie + Flynn + Heckman + Aron + Lovaas + TEACCH + Statistik Austria + BMSGPK + AT BGStG + EU EAA + ASAN + ÖAR)",
        "type": "inclusion_consensus",
        "results": results,
    }
