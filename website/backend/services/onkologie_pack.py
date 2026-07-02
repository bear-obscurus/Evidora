"""Onkologie-Pack — kuratierte Konsens-Daten zu klassischen Krebs-
Präventions- und -Therapie-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: medizinisch-statistische Onkologie-Konsens-Aussagen
(peer-reviewed, NCI/DKFZ/Cochrane-gestützt). Bewusst RAUS: politisch
polarisierte Krebs-Themen (Krebsregister-Debatten DKR, Cannabis-Krebs
das schon im gesundheits_autoritaeten_pack ist).

Topics (10):
  - mikrowelle_krebs_mythos (FALSE — FDA + WHO + BfS, nicht-ionisierend)
  - zucker_fuettert_krebs_mythos (FALSE/MIXED — Warburg-Effekt erklärt,
    metabolic flexibility; ketogene Diät Cochrane 2018 keine Wirkung)
  - saeure_basen_krebs_mythos (FALSE — Blut-pH 7,35-7,45 homöostatisch,
    Robert Young 2017 strafrechtlich verurteilt)
  - aprikosenkerne_laetril_mythos (FALSE — Cochrane Milazzo 2015,
    Cyanid-Toxizität, Moertel NEJM 1982)
  - alkalisches_wasser_krebs_mythos (FALSE — Magensäure neutralisiert
    sofort, Mayo Clinic + NCCIH)
  - deo_brustkrebs_mythos (FALSE — DKFZ + ACS + BfR; Mirick 2002,
    Hardefeldt 2013)
  - bh_brustkrebs_mythos (FALSE — Singer 'Dressed to Kill' 1995
    methodisch widerlegt; Chen 2014)
  - mistel_krebs_therapie_mythos (MIXED — Cochrane Horneber 2008,
    DGHO/ÖGHO: keine Wirkung Tumor-Progression)
  - vitamin_c_hochdosis_krebs_mythos (FALSE — Pauling-Cameron 1976,
    Mayo RCTs Creagan 1979 + Moertel 1985, NCI PDQ)
  - frueh_erkennung_uebertherapie_konsens (NUANCED — USPSTF/IQWiG/
    Cochrane: Mammographie 50-69 nutzen, PSA mixed, Schilddrüse
    Korea-Lehrstück, Welch 'Less Medicine')

Quellen-Mix: NCI PDQ, DKFZ Krebsinformationsdienst, Cancer Research UK,
American Cancer Society, AICR/WCRF, Cochrane Library (Horneber 2008,
Milazzo 2015, Vollbracht 2014), Moertel CG NEJM 1982 + 1985, Creagan
1979 NEJM, Chen 2014 Cancer Epidemiol Biomarkers Prev, Mirick 2002
JNCI, Ahn 2014 NEJM, Welch HG 'Overdiagnosed' 2011, USPSTF, IQWiG,
DGHO, ÖGHO, FDA, BfR, BfS, WHO IARC, Mayo Clinic, NIH NCCIH.

Politische Sensibilität: niedrig. Hoher Schaden-Reduktions-Wert
(Therapie-Verweigerung-Risiko bei Krebs-Patienten). Hohe Lehrer-
Relevanz für Bio-/Sex-Kunde-Unterricht AT/DE.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "onkologie_pack.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=None,  # Cosine-Backup deaktiviert (Mythen-Pack-Welle 2026-07-02, analog #41): Marker-Packs kontaminieren via Cosine; Battery 100%
    )


def claim_mentions_onkologie_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_onkologie(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (IST FALSE / IST EMPIRISCH WIDERLEGT / etc.)
    — siehe services/_struct_marker.py.
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_onkologie(analysis: dict) -> dict:
    empty = {
        "source": "Onkologie-Konsens (NCI + DKFZ + Cochrane + Cancer Research UK + USPSTF + IARC)",
        "type": "oncology_consensus",
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
        label = fact.get("source_label", "NCI / DKFZ / Cochrane / Cancer Research UK / USPSTF")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "onkologie_konsens_fact",
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
        "source": "Onkologie-Konsens (NCI + DKFZ + Cochrane + Cancer Research UK + USPSTF + IARC)",
        "type": "oncology_consensus",
        "results": results,
    }
