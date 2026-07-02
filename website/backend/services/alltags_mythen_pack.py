"""Alltags-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
Gesundheits-/Alltags-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: medizinisch-statistische Alltags-Konsens-Aussagen
(NHS/AAO/AASM/Cochrane/Mayo Clinic/NIH-gestützt). Diese Topics
adressieren Mythen mit höchster Verbreitungs-Quote im Alltag.

Topics (10):
  - acht_glaeser_wasser_mythos (FALSE — Valtin 2002 AJP, EFSA;
    Durst-Mechanismus zuverlässig)
  - lesen_im_dunkeln_augen_mythos (FALSE — AAO, NHS; nur Eye Strain
    keine dauerhaften Schäden)
  - acht_stunden_schlaf_zwingend_mythos (NUANCED — Hirshkowitz 2015
    Sleep Health, individueller Bedarf 7-9h Range)
  - kalter_boden_blasenentzuendung_mythos (FALSE — NHS; UTI ist
    bakteriell E. coli, nicht Kälte-induziert)
  - linkshaender_sterblichkeit_mythos (FALSE — Coren 1991 NEJM
    Cohort-Effekt-Artefakt; Aggleton 1993 Lancet widerlegte)
  - glas_kaffee_dehydrierung_mythos (FALSE — Killer 2014 PLoS One;
    Kaffee netto Flüssigkeitsbilanz-positiv)
  - nasse_haare_erkaeltung_mythos (FALSE — CDC/NHS/RKI; Erkältung
    ist viral, Kälte modulierend nicht kausal)
  - gehirn_10_prozent_mythos (FALSE — fMRT/PET-Studien zeigen
    alle Hirnregionen aktiv)
  - vollmond_schlaf_geburten_mythos (FALSE — Periti 1998, Schwartz
    1991, Confirmation-Bias-Artefakte)
  - wundbrand_jod_mythos (FALSE — NICE Wound Care, Cochrane;
    Cytotoxizität verzögert Heilung)

Quellen-Mix: NHS UK, American Academy of Ophthalmology (AAO),
American Academy of Sleep Medicine (AASM), National Sleep Foundation,
Mayo Clinic, NIH, Cochrane Reviews, CDC, RKI, EFSA, NICE Guidelines,
peer-reviewed Studien (Valtin 2002 American J Physiology, Hirshkowitz
2015 Sleep Health, Killer 2014 PLoS One, Coren 1991 NEJM, Aggleton
1993 Lancet, Boyd 2008 Scientific American, Periti 1998 Minerva
Ginecologica, Schwartz 1991 J Reprod Med).

Politische Sensibilität: sehr niedrig — alle medizinisch-statistisch
unkontrovers. Hohe Lebensalltag-Relevanz für Familie, Beruf, Schule.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "alltags_mythen_pack.json",
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


def claim_mentions_alltags_mythen_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_alltags_mythen(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_alltags_mythen(analysis: dict) -> dict:
    empty = {
        "source": "Alltags-Mythen-Konsens (NHS + AAO + AASM + Mayo Clinic + NIH + Cochrane + RKI)",
        "type": "everyday_myths_consensus",
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
        label = fact.get("source_label", "NHS / AAO / AASM / Mayo Clinic / NIH / Cochrane")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "alltags_mythen_konsens_fact",
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
        "source": "Alltags-Mythen-Konsens (NHS + AAO + AASM + Mayo Clinic + NIH + Cochrane + RKI)",
        "type": "everyday_myths_consensus",
        "results": results,
    }
