"""WHO World Report on Hearing 2021 — Static-Pack-Service.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md SS3.5.

Hintergrund — Top-30 Position #28 aus dem
metastudien_quellen_inventar_2026_05_16.md. Der WHO World Report on
Hearing 2021 ist ein einmaliger, oeffentlich verfuegbarer PDF-Bericht
(keine Live-API) mit zentralen Faktenzahlen zu globaler Hoer-
Gesundheit. Diese Zahlen werden bislang in Evidora nur unsauber
ueber den allgemeinen WHO-GHO-Service (services/who.py) abgedeckt,
der OData-Indicators-Daten zieht, aber den Report-Kontext nicht
kennt. Deshalb separater Static-Pack-Service.

Quellen-Mix:
  - WHO World Report on Hearing 2021 (Primaer-Quelle)
  - WHO/ITU H.870 Safe-Listening-Standard 2019/2022
  - WHO Environmental Noise Guidelines for the European Region 2018
  - Jarach et al. 2022 (JAMA Neurology, Tinnitus-Meta-Analyse)
  - Livingston et al. 2024 (Lancet Commission on Dementia Prevention)
  - Lin et al. 2023 (Lancet, ACHIEVE Trial)
  - EHIMA EuroTrak (DACH-Hoergeraet-Versorgungs-Daten)
  - EU-Richtlinie 2003/10/EG (Arbeitsplatz-Laermschutz)
  - Joint Committee on Infant Hearing (JCIH) 2019

Topics (~17 facts):
  - hoerverlust_globale_praevalenz, phl_jugend_audio_devices,
    hoerverlust_lmic_verteilung, saeuglings_screening_coverage,
    who_safe_listening_h870, who_environmental_noise_guidelines,
    tinnitus_praevalenz, hoergeraet_versorgungsquote,
    cochlea_implantat_at_versorgung, hoerverlust_oekonomische_kosten,
    hoerverlust_demenz_risiko, otitis_media_kinder,
    presbyakusis_altersbedingt, arbeitsplatz_laerm_hoerverlust,
    praevention_hoerverlust_intervention, ototoxizitaet_medikamente

Lizenz-Hinweis: WHO-Publikationen stehen unter CC BY-NC-SA 3.0 IGO
(Wiederverwendung mit Quellenangabe und identischer Lizenz, nicht-
kommerziell). Faktischer Inhalt (Zahlen, statistische Schaetzungen) ist
nicht urheberrechtlich geschuetzt — wir zitieren Datenpunkte und
verlinken auf die Originalquelle.

Methodisches Caveat (siehe kernsatz_fuer_synthesizer pro fact):
WHO-Zahlen beruhen typisch auf Global-Burden-of-Disease-Modellierung
mit grossen Unsicherheits-Intervallen. Definitions-Schwellen
(disabling = 35 vs. 40 dB) und Self-Reporting-Bias machen Vergleiche
ueber Reports hinweg vorsichtig zu interpretieren.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "who_hearing.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    """Descriptor for the cosine-similarity backup trigger."""
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_who_hearing_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_who_hearing(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    WHO-Hearing-Facts sind ueberwiegend deskriptiv (Praevalenz-Zahlen,
    Schwellwerte), nicht Mythen-widerlegende — der STRUKTURELL-Marker
    aktiviert sich nur, wenn ein kernsatz_fuer_synthesizer explizite
    Override-Tokens enthaelt (siehe services/_struct_marker.py). Bei
    rein deskriptiven Daten greift der Standard-Render ohne Marker.
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_who_hearing(analysis: dict) -> dict:
    empty = {
        "source": "WHO World Report on Hearing 2021 (+ WHO Safe Listening + WHO Noise Guidelines)",
        "type": "who_report",
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
        label = fact.get("source_label", "WHO World Report on Hearing 2021")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "who_hearing_fact",
            "country": "global",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "WHO World Report on Hearing 2021 (+ WHO Safe Listening + WHO Noise Guidelines)",
        "type": "who_report",
        "results": results,
    }
