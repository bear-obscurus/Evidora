"""Lebensmittel-Pack — kuratierte Konsens-Daten zu Lebensmittel-
Sicherheits-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: lebensmittelhygiene-statistische Konsens-Aussagen
(BfR/EFSA/FDA/RKI/AAP/ÖLMB-gestützt). Komplementär zum existierenden
ernaehrungs_pack (das eher Hausmythen wie 5-Sek-Regel adressiert).

Topics (10):
  - aufgewaermter_spinat_nitrit_konsens (NUANCED — bei korrekter
    Lagerung sicher; alter Mythos aus Vor-Kühlschrank-Zeit)
  - pilze_aufwaermen_konsens (NUANCED — bei zügiger Kühlung +
    Wiedererhitzung sicher; biogene Amine bei Lagerungs-Fehlern)
  - kartoffel_solanin_grun_konsens (NUANCED — EFSA 2020 ARfD
    1 mg/kg KG; Mengen-Abhängigkeit)
  - mikrowelle_ei_explosion_konsens (TRUE — Dampf-Druck-Aufbau,
    FDA + AAP Verbrennungs-Verletzungen)
  - honig_baby_botulismus_konsens (TRUE — kein Honig vor 12
    Monaten, AAP/RKI/WHO konsistent)
  - kuehlschrank_eier_lagerung_konsens (NUANCED — EU-Eier mit
    Cuticula vs. USA-Eier gewaschen; regionaler Unterschied)
  - mhd_verbrauchsdatum_konsens (TRUE — MHD ≠ Verbrauchsdatum;
    EU-Verordnung 1169/2011)
  - wuerstchen_kinder_erstickungsrisiko_konsens (TRUE — AAP-Daten:
    17% pädiatrische Erstickungs-Hospitalisierungen, Längs-Schnitt-
    Empfehlung)
  - kuehlkette_unterbrochen_konsens (TRUE — FDA Danger Zone 5-65 °C,
    2-Stunden-Regel)
  - auftauen_zimmertemperatur_mythos (FALSE — Außenseite erreicht
    Bakterien-Wachstums-Bereich, Kühlschrank-Auftauen sicherer)

Quellen-Mix: BfR Bundesinstitut für Risikobewertung, EFSA European
Food Safety Authority, FDA Food Safety, RKI Robert Koch-Institut,
AAP American Academy of Pediatrics, ÖLMB Österreichisches Lebens-
mittelbuch, BLE Bundesanstalt für Landwirtschaft und Ernährung,
DGE Deutsche Gesellschaft für Ernährung, Verbraucherzentrale, WHO,
USDA Food Safety, NHS UK, EU-Verordnung 1169/2011 + 589/2008,
peer-reviewed Studien (Nakano 1990 J Pediatr, BMEL 2022 Studie,
EFSA Glycoalkaloids Risk Assessment 2020), Foodsharing, WWF
Lebensmittel-Verschwendungs-Daten.

Politische Sensibilität: sehr niedrig — alle lebensmittelhygiene-
statistisch belegt.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "lebensmittel_pack.json",
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


def claim_mentions_lebensmittel_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_lebensmittel(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_lebensmittel(analysis: dict) -> dict:
    empty = {
        "source": "Lebensmittel-Sicherheit-Konsens (BfR + EFSA + FDA + RKI + AAP + ÖLMB)",
        "type": "food_safety_consensus",
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
        label = fact.get("source_label", "BfR / EFSA / FDA / RKI / AAP / ÖLMB")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "lebensmittel_konsens_fact",
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
        "source": "Lebensmittel-Sicherheit-Konsens (BfR + EFSA + FDA + RKI + AAP + ÖLMB)",
        "type": "food_safety_consensus",
        "results": results,
    }
