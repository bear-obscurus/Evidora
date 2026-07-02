"""Datenschutz-Pack — kuratierte Konsens-Daten zu Datenschutz-, Überwachungs-
und digitalen Bürgerrechts-Mythen + Halbwahrheiten in DACH/EU.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Mythen rund um DSGVO,
Cookie-Banner, Vorratsdatenspeicherung, Snowden-NSA-Programme, AT-Massen-
überwachung / BVT-Reform, Pegasus-Spyware, Klarnamen-Pflicht, Recht auf
Anonymität, Bestandsdaten-Auskunft, Bundestrojaner AT, Smartmeter, Browser-
Fingerprinting.

Topics (12):
  - dsgvo_buerokratie_mythos (EDPB + Bitkom 2024 — KMU-Anteil unter 5 %
    der EU-DSGVO-Bußgelder; 90+ % Großkonzerne)
  - cookie_banner_wirkung_konsens (EuGH C-673/17 Planet49 2019 +
    deutsches TTDSG 2021 + NOYB-Report 2023 — über 75 % Verstöße)
  - vorratsdatenspeicherung_stand_konsens (EuGH C-203/15 Tele2 + AT-VfGH
    G47/2012 + DE seit 2017 ausgesetzt nach BVerwG 6 C 12.18)
  - snowden_nsa_faktoide_konsens (PRISM Section 702 + XKeyScore +
    MUSCULAR + BULLRUN — Original-Dokumente + Pulitzer 2014)
  - massenueberwachung_at_konsens (BVT-U-Ausschuss-Endbericht 2020 +
    DSN-Reform 1.12.2021 + StPO §§ 134/135a)
  - pegasus_spyware_konsens (Citizen Lab + Amnesty Tech 2021-2024 +
    EU PEGA-Endbericht 22.5.2023 + 45+ Länder)
  - klarnamen_pflicht_konsens (Süd-Korea 2007-2012 Cho/Kim 2012 +
    Verfassungs-Urteil 23.8.2012 + NetzDG 2018-Vergleich)
  - anonymitaet_im_netz_recht_konsens (Art. 8 EMRK + BVerfG 1 BvR 1873/13
    27.5.2020 + EuGH Breyer + La Quadrature du Net 2024)
  - bestandsdaten_auskunft_konsens (BVerfG 27.5.2020 + Reform 30.3.2021 +
    Bundesnetzagentur-Statistik 2023)
  - bundestrojaner_at_konsens (Sicherheits-Paket BGBl. I 27/2018 +
    AT-VfGH G72/2019 vom 11.12.2019 + DSN-Berichte 2022/2023)
  - smartmeter_datenschutz_konsens (DSGVO Erwägungsgrund 26 + BSI
    TR-03109 + MsbG-Reform 2023 + AT § 81 ElWOG)
  - browser_fingerprinting_konsens (EFF Panopticlick 2010 +
    Laperdrix et al. ACM 2020 + EDSA-Leitlinien 2/2023)

Quellen-Mix: EuGH + BVerfG + AT-VfGH + Datenschutz-Behörde AT + Bitkom +
NOYB + LfM + BSI + Citizen Lab + Amnesty Tech + EFF + Mozilla + Snowden-
Dokumente + DSN-Berichte + BVT-Untersuchungs-Ausschuss + peer-reviewed
Forschung (Eckersley 2010 PETS, Laperdrix et al. 2020 ACM, Cho/Kim 2012,
Brown/Jones-Yelvington 2018).

Politische Sensibilität: HOCH — Pack hält strikt die 3 Tabus aus
project_political_guardrails.md ein. Pack adressiert AUSSCHLIESSLICH
empirisch falsifizierbare Mythen + Halbwahrheiten, NICHT normative
Werte-Fragen ('DSGVO ist gut/schlecht', 'Anonymität ist gut/schlecht').
Pack zitiert für AT-Themen (BVT-Affäre, Bundestrojaner, DSN) NUR
offizielle Untersuchungs-Ausschuss-Berichte + Verfassungsgerichts-
Erkenntnisse — KEINE Spekulation oder ungeprüfte Behauptung.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "datenschutz_pack.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=None,  # Cosine-Backup deaktiviert (#41): Multi-Topic-Pack, Backup zog themenfremde Claims; Trigger-Abdeckung via claim_phrasings-Battery 100% (2026-07-02)
    )


def claim_mentions_datenschutz_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_datenschutz(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_datenschutz(analysis: dict) -> dict:
    empty = {
        "source": "Datenschutz-Konsens (EuGH + BVerfG + AT-VfGH + DSB AT + Bitkom + NOYB + BSI + Citizen Lab + Amnesty Tech + EFF + Mozilla + Snowden-Dokumente + DSN/BVT-U-Ausschuss + peer-reviewed Forschung)",
        "type": "data_protection_consensus",
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
                         "EuGH + BVerfG + AT-VfGH + DSB AT + Bitkom + NOYB + BSI + Citizen Lab + Amnesty Tech + EFF + Mozilla + Snowden-Dokumente")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "datenschutz_konsens_fact",
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
        "source": "Datenschutz-Konsens (EuGH + BVerfG + AT-VfGH + DSB AT + Bitkom + NOYB + BSI + Citizen Lab + Amnesty Tech + EFF + Mozilla + Snowden-Dokumente + DSN/BVT-U-Ausschuss + peer-reviewed Forschung)",
        "type": "data_protection_consensus",
        "results": stamp_provenance(results, matches),
    }
