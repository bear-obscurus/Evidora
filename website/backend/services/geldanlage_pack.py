"""Geldanlage-Pack — kuratierte Konsens-Daten zu Anlage-Betrug- und
Geldanlage-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: regulatorisch-statistische Geldanlage-Konsens-
Aussagen (BaFin/FCA/SEC/ESMA-gestützt). Bewusst RAUS: spezifische
Coin-/Aktien-Empfehlungen, politisch motivierte Anti-/Pro-Krypto-
Aussagen, individuelle Anlage-Beratung (Beratung nur durch lizensierte
Berater:innen).

Topics (10):
  - krypto_promi_endorsement_mythos (FALSE — BaFin/FCA/SEC, Chainalysis
    2024 ~5,6 Mrd USD Schaden global)
  - daytrading_erfolg_mythos (FALSE — Barber & Odean 2000, Chague 2017
    97% Verlust, ESMA 74-89% CFD-Privatanleger verlieren)
  - garantierte_rendite_mythos (FALSE — Risiko-Rendite-Trade-off,
    Madoff 65 Mrd, Phoenix 700 Mio Schaden)
  - forex_plattform_mythos (FALSE — ESMA 2018 Beschränkung 1:30
    Hebel; auch danach >70% Privat-Verluste)
  - pyramidensystem_mlm_mythos (NUANCED — MLM legal, 99% verdienen
    weniger als Mindestlohn FTC-Daten)
  - gold_inflation_schutz_mythos (NUANCED — Erb & Harvey 2013
    FAJ; Inflations-Korrelation nur in spezifischen Phasen)
  - compound_interest_24_mythos (FALSE — Buffett extreme Ausnahme
    20-22%; S&P 500 7% real, Dalbar Behavior Gap 3-5%)
  - versicherung_kapital_anlage_mythos (FALSE — Stiftung Warentest,
    BdV: 1-3% Rendite-Reduktion durch Kosten; getrennte Lösung besser)
  - fomo_signal_anlage_mythos (FALSE — Cialdini 6 Prinzipien, SEC
    + BaFin rote Flaggen; Hochdruck = Betrugs-Indikator)
  - bitcoin_inflations_safe_haven_mythos (FALSE — Volatilität 70%+
    vs. Gold 15-20%, EZB/IMF: Bitcoin ist Risk-On, nicht Safe-Haven)

Quellen-Mix: BaFin (Bundesanstalt für Finanzdienstleistungsaufsicht),
FCA (UK), SEC (US), ESMA (EU), FMA (Österreich), EZB Working Papers,
IMF Working Papers, Chainalysis 2024 Crypto Crime Report, FTC Multi-
Level Marketing Guidance, Stiftung Warentest, BdV (Bund der
Versicherten), Verbraucherzentrale, Finanztip, DAI (Deutsches Aktien-
institut), Vanguard / Bogleheads, Dalbar QAIB Annual Study, peer-
reviewed Studien (Barber & Odean 2000 J Finance, Chague & De-Losso
2017, Erb & Harvey 2013 Financial Analysts Journal, Baur & Lucey 2010
J Financ Stab, Cialdini Influence 1984), Roy 2024 Energy Economics,
Dimson Marsh Staunton Triumph of Optimists, Bouri 2017.

Politische Sensibilität: niedrig bis mittel (Krypto-/Gold-/MLM-
Themen leicht polarisiert, aber wir bleiben streng bei regulatorisch
+ peer-reviewed Daten). Hohe Schadens-Reduktions-Wirkung wegen
Lebensersparnisse-Risiko bei Anlage-Betrug.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "geldanlage_pack.json",
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


def claim_mentions_geldanlage_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_geldanlage(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_geldanlage(analysis: dict) -> dict:
    empty = {
        "source": "Geldanlage-Konsens (BaFin + FCA + SEC + ESMA + FMA + Stiftung Warentest)",
        "type": "investment_consensus",
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
        label = fact.get("source_label", "BaFin / FCA / SEC / ESMA / Stiftung Warentest")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "geldanlage_konsens_fact",
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
        "source": "Geldanlage-Konsens (BaFin + FCA + SEC + ESMA + FMA + Stiftung Warentest)",
        "type": "investment_consensus",
        "results": results,
    }
