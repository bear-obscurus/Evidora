"""Mobilität-Pack — kuratierte Konsens-Daten zu Verkehrs- + E-Mobilitäts- +
Verkehrswende-Mythen + Halbwahrheiten in DACH (DE/AT/CH).

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Verkehrs-Politik-Mythen
(E-Auto-Reichweite, ÖBB-Pünktlichkeit, Tempolimit 130, Klimaticket-
Bilanz, SUV-Sicherheit, Wasserstoff-PKW, ÖPNV-Ausbau, Diesel-Skandal-
Kosten, Tempo-30-Wirkung, Auto-Subventionen, LKW-Maut-Wirkung, Bahn-
Investitionen, Park-Verkehr, Lade-Infrastruktur).

Topics (14):
  - e_auto_reichweite_konsens (ADAC Wintertest 2024 + ICCT 2024 —
    Real-Reichweite ~70-80 % WLTP, Schnelllader-Netz-Ausbau)
  - oebb_puenktlichkeit_konsens (ÖBB-Berichte 2024 — Personen ~95 %
    pünktlich, Fern 88-92 %, vs. DB Fern 64 %)
  - tempolimit_130_konsens (UBA + Helmholtz 2023 — CO2 -1,9 Mio t/
    Jahr, Unfälle -22 %)
  - klimaticket_bilanz_konsens (BMK Bilanz 2024 — 250.000 Tickets,
    ~30 % Modal-Shift bei Pendlern)
  - suv_sicherheit_mythos (IIHS US 2024 + BASt 2023 — Insassen-Schutz
    +, Fußgänger-Tot-Risiko +2-3×)
  - wasserstoff_pkw_mythos (Fraunhofer ISE + Agora Verkehrswende
    2024 — H2-PKW 25-30 % Effizienz vs. BEV 70-80 %)
  - oepnv_ausbau_empirie_konsens (VDV + KCW 2024 — Investitions-
    Bedarf 64 Mrd EUR bis 2030)
  - diesel_skandal_kosten_konsens (UBA 2024 + EU-Kommission — VW-
    Skandal ~33 Mrd EUR Bußgelder, 6.000-10.000 vorzeitige Tode/Jahr)
  - tempo_30_wirkung_konsens (Helsinki + Brussels + Bremen 2018-2023
    — Schwerverletzte -30-50 %, Reisezeit-Effekt minimal)
  - auto_subventionen_konsens (FÖS 2024 + UBA — DE 28-65 Mrd EUR/
    Jahr klima-schädliche Subventionen)
  - lkw_maut_wirkung_konsens (BAG + ASFINAG 2024 — DE ~7 Mrd EUR/
    Jahr, AT ~1,8 Mrd EUR/Jahr)
  - bahn_investition_de_at_konsens (Allianz pro Schiene 2024 — AT
    270 EUR/Person, DE 120 EUR/Person, CH 440 EUR/Person)
  - parken_mythos (Knoflacher 2018 + UVP — Park-Suchverkehr ~30 %
    Stadt-Verkehr, Auto steht 23/24 h)
  - lade_infrastruktur_de_at_konsens (BNetzA 2024 + EU AFIR — DE
    100k Ladepunkte, AT 21k, EU AFIR alle 60 km bis 2026)

Quellen-Mix: ADAC + ICCT International Council on Clean Transportation
+ ÖBB-Konzern-Berichte + Deutsche Bahn Pünktlichkeits-Statistik + UBA
Umweltbundesamt DE + Helmholtz-Institut + BMK Bundesministerium
Klimaschutz AT + IIHS Insurance Institute for Highway Safety + BASt
Bundesanstalt für Straßenwesen + Fraunhofer ISE + Agora Verkehrswende
+ VDV Verband Deutscher Verkehrsunternehmen + KCW Berater + EU-
Kommission Diesel-Bilanz + FÖS Forum Ökologisch-Soziale Marktwirtschaft
+ BAG Bundesamt für Güterverkehr + ASFINAG + Allianz pro Schiene +
BNetzA Bundesnetzagentur + EU AFIR Alternative Fuels Infrastructure
Regulation 2023/1804 + peer-reviewed Forschung (Knoflacher TU Wien
2018, Helsinki/Brussels/Bremen Tempo-30-Studien).

Politische Sensibilität: HOCH — Pack hält strikt die 3 Tabus aus
project_political_guardrails.md ein. Pack adressiert AUSSCHLIESSLICH
empirisch falsifizierbare Mythen + Halbwahrheiten, NICHT normative
Werte-Fragen ('Auto ist gut/schlecht', 'Verkehrswende ist richtig',
'Tempolimit ist Bevormundung'). Pack erkennt empirische Forschungs-
Streitigkeiten explizit an wo sie existieren.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "mobilitaet_pack.json",
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


def claim_mentions_mobilitaet_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_mobilitaet(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (IST EMPIRISCH FALSCH / IST DIFFERENZIERT FALSCH
    / KEINE EVIDENZ FÜR etc.) — siehe services/_struct_marker.py.
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_mobilitaet(analysis: dict) -> dict:
    empty = {
        "source": "Mobilität-Konsens (ADAC + ICCT + ÖBB + DB + UBA + Helmholtz + BMK + IIHS + BASt + Fraunhofer ISE + Agora Verkehrswende + VDV + KCW + EU-Kommission + FÖS + BAG + ASFINAG + Allianz pro Schiene + BNetzA + EU AFIR)",
        "type": "mobility_consensus",
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
                         "ADAC + ICCT + ÖBB + DB + UBA + Helmholtz + BMK + IIHS + BASt + Fraunhofer ISE + Agora Verkehrswende + VDV + KCW + FÖS + BAG + ASFINAG + Allianz pro Schiene + BNetzA + EU AFIR")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "mobilitaet_konsens_fact",
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
        "source": "Mobilität-Konsens (ADAC + ICCT + ÖBB + DB + UBA + Helmholtz + BMK + IIHS + BASt + Fraunhofer ISE + Agora Verkehrswende + VDV + KCW + EU-Kommission + FÖS + BAG + ASFINAG + Allianz pro Schiene + BNetzA + EU AFIR)",
        "type": "mobility_consensus",
        "results": stamp_provenance(results, matches),
    }
