"""Welthandel-Pack — kuratierte Konsens-Daten zu Welthandel-/Globalisierung-
Mythen + Halbwahrheiten in DACH (DE/AT/CH) und global.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Welthandel-Mythen
(China-Lieferketten, Made-in-Germany-Realität, TRIPS-Pharma-Patente,
Globalisierung-Gewinner/Verlierer, AT-Export-Quote, EU-Binnenmarkt,
Handelsbilanz-Defizit-Mythen, Brexit-Folgen, RUS-Sanktionen-Wirkung,
Subventions-Kriege, AT-Energie-Importabhängigkeit, China-Solar-Markt).

Topics (12):
  - china_lieferketten_abhaengigkeit_konsens (McKinsey 2023 + BCG
    2024 — AT 8 %, DE 11 %, EU 21 % Importe, Solar/Lithium/Seltene
    Erden Komponenten-Abhängigkeit)
  - made_in_germany_realitaet_konsens (IFO Mecklenburg/Hochmuth
    2023 — VW Tiguan ~30 % deutsche Wertschöpfung, BGH/DIHK 45 %-
    Schwelle für Made-in-Germany Label)
  - trips_pharma_patente_konsens (TRIPS 1995 + Doha 2001 +
    COVID-Vakzin-Waiver MC12 Juni 2022 minimal-Kompromiss; Generika
    -60-80 % Preis-Reduktion)
  - welthandel_gewinner_konsens (Weltbank/FAO 1 Mrd aus extremer
    Armut 1990-2024; Piketty/Saez US-Bottom-50 stagniert seit 1980;
    EU-Mittelschicht +0,5-1,0 % p.a.)
  - at_export_quote_konsens (Statistik Austria — AT 58 % BIP-
    Export, höher als DE 47 %, DE-Top-Markt 28 %)
  - eu_binnenmarkt_wirkung_konsens (Cecchini 1988 +4-7 % BIP
    prognostiziert, real 1992-2024 ~+1,5-2 %; AT-Beitritt 1995
    +0,6 % p.a. WIFO Aiginger/Breuss)
  - handelsbilanz_defizit_mythen_konsens (USA -$773 Mrd 2024;
    Krugman + Heckscher-Ohlin: Saldo = Sparquote-Investitions-
    Differential, NICHT Wettbewerbsfähigkeit)
  - brexit_folge_handel_konsens (Bank of England + LSE Dhingra
    2024 — UK-EU-Handel -10-15 % real, UK-BIP -4-5,5 % vs.
    counterfactual)
  - russland_sanktionen_wirkung_2022_2024_konsens (14 EU-Pakete;
    RUS-Öl-Erlöse 2023 -27 %, 2024 -15 %; EU-Gas RUS-Anteil
    40 %→8 %)
  - subventions_kriege_konsens (USA IRA $369 Mrd 2022 + EU NZIA
    + CRMA Feb/März 2024; CSIS ~$50 Mrd EU→USA Investitions-
    Verschiebung)
  - at_energie_importabhaengigkeit_konsens (E-Control + Statistik
    Austria — AT 73 % Energie-Import; Erdgas RUS 80 %→25 %; Strom
    80 % erneuerbar aber nur 20 % Primärenergie)
  - china_solar_markt_dominanz_konsens (BNEF + IEA — China 80 %
    Module, 95 % Polysilizium, 96 % Wafer; Modul-Preise -94 %
    seit 2010; EU NZIA 2024 Ziel 40 % EU bis 2030)

Quellen-Mix: Statistik Austria + WIFO + IFO + Bank of England + LSE
Centre for Economic Performance + EU-Kommission + Weltbank + FAO
+ IEA + BNEF + WTO + UNCTAD + Boston Consulting Group + McKinsey
+ CSIS + peer-reviewed (Piketty/Saez 2020 WID, Krugman 1996 Pop
Internationalism, Heckscher 1919/Ohlin 1933, Cecchini-Bericht 1988,
Lakner/Milanovic 2016 Elephant Curve, Autor/Dorn/Hanson 2013 China
Shock, Amiti/Redding/Weinstein 2019, Fajgelbaum/Khandelwal 2021,
Dhingra/Sampson LSE 2024, Aiginger/Breuss WIFO 2003/2015).

Politische Sensibilität: HOCH — Pack hält strikt die 3 Tabus aus
project_political_guardrails.md ein. Pack adressiert AUSSCHLIESSLICH
empirisch falsifizierbare Mythen + Halbwahrheiten, NICHT normative
Werte-Fragen ('Globalisierung ist gut/schlecht', 'EU ist gut/
schlecht', 'Trump-Politik ist richtig/falsch'). Pack erkennt
empirische Forschungs-Streitigkeiten explizit an wo sie existieren.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "welthandel_pack.json",
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


def claim_mentions_welthandel_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_welthandel(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_welthandel(analysis: dict) -> dict:
    empty = {
        "source": "Welthandel-Konsens (Statistik Austria + WIFO + IFO + Bank of England + LSE CEP + EU-Kommission + Weltbank + IEA + BNEF + WTO + UNCTAD + McKinsey + BCG + CSIS)",
        "type": "world_trade_consensus",
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
                         "Statistik Austria + WIFO + IFO + Bank of England + LSE CEP + EU-Kommission + Weltbank + IEA + BNEF + WTO + UNCTAD + McKinsey + BCG + CSIS")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "welthandel_konsens_fact",
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
        "source": "Welthandel-Konsens (Statistik Austria + WIFO + IFO + Bank of England + LSE CEP + EU-Kommission + Weltbank + IEA + BNEF + WTO + UNCTAD + McKinsey + BCG + CSIS)",
        "type": "world_trade_consensus",
        "results": stamp_provenance(results, matches),
    }
