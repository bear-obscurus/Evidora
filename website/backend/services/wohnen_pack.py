"""Wohnen-Pack — kuratierte Konsens-Daten zu Wohn-Mythen + Halbwahrheiten
in DACH (DE/AT/CH).

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Wohnungsmarkt-Mythen
(Mietregulierung, Leerstand, Sozialwohnungsbau, Frei-Markt-These,
AirBnB-Wirkung, Migration-Mietpreis, Eigentumsquote, Genossenschaft,
Mietendeckel-Berlin, Wohnungs-Mangel, Wohngeld, Spekulation,
Wohnkosten-Belastung, Leerstandsabgabe).

Topics (14):
  - mietregulierung_investitionseffekt_konsens (DIW 2024 + BVerfG
    Berlin Mietendeckel 2021 — DIFFERENZIERT)
  - leerstand_umverteilung_mythos (DESTATIS Zensus 2022: 1,9 Mio
    Leerstand, davon 70 % friktionell + Sanierung; geografisch
    nicht umverteilbar)
  - sozialwohnungsbau_konsens (DE Bestand 1990 4 Mio → 2024 1,1 Mio
    -72 %; AT Wiener Modell stabil 60 %)
  - wohnungsmarkt_freier_markt_mythos (Glaeser/Gyourko 2018 +
    Hilber LSE — Bauleit-Planung 36-50 % Mietpreis-Beitrag)
  - airbnb_wohnungsmarkt_konsens (DIW Berlin 2018 + WIFO Wien 2024
    — Top-Lagen 3-5 %, Gesamt-Stadt 0,5-1,5 %)
  - migration_wohnungspreis_mythos (IAB + Bundesbank 2024 — 10-20 %
    Beitrag, NICHT primär)
  - eigentumsquote_dach_eu_konsens (Eurostat: DE 49 % EU-Schluss,
    AT 55 %, EU-Mittel 70 % — strukturell-historisch)
  - genossenschaftswohnen_konsens (AT GBV 750k Wohnungen, DE 2,2
    Mio, Wien-Modell UN-Habitat 2018)
  - mietendeckel_berlin_bilanz_konsens (DIW 2021: -3,5 % Bestands-
    miete, BVerfG-Aufhebung 2021 wegen Bundes-Kompetenz)
  - wohnungs_mangel_de_konsens (Pestel 700k + IW Köln 600k-1 Mio,
    geografisch konzentriert Boom-Städte)
  - wohngeld_faulenzer_konsens (DESTATIS 2 Mio Empfänger nach Plus-
    Reform 2023, 80 % arbeiten/Rentner)
  - spekulations_preistreiber_konsens (DIW 2024: ~10-15 %, NICHT
    Hauptursache; Großinvestoren Vonovia + Co. Effekt 5-15 %)
  - wohnkosten_50_prozent_konsens (Eurostat: DE 12,1 % >40 %-
    Schwelle, OBERES MITTELFELD; Skandinavien 4-6 %)
  - leerstandsabgabe_wirkung_konsens (Vorarlberg + Tirol seit 2024,
    erste Bilanz ~600 Wohnungen zurück; Frankreich TLV +10-15 %
    Mietangebot)

Quellen-Mix: DESTATIS Zensus 2022 + Statistik Austria + BMWSB DE +
Wien-Wohnen + WIFO + IHS + DIW + IFO + Empirica + Pestel-Institut
+ IW Köln + Eurostat + OECD Housing + Mieterbund + AK Wien + GBV
+ BBU + BBSR + Bundesbank + Senatsverwaltung Berlin + Land
Vorarlberg + Land Tirol + peer-reviewed Forschung (Glaeser/Gyourko
2018 NBER, Hilber LSE 2021, Diamond/McQuade/Qian 2019 AER, INSEE
2018 Frankreich Leerstandsabgabe).

Politische Sensibilität: HOCH — Pack hält strikt die 3 Tabus aus
project_political_guardrails.md ein. Pack adressiert AUSSCHLIESSLICH
empirisch falsifizierbare Mythen + Halbwahrheiten, NICHT normative
Werte-Fragen ('Eigentum ist besser als Miete', 'Sozialwohnungsbau
ist gerecht'). Pack erkennt empirische Forschungs-Streitigkeiten
explizit an wo sie existieren.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "wohnen_pack.json",
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


def claim_mentions_wohnen_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_wohnen(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_wohnen(analysis: dict) -> dict:
    empty = {
        "source": "Wohnen-Konsens (DESTATIS Zensus 2022 + Statistik Austria + BMWSB + Wien-Wohnen + DIW + IFO + Pestel + IW Köln + Eurostat + Empirica + GBV + AK Wien)",
        "type": "housing_consensus",
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
                         "DESTATIS + Statistik Austria + BMWSB + Wien-Wohnen + DIW + IFO + Pestel + IW Köln + Eurostat + Empirica")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "wohnen_konsens_fact",
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
        "source": "Wohnen-Konsens (DESTATIS Zensus 2022 + Statistik Austria + BMWSB + Wien-Wohnen + DIW + IFO + Pestel + IW Köln + Eurostat + Empirica + GBV + AK Wien)",
        "type": "housing_consensus",
        "results": results,
    }
