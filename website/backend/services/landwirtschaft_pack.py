"""Landwirtschaft-Pack — kuratierte Konsens-Daten zu Agrar-/Lebensmittelproduktions-Mythen
in DACH (DE/AT/CH) + EU/Welt-Vergleich.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Mythen rund um Agrar-
Wirtschaft, Lebensmittel-Produktion + Klima.

Topics (13):
  - gvo_gentechnik_risiko_konsens (EFSA + Royal Society + WHO Konsens —
    keine erhöhten Gesundheits-Risiken zugelassener GVO; Nicolia 2014
    Meta-Analyse 1.783 Studien; EU NGT-Verordnung 2024)
  - bio_landbau_ertrags_oekobilanz_konsens (Seufert 2012 Nature, Ponisio
    2015, Smith 2019 NatComm — Bio-Yield-Gap 20-25 % gemittelt; AT 26 %
    Bio-Acker-Anteil)
  - glyphosat_empirie_konsens (IARC 2015 Group 2A vs. EFSA/EPA 'unlikely'
    — Hazard-vs-Risk-Methodik-Diff; EU-Genehmigung 2023 +10 Jahre)
  - saatgut_konzentration_konsens (ETC Group 'Food Barons 2022' — Top-6
    ~70 % kommerzieller Markt; FAO 2010 ~75 % Pflanzen-Vielfalt-Verlust
    Multi-Faktor)
  - agrar_subventionen_at_konsens (~7 Mrd EUR/Jahr inkl. ÖPUL +
    Bergbauern; 80-20-Pareto bei GAP-Direkt-Zahlungen)
  - regionale_versorgung_mythen_konsens (AT-Selbstversorgung Statistik
    AT 2024: Fleisch 109 %, Milch 165 %, Gemüse 56 %, Obst 47 %,
    Energie 36 %)
  - vertical_farming_realitaet_konsens (Wagenigen UR 2022: 50-100×
    höherer Energieverbrauch; Branchen-Pleiten 2022-2024 Infarm/
    AeroFarms/Plenty)
  - massentierhaltung_empirie_konsens (AT 2024 Statistik: 2.6 Mio
    Schweine, 2.5 Mio Rinder, 21 Mio Hühner; Hofgrößen-Median AT
    deutlich kleiner als DE/NL/DK)
  - pestizid_rueckstaende_konsens (EFSA Annual Report 2024: 96.4 %
    EU-MRL-konform; AT 99.4 %; Mehrfach-Rückstände typisch Median
    2-3 Wirkstoffe)
  - duengemittel_importabhaengigkeit_konsens (Russland/Belarus 35-40 %
    EU-Stickstoff-Importe vor 2022 → ~25 % 2024; Preis-Spike 2022-2023
    × 4-5; AT 460k t Stickstoff/Jahr 80 % Import)
  - welternaehrung_knappheit_mythen_konsens (FAO SOFI 2024: 733 Mio
    unter-ernährt; Welt-Produktion theoretisch genug für 12 Mrd;
    Verteilungs-/Verschwendungs-/Konflikt-Faktoren)
  - bauern_sterben_empirie_konsens (AT-Höfe-Schwund 1951-2024 -64 %;
    1.4 % BIP, 4 % Beschäftigte; EU-Trend ähnlich)
  - klima_landwirtschaft_konsens (UBA AT 2024: 8.0 Mt CO2e ~10 %
    Gesamt-Emissionen; Klima-Anpassung Wein-Verlagerung + Mais-
    Hitze-Stress)

Quellen-Mix: AGES + EFSA + IARC + WHO + BMVL + BOKU Wien + Statistik
Austria + FAO + IPES-Food + ETC Group + Wagenigen UR + Royal Society
+ ÖPUL + Bergbauern-Förderung + EU-Kommission + peer-reviewed
Forschung (Seufert 2012 Nature, Ponisio 2015 ProcRoyalSocB, Smith
2019 NatComm).

Politische Sensibilität: MITTEL — Pack hält strikt die 3 Tabus aus
project_political_guardrails.md ein. Pack adressiert AUSSCHLIESSLICH
empirisch falsifizierbare Mythen + Halbwahrheiten, NICHT normative
Werte-Fragen ('Bio ist gut/schlecht', 'GVO ist gut/schlecht'). Pack
erkennt empirische Forschungs-Streitigkeiten explizit an wo sie
existieren (IARC vs. EFSA Glyphosat-Bewertung).
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "landwirtschaft_pack.json",
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


def claim_mentions_landwirtschaft_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_landwirtschaft(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_landwirtschaft(analysis: dict) -> dict:
    empty = {
        "source": "Landwirtschaft-Konsens (AGES + EFSA + IARC + WHO + BMVL + BOKU + Statistik Austria + FAO + IPES-Food + ETC Group + Wagenigen UR + Royal Society)",
        "type": "agriculture_consensus",
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
                         "AGES + EFSA + IARC + WHO + BMVL + BOKU + Statistik Austria + FAO + IPES-Food + ETC Group + Wagenigen UR + Royal Society")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "landwirtschaft_konsens_fact",
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
        "source": "Landwirtschaft-Konsens (AGES + EFSA + IARC + WHO + BMVL + BOKU + Statistik Austria + FAO + IPES-Food + ETC Group + Wagenigen UR + Royal Society)",
        "type": "agriculture_consensus",
        "results": stamp_provenance(results, matches),
    }
