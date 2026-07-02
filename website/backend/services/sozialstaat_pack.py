"""Sozialstaat-Pack — kuratierte Konsens-Daten zu AT-Sozialleistungen
(welfare state) Mythen + Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare AT-Sozialstaat-Mythen
(Mindestsicherung-Trittbrett, Familienbeihilfe-Wirkung, Pflegegeld-
Stufen, Kinderbetreuungsgeld, Pensions-Generationen, Wohnbeihilfe,
Sozialhilfe-Reform 2019, Notstandshilfe-Reform 2018, Studienbeihilfe,
Bildungskarenz, Reha-Geld, Pflegende Angehoerige, Familienbonus Plus).

Topics (13):
  - mindestsicherung_trittbrett_mythos (Statistik Austria 290k Bezieher
    + 25 % Aufstocker, WIFO/IHS-Studientradition zur Anreiz-Empirie)
  - familienbeihilfe_wirkung_konsens (BMF FB seit 1955, ~1,8 Mio
    Kinder, WIFO/IHS Geburtenrate-Effekt ~+0,05 Kinder/Frau)
  - pflegegeld_stufen_konsens (BMSGPK 7 Stufen 175-2061 EUR, ~470k
    Bezieher, ~80 % daheim, BPGG 1993)
  - kinderbetreuungsgeld_konsens (BMSGPK Pauschal/einkommensabhaengig,
    Vater-Anteil 22 %, Lalive/Zweimueller 2009 AER)
  - pension_generationen_mythos (PVA ASVG 22,8 %, WIFO Generationen-
    bilanz 2024, EU Ageing Report 2024)
  - wohnbeihilfe_konsens (Bundeslaender-Sache, ~280k Bezieher, Wien
    140k, AK Wien Wirkungs-Studie 2023)
  - sozialhilfe_at_spezial_konsens (Reform 2019 Sozialhilfe-Grundsatz-
    gesetz, VfGH G164/2019 + G65/2020 verfassungswidrig)
  - notstandshilfe_streichung_diskurs (OeVP-FPOe 2018 Reform-Plan,
    Ibiza-Stop, AMS 150k Bezieher unveraendert 2024)
  - studienbeihilfe_konsens (StuPF 715-925 EUR, ~50k Bezieher, OECD
    EaG 2024 mittleres Niveau, OeH Erwerbstaetigkeit 75 %)
  - bildungskarenz_wirkung_konsens (AlVG seit 1998, WIFO 2018
    +3-5 PP Beschaeftigung, Reform 2024 Verschaerfung)
  - reha_geld_konsens (Sozialrechts-Aenderungs-Gesetz 2014, PVA
    22k Bezieher, WIFO -40 % I-Pensions-Neuzugang)
  - pflegende_angehoerige_konsens (Statistik Austria 947k Personen,
    75 % Frauen, 80 % daheim, Bertelsmann + OeROK Vergleich)
  - familienbonus_plus_konsens (BMF seit 2019, max 2k EUR/Kind/Jahr,
    WIFO 2022 Verteilung 70 % oberes Drittel)

Quellen-Mix: Statistik Austria + BMSGPK + WIFO + IHS + AK Wien + AMS
+ PVA + AT-VfGH + OECD Family Database + OECD Education at a Glance
+ Eurostat ESSPROS + EU Ageing Report 2024 + Bertelsmann Stiftung
Familienpolitik-Monitor + OeROK + StuPF Studienbeihilfenbehoerde +
BMF + Sozialministerium + peer-reviewed Forschung (Lalive/Zweimueller
2009 AER, Bock-Schappelwein/Famira-Muehlberger WIFO 2022, Pichelmann
IHS-Tradition).

Politische Sensibilitaet: HOCH — Pack haelt strikt die 3 Tabus aus
project_political_guardrails.md ein. Pack adressiert AUSSCHLIESSLICH
empirisch falsifizierbare Mythen + Halbwahrheiten, NICHT normative
Werte-Fragen ('Mindestsicherung soll hoeher sein', 'Familienbonus
ist gerecht'). VfGH-Entscheidungen werden faktisch zitiert (G164/2019
Asylberechtigte-Differenzierung, G65/2020 Kinder-Saetze-Staffelung)
— ohne politische Wertung der Reform 2019. Pack distanziert sich
explizit von Pro/Contra-Reform-Bewertungen am Ende jedes Kernsatzes.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "sozialstaat_pack.json",
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


def claim_mentions_sozialstaat_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_sozialstaat(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_sozialstaat(analysis: dict) -> dict:
    empty = {
        "source": "Sozialstaat-Konsens (Statistik Austria + BMSGPK + WIFO + IHS + AK Wien + AMS + PVA + AT-VfGH + OECD + Eurostat ESSPROS + Bertelsmann)",
        "type": "welfare_state_consensus",
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
                         "Statistik Austria + BMSGPK + WIFO + IHS + AK Wien + AMS + PVA + AT-VfGH + OECD + Eurostat ESSPROS + Bertelsmann")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "sozialstaat_konsens_fact",
            "country": "AT",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Sozialstaat-Konsens (Statistik Austria + BMSGPK + WIFO + IHS + AK Wien + AMS + PVA + AT-VfGH + OECD + Eurostat ESSPROS + Bertelsmann)",
        "type": "welfare_state_consensus",
        "results": stamp_provenance(results, matches),
    }
