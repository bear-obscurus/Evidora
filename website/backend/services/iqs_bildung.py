"""IQS Nationaler Bildungsbericht 2024 — Static-First-Topic-Service mit
kuratierten AT-Bildungs-System-Eckwerten (NBB-2024-Snapshot).

Datenquelle: IQS (Institut des Bundes für Qualitätssicherung im
österreichischen Schulwesen) publiziert alle 3-5 Jahre einen
mehrbändigen Nationalen Bildungsbericht. Letzte Edition: NBB 2024
in 3 Bänden (Indikatoren / Controlling / Wissenschaft).

Pack-Snapshot 2026-05-23 enthält 10 Kern-Facts:
  - schueler_eckwerte_at_konsens (Volksschule + Sek-I + Sek-II Bestand)
  - pisa_2022_at_konsens (Mathematik 487 / Lesen 480 / Naturwiss 491)
  - lehrkraefte_pensionierung_at_konsens (132.000 VAE + 32.000 Pension
    bis 2030, -34 % Studienanfänger Lehramt 2014-2024)
  - bildungsausgaben_at_konsens (~5,2 % BIP, OECD-Top-5 pro Schüler:in)
  - ganztagsschule_at_konsens (33 % Pflichtschüler, Wien-Spread)
  - klassengroesse_dach_konsens (AT 19/22 Schüler:innen, OECD 21/23)
  - migrant_leistungs_spread_at_konsens (PISA-Spread 50-75 Punkte,
    DESKRIPTIV ohne Ursachen-Aussage — strikter Politik-Tabu-Guard)
  - tertiaer_quote_at_konsens (42 % AT 25-34 vs. 47 % OECD, duale
    Lehre erklärt Strukturunterschied)
  - lehrplan_reform_2023_at_konsens (kompetenzorientierte Curricula
    + 8 übergreifende Themen seit 2023/24)
  - spf_bestand_at_konsens (5,0 % SPF-Quote, 62 % Inklusion AT-intern;
    komplementär zu services/easie.py EU-Cross-Country)

Komplementär zu existierenden Quellen:
  - services/education_dach.py (TIMSS/PIRLS/PISA-DACH/Lehrermangel/
    Sitzenbleiben) — überlappt teilweise bei PISA + Lehrermangel,
    dieser Pack ergänzt NBB-2024-spezifische Indikatoren + Bundes-
    länder-Detail.
  - services/easie.py (EASIE European Agency Cross-Country-Inklusions-
    Statistik) — dort EU-Vergleich, hier AT-interne Bundesländer.
  - services/bildung_pack.py (Bildungs-Mythen Lernstile/Mozart-Effekt/
    Gehirnhälften) — keine Überlappung.

GUARDRAILS — Politik-Tabu (siehe project_political_guardrails.md):
  - Pack hält strikt die 4 Tabus ein:
    * Keine Partei-Bewertung (auch wenn Bildungs-Debatte politisch ist)
    * Keine Prognose (außer dokumentierten Bedarfs-Prognosen, NBB-
      Quelle muss explizit angegeben sein)
    * Keine eigene normative Klassifikation ("AT-Schulsystem ist gut/
      schlecht/rückständig")
    * Bei Migrations-Hintergrund-Leistungs-Spread: nur DESKRIPTIVE
      Quote, KEIN Ursachen-Statement
  - Wertungs-Frame ("Bildungs-Krise", "Schulsystem versagt") wird im
    kernsatz_fuer_synthesizer explizit dekonstruktiv adressiert.

Static-First-Topic-Pattern: substring/composite-Match (siehe
services/_topic_match.py) mit Reranker-Backup-Fallback.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "iqs_bildung.json",
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


def claim_mentions_iqs_bildung_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_iqs_bildung(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Der NBB-Pack ist überwiegend deskriptiv (keine FALSE-Verdict-
    Overrides), aber wir nutzen denselben Helper für konsistentes
    Rendering. Falls in zukünftigen Updates Mythen-Widerlegungen
    eingebaut werden (z.B. 'AT-Bildungsausgaben sind viel zu niedrig'
    -> FALSE), greift der Helper automatisch.
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_iqs_bildung(analysis: dict) -> dict:
    empty = {
        "source": "IQS Nationaler Bildungsbericht 2024 (Indikatoren + Controlling + Wissenschaft) + OECD Education at a Glance + Statistik Austria",
        "type": "education_indicators_at",
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
                         "IQS Nationaler Bildungsbericht 2024 + OECD Education at a Glance + Statistik Austria")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "iqs_bildung_fact",
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
        "source": "IQS Nationaler Bildungsbericht 2024 (Indikatoren + Controlling + Wissenschaft) + OECD Education at a Glance + Statistik Austria",
        "type": "education_indicators_at",
        "results": stamp_provenance(results, matches),
    }
