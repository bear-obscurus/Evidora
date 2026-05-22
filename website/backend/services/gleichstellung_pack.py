"""Gleichstellung-Pack — kuratierte Konsens-Daten zu Gleichstellung +
strukturellen Ungleichheiten zwischen den Geschlechtern.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch-deskriptive Konsens-Aussagen zu
Gleichstellung — Pay Gap, Frauenquote-Wirksamkeit, MINT-Anteil,
Femizide AT/DE, sexualisierte Gewalt Dunkelziffer, Gender Care Gap,
Frauen in Politik, Sexismus-Erfahrungen, Vereinbarkeit Familie/Beruf,
EIGE Gender Equality Index.

Topics (10):
  - gender_pay_gap_konsens (NUANCED — unbereinigt 12-18 % vs.
    bereinigt ~6 %; strukturell, nicht nur Lifestyle)
  - frauenquote_wirksamkeit_konsens (NUANCED — empirisch-Anteils-
    wirksam, neutraler Performance-Effekt; keine pro/contra-Wertung)
  - mint_frauen_anteil_konsens (FALSE auf 'biologische Begabung' —
    Stereotype-Threat-Forschung + internationaler Variabilitäts-
    Vergleich)
  - femizide_at_de_konsens (TRUE auf strukturelle Asymmetrie —
    BMI/BKA Bundeslagebild, ~85 % Täter Partner/Ex-Partner)
  - sexualisierte_gewalt_dunkelfeld_konsens (FALSE auf 'falsche
    Anzeigen Hauptproblem' — Dunkelfeld-Faktor 5-10, Falsch-Anzeigen
    2-10 %)
  - gender_care_gap_konsens (TRUE auf 52 % Mehr-Care-Arbeit Frauen,
    DESTATIS Zeitverwendungserhebung 2022/23)
  - frauen_in_politik_konsens (NUANCED — Parlaments-Anteile messbar,
    Glasdecke kommunal stark, EIGE Power-Domain EU-weit schlechteste)
  - sexismus_erfahrungen_konsens (FALSE auf 'verschwunden' —
    FRA 2024 33 % Belästigung, Eurobarometer 75 %)
  - vereinbarkeit_familie_beruf_konsens (TRUE auf strukturellen
    Karrierebruch — IAB child penalty DE 50-60 %, Skandinavien-
    Vergleich 21-31 %)
  - gender_equality_index_konsens (FACTUAL — EIGE 2024 EU-27 70,2;
    DE 70,8 Platz 11, AT 68,8 Platz 13)

Quellen-Mix: EIGE European Institute for Gender Equality
(Gender Equality Index 2024 + DGS Database), Eurostat
(Pay Gap + Education + Employment), OECD (Gender Wage Gap +
Family Database), FRA Fundamental Rights Agency (Violence-
Against-Women-Survey 2024), BMI Österreich (Sicherheitsbericht),
BKA Deutschland (Bundeslagebild Partnerschaftsgewalt), Statistik
Austria + DESTATIS (Zeitverwendungserhebung), IAB
(Mutterschaft + Karriere), BMFSFJ (Elterngeld + Sexismus-Studie),
Antidiskriminierungsstelle DE, IPU Inter-Parliamentary Union,
peer-reviewed Studien (Steele Aronson 1995, Spencer Steele Quinn
1999, Stoet Geary 2018, Kleven et al. 2019, Post Byron 2015,
Ahern Dittmar 2012, Lisak Miller 2010, Campbell 2003 Lancet).

Politische Sensibilität: mittel-hoch — Pack distanziert sich
explizit von normativen Wertungen ("Quote ist gut/schlecht",
"Frauen sollen MINT machen") und präsentiert nur empirische
Studienlage. Hält die 3 Tabus aus project_political_guardrails.md
(keine Partei-Bewertung, keine Wahlprognosen, keine selbst-
definierte Links/Rechts-Klassifizierung) konsequent ein.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "gleichstellung_pack.json",
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


def claim_mentions_gleichstellung_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_gleichstellung(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_gleichstellung(analysis: dict) -> dict:
    empty = {
        "source": "Gleichstellung-Konsens (EIGE + Eurostat + OECD + FRA + BMI/BKA + Statistik Austria + DESTATIS + UN Women)",
        "type": "gender_equality_consensus",
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
                         "EIGE / Eurostat / OECD / FRA / BMI / BKA / Statistik Austria / DESTATIS")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "gleichstellung_konsens_fact",
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
        "source": "Gleichstellung-Konsens (EIGE + Eurostat + OECD + FRA + BMI/BKA + Statistik Austria + DESTATIS + UN Women)",
        "type": "gender_equality_consensus",
        "results": results,
    }
