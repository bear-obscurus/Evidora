"""Arbeitsmarkt-Pack — kuratierte Konsens-Daten zu Arbeitsmarkt-Mythen
+ Halbwahrheiten in DACH (DE/AT/CH).

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Arbeitsmarkt-Mythen
(Mindestlohn-Beschaeftigung, 4-Tage-Woche, Homeoffice-Produktivitaet,
Burnout-ICD-Status, Fachkraefte-Mangel, Gewerkschafts-Lohn-Effekt,
KI-Job-Verdraengung, Migration-Lohn-Effekt, Karenz-Lohn-Verlust,
Generation Z, AMS-Quote-Methodik, Gender Pay Gap, Pflege-Mangel,
Scheinselbststaendigkeit Plattform-Arbeit).

Topics (14):
  - mindestlohn_arbeitsplatzverlust_konsens (Card/Krueger 1994 AER +
    Cengiz et al. 2019 QJE — Mindestlohn-Erhoehungen ohne messbare
    Job-Verluste in low-wage sectors)
  - vier_tage_woche_konsens (Iceland Trial 2015-2019 + UK 4-Day-Week
    Pilot 2022 Cambridge/Boston-College — 92 % Unternehmen behielten
    bei, Burnout -71 %)
  - homeoffice_produktivitaet_konsens (Bloom 2015 QJE Ctrip + Bloom/
    Han/Liang 2024 Nature Hybrid — Hybrid 2-3 Tage WFH optimal)
  - burnout_praevalenz_konsens (WHO ICD-11 QD85 'Berufs-Phaenomen'
    KEINE Krankheit + AT AK Wien 20 % Symptome 2024)
  - fachkraeftemangel_empirie_konsens (IAB Arbeits-LUECKE vs.
    Arbeits-MANGEL + WIFO ~250k offene Stellen vs. ~330k Suchende)
  - gewerkschafts_lohneffekt_konsens (Bryson NIESR 10-15 % Premium
    + Card/Lemieux/Riddell 2003 NBER + AT 98 % KV-Abdeckung)
  - ki_job_verdraengung_konsens (Frey/Osborne 2013 Original 47 % vs.
    Arntz et al. 2016 OECD 9 % vs. OECD 2024 ~14 % hoechstes Risiko)
  - migration_arbeitsmarkt_konsens (Dustmann/Frattini 2014 EJ +
    Dustmann/Schoenberg/Stuhler 2017 QJE — Lohn-Effekt -1 % low-wage)
  - karenz_lohnverlust_konsens (Lalive/Zweimueller 2009 QJE + Lalive
    et al. 2014 RES — AT-Reform 1990 als natuerliches Experiment)
  - generation_z_arbeit_mythos (Pew Research + Deloitte Gen Z Survey
    + Eurostat 87-91 % Gen Z arbeiten oder studieren)
  - ams_quote_realitaet_konsens (AMS-registriert 7-8 % vs. Eurostat
    ILO 4-5 % — Methodik-Differenz erklaert ~80 %)
  - gender_pay_gap_konsens (Eurostat unadjusted AT 18,4 % vs.
    adjusted ~5-7 % — Branchen-Konzentration + Teilzeit + Karenz)
  - pflegekraeftemangel_konsens (AT 75-100k 2030, DE 500k 2030 —
    Statistik Austria + Bertelsmann 2022 + DGB)
  - scheinselbststaendigkeit_konsens (BAG 9 AZR 102/20 vom 1.12.2020
    + AT OGH 8 ObA 70/22f vom 24.5.2023 + EuGH Yodel C-692/19)

Quellen-Mix: AMS + WIFO + IHS + IAB + DESTATIS + Statistik Austria
+ AK Wien + DGB + OECD Employment Outlook + Eurostat + Bundesagentur
fuer Arbeit + EIGE + Pew Research + Deloitte + Bertelsmann + WSI +
peer-reviewed Forschung (Card/Krueger 1994 AER, Cengiz et al. 2019
QJE, Bloom et al. 2015 QJE + 2024 Nature, Lalive/Zweimueller 2009
QJE + 2014 RES, Dustmann/Frattini 2014 EJ + Dustmann/Schoenberg/
Stuhler 2017 QJE, Card/Lemieux/Riddell 2003 NBER, Frey/Osborne 2013
+ 2017, Arntz/Gregory/Zierahn 2016 OECD WP 189, Acemoglu/Restrepo
2024 AER P&P, Heinze/Wolf 2010 Journal of Population Economics) +
Rechtsprechung (BAG 9 AZR 102/20, AT OGH 8 ObA 70/22f, EuGH C-692/19,
WHO ICD-11 QD85).

Politische Sensibilitaet: HOCH — Pack haelt strikt die 3 Tabus aus
project_political_guardrails.md ein. Pack adressiert AUSSCHLIESSLICH
empirisch falsifizierbare Mythen + Halbwahrheiten, NICHT normative
Werte-Fragen ('Mindestlohn ist gerecht', 'Gewerkschaften sind gut',
'Migration ist gut/schlecht'). Pack erkennt empirische Forschungs-
Streitigkeiten explizit an wo sie existieren (z.B. Card/Krueger vs.
Neumark/Wascher zum Mindestlohn-Beschaeftigungs-Effekt; Borjas vs.
Card/Dustmann zum Migrations-Lohn-Effekt).
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "arbeitsmarkt_pack.json",
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


def claim_mentions_arbeitsmarkt_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_arbeitsmarkt(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_arbeitsmarkt(analysis: dict) -> dict:
    empty = {
        "source": "Arbeitsmarkt-Konsens (AMS + WIFO + IHS + IAB + DESTATIS + Statistik Austria + AK Wien + DGB + OECD + Eurostat + peer-reviewed Forschung)",
        "type": "labor_market_consensus",
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
                         "AMS + WIFO + IHS + IAB + DESTATIS + Statistik Austria + AK Wien + DGB + OECD + Eurostat + peer-reviewed Forschung")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "arbeitsmarkt_konsens_fact",
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
        "source": "Arbeitsmarkt-Konsens (AMS + WIFO + IHS + IAB + DESTATIS + Statistik Austria + AK Wien + DGB + OECD + Eurostat + peer-reviewed Forschung)",
        "type": "labor_market_consensus",
        "results": stamp_provenance(results, matches),
    }
