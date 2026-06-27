"""AMS-Arbeitsmarktdaten via WIFO-Pressespiegel — AT-spezifische
Arbeitslosen-Eckdaten nach AMS-Methodik (nationale Berechnung).

DATENQUELLE: AMS Arbeitsmarktservice Österreich Jahresbericht 'Die
Arbeitsmarktlage 2024' (Februar 2025, Folder-Format) + AMS Spezial-
themen zum Arbeitsmarkt + WIFO-Konjunktur-Bezug (zitiert im AMS-
Bericht-Vorwort). Sekundär-Zitierung von Behörden-Daten (CC-0-aehnlich)
über die AMS-Forschungsabteilung. Primär-PDF: 001_JB-2024.pdf.

USE-CASE-LÜCKE die geschlossen wird: AT-spezifische Arbeitslosenquote
nach AMS-Methodik (national, inkl. registrierte AL) ist in politischen
Debatten häufiger zitiert als die ILO-Eurostat-Zahl von Statistik
Austria. Existierende services/arbeitsmarkt_pack.py:ams_quote_realitaet_
konsens deckt die METHODIK-Diskussion (AMS vs. Eurostat) ab; dieser
Service liefert die KONKRETEN WERTE pro Jahr + Bundesland + Sektor +
Altersgruppe + Langzeit-Bilanz + Stellenmarkt.

Pack-Snapshot 2026-05-23 enthält 10 Facts:
  - ams_at_quote_2024_gesamt — AT-AL-Quote 7,0 % (Frauen 6,4 %,
    Männer 7,5 %), Bestand 297.851 + Schulungen 75.524 = 373.376
  - ams_at_bundeslaender_konsens — 9 Bundesländer (Wien 11,4 % bis
    Salzburg 4,2 %), Bestand-Werte mit Veränderung 2024 vs. 2023
  - ams_vs_eurostat_diff_konsens — Methodik + reale Zahlen-Differenz
    2024 (7,0 % AMS vs. 4,8 % Eurostat = 2,2 PP)
  - ams_at_alterstruktur_konsens — Jugendliche/Haupterwerbsalter/50+
    + Bildungs-Aufschluesselung
  - ams_at_langzeit_konsens — Langzeit-AL (>12 Mo) + Langzeit-Be-
    schaeftigungslos (weiter gefasst) + Wien-Spitze 37 %
  - ams_at_stellenmarkt_konsens — offene Stellen + Lehrstellen +
    Bundeslaender-Andrang-Spread (Wien 4,4 vs. Tirol 0,3)
  - ams_at_beschaeftigung_konsens — unselbst. Beschäftigte 3,96 Mio
    + Bundeslaender-Aufschluesselung (Wien +1,0 %, OÖ -0,6 %)
  - ams_at_zeitreihe_konsens — 2015-2024 Zeitreihe + Corona-Schock
    (2020 +28,5 %) + AL-Quote-Vergleich 2019 7,4 % vs. 2024 7,0 %
  - ams_at_branche_konsens — Sektor-Aufschluesselung (Warenerz.
    +17,8 % vs. Gesundheit +2,0 %)
  - ams_at_quellen_konsens — Quellen-Vermerk + Aktualitaets-Hinweis

Komplementär zu existierenden Quellen:
  - services/arbeitsmarkt_pack.py:ams_quote_realitaet_konsens —
    deckt die METHODIK-Diskussion AMS vs. Eurostat ab. Dieser Service
    liefert die KONKRETEN VALUES pro Jahr/Bundesland/Sektor.
  - services/arbeitsmarkt_pack.py:fachkraeftemangel_empirie_konsens —
    deckt Mangel-vs.-Luecke-Diskussion ab. Pack hier komplementiert
    mit offenen-Stellen-Anzahl + Lehrstellen-Andrang.

GUARDRAILS — Politik-Tabu (siehe project_political_guardrails.md):
  - Pack hält strikt die 4 Tabus ein:
    * Keine Bewertung 'AL-Quote hoch/niedrig ist gut/schlecht'
    * Keine Partei-Schuld-Zuweisung (Arbeitsmarkt-Politik ist
      politisch sensitiv)
    * Keine Prognosen über zukünftige AL-Entwicklung
    * Nur deskriptive Zahl-Aussagen + Methodik-Caveat
  - Kernsatz_fuer_synthesizer enthält STETS den Methodik-Caveat:
    'AMS-Quote umfasst registrierte Arbeitslose inkl. Schulungs-
    Teilnehmer und ist systematisch HÖHER als die ILO-Quote'
  - KEIN harter STRUKTURELL-FALSCH-Marker — Pack ist deskriptiv-only.

Static-First-Topic-Pattern: substring/composite-Match (siehe
services/_topic_match.py) mit Reranker-Backup-Fallback.

LIZENZ-Vermerk: AMS-Daten = Behörden-Daten unterliegen typischer
CC-0-ähnlicher Open-Data-Lizenz (data.gv.at-Schnittstelle). Sekundär-
Zitierung mit URL + Datum erlaubt. In jedem result-display_value wird
'Quelle: AMS via Jahresbericht 2024' explizit gemacht.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items, stamp_provenance

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "ams_wifo.json",
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


def claim_mentions_ams_wifo_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_ams_wifo(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Pack ist DESKRIPTIV-only — KEIN STRUKTURELL-FALSCH-Marker. Falls
    in zukünftigen Updates Mythen-Widerlegungen aufgenommen werden
    (z.B. 'AMS verschönert die Statistik' -> FALSE), greift der Helper
    automatisch. Aktuell wird nur der kernsatz_fuer_synthesizer als
    erste Zeile geliefert + alle data-Felder als k:v-Zeilen.
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_ams_wifo(analysis: dict) -> dict:
    empty = {
        "source": "AMS Arbeitsmarktservice Österreich Jahresbericht 2024 + AMS Spezialthema zum Arbeitsmarkt + WIFO-Konjunktur-Bezug + Eurostat Labour Force Survey",
        "type": "labor_market_at_ams_methodik",
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
                         "AMS Arbeitsmarktservice Österreich Jahresbericht 2024 + WIFO-Konjunktur-Bezug")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "ams_wifo_fact",
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
        "source": "AMS Arbeitsmarktservice Österreich Jahresbericht 2024 + AMS Spezialthema zum Arbeitsmarkt + WIFO-Konjunktur-Bezug + Eurostat Labour Force Survey",
        "type": "labor_market_at_ams_methodik",
        "results": stamp_provenance(results, matches),
    }
