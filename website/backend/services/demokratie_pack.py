"""Demokratie-Pack — kuratierte Konsens-Daten zu Wahlen + Demokratie-Indizes
+ Demokratie-Verfall-Empirie + AT-Politik-Institutionen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch falsifizierbare Mythen rund um Wahlen,
Wahlsysteme, Demokratie-Verfall, Pressefreiheit, Korruption, Demokratie-
Zufriedenheit. AT-/EU-/Welt-Vergleichs-Empirie.

Topics (12):
  - wahlbeteiligung_at_trends_konsens (BMI 1949 96 % → 2024 77.5 % NR;
    EU-Wahl 2024 ~57 %; OECD-Mittelfeld)
  - briefwahl_manipulation_mythen (VfGH G203/2016 BPräs-Stichwahl
    aufgehoben wegen FORMFEHLER, kein Manipulations-Beweis;
    US-2020-Big-Lie 60+ Court Cases abgelehnt + FBI/DOJ/CISA)
  - volksbegehren_wirkung_empirie_konsens (Art 41 B-VG, 100k Quorum,
    parlamentarische Behandlungs-Pflicht KEIN bindender Effekt;
    50 % erfolgreiche VB → legislative Änderung)
  - at_bundesrat_funktion_konsens (62 Mitglieder, suspensives Veto,
    1 % Einspruchs-Quote, 100 % überstimmt — vs. DE-Bundesrat-Macht)
  - wahlfaelschungs_mythen_konsens (AT 2016 Formfehler nicht Betrug;
    US 2020 60+ abgelehnte Court Cases + FBI/DOJ/CISA Statement;
    Heritage 1.500 Fälle bei Hunderten Mio Stimmen 0.0001 %)
  - mehrheits_verhaeltniswahl_konsens (Duverger's Law + Lijphart 2012
    Patterns of Democracy; AT Gallagher 4.5 vs. UK 23.7)
  - epartizipation_wirkung_konsens (EU-Bürgerinitiative seit 2012,
    1 Mio Quorum, ~10 erfolgreich, 40 % legislativer Effekt;
    Bertelsmann 2020 — 3-5 % aktive Beteiligung; Hindman 2009)
  - demokratie_verfall_empirie_konsens (V-Dem v14 Liberal Democracy
    Index AT 0.78, DE 0.81, HU 0.32; Freedom House FIW 2024 AT 92,
    USA 83, RU 13; AT/DE stabil)
  - pressefreiheit_trends_konsens (RSF 2024: AT 32. Platz von 16.
    2019 wegen Inseraten + ÖVP-Chats; DE 10., USA 55., RUS 162., NK 180.)
  - korruption_index_konsens (Transparency CPI 2024: AT 71/100 Rang 20,
    DE 75 Rang 15, CH 81 Rang 6, DK 90 Rang 1, RUS 26 Rang 154;
    AT-Verschlechterung -6 P. seit 2019)
  - us_wahl_system_faktoide_konsens (Electoral College 538/270;
    Swing-States PA/MI/WI/AZ/GA/NV ~93 Electors; 5 von 60 Wahlen
    Popular ≠ Electoral; Voter-ID 36 Bundesstaaten)
  - at_demokratie_zufriedenheit_konsens (Eurobarometer 2024: AT 67 %
    Demokratie-Zufriedenheit vs. EU-Mittel 53 %; Vertrauen Justiz
    71 %, Politiker:innen 26 %; -9 P. seit 2014, Stabilisierung)

Quellen-Mix: V-Dem Institute (University of Gothenburg) + Freedom House
FIW + Transparency International CPI + Reporters Without Borders RSF +
IDEA Voter Turnout Database + BMI Statistik + Statistik Austria +
AT-VfGH + Eurobarometer + Bertelsmann + peer-reviewed Forschung
(Duverger 1951 'Political Parties', Lijphart 2012 'Patterns of
Democracy', Hindman 2009 'The Myth of Digital Democracy').

Politische Sensibilität: HOCH — Pack hält strikt die 4 Tabus aus
project_political_guardrails.md ein. Pack zitiert externe Demokratie-/
Pressefreiheit-/Korruptions-Indizes (V-Dem + Freedom House + RSF +
Transparency CPI), übernimmt KEINE eigene Klassifikation. AT-VfGH-/
SCOTUS-/EuGH-Entscheidungen werden faktisch zitiert.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "demokratie_pack.json",
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


def claim_mentions_demokratie_cached(claim: str) -> bool:
    if not claim:
        return False
    # Politik-Tabu-Guard 2.0 (Lehrgeld 2026-05-17): Demokratie-Konsens-Pack
    # aggregiert V-Dem + FH + CPI + RSF + IDEA + Eurobarometer — alle
    # Country-Level. Bei Partei+Korruption+Superlativ ohne Anker blockieren,
    # weil Country-Daten dann Partei-Wertung implizieren würden.
    from services._topic_match import is_party_corruption_superlative_claim
    if is_party_corruption_superlative_claim(claim.lower()):
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_demokratie(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_demokratie(analysis: dict) -> dict:
    empty = {
        "source": "Demokratie-Konsens (V-Dem + Freedom House + Transparency CPI + RSF + IDEA + BMI + Statistik Austria + AT-VfGH + Eurobarometer + Bertelsmann)",
        "type": "democracy_consensus",
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
                         "V-Dem + Freedom House + Transparency CPI + RSF + IDEA + BMI + Statistik Austria + AT-VfGH + Eurobarometer + Bertelsmann")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "demokratie_konsens_fact",
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
        "source": "Demokratie-Konsens (V-Dem + Freedom House + Transparency CPI + RSF + IDEA + BMI + Statistik Austria + AT-VfGH + Eurobarometer + Bertelsmann)",
        "type": "democracy_consensus",
        "results": results,
    }
