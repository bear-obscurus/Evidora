"""Sicherheitspolitik-Pack — kuratierte Konsens-Daten zu AT-Neutralität,
NATO-Diskurs + Sicherheitspolitik-Mythen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: empirisch + rechtlich falsifizierbare Sicherheits-
politik-Mythen + Halbwahrheiten (AT-Neutralität, Wehrpflicht, NATO-
Diskurs, EU-Verteidigung, Drohnen-Krieg, Atomwaffen, Bundesheer-
Personal, Sky Shield, Russland-Bedrohung, Krim/Donbas-Völkerrecht,
Cyber-Verteidigung, Hybrid-Krieg).

Topics (12):
  - at_neutralitaet_recht_konsens (BVG 1955 BGBl 211/1955 + EU-
    Beitritt 1995 + VfGH G170/97)
  - wehrpflicht_reform_debatte_konsens (Volksbefragung 20.1.2013
    59,7 % für Wehrpflicht; Wehrdienst 6 Monate; ~17.000
    Grundwehrdiener/Jahr)
  - nato_mitgliedstaaten_at_diskurs_konsens (NATO 32 Mitglieder seit
    7.3.2024; AT-Beitritt erfordert BVG-Aufhebung + Volks-
    abstimmung; Eurobarometer ~63 % gegen NATO)
  - eu_militaerfonds_pesco_konsens (PESCO 11.12.2017 + EDF 7,953 Mrd
    EUR 2021-2027 + Strategic Compass 21.3.2022; AT 9 von 60
    PESCO-Projekten)
  - drohnen_krieg_empirie_konsens (~15.000 US-Strikes 2002-2024,
    ~1.500-2.500 Zivilist-Tote; Targeted Killing umstritten;
    BVerwG 25.11.2020 Ramstein-Mit-Verantwortung DE)
  - atomwaffen_faktoide_konsens (SIPRI 2024: ~12.121 Atomwaffen, RU
    5.580 + USA 5.044 = 90 %; 9 Atomwaffen-Staaten; AT TPNW-
    Ratifizierung 8.5.2018 + Wien-Konferenz 2022)
  - bundesheer_personal_krise_konsens (Soll 55.000 vs. Ist 46.000
    2024 -16 %; Verteidigungsbudget 1,0 % BIP; Aufbauplan 2032
    für 1,5 % BIP)
  - sky_shield_beitritts_diskurs_konsens (AT-ESSI-Beitritt 7.7.2023;
    neutralitäts-rechtlich umstritten; KEINE VfGH-Vorlage 2024)
  - russland_bedrohungs_empirie_konsens (DSN 2024 + RAND 2024:
    Heeres-Capability degradiert, Cyber + Hybrid stabil; ~2.000
    AT-Cyberangriffe 2024)
  - krim_donbas_ukraine_voelkerrecht_konsens (UN-GA-Res 68/262 vom
    27.3.2014: 100/11/58; ES-11/4 vom 12.10.2022: 143/5/35; ICJ
    16.3.2022)
  - cyber_verteidigung_at_konsens (NIS-Gesetz 2018 + NIS2 2024;
    Cybersicherheits-Strategie 2021; BVT-Affäre 2018-2019 + DSN-
    Reform 2021)
  - hybrid_krieg_realitaet_konsens (NATO Strategic Concept 29.6.2022
    + EU Hybrid Toolbox 21.6.2022 + Hybrid CoE Helsinki seit
    2017; Egisto-Ott-Affäre 2024)

Quellen-Mix: AT BVG 1955 Neutralitätsgesetz + AT-VfGH + B-VG +
UN-Resolutionen + UN-GA-Res 68/262 + UN-GA-Res ES-11/4 + ICJ +
EuGH + BVerwG + NATO Strategic Concept 2022 + EU PESCO + EU
Strategic Compass + EU Hybrid Toolbox + EU NIS-Richtlinien + SIPRI
+ RAND + New America Foundation + Bureau of Investigative
Journalism + DSN + BMLV + BMI + BMF + Eurobarometer + ICRC +
Hybrid CoE Helsinki + peer-reviewed Forschung.

Politische Sensibilität: SEHR HOCH — Pack hält strikt die 3 Tabus
aus project_political_guardrails.md ein PLUS zusätzliche Pack-
spezifische Tabus: KEINE Pro/Contra-NATO-Beitritt-Wertung, KEINE
Pro/Contra-Russland-Wertung außer dokumentiertem völker-
rechtlichen Faktum (Krim-Annexion + Donbas-Quasi-Annexion = UN-GA-
Resolutions-illegal), KEINE Pro/Contra-Aufrüstungs-Wertung. Pack
adressiert AUSSCHLIESSLICH empirisch + rechtlich falsifizierbare
Mythen + Halbwahrheiten — KEINE normative Werte-Fragen.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "sicherheitspolitik_pack.json",
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


def claim_mentions_sicherheitspolitik_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_sicherheitspolitik(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_sicherheitspolitik(analysis: dict) -> dict:
    empty = {
        "source": "Sicherheitspolitik-Konsens (AT BVG 1955 + AT-VfGH + B-VG + UN-Resolutionen + ICJ + EuGH + NATO Strategic Concept 2022 + EU PESCO + EU Strategic Compass + EU Hybrid Toolbox + SIPRI + RAND + DSN + BMLV + BMI + Eurobarometer)",
        "type": "security_policy_consensus",
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
                         "AT BVG 1955 + AT-VfGH + UN-Resolutionen + ICJ + NATO Strategic Concept 2022 + EU PESCO + SIPRI + RAND + DSN + BMLV + BMI")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "sicherheitspolitik_konsens_fact",
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
        "source": "Sicherheitspolitik-Konsens (AT BVG 1955 + AT-VfGH + B-VG + UN-Resolutionen + ICJ + EuGH + NATO Strategic Concept 2022 + EU PESCO + EU Strategic Compass + EU Hybrid Toolbox + SIPRI + RAND + DSN + BMLV + BMI + Eurobarometer)",
        "type": "security_policy_consensus",
        "results": results,
    }
