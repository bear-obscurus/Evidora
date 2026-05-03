"""Migrations-Pack — kuratierte Konsens-Daten zu klassischen Migrations-/
Asyl-politischen Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

POLITISCHE GUARDRAILS (siehe project_political_guardrails.md):

1. Wir bewerten KEINE politischen Bewegungen oder Parteien pauschal.
2. Wir zitieren BEHÖRDLICHE Einstufungen (Verfassungsschutz, BMI),
   nicht eigene Wertungen.
3. Bei Statistiken werden METHODIK-CAVEATS prominent zitiert
   (z.B. PKS-Kategorie 'Nichtdeutsche', Anzeige-Quoten-Verzerrungen,
   Aufenthaltsstatus-Mischung).
4. Bei MIXED-Bewertungen wird die Differenzierung explizit gemacht
   (z.B. Integration: Generation/Herkunft/Bildungsstand).
5. Antisemitismus-Klausel: 'Großer Austausch'-Theorie wird via BfV +
   bpb + IDZ Jena als rechtsextrem + antisemitisch eingestuft —
   Behörden-Einstufung zitieren, nicht selbst eingruppieren.

Komplementaer zu:
- frontex.py (EU-Außengrenzen-Daten)
- UNHCR (UN-Flüchtlings-Daten)
- factbook_eu_pakt_* (EU-Asylpakt-Topics in at_factbook)
- bamf_asyl_de_2025_2026 (im dach_factbook)
- dach_asylbLG_counter (im dach_factbook)
- verschwoerungen_pack.py (allgemeine Verschwörungs-Narrative)

Während die anderen Quellen ZAHLEN + AKTUELLE EREIGNISSE liefern,
fokussiert migration_pack auf strukturelle Narrativ-Halbwahrheiten:

Topics (Probe-Pack, 6 Topics):
  - demographic_replacement_mythos ('Großer Austausch' als rechtsex.)
  - migration_kriminalitaet_pauschal_mythos (PKS-Methodik-Caveats)
  - sozialmagnet_asyl_mythos (Pull-Faktor empirisch nicht nachweisbar)
  - abschiebungen_simpel_mythos (operative Komplexität, nicht Politik)
  - integration_gescheitert_mythos (IAB-/OECD-Befunde widerlegen)
  - asyl_arbeitsverbot_mythos (3-Monats-Regel, § 61 AsylG)

Quellen-Mix:
  - Bundesamt für Verfassungsschutz (BfV) für Einstufungen rechtsex.
  - bpb (Bundeszentrale für politische Bildung) für Erklär-Dossiers
  - IDZ Jena (Institut für Demokratie und Zivilgesellschaft)
  - BAMF + BAMF-FZ (Bundesamt für Migration und Flüchtlinge)
  - IAB (Institut für Arbeitsmarkt- und Berufsforschung)
  - DIW Berlin (Deutsches Institut für Wirtschaftsforschung) — SOEP
  - OECD International Migration Outlook
  - BKA-PKS Methodik-Hinweise + Wissenschaftler (Feltes, KFN Hannover)
  - Mediendienst Integration (NGO-Faktencheck)
  - Bundestags-Drucksachen (offizielle Antworten auf Anfragen)
  - § 61 AsylG, § 60 AufenthG, Dublin-III-VO als Rechtsgrundlagen

Methodische Disziplin: bei jedem Topic wird der `kernsatz_fuer_synthesizer`
mit der WICHTIGSTEN Methoden-Caveat eröffnet, damit der Synthesizer NICHT
in pauschale Zustimmung oder Ablehnung verfällt — sondern die spezifische
Bedingung, unter der die Aussage TRUE/FALSE/MIXED ist, prominent macht.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "migration_pack.json",
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


def claim_mentions_migration_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_migration(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    parts: list[str] = []
    for key, val in d.items():
        if key == "context":
            continue
        if isinstance(val, str) and val.strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)


async def search_migration(analysis: dict) -> dict:
    empty = {
        "source": "Migrations-Konsens (BfV + bpb + IAB + DIW + OECD + BKA + Mediendienst)",
        "type": "migration_consensus",
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
        label = fact.get("source_label", "BfV / bpb / IAB / DIW / OECD / BKA / Mediendienst")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "migration_konsens_fact",
            "country": "DE/AT/EU",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Migrations-Konsens (BfV + bpb + IAB + DIW + OECD + BKA + Mediendienst)",
        "type": "migration_consensus",
        "results": results,
    }
