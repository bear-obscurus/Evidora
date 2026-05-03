"""Eurobarometer-Service — kuratierte Konsens-Daten zu EU-Bürger-
Einstellungen, basierend auf den Standard- und Special-Eurobarometer-
Umfragen der Europäischen Kommission und des Europäischen Parlaments.

Static-First-Aggregate-Service. Eurobarometer hat kein offen zugäng-
liches API für Roh-Daten — die Standard-Umfragen werden als PDF-Reports
+ interactive dashboards publiziert. Der Service liefert daher
kuratierte Eckwerte zu den am häufigsten zitierten Topics + verweist
auf die Original-Reports.

Komplementaer zu:
- v_dem (akademischer Demokratie-Index)
- transparency_international (Korruptions-Index)
- migration_pack (Migrations-Mythen)
- verschwoerungen_pack (Verschwörungs-Narrative)

Topics:
  - eb_eu_vertrauen (Standard EB 102 / Herbst 2024: 51 % EU-Vertrauen,
    höchster Wert seit 2007)
  - eb_top_themen (EU-Bürger-Sorgen + Prioritäten 2024)
  - eb_klimawandel_einstellung (77 % halten Klimawandel für sehr ernst)
  - eb_demokratie_zufriedenheit (Mehrheit zufrieden, Differenzierung
    Demokratie-System vs Regierung)
  - eb_eu_mitgliedschaft (~76 % 'EU-Mitgliedschaft eine gute Sache')
  - eb_einwanderung_einstellung (nuancierte Position: 65 % Sorge um
    unkontrollierte Migration, 60 % Asyl-Pakt-Unterstützung)

Methodische Disziplin: Eurobarometer-Daten sind Bevölkerungs-Stim-
mungen, nicht objektive Wahrheiten. Aussagen über die EU-Bürger-
Mehrheit ('Bürger wollen X') werden mit konkreten EB-Zahlen +
Differenzierung (national, Generation, Bildung) belegt.

Aktualisierung: jährlich (oder bei neuen EB-Veröffentlichungen). Pack
ist primär für Aussagen mit Bezug 'die Bürger / die Mehrheit / die
Europäer'.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "eurobarometer.json",
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


def claim_mentions_eurobarometer_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_eurobarometer(client=None):
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


async def search_eurobarometer(analysis: dict) -> dict:
    empty = {
        "source": "Eurobarometer (Europäische Kommission + EP)",
        "type": "public_opinion_data",
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
        label = fact.get("source_label", "Standard Eurobarometer 102 (Herbst 2024)")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "eurobarometer_fact",
            "country": "EU",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Eurobarometer (Europäische Kommission + EP)",
        "type": "public_opinion_data",
        "results": results,
    }
