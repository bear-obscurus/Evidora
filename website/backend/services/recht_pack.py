"""Recht-/Rechtsmythen-Pack — kuratierte Konsens-Daten zu klassischen
rechtlichen Halbwahrheiten in DACH (DE/AT, mit CH-Verweisen).

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- eu_courts.py (EuGH/EGMR — Rechtsprechung-Topics)
- at_courts.py (VfGH/VwGH — AT-Höchstgerichts-Topics)
- ris.bka.gv.at (AT-Gesetzes-DB)

Während eu_courts und at_courts Höchstgerichts-Urteile abdecken,
fokussiert recht_pack auf populäre RECHTLICHE HALBWAHRHEITEN —
'Notwehr-Mythen', 'Schimmel-Haftungs-Pauschalen', 'Schönheitsreparaturen-
Pflicht', 'Hund-Hund-Pech', 'Erbrecht-Pflichtteil-Umgehung' usw.

Quellen-Mix:
  - Bundesgesetzblatt (BGBl, DE) + RIS (AT-Bundesgesetze)
  - Bundesgerichtshof (BGH) DE + Oberster Gerichtshof (OGH) AT
    Schlüssel-Urteile zu Mietrecht, Arbeitsrecht, Erbrecht
  - Verbraucherzentrale (DE) + Arbeiterkammer (AK Wien, AT)
  - DGB Beratung + Mieterverein (DE)
  - Antidiskriminierungsstelle des Bundes (DE) + Gleichbehandlungs-
    anwaltschaft (AT)
  - Notarkammern für Erbrecht-Hinweise

Topics:
  - notwehr_pauschalen_mythos
  - mietrecht_schimmel_mythos (MIXED — Ursache-abhängig)
  - schoenheitsreparaturen_mythos (BGH 2018: Klauseln oft unwirksam)
  - probezeit_kuendigung_mythos (TEILWEISE TRUE mit Fristen)
  - screenshot_whatsapp_mythos
  - hausrecht_geschaefte_mythos (TRUE-mit-AGG-Einschränkung)
  - erbrecht_pflichtteil_mythos (Vollständige Enterbung FALSCH)
  - arbeitsrecht_pause_mythos (Pflicht-Pausen unbezahlt)
  - kuendigungsschutz_kleinbetrieb_mythos (KSchG nicht in Kleinbetrieb)
  - hund_haftung_mythos (Gefährdungshaftung)

Methodik: substring + composite trigger; bei rechtlichen Klauseln
oft MIXED-Verdict (z.B. Schimmel je nach Ursache). Synthesizer wird
durch kernsatz_fuer_synthesizer instruiert, präzise Bedingungen zu
nennen statt pauschale Aussage.

Politische Sensibilität: niedrig. Pack ist methodisch unproblematisch,
zitiert Gesetzestexte und höchstrichterliche Rechtsprechung.

WICHTIG: Pack ersetzt KEINE anwaltliche Beratung — die Topics zitieren
allgemeine Rechtslage. Im Konfliktfall ist immer Fachanwalt /
Verbraucherzentrale / AK Wien einzubeziehen.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "recht_pack.json",
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


def claim_mentions_recht_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_recht(client=None):
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


async def search_recht(analysis: dict) -> dict:
    empty = {
        "source": "Recht/Rechtsmythen (RIS + BGBl + BGH/OGH + AK + Verbraucherzentrale)",
        "type": "legal_consensus",
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
        label = fact.get("source_label", "RIS / BGBl / BGH/OGH / AK / Verbraucherzentrale")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "recht_konsens_fact",
            "country": "DE/AT",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Recht/Rechtsmythen (RIS + BGBl + BGH/OGH + AK + Verbraucherzentrale)",
        "type": "legal_consensus",
        "results": results,
    }
