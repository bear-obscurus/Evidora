"""Bildungs-Mythen-Pack — kuratierte Konsens-Daten zu klassischen
pädagogischen + neuro-didaktischen Halbwahrheiten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Komplementaer zu:
- education_dach (TIMSS/PIRLS/PISA Daten DE/AT/CH)
- esoterik_pack (Pseudo-Wissenschaft)

Topics (7):
  - lernstile_mythos (FALSE — Pashler 2008)
  - mozart_effekt_mythos (FALSE — Pietschnig 2010 Meta)
  - gehirnhaelften_mythos (FALSE — Nielsen 2013 fMRI)
  - multitasking_mythos (FALSE — Switching-Costs 20-40 %)
  - 10_prozent_gehirn_mythos (FALSE — fMRI/PET zeigen 100 % Nutzung)
  - frueh_lernen_immer_besser_mythos (MIXED — Spielen wichtiger als
    strukturiertes Lernen vor 4-5 Jahren)
  - hattie_visible_learning_konsens (Liefert evidenzbasierte
    Wirksamkeit-Reihenfolge der Unterrichts-Methoden)

Quellen-Mix: Pashler 2008 + APA Top 20 Principles + EEF Toolkit
(UK), Nielsen 2013 PLoS ONE (fMRI Hemisphären-Studie), Pietschnig 2010
PNAS (Mozart-Meta-Analyse), Rubinstein/Meyer/Evans 2001 (Switching-
Costs), NICHD-Längsschnitt + OECD Early Childhood, Hattie Visible
Learning, Marcon 2002 (Spielen vs Akademie-Vorschule).

Politische Sensibilität: niedrig. Pack ist primär bildungs-
wissenschaftlich; politisch unauffällig.

Lehrer-Relevanz (User-Profil!): hoch. Diese Pseudo-Pädagogik (Lernstile,
Mozart-Effekt, Hirn-Hälften, '10 % Gehirn') findet sich häufig in
Lehrer-Fortbildungen + populären Pädagogik-Ratgebern. Pack liefert
empirisch fundierte Gegenargumente.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "bildung_pack.json",
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


def claim_mentions_bildung_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_bildung(client=None):
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


async def search_bildung(analysis: dict) -> dict:
    empty = {
        "source": "Bildungs-Mythen (APA + Hattie + EEF + Pashler 2008 + Nielsen 2013 + OECD ECE)",
        "type": "education_consensus",
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
        label = fact.get("source_label", "APA / Hattie / EEF / Pashler 2008 / OECD")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "bildung_konsens_fact",
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
        "source": "Bildungs-Mythen (APA + Hattie + EEF + Pashler 2008 + Nielsen 2013 + OECD ECE)",
        "type": "education_consensus",
        "results": results,
    }
