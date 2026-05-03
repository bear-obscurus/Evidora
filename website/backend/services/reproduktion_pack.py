"""Reproduktion-Pack — kuratierte Konsens-Daten zu klassischen
Reproduktions-Medizin-Mythen (Sexualkunde-Lehrplan-Relevanz).

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: medizinisch-statistische Fakten (peer-reviewed
Konsens). KEINE politisch polarisierten Themen (Abtreibung, Trans-
Gesundheit, Sexual-Identität — die fallen unter Political Guardrails
oder werden über andere Quellen (NIH/CDC Live-API, OECD) abgedeckt).

Topics (8):
  - spermien_lebensdauer_mythos (FALSE — Wilcox NEJM 1995, 3-5 Tage)
  - fruchtbarstes_fenster_mythos (FALSE — 6 Tage vor Ovulation)
  - eltern_geschlecht_vererbung_mythos (FALSE — Spermium determiniert)
  - antibabypille_gewicht_mythos (FALSE — Cochrane 2014, 49 RCTs)
  - vasektomie_libido_mythos (FALSE — AUA Guideline 2012)
  - menstruation_synchronisation_mythos (FALSE — McClintock 1971
    nicht reproduzierbar; Yang/Schank 2006)
  - stillen_verhuetung_mythos (FALSE/MIXED — LAM nur unter 3
    strengen Kriterien)
  - wechseljahre_alter_mythos (MIXED — Median 51, Spanne 45-55)

Quellen-Mix: NEJM Wilcox 1995, Cochrane Library (Gallo 2014),
ACOG, AUA (American Urological Association), NAMS (North American
Menopause Society), NIH/NICHD/MedlinePlus, WHO Family Planning
Handbook, Bellagio Consensus 1988, Mayo Clinic, Cleveland Clinic,
Yang & Schank 2006 Proc R Soc B, Strassmann 1999 Hum Reprod.

Politische Sensibilität: niedrig bei diesen 8 Topics — alle medizinisch-
statistisch, keine Wert-Urteile. Hohe Lehrer-Relevanz für Sexualkunde-
Unterricht AT/DE Lehrplan + Erwachsenen-Aufklärung.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "reproduktion_pack.json",
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


def claim_mentions_reproduktion_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_reproduktion(client=None):
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


async def search_reproduktion(analysis: dict) -> dict:
    empty = {
        "source": "Reproduktions-Medizin-Konsens (NEJM + Cochrane + ACOG + AUA + NAMS + NIH + WHO)",
        "type": "reproduction_consensus",
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
        label = fact.get("source_label", "NEJM / Cochrane / ACOG / AUA / NAMS / NIH / WHO")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "reproduktion_konsens_fact",
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
        "source": "Reproduktions-Medizin-Konsens (NEJM + Cochrane + ACOG + AUA + NAMS + NIH + WHO)",
        "type": "reproduction_consensus",
        "results": results,
    }
