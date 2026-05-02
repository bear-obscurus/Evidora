"""Geschichte / historischer Mythen-Konsens — kuratierte Daten.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.
Deckt klassische Geschichts-Mythen ab, zu denen mainstream-medizinische
und naturwissenschaftliche Datenbanken (PubMed, Cochrane) nichts liefern
(Geschichts-Forschung wird dort nicht indiziert), und Semantic Scholar /
OpenAlex zwar Geschichts-Papers haben, aber zu spezifischen DACH-Mythen
oft nichts direkt Passendes finden.

Der Stress-Test vom 2026-05-02 zeigte 4 false-negatives bei genau diesen
Mythen-Profilen (Stalin-Hitler-Gleichsetzung, DDR-Mauer-Apologie,
Marshall-Plan-Politik, Wikinger-Hörner) plus 1 false-positive bei einer
Wording-Frage (Putin-Aussagen Februar 2022).

Quellen-Mix: DÖW (Dokumentationsarchiv des österreichischen Widerstandes),
USHMM, Yad Vashem, Bundesarchiv, Stiftung Berliner Mauer, Bundeszentrale
für politische Bildung (bpb), NIST 9/11-Reports, NASA, Iraq Survey Group
(Duelfer Report), Sarotte 'Not One Inch' 2021, Russell 'Inventing the
Flat Earth' 1991, Hamburger Wehrmachtsausstellung 2001/2004.

Topics:
  - hitler_sozialist_mythos, wehrmacht_kriegsverbrechen,
    holocaust_wissen_bevoelkerung, opfer_these_oesterreich,
    dresden_auschwitz_vergleich, stalin_hitler_gleichsetzung,
    versailles_2wk_kausalitaet, ddr_mauer_tote,
    marshall_plan_politische_bedingungen, hiroshima_alternativen,
    irak_krieg_wmd, nato_osterweiterung_versprechen,
    putin_ukraine_2022_aussagen, mondlandung_inszenierung,
    9_11_inside_job, aids_cia_labor_mythos, wikinger_hoerner_helme,
    mittelalter_erde_flach_mythos

Politische Guardrails (project_political_guardrails.md):
- Wir zitieren historischen Forschungs-Konsens, keine eigenen
  Bewertungen.
- Bei kontroversen Themen (Hiroshima, Marshall-Plan, NATO-Osterweiterung)
  wird die Forschungs-Kontroverse explizit benannt.
- Klar pseudohistorische Mythen (Holocaust-Wissen-Mythos, Wehrmacht
  sauber, Mondlandungs-Inszenierung, AIDS-CIA) werden als 'mostly_false'
  / 'false' eingeordnet — das ist kein politisches Urteil, sondern
  Faktenkonsens.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "geschichte_pack.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    """Descriptor for the cosine-similarity backup trigger."""
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_geschichte_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_geschichte(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Concat all string-valued data fields except 'context' (kept separate
    in description) into a compact display value."""
    parts: list[str] = []
    for key, val in d.items():
        if key == "context":
            continue
        if isinstance(val, str) and val.strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)


async def search_geschichte(analysis: dict) -> dict:
    empty = {
        "source": "Geschichts-Faktencheck (DÖW + USHMM + bpb + Konsens)",
        "type": "historical_consensus",
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
        label = fact.get("source_label", "DÖW / USHMM / bpb / Geschichts-Konsens")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "geschichte_konsens_fact",
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
        "source": "Geschichts-Faktencheck (DÖW + USHMM + bpb + Konsens)",
        "type": "historical_consensus",
        "results": results,
    }
