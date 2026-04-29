"""IEA TIMSS + IEA PIRLS + Statistik Austria Bildung + BMBWF Lehrer-Bedarf —
kuratierte Bildungs-Eckwerte für die häufigsten Boulevard-Themen
('Bildungs-Krise Österreich', 'Lehrermangel', 'jeder dritte Volksschüler
kann nicht lesen').

Datenquelle: Static-curated JSON in data/education_dach.json. Live-API-Pfad
zu IEA TIMSS/PIRLS und PISA wäre möglich, aber die Studien werden alle
4–5 Jahre publiziert; jährliche Aktualisierung der Lehrer-Bedarf-Statistik
genügt.
"""

import logging
import os

from services._static_cache import load_json_mtime_aware
from services._reranker_backup import best_matches as _backup_best_matches

logger = logging.getLogger("evidora")


def _fact_with_descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "education_dach.json",
)


def _load_static_json() -> dict | None:
    data = load_json_mtime_aware(STATIC_JSON_PATH)
    if data is None:
        return None
    if "facts" not in data:
        logger.warning("education_dach.json missing 'facts' key")
        return None
    return data


def _fact_matches(fact: dict, claim_lc: str) -> bool:
    for kw in fact.get("trigger_keywords") or ():
        if kw.lower() in claim_lc:
            return True
    composite = fact.get("trigger_composite") or []
    if composite and all(
        isinstance(alt, (list, tuple)) and any(tok in claim_lc for tok in alt)
        for alt in composite
    ):
        return True
    return False


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    data = _load_static_json()
    if not data:
        return []
    facts = data.get("facts") or []
    matches = [f for f in facts if _fact_matches(f, claim_lc)]
    if matches:
        return matches
    if not full_claim:
        return []
    items = [_fact_with_descriptor(f) for f in facts]
    return _backup_best_matches(full_claim, items, threshold=0.62, top_n=3)


def claim_mentions_education_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_education(client=None):
    data = _load_static_json()
    if not data:
        return []
    return data.get("facts") or []


async def search_education(analysis: dict) -> dict:
    empty = {
        "source": "Bildung (TIMSS/PIRLS/PISA + Lehrer-Bedarf)",
        "type": "official_data",
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
        label = fact.get("source_label", "IEA / OECD / BMBWF")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        if topic == "timss_at":
            display = (
                f"TIMSS 2023 Österreich (4. Klasse): "
                f"Mathematik = {d.get('timss_4_klasse_mathe_at')} Punkte "
                f"(EU-Schnitt {d.get('timss_4_klasse_eu_avg')}, OECD "
                f"{d.get('timss_4_klasse_oecd_avg')} — Rang AT in EU: "
                f"{d.get('rang_at_in_eu_4_klasse_mathe')}/22). "
                f"Naturwissenschaften = {d.get('timss_4_klasse_naturwiss_at')} "
                f"(über EU-Schnitt). "
                f"Trend Mathematik 2007–2023: {d.get('trend_mathe_at_2007_2023')}."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "pirls_at":
            display = (
                f"PIRLS 2021 Österreich Lesekompetenz 4. Klasse: "
                f"{d.get('pirls_at_score_2021')} Punkte "
                f"(EU-Schnitt {d.get('pirls_eu_avg_2021')}, OECD "
                f"{d.get('pirls_oecd_avg_2021')} — Rang AT in EU: "
                f"{d.get('rang_at_in_eu_2021')}/22). "
                f"Risikoschüler:innen-Anteil: "
                f"{d.get('anteil_risikoschueler_pct_2021')} %. "
                f"Trend: {d.get('pirls_at_trend_2006_2021')}."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "lehrermangel_at":
            display = (
                f"Lehrer:innen-Bedarf Österreich 2024: "
                f"{d.get('anzahl_lehrkraefte_at_2024'):,} Lehrkräfte ".replace(",", ".")
                + f"(2014: {d.get('anzahl_lehrkraefte_at_2014'):,} ".replace(",", ".")
                + "— +10.000 in 10 Jahren). "
                f"Vakanzquote 2024 ~{d.get('vakanzquote_2024_pct')} % — KEIN flächendeckender "
                f"Mangel. ABER: Studienanfänger Lehramt sanken um "
                f"{d.get('rueckgang_studienanfaenger_pct_2014_2024')} % in 10 Jahren "
                f"({d.get('studienanfaenger_lehramt_at_2014'):,} → ".replace(",", ".")
                + f"{d.get('studienanfaenger_lehramt_at_2024'):,}). ".replace(",", ".")
                + f"Bis 2030 gehen {d.get('prognose_pensionierungen_bis_2030'):,} ".replace(",", ".")
                + f"Lehrkräfte in Pension ({d.get('prognose_pensionierungen_bis_2030_anteil_pct')} %)."
            )
            description = d.get("context", "") + " " + notes_joined
        elif topic == "pisa_dach":
            display = (
                f"PISA 2022 DACH (15-Jährige, Mathematik): "
                f"AT = {d.get('pisa_mathe_at')}, "
                f"DE = {d.get('pisa_mathe_de')}, "
                f"CH = {d.get('pisa_mathe_ch')} "
                f"(OECD-Schnitt {d.get('pisa_oecd_avg_mathe')}). "
                f"Lesen: AT {d.get('pisa_lesen_at')}, DE {d.get('pisa_lesen_de')}, "
                f"CH {d.get('pisa_lesen_ch')}. "
                f"Naturwissenschaften: AT {d.get('pisa_nawi_at')}, "
                f"DE {d.get('pisa_nawi_de')}, CH {d.get('pisa_nawi_ch')}."
            )
            description = d.get("context", "") + " " + notes_joined
        else:
            display = fact.get("headline", "?")
            description = notes_joined

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "education_dach_fact",
            "country": "AT/DE/CH",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description.strip(" |").strip(),
            "url": url,
            "source": label,
        })

    return {
        "source": "Bildung (TIMSS/PIRLS/PISA + Lehrer-Bedarf)",
        "type": "official_data",
        "results": results,
    }
