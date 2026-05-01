"""Reporters Without Borders (RSF) — World Press Freedom Index.

Datenquelle: Reporters sans frontières / Reporters Without Borders, jährlicher
Index der Pressefreiheit in 180 Ländern (seit 2013, aktuelle Methodik seit 2022).

Skala: 0–100 Punkte (höher = freiere Presse).
Kategorien: >85 Gut | 70–85 Zufriedenstellend | 55–70 Problematisch |
40–55 Schwierig | <40 Sehr ernst.

Fünf Sub-Indikatoren: Political Context, Economic Context, Legal Context,
Social Context, Safety.

Lizenz: RSF publiziert den Index als Open Data; Attribution an "Reporters
Without Borders / RSF" erforderlich.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren RSF-Scores, keine eigene Bewertung der Pressefreiheit.
- Caveat zur Methodik (Mix aus Fragebogen + Sicherheitsvorfälle) ist Pflicht.

Datenpfad seit 2026-05-01 (Open-Source-Compliance-Audit):
- Daten als Static-First-Topic in ``data/rsf.json``.
- Refresh einmal pro Jahr manuell via ``tools/refresh_rsf.py``.
- Kein Live-API-Call mehr aus der Pipeline (eliminiert die frühere
  User-Agent-Maskierung als TOS-Risiko).
"""

import logging
import os

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "rsf.json",
)

# Trigger-Keywords (DE + EN) — unverändert vom alten Service
RSF_KEYWORDS = [
    "pressefreiheit", "press freedom",
    "medienfreiheit", "media freedom",
    "pressefreiheitsindex", "press freedom index",
    "reporter ohne grenzen", "reporters without borders", "reporters sans frontieres",
    "rsf",
    "journalismus", "journalism",
    "journalist:innen", "journalists", "journalisten",
    "medien", "media",
    "zensur", "censorship",
    "medienzensur",
    "pressezensur",
    "pressefreiheit eingeschränkt",
    "medienvielfalt", "media pluralism",
]

# RSF verwendet in der CSV ISO3-Codes (z.B. FIN, EST, AUT, NLD)
COUNTRY_MAP = {
    "österreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "schweiz": "CHE", "switzerland": "CHE",
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czech republic": "CZE", "czechia": "CZE",
    "ungarn": "HUN", "hungary": "HUN",
    "rumänien": "ROU", "romania": "ROU",
    "bulgarien": "BGR", "bulgaria": "BGR",
    "kroatien": "HRV", "croatia": "HRV",
    "slowenien": "SVN", "slovenia": "SVN",
    "slowakei": "SVK", "slovakia": "SVK",
    "dänemark": "DNK", "denmark": "DNK",
    "schweden": "SWE", "sweden": "SWE",
    "norwegen": "NOR", "norway": "NOR",
    "finnland": "FIN", "finland": "FIN",
    "portugal": "PRT",
    "griechenland": "GRC", "greece": "GRC",
    "irland": "IRL", "ireland": "IRL",
    "luxemburg": "LUX", "luxembourg": "LUX",
    "estland": "EST", "estonia": "EST",
    "lettland": "LVA", "latvia": "LVA",
    "litauen": "LTU", "lithuania": "LTU",
    "vereinigtes königreich": "GBR", "united kingdom": "GBR", "großbritannien": "GBR",
    "türkei": "TUR", "turkey": "TUR",
    "serbien": "SRB", "serbia": "SRB",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "belarus": "BLR", "weißrussland": "BLR",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "südkorea": "KOR", "south korea": "KOR",
    "nordkorea": "PRK", "north korea": "PRK",
    "iran": "IRN",
    "israel": "ISR",
    "saudi-arabien": "SAU", "saudi arabia": "SAU",
    "ägypten": "EGY", "egypt": "EGY",
}


def _load_rsf_data() -> tuple[dict, int | None]:
    """Return (by_country_dict, year). Empty dict + None if file is missing."""
    pack = load_json_mtime_aware(STATIC_JSON_PATH)
    if not pack:
        return {}, None
    facts = pack.get("facts") or []
    if not facts:
        return {}, None
    fact = facts[0]
    data = fact.get("data") or {}
    by_country = data.get("by_country") or {}
    year = data.get("year") or fact.get("year")
    return by_country, year


def _find_countries(analysis: dict, max_n: int = 3) -> list[str]:
    """Extract ISO3 country codes from claim."""
    ner_countries = analysis.get("ner_entities", {}).get("countries", [])
    claim = analysis.get("claim", "")
    search_terms = ner_countries + [claim]

    found: list[str] = []
    seen: set[str] = set()
    for term in search_terms:
        term_lower = term.lower()
        for name, code in COUNTRY_MAP.items():
            if name in term_lower and code not in seen:
                found.append(code)
                seen.add(code)
                if len(found) >= max_n:
                    return found
    return found


def _claim_mentions_rsf(claim: str) -> bool:
    claim_lower = claim.lower()
    return any(kw in claim_lower for kw in RSF_KEYWORDS)


def claim_mentions_rsf_cached(claim: str) -> bool:
    """Public alias used by main.py fan-out."""
    if not claim:
        return False
    return _claim_mentions_rsf(claim)


def _category_label(score: float | None) -> str:
    if score is None:
        return "keine Daten"
    if score > 85:
        return "Gut"
    if score >= 70:
        return "Zufriedenstellend"
    if score >= 55:
        return "Problematisch"
    if score >= 40:
        return "Schwierig"
    return "Sehr ernst"


async def fetch_rsf(client=None) -> dict:
    """Compatibility shim: returns the by_country dict so tests / data_updater
    that still reference fetch_rsf() keep working. The argument is ignored —
    no HTTP client is used any more.
    """
    by_country, _year = _load_rsf_data()
    return by_country


async def search_rsf(analysis: dict) -> dict:
    """Search the static RSF pack for press-freedom data."""
    if not _claim_mentions_rsf(analysis.get("claim", "")):
        return {"source": "Reporter ohne Grenzen (RSF)", "type": "official_data", "results": []}

    data, year = _load_rsf_data()
    if not data:
        return {"source": "Reporter ohne Grenzen (RSF)", "type": "official_data", "results": []}

    countries = _find_countries(analysis)
    if not countries:
        countries = ["AUT", "DEU"]

    results: list[dict] = []
    for code in countries:
        entry = data.get(code)
        if not entry:
            continue
        score = entry.get("score")
        rank = entry.get("rank")
        entry_year = entry.get("year") or year
        entity = entry.get("country_en") or code
        category = _category_label(score)

        # Vorjahresvergleich
        trend_note = ""
        score_prev = entry.get("score_prev")
        if score is not None and score_prev is not None:
            delta = score - score_prev
            if abs(delta) >= 0.5:
                arrow = "↑" if delta > 0 else "↓"
                trend_note = f" | Vorjahr: {arrow} ({delta:+.1f})"

        # Sub-Indikatoren (wenn verfügbar)
        sub_parts = []
        for key, label in [
            ("political", "Politisch"),
            ("economic", "Wirtschaft"),
            ("legal", "Recht"),
            ("social", "Sozial"),
            ("safety", "Sicherheit"),
        ]:
            v = entry.get(key)
            if v is not None:
                sub_parts.append(f"{label}: {v:.1f}")
        sub_line = " | ".join(sub_parts) if sub_parts else ""

        rank_text = f"Rang {rank}/180" if rank else ""
        score_text = f"{score:.1f}/100" if score is not None else "–"

        name = f"RSF Pressefreiheit {entity} ({entry_year}): {score_text} — {category}"
        if rank_text:
            name += f" ({rank_text})"
        name += trend_note

        results.append({
            "indicator_name": name,
            "indicator": "rsf_press_freedom_score",
            "country": code,
            "country_name": entity,
            "year": str(entry_year) if entry_year else "",
            "value": score,
            "display_value": score_text,
            "url": "https://rsf.org/en/index",
            "description": sub_line,
        })

    if results:
        results.append({
            "indicator_name": "WICHTIGER KONTEXT: RSF-Index kombiniert Umfrage und Vorfallsdaten",
            "indicator": "context",
            "country": "",
            "country_name": "",
            "year": "",
            "value": "",
            "display_value": "",
            "url": "https://rsf.org/en/methodology-used-compiling-world-press-freedom-index-2024",
            "description": (
                "Der RSF World Press Freedom Index wird jährlich von Reporters Without Borders "
                "publiziert. Skala: 0 (sehr ernst) bis 100 (gut). Kategorien: >85 Gut, "
                "70–85 Zufriedenstellend, 55–70 Problematisch, 40–55 Schwierig, <40 Sehr ernst. "
                "Der Gesamtscore setzt sich aus fünf Sub-Indikatoren zusammen: "
                "Political Context, Economic Context, Legal Context, Social Context, Safety. "
                "Einschränkungen: "
                "(1) Zwei Methoden kombiniert — ein quantitativer Fragebogen an Medienexpert:innen "
                "sowie eine Zählung dokumentierter Übergriffe/Tötungen an Journalist:innen. "
                "(2) Methodenbruch 2022 — aktuelle Methodik ist mit der vor 2022 nur bedingt "
                "vergleichbar (neue Sub-Indikatoren, größerer Expertenkreis). "
                "(3) Nur öffentliche Medien — Social Media, Plattform-Moderation und Desinformation "
                "sind nur teilweise erfasst. "
                "(4) Länderfokus, nicht Regionen — innerhalb großer Länder kann die Pressefreiheit "
                "stark variieren; der Index aggregiert auf Staatsebene."
            ),
        })

    logger.info(f"RSF: {len(results) - (1 if results else 0)} country results, countries={countries}")
    return {"source": "Reporter ohne Grenzen (RSF)", "type": "official_data", "results": results}
