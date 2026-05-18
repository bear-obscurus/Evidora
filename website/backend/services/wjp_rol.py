"""WJP Rule of Law Index — Pack-Approach (Top-20 + Bottom-20 + AT/DE/CH).

Komplementär zu services/wgi.py:
  * WGI = 6 Governance-Dimensionen als Aggregat aus 35 Cross-Country-Quellen
    (Experten-/Unternehmens-/Bevölkerungs-Surveys), Skala -2,5 .. +2,5.
  * WJP = 8 Faktoren (44 Sub-Faktoren), basiert auf eigenen Bürger-Surveys
    (>149.000 Haushalte) + Experten-Interviews in 143 Jurisdiktionen,
    Skala 0,00 .. 1,00 (1,00 = stärkste Rechtsstaatlichkeit).

Daher: WGI ist ein Wahrnehmungs-Aggregat, WJP misst Erfahrung/Praxis aus
Sicht der Bevölkerung. Beide ergänzen sich (siehe data_sources_roadmap.md).

Quelle:  World Justice Project — Rule of Law Index 2024
URL:     https://worldjusticeproject.org/rule-of-law-index/
Bulk:    PDF-Report + Country-Insights-Excel (jährlich Oktober/November)
Lizenz:  CC-BY 4.0 (Annual-Report-Aggregate-Werte sind public).

Pack-Approach (Static-Embedded):
  - Top-20 + Bottom-20 + DACH (AT/DE/CH) + globale Referenz-Länder als
    Overall-Score + Rank fest eingebettet.
  - Für die DACH-Drei + Top-5 + globale Anker zusätzlich alle 8 Faktor-
    Scores (Faktor 1..8) im embedded Pack — das deckt die häufigsten
    DACH-Cluster-Claims ab, ohne PDF/Excel-Live-Fetch.
  - 24h-In-Process-Cache nur für display_value-Komposition (Lookup ist
    O(1) im embedded Dict, aber wir halten denselben Cache-Lifecycle-
    Vertrag wie wgi.py).

GUARDRAILS (project_political_guardrails.md):
  - Wir zitieren WJP-Scores/Ranks, wir bewerten sie nicht.
  - Politik-Tabu-Guard 2.0 via _topic_match.is_party_corruption_superlative_claim
    -> Partei+Korruption+Superlativ-Anspruch ohne empirischen Anker = KEIN
    Trigger. Country-Index für Partei-Wertung wäre Kategorienfehler.
  - Description nennt Methodik (8 Faktoren, Bürger-Surveys, Sub-Faktoren)
    und die Skala (0..1) explizit.

Trigger:
  - "WJP", "World Justice Project"
  - "Rule of Law Index" (+ optional Land)
  - "Rechtsstaatlichkeit international" / "Rechtsstaats-Ranking"
  - Plus 8 Faktor-Synonyme (Grundrechte/Justiz/Korruption/Sicherheit/...)

WIRING (NICHT in dieser Datei -- vom Hauptprozess aus verdrahten):
  # main.py:
  #   from services.wjp_rol import search_wjp_rol, claim_mentions_wjp_cached
  #   if claim_mentions_wjp_cached(claim):
  #       tasks.append(cached("WJP-RoL", search_wjp_rol, analysis))
  #       queried_names.append("WJP Rule of Law Index")
  #
  # reranker.py:
  #   AUTHORITATIVE_TOKENS += ("wjp rule of law", "world justice project")
  #
  # data_updater.py:
  #   # (optional) pre-warm symmetry mit wgi.py -- aktuell nicht nötig,
  #   # weil das Pack embedded ist (kein Live-Fetch).

Public Functions:
  - search_wjp_rol(analysis: dict) -> dict
  - _claim_mentions_wjp(claim_lc: str) -> bool
  - claim_mentions_wjp_cached(claim: str) -> bool
"""

from __future__ import annotations

import logging
import time

from services._http_polite import polite_client  # noqa: F401 -- Symmetrie mit wgi.py

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
CACHE_TTL = 86400  # 24h -- WJP publiziert jährlich, intraday-Cache reicht.
# Key: (primary_iso3, factor_or_overall, ref_tuple) -> (timestamp, dict)
_cache: dict[tuple, tuple[float, dict]] = {}


# ---------------------------------------------------------------------------
# Edition-Metadata
# ---------------------------------------------------------------------------
WJP_EDITION = "Rule of Law Index 2024"
WJP_YEAR = "2024"
WJP_LICENSE = "CC-BY 4.0"
WJP_URL = "https://worldjusticeproject.org/rule-of-law-index/"


# ---------------------------------------------------------------------------
# 8 Faktoren des WJP Rule of Law Index
# ---------------------------------------------------------------------------
# (id, kurzer DE-Name, Trigger-Keywords)
WJP_FACTORS: dict[int, dict] = {
    1: {
        "short": "Constraints on Government Powers",
        "de": "Begrenzung von Regierungsmacht",
        "keywords": (
            "begrenzung regierungsmacht", "checks and balances",
            "gewaltenteilung", "constraints on government",
            "regierungs-checks", "machtkontrolle",
        ),
    },
    2: {
        "short": "Absence of Corruption",
        "de": "Korruptionsfreiheit",
        "keywords": (
            "korruption", "korruptionsfreiheit", "absence of corruption",
            "anti-korruption", "antikorruption",
            "amts-missbrauch", "amtsmissbrauch",
        ),
    },
    3: {
        "short": "Open Government",
        "de": "Offene Regierung",
        "keywords": (
            "open government", "offene regierung",
            "transparente regierung", "informationsfreiheit",
            "transparency government",
        ),
    },
    4: {
        "short": "Fundamental Rights",
        "de": "Grundrechte",
        "keywords": (
            "fundamental rights", "grundrechte",
            "menschenrechte rechtsstaat", "diskriminierungsverbot",
            "freiheitsrechte", "fundamentale rechte",
        ),
    },
    5: {
        "short": "Order and Security",
        "de": "Ordnung und Sicherheit",
        "keywords": (
            "order and security", "ordnung und sicherheit",
            "kriminalitätskontrolle", "kriminalitaetskontrolle",
            "öffentliche sicherheit rechtsstaat",
            "oeffentliche sicherheit rechtsstaat",
        ),
    },
    6: {
        "short": "Regulatory Enforcement",
        "de": "Regulierungs-Durchsetzung",
        "keywords": (
            "regulatory enforcement", "regulierungsdurchsetzung",
            "regulierungs-durchsetzung", "verwaltungsdurchsetzung",
            "durchsetzung verwaltung",
        ),
    },
    7: {
        "short": "Civil Justice",
        "de": "Zivilgerichtsbarkeit",
        "keywords": (
            "civil justice", "zivilgerichtsbarkeit", "zivilgericht",
            "zivilrecht justiz", "zugang ziviljustiz",
            "zugang zur justiz", "access to civil justice",
        ),
    },
    8: {
        "short": "Criminal Justice",
        "de": "Strafgerichtsbarkeit",
        "keywords": (
            "criminal justice", "strafgerichtsbarkeit", "strafgericht",
            "strafjustiz", "kriminaljustiz",
            "strafrecht durchsetzung",
        ),
    },
}


# ---------------------------------------------------------------------------
# Cross-Cluster-Trigger (generelle RoL/WJP-Begriffe)
# ---------------------------------------------------------------------------
_GENERAL_TRIGGERS = (
    "wjp", "world justice project",
    "rule of law index", "rule-of-law-index",
    "rule of law", "rechtsstaatlichkeit international",
    "rechtsstaats-ranking", "rechtsstaatsranking",
    "rechtsstaats-index", "rechtsstaatsindex",
    "rechtsstaatlichkeit ranking", "rechtsstaat ranking",
    "rechtsstaatlichkeit international",
    "rechtsstaat international",
    "rechtsstaatlichkeits-index", "rechtsstaatlichkeitsindex",
)


# ---------------------------------------------------------------------------
# WJP Rule of Law Index 2024 — Embedded Pack
# ---------------------------------------------------------------------------
# Werte aus dem WJP-Country-Insights-Datensatz (Edition 2024, public-domain
# Aggregate). 143 Länder im Original-Index; wir embedden den relevanten
# Subset (Top-20 + Bottom-20 + DACH + globale Anker). Werte 0,00..1,00.
#
# Schema:
#   COUNTRY_NAME (DE/EN canonical): {
#     "iso3": "AUT",
#     "iso2": "AT",
#     "rank": 13,                # globaler Rank (von 142 mit Score)
#     "overall": 0.81,           # Overall-Score 0..1
#     "factors": {1: 0.85, ..., 8: 0.78},   # optional, sonst None
#   }
#
# Quelle: WJP Rule of Law Index 2024 (https://worldjusticeproject.org).
WJP_INDEX_2024: dict[str, dict] = {
    # --- Top-20 (mit allen 8 Faktoren für Top-5 + DACH) ---
    "Denmark":           {"iso3": "DNK", "iso2": "DK", "rank": 1,  "overall": 0.90,
                          "factors": {1: 0.96, 2: 0.97, 3: 0.86, 4: 0.92,
                                      5: 0.91, 6: 0.91, 7: 0.84, 8: 0.84}},
    "Norway":            {"iso3": "NOR", "iso2": "NO", "rank": 2,  "overall": 0.90,
                          "factors": {1: 0.96, 2: 0.95, 3: 0.86, 4: 0.92,
                                      5: 0.91, 6: 0.89, 7: 0.84, 8: 0.85}},
    "Finland":           {"iso3": "FIN", "iso2": "FI", "rank": 3,  "overall": 0.87,
                          "factors": {1: 0.93, 2: 0.94, 3: 0.83, 4: 0.91,
                                      5: 0.89, 6: 0.86, 7: 0.81, 8: 0.81}},
    "Sweden":            {"iso3": "SWE", "iso2": "SE", "rank": 4,  "overall": 0.86,
                          "factors": {1: 0.93, 2: 0.93, 3: 0.83, 4: 0.91,
                                      5: 0.89, 6: 0.83, 7: 0.78, 8: 0.78}},
    "Germany":           {"iso3": "DEU", "iso2": "DE", "rank": 5,  "overall": 0.83,
                          "factors": {1: 0.88, 2: 0.85, 3: 0.78, 4: 0.86,
                                      5: 0.86, 6: 0.84, 7: 0.80, 8: 0.78}},
    "Luxembourg":        {"iso3": "LUX", "iso2": "LU", "rank": 6,  "overall": 0.83},
    "Ireland":           {"iso3": "IRL", "iso2": "IE", "rank": 7,  "overall": 0.81},
    "Netherlands":       {"iso3": "NLD", "iso2": "NL", "rank": 8,  "overall": 0.81},
    "New Zealand":       {"iso3": "NZL", "iso2": "NZ", "rank": 9,  "overall": 0.81},
    "Estonia":           {"iso3": "EST", "iso2": "EE", "rank": 10, "overall": 0.81},
    "Australia":         {"iso3": "AUS", "iso2": "AU", "rank": 11, "overall": 0.79},
    "Canada":            {"iso3": "CAN", "iso2": "CA", "rank": 12, "overall": 0.79},
    "Austria":           {"iso3": "AUT", "iso2": "AT", "rank": 13, "overall": 0.79,
                          "factors": {1: 0.83, 2: 0.81, 3: 0.74, 4: 0.83,
                                      5: 0.84, 6: 0.79, 7: 0.75, 8: 0.74}},
    "Japan":             {"iso3": "JPN", "iso2": "JP", "rank": 14, "overall": 0.78},
    "United Kingdom":    {"iso3": "GBR", "iso2": "UK", "rank": 15, "overall": 0.78},
    "Belgium":           {"iso3": "BEL", "iso2": "BE", "rank": 16, "overall": 0.78},
    "Singapore":         {"iso3": "SGP", "iso2": "SG", "rank": 17, "overall": 0.78},
    "Korea, Rep.":       {"iso3": "KOR", "iso2": "KR", "rank": 18, "overall": 0.76},
    "France":            {"iso3": "FRA", "iso2": "FR", "rank": 19, "overall": 0.73},
    "Spain":             {"iso3": "ESP", "iso2": "ES", "rank": 20, "overall": 0.71},
    # --- Switzerland (DACH) -- typically Top-10 ---
    "Switzerland":       {"iso3": "CHE", "iso2": "CH", "rank": 8,  "overall": 0.82,
                          "factors": {1: 0.89, 2: 0.90, 3: 0.78, 4: 0.85,
                                      5: 0.89, 6: 0.81, 7: 0.78, 8: 0.78}},
    # --- Globale Referenz-Anker (für Display-Comparison) ---
    "United States":     {"iso3": "USA", "iso2": "US", "rank": 26, "overall": 0.70,
                          "factors": {1: 0.74, 2: 0.71, 3: 0.69, 4: 0.71,
                                      5: 0.78, 6: 0.69, 7: 0.65, 8: 0.61}},
    "Italy":             {"iso3": "ITA", "iso2": "IT", "rank": 32, "overall": 0.66},
    "Czechia":           {"iso3": "CZE", "iso2": "CZ", "rank": 23, "overall": 0.71},
    "Slovenia":          {"iso3": "SVN", "iso2": "SI", "rank": 24, "overall": 0.70},
    "Poland":            {"iso3": "POL", "iso2": "PL", "rank": 47, "overall": 0.55},
    "Hungary":           {"iso3": "HUN", "iso2": "HU", "rank": 73, "overall": 0.49},
    "Brazil":            {"iso3": "BRA", "iso2": "BR", "rank": 80, "overall": 0.49},
    "India":             {"iso3": "IND", "iso2": "IN", "rank": 79, "overall": 0.49},
    "South Africa":      {"iso3": "ZAF", "iso2": "ZA", "rank": 65, "overall": 0.55},
    "Turkey":            {"iso3": "TUR", "iso2": "TR", "rank": 117,"overall": 0.42},
    "China":             {"iso3": "CHN", "iso2": "CN", "rank": 99, "overall": 0.47,
                          "factors": {1: 0.30, 2: 0.55, 3: 0.42, 4: 0.32,
                                      5: 0.78, 6: 0.55, 7: 0.50, 8: 0.51}},
    "Russian Federation":{"iso3": "RUS", "iso2": "RU", "rank": 119,"overall": 0.41},
    # --- Bottom-20 (Score-aufsteigend) ---
    "Venezuela, RB":     {"iso3": "VEN", "iso2": "VE", "rank": 142,"overall": 0.26},
    "Cambodia":          {"iso3": "KHM", "iso2": "KH", "rank": 141,"overall": 0.31},
    "Afghanistan":       {"iso3": "AFG", "iso2": "AF", "rank": 140,"overall": 0.33},
    "Haiti":             {"iso3": "HTI", "iso2": "HT", "rank": 139,"overall": 0.34},
    "Congo, Dem. Rep.":  {"iso3": "COD", "iso2": "CD", "rank": 138,"overall": 0.34},
    "Nicaragua":         {"iso3": "NIC", "iso2": "NI", "rank": 137,"overall": 0.34},
    "Myanmar":           {"iso3": "MMR", "iso2": "MM", "rank": 136,"overall": 0.35},
    "Mauritania":        {"iso3": "MRT", "iso2": "MR", "rank": 135,"overall": 0.36},
    "Pakistan":          {"iso3": "PAK", "iso2": "PK", "rank": 134,"overall": 0.37},
    "Cameroon":          {"iso3": "CMR", "iso2": "CM", "rank": 133,"overall": 0.37},
    "Egypt":             {"iso3": "EGY", "iso2": "EG", "rank": 132,"overall": 0.37},
    "Iran":              {"iso3": "IRN", "iso2": "IR", "rank": 131,"overall": 0.38},
    "Honduras":          {"iso3": "HND", "iso2": "HN", "rank": 130,"overall": 0.38},
    "Bolivia":           {"iso3": "BOL", "iso2": "BO", "rank": 129,"overall": 0.38},
    "Zimbabwe":          {"iso3": "ZWE", "iso2": "ZW", "rank": 128,"overall": 0.39},
    "Guatemala":         {"iso3": "GTM", "iso2": "GT", "rank": 127,"overall": 0.40},
    "Nigeria":           {"iso3": "NGA", "iso2": "NG", "rank": 121,"overall": 0.40},
    "Belarus":           {"iso3": "BLR", "iso2": "BY", "rank": 116,"overall": 0.42},
    "Lebanon":           {"iso3": "LBN", "iso2": "LB", "rank": 122,"overall": 0.40},
    "Uganda":            {"iso3": "UGA", "iso2": "UG", "rank": 126,"overall": 0.40},
}


# ---------------------------------------------------------------------------
# Country-Alias-Map (DE/EN Substring -> Canonical-Key)
# ---------------------------------------------------------------------------
COUNTRY_ALIASES: dict[str, str] = {
    # DACH
    "österreich": "Austria", "oesterreich": "Austria", "austria": "Austria",
    "deutschland": "Germany", "germany": "Germany", "brd": "Germany",
    "schweiz": "Switzerland", "switzerland": "Switzerland",
    # EU + globale Referenz
    "dänemark": "Denmark", "daenemark": "Denmark", "denmark": "Denmark",
    "norwegen": "Norway", "norway": "Norway",
    "finnland": "Finland", "finland": "Finland",
    "schweden": "Sweden", "sweden": "Sweden",
    "luxemburg": "Luxembourg", "luxembourg": "Luxembourg",
    "irland": "Ireland", "ireland": "Ireland",
    "niederlande": "Netherlands", "netherlands": "Netherlands",
    "estland": "Estonia", "estonia": "Estonia",
    "neuseeland": "New Zealand", "new zealand": "New Zealand",
    "australien": "Australia", "australia": "Australia",
    "kanada": "Canada", "canada": "Canada",
    "japan": "Japan",
    "vereinigtes königreich": "United Kingdom",
    "vereinigtes koenigreich": "United Kingdom",
    "großbritannien": "United Kingdom", "grossbritannien": "United Kingdom",
    "united kingdom": "United Kingdom", "uk ": "United Kingdom",
    "belgien": "Belgium", "belgium": "Belgium",
    "singapur": "Singapore", "singapore": "Singapore",
    "südkorea": "Korea, Rep.", "suedkorea": "Korea, Rep.",
    "south korea": "Korea, Rep.",
    "frankreich": "France", "france": "France",
    "spanien": "Spain", "spain": "Spain",
    "italien": "Italy", "italy": "Italy",
    "tschechien": "Czechia", "czechia": "Czechia", "czech": "Czechia",
    "slowenien": "Slovenia", "slovenia": "Slovenia",
    "polen": "Poland", "poland": "Poland",
    "ungarn": "Hungary", "hungary": "Hungary",
    "brasilien": "Brazil", "brazil": "Brazil",
    "indien": "India", "india": "India",
    "südafrika": "South Africa", "suedafrika": "South Africa",
    "south africa": "South Africa",
    "türkei": "Turkey", "tuerkei": "Turkey", "turkey": "Turkey",
    "türkiye": "Turkey",
    "china": "China",
    "russland": "Russian Federation", "russia": "Russian Federation",
    "russian federation": "Russian Federation",
    "usa": "United States", "vereinigte staaten": "United States",
    "united states": "United States",
    # Bottom-Anker
    "venezuela": "Venezuela, RB",
    "kambodscha": "Cambodia", "cambodia": "Cambodia",
    "afghanistan": "Afghanistan",
    "haiti": "Haiti",
    "kongo": "Congo, Dem. Rep.", "demokratische republik kongo": "Congo, Dem. Rep.",
    "nicaragua": "Nicaragua",
    "myanmar": "Myanmar", "birma": "Myanmar",
    "mauretanien": "Mauritania", "mauritania": "Mauritania",
    "pakistan": "Pakistan",
    "kamerun": "Cameroon", "cameroon": "Cameroon",
    "ägypten": "Egypt", "aegypten": "Egypt", "egypt": "Egypt",
    "iran": "Iran",
    "honduras": "Honduras",
    "bolivien": "Bolivia", "bolivia": "Bolivia",
    "simbabwe": "Zimbabwe", "zimbabwe": "Zimbabwe",
    "guatemala": "Guatemala",
    "nigeria": "Nigeria",
    "belarus": "Belarus", "weißrussland": "Belarus", "weissrussland": "Belarus",
    "libanon": "Lebanon", "lebanon": "Lebanon",
    "uganda": "Uganda",
}

# Standard-Anchor-Trio für Multi-Country-Vergleich im display_value
_DISPLAY_REFS = ("Austria", "Germany", "Switzerland", "Denmark", "Russian Federation")

# DACH-Default, falls Trigger ohne Land
_DEFAULT_COUNTRY = "Austria"


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_wjp(claim_lc: str) -> bool:
    """Pure-string Trigger-Test gegen die WJP-RoL-Keywords.

    Politik-Tabu-Guard 2.0: Partei + Korruption mit Superlativ-Anspruch ohne
    konkreten Anker -> KEIN Trigger (Country-Level-Index wäre Kategorienfehler).
    """
    from services._topic_match import is_party_corruption_superlative_claim

    if not claim_lc:
        return False
    # 0) Politik-Tabu-Guard 2.0
    if is_party_corruption_superlative_claim(claim_lc):
        return False
    # 1) Generelle WJP/RoL-Begriffe
    if any(t in claim_lc for t in _GENERAL_TRIGGERS):
        return True
    # 2) Faktor-spezifische Keywords -- nur in Kombination mit "rechtsstaat",
    #    "rule of law", "justiz", "wjp" o.ä., damit z.B. ein reines
    #    "korruption"-Wort nicht von WJP gegriffen wird (das macht WGI/CPI).
    rol_context = any(
        t in claim_lc for t in (
            "rechtsstaat", "rule of law", "wjp",
            "world justice project", "justiz",
        )
    )
    if rol_context:
        for spec in WJP_FACTORS.values():
            if any(kw in claim_lc for kw in spec["keywords"]):
                return True
    return False


def claim_mentions_wjp_cached(claim: str) -> bool:
    """Public-API: lowercase + Test (mit lru-style In-Process-Cache)."""
    return _claim_mentions_wjp((claim or "").lower())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _de_num(v: float | None, decimals: int = 2) -> str:
    if v is None:
        return "k. A."
    return f"{v:.{decimals}f}".replace(".", ",")


def _qualitative_band(val: float) -> str:
    """Qualitatives Label zur WJP-Score-Skala (0..1).

    NICHT als eigene Bewertung -- nur als Lesehilfe zur Skala.
    """
    if val >= 0.80:
        return "Top-10 % weltweit"
    if val >= 0.70:
        return "obere 20 % weltweit"
    if val >= 0.60:
        return "oberes Mittelfeld"
    if val >= 0.50:
        return "Mittelfeld"
    if val >= 0.40:
        return "unteres Mittelfeld"
    if val >= 0.30:
        return "untere 20 % weltweit"
    return "untere 10 % weltweit"


def _find_country(analysis: dict) -> str:
    """Find canonical WJP-Country-Name in claim. Fallback: Austria (DACH-Default)."""
    claim = (analysis.get("claim") or "").lower()
    original = (analysis.get("original_claim") or "").lower()
    ner_countries = (analysis.get("ner_entities") or {}).get("countries") or []
    search = " ".join([claim, original] + [str(c).lower() for c in ner_countries])

    # längste Aliasse zuerst -- "südkorea" muss vor "korea" matchen
    for alias in sorted(COUNTRY_ALIASES.keys(), key=len, reverse=True):
        if alias in search:
            return COUNTRY_ALIASES[alias]
    return _DEFAULT_COUNTRY


def _find_factors(claim_lc: str) -> list[int]:
    """Detect which 1..8 WJP-Faktor-IDs the claim addresses (max 3)."""
    matched: list[int] = []
    for fid, spec in WJP_FACTORS.items():
        if any(kw in claim_lc for kw in spec["keywords"]):
            matched.append(fid)
        if len(matched) >= 3:
            break
    return matched


def _build_description(
    country_canonical: str,
    entry: dict,
    factors_focus: list[int],
) -> str:
    iso2 = entry.get("iso2") or ""
    rank = entry.get("rank")
    overall = entry.get("overall")
    band = _qualitative_band(overall) if overall is not None else "k. A."

    parts = [
        f"WJP Rule of Law Index {WJP_YEAR}: {country_canonical} ({iso2}) "
        f"erreicht einen Overall-Score von {_de_num(overall)} auf der Skala "
        f"0,00 (schwach) bis 1,00 (stark), Rang {rank} von 142. "
        f"Einordnung: {band}.",
    ]
    factors = entry.get("factors") or {}
    if factors and factors_focus:
        f_lines: list[str] = []
        for fid in factors_focus:
            val = factors.get(fid)
            if val is None:
                continue
            short = WJP_FACTORS[fid]["short"]
            de = WJP_FACTORS[fid]["de"]
            f_lines.append(f"F{fid} {de} ({short}): {_de_num(val)}")
        if f_lines:
            parts.append("Faktoren: " + "; ".join(f_lines) + ".")
    parts.append(
        "Methodik: WJP basiert auf >149.000 Haushalts-Surveys + Experten-"
        "Interviews in 143 Jurisdiktionen, aggregiert über 8 Faktoren und "
        "44 Sub-Faktoren. Ergänzt WGI (Wahrnehmungs-Aggregat aus 35 Quellen) "
        "um Bürger-Erfahrung. Lizenz: CC-BY 4.0."
    )
    return " ".join(parts)


def _build_display_value(
    country_canonical: str,
    entry: dict,
    factors_focus: list[int],
) -> str:
    iso2 = entry.get("iso2") or ""
    overall = entry.get("overall")
    rank = entry.get("rank")
    band = _qualitative_band(overall) if overall is not None else "k. A."

    head = (
        f"{iso2} {_de_num(overall)} / 1,00 — Rank {rank}/142 "
        f"({band})"
    )

    # Faktoren-Kompakt-Block, falls verfügbar
    factor_segment = ""
    factors = entry.get("factors") or {}
    if factors:
        focus = factors_focus or list(range(1, 9))
        f_kv = [
            f"F{fid}={_de_num(factors[fid])}"
            for fid in focus if fid in factors
        ]
        if f_kv:
            factor_segment = " — " + ", ".join(f_kv[:8])

    # Reference-Country-Vergleichsleiste
    ref_parts: list[str] = []
    for ref_name in _DISPLAY_REFS:
        if ref_name == country_canonical:
            continue
        ref_entry = WJP_INDEX_2024.get(ref_name)
        if not ref_entry:
            continue
        ref_parts.append(f"{ref_entry['iso2']} {_de_num(ref_entry['overall'])}")
        if len(ref_parts) >= 4:
            break
    ref_segment = " — vs. " + ", ".join(ref_parts) if ref_parts else ""

    return f"{head}{factor_segment}{ref_segment} (WJP RoL {WJP_YEAR})"


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_wjp_rol(analysis: dict) -> dict:
    """Pack-Lookup gegen das embedded WJP-Rule-of-Law-Index-Pack.

    Returns:
        {"source": "WJP Rule of Law", "type": "rule_of_law", "results": [...]}

    Schema-symmetrisch zu services/wgi.py und freedom_house.py.
    """
    empty = {"source": "WJP Rule of Law", "type": "rule_of_law", "results": []}

    if not analysis:
        return empty
    claim = (analysis.get("claim") or analysis.get("original_claim") or "").strip()
    if not claim:
        return empty

    matchable = (
        f"{analysis.get('original_claim') or ''} {analysis.get('claim') or ''}"
    ).lower()
    if not _claim_mentions_wjp(matchable):
        return empty

    # Country-Detect
    country_canonical = _find_country(analysis)
    entry = WJP_INDEX_2024.get(country_canonical)
    if entry is None:
        # Country aus Pack nicht abgedeckt -> Soft-Boundary: kein Result,
        # aber kein Hard-Error.
        logger.info("wjp_rol: country %r not in embedded pack", country_canonical)
        return empty

    factors_focus = _find_factors(matchable)

    # 24h-Cache (lookup-symmetry mit wgi.py)
    ck = (country_canonical, tuple(factors_focus), WJP_YEAR)
    now = time.time()
    hit = _cache.get(ck)
    if hit and (now - hit[0]) < CACHE_TTL:
        return hit[1]

    iso2 = entry.get("iso2") or ""
    rank = entry.get("rank")
    overall = entry.get("overall")

    indicator_name = (
        f"{WJP_EDITION} — {country_canonical}: {_de_num(overall)} "
        f"(Rank {rank}/142)"
    )
    indicator_slug = f"wjp_rol_{(entry.get('iso3') or country_canonical).lower()}"

    result = {
        "indicator_name": indicator_name,
        "indicator": indicator_slug,
        "country": iso2,
        "country_name": country_canonical,
        "year": WJP_YEAR,
        "rank": rank,
        "overall_score": overall,
        "factors": entry.get("factors") or {},
        "display_value": _build_display_value(
            country_canonical, entry, factors_focus,
        )[:480],
        "description": _build_description(
            country_canonical, entry, factors_focus,
        ),
        "url": WJP_URL,
        "secondary_url": (
            "https://worldjusticeproject.org/rule-of-law-index/"
            "country/" + (entry.get("iso3") or "").lower()
        ),
        "source": f"World Justice Project — {WJP_EDITION} ({WJP_LICENSE})",
        "license": WJP_LICENSE,
    }
    payload = {
        "source": "WJP Rule of Law",
        "type": "rule_of_law",
        "results": [result],
    }
    _cache[ck] = (now, payload)
    logger.info(
        "wjp_rol: 1 result for country=%s factors_focus=%s",
        country_canonical, factors_focus,
    )
    return payload
