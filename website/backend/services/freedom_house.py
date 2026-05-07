"""Freedom House Live-Connector — FIW 2024 Country-Ratings (Static-First-Cache).

Freedom House (https://freedomhouse.org) ist eine US-amerikanische NGO, die
seit 1972 jährlich ihre 'Freedom in the World' (FIW)-Studie veröffentlicht.
Sie misst politische Rechte (Political Rights, 0-40 Punkte) und Bürgerrechte
(Civil Liberties, 0-60 Punkte) pro Land. Die Summe (0-100) bestimmt den
Status: Free (70-100), Partly Free (35-69), Not Free (0-34).

Komplementär zu V-Dem:
  - V-Dem: Continuous-Indizes 0-1, methodisch via Bayesian-IRT-Aggregation
    aus Experten-Befragungen, sehr granular (470+ Sub-Indikatoren).
  - Freedom House: Aggregat-Score 0-100 mit Schwellen-Status (Free/Partly
    Free/Not Free), pragmatisch + öffentlichkeitswirksam.

Strategie: STATIC-FIRST-PRE-CACHE
=================================
Freedom House publiziert keine REST-API. Daten werden als Excel/CSV-Download
veröffentlicht (jährlich Februar/März). Wir halten einen kuratierten Subset
von ~55 Schlüssel-Ländern in ``data/freedom_house_2024.json``:

  - DACH + EU + globale Referenz (USA, RUS, CHN, IND, BRA, ZAF, ...)
  - Osteuropa + Westbalkan + Kaukasus + Zentralasien (autoritäre Vergleichs-
    Länder)
  - 6 Indikatoren pro Land: total_score, pr_score, cl_score, status,
    pr_rating, cl_rating

Refresh-Workflow (manuell oder per Cron):
  1. Download All_data_FIW_2013-{year}.xlsx von freedomhouse.org
  2. Filter auf neuestes Edition-Year + auf relevante ISO3-Länder
  3. JSON regenerieren, mtime ändert → Hot-Reload greift automatisch

Trigger:
  - Claim enthält Länder-Alias UND Demokratie-/Freiheits-/Pressefreiheits-
    Keyword
  - ODER allgemeines Freedom-House-Vokabular ("freedom-house",
    "freie länder", "demokratie-status", ...) auch ohne Land-Nennung
    → DACH-Default (AT/DE/CH).

Limitations:
  - FIW publiziert jährlich (Februar) — Daten sind ~1 Jahr alt
    (FIW 2024 deckt Events 2023 ab).
  - Methodik basiert auf Experten-Bewertung (Freedom-House-Analysten,
    interne + externe Reviewer). Subjektiv, aber transparent dokumentiert.
  - Manche Länder/Gebiete (z.B. Taiwan) haben Sonderstatus, sind aber im
    Static-Cache aktuell nicht enthalten.
  - AUDIT-FLAG: Werte aktuell LLM-Approximationen — Refresh aus offiziellem
    CSV via Cron-Job einmal/Jahr (Februar/März) nötig.

GUARDRAILS (siehe project_political_guardrails.md):
  - Wir zitieren Freedom-House-Scores, wir bewerten sie nicht.
  - Wir nehmen keine eigene Partei-/Politiker-Bewertung vor.
  - Caveat zur Methodik (Experten-Befragung, US-NGO) ist Pflicht.

Result-Schema:
  {
    "indicator_name": "Freedom House FIW 2024 — Russia: 13/100 (Not Free)",
    "indicator": "freedom_house_score",
    "country": "RU",
    "year": "2024",
    "topic": "freedom_house_ranking",
    "display_value": "RU 13/100 'Not Free' (PR 5/40, CL 8/60) — vs. AT 93, DE 93, SE 100, CN 9 (FIW 2024)",
    "description": "Freedom in the World 2024 misst PR (40 P) + CL (60 P). Schwellen: 70-100 Free, 35-69 Partly Free, 0-34 Not Free.",
    "url": "https://freedomhouse.org/country/russia",
    "secondary_url": "https://freedomhouse.org/report/freedom-world",
    "source": "Freedom House — Freedom in the World 2024 (covering 2023 events)",
  }

Wiring (NICHT in dieser Datei — vom Hauptprozess manuell):
  - main.py: import + tasks.append(search_freedom_house(analysis))
  - reranker.py: Indicator-Whitelist-Marker für 'freedom_house_score' /
    'Freedom House' möglich. Live-Quelle, NICHT in AUTHORITATIVE-Pack-
    Markern (kein kuratiertes Pack, sondern Live-Static-Cache).
  - confidence_calibration.py: optional, für Boosts.
"""

from __future__ import annotations

import logging
import os

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "freedom_house_2024.json",
)

# DACH-Default-Länder, wenn Claim Freedom-House-Keyword nennt aber kein Land.
_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS = ("AUT", "DEU", "CHE")

# Trigger-Keywords (DE + EN). Bei Match + Land → Treffer.
# Bei Match ohne Land → DACH-Default.
_FH_KEYWORDS = (
    "freedom house", "freedom-house", "fiw",
    "freedom in the world",
    "freedom-of-press", "freedom of press",
    "pressefreiheit", "presse freiheit",
    "political rights", "politische rechte",
    "bürgerrechte", "buergerrechte", "civil liberties",
    "freie wahlen", "free elections",
    "demokratie-ranking", "demokratieranking",
    "demokratie-status", "demokratiestatus", "democracy status",
    "freie länder", "freie laender", "free countries",
    "unfreie länder", "unfreie laender", "not free countries",
    "partly free", "teilweise frei",
    "freiheitsindex", "freedom index",
    "country freedom rating", "länder-freiheits-rating",
    "demokratie-niveau", "demokratieniveau",
    "autoritäres regime", "autoritaeres regime", "authoritarian regime",
)

# Reference-Länder für display_value Multi-Country-Comparison.
# AT-Bias: AT zuerst, dann DE/CH, dann Kontrast (SE top, CN/RUS bottom).
_DISPLAY_REFERENCE_COUNTRIES = (
    "AUT", "DEU", "CHE", "SWE", "NOR", "USA", "HUN", "POL",
    "RUS", "CHN", "TUR", "BLR",
)

# Maximum Anzahl Reference-Länder im display_value.
MAX_COUNTRIES_IN_DISPLAY = 5

# Maximum Primär-Länder pro Claim.
MAX_PRIMARY_COUNTRIES = 1

# ISO3 → ISO2 Mapping (für display_value-Kompaktheit).
_ISO3_TO_ISO2 = {
    "AUT": "AT", "DEU": "DE", "CHE": "CH", "FRA": "FR", "ITA": "IT",
    "ESP": "ES", "GBR": "UK", "SWE": "SE", "NOR": "NO", "DNK": "DK",
    "FIN": "FI", "NLD": "NL", "BEL": "BE", "IRL": "IE", "GRC": "GR",
    "PRT": "PT", "USA": "US", "CAN": "CA", "AUS": "AU", "NZL": "NZ",
    "JPN": "JP", "ISR": "IL", "RUS": "RU", "CHN": "CN", "IND": "IN",
    "BRA": "BR", "ZAF": "ZA", "TUR": "TR", "HUN": "HU", "POL": "PL",
    "CZE": "CZ", "SVK": "SK", "SVN": "SI", "EST": "EE", "LVA": "LV",
    "LTU": "LT", "ROU": "RO", "BGR": "BG", "HRV": "HR", "ALB": "AL",
    "BIH": "BA", "SRB": "RS", "MKD": "MK", "MNE": "ME", "BLR": "BY",
    "UKR": "UA", "MDA": "MD", "GEO": "GE", "ARM": "AM", "AZE": "AZ",
    "KAZ": "KZ", "KGZ": "KG", "TJK": "TJ", "TKM": "TM", "UZB": "UZ",
}

# Status-Übersetzung für indicator_name.
_STATUS_DE = {
    "Free": "Free",
    "Partly Free": "Partly Free",
    "Not Free": "Not Free",
}


def _load_data() -> dict | None:
    """Lade JSON mit Hot-Reload-Awareness."""
    return load_json_mtime_aware(STATIC_JSON_PATH)


def _country_aliases(data: dict) -> dict:
    """Country-Code → Liste von DE/EN-Substring-Aliassen."""
    return data.get("country_aliases") or {}


def _detect_countries_in_claim(claim_lc: str, data: dict) -> list[str]:
    """Erkenne welche ISO3-Country-Codes der Claim erwähnt.

    Returns Liste der ISO3-Codes (jedes Land höchstens einmal).
    """
    aliases = _country_aliases(data)
    found: list[str] = []
    for iso3, alias_list in aliases.items():
        for alias in alias_list:
            if alias.lower() in claim_lc:
                found.append(iso3)
                break  # nur einmal pro Land
    return found


def _has_fh_keyword(claim_lc: str) -> bool:
    """Trifft mindestens ein Freedom-House-Trigger-Keyword?"""
    return any(kw in claim_lc for kw in _FH_KEYWORDS)


def claim_mentions_freedom_house_cached(claim: str) -> bool:
    """Trigger-Pre-Check (für main.py-Pipeline-Routing).

    Returns True, wenn der Claim ein FH-Keyword enthält UND entweder
    ein Land aus den Aliassen ODER generisches Freedom-House-Vokabular
    (dann DACH-Default).
    """
    if not claim:
        return False
    data = _load_data()
    if not data:
        return False
    claim_lc = claim.lower()

    if not _has_fh_keyword(claim_lc):
        return False

    # Wenn FH-Keyword + Land → trigger.
    countries_found = _detect_countries_in_claim(claim_lc, data)
    if countries_found:
        return True

    # Wenn FH-Keyword ohne Land → trigger mit DACH-Default.
    return True


async def fetch_freedom_house(client=None) -> dict:
    """On-Demand-Load der Freedom-House-Ratings aus dem Static-JSON.

    Returns das gesamte JSON-Dict (mit ratings/country_aliases/source_label/...).
    ``client`` wird ignoriert (nur für Signatur-Symmetrie mit anderen
    Live-Connectoren).
    """
    data = _load_data()
    if not data:
        return {}
    return data


def _format_country_total(iso3: str, ratings: dict) -> str:
    """Hilfs-Format: 'AT 93'."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    rating = ratings.get(iso3) or {}
    total = rating.get("total_score")
    if total is None:
        return ""
    return f"{iso2} {total}"


def _select_display_countries(
    requested_countries: list[str],
    ratings: dict,
    primary: str,
) -> list[str]:
    """Wähle bis zu MAX_COUNTRIES_IN_DISPLAY Länder für den display_value.

    Strategie: Erst alle aus dem Claim genannten (außer primary, das wird
    separat dargestellt), dann auffüllen mit _DISPLAY_REFERENCE_COUNTRIES.
    """
    selected: list[str] = []
    for c in requested_countries:
        if c == primary:
            continue
        if c in ratings and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    for c in _DISPLAY_REFERENCE_COUNTRIES:
        if c == primary:
            continue
        if c in ratings and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    return selected


def _select_primary_country(
    requested_countries: list[str],
    ratings: dict,
) -> str | None:
    """Wähle das primäre Land für indicator_name + country-Feld.

    Erstes Match aus dem Claim mit verfügbaren Daten. Wenn keine Country-
    Detection erfolgt ist, fällt auf AUT zurück.
    """
    for c in requested_countries:
        if c in ratings:
            return c
    if "AUT" in ratings:
        return "AUT"
    for k in ratings:
        return k
    return None


def _build_display_value(
    primary_iso3: str,
    primary_rating: dict,
    display_countries: list[str],
    ratings: dict,
) -> str:
    """Build 'RU 13/100 'Not Free' (PR 5/40, CL 8/60) — vs. AT 93, DE 93, ...
    (Freedom House FIW 2024)'.
    """
    iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
    total = primary_rating.get("total_score", "?")
    status = primary_rating.get("status", "—")
    pr = primary_rating.get("pr_score", "?")
    cl = primary_rating.get("cl_score", "?")

    head = (
        f"{iso2} {total}/100 '{status}' "
        f"(PR {pr}/40, CL {cl}/60)"
    )

    parts: list[str] = []
    for iso3 in display_countries:
        formatted = _format_country_total(iso3, ratings)
        if formatted:
            parts.append(formatted)

    if parts:
        ref = " — vs. " + ", ".join(parts)
    else:
        ref = ""

    return f"{head}{ref} (Freedom House FIW 2024)"


def _country_url(iso3: str, data: dict) -> str:
    """Konstruiere die offizielle Freedom-House-Country-URL.

    Format: https://freedomhouse.org/country/{slug}
    Slug aus country_slugs (oder lower-case ISO3 als Fallback).
    """
    slugs = data.get("country_slugs") or {}
    slug = slugs.get(iso3) or iso3.lower()
    return f"https://freedomhouse.org/country/{slug}"


async def search_freedom_house(analysis: dict) -> dict:
    """Live-Lookup gegen den Freedom-House-Static-Cache für Freiheits-Claims.

    Returns Dict mit Pipeline-Standard-Schema:
      {
        "source": "Freedom House",
        "type": "freedom_rating",
        "results": [...],   # max 1 primary country (komplementär zu V-Dem)
      }
    """
    empty = {"source": "Freedom House", "type": "freedom_rating", "results": []}

    if not analysis:
        return empty
    claim = (
        analysis.get("claim")
        or analysis.get("original_claim")
        or analysis.get("text")
        or ""
    ).strip()
    if not claim:
        return empty

    data = _load_data()
    if not data:
        logger.warning("freedom_house: static JSON konnte nicht geladen werden")
        return empty

    claim_lc = claim.lower()

    if not _has_fh_keyword(claim_lc):
        return empty

    # Country-Detection: Claim selbst + Entity-Liste.
    requested_countries = _detect_countries_in_claim(claim_lc, data)
    entities = (analysis.get("entities") or [])
    if entities:
        ents_lc = " ".join(str(e).lower() for e in entities)
        for c in _detect_countries_in_claim(ents_lc, data):
            if c not in requested_countries:
                requested_countries.append(c)

    # Wenn keine Land-Detection: DACH-Default.
    if not requested_countries:
        requested_countries = list(_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS)

    ratings = data.get("ratings") or {}
    if not ratings:
        return empty

    source_label = data.get(
        "source_label",
        "Freedom House — Freedom in the World 2024 (covering events 2023)",
    )
    secondary_url = data.get(
        "source_url", "https://freedomhouse.org/report/freedom-world"
    )
    report_year = data.get("report_year", 2024)

    # Methodik-Caveat als description.
    methodology_short = (
        data.get("methodology_note")
        or "FIW misst Political Rights (0-40) + Civil Liberties (0-60). "
           "Status: Free 70-100, Partly Free 35-69, Not Free 0-34."
    )

    results: list[dict] = []
    primary_iso3 = _select_primary_country(requested_countries, ratings)
    if primary_iso3 is None:
        return empty

    primary_rating = ratings.get(primary_iso3) or {}
    if not primary_rating:
        return empty

    iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
    total = primary_rating.get("total_score", "?")
    status = primary_rating.get("status", "—")

    # Country-Display-Name (für indicator_name): aus aliases den ersten
    # English-Alias holen, sonst ISO3.
    aliases = data.get("country_aliases", {}).get(primary_iso3) or []
    display_name = aliases[0].title() if aliases else primary_iso3
    # Versuche, einen "schöneren" englischen Namen zu finden.
    for a in aliases:
        if a in (
            "austria", "germany", "switzerland", "france", "italy",
            "spain", "united kingdom", "sweden", "norway", "denmark",
            "finland", "netherlands", "belgium", "ireland", "greece",
            "portugal", "united states", "canada", "australia",
            "new zealand", "japan", "israel", "russia", "china",
            "india", "brazil", "south africa", "turkey", "hungary",
            "poland", "czech republic", "czechia", "slovakia",
            "slovenia", "estonia", "latvia", "lithuania", "romania",
            "bulgaria", "croatia", "albania",
            "bosnia and herzegovina", "serbia", "north macedonia",
            "montenegro", "belarus", "ukraine", "moldova", "georgia",
            "armenia", "azerbaijan", "kazakhstan", "kyrgyzstan",
            "tajikistan", "turkmenistan", "uzbekistan",
        ):
            display_name = a.title()
            break

    display_countries = _select_display_countries(
        requested_countries, ratings, primary_iso3
    )

    display_value = _build_display_value(
        primary_iso3, primary_rating, display_countries, ratings
    )

    indicator_name = (
        f"Freedom House FIW {report_year} — "
        f"{display_name}: {total}/100 ({status})"
    )

    results.append({
        "indicator_name": indicator_name,
        "indicator": "freedom_house_score",
        "country": iso2,
        "year": str(report_year),
        "topic": "freedom_house_ranking",
        "display_value": display_value[:480],
        "description": methodology_short[:300],
        "url": _country_url(primary_iso3, data),
        "secondary_url": secondary_url,
        "source": source_label,
    })

    logger.info(
        f"freedom_house: 1 Treffer für country={primary_iso3} "
        f"(claim countries: {requested_countries[:3]})"
    )

    return {
        "source": "Freedom House",
        "type": "freedom_rating",
        "results": results,
    }
