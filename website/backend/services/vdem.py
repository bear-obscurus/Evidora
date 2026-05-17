"""V-Dem (Varieties of Democracy) Live-API-Connector — Demokratie-Indizes.

V-Dem (https://v-dem.net) ist die größte Demokratie-Datenbank weltweit
(University of Gothenburg, Sweden), mit Daten für 202 Länder von 1789
bis heute. V-Dem misst Demokratie nicht als binäres Konzept, sondern
über fünf Hochlevel-Indizes (Liberal, Electoral, Participatory,
Deliberative, Egalitarian) plus 470+ Sub-Indikatoren.

Strategie: STATIC-FIRST-PRE-CACHE
=================================
Der V-Dem-Datensatz ist groß (~30 MB CSV, 1789-2023, 202 Länder × 470
Indikatoren) und wird nur einmal pro Jahr aktualisiert (Frühjahr).
Statt dem Live-Download bei jedem Claim halten wir einen kuratierten
Subset in ``data/vdem_indicators.json``:

  - ~12 Schlüssel-Indikatoren (v2x_libdem, v2x_polyarchy, etc.)
  - ~32 Länder (DACH + EU + globale Referenz: USA, RUS, CHN, ...)
  - 5 Jahre (2019-2023)

Bei Bedarf kann der JSON via Cron-Job aus dem offiziellen V-Dem v14
CSV-Release neu generiert werden.

Trigger:
  - Claim enthält Demokratie-/Rechtsstaats-Keyword (eines der Indikator-
    keywords_de/keywords_en) UND nennt ein Land aus dem Static-Set.
  - ODER: Claim enthält allgemeines Demokratie-Vokabular ("demokratie-
    index", "rechtsstaat", "freie wahlen", ...) auch ohne explizite
    Land-Nennung — dann werden DACH-Default-Länder geliefert.

Limitations:
  - V-Dem-Daten sind 1-2 Jahre alt (jährlicher Release-Zyklus).
  - V-Dem-Aggregat-Indizes haben methodische Konfidenzintervalle
    (typisch +/- 0.05 auf 0-1-Skala).
  - Aktuelle Werte basieren auf V-Dem v14 (März 2024). Werte sind
    LLM-Wissens-Approximationen — vor produktivem Einsatz mit den
    offiziellen v14-CSVs gegenchecken (audit_flag im JSON).
  - Aktuelle Version 14 (Stand März 2024), nächste v15 Frühjahr 2025.

GUARDRAILS (siehe project_political_guardrails.md):
  - Wir zitieren V-Dem-Scores, wir bewerten sie nicht.
  - Wir nehmen keine eigene Partei- oder Politiker-Bewertung vor.
  - Caveat zur Methodik (Experten-Befragung, Bayesian-IRT) ist Pflicht.

Result-Schema (siehe data/vdem_indicators.json):
  Jeder Treffer enthält indicator_name + display_value mit
  Multi-Country-Comparison (max 5 Länder), description, url, source.

Wiring (NICHT in dieser Datei — vom Hauptprozess manuell):
  - main.py: import + tasks.append(search_vdem(analysis))
  - reranker.py: V-Dem ist Live-Quelle, NICHT in AUTHORITATIVE-Pack-Markern
    (Indicator-Whitelist-Marker für indicator_name kann aufgenommen werden).
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
    "vdem_indicators.json",
)

# DACH-Default-Länder, wenn Claim Demokratie-Keyword nennt aber kein Land.
_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS = ("AUT", "DEU", "CHE")

# Allgemeine Demokratie-Vokabeln (zusätzlich zu Indicator-spezifischen
# Keywords) — triggern auch ohne explizite Land-Nennung mit Default-DACH.
_DEMOCRACY_VOCAB = (
    "demokratie-index", "demokratieindex", "demokratie",
    "demokratie-rückgang", "demokratie-rueckgang", "demokratie-niveau",
    "demokratiequalität", "demokratiequalitaet",
    "rechtsstaatlichkeit", "rechtsstaat",
    "demokratiemessung",
    "v-dem", "vdem", "varieties of democracy",
    "autokratisierung", "autoritarismus",
    "autoritär", "autoritaer", "autoritäres", "autoritaeres",
    "autocratization", "authoritarian",
    "demokratie-monitor",
    "demokratie geht zurück",
    "demokratie schwächt sich ab",
    "diktatur", "diktator", "autokratie", "autokrat",
    "regime", "repression", "unfreie wahlen", "scheinwahlen",
    "polizeistaat", "überwachungsstaat",
)

# Reference-Länder für display_value Multi-Country-Comparison.
# AT-Bias: AT immer first, dann DE, dann höchst- und niedrigst-Wertige
# als Kontrast.
_DISPLAY_REFERENCE_COUNTRIES = (
    "AUT", "DEU", "CHE", "SWE", "NOR", "USA", "HUN", "POL",
    "RUS", "CHN", "TUR",
)

# Maximum Anzahl Indikatoren, die wir pro Claim liefern.
MAX_INDICATORS_PER_CLAIM = 3
# Maximum Anzahl Länder im display_value pro Indikator.
MAX_COUNTRIES_IN_DISPLAY = 5

# ISO3 → ISO2 Mapping (nur für Display-Kompaktheit).
_ISO3_TO_ISO2 = {
    "AUT": "AT", "DEU": "DE", "CHE": "CH", "FRA": "FR", "ITA": "IT",
    "ESP": "ES", "GBR": "UK", "SWE": "SE", "NOR": "NO", "DNK": "DK",
    "NLD": "NL", "BEL": "BE", "USA": "US", "CAN": "CA", "AUS": "AU",
    "NZL": "NZ", "JPN": "JP", "ISR": "IL", "RUS": "RU", "CHN": "CN",
    "IND": "IN", "BRA": "BR", "ZAF": "ZA", "TUR": "TR", "HUN": "HU",
    "POL": "PL", "CZE": "CZ", "SVK": "SK", "SVN": "SI", "EST": "EE",
    "LVA": "LV", "LTU": "LT",
}


def _load_data() -> dict | None:
    """Lade JSON mit Hot-Reload-Awareness."""
    return load_json_mtime_aware(STATIC_JSON_PATH)


def _country_aliases(data: dict) -> dict:
    """Country-Code → Liste von DE/EN-Substring-Aliassen."""
    return data.get("country_aliases") or {}


def _detect_countries_in_claim(claim_lc: str, data: dict) -> list[str]:
    """Erkenne welche ISO3-Country-Codes der Claim erwähnt.

    Returns Liste der ISO3-Codes (in Reihenfolge des Country-Code-Dicts;
    ein Land wird höchstens einmal eingefügt).
    """
    aliases = _country_aliases(data)
    found: list[str] = []
    for iso3, alias_list in aliases.items():
        for alias in alias_list:
            if alias.lower() in claim_lc:
                found.append(iso3)
                break  # nur einmal pro Land
    return found


def _indicator_matches_claim(indicator: dict, claim_lc: str) -> bool:
    """Trifft der Indicator den Claim via Keyword?"""
    for kw in indicator.get("keywords_de") or ():
        if kw.lower() in claim_lc:
            return True
    for kw in indicator.get("keywords_en") or ():
        if kw.lower() in claim_lc:
            return True
    return False


def _has_general_democracy_vocab(claim_lc: str) -> bool:
    """Generische Demokratie-Vokabel — auch wenn keine spezifische
    Indikator-Keyword matcht."""
    return any(v in claim_lc for v in _DEMOCRACY_VOCAB)


def claim_mentions_vdem_cached(claim: str) -> bool:
    """Trigger-Pre-Check (für main.py-Pipeline-Routing).

    Returns True, wenn der Claim eine Land+Demokratie-Kombi enthält
    ODER eine generische Demokratie-Vokabel.

    Der Alias ``_claim_mentions_vdem`` unten erhält Backward-Compat
    für pre-Phase-4a-Imports in main.py.
    """
    if not claim:
        return False
    # Politik-Tabu-Guard 2.0: Partei + Korruption + Superlativ ohne Anker → block
    # V-Dem misst Länder-Demokratie, Partei-Werturteile sind Kategorienfehler.
    from services._topic_match import is_party_corruption_superlative_claim
    if is_party_corruption_superlative_claim(claim.lower()):
        return False
    data = _load_data()
    if not data:
        return False
    claim_lc = claim.lower()

    # Generische Demokratie-Vokabel reicht.
    if _has_general_democracy_vocab(claim_lc):
        return True

    # Sonst: Land + indicator-keyword.
    countries_found = _detect_countries_in_claim(claim_lc, data)
    if not countries_found:
        return False

    indicators = data.get("indicators") or []
    return any(_indicator_matches_claim(ind, claim_lc) for ind in indicators)


async def fetch_vdem(client=None) -> list[dict]:
    """On-Demand-Load der V-Dem-Indikator-Liste aus dem Static-JSON.

    Returns Liste der Indicator-Dicts (mit code, label_de, label_en,
    keywords, data, ...). ``client`` wird ignoriert (nur für Signatur-
    Symmetrie mit anderen Live-Connectoren).
    """
    data = _load_data()
    if not data:
        return []
    return data.get("indicators") or []


def _format_country_year(iso3: str, year_data: dict, year: str) -> str:
    """Hilfs-Format: 'AT 0.85'."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    val = year_data.get(year)
    if val is None:
        return ""
    return f"{iso2} {val:.2f}"


def _build_display_value(
    indicator: dict,
    countries_in_display: list[str],
    year: str,
    data_block: dict,
) -> str:
    """Build display_value: 'AT 0.85 / DE 0.85 / SE 0.92 / HU 0.34 / RU 0.07
    / WORLD median ~0.40 (V-Dem v14, 2023)'."""
    parts: list[str] = []
    for iso3 in countries_in_display:
        country_data = data_block.get(iso3) or {}
        formatted = _format_country_year(iso3, country_data, year)
        if formatted:
            parts.append(formatted)
    median = indicator.get("world_median_2023")
    median_str = (
        f" / WORLD median ~{median:.2f}" if isinstance(median, (int, float)) else ""
    )
    return (
        f"{' / '.join(parts)}{median_str} (V-Dem v14, {year})"
    )


def _select_display_countries(
    requested_countries: list[str],
    indicator_data: dict,
) -> list[str]:
    """Wähle bis zu MAX_COUNTRIES_IN_DISPLAY Länder für den display_value.

    Strategie: Erst die im Claim genannten Länder, dann auffüllen mit
    _DISPLAY_REFERENCE_COUNTRIES (DACH-Default + Kontrast-Länder), aber
    nur wenn sie Daten haben.
    """
    selected: list[str] = []
    for c in requested_countries:
        if c in indicator_data and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    for c in _DISPLAY_REFERENCE_COUNTRIES:
        if c in indicator_data and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    return selected


def _select_primary_country(
    requested_countries: list[str],
    indicator_data: dict,
) -> str:
    """Wähle das primäre Land für indicator_name + country-Feld.

    Erstes Match aus dem Claim, sonst AUT als DACH-Default.
    """
    for c in requested_countries:
        if c in indicator_data:
            return c
    if "AUT" in indicator_data:
        return "AUT"
    for k in indicator_data:
        return k
    return "AUT"


def _latest_year(country_data: dict) -> str:
    """Neuestes Jahr im country_data — als String. Fallback: '2023'."""
    if not country_data:
        return "2023"
    try:
        return max(country_data.keys(), key=lambda y: int(y))
    except (ValueError, TypeError):
        return "2023"


async def search_vdem(analysis: dict) -> dict:
    """Live-Lookup gegen V-Dem-Static-Cache für Demokratie-Claims.

    Returns Dict mit Pipeline-Standard-Schema:
      {
        "source": "V-Dem",
        "type": "democracy_index",
        "results": [...],   # max 3 Indicators
      }
    """
    empty = {"source": "V-Dem", "type": "democracy_index", "results": []}

    if not analysis:
        return empty
    claim = (analysis.get("claim") or analysis.get("original_claim") or analysis.get("text") or "").strip()
    if not claim:
        return empty

    data = _load_data()
    if not data:
        logger.warning("V-Dem: static JSON konnte nicht geladen werden")
        return empty

    claim_lc = claim.lower()

    # Country-Detection: Erst aus dem Claim selbst, dann aus Entity-Liste.
    requested_countries = _detect_countries_in_claim(claim_lc, data)
    entities = (analysis.get("entities") or [])
    if entities:
        ents_lc = " ".join(str(e).lower() for e in entities)
        for c in _detect_countries_in_claim(ents_lc, data):
            if c not in requested_countries:
                requested_countries.append(c)

    has_general = _has_general_democracy_vocab(claim_lc)

    if not requested_countries and not has_general:
        return empty

    # Wenn nur generic-democracy-Vokabel ohne Land, default DACH.
    if not requested_countries and has_general:
        requested_countries = list(_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS)

    # Wähle passende Indikatoren: Erst die mit Keyword-Match, dann (falls
    # leer und has_general) Default-Set: libdem + polyarchy + freexp.
    indicators = data.get("indicators") or []
    matched_indicators = [
        ind for ind in indicators if _indicator_matches_claim(ind, claim_lc)
    ]

    if not matched_indicators and has_general:
        # Default-Auswahl der drei aussagekräftigsten für general-democracy
        default_codes = ("v2x_libdem", "v2x_polyarchy", "v2x_freexp")
        matched_indicators = [
            ind for ind in indicators if ind.get("code") in default_codes
        ]

    if not matched_indicators:
        return empty

    matched_indicators = matched_indicators[:MAX_INDICATORS_PER_CLAIM]

    source_label = data.get(
        "source_label",
        "V-Dem Institute (University of Gothenburg, Sweden)",
    )
    source_url = data.get(
        "source_url", "https://www.v-dem.net/data/the-v-dem-dataset/"
    )
    secondary_url = data.get(
        "secondary_url", "https://en.wikipedia.org/wiki/V-Dem_Democracy_Indices"
    )

    results: list[dict] = []
    for ind in matched_indicators:
        ind_data = ind.get("data") or {}
        if not ind_data:
            continue

        primary_iso3 = _select_primary_country(requested_countries, ind_data)
        primary_iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
        primary_country_data = ind_data.get(primary_iso3) or {}
        year = _latest_year(primary_country_data)
        primary_value = primary_country_data.get(year)

        if primary_value is None:
            continue

        display_countries = _select_display_countries(
            requested_countries, ind_data
        )
        display_value = _build_display_value(
            ind, display_countries, year, ind_data
        )

        label_de = ind.get("label_de") or ind.get("code") or ""
        scale = ind.get("scale") or ""
        description = ind.get("description_de") or ind.get(
            "description_en"
        ) or ""

        # indicator_name: knapp + AT-zentriert (wenn AT detected).
        indicator_name = (
            f"{label_de} {primary_iso2} {year}: "
            f"{primary_value:.2f} (V-Dem)"
        )

        results.append({
            "indicator_name": indicator_name,
            "indicator": "vdem_index",
            "country": primary_iso2,
            "year": str(year),
            "topic": "vdem_democracy",
            "display_value": display_value[:480],
            "description": (
                f"{description} ({scale})" if scale else description
            )[:300],
            "url": source_url,
            "secondary_url": secondary_url,
            "source": source_label,
        })

    if not results:
        logger.info(
            f"V-Dem: kein Indicator mit Daten für Claim "
            f"'{claim[:60]}...' (countries={requested_countries})"
        )
        return empty

    logger.info(
        f"V-Dem: {len(results)} Indicator-Treffer "
        f"(countries={requested_countries[:3]}, "
        f"codes={[r['indicator_name'][:25] for r in results]})"
    )

    return {
        "source": "V-Dem",
        "type": "democracy_index",
        "results": results,
    }


# Backward-compat alias for pre-Phase-4a main.py imports.
_claim_mentions_vdem = claim_mentions_vdem_cached
