"""Polity 5 Live-Connector — Regime-Typologie (Center for Systemic Peace).

Polity 5 (https://www.systemicpeace.org/inscrdata.html) ist das älteste
politikwissenschaftliche Demokratie-Datenset (Marshall, Gurr et al.). Es
weist jedem Staat seit 1800 einen jährlichen "Polity Score" von -10
(volle Autokratie) bis +10 (volle Demokratie) zu, plus eine Regime-
Typologie:

  +10           Full Democracy
  +6 .. +9      Democracy
  +1 .. +5      Open Anocracy
  -5 .. 0       Closed Anocracy
  -10 .. -6     Autocracy

Polity 5 deckt 167 unabhängige Staaten ab; aktuelles Coverage-Jahr ist
2018 (letzter offizieller CSP-Release p5v2018).

Komplementär zu V-Dem:
  - V-Dem: 11 Aggregat-Indizes 0-1, Bayesian-IRT-Aggregation, 470+ Sub-
    Indikatoren, Coverage 1789-aktuell, jährliches Update.
  - Polity 5: einzelner Polity-Score -10..+10, kategorische Typologie,
    längere Zeitreihe seit 1800, statische CSV (letzter Release 2018).
  - Freedom House (FIW): 0-100-Score, Status Free/Partly Free/Not Free,
    Coverage ~210 Länder, jährlich Februar.
  Die drei Quellen sind methodisch komplementär und werden im
  AT-Bildungs-Kontext häufig gemeinsam zitiert.

Strategie: STATIC-FIRST-PRE-CACHE
=================================
Polity 5 publiziert keine REST-API; der Datensatz wird als CSV/Excel
veröffentlicht (``p5v2018.xls`` bzw. ``p5v2018.csv``). Wir halten einen
kuratierten Subset in ``data/polity5.json``:

  - 2018-Scores (letztes Coverage-Jahr) für ~40 wichtigste Länder
    (DACH + EU + globale Referenz + autoritäre Vergleichs-Länder)
  - Verlauf-Highlights für 8 paradigmatische Demokratisierungs-/
    Erosions-Sprünge (DDR-Wiedervereinigung, Spanien-Transición,
    Hungary 1990, Solidarność, Russland-Erosion 1992-2018, Türkei-
    Backsliding 2016, Südafrika 1994, Österreich 1945/46).
  - 5 Regime-Kategorien (regime_categories) mit Ranges + DE/EN-Labels.

Refresh-Workflow (manuell): Wenn CSP einen p5v2019+ veröffentlicht
(stand 2026-05 noch nicht), p5v{year}.csv herunterladen, Subset
regenerieren, mtime ändern → Hot-Reload greift automatisch.

Trigger:
  - "Polity 5" / "Polity-Score" / "Polity Score" / "Polity-Index"
  - "Regime-Typologie" / "regime typology"
  - "Anokratie" / "anocracy"
  - "Polity Score [Land]" / "Polity-Score [Land]"
  - Allgemeines Polity-Vokabular ohne Land → DACH-Default.

Cache: 24h via mtime-aware Static-Loader (Hot-Reload bei JSON-Edit).
Kein HTTP — daher kein polite_client nötig. Die ``polite_client``-
Konvention bleibt für zukünftige Live-CSV-Ingestion reserviert.

Limitations:
  - Letztes offizielles Coverage-Jahr ist 2018; das ist seit 2026 fast
    8 Jahre alt. Aktuelle Politik-Entwicklungen (Putin 2022, Trump 2024,
    Orbán 2020+) sind NICHT abgedeckt — siehe V-Dem oder Freedom House.
  - Polity-Score ist ein einzelner Aggregat-Indikator; Sub-Komponenten
    (XCONST, PARCOMP, PARREG, XRCOMP, XROPEN) sind im JSON nicht
    enthalten (vereinfachender Subset).
  - Methodisch ist Polity 5 anders kalibriert als V-Dem; Vergleiche
    nicht 1:1 mit V-Dem-Indizes möglich. Caveat im display_value.

GUARDRAILS (siehe project_political_guardrails.md):
  - Wir zitieren Polity-Scores, wir bewerten sie nicht.
  - Wir nehmen keine eigene Partei-/Politiker-Bewertung vor.
  - Country-Level-Daten dürfen NICHT für Partei-Werturteile genutzt werden
    → ``is_party_corruption_superlative_claim``-Guard blockt solche Claims
    bereits im Trigger.
  - Caveat zur Methodik (autoritative Bewertung durch CSP-Forscher) ist
    Pflicht.

Result-Schema:
  {
    "indicator_name": "Polity 5 — Russia 2018: +4 (Open Anocracy)",
    "indicator": "polity5_score",
    "country": "RU",
    "year": "2018",
    "topic": "polity5_regime",
    "display_value": "RU +4 'Open Anocracy' — vs. AT +10, DE +10, ...",
    "description": "Polity 5 misst Regime-Typologie -10 (Autokratie) bis +10 (Demokratie). Letztes Coverage-Jahr 2018.",
    "url": "http://www.systemicpeace.org/inscrdata.html",
    "secondary_url": "https://en.wikipedia.org/wiki/Polity_data_series",
    "source": "Polity 5 — Center for Systemic Peace (Marshall/Gurr)",
  }

Lizenz: Public-Domain (CSP — Center for Systemic Peace, USA).

Wiring (NICHT in dieser Datei — vom Hauptprozess manuell zu setzen):
  - main.py: import + tasks.append(search_polity5(analysis)), nach
    Trigger-Check via claim_mentions_polity5_cached.
  - data_updater.py: optional Prefetch des Static-JSON beim Boot (nur
    Static-Load; kein HTTP nötig).
  - reranker.py: Indicator-Whitelist-Marker für 'polity5_score' /
    'Polity 5' aufnehmen; Live-Quelle, NICHT in AUTHORITATIVE-Pack-
    Markern.
  - confidence_calibration.py: optional, für Boosts wie V-Dem/Freedom-
    House (Demokratie-Themen-Boost).
"""

from __future__ import annotations

import logging
import os

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "polity5.json",
)

# DACH-Default-Länder, wenn Claim Polity-Keyword nennt aber kein Land.
_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS = ("AUT", "DEU", "CHE")

# Trigger-Keywords (DE + EN). Bei Match + Land → Treffer.
# Bei Match ohne Land → DACH-Default.
_POLITY_KEYWORDS = (
    "polity 5", "polity5", "polity-5",
    "polity 4", "polity4", "polity-4",
    "polity score", "polity-score", "polityscore",
    "polity index", "polity-index", "polityindex",
    "polity-typologie", "polity typology",
    "polity-skala", "polity skala",
    "polity project", "polity-project", "polity projekt",
    "regime-typologie", "regimetypologie", "regime typology",
    "demokratie-autokratie-skala", "demokratie autokratie skala",
    "demokratie-autokratie skala",
    "anokratie", "anokratien", "anocracy", "anocracies",
    "offene anokratie", "geschlossene anokratie",
    "open anocracy", "closed anocracy",
    "marshall gurr", "marshall/gurr",
    "center for systemic peace", "systemic peace",
    "csp polity",
)

# Composite-Trigger: 20-30 wichtige Country-Tokens (DE + EN).
# Wird zusammen mit Polity-Keywords zu Composite-Triggern verknüpft,
# damit "Polity 5 Russland …" / "Regime-Typologie China" robust greift,
# auch wenn die data/polity5.json gerade nicht ladbar ist.
_COUNTRY_TOKENS: tuple[str, ...] = (
    "österreich", "oesterreich", "austria",
    "deutschland", "germany",
    "schweiz", "switzerland",
    "russland", "russia", "russland", "russische föderation",
    "usa", "vereinigte staaten", "united states",
    "china", "volksrepublik china",
    "türkei", "tuerkei", "turkey",
    "ungarn", "hungary",
    "polen", "poland",
    "frankreich", "france",
    "italien", "italy",
    "spanien", "spain",
    "schweden", "sweden",
    "ukraine",
    "belarus", "weißrussland", "weissrussland",
    "iran",
    "saudi-arabien", "saudi arabien", "saudi arabia",
    "nordkorea", "north korea",
    "südkorea", "suedkorea", "south korea",
    "venezuela",
    "südafrika", "suedafrika", "south africa",
    "indien", "india",
    "japan",
    "brasilien", "brazil",
    "großbritannien", "grossbritannien", "vereinigtes königreich",
    "united kingdom",
)

# Year-Pattern: 4-stellige Jahreszahl 19xx oder 20xx (1800-2099 grob).
import re as _re  # noqa: E402  (local import to keep top section tidy)
_YEAR_PATTERN = _re.compile(r"\b(1[89]\d{2}|20\d{2})\b")

# Reference-Länder für display_value Multi-Country-Comparison.
# AT-Bias: AT zuerst, dann DE/CH, dann Demokratie-/Autokratie-Kontrast.
_DISPLAY_REFERENCE_COUNTRIES = (
    "AUT", "DEU", "CHE", "SWE", "USA", "HUN", "TUR",
    "RUS", "CHN", "PRK", "SAU",
)

# Maximum Anzahl Reference-Länder im display_value (neben Primary).
MAX_COUNTRIES_IN_DISPLAY = 5

# ISO3 → ISO2 Mapping (für display_value-Kompaktheit).
_ISO3_TO_ISO2 = {
    "AUT": "AT", "DEU": "DE", "CHE": "CH", "FRA": "FR", "ITA": "IT",
    "ESP": "ES", "GBR": "UK", "SWE": "SE", "NOR": "NO", "DNK": "DK",
    "FIN": "FI", "NLD": "NL", "BEL": "BE", "IRL": "IE", "GRC": "GR",
    "USA": "US", "CAN": "CA", "AUS": "AU", "NZL": "NZ", "JPN": "JP",
    "ISR": "IL", "RUS": "RU", "CHN": "CN", "IND": "IN", "BRA": "BR",
    "MEX": "MX", "ARG": "AR", "ZAF": "ZA", "TUR": "TR", "HUN": "HU",
    "POL": "PL", "CZE": "CZ", "SVK": "SK", "ROU": "RO", "BGR": "BG",
    "UKR": "UA", "BLR": "BY", "EGY": "EG", "IRN": "IR", "SAU": "SA",
    "PRK": "KP", "KOR": "KR", "VEN": "VE",
}


def _load_data() -> dict | None:
    """Lade JSON mit Hot-Reload-Awareness (24h-Cache via mtime)."""
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


def _has_polity_keyword(claim_lc: str) -> bool:
    """Trifft mindestens ein Polity-Trigger-Keyword?"""
    return any(kw in claim_lc for kw in _POLITY_KEYWORDS)


def _has_country_token(claim_lc: str) -> bool:
    """Trifft mindestens einen der hartcodierten Country-Tokens?

    Unabhängig von ``data/polity5.json`` — damit der Trigger auch bei
    Static-JSON-Reload-Pausen / fehlender Datei robust bleibt.
    """
    return any(tok in claim_lc for tok in _COUNTRY_TOKENS)


def _has_year_pattern(claim_lc: str) -> bool:
    """Trifft eine 4-stellige Jahreszahl (1800-2099)?"""
    return bool(_YEAR_PATTERN.search(claim_lc))


def _claim_mentions_polity5(claim: str) -> bool:
    """Trigger-Pre-Check für main.py-Pipeline-Routing.

    Composite-Trigger-Logik:
      1. Politik-Tabu-Guard 2.0 (Partei+Korruption+Superlativ) → block.
      2. Polity-Keyword + Country-Token (+ optional Year) → trigger.
      3. Polity-Keyword allein → trigger mit DACH-Default.
      4. Country-Token + Year ohne Polity-Keyword → KEIN Trigger
         (verhindert False-Positives bei generischen Länder-Claims).
    """
    if not claim:
        return False
    claim_lc = claim.lower()
    # Politik-Tabu-Guard 2.0 ZUERST — Country-Level-Demokratie-Daten
    # dürfen NICHT für Partei-Werturteile missbraucht werden.
    from services._topic_match import is_party_corruption_superlative_claim
    if is_party_corruption_superlative_claim(claim_lc):
        return False
    if not _has_polity_keyword(claim_lc):
        return False
    # Composite-Bonus: Polity-Keyword + Country (+ optional Year) → trigger
    # auch ohne data-Load (Static-JSON-Reload-Robustheit).
    if _has_country_token(claim_lc):
        # Year-Pattern ist optional, dient nur Logging/Boost.
        return True
    # Polity-Keyword alleine: Data muss laden, sonst kein DACH-Fallback.
    data = _load_data()
    if not data:
        return False
    return True


# Public alias matching the V-Dem/freedom_house naming convention.
claim_mentions_polity5_cached = _claim_mentions_polity5


def _regime_label(category: str, data: dict, lang: str = "de") -> str:
    """Resolve regime_category → display-Label (DE oder EN)."""
    cats = data.get("regime_categories") or {}
    cat = cats.get(category) or {}
    key = f"label_{lang}"
    return cat.get(key) or category.replace("_", " ").title()


def _format_country_score(iso3: str, scores: dict) -> str:
    """Hilfs-Format: 'AT +10' oder 'RU +4' oder 'PRK -10'."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    rec = scores.get(iso3) or {}
    s = rec.get("score")
    if s is None:
        return ""
    sign = "+" if s > 0 else ("" if s == 0 else "")  # '-' kommt aus str(s)
    return f"{iso2} {sign}{s}"


def _select_display_countries(
    requested_countries: list[str],
    scores: dict,
    primary: str,
) -> list[str]:
    """Wähle bis zu MAX_COUNTRIES_IN_DISPLAY Länder für den display_value.

    Strategie: Erst Claim-Länder (ohne primary), dann
    _DISPLAY_REFERENCE_COUNTRIES (DACH-Default + Kontrast).
    """
    selected: list[str] = []
    for c in requested_countries:
        if c == primary:
            continue
        if c in scores and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    for c in _DISPLAY_REFERENCE_COUNTRIES:
        if c == primary:
            continue
        if c in scores and c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    return selected


def _select_primary_country(
    requested_countries: list[str],
    scores: dict,
) -> str | None:
    """Wähle das primäre Land für indicator_name + country-Feld.

    Erstes Match aus dem Claim mit verfügbaren Daten, sonst AUT-Fallback.
    """
    for c in requested_countries:
        if c in scores:
            return c
    if "AUT" in scores:
        return "AUT"
    for k in scores:
        return k
    return None


def _build_display_value(
    primary_iso3: str,
    primary_rec: dict,
    display_countries: list[str],
    scores: dict,
    data: dict,
    year: int | str,
) -> str:
    """Build 'RU +4 'Open Anocracy' — vs. AT +10, DE +10, SE +10, ...
    (Polity 5, 2018)'.
    """
    iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
    s = primary_rec.get("score")
    cat = primary_rec.get("category", "")
    label = _regime_label(cat, data, lang="en")
    if s is None:
        head = f"{iso2} ? '{label}'"
    else:
        sign = "+" if s > 0 else ""
        head = f"{iso2} {sign}{s} '{label}'"

    parts: list[str] = []
    for iso3 in display_countries:
        formatted = _format_country_score(iso3, scores)
        if formatted:
            parts.append(formatted)
    ref = (" — vs. " + ", ".join(parts)) if parts else ""
    return f"{head}{ref} (Polity 5, {year})"


async def search_polity5(analysis: dict) -> dict:
    """Live-Lookup gegen den Polity-5-Static-Cache für Regime-Claims.

    Returns Dict mit Pipeline-Standard-Schema:
      {
        "source": "Polity 5",
        "type": "regime_data",
        "results": [...],   # 1 primary country + optional 1 history-highlight
      }

    Wenn der Claim ein im JSON markiertes History-Highlight (z. B. "DDR",
    "Wiedervereinigung", "Transición", "Solidarność") trifft, wird ein
    Zusatz-Result mit Verlauf angehängt.
    """
    empty = {"source": "Polity 5", "type": "regime_data", "results": []}

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
        logger.warning("polity5: static JSON konnte nicht geladen werden")
        return empty

    claim_lc = claim.lower()

    if not _has_polity_keyword(claim_lc):
        return empty

    # Country-Detection: Claim selbst + Entity-Liste.
    requested_countries = _detect_countries_in_claim(claim_lc, data)
    entities = analysis.get("entities") or []
    if entities:
        ents_lc = " ".join(str(e).lower() for e in entities)
        for c in _detect_countries_in_claim(ents_lc, data):
            if c not in requested_countries:
                requested_countries.append(c)

    # Wenn keine Land-Detection: DACH-Default.
    if not requested_countries:
        requested_countries = list(_DEFAULT_COUNTRIES_FOR_DACH_CLAIMS)

    scores = data.get("scores_2018") or {}
    if not scores:
        return empty

    source_label = data.get(
        "source_label",
        "Polity 5 — Center for Systemic Peace (Marshall/Gurr)",
    )
    source_url = data.get("source_url", "http://www.systemicpeace.org/inscrdata.html")
    secondary_url = data.get(
        "secondary_url", "https://en.wikipedia.org/wiki/Polity_data_series"
    )
    year_label = 2018

    description = (
        "Polity 5 misst Regime-Typologie auf einer Skala von -10 (volle "
        "Autokratie) bis +10 (volle Demokratie); Kategorien: Full Democracy "
        "(+10), Democracy (+6..+9), Open Anocracy (+1..+5), Closed Anocracy "
        "(-5..0), Autocracy (-10..-6). Letztes Coverage-Jahr 2018 (CSP)."
    )

    results: list[dict] = []
    primary_iso3 = _select_primary_country(requested_countries, scores)
    if primary_iso3 is None:
        return empty

    primary_rec = scores.get(primary_iso3) or {}
    if not primary_rec:
        return empty

    iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
    s = primary_rec.get("score")
    cat = primary_rec.get("category", "")
    label_en = _regime_label(cat, data, lang="en")
    label_de = _regime_label(cat, data, lang="de")

    # Country-Display-Name aus country_codes (DE) — Fallback ISO3.
    country_codes = data.get("country_codes") or {}
    display_name = country_codes.get(primary_iso3) or primary_iso3

    display_countries = _select_display_countries(
        requested_countries, scores, primary_iso3
    )
    display_value = _build_display_value(
        primary_iso3, primary_rec, display_countries, scores, data, year_label
    )

    sign = "+" if isinstance(s, int) and s > 0 else ""
    indicator_name = (
        f"Polity 5 — {display_name} {year_label}: "
        f"{sign}{s} ({label_de} / {label_en})"
    )

    results.append({
        "indicator_name": indicator_name,
        "indicator": "polity5_score",
        "country": iso2,
        "year": str(year_label),
        "topic": "polity5_regime",
        "display_value": display_value[:480],
        "description": description[:300],
        "url": source_url,
        "secondary_url": secondary_url,
        "source": source_label,
    })

    # Optional: History-Highlight wenn Claim einen Transition-Marker trifft.
    history_marker_keywords = (
        "ddr", "wiedervereinigung", "mauerfall",
        "transición", "transicion", "franco",
        "solidarność", "solidarnosc", "solidarnos",
        "putin", "jelzin", "yeltsin",
        "erdogan", "erdoğan", "putschversuch",
        "apartheid", "mandela",
        "staatsvertrag", "1955", "1945", "1946",
        "kádár", "kadar",
        "demokratisierung", "demokratischer übergang",
        "demokratischer uebergang", "democratic transition",
        "backsliding", "demokratie-erosion", "demokratie-abbau",
    )
    history_match: dict | None = None
    if any(m in claim_lc for m in history_marker_keywords):
        for hist in (data.get("history_highlights") or []):
            hist_country = hist.get("country")
            if not hist_country:
                continue
            # Trigger History-Highlight nur wenn entweder das Country
            # selbst im Claim/Entities erkannt wurde, oder im
            # Highlight-Label ein Marker steckt, der im Claim auftaucht.
            if hist_country in requested_countries:
                history_match = hist
                break
            label_text = (
                f"{hist.get('label_de', '')} {hist.get('label_en', '')}"
            ).lower()
            if any(m in label_text and m in claim_lc for m in history_marker_keywords):
                history_match = hist
                break

    if history_match is not None:
        h_country = history_match.get("country", "")
        h_iso2 = _ISO3_TO_ISO2.get(h_country, h_country[:2])
        h_label_de = history_match.get("label_de", "")
        transitions = history_match.get("transitions") or []
        trans_parts: list[str] = []
        for t in transitions:
            y = t.get("year")
            sc = t.get("score")
            note = t.get("note_de", "")
            if y is None or sc is None:
                continue
            sign = "+" if isinstance(sc, int) and sc > 0 else ""
            trans_parts.append(f"{y}: {sign}{sc} ({note})")
        if trans_parts:
            hist_display = " | ".join(trans_parts)
            results.append({
                "indicator_name": (
                    f"Polity 5 Verlauf — {h_label_de} ({h_country})"
                ),
                "indicator": "polity5_history",
                "country": h_iso2,
                "year": "varies",
                "topic": "polity5_regime_history",
                "display_value": hist_display[:480],
                "description": (
                    f"Historische Polity-5-Transition: {h_label_de}. "
                    "Punkt-Auszüge aus der CSP-Annual-Series."
                )[:300],
                "url": source_url,
                "secondary_url": secondary_url,
                "source": source_label,
            })

    logger.info(
        f"polity5: {len(results)} Treffer für country={primary_iso3} "
        f"(claim countries: {requested_countries[:3]}, "
        f"history={'yes' if history_match else 'no'})"
    )

    return {
        "source": "Polity 5",
        "type": "regime_data",
        "results": results,
    }
