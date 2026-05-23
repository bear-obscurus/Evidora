"""MIPEX (Migrant Integration Policy Index) Static-Pack-Connector.

Der Migrant Integration Policy Index (MIPEX, https://mipex.eu) wird von der
Migration Policy Group (MPG, Brüssel) gemeinsam mit dem Barcelona Centre for
International Affairs (CIDOB) publiziert. MIPEX 2020 ist die letzte
umfassende Vollerhebung und deckt 56 Länder (DACH + EU + OECD + Westbalkan +
ausgewählte Drittstaaten) auf 167 Indikatoren in 8 Politikbereichen ab:

  1. Arbeitsmarkt-Mobilität (Labour Market Mobility)
  2. Familiennachzug (Family Reunion)
  3. Bildung (Education)
  4. Politische Teilhabe (Political Participation)
  5. Daueraufenthalt (Permanent Residence)
  6. Zugang zur Staatsbürgerschaft (Access to Nationality)
  7. Anti-Diskriminierung
  8. Gesundheit (Health)

Skala 0-100 pro Bereich; aggregierter Gesamt-Wert ist arithmetisches Mittel
der 8 Bereiche.

Strategie: STATIC-FIRST-PRE-CACHE
=================================
MIPEX publiziert keine REST-API. Daten kommen als Excel/CSV-Download aus
https://www.mipex.eu/play/ (Lizenz laut MIPEX-Methodik vermutlich CC BY 4.0
— vor produktivem Einsatz verifizieren). Wir halten in ``data/mipex.json``
einen kuratierten Subset von 22 Ländern (DACH + EU-Schwerpunkt +
Vergleichs-Refs USA/CA/AU):

  - DACH: AT, DE, CH
  - EU-Top: SE, FI, PT (Top-3 nach MIPEX 2020 Overall)
  - EU-mittelfeld: BE, NL, FR, IT, ES, IE, NO, DK, GB, GR, CZ, PL, HU
  - Außerhalb EU: USA, CA, AU als internationale Vergleichs-Referenz

Refresh-Workflow (manuell, nach MIPEX-Voll-Update):
  1. Excel-Download von https://www.mipex.eu/play/
  2. Filter auf Subset-Länder + 8 Politikbereiche
  3. JSON regenerieren, mtime ändert → Hot-Reload greift automatisch

Trigger:
  - Claim enthält MIPEX-Keyword ("MIPEX", "Integrations-Index",
    "Integrationsindex", "Integrations-Politik", "Integrations-Politik-Index",
    "Migrant Integration Policy Index") UND nennt ein Land aus dem Static-Set.
  - ODER: Claim enthält MIPEX-Keyword ohne Land → Fallback auf MIPEX-Top-3
    + Bottom-3 Übersicht.
  - ODER: Claim enthält Land + ein MIPEX-Themenfeld-Schlüsselwort
    ("familiennachzug", "einbürgerung", "anti-diskriminierung", ...) UND
    explizites Index-/Vergleichs-Signal → konkretes Country-Result für das
    relevante Politikfeld.

GUARDRAILS (siehe project_political_guardrails.md):
============================================================
MIPEX bewertet "Integrations-Politik" eines Landes normativ — höhere Punkte
= "umfassendere" Politik laut MPG-Methodik. Im Kontext der Evidora-Politik-
Tabu-Guardrails muss der Service deskriptiv-deskriptiv bleiben:

  - ERLAUBT: "MIPEX 2020 vergibt für Österreich 46/100 Gesamtpunkte"
  - ERLAUBT: "AT erreicht 78/100 im Bereich Anti-Diskriminierung"
  - VERBOTEN: Eigene normative Wertung eines Landes auf einer 'gut/schlecht'-
    Skala (z.B. 'integrations-feindlich', 'rückständig', 'progressiv'). Wir
    zitieren die MIPEX-Zahl, ordnen ein Land aber nicht selbst ein.

Verdict-Strategie:
  - Bei konkreten Zahlen-Claims ("AT hat 46 MIPEX-Punkte", "Schweden hat
    über 80 von 100 MIPEX-Punkten") → true/false basierend auf Match mit
    Static-Snapshot.
  - Bei Wertungs-Claims ("X ist integrations-freundlich", "X ist
    integrations-feindlich") → die Pipeline (Synthesizer) entscheidet
    typischerweise auf 'mixed', weil der Pack einen Methodik-Caveat
    liefert: "MIPEX-Punktzahlen sind aggregierte Politik-Bewertungen,
    KEINE Aussage zu individueller Integration-Erfahrung."

KEIN harter STRUKTURELL-FALSCH-Override (siehe _struct_marker.py): MIPEX-
Zahlen sind deskriptiv, sie binär-verdicten Wertungs-Claims NICHT.

Lizenz: CC BY 4.0 (laut MIPEX-Methodik-Seite, vor produktivem Einsatz
verifizieren).

Result-Schema:
  {
    "indicator_name": "MIPEX 2020 — Österreich: 46/100 Gesamt (Rang 28/52)",
    "indicator": "mipex_overall",
    "country": "AT",
    "year": "2020",
    "topic": "mipex_integration_policy_index",
    "display_value": "AT 46/100 Gesamt (Rang 28/52) — Anti-Diskriminierung 78, Familiennachzug 31, Einbürgerung 13. Vergleich: SE 86, DE 58, CH 50 (MIPEX 2020)",
    "description": "MIPEX misst Integrations-Politiken in 56 Ländern ...",
    "url": "https://www.mipex.eu/austria",
    "secondary_url": "https://www.mipex.eu/play/",
    "source": "Migrant Integration Policy Index (MIPEX) 2020 — MPG/CIDOB",
  }

Public API:
  - claim_mentions_mipex_cached(claim: str) -> bool
  - _claim_mentions_mipex = claim_mentions_mipex_cached (Backward-Compat)
  - search_mipex(analysis: dict) -> dict

WIRING (NICHT in dieser Datei — vom Hauptprozess manuell):
  - main.py: import + tasks.append(search_mipex(analysis)) wenn
    _claim_mentions_mipex(claim) returns True. Cluster: Migration /
    Integration (neben migration_pack.py).
  - data_updater.py: MIPEX-Daten sind STATIC-PRE-CACHED, kein Prefetch nötig.
  - reranker.py: Marker "MIPEX", "Migrant Integration Policy Index",
    "Integrations-Index" als Live-Quellen-Whitelist möglich.

24h-Cache via services/cache.py.
"""

from __future__ import annotations

import logging
import os

from services._static_cache import load_json_mtime_aware
from services import cache

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "mipex.json",
)

# 24-Hour-Cache-TTL (Static-JSON ändert sich nur bei Refresh).
CACHE_TTL_SECONDS = 86400

# Hard-Trigger-Keywords (DE + EN). Bei Match → MIPEX-Pipeline aktiviert.
_MIPEX_KEYWORDS = (
    "mipex",
    "migrant integration policy index",
    "migrant-integration-policy-index",
    "integrations-index",
    "integrationsindex",
    "integrations-politik-index",
    "integrationspolitik-index",
    "integration policy index",
    "integration index",
    "mpg-index",
    "cidob-index",
)

# Soft-Trigger Politikfeld-Schlüsselwörter: Wenn diese in einem Claim mit
# einem MIPEX-Land + einem "Index"/"Vergleich"/"Punkte"-Signal stehen,
# aktiviert MIPEX ebenfalls (für Themen-spezifische Country-Vergleiche).
_MIPEX_POLICY_FIELD_KEYWORDS = (
    # 1. Arbeitsmarkt
    "arbeitsmarkt-mobilität", "arbeitsmarktmobilität",
    "arbeitsmarkt-zugang", "arbeitsmarktzugang",
    "labour market mobility", "labor market mobility",
    # 2. Familiennachzug
    "familiennachzug", "familienzusammenführung",
    "family reunion", "family reunification",
    # 3. Bildung
    "bildungs-integration", "bildungsintegration",
    "schul-integration", "schulintegration",
    "education integration",
    # 4. Politische Teilhabe
    "politische teilhabe", "politische partizipation",
    "ausländer-wahlrecht", "auslaenderwahlrecht",
    "kommunalwahlrecht", "political participation",
    # 5. Daueraufenthalt
    "daueraufenthalt", "permanent residence", "niederlassungserlaubnis",
    # 6. Staatsbürgerschaft
    "einbürgerung", "einbuergerung",
    "staatsbürgerschaft", "staatsbuergerschaft",
    "access to nationality", "citizenship",
    # 7. Anti-Diskriminierung
    "anti-diskriminierung", "antidiskriminierung",
    "gleichbehandlung", "anti-discrimination", "antidiscrimination",
    # 8. Gesundheit
    "gesundheits-integration", "gesundheitsintegration",
    "health integration", "migrant health",
)

# Signal-Wörter, die zusammen mit einem Politikfeld-Soft-Trigger einen
# echten MIPEX-Bezug nahelegen (sonst würden zu viele False-Positives
# bei z.B. Wahlrechts-Claims ohne Index-Bezug triggern).
_MIPEX_INDEX_SIGNAL_WORDS = (
    "index", "indikator", "vergleich", "ranking", "rang ", "platz",
    "punkte", "punktzahl", "score", "bewert",
    "international", "im vergleich", "im internat",
    "studie", "report", "bericht",
)

# Reference-Länder für display_value-Vergleich (DACH-Bias + EU-Spitze).
_DISPLAY_REFERENCE_COUNTRIES = (
    "AUT", "DEU", "CHE", "SWE", "FIN", "PRT",
)

# Maximum Anzahl Reference-Länder im display_value.
MAX_COUNTRIES_IN_DISPLAY = 5

# Maximum Primär-Länder pro Claim.
MAX_PRIMARY_COUNTRIES = 2

# Maximum Politikbereiche, die wir im display_value zeigen (sonst zu lang).
MAX_AREAS_IN_DISPLAY = 3

# ISO3 → ISO2 Mapping (für display_value-Kompaktheit).
_ISO3_TO_ISO2 = {
    "AUT": "AT", "DEU": "DE", "CHE": "CH",
    "SWE": "SE", "FIN": "FI", "NOR": "NO", "DNK": "DK",
    "NLD": "NL", "BEL": "BE", "FRA": "FR", "ITA": "IT",
    "ESP": "ES", "PRT": "PT", "GBR": "GB", "IRL": "IE",
    "POL": "PL", "HUN": "HU", "CZE": "CZ", "GRC": "GR",
    "USA": "US", "CAN": "CA", "AUS": "AU",
}

# MIPEX-Country-Slug für offizielle MIPEX-Country-Page-URL.
# Format: https://www.mipex.eu/{slug}
_MIPEX_COUNTRY_SLUGS = {
    "AUT": "austria", "DEU": "germany", "CHE": "switzerland",
    "SWE": "sweden", "FIN": "finland", "NOR": "norway", "DNK": "denmark",
    "NLD": "netherlands", "BEL": "belgium", "FRA": "france", "ITA": "italy",
    "ESP": "spain", "PRT": "portugal", "GBR": "united-kingdom",
    "IRL": "ireland", "POL": "poland", "HUN": "hungary", "CZE": "czech-republic",
    "GRC": "greece", "USA": "usa", "CAN": "canada", "AUS": "australia",
}

# Politikbereiche in fester Reihenfolge (für display_value-Konsistenz).
_POLICY_AREAS_ORDER = (
    "labour_market_mobility",
    "family_reunion",
    "education",
    "political_participation",
    "permanent_residence",
    "access_to_nationality",
    "anti_discrimination",
    "health",
)

# Kurz-Bezeichnungen für display_value (lang-form ist in policy_areas-Dict).
_POLICY_AREA_SHORT = {
    "labour_market_mobility": "Arbeitsmarkt",
    "family_reunion": "Familiennachzug",
    "education": "Bildung",
    "political_participation": "Polit. Teilhabe",
    "permanent_residence": "Daueraufenthalt",
    "access_to_nationality": "Einbürgerung",
    "anti_discrimination": "Anti-Diskriminierung",
    "health": "Gesundheit",
}

# Politikfeld-Keyword → Politikbereich-Key (für gezielte Themen-
# Highlighting im display_value).
_KEYWORD_TO_POLICY_AREA = {
    # 1. Arbeitsmarkt
    "arbeitsmarkt-mobilität": "labour_market_mobility",
    "arbeitsmarktmobilität": "labour_market_mobility",
    "arbeitsmarkt-zugang": "labour_market_mobility",
    "arbeitsmarktzugang": "labour_market_mobility",
    "labour market mobility": "labour_market_mobility",
    "labor market mobility": "labour_market_mobility",
    # 2. Familiennachzug
    "familiennachzug": "family_reunion",
    "familienzusammenführung": "family_reunion",
    "family reunion": "family_reunion",
    "family reunification": "family_reunion",
    # 3. Bildung
    "bildungs-integration": "education",
    "bildungsintegration": "education",
    "schul-integration": "education",
    "schulintegration": "education",
    "education integration": "education",
    # 4. Politische Teilhabe
    "politische teilhabe": "political_participation",
    "politische partizipation": "political_participation",
    "ausländer-wahlrecht": "political_participation",
    "auslaenderwahlrecht": "political_participation",
    "kommunalwahlrecht": "political_participation",
    "political participation": "political_participation",
    # 5. Daueraufenthalt
    "daueraufenthalt": "permanent_residence",
    "permanent residence": "permanent_residence",
    "niederlassungserlaubnis": "permanent_residence",
    # 6. Staatsbürgerschaft
    "einbürgerung": "access_to_nationality",
    "einbuergerung": "access_to_nationality",
    "staatsbürgerschaft": "access_to_nationality",
    "staatsbuergerschaft": "access_to_nationality",
    "access to nationality": "access_to_nationality",
    "citizenship": "access_to_nationality",
    # 7. Anti-Diskriminierung
    "anti-diskriminierung": "anti_discrimination",
    "antidiskriminierung": "anti_discrimination",
    "gleichbehandlung": "anti_discrimination",
    "anti-discrimination": "anti_discrimination",
    "antidiscrimination": "anti_discrimination",
    # 8. Gesundheit
    "gesundheits-integration": "health",
    "gesundheitsintegration": "health",
    "health integration": "health",
    "migrant health": "health",
}


def _load_data() -> dict | None:
    """Lade JSON mit Hot-Reload-Awareness."""
    return load_json_mtime_aware(STATIC_JSON_PATH)


def _country_aliases(data: dict) -> dict:
    """Country-Code → Liste von DE/EN-Substring-Aliassen."""
    return data.get("country_aliases") or {}


def _detect_countries_in_claim(claim_lc: str, data: dict) -> list[str]:
    """Erkenne welche ISO3-Country-Codes der Claim erwähnt.

    Returns Liste der ISO3-Codes (jedes Land höchstens einmal, in der
    Reihenfolge des Country-Alias-Dicts).
    """
    aliases = _country_aliases(data)
    found: list[str] = []
    for iso3, alias_list in aliases.items():
        for alias in alias_list:
            if alias.lower() in claim_lc:
                found.append(iso3)
                break
    return found


def _has_mipex_keyword(claim_lc: str) -> bool:
    """Trifft mindestens ein hartes MIPEX-Trigger-Keyword?"""
    return any(kw in claim_lc for kw in _MIPEX_KEYWORDS)


def _has_policy_field_with_index_signal(claim_lc: str) -> tuple[bool, str | None]:
    """Soft-Trigger: Politikfeld-Keyword + Index/Vergleichs-Signal.

    Returns (True, policy_area_key) wenn der Claim ein MIPEX-Politikfeld-
    Schlüsselwort enthält UND ein Index-/Vergleichs-Signal — sonst (False, None).

    Beispiel: 'Wie schneidet Österreich beim Familiennachzug im internationalen
    Vergleich ab?' → (True, 'family_reunion')

    Aber: 'Hat Österreich einen Familiennachzug?' → (False, None), weil
    kein Index-/Vergleichs-Signal.
    """
    matched_kw: str | None = None
    for kw in _MIPEX_POLICY_FIELD_KEYWORDS:
        if kw in claim_lc:
            matched_kw = kw
            break
    if matched_kw is None:
        return (False, None)

    has_signal = any(sig in claim_lc for sig in _MIPEX_INDEX_SIGNAL_WORDS)
    if not has_signal:
        return (False, None)

    return (True, _KEYWORD_TO_POLICY_AREA.get(matched_kw))


def _claim_mentions_mipex(claim: str) -> bool:
    """Trigger-Pre-Check (für main.py-Pipeline-Routing).

    Aktiviert MIPEX, wenn:
      - Claim enthält hartes MIPEX-Keyword, ODER
      - Claim enthält Politikfeld-Keyword + Index-/Vergleichs-Signal.

    Politik-Tabu-Guard 2.0: MIPEX misst Politik-Bereiche eines Landes,
    nicht Parteien. Daher kein Partei-Korruption-Superlativ-Block nötig —
    aber wir lassen den Guard zur Sicherheit aktiv.
    """
    if not claim:
        return False

    from services._topic_match import is_party_corruption_superlative_claim
    if is_party_corruption_superlative_claim(claim.lower()):
        return False

    data = _load_data()
    if not data:
        return False

    claim_lc = claim.lower()

    if _has_mipex_keyword(claim_lc):
        return True

    matched, _area = _has_policy_field_with_index_signal(claim_lc)
    return matched


def claim_mentions_mipex_cached(claim: str) -> bool:
    """Public Trigger-Check (Caching via mtime; ``_cached``-Suffix für
    Konsistenz mit anderen Service-Triggers)."""
    return _claim_mentions_mipex(claim)


# Backward-Compat-Alias (falls main.py die Underscore-Variante importiert).
_claim_mentions_mipex_alias = claim_mentions_mipex_cached


async def fetch_mipex(client=None) -> dict:
    """On-Demand-Load des MIPEX-Static-Cache.

    Returns das gesamte JSON-Dict. ``client`` wird ignoriert (nur für
    Signatur-Symmetrie mit anderen Live-Connectoren).
    """
    data = _load_data()
    if not data:
        return {}
    return data


def _format_country_overall(iso3: str, scores: dict) -> str:
    """Hilfs-Format: 'SE 86' oder 'AT n/a'."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    rec = scores.get(iso3) or {}
    val = rec.get("overall")
    if val is None:
        return f"{iso2} n/a"
    return f"{iso2} {val}"


def _select_display_countries(
    requested_countries: list[str],
    scores: dict,
    primary: str,
) -> list[str]:
    """Wähle bis zu MAX_COUNTRIES_IN_DISPLAY Vergleichsländer.

    Strategie: Erst Claim-genannte (außer primary), dann Reference-Liste
    (DACH + EU-Spitze).
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


def _select_primary_countries(
    requested_countries: list[str],
    scores: dict,
) -> list[str]:
    """Wähle primäre Länder für die Result-Liste."""
    primaries: list[str] = []
    for c in requested_countries:
        if c in scores and c not in primaries:
            primaries.append(c)
        if len(primaries) >= MAX_PRIMARY_COUNTRIES:
            return primaries
    return primaries


def _top_and_bottom(scores: dict, n: int = 1) -> list[str]:
    """Hilfs: ISO3-Codes der Top-n + Bottom-n nach overall-Score.

    Wird verwendet, wenn der Claim kein bekanntes Land erwähnt — damit
    das MIPEX-Result trotzdem nicht leer ist (Top-/Bottom-Überblick).
    """
    by_rank: list[tuple[int, str]] = []
    for iso3, rec in scores.items():
        rank = rec.get("rank")
        if isinstance(rank, int):
            by_rank.append((rank, iso3))
    by_rank.sort()
    if not by_rank:
        return []
    head = [iso3 for _, iso3 in by_rank[:n]]
    tail = [iso3 for _, iso3 in by_rank[-n:]]
    out: list[str] = []
    for c in head + tail:
        if c not in out:
            out.append(c)
    return out


def _select_focus_areas(
    primary_rec: dict,
    focus_area: str | None,
) -> list[str]:
    """Wähle MAX_AREAS_IN_DISPLAY Politikbereiche für den display_value.

    Strategie:
      - Wenn focus_area gesetzt (aus Soft-Trigger), zeige diesen Bereich
        + zwei kontrastierende (höchster + niedrigster sonstiger).
      - Sonst: Highest + Lowest + Mittel-Bereich (für Bandbreite-Signal).
    """
    available = [a for a in _POLICY_AREAS_ORDER if a in primary_rec]
    if not available:
        return []
    if focus_area and focus_area in available:
        rest = [a for a in available if a != focus_area]
        rest.sort(key=lambda a: primary_rec.get(a) or 0)
        if not rest:
            return [focus_area]
        lowest = rest[0]
        highest = rest[-1]
        out = [focus_area]
        for x in (highest, lowest):
            if x not in out:
                out.append(x)
            if len(out) >= MAX_AREAS_IN_DISPLAY:
                break
        return out[:MAX_AREAS_IN_DISPLAY]

    sorted_areas = sorted(available, key=lambda a: primary_rec.get(a) or 0)
    if len(sorted_areas) <= MAX_AREAS_IN_DISPLAY:
        return sorted_areas
    lowest = sorted_areas[0]
    highest = sorted_areas[-1]
    middle = sorted_areas[len(sorted_areas) // 2]
    out: list[str] = []
    for x in (highest, lowest, middle):
        if x not in out:
            out.append(x)
    return out[:MAX_AREAS_IN_DISPLAY]


def _build_display_value(
    primary_iso3: str,
    primary_rec: dict,
    display_countries: list[str],
    scores: dict,
    focus_area: str | None,
) -> str:
    """Build deskriptiven display_value (keine Wertung).

    Beispiel: 'AT 46/100 Gesamt (Rang 28/52) — Anti-Diskriminierung 78,
    Familiennachzug 31, Einbürgerung 13. Vergleich: SE 86, DE 58, CH 50
    (MIPEX 2020)'
    """
    iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
    overall = primary_rec.get("overall", "?")
    rank = primary_rec.get("rank", "?")

    head = f"{iso2} {overall}/100 Gesamt"
    if isinstance(rank, int):
        head = f"{iso2} {overall}/100 Gesamt (Rang {rank}/52)"

    areas = _select_focus_areas(primary_rec, focus_area)
    area_parts: list[str] = []
    for area_key in areas:
        val = primary_rec.get(area_key)
        if val is None:
            continue
        short_name = _POLICY_AREA_SHORT.get(area_key, area_key)
        area_parts.append(f"{short_name} {val}")

    detail = ""
    if area_parts:
        detail = " — " + ", ".join(area_parts)

    comp_parts: list[str] = []
    for iso3 in display_countries:
        comp_parts.append(_format_country_overall(iso3, scores))

    comp = ""
    if comp_parts:
        comp = ". Vergleich: " + ", ".join(comp_parts)

    return f"{head}{detail}{comp} (MIPEX 2020)"


def _country_url(iso3: str) -> str:
    """Offizielle MIPEX-Country-Page-URL."""
    slug = _MIPEX_COUNTRY_SLUGS.get(iso3) or iso3.lower()
    return f"https://www.mipex.eu/{slug}"


def _build_indicator_name(
    iso3: str,
    rec: dict,
    data: dict,
    focus_area: str | None,
) -> str:
    """Baue indicator_name (deskriptiv, keine Wertung)."""
    display_name = (data.get("country_display_names") or {}).get(iso3) or iso3
    overall = rec.get("overall", "?")
    rank = rec.get("rank")
    report_year = data.get("report_year", 2020)

    base = f"MIPEX {report_year} — {display_name}: {overall}/100 Gesamt"
    if isinstance(rank, int):
        base += f" (Rang {rank}/52)"

    if focus_area and focus_area in rec:
        short_name = _POLICY_AREA_SHORT.get(focus_area, focus_area)
        focus_val = rec.get(focus_area)
        if focus_val is not None:
            base += f", {short_name} {focus_val}/100"

    return base


def _build_description(data: dict) -> str:
    """Baue description: Methodik + Caveat. Bewusst deskriptiv."""
    methodology = data.get("methodology_note") or ""
    caveat = data.get("methodik_caveat") or ""
    parts: list[str] = []
    if methodology:
        parts.append(methodology)
    if caveat:
        parts.append(caveat)
    out = " ".join(parts).strip()
    return out[:600]


def _build_result_row(
    iso3: str,
    rec: dict,
    display_countries: list[str],
    scores: dict,
    data: dict,
    focus_area: str | None,
) -> dict:
    """Baue Result-Eintrag für ein Land."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    report_year = data.get("report_year", 2020)

    display_value = _build_display_value(
        iso3, rec, display_countries, scores, focus_area
    )
    indicator_name = _build_indicator_name(iso3, rec, data, focus_area)
    description = _build_description(data)

    return {
        "indicator_name": indicator_name[:200],
        "indicator": "mipex_overall" if not focus_area else f"mipex_{focus_area}",
        "country": iso2,
        "year": str(report_year),
        "topic": "mipex_integration_policy_index",
        "display_value": display_value[:480],
        "description": description,
        "url": _country_url(iso3),
        "secondary_url": data.get("source_url", "https://www.mipex.eu/play/"),
        "source": data.get(
            "source_label",
            "Migrant Integration Policy Index (MIPEX) 2020 — MPG/CIDOB",
        ),
    }


async def search_mipex(analysis: dict) -> dict:
    """Live-Lookup gegen MIPEX-Static-Cache.

    Returns Dict mit Pipeline-Standard-Schema:
      {
        "source": "MIPEX",
        "type": "integration_policy_index",
        "results": [...],   # max MAX_PRIMARY_COUNTRIES Country-Results
      }

    24h-Cache via services/cache.py.
    """
    empty = {"source": "MIPEX", "type": "integration_policy_index", "results": []}

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

    cached = cache.get("MIPEX", analysis, ttl=CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    data = _load_data()
    if not data:
        logger.warning("mipex: static JSON konnte nicht geladen werden")
        return empty

    claim_lc = claim.lower()

    # Trigger-Check: hartes Keyword ODER Soft-Trigger.
    has_hard = _has_mipex_keyword(claim_lc)
    has_soft, focus_area = _has_policy_field_with_index_signal(claim_lc)

    if not has_hard and not has_soft:
        return empty

    # Country-Detection.
    requested_countries = _detect_countries_in_claim(claim_lc, data)
    entities = (analysis.get("entities") or [])
    if entities:
        ents_lc = " ".join(str(e).lower() for e in entities)
        for c in _detect_countries_in_claim(ents_lc, data):
            if c not in requested_countries:
                requested_countries.append(c)

    scores = data.get("scores") or {}
    if not scores:
        return empty

    primaries = _select_primary_countries(requested_countries, scores)

    # Wenn keines der genannten Länder im Cache → Fallback auf
    # Top-1 + Bottom-1 als Allgemein-Overview.
    if not primaries:
        primaries = _top_and_bottom(scores, n=1)

    if not primaries:
        return empty

    results: list[dict] = []
    for iso3 in primaries:
        rec = scores.get(iso3) or {}
        if not rec:
            continue
        display_countries = _select_display_countries(
            requested_countries, scores, iso3
        )
        results.append(
            _build_result_row(iso3, rec, display_countries, scores, data, focus_area)
        )

    if not results:
        logger.info(
            f"mipex: kein Country-Result für Claim '{claim[:60]}...' "
            f"(countries={requested_countries[:3]})"
        )
        return empty

    out = {
        "source": "MIPEX",
        "type": "integration_policy_index",
        "results": results,
    }

    logger.info(
        f"mipex: {len(results)} Country-Result(s) für countries={requested_countries[:3]} "
        f"(primaries={primaries}, focus_area={focus_area})"
    )

    cache.put("MIPEX", analysis, out)
    return out
