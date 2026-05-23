"""Climate Action Tracker (CAT) — Static-First-Pack-Service für Klima-Pledge-Bewertungen.

Climate Action Tracker (https://climateactiontracker.org) ist eine wissenschaftlich
unabhängige Analyse-Initiative von Climate Analytics + NewClimate Institute. CAT
bewertet seit 2009 die Klimaschutz-Pledges der ~40 wichtigsten Emitter-Staaten
gegen das 1.5°C-Limit des Pariser Abkommens.

Bewertungs-Skala (CAT 2024):
  - "1.5°C Paris Agreement compatible" — kompatibel mit 1.5°C-Limit
  - "Almost sufficient" — knapp darunter (~1.5-2°C)
  - "Insufficient" — ~2-3°C globale Erwärmung
  - "Highly insufficient" — ~3-4°C
  - "Critically insufficient" — >4°C (schlechtester CAT-Score)

Vier Bewertungs-Dimensionen pro Land:
  1. Overall-Rating: Gesamteinschätzung
  2. Targets-Rating: NDC-Ambition (offizielle nationale Klimaziele)
  3. Current-Policies-Rating: Was tatsächlich umgesetzt wird
  4. Fair-Share-Rating: Lasten-Verteilung (globale Gerechtigkeit)

Strategie: STATIC-FIRST-SNAPSHOT
=================================
CAT publiziert keine REST-API. Daten kommen von den öffentlichen
Country-Pages (https://climateactiontracker.org/countries/COUNTRY/).
Wir halten in ``data/climate_action_tracker.json`` einen kuratierten
Snapshot von ~41 Ländern: alle DACH-Staaten, EU-relevante, BRICS, OPEC,
sowie alle CAT-getrackten Großemittenten.

Refresh-Workflow (manuell, ~halbjährlich nach CAT-Update):
  1. CAT publiziert vor jeder COP (November) ein Major-Update.
  2. Pro Land Page laden, Rating-Bewertungen aktualisieren.
  3. JSON regenerieren, mtime ändert → Hot-Reload greift automatisch.

Trigger:
  - Claim enthält CAT-Keyword ("climate action tracker", "CAT",
    "Pariser Abkommen", "NDC", "Klimaziele", "1.5-Grad-Limit", ...)
    UND nennt ein Land aus dem Static-Set
  - ODER: Claim enthält CAT-Keyword OHNE Land → Default DACH (AT/DE/CH)

POLITIK-TABU-COMPLIANCE
========================
CAT bewertet LÄNDER (nicht Parteien), und das ist evidenzbasiert
(Pledge-vs-Trajectory-Modell). Wir zitieren CAT-Bewertungen DESKRIPTIV:

  "CAT (Stand 2024-11) bewertet Österreich als 'Insufficient'
   bei den NDC-Targets."   ← OK

NICHT erlaubt (eigene Wertung):
  "AT ist Klimaschutz-Versager."         ← NEIN, das ist Sentiment.
  "AT ist Klimaschutz-Pionier."          ← NEIN, ebenfalls Sentiment.

CAT publiziert eine technische Skala, kein moralisches Urteil. Aussagen
wie "X ist Pionier" oder "X ist Versager" sind in beiden Richtungen
Sentiment-Wertungen, die CAT NICHT vornimmt — und Evidora übernimmt
sie deshalb auch NICHT.

Methodik-Caveat ist PFLICHT in description: CAT ist nur EINE Methodik
(Pledge-vs-Trajectory). Alternative Methodologien (Climate Watch /
OECD ENV-Linkages / IPCC AR6 Pathways) kommen zu anderen Bewertungen.

Lizenz: CAT erlaubt freie Nutzung mit Quellenangabe (Attribution an
"Climate Action Tracker / Climate Analytics + NewClimate Institute").
Keine kommerzielle Re-Lizenzierung. Quellen-URL pro Land im Result.

Result-Schema:
  {
    "indicator_name": "CAT (Stand 2024-11): Österreich Overall 'Insufficient'",
    "indicator": "cat_climate_rating",
    "country": "AT",
    "year": "2024",
    "topic": "climate_action_tracker",
    "display_value": "AT Overall: Insufficient | Targets: Insufficient | Current Policies: Insufficient | Fair Share: Insufficient",
    "description": "CAT-Methodik: Pledge-vs-Pariser-Trajectory. Caveat: alternative Methodologien (Climate Watch, OECD ENV-Linkages, IPCC AR6) kommen zu anderen Bewertungen.",
    "url": "https://climateactiontracker.org/countries/eu-countries/",
    "secondary_url": "https://climateactiontracker.org/countries/",
    "source": "Climate Action Tracker (CAT) — Country Assessments (Stand 2024-11)",
  }

Public API:
  - claim_mentions_cat_cached(claim: str) -> bool   (Trigger-Pre-Check)
  - search_cat(analysis: dict) -> dict              (Pipeline-Result)
  - fetch_cat(client=None) -> dict                  (Static-Load für data_updater)

WIRING (NICHT in dieser Datei — vom Hauptprozess manuell):
  - main.py: import + tasks.append(search_cat(analysis)) wenn
    claim_mentions_cat_cached(claim) returns True.
  - reranker.py: "Climate Action Tracker", "CAT", "climateactiontracker.org"
    als Live-Quellen-Whitelist möglich.
  - data_updater.py: keine Prefetch nötig (Static-First, ~halbjährliches
    manuelles Refresh).
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
    "climate_action_tracker.json",
)

# 24-Hour-Cache-TTL (CAT-JSON ändert sich nur bei Refresh ~halbjährlich).
CACHE_TTL_SECONDS = 86400

# Trigger-Keywords (DE + EN). Bei Match + Land → konkretes Country-Result.
# Bei Match ohne Land → DACH-Default (AT/DE/CH).
_CAT_KEYWORDS = (
    # Eigenname CAT
    "climate action tracker",
    "climateactiontracker",
    " cat ",
    "cat-bewertung",
    "cat bewertung",
    "cat rating",
    "cat-rating",
    # Pariser Abkommen
    "pariser abkommen",
    "paris agreement",
    "paris-abkommen",
    "pariser klimaabkommen",
    "pariser klimavertrag",
    "paris climate agreement",
    "paris climate accord",
    # NDC / Klimaziele
    "ndc",
    "nationally determined contribution",
    "nationally determined contributions",
    "national bestimmter beitrag",
    "national determined contribution",
    "klimaziele",
    "klima-ziele",
    "climate target",
    "climate targets",
    "klimaschutzziele",
    "klimaschutz-ziele",
    # 1.5°C-Limit
    "1.5 grad",
    "1.5-grad",
    "1.5 °c",
    "1.5°c",
    "1,5 grad",
    "1,5-grad",
    "1,5°c",
    "1.5 degree",
    "1.5-degree",
    "1.5°c-limit",
    "1.5°c-pfad",
    "1.5 grad pfad",
    "1.5-grad-pfad",
    "2-grad-ziel",
    "2 grad ziel",
    # CAT-Klassifikatoren
    "1.5°c compatible",
    "1.5 compatible",
    "almost sufficient",
    "highly insufficient",
    "critically insufficient",
    # Klima-Pledge-Vokabular (eng mit CAT verknüpft)
    "klimapledge", "klima-pledge", "climate pledge",
    "net-zero-pledge", "netto-null-pledge",
    "klimaschutz-pledge", "klimaschutz-zusage",
    # Pioneer/Versager-Sentiment (für Trigger, in Pipeline normativ-tabu)
    "klimaschutz-pionier", "klimaschutz pionier",
    "klimaschutz-versager", "klimaschutz versager",
    "klimaschutz-vorbild", "klimaschutz vorbild",
    "klima-pionier", "klima-versager",
)

# DACH-Default-Länder, wenn Claim CAT-Keyword nennt aber kein Land.
_DEFAULT_COUNTRIES_FOR_DACH = ("AUT", "DEU", "CHE")

# Maximum Primär-Länder pro Claim (CAT ist Country-Profile-Service).
MAX_PRIMARY_COUNTRIES = 3

# ISO3 → ISO2 für display_value-Kompaktheit.
_ISO3_TO_ISO2 = {
    "AUT": "AT", "DEU": "DE", "CHE": "CH", "FRA": "FR", "GBR": "GB",
    "ITA": "IT", "ESP": "ES", "POL": "PL", "NLD": "NL", "SWE": "SE",
    "NOR": "NO", "TUR": "TR", "USA": "US", "CAN": "CA", "MEX": "MX",
    "BRA": "BR", "ARG": "AR", "CHL": "CL", "COL": "CO", "PER": "PE",
    "CHN": "CN", "IND": "IN", "JPN": "JP", "KOR": "KR", "IDN": "ID",
    "THA": "TH", "VNM": "VN", "PHL": "PH", "KAZ": "KZ", "RUS": "RU",
    "UKR": "UA", "ZAF": "ZA", "MAR": "MA", "EGY": "EG", "NGA": "NG",
    "ETH": "ET", "AUS": "AU", "NZL": "NZ", "SAU": "SA", "ARE": "AE",
    "IRN": "IR",
}


def _load_data() -> dict | None:
    """Lade CAT-JSON mit Hot-Reload-Awareness."""
    return load_json_mtime_aware(STATIC_JSON_PATH)


def _country_aliases(data: dict) -> dict:
    """Country-Code → Liste von DE/EN-Substring-Aliassen."""
    return data.get("country_aliases") or {}


def _detect_countries_in_claim(claim_lc: str, data: dict) -> list[str]:
    """Erkenne welche ISO3-Country-Codes der Claim erwähnt."""
    aliases = _country_aliases(data)
    found: list[str] = []
    for iso3, alias_list in aliases.items():
        for alias in alias_list:
            if alias.lower() in claim_lc:
                if iso3 not in found:
                    found.append(iso3)
                break
    return found


def _has_cat_keyword(claim_lc: str) -> bool:
    """Trifft mindestens ein CAT-Trigger-Keyword?

    Beachte: " cat " hat Whitespace-Padding, damit es nicht in "category",
    "catch", "scatter" etc. matcht.
    """
    # Padding-Trick für ' cat ' — anfügen damit Wort-Boundary geprüft wird.
    padded = f" {claim_lc} "
    return any(kw in padded for kw in _CAT_KEYWORDS)


def _claim_mentions_cat(claim: str) -> bool:
    """Trigger-Pre-Check (für main.py-Pipeline-Routing).

    Returns True, wenn:
      - Claim enthält CAT-/Pariser-Abkommen-/NDC-Keyword UND
      - kein Politik-Tabu-Guard-2.0-Block (Partei-Korruption-Superlativ).
    """
    if not claim:
        return False

    # Politik-Tabu-Guard 2.0: CAT misst Länder, nicht Parteien.
    try:
        from services._topic_match import is_party_corruption_superlative_claim
        if is_party_corruption_superlative_claim(claim.lower()):
            return False
    except Exception:
        # Falls Helper nicht verfügbar: graceful Fallback.
        pass

    return _has_cat_keyword(claim.lower())


def claim_mentions_cat_cached(claim: str) -> bool:
    """Public Trigger-Check für main.py-Fan-Out.

    Caching ist im _load_data via mtime gehandhabt; Funktionsname behält
    das ``_cached``-Suffix für Konsistenz mit anderen Service-Triggers.
    """
    return _claim_mentions_cat(claim)


async def fetch_cat(client=None) -> dict:
    """On-Demand-Load des CAT-Static-Cache.

    Returns das gesamte JSON-Dict (mit scores/country_aliases/...).
    ``client`` wird ignoriert (nur für Signatur-Symmetrie mit anderen
    Live-Connectoren).
    """
    data = _load_data()
    if not data:
        return {}
    return data


def _country_iso2(iso3: str) -> str:
    return _ISO3_TO_ISO2.get(iso3, iso3[:2])


def _build_display_value(iso3: str, rec: dict) -> str:
    """'AT Overall: Insufficient | Targets: Insufficient | ...' """
    iso2 = _country_iso2(iso3)
    parts = [
        f"{iso2} Overall: {rec.get('overall_rating', 'n/a')}",
        f"Targets: {rec.get('targets_rating', 'n/a')}",
        f"Current Policies: {rec.get('current_policies_rating', 'n/a')}",
        f"Fair Share: {rec.get('fair_share_rating', 'n/a')}",
    ]
    return " | ".join(parts)


def _build_indicator_name(iso3: str, rec: dict, display_name: str) -> str:
    """'CAT (Stand 2024-11): Österreich Overall \\'Insufficient\\''."""
    assessment_date = rec.get("assessment_date") or "2024"
    overall = rec.get("overall_rating", "n/a")
    return f"CAT (Stand {assessment_date}): {display_name} Overall '{overall}'"


def _build_description(rec: dict, data: dict) -> str:
    """Description = Methodik-Caveat + ggf. Country-Note."""
    methodology = (
        "CAT-Methodik: Bewertung der Klimaschutz-Pledges gegen das 1.5°C-Limit "
        "des Pariser Abkommens (Pledge-vs-Trajectory-Modell von Climate Analytics "
        "+ NewClimate Institute). Skala: '1.5°C compatible' / 'Almost sufficient' / "
        "'Insufficient' / 'Highly insufficient' / 'Critically insufficient'. "
        "CAT publiziert ~halbjährlich vor COP-Treffen. "
        "WICHTIGER KONTEXT: CAT ist EINE Methodik; alternative Bewertungen "
        "(Climate Watch, OECD ENV-Linkages, IPCC AR6 Pathways) kommen teils zu "
        "anderen Ergebnissen. Vergleich nur innerhalb gleicher Methodik-Version sinnvoll. "
        "Die CAT-Skala ist deskriptiv-technisch (Pledge-Konsistenz mit 1.5°C-Pfad), "
        "NICHT normativ-wertend ('Pionier/Versager')."
    )
    note = rec.get("note", "").strip()
    if note:
        return f"{methodology} Hinweis zum Land: {note}"
    return methodology


def _country_url(iso3: str, rec: dict, data: dict) -> str:
    """Country-spezifische CAT-URL aus dem Record (Fallback: globale URL)."""
    return rec.get("url") or data.get("source_url", "https://climateactiontracker.org/countries/")


def _build_result_row(iso3: str, rec: dict, data: dict) -> dict:
    """Baue einen Result-Eintrag für ein Land."""
    iso2 = _country_iso2(iso3)
    display_name = (data.get("country_display_names") or {}).get(iso3) or iso3
    assessment_date = rec.get("assessment_date") or "2024-11"

    return {
        "indicator_name": _build_indicator_name(iso3, rec, display_name)[:200],
        "indicator": "cat_climate_rating",
        "country": iso2,
        "year": str(assessment_date)[:4],
        "topic": "climate_action_tracker",
        "display_value": _build_display_value(iso3, rec)[:480],
        "description": _build_description(rec, data)[:600],
        "url": _country_url(iso3, rec, data),
        "secondary_url": data.get("source_url", "https://climateactiontracker.org/countries/"),
        "source": data.get(
            "source_label",
            "Climate Action Tracker (CAT) — Country Assessments",
        ),
    }


def _build_methodology_caveat_row(data: dict) -> dict:
    """Methodik-Caveat-Eintrag als letzter Result (Pflicht für Synthesizer)."""
    kernsatz = data.get(
        "kernsatz_fuer_synthesizer",
        "CAT ist EINE Methodik (Pledge-vs-Trajectory); alternative Methodologien "
        "(Climate Watch, OECD ENV-Linkages) kommen zu anderen Bewertungen. "
        "Vergleich nur innerhalb gleicher Methodik-Version sinnvoll. "
        "CAT-Skala ist deskriptiv, nicht normativ — Aussagen wie "
        "'Klimaschutz-Pionier/-Versager' sind Wertungen, die CAT nicht "
        "vornimmt und Evidora nicht übernimmt.",
    )
    return {
        "indicator_name": "WICHTIGER KONTEXT: CAT-Methodik-Caveat",
        "indicator": "cat_methodology_caveat",
        "country": "",
        "year": str(data.get("report_year", 2024)),
        "topic": "climate_action_tracker",
        "display_value": "",
        "description": kernsatz[:600],
        "url": data.get("source_url", "https://climateactiontracker.org/countries/"),
        "secondary_url": "https://climateactiontracker.org/methodology/",
        "source": data.get(
            "source_label",
            "Climate Action Tracker (CAT) — Country Assessments",
        ),
    }


async def search_cat(analysis: dict) -> dict:
    """Lookup gegen CAT-Static-Cache.

    Returns Dict mit Pipeline-Standard-Schema:
      {
        "source": "Climate Action Tracker (CAT)",
        "type": "climate_pledge_rating",
        "results": [...],   # max MAX_PRIMARY_COUNTRIES + 1 Caveat
      }

    24h-Cache via services/cache.py.
    """
    empty = {
        "source": "Climate Action Tracker (CAT)",
        "type": "climate_pledge_rating",
        "results": [],
    }

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

    # 24h-Cache-Hit?
    cached = cache.get("CAT", analysis, ttl=CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    data = _load_data()
    if not data:
        logger.warning("climate_action_tracker: static JSON konnte nicht geladen werden")
        return empty

    claim_lc = claim.lower()

    if not _has_cat_keyword(claim_lc):
        return empty

    # Politik-Tabu-Guard 2.0: CAT misst Länder, nicht Parteien.
    try:
        from services._topic_match import is_party_corruption_superlative_claim
        if is_party_corruption_superlative_claim(claim_lc):
            return empty
    except Exception:
        pass

    # Country-Detection: Claim selbst + Entity-Liste.
    requested_countries = _detect_countries_in_claim(claim_lc, data)
    entities = (analysis.get("entities") or [])
    if entities:
        ents_lc = " ".join(str(e).lower() for e in entities)
        for c in _detect_countries_in_claim(ents_lc, data):
            if c not in requested_countries:
                requested_countries.append(c)
    ner_countries = (analysis.get("ner_entities", {}) or {}).get("countries", []) or []
    if ner_countries:
        ner_lc = " ".join(str(c).lower() for c in ner_countries)
        for c in _detect_countries_in_claim(ner_lc, data):
            if c not in requested_countries:
                requested_countries.append(c)

    scores = data.get("scores") or {}
    if not scores:
        return empty

    # Primary-Country-Selection.
    primaries: list[str] = []
    for c in requested_countries:
        if c in scores and c not in primaries:
            primaries.append(c)
        if len(primaries) >= MAX_PRIMARY_COUNTRIES:
            break

    # Wenn keines der genannten Länder im CAT-Cache: Fallback DACH-Default.
    if not primaries:
        for c in _DEFAULT_COUNTRIES_FOR_DACH:
            if c in scores:
                primaries.append(c)

    if not primaries:
        return empty

    results: list[dict] = []
    for iso3 in primaries:
        rec = scores.get(iso3) or {}
        if not rec:
            continue
        results.append(_build_result_row(iso3, rec, data))

    if not results:
        logger.info(
            f"cat: kein Country-Result für Claim '{claim[:60]}...' "
            f"(countries={requested_countries[:3]})"
        )
        return empty

    # Methodik-Caveat als letzter Eintrag — Pflicht für Synthesizer.
    results.append(_build_methodology_caveat_row(data))

    out = {
        "source": "Climate Action Tracker (CAT)",
        "type": "climate_pledge_rating",
        "results": results,
    }

    logger.info(
        f"cat: {len(results) - 1} Country-Result(s) + 1 Caveat für "
        f"countries={requested_countries[:3]} (primaries={primaries})"
    )

    cache.put("CAT", analysis, out)
    return out
