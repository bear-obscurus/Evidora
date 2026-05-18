"""Bertelsmann Transformation Index (BTI) Live-Connector — 137 Transformationsländer.

Der Bertelsmann Transformation Index (BTI, https://bti-project.org) wird seit
2003 alle 2 Jahre von der Bertelsmann Stiftung (Gütersloh) veröffentlicht und
bewertet 137 Transformationsländer (also keine etablierten Demokratien wie
AT/DE/CH/USA) auf einer 1-10-Skala in drei Dimensionen:

  1. Politische Transformation (Demokratie-Status, 10 Kriterien)
  2. Wirtschaftliche Transformation (Marktwirtschafts-Status, 7 Kriterien)
  3. Governance-Performance (Steuerungsleistung, 5 Kriterien)

Komplementär zu V-Dem (Continuous-Index) und Freedom House (Aggregat-Score):
  - V-Dem: jährlich, 202 Länder, Bayesian-IRT aus Experten-Befragungen.
  - Freedom House: jährlich, 195 Länder, Political-Rights + Civil-Liberties.
  - BTI: 2-jährlich, 137 Transformationsländer, Doppel-Gutachter + Board-Review,
    inklusive Marktwirtschafts- und Governance-Dimensionen.

Strategie: STATIC-FIRST-PRE-CACHE
=================================
BTI publiziert keine REST-API. Daten kommen als Excel/CSV-Download aus
https://bti-project.org/en/downloads (CC-BY 4.0). Wir halten in
``data/bti_2024.json`` einen kuratierten Subset von ~54 Ländern:

  - Top-20-BTI-Status-Index (Taiwan, Estland, Tschechien, Uruguay, ...)
  - Bottom-20-BTI-Status-Index (Eritrea, Nordkorea, Afghanistan, Myanmar, ...)
  - AT/DE-relevante Vergleichsländer (Russland, China, Türkei, Ungarn, Polen,
    Ukraine — werden im Faktencheck-Kontext häufig genannt)

Refresh-Workflow (manuell, alle 2 Jahre nach BTI-Release):
  1. Excel-Download von bti-project.org/en/downloads
  2. Filter auf Top-20 + Bottom-20 + AT/DE-Comparator-Liste
  3. JSON regenerieren, mtime ändert → Hot-Reload greift automatisch

Trigger:
  - Claim enthält BTI-Keyword ("BTI", "Bertelsmann Transformation Index",
    "Transformations-Index", "Demokratie-Status", "Marktwirtschafts-Status",
    "Governance-Index") UND nennt ein Land aus dem Static-Set.
  - ODER: Claim enthält BTI-Keyword ohne Land → Fallback auf BTI-Top-3
    + Bottom-3 Übersicht (signalisiert "BTI-Ranking allgemein").

Limitations:
  - AT/DE/CH/USA werden vom BTI NICHT erfasst (sind etablierte Demokratien,
    nicht in Transformation). Bei Claims über diese Länder liefern wir nur
    Vergleichs-Kontext für genannte Drittstaaten.
  - BTI-Werte alle 2 Jahre (BTI 2024 = Erhebungszeitraum 2021-2023).
  - Werte sind LLM-Approximationen — AUDIT-FLAG im JSON. Refresh aus
    offiziellem Excel nötig vor produktivem Einsatz.

GUARDRAILS (siehe project_political_guardrails.md):
  - Wir zitieren BTI-Scores, wir bewerten Parteien NICHT.
  - Politik-Tabu-Guard 2.0 (services/_topic_match.is_party_corruption_superlative_claim)
    blockt Partei-Korruption-Superlativ-Claims ohne empirischen Anker.
  - Methodik-Caveat (Doppel-Gutachter, Bertelsmann Stiftung als deutsche
    Privat-Stiftung, 2-Jahres-Zyklus) ist Pflicht in der description.

Lizenz: CC-BY 4.0 (Bertelsmann Stiftung, BTI Datasets)

Result-Schema:
  {
    "indicator_name": "BTI 2024 — Russia: Status-Index 3.35/10 (Rang 117/137, Harte Autokratie)",
    "indicator": "bti_status_index",
    "country": "RU",
    "year": "2024",
    "topic": "bti_transformation_index",
    "display_value": "RU 3.35/10 (Rang 117/137) — vs. AT n/a (BTI deckt nur Transformationsländer), DE n/a, HU 6.94, TR 4.70, CN 4.15, PRK 1.50 (BTI 2024)",
    "description": "BTI misst 137 Transformationsländer auf 1-10-Skala in 3 Dimensionen ...",
    "url": "https://bti-project.org/en/reports/country-report/rus",
    "secondary_url": "https://bti-project.org/en/downloads",
    "source": "Bertelsmann Transformation Index (BTI) 2024 — CC-BY 4.0",
  }

Public API:
  - claim_mentions_bti_cached(claim: str) -> bool  (Trigger-Pre-Check)
  - _claim_mentions_bti = claim_mentions_bti_cached (Backward-Compat-Alias)
  - search_bti(analysis: dict) -> dict             (Pipeline-Result)

WIRING (NICHT in dieser Datei — vom Hauptprozess manuell):
  - main.py: import + tasks.append(search_bti(analysis)) wenn
    _claim_mentions_bti(claim) returns True. Cluster: Demokratie/Politik
    (neben vdem.py und freedom_house.py).
  - data_updater.py: BTI-Daten sind STATIC-PRE-CACHED, kein Prefetch nötig.
    Refresh-Workflow alle 2 Jahre manuell (siehe oben).
  - reranker.py: Marker "BTI", "Bertelsmann Transformation Index",
    "bti_status_index" als Live-Quellen-Whitelist möglich (NICHT in
    AUTHORITATIVE-Pack-Markern).
  - confidence_calibration.py: optional, BTI als Demokratie-Cluster-Boost.

24h-Cache: Service-Result wird via services/cache.py mit ttl=86400 gehalten
(Static-First-JSON ändert sich nur bei Refresh, also kann 24h gecachet werden).
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
    "bti_2024.json",
)

# 24-Hour-Cache-TTL (BTI-JSON ändert sich nur bei Refresh alle 2 Jahre).
CACHE_TTL_SECONDS = 86400

# Trigger-Keywords (DE + EN). Bei Match + Land → konkretes BTI-Result.
# Bei Match ohne Land → BTI-Top/Bottom-Overview.
_BTI_KEYWORDS = (
    "bti",
    "bertelsmann transformation index",
    "bertelsmann-transformation-index",
    "bertelsmann transformationsindex",
    "transformations-index", "transformationsindex",
    "transformation index",
    "bti-ranking", "bti ranking",
    "demokratie-status", "demokratiestatus", "democracy status",
    "marktwirtschafts-status", "marktwirtschaftsstatus",
    "governance-index", "governanceindex", "governance index",
    "transformations-status", "transformationsstatus",
    "transformationsländer", "transformationslaender",
    "transformation countries",
    "bertelsmann stiftung index",
    "demokratie in konsolidierung",
    "defekte demokratie", "hochdefekte demokratie",
    "moderate autokratie", "harte autokratie",
)

# Reference-Länder für display_value-Vergleich.
# AT-Bias: AT/DE explizit als "n/a" markieren (BTI deckt sie nicht ab) —
# stattdessen Kontrast-Länder aus dem AT-Diskurs (Russland, China, Türkei,
# Ungarn, Polen, Ukraine).
_DISPLAY_REFERENCE_COUNTRIES = (
    "HUN", "POL", "TUR", "UKR", "RUS", "CHN", "PRK", "TWN", "EST",
)

# Maximum Anzahl Reference-Länder im display_value.
MAX_COUNTRIES_IN_DISPLAY = 5

# Maximum Primär-Länder pro Claim (BTI ist Country-Profile-Service).
MAX_PRIMARY_COUNTRIES = 2

# ISO3 → ISO2 Mapping (für display_value-Kompaktheit).
_ISO3_TO_ISO2 = {
    "TWN": "TW", "EST": "EE", "CZE": "CZ", "URY": "UY", "LTU": "LT",
    "SVN": "SI", "CHL": "CL", "KOR": "KR", "LVA": "LV", "POL": "PL",
    "CRI": "CR", "MUS": "MU", "BWA": "BW", "BGR": "BG", "HRV": "HR",
    "ROU": "RO", "PAN": "PA", "JAM": "JM", "ARG": "AR", "BRA": "BR",
    "GHA": "GH", "IND": "IN", "ZAF": "ZA", "MEX": "MX", "PHL": "PH",
    "IDN": "ID", "UKR": "UA", "GEO": "GE", "MDA": "MD", "ARM": "AM",
    "HUN": "HU", "SRB": "RS", "TUR": "TR", "TUN": "TN", "PAK": "PK",
    "BGD": "BD", "EGY": "EG", "BLR": "BY", "RUS": "RU", "CUB": "CU",
    "VEN": "VE", "NIC": "NI", "IRN": "IR", "MMR": "MM", "AFG": "AF",
    "PRK": "KP", "ERI": "ER", "SOM": "SO", "SYR": "SY", "YEM": "YE",
    "SDN": "SD", "TKM": "TM", "TJK": "TJ", "CHN": "CN",
    # AT/DE/CH werden vom BTI nicht erfasst, aber Mapping für Erkennung
    # in Claims:
    "AUT": "AT", "DEU": "DE", "CHE": "CH", "USA": "US",
}

# Country-Slug für offizielle BTI-Country-Report-URL.
# Format: https://bti-project.org/en/reports/country-report/{slug}
_BTI_COUNTRY_SLUGS = {
    "TWN": "twn", "EST": "est", "CZE": "cze", "URY": "ury", "LTU": "ltu",
    "SVN": "svn", "CHL": "chl", "KOR": "kor", "LVA": "lva", "POL": "pol",
    "CRI": "cri", "MUS": "mus", "BWA": "bwa", "BGR": "bgr", "HRV": "hrv",
    "ROU": "rou", "PAN": "pan", "JAM": "jam", "ARG": "arg", "BRA": "bra",
    "GHA": "gha", "IND": "ind", "ZAF": "zaf", "MEX": "mex", "PHL": "phl",
    "IDN": "idn", "UKR": "ukr", "GEO": "geo", "MDA": "mda", "ARM": "arm",
    "HUN": "hun", "SRB": "srb", "TUR": "tur", "TUN": "tun", "PAK": "pak",
    "BGD": "bgd", "EGY": "egy", "BLR": "blr", "RUS": "rus", "CUB": "cub",
    "VEN": "ven", "NIC": "nic", "IRN": "irn", "MMR": "mmr", "AFG": "afg",
    "PRK": "prk", "ERI": "eri", "SOM": "som", "SYR": "syr", "YEM": "yem",
    "SDN": "sdn", "TKM": "tkm", "TJK": "tjk", "CHN": "chn",
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
                break  # nur einmal pro Land
    return found


def _has_bti_keyword(claim_lc: str) -> bool:
    """Trifft mindestens ein BTI-Trigger-Keyword?"""
    return any(kw in claim_lc for kw in _BTI_KEYWORDS)


def _claim_mentions_bti(claim: str) -> bool:
    """Trigger-Pre-Check (für main.py-Pipeline-Routing).

    Returns True, wenn:
      - Claim enthält BTI-Keyword UND
      - kein Politik-Tabu-Guard-2.0-Block (Partei-Korruption-Superlativ).

    Bei BTI-Keyword + Land → konkretes Country-Result.
    Bei BTI-Keyword ohne Land → BTI-Top/Bottom-Overview.

    Auch ohne BTI-Keyword: Wenn Claim einen Transformationsland-Namen +
    einen der spezifischen BTI-Klassifikations-Begriffe enthält
    ("defekte demokratie", "harte autokratie", ...), triggert ebenfalls.
    """
    if not claim:
        return False

    # Politik-Tabu-Guard 2.0: BTI misst Länder, nicht Parteien.
    from services._topic_match import is_party_corruption_superlative_claim
    if is_party_corruption_superlative_claim(claim.lower()):
        return False

    data = _load_data()
    if not data:
        return False

    claim_lc = claim.lower()
    return _has_bti_keyword(claim_lc)


def claim_mentions_bti_cached(claim: str) -> bool:
    """Public Trigger-Check (Caching ist im _load_data via mtime gehandhabt;
    Funktionsname behält das ``_cached``-Suffix für Konsistenz mit anderen
    Service-Triggers in der Pipeline)."""
    return _claim_mentions_bti(claim)


async def fetch_bti(client=None) -> dict:
    """On-Demand-Load des BTI-Static-Cache.

    Returns das gesamte JSON-Dict (mit scores/country_aliases/source_label/...).
    ``client`` wird ignoriert (nur für Signatur-Symmetrie mit anderen
    Live-Connectoren).
    """
    data = _load_data()
    if not data:
        return {}
    return data


def _format_country_status(iso3: str, scores: dict) -> str:
    """Hilfs-Format: 'RU 3.35/10' oder 'AT n/a'."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    rec = scores.get(iso3) or {}
    val = rec.get("status_index")
    if val is None:
        return f"{iso2} n/a"
    return f"{iso2} {val:.2f}/10"


def _select_display_countries(
    requested_countries: list[str],
    scores: dict,
    primary: str,
) -> list[str]:
    """Wähle bis zu MAX_COUNTRIES_IN_DISPLAY Vergleichsländer für display_value.

    Strategie: Erst Claim-genannte (außer primary), dann Reference-Liste
    (AT-Bias: Russland, China, Ungarn, Polen, Türkei, Ukraine).
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
    """Wähle die primären Länder für die Result-Liste.

    Strategie: Erst die im Claim genannten + im BTI-Cache erfassten Länder
    (bis MAX_PRIMARY_COUNTRIES). Wenn keines im BTI-Cache (z.B. nur
    AT/DE/CH/USA), fällt zurück auf Top-1 + Bottom-1 als Allgemein-Overview.
    """
    primaries: list[str] = []
    for c in requested_countries:
        if c in scores and c not in primaries:
            primaries.append(c)
        if len(primaries) >= MAX_PRIMARY_COUNTRIES:
            return primaries
    return primaries


def _top_and_bottom(scores: dict, n: int = 1) -> list[str]:
    """Hilfs: ISO3-Codes der Top-n + Bottom-n nach status_index.

    Wird verwendet, wenn der Claim kein BTI-Land erwähnt (z.B. nur AT/DE),
    damit das BTI-Result trotzdem nicht leer ist.
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


def _build_display_value(
    primary_iso3: str,
    primary_rec: dict,
    display_countries: list[str],
    scores: dict,
) -> str:
    """Build 'RU 3.35/10 (Rang 117/137) — vs. AT n/a (BTI deckt nur
    Transformationsländer), HU 6.94, TR 4.70, CN 4.15 (BTI 2024)'.
    """
    iso2 = _ISO3_TO_ISO2.get(primary_iso3, primary_iso3[:2])
    val = primary_rec.get("status_index", "?")
    rank = primary_rec.get("rank", "?")
    head = f"{iso2} {val:.2f}/10 (Rang {rank}/137)" if isinstance(val, (int, float)) else f"{iso2} n/a"

    parts: list[str] = []
    for iso3 in display_countries:
        parts.append(_format_country_status(iso3, scores))

    if parts:
        ref = " — vs. " + ", ".join(parts)
    else:
        ref = ""

    return f"{head}{ref} (BTI 2024)"


def _country_url(iso3: str) -> str:
    """Offizielle BTI-Country-Report-URL."""
    slug = _BTI_COUNTRY_SLUGS.get(iso3) or iso3.lower()
    return f"https://bti-project.org/en/reports/country-report/{slug}"


def _build_result_row(
    iso3: str,
    rec: dict,
    display_countries: list[str],
    scores: dict,
    data: dict,
) -> dict:
    """Baue einen einzelnen Result-Eintrag für ein Land."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    display_name = (data.get("country_display_names") or {}).get(iso3) or iso3
    val = rec.get("status_index")
    rank = rec.get("rank")
    political_status = rec.get("political_status") or ""
    political = rec.get("political")
    economic = rec.get("economic")
    governance = rec.get("governance")
    report_year = data.get("report_year", 2024)

    display_value = _build_display_value(iso3, rec, display_countries, scores)

    val_str = f"{val:.2f}" if isinstance(val, (int, float)) else "n/a"
    indicator_name = (
        f"BTI {report_year} — {display_name}: "
        f"Status-Index {val_str}/10 (Rang {rank}/137, {political_status})"
    )

    methodology_short = (
        data.get("methodology_note")
        or "BTI misst 137 Transformationsländer auf 1-10-Skala in drei Dimensionen "
           "(politisch, wirtschaftlich, governance). Doppel-Gutachter + Board-Review, "
           "2-Jahres-Zyklus."
    )
    # Kurze Dimensions-Zusammenfassung an description hängen.
    dim_summary = ""
    if isinstance(political, (int, float)) and isinstance(economic, (int, float)) and isinstance(governance, (int, float)):
        dim_summary = (
            f" {display_name} Dimensionen: politisch {political:.2f}, "
            f"wirtschaftlich {economic:.2f}, governance {governance:.2f}."
        )

    description = (methodology_short + dim_summary)[:300]

    return {
        "indicator_name": indicator_name[:200],
        "indicator": "bti_status_index",
        "country": iso2,
        "year": str(report_year),
        "topic": "bti_transformation_index",
        "display_value": display_value[:480],
        "description": description,
        "url": _country_url(iso3),
        "secondary_url": data.get("source_url", "https://bti-project.org/en/downloads"),
        "source": data.get(
            "source_label",
            "Bertelsmann Transformation Index (BTI) 2024 — CC-BY 4.0",
        ),
    }


async def search_bti(analysis: dict) -> dict:
    """Live-Lookup gegen BTI-Static-Cache für Transformations-Index-Claims.

    Returns Dict mit Pipeline-Standard-Schema:
      {
        "source": "BTI",
        "type": "transformation_index",
        "results": [...],   # max 2 primary countries
      }

    24h-Cache via services/cache.py.
    """
    empty = {"source": "BTI", "type": "transformation_index", "results": []}

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
    cached = cache.get("BTI", analysis, ttl=CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    data = _load_data()
    if not data:
        logger.warning("bti: static JSON konnte nicht geladen werden")
        return empty

    claim_lc = claim.lower()

    if not _has_bti_keyword(claim_lc):
        return empty

    # Country-Detection: Claim selbst + Entity-Liste.
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

    # Primary-Country-Selection.
    primaries = _select_primary_countries(requested_countries, scores)

    # Wenn keines der genannten Länder im BTI-Cache (z.B. nur AT/DE):
    # Fallback auf BTI-Top-1 + Bottom-1 als Allgemein-Overview.
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
        results.append(_build_result_row(iso3, rec, display_countries, scores, data))

    if not results:
        logger.info(
            f"bti: kein Country-Result für Claim '{claim[:60]}...' "
            f"(countries={requested_countries[:3]})"
        )
        return empty

    out = {
        "source": "BTI",
        "type": "transformation_index",
        "results": results,
    }

    logger.info(
        f"bti: {len(results)} Country-Result(s) für countries={requested_countries[:3]} "
        f"(primaries={primaries})"
    )

    # 24h-Cache füllen.
    cache.put("BTI", analysis, out)
    return out
