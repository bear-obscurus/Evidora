"""WID.world — World Inequality Database (Piketty/Saez/Zucman/Chancel).

WID.world (https://wid.world/) ist die weltweit groesste oeffentlich
zugaengliche Datenbank zu Einkommens- und Vermoegens-Ungleichheit.
Distributional National Accounts (DINA) auf Basis von Steuerdaten,
Volkswirtschaftlichen Gesamtrechnungen und Surveys — Methodik von
Piketty, Saez, Zucman, Chancel.

Komplementaer zu existierenden Quellen:
  - worldbank.py: Gini-Index (Survey-basiert, oft 5-10 Jahre alt).
  - oecd_inequality (falls vorhanden): OECD-S80/S20 fuer Mitglieder.
  - WID: Top-Shares (Top-1 %, Top-10 %, Bottom-50 %) inkl. Wealth,
    inkl. Schwellenlaender, mit konsistenten DINA-Definitionen.

Strategie: STATIC-FIRST-SNAPSHOT
================================
WID hat eine REST-API (https://wid.world/codes-dictionary/) und ein
R-/Python-Package ``wid``. Fuer Evidora halten wir die Top-Shares
fuer 30+ Schluessellaender als kuratierten Snapshot in
``data/wid.json`` vor (Data Year 2022, Snapshot 2024). Vorteile:
  - Keine Live-API-Latenz (WID-Snapshots aendern sich ~jaehrlich)
  - Konsistente Werte, die manuell gegen den World Inequality
    Report 2022 + WID-Country-Pages validiert wurden
  - 24h-Hot-Reload via _static_cache.load_json_mtime_aware

Refresh-Workflow (manuell, ~jaehrlich):
  1. WID Update-Welle abwarten (i. d. R. Q1)
  2. Pro Land Top-1 %, Top-10 %, Bottom-50 %-Income- und
     Wealth-Shares aus WID.world pruefen
  3. ``data/wid.json`` patchen + ``data_year``/``fetched_at_iso`` updaten

Trigger:
  - Direkt-Trigger: "WID.world", "Piketty-Daten", "Saez-Zucman"
  - Composite: Top-1 %/Top-10 %/Bottom-50 % + Einkommen/Vermoegen
  - Composite: Einkommens-/Vermoegens-Ungleichheit + Land-Alias

GUARDRAILS (project_political_guardrails.md):
  - Wir zitieren WID-Top-Shares, wir bewerten sie nicht.
  - Description erwaehnt Methodik (DINA, Steuerdaten + SNA) + Caveat.
  - Keine eigene Bewertung "ungerecht/gerecht".
  - Politik-Tabu-Guard 2.0: Partei + Korruption + Superlativ blocken.

Lizenz: Open Access mit Zitation. Chancel/Piketty/Saez/Zucman (2022),
World Inequality Report 2022, WID.world.

Wiring (NICHT in dieser Datei — main.py manuell verdrahten):
  # from services.wid import search_wid, claim_mentions_wid_cached
  # if claim_mentions_wid_cached(claim):
  #     tasks.append(cached("WID.world", search_wid, analysis))
  #     queried_names.append("WID.world (Piketty/Saez/Zucman)")
  #
  # reranker.py: "wid_" Prefix in INDICATOR_WHITELIST_PREFIXES.
  # data_updater.py: KEIN Prefetch (Static-First-Snapshot, kein Live-Call).
"""

from __future__ import annotations

import logging
import os
import time

from services._static_cache import load_json_mtime_aware

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "wid.json",
)

# 24h cache TTL — WID-Snapshots aendern sich nur jaehrlich.
CACHE_TTL_S = 24 * 3600

# Default-Laender (AT-Bias + DACH), wenn Claim WID-Term aber kein Land nennt.
_DEFAULT_COUNTRIES = ("AUT", "DEU", "CHE")

# Maximum primaere Laender im Result-Set (max 3 Treffer pro Claim).
MAX_PRIMARY_COUNTRIES = 3

# Referenz-Laender fuer Vergleichs-Anker im display_value (AT-Bias).
_DISPLAY_REFERENCE_COUNTRIES = (
    "AUT", "DEU", "USA", "FRA", "GBR", "CHN", "IND", "BRA", "ZAF",
)
MAX_COUNTRIES_IN_DISPLAY = 4

# ISO3 -> ISO2 (kompaktes display_value-Format).
_ISO3_TO_ISO2 = {
    "AUT": "AT", "DEU": "DE", "CHE": "CH", "FRA": "FR", "ITA": "IT",
    "ESP": "ES", "GBR": "UK", "USA": "US", "CAN": "CA", "SWE": "SE",
    "NOR": "NO", "DNK": "DK", "FIN": "FI", "NLD": "NL", "BEL": "BE",
    "IRL": "IE", "POL": "PL", "CZE": "CZ", "HUN": "HU", "ROU": "RO",
    "PRT": "PT", "GRC": "GR", "RUS": "RU", "CHN": "CN", "IND": "IN",
    "BRA": "BR", "JPN": "JP", "KOR": "KR", "AUS": "AU", "ZAF": "ZA",
    "MEX": "MX", "TUR": "TR", "EUR": "EU", "WLD": "WLD",
}

# ---------------------------------------------------------------------------
# Trigger-Keywords
# ---------------------------------------------------------------------------

# Direkt-Trigger: Claim erwaehnt WID/Piketty namentlich.
_DIRECT_TRIGGERS = (
    "wid.world", "wid world", "world inequality database",
    "world inequality report", "weltungleichheits-bericht",
    "weltungleichheitsreport", "piketty-daten", "piketty daten",
    "saez-zucman", "saez zucman", "thomas piketty",
    "lucas chancel", "gabriel zucman", "emmanuel saez",
    "distributional national accounts", "dina",
)

# Top-Share-Trigger (Income oder Wealth — qualifizieren ueber _SHARE_TYPE_HINTS).
_SHARE_TRIGGERS = (
    "top-1%", "top 1%", "top-1 %", "top 1 %",
    "top-1-prozent", "top 1 prozent", "top 1-prozent",
    "top-10%", "top 10%", "top-10 %", "top 10 %",
    "top-10-prozent", "top 10 prozent", "top 10-prozent",
    "obere 1%", "obere 1 %", "obersten 1%", "obersten 1 %",
    "obere 10%", "obere 10 %", "obersten 10%", "obersten 10 %",
    "unteren 50%", "unteren 50 %", "untere 50%", "untere 50 %",
    "bottom-50%", "bottom 50%", "bottom 50 %", "bottom-50 %",
    "ärmste hälfte", "aermste haelfte", "untere hälfte", "untere haelfte",
)

# Income/Wealth-Inequality-Trigger (allgemein).
_INEQUALITY_TRIGGERS = (
    "vermögensverteilung", "vermoegensverteilung",
    "vermögensungleichheit", "vermoegensungleichheit",
    "einkommensverteilung", "einkommensungleichheit",
    "income inequality", "wealth inequality",
    "vermögenskonzentration", "vermoegenskonzentration",
    "einkommenskonzentration",
    "reichtum verteilung", "reichtumsverteilung",
    "ungleichverteilung einkommen", "ungleichverteilung vermögen",
    "ungleichverteilung vermoegen",
)

# Hinweise auf Share-Typ (Income vs Wealth) im Claim.
_INCOME_HINTS = (
    "einkommen", "einkommens", "income", "lohn", "gehalt", "verdienst",
    "earnings", "pretax", "vorsteuerlich", "national income",
)
_WEALTH_HINTS = (
    "vermögen", "vermoegen", "wealth", "reichtum", "besitz",
    "net worth", "personal wealth", "household wealth",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_data() -> dict | None:
    """Lade WID-JSON mit Hot-Reload-Awareness."""
    return load_json_mtime_aware(STATIC_JSON_PATH)


def _country_aliases(data: dict) -> dict[str, list[str]]:
    return data.get("country_aliases") or {}


def _detect_countries_in_claim(claim_lc: str, data: dict) -> list[str]:
    """Erkenne ISO3-Country-Codes aus Aliasen im Claim (laengster Alias zuerst).

    Heuristik gegen WID-Brand-Name-Kollisionen: Wenn der einzige WLD-/EUR-Hit
    aus dem Wort "world" / "europe" innerhalb von "wid.world" / "world
    inequality" / "european inequality" stammt, ignorieren — der Begriff
    referenziert die Datenbank, nicht das Aggregat-Gebiet.
    """
    # Sanitize: brand-collisions ausblenden, damit "WID.world" nicht WLD triggert.
    sanitized = claim_lc
    for noise in (
        "wid.world", "wid world", "world inequality database",
        "world inequality report", "world inequality lab",
    ):
        sanitized = sanitized.replace(noise, " ")

    aliases = _country_aliases(data)
    found: list[str] = []
    # Iterate by longest alias first, damit "südkorea" nicht zu "korea" entartet.
    flat: list[tuple[str, str]] = []
    for iso3, alist in aliases.items():
        for a in alist:
            flat.append((a.lower(), iso3))
    flat.sort(key=lambda x: len(x[0]), reverse=True)
    for alias, iso3 in flat:
        if alias in sanitized and iso3 not in found:
            found.append(iso3)
    return found


def _has_direct_trigger(claim_lc: str) -> bool:
    return any(t in claim_lc for t in _DIRECT_TRIGGERS)


def _has_share_trigger(claim_lc: str) -> bool:
    return any(t in claim_lc for t in _SHARE_TRIGGERS)


def _has_inequality_trigger(claim_lc: str) -> bool:
    return any(t in claim_lc for t in _INEQUALITY_TRIGGERS)


def _share_types(claim_lc: str) -> list[str]:
    """Wahle Share-Typen (income / wealth) basierend auf Claim-Hinweisen."""
    has_income = any(t in claim_lc for t in _INCOME_HINTS)
    has_wealth = any(t in claim_lc for t in _WEALTH_HINTS)
    if has_income and not has_wealth:
        return ["income"]
    if has_wealth and not has_income:
        return ["wealth"]
    # Default: beide melden (Income zuerst, Wealth danach).
    return ["income", "wealth"]


def _share_buckets(claim_lc: str) -> list[str]:
    """Wahle Buckets (top1 / top10 / bottom50) basierend auf Claim-Hinweisen."""
    buckets: list[str] = []
    if any(t in claim_lc for t in (
        "top-1%", "top 1%", "top-1 %", "top 1 %",
        "top-1-prozent", "top 1 prozent", "obere 1", "obersten 1",
    )):
        buckets.append("top1")
    if any(t in claim_lc for t in (
        "top-10%", "top 10%", "top-10 %", "top 10 %",
        "top-10-prozent", "top 10 prozent", "obere 10", "obersten 10",
    )):
        buckets.append("top10")
    if any(t in claim_lc for t in (
        "bottom-50%", "bottom 50%", "bottom 50 %", "bottom-50 %",
        "unteren 50%", "unteren 50 %", "untere 50%", "untere 50 %",
        "untere hälfte", "untere haelfte",
        "ärmste hälfte", "aermste haelfte",
    )):
        buckets.append("bottom50")
    if not buckets:
        # Default: top1 + top10 (haeufigste Such-Anfragen).
        buckets = ["top1", "top10"]
    return buckets


def _de_num(v: float | None, decimals: int = 1) -> str:
    if v is None:
        return "k. A."
    return f"{v:.{decimals}f}".replace(".", ",")


# ---------------------------------------------------------------------------
# Trigger-API
# ---------------------------------------------------------------------------
def _claim_mentions_wid(claim_lc: str) -> bool:
    """Pure-String-Trigger-Test gegen die WID-Themenkeywords.

    Logik:
      1. Direkt-Trigger (WID/Piketty namentlich) -> True
      2. Top-Share-Trigger + Income/Wealth-Hint -> True
      3. Inequality-Trigger + Land oder Income/Wealth-Hint -> True
      4. Politik-Tabu-Guard 2.0: Partei+Korruption+Superlativ -> False
    """
    if not claim_lc:
        return False

    # Politik-Tabu-Guard 2.0
    try:
        from services._topic_match import is_party_corruption_superlative_claim
        if is_party_corruption_superlative_claim(claim_lc):
            return False
    except Exception:  # noqa: BLE001
        pass

    # 1) Direkt
    if _has_direct_trigger(claim_lc):
        return True

    has_share = _has_share_trigger(claim_lc)
    has_ineq = _has_inequality_trigger(claim_lc)
    has_income = any(t in claim_lc for t in _INCOME_HINTS)
    has_wealth = any(t in claim_lc for t in _WEALTH_HINTS)

    # 2) Top-Share + Income/Wealth-Hint -> WID
    if has_share and (has_income or has_wealth):
        return True

    # 3) Inequality-Term + Income/Wealth-Hint -> WID
    if has_ineq and (has_income or has_wealth):
        return True

    # 4) Inequality-Term + Land-Alias -> WID (z. B. "Einkommensungleichheit USA")
    if has_ineq:
        data = _load_data()
        if data:
            countries = _detect_countries_in_claim(claim_lc, data)
            if countries:
                return True

    return False


# Trigger-Cache (claim_lc -> (ts, result))
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_wid_cached(claim: str) -> bool:
    """24h-Cache-Wrapper fuer den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_wid(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    # Cache-Hygiene
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Display-Builder
# ---------------------------------------------------------------------------
_BUCKET_LABEL_DE = {
    "top1": "Top-1 %",
    "top10": "Top-10 %",
    "bottom50": "Bottom-50 %",
}

_SHARE_TYPE_LABEL_DE = {
    "income": "Einkommen (pretax national income)",
    "wealth": "Netto-Vermoegen (personal wealth)",
}


def _share_key(share_type: str, bucket: str) -> str:
    """Map (income/wealth, top1/top10/bottom50) -> JSON-Key."""
    return f"{share_type}_{bucket}"


def _country_value(
    shares: dict, iso3: str, share_type: str, bucket: str,
) -> float | None:
    entry = shares.get(iso3)
    if not entry:
        return None
    return entry.get(_share_key(share_type, bucket))


def _format_country_value(
    shares: dict, iso3: str, share_type: str, bucket: str,
) -> str:
    """Hilfs-Format: 'AT 10,8 %'."""
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    val = _country_value(shares, iso3, share_type, bucket)
    if val is None:
        return ""
    return f"{iso2} {_de_num(val)} %"


def _select_display_countries(
    requested: list[str], shares: dict, primary: str,
) -> list[str]:
    selected: list[str] = []
    for c in requested:
        if c == primary or c not in shares:
            continue
        if c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    for c in _DISPLAY_REFERENCE_COUNTRIES:
        if c == primary or c not in shares:
            continue
        if c not in selected:
            selected.append(c)
        if len(selected) >= MAX_COUNTRIES_IN_DISPLAY:
            return selected
    return selected


def _build_display_value(
    iso3: str,
    share_type: str,
    bucket: str,
    value: float,
    display_countries: list[str],
    shares: dict,
    data_year: int,
) -> str:
    """Bsp: 'AT Top-1 % Einkommen 2022: 10,8 % — vs. DE 12,8, USA 19,0, ...
    (WID.world DINA)'.
    """
    iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
    bucket_de = _BUCKET_LABEL_DE.get(bucket, bucket)
    type_de = "Einkommen" if share_type == "income" else "Vermoegen"

    head = (
        f"{iso2} {bucket_de} {type_de} {data_year}: {_de_num(value)} %"
    )

    parts: list[str] = []
    for ref in display_countries:
        formatted = _format_country_value(shares, ref, share_type, bucket)
        if formatted:
            parts.append(formatted)

    ref_str = ""
    if parts:
        ref_str = " — vs. " + ", ".join(parts)

    return f"{head}{ref_str} (WID.world DINA)"


def _build_description(
    share_type: str, bucket: str, country_name: str, data_year: int,
) -> str:
    bucket_de = _BUCKET_LABEL_DE.get(bucket, bucket)
    type_de = _SHARE_TYPE_LABEL_DE.get(share_type, share_type)
    return (
        f"WID.world: Anteil des {bucket_de}-Perzentils am {type_de} in "
        f"{country_name} ({data_year}). Distributional National Accounts "
        f"(DINA) — Methodik kombiniert Steuerdaten, Volkswirtschaftliche "
        f"Gesamtrechnungen und Surveys. Erwachsene Bevoelkerung (20+). "
        f"Pretax-Einkommen vor Sozialtransfers, nach Renten. "
        f"Schwellenwert-Bandbreite typisch +/- 1 Prozentpunkt. Werte "
        f"sind WID-Best-Estimates, keine politische Bewertung."
    )


def _country_url(iso3: str, data: dict) -> str:
    slugs = data.get("country_slugs") or {}
    slug = slugs.get(iso3) or iso3.lower()
    return f"https://wid.world/country/{slug}/"


def _country_display_name(iso3: str, data: dict) -> str:
    aliases = _country_aliases(data).get(iso3) or []
    if not aliases:
        return iso3
    # Bevorzugte deutsche/englische Vollnamen
    preferred = {
        "österreich", "deutschland", "schweiz", "frankreich", "italien",
        "spanien", "vereinigtes königreich", "vereinigte staaten",
        "kanada", "schweden", "norwegen", "dänemark", "finnland",
        "niederlande", "belgien", "irland", "polen", "tschechien",
        "ungarn", "rumänien", "portugal", "griechenland", "russland",
        "china", "indien", "brasilien", "japan", "südkorea",
        "australien", "südafrika", "mexiko", "türkei", "europa", "welt",
    }
    for a in aliases:
        if a in preferred:
            return a.title()
    return aliases[0].title()


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_wid(analysis: dict) -> dict:
    """Static-First-Lookup gegen den WID.world-Snapshot.

    Returns: {"source": "WID.world", "type": "inequality_data", "results": [...]}.
    """
    empty = {"source": "WID.world", "type": "inequality_data", "results": []}

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

    claim_lc = claim.lower()
    original = (analysis.get("original_claim") or claim).lower()
    matchable = f"{original} {claim_lc}".strip()

    if not _claim_mentions_wid(matchable):
        return empty

    data = _load_data()
    if not data:
        logger.warning("wid: static JSON konnte nicht geladen werden")
        return empty

    shares = data.get("shares") or {}
    if not shares:
        return empty

    source_label = data.get(
        "source_label",
        "WID.world — World Inequality Database 2024 (Piketty/Saez/Zucman/Chancel)",
    )
    secondary_url = data.get(
        "secondary_url", "https://wir2022.wid.world/"
    )
    data_year = int(data.get("data_year", 2022))

    # Country-Detection: Claim + Entities + NER-Countries
    requested = _detect_countries_in_claim(matchable, data)
    ner_countries = (analysis.get("ner_entities") or {}).get("countries") or []
    if ner_countries:
        ner_lc = " ".join(str(c).lower() for c in ner_countries)
        for c in _detect_countries_in_claim(ner_lc, data):
            if c not in requested:
                requested.append(c)
    entities = analysis.get("entities") or []
    if entities:
        ent_lc = " ".join(str(e).lower() for e in entities)
        for c in _detect_countries_in_claim(ent_lc, data):
            if c not in requested:
                requested.append(c)

    # Wenn kein Land erkannt: DACH-Default.
    if not requested:
        requested = list(_DEFAULT_COUNTRIES)

    # Welche Share-Typen + Buckets fragt der Claim?
    share_types = _share_types(matchable)
    buckets = _share_buckets(matchable)

    # Begrenze auf max. MAX_PRIMARY_COUNTRIES.
    primary_countries = [c for c in requested if c in shares][:MAX_PRIMARY_COUNTRIES]
    if not primary_countries:
        return empty

    results: list[dict] = []

    # Pro (Land x Share-Typ x Bucket) ein Result — aber gesamt max 3.
    # Reihenfolge: Land in Claim-Reihenfolge, Share-Typ Income vor Wealth,
    # Bucket top1 vor top10 vor bottom50 (auffaelligste Ungleichheits-Kennzahl).
    bucket_priority = {"top1": 0, "top10": 1, "bottom50": 2}
    type_priority = {"income": 0, "wealth": 1}

    combinations: list[tuple[str, str, str]] = []
    for iso3 in primary_countries:
        for share_type in share_types:
            for bucket in buckets:
                combinations.append((iso3, share_type, bucket))
    # Stable Sort: erst Land-Order, dann Bucket-Prio (top1 > top10 > bottom50),
    # dann Type-Prio (income vor wealth).
    combinations.sort(
        key=lambda c: (
            primary_countries.index(c[0]),
            bucket_priority.get(c[2], 99),
            type_priority.get(c[1], 99),
        )
    )

    seen: set[tuple[str, str, str]] = set()
    for iso3, share_type, bucket in combinations:
        key = (iso3, share_type, bucket)
        if key in seen:
            continue
        seen.add(key)

        value = _country_value(shares, iso3, share_type, bucket)
        if value is None:
            continue

        country_name = _country_display_name(iso3, data)
        iso2 = _ISO3_TO_ISO2.get(iso3, iso3[:2])
        display_countries = _select_display_countries(
            requested, shares, iso3
        )

        display_value = _build_display_value(
            iso3, share_type, bucket, value, display_countries, shares, data_year,
        )
        description = _build_description(share_type, bucket, country_name, data_year)

        bucket_de = _BUCKET_LABEL_DE.get(bucket, bucket)
        type_de = "Einkommen" if share_type == "income" else "Vermoegen"
        indicator_name = (
            f"WID.world {data_year} — {country_name}: "
            f"{bucket_de}-Anteil {type_de} {_de_num(value)} %"
        )

        results.append({
            "indicator_name": indicator_name[:300],
            "indicator": f"wid_{share_type}_{bucket}_{iso3.lower()}",
            "country": iso2,
            "country_name": country_name,
            "year": str(data_year),
            "topic": "wid_inequality",
            "value": value,
            "display_value": display_value[:480],
            "description": description[:600],
            "url": _country_url(iso3, data),
            "secondary_url": secondary_url,
            "source": source_label,
        })

        if len(results) >= MAX_PRIMARY_COUNTRIES:
            break

    logger.info(
        "wid: %d Treffer fuer countries=%s share_types=%s buckets=%s",
        len(results), primary_countries[:3], share_types, buckets,
    )
    return {
        "source": "WID.world",
        "type": "inequality_data",
        "results": results,
    }
