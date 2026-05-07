"""UN Comtrade Live-Connector — Welthandels-Statistik Lookup.

UN Comtrade ist die größte öffentliche Welthandels-Datenbank — jährliche
und monatliche Imports/Exports nach Land × Warenklasse seit 1962.
Komplementär zu existierenden Quellen:
- World Bank: Trade-Aggregate als Anteil des BIP (NE.TRD.GNFS.ZS),
  aber KEINE bilateralen Flüsse / Warenklassen.
- Eurostat (Comext): EU-Außenhandel, aber nicht-EU-Reporter unvollständig.
- Static-First-Packs: kuratierte Konsens-Daten zu konkreten Themen.
- UN Comtrade: bilaterale Trade-Flows (Reporter × Partner × HS-Chapter)
  weltweit, frei abrufbar.

API: https://comtradeapi.un.org/data/v1/get/{typeCode}/{freqCode}/{clCode}
- typeCode = C (Commodity Goods) | S (Services)
- freqCode = A (Annual) | M (Monthly)
- clCode   = HS (Harmonized System) | SITC | BEC

Reporter/Partner: M49 country codes (AT=040, DE=276, USA=842, ...).
HS-Codes: 2-stellig (Top-Level Chapter) für Aggregat-Antworten.

AUTH:
- Optional env-var COMTRADE_API_KEY (höhere Rate-Limits + Monthly/sub-
  Chapter-Granularität). Free-Tier (~100 calls/h) braucht KEINE
  Registrierung für Annual + Top-Level-HS, was wir hier abfragen.

Trigger: Claim hat Trade-Keyword (exportiert / importiert / handel /
zoll / lieferung / lieferkette / abnehmer / markt) UND ≥1 Land-
Erwähnung. ODER `analysis.entities` enthält 2 Länder (auch ohne
explizites Trade-Verb — bilaterale Konstellation reicht).

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — Live-Quelle, keine kuratierte Konsens-DB).

Limitations (im display_value-Synthesizer-Kontext zu erwähnen):
- Daten ~6-12 Monate hinterher (Annual-Reports kommen Mitte des
  Folge-Jahrs, daher Default-Year = aktuelles Jahr - 1).
- Reporter-self-deklariert — Mirror-Discrepancy zwischen Export-
  Meldung des Reporters und Import-Meldung des Partners üblich
  (typisch 5-15 % Abweichung wegen CIF/FOB + Transit-Routing).
- HS-Klassifikation revidiert ~alle 5 Jahre (HS-2017, HS-2022) —
  Zeitreihen über Klassifikations-Wechsel hinweg vorsichtig deuten.
- Service-Trade weniger granular als Goods-Trade.
- Free-Tier rate-limited (~100 calls/h) ohne Key.
"""

from __future__ import annotations

import logging
import os

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# Comtrade v1 API — Annual + Commodity (Goods) + HS-Klassifikation
COMTRADE_API = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

# Default: aktuelles Jahr - 1 (Annual-Daten kommen Mitte des Folge-Jahrs)
# Beim Build-Zeitpunkt Mai 2026 ist 2024 verfügbar, 2023 stabil.
DEFAULT_YEAR = "2024"
FALLBACK_YEAR = "2023"  # Falls 2024 noch nicht überall vollständig
TIMEOUT_S = 30.0
MAX_RECORDS_PER_CALL = 500  # weit unter 50.000 Free-Tier-Limit

# M49-Country-Codes (Standard ISO/UN-Numerische Codes)
COUNTRY_M49: dict[str, str] = {
    "AUT": "040", "DEU": "276", "CHE": "756", "USA": "842",
    "CHN": "156", "RUS": "643", "GBR": "826", "FRA": "251",
    "ITA": "381", "ESP": "724", "POL": "616", "TUR": "792",
    "JPN": "392", "IND": "699", "BRA": "076", "KOR": "410",
    "NLD": "528", "BEL": "056", "CZE": "203", "HUN": "348",
    "SWE": "752", "DNK": "208", "NOR": "578", "FIN": "246",
    "PRT": "620", "GRC": "300", "IRL": "372", "ROU": "642",
    "BGR": "100", "HRV": "191", "SVN": "705", "SVK": "703",
    "EST": "233", "LVA": "428", "LTU": "440", "LUX": "442",
    "UKR": "804", "BLR": "112", "SRB": "688", "MEX": "484",
    "CAN": "124", "AUS": "036", "ZAF": "710", "EGY": "818",
    "NGA": "566", "IDN": "360", "VNM": "704", "THA": "764",
    "MYS": "458", "PHL": "608", "SGP": "702", "ARG": "032",
    # World aggregate (für Welt-Total-Vergleiche)
    "WLD": "0",
}

# Aliases (DE + EN) — Mapping zu ISO-3-Codes (dann via COUNTRY_M49 zu M49)
COUNTRY_ALIASES: dict[str, list[str]] = {
    "AUT": ["österreich", "austria"],
    "DEU": ["deutschland", "germany"],
    "CHE": ["schweiz", "switzerland"],
    "USA": ["usa", "vereinigte staaten", "united states", "amerika"],
    "CHN": ["china"],
    "RUS": ["russland", "russia"],
    "GBR": ["großbritannien", "united kingdom", "vereinigtes königreich", "uk"],
    "FRA": ["frankreich", "france"],
    "ITA": ["italien", "italy"],
    "ESP": ["spanien", "spain"],
    "POL": ["polen", "poland"],
    "TUR": ["türkei", "turkey", "türkiye"],
    "JPN": ["japan"],
    "IND": ["indien", "india"],
    "BRA": ["brasilien", "brazil"],
    "KOR": ["südkorea", "south korea", "korea"],
    "NLD": ["niederlande", "netherlands", "holland"],
    "BEL": ["belgien", "belgium"],
    "CZE": ["tschechien", "czech", "czechia"],
    "HUN": ["ungarn", "hungary"],
    "SWE": ["schweden", "sweden"],
    "DNK": ["dänemark", "denmark"],
    "NOR": ["norwegen", "norway"],
    "FIN": ["finnland", "finland"],
    "PRT": ["portugal"],
    "GRC": ["griechenland", "greece"],
    "IRL": ["irland", "ireland"],
    "ROU": ["rumänien", "romania"],
    "BGR": ["bulgarien", "bulgaria"],
    "HRV": ["kroatien", "croatia"],
    "SVN": ["slowenien", "slovenia"],
    "SVK": ["slowakei", "slovakia"],
    "EST": ["estland", "estonia"],
    "LVA": ["lettland", "latvia"],
    "LTU": ["litauen", "lithuania"],
    "LUX": ["luxemburg", "luxembourg"],
    "UKR": ["ukraine"],
    "BLR": ["belarus", "weißrussland"],
    "SRB": ["serbien", "serbia"],
    "MEX": ["mexiko", "mexico"],
    "CAN": ["kanada", "canada"],
    "AUS": ["australien", "australia"],
    "ZAF": ["südafrika", "south africa"],
    "EGY": ["ägypten", "egypt"],
    "NGA": ["nigeria"],
    "IDN": ["indonesien", "indonesia"],
    "VNM": ["vietnam"],
    "THA": ["thailand"],
    "MYS": ["malaysia"],
    "PHL": ["philippinen", "philippines"],
    "SGP": ["singapur", "singapore"],
    "ARG": ["argentinien", "argentina"],
}

# HS-Top-Level-Chapter-Mapping über keyword-Erkennung im Claim
# Format: HS-Code (2-stellig) → Liste DE+EN-Keywords
HS_KEYWORDS: dict[str, list[str]] = {
    "27": [
        "öl", "gas", "erdgas", "erdöl", "energie",
        "mineral", "petroleum", "oil", "fuel",
        "kohle", "coal", "rohstoff",
    ],
    "84": [
        "maschinen", "machinery", "machines", "anlagen",
        "industrieanlagen",
    ],
    "85": [
        "elektro", "electrical", "elektronik", "electronics",
        "halbleiter", "chips", "semiconductor",
    ],
    "87": [
        "auto", "fahrzeug", "fahrzeuge", "kfz", "vehicle",
        "vehicles", "car", "cars", "automobile", "lkw",
    ],
    "61": [
        "kleidung", "textil", "textilien", "clothing",
        "apparel", "garment",
    ],
    "10": [
        "getreide", "weizen", "grain", "wheat", "korn",
    ],
    "72": [
        "stahl", "eisen", "steel", "iron",
    ],
    "29": [
        "chemie", "chemicals", "chemikalien", "pharma",
        "pharmaceutical", "arzneimittel",
    ],
    "30": [
        "medikament", "medikamente", "pharmaceuticals",
        "drug", "drugs",
    ],
    "71": [
        "gold", "edelmetall", "schmuck", "diamant",
        "precious", "jewellery",
    ],
}

# Lesbare HS-Chapter-Namen für display_value
HS_CHAPTER_NAMES: dict[str, str] = {
    "27": "Mineral fuels (HS-27)",
    "84": "Machinery (HS-84)",
    "85": "Electrical equipment (HS-85)",
    "87": "Vehicles (HS-87)",
    "61": "Clothing knit (HS-61)",
    "62": "Clothing not-knit (HS-62)",
    "10": "Cereals (HS-10)",
    "72": "Iron & steel (HS-72)",
    "29": "Organic chemicals (HS-29)",
    "30": "Pharmaceuticals (HS-30)",
    "71": "Precious metals (HS-71)",
    "TOTAL": "All commodities (Total trade)",
}

# Trade-Verben/-Substantive — Trigger-Lexikon
TRADE_KEYWORDS: tuple[str, ...] = (
    "exportiert", "exportier", "export", "exports", "ausfuhr",
    "importiert", "importier", "import", "imports", "einfuhr",
    "handel", "trade", "trades", "trading",
    "zoll", "tariff", "tariffs",
    "lieferung", "lieferungen", "lieferkette", "lieferketten",
    "supply chain", "supply-chain",
    "abnehmer", "abnehmerin", "abnehmerland",
    "absatzmarkt", "exportmarkt", "importmarkt",
    "warenverkehr", "warenstrom", "handelsbilanz",
)

# Wenn 1 Land + spezifische Commodity (kein World-Total) — auch triggern
SPECIFIC_COMMODITY_TRIGGERS: set[str] = {
    "27", "87", "85", "72", "84", "71", "30",
}


# -------- Detection helpers --------

def _detect_countries_in_claim(claim: str) -> list[str]:
    """Find ISO-3-Codes mentioned in claim (longest-first matching).

    Returns up to 3 ISO-3-Codes in order of first occurrence.
    """
    if not claim:
        return []
    text = claim.lower()
    found: list[tuple[int, str]] = []
    seen: set[str] = set()
    for iso3, aliases in COUNTRY_ALIASES.items():
        # Sort aliases longest-first so "vereinigte staaten" wins über "staaten"
        for alias in sorted(aliases, key=len, reverse=True):
            idx = text.find(alias)
            if idx >= 0 and iso3 not in seen:
                found.append((idx, iso3))
                seen.add(iso3)
                break
    found.sort(key=lambda t: t[0])
    return [iso for _, iso in found[:3]]


def _detect_commodity_in_claim(claim: str) -> str | None:
    """Map first matching keyword to HS-Chapter-Code.

    Returns HS-Code (e.g. "27") or None if no specific commodity mentioned.
    """
    if not claim:
        return None
    text = claim.lower()
    # Sort all keywords longest-first to avoid "öl" winning over "erdöl"
    candidates: list[tuple[int, str, str]] = []
    for hs_code, keywords in HS_KEYWORDS.items():
        for kw in keywords:
            idx = text.find(kw)
            if idx >= 0:
                candidates.append((idx, len(kw), hs_code))
    if not candidates:
        return None
    # Earliest position wins; ties → longest match
    candidates.sort(key=lambda t: (t[0], -t[1]))
    return candidates[0][2]


def claim_mentions_trade(claim: str, analysis: dict) -> bool:
    """Trigger-Check: Trade-Keyword + ≥1 Country, ODER 2 Countries.

    Returns True wenn Connector laufen soll.
    """
    text_lc = (claim or "").lower()
    has_trade_kw = any(kw in text_lc for kw in TRADE_KEYWORDS)

    countries = _detect_countries_in_claim(claim or "")
    n_countries = len(countries)

    if has_trade_kw and n_countries >= 1:
        return True
    if n_countries >= 2:
        return True

    # Fallback: NER-Country-Liste aus claim-Analyzer kann mehr finden
    ner_countries = (analysis or {}).get("ner_entities", {}).get("countries", []) or []
    if has_trade_kw and ner_countries:
        return True
    if len(ner_countries) >= 2:
        return True

    return False


# -------- Query construction --------

def _api_headers() -> dict[str, str]:
    """Optional API-Key aus env-var COMTRADE_API_KEY für höhere Limits."""
    headers: dict[str, str] = {}
    api_key = os.getenv("COMTRADE_API_KEY", "").strip()
    if api_key:
        # Comtrade v1 erwartet den Key im Header `Ocp-Apim-Subscription-Key`
        headers["Ocp-Apim-Subscription-Key"] = api_key
    return headers


def _format_usd(value: float | int | None) -> str:
    """Format raw USD-cif/fob value as Mrd / Mio USD-string."""
    if value is None:
        return "k. A."
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "k. A."
    if abs(v) >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f} Mrd USD"
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.1f} Mio USD"
    if abs(v) >= 1_000:
        return f"{v / 1_000:.0f} Tsd USD"
    return f"{v:.0f} USD"


async def _fetch_comtrade(
    client,
    reporter_iso3: str,
    partner_iso3: str | None,
    cmd_code: str,
    flow_code: str,
    period: str,
) -> list[dict]:
    """Issue 1 query against Comtrade v1.

    flow_code: "M" (Imports) | "X" (Exports). Comtrade v1 nutzt M/X.
    Returns list of raw record-dicts (or [] on error / no data).
    """
    reporter_m49 = COUNTRY_M49.get(reporter_iso3)
    if not reporter_m49:
        return []
    partner_m49 = "0"  # Default: World aggregate
    if partner_iso3 and partner_iso3 in COUNTRY_M49:
        partner_m49 = COUNTRY_M49[partner_iso3]

    params: dict[str, str] = {
        "reporterCode": reporter_m49,
        "period": period,
        "partnerCode": partner_m49,
        "cmdCode": cmd_code,
        "flowCode": flow_code,
        "partner2Code": "0",
        "customsCode": "C00",
        "motCode": "0",
        "maxRecords": str(MAX_RECORDS_PER_CALL),
        "format": "JSON",
        "aggregateBy": "",
        "breakdownMode": "classic",
        "includeDesc": "true",
    }
    try:
        resp = await client.get(
            COMTRADE_API,
            params=params,
            headers=_api_headers(),
        )
        if resp.status_code == 401:
            logger.info(
                "UN Comtrade: 401 — Free-Tier-Path möglicherweise restricted; "
                "ggf. COMTRADE_API_KEY setzen."
            )
            return []
        if resp.status_code == 429:
            logger.warning("UN Comtrade: 429 rate-limit — skip diesen Call")
            return []
        if resp.status_code != 200:
            logger.debug(
                f"UN Comtrade: HTTP {resp.status_code} für "
                f"{reporter_iso3}/{partner_iso3}/{cmd_code}/{flow_code}"
            )
            return []
        data = resp.json()
        if not isinstance(data, dict):
            return []
        rows = data.get("data") or []
        if not isinstance(rows, list):
            return []
        return rows
    except Exception as e:
        logger.debug(
            f"UN Comtrade fetch failed "
            f"({reporter_iso3}->{partner_iso3} {cmd_code} {flow_code}): {e}"
        )
        return []


def _pick_total_value(rows: list[dict]) -> float | None:
    """Aggregate primary trade-value (USD) over returned records.

    Comtrade-Response liefert pro Reporter×Partner×Period×Cmd×Flow einen Row;
    bei mehreren Sub-Klassifikationen aggregieren wir auf primaryValue.
    """
    if not rows:
        return None
    total = 0.0
    found = False
    for row in rows:
        v = row.get("primaryValue")
        if v is None:
            continue
        try:
            total += float(v)
            found = True
        except (TypeError, ValueError):
            continue
    return total if found else None


# -------- Main entry-point --------

async def search_uncomtrade(analysis: dict) -> dict:
    """Live-Lookup gegen UN Comtrade für Trade-Claims.

    Strategy:
    1. Trigger-Check (claim_mentions_trade). Wenn nein → empty.
    2. Detect Reporter (1.) + Partner (2.) Land aus Claim.
    3. Detect Commodity (HS-Chapter) ODER Default = TOTAL.
    4. EINE Query: Reporter Exports → Partner. PLUS optional 1 Mirror-
       Query Reporter Imports ← Partner. Max 2 Calls pro Claim
       (API-budget-schonend).
    5. Bei nur 1 Land: Reporter Exports → World (aggregate WLD).
    6. Format display_value mit Mrd-USD-Werten + Limitations-Hinweis.
    """
    empty = {"source": "UN Comtrade", "type": "trade_statistics", "results": []}

    claim = (analysis or {}).get("claim", "") or ""
    if not claim:
        return empty

    if not claim_mentions_trade(claim, analysis or {}):
        return empty

    # Reporter / Partner aus Claim
    countries = _detect_countries_in_claim(claim)
    if not countries:
        # NER-Fallback aus claim_analyzer
        ner = (analysis or {}).get("ner_entities", {}).get("countries", []) or []
        if ner:
            countries = _detect_countries_in_claim(" ".join(ner))
    if not countries:
        return empty

    reporter = countries[0]
    partner = countries[1] if len(countries) >= 2 else None

    # Commodity → HS-Chapter
    cmd_code = _detect_commodity_in_claim(claim)
    if cmd_code is None:
        cmd_code = "TOTAL"
    chapter_name = HS_CHAPTER_NAMES.get(cmd_code, f"HS-{cmd_code}")

    # Period: Default 2024, Fallback 2023 falls leer
    period = DEFAULT_YEAR

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        # Query 1: Reporter Exports → Partner (oder World)
        rows_x = await _fetch_comtrade(
            client,
            reporter_iso3=reporter,
            partner_iso3=partner,
            cmd_code=cmd_code,
            flow_code="X",
            period=period,
        )
        # Fallback-Year falls 2024 leer
        if not rows_x and period != FALLBACK_YEAR:
            period = FALLBACK_YEAR
            rows_x = await _fetch_comtrade(
                client,
                reporter_iso3=reporter,
                partner_iso3=partner,
                cmd_code=cmd_code,
                flow_code="X",
                period=period,
            )

        export_value = _pick_total_value(rows_x)

        # Query 2 (optional): Mirror-Query Reporter Imports ← Partner
        # nur wenn Partner gesetzt (sonst hätten wir Total-Imports vom World,
        # Komplett-Bild bilateral interessanter)
        import_value: float | None = None
        if partner:
            rows_m = await _fetch_comtrade(
                client,
                reporter_iso3=reporter,
                partner_iso3=partner,
                cmd_code=cmd_code,
                flow_code="M",
                period=period,
            )
            import_value = _pick_total_value(rows_m)

    if export_value is None and import_value is None:
        logger.info(
            f"UN Comtrade: 0 Treffer für "
            f"{reporter}->{partner or 'WLD'} {cmd_code} {period}"
        )
        return empty

    # Build display_value
    partner_label = partner if partner else "Welt"
    bilateral_part = ""
    if export_value is not None:
        bilateral_part += (
            f"{reporter} exportierte {period} {chapter_name} im Wert von "
            f"{_format_usd(export_value)} nach {partner_label}"
        )
    if import_value is not None:
        if bilateral_part:
            bilateral_part += "; "
        bilateral_part += (
            f"umgekehrt importierte {reporter} {_format_usd(import_value)} "
            f"aus {partner_label}"
        )
    display = (
        f"{bilateral_part}. "
        f"Quelle: UN Comtrade Annual {period}, HS-Klassifikation. "
        f"Hinweis: Daten sind reporter-self-deklariert; "
        f"Mirror-Discrepancy zwischen Reporter-/Partner-Meldung typisch 5-15 %."
    )[:600]

    indicator_name = (
        f"{reporter}{('→' + partner) if partner else '→Welt'} "
        f"{chapter_name} Trade {period}"
    )

    # comtradeplus.un.org Public-UI für deep-links
    secondary = (
        "https://comtradeapi.un.org/data/v1/get/C/A/HS"
        f"?reporterCode={COUNTRY_M49.get(reporter, '')}"
        f"&period={period}"
        f"&partnerCode={COUNTRY_M49.get(partner, '0') if partner else '0'}"
        f"&cmdCode={cmd_code}&flowCode=X"
    )

    results.append({
        "indicator_name": indicator_name,
        "indicator": "uncomtrade_trade",
        "country": f"{reporter}/{partner}" if partner else reporter,
        "year": period,
        "topic": "uncomtrade_bilateral_flow",
        "display_value": display,
        "description": (
            "Bilaterale Handelsflüsse via UN Comtrade — Export-/Import-"
            "Werte in USD CIF/FOB, Aggregate auf HS-Chapter-Ebene. "
            "Daten ~6-12 Monate hinterher; reporter-self-deklariert mit "
            "Mirror-Discrepancy ~5-15 %; HS-Klassifikation revidiert "
            "alle 5 Jahre (HS-2017, HS-2022)."
        ),
        "url": "https://comtradeplus.un.org/",
        "secondary_url": secondary,
        "source": (
            "UN Comtrade (frei, M49-Country-Codes + HS-Klassifikation, "
            "Annual Goods-Trade)"
        ),
    })

    logger.info(
        f"UN Comtrade: 1 Treffer für {reporter}->{partner or 'WLD'} "
        f"{cmd_code} {period} (export={export_value} import={import_value})"
    )
    return {
        "source": "UN Comtrade",
        "type": "trade_statistics",
        "results": results,
    }
