"""WITS — World Integrated Trade Solution (Live-API).

Komplementär zu services/worldbank.py (Development-Indicators, generelle
Indikatoren wie Handelsanteil am BIP), services/wgi.py (Governance-
Aggregate) und services/uncomtrade.py (bilaterale Trade-Flows in USD).

Was WITS abdeckt, was die anderen NICHT haben:
- MFN-Zolltarife (Most-Favoured-Nation, einfacher + handelsgewichteter
  Durchschnitt) je Reporter × Partner × Produktgruppe × Jahr.
- AHS-Zolltarife (Effectively Applied) — die tatsächlich erhobenen
  Zollsätze inkl. präferentieller Zollnachlässe.
- Sektor-Aufgliederung nach 21 HS-Gruppen (Animal, Vegetable,
  Fuels, Chemicals, Textiles, Machinery, …).
- Datenquelle ist UNCTAD-TRAINS, konsolidiert über die World Bank.

Quelle: WITS — World Bank Group + UNCTAD TRAINS.
URL: https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-tariff/reporter/{iso3}/year/{year}/partner/{wld}/product/{slug}/indicator/{ind}?format=JSON
Lizenz: World-Bank Open Data (CC BY 4.0).

Achtung Cloudflare-Edge: Der ältere SDMX-Pfad
  /API/V1/SDMX/V21/datasource/TRN/...
ist mit 403 blockiert. Der korrekte Pfad nutzt
  /API/V1/SDMX/V21/datasource/tradestats-tariff/...
und liefert JSON wenn `?format=JSON` gesetzt ist (sonst SDMX-ML XML).

Trigger-Themen (Cluster Wirtschaft/Welthandel):
- "Zolltarif <Land>", "Tariff", "MFN", "applied tariff", "AHS"
- "Zollfrei", "duty free", "Präferenz-Zoll", "preferential"
- "Handelsabkommen <X>", "Freihandel", "Trade Agreement"
- "Non-Tariff-Measures", "NTM", "Importschranken", "WITS"

Komplement zu UN Comtrade (services/uncomtrade.py):
- Comtrade → wieviel Handelswert in USD fließt bilateral?
- WITS    → welche Zollsätze gelten dabei (gebunden, angewandt,
            präferentiell), und wieviel davon ist zollfrei?

WIRING (NICHT in dieser Datei einbauen, nur als Hinweis):
  # main.py imports:
  from services.wits import search_wits, claim_mentions_wits_cached
  # main.py inside the source-fan-out (z. B. nach UN Comtrade):
  if claim_mentions_wits_cached(claim):
      tasks.append(cached("WITS", search_wits, analysis))
      queried_names.append("WITS (World Bank Tariffs)")
  # reranker (services/reranker.py) Whitelist:
  #   "WITS" → trade_tariffs (no AUTHORITATIVE_INDICATOR-Boost,
  #            da Live-Quelle, keine kuratierte Konsens-DB).

Limitations (im Synthesizer-Kontext zu erwähnen):
- Daten sind 1–3 Jahre verzögert (UNCTAD/TRAINS-Verarbeitung).
- "Simple Average" = arithmetischer Mittelwert über alle Tariff-Linien,
  "Weighted Average" = nach Handelsvolumen gewichtet — Werte können
  deutlich abweichen (Weighted typisch niedriger, weil große Importmengen
  oft niedrigere Zollsätze haben).
- WITS deckt KEINE non-tariff measures (NTM/SPS/TBT) ab — dafür
  ist UNCTAD TRAINS direkt nötig (separate Quelle, hier nicht abgefragt).
- Beim EU-Binnenmarkt (intra-EU) gibt es seit 1993 keine Zölle mehr —
  AUT vs EU-Partner liefert 0 % zurück; AUT vs WLD nutzt EU-MFN-Satz.
"""

from __future__ import annotations

import logging
import time

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BASE_URL = "https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-tariff"

# 24h Cache — Tarif-Daten ändern sich nur jährlich.
CACHE_TTL = 86400
# Cache-Key = (reporter_iso3, partner_slug, product_slug, year, indicator)
_cache: dict[tuple[str, str, str, str, str], tuple[float, dict | None]] = {}

# Default-Jahr: WITS-Daten sind 1–3 Jahre verzögert; 2022 ist breit gefüllt.
DEFAULT_YEAR = "2022"
FALLBACK_YEAR = "2021"


# ---------------------------------------------------------------------------
# Indicator-Map — die wichtigsten 4 + 3 sind die einzigen aggregat-fähigen.
# ---------------------------------------------------------------------------
# Format: indicator_code → (display_name, description_snippet, keyword-Liste)
WITS_INDICATORS: dict[str, dict] = {
    "MFN-SMPL-AVRG": {
        "name": "MFN Simple Average Tariff (%)",
        "short": "MFN-Simple-Avg",
        "description": (
            "Most-Favoured-Nation-Zollsatz, einfacher Durchschnitt über "
            "alle Tariff-Linien — der Standard-Zollsatz, den der Reporter "
            "auf Importe aus WTO-Mitgliedern ohne präferentielles Abkommen "
            "anwendet."
        ),
        "keywords": (
            "mfn tarif", "mfn-tarif", "mfn tariff", "mfn zoll",
            "meistbegünstigung", "meistbeguenstigung",
            "most favoured nation", "most-favoured-nation",
            "wto-zoll", "wto zoll", "standardzoll", "regelzoll",
        ),
    },
    "MFN-WGHTD-AVRG": {
        "name": "MFN Weighted Average Tariff (%)",
        "short": "MFN-Weighted-Avg",
        "description": (
            "MFN-Zollsatz handelsgewichtet (Importwert-gewichtet). "
            "Aussagekräftiger als der einfache Schnitt, weil Tariff-Linien "
            "mit hohem Handelsvolumen stärker eingehen."
        ),
        "keywords": (
            "gewichteter mfn", "gewichteter zoll", "weighted tariff",
            "weighted average tariff", "handelsgewichtet",
        ),
    },
    "AHS-SMPL-AVRG": {
        "name": "AHS Simple Average Tariff (%)",
        "short": "Applied-Simple-Avg",
        "description": (
            "Effectively Applied Tariff — der tatsächlich erhobene "
            "Zollsatz inkl. präferentieller Reduktionen (z. B. aus FTAs). "
            "Liegt typisch unter dem MFN-Satz."
        ),
        "keywords": (
            "angewandter zoll", "applied tariff", "ahs tarif",
            "ahs tariff", "effectively applied", "effective tariff",
            "tatsächlicher zoll", "tatsaechlicher zoll",
            "präferentieller zoll", "praeferentieller zoll",
            "präferenzzoll", "praeferenzzoll", "preferential tariff",
            "freihandelszoll",
        ),
    },
    "AHS-WGHTD-AVRG": {
        "name": "AHS Weighted Average Tariff (%)",
        "short": "Applied-Weighted-Avg",
        "description": (
            "Effectively Applied Tariff, handelsgewichtet. Spiegelt den "
            "real bezahlten durchschnittlichen Zollsatz wider, inkl. "
            "Präferenzen."
        ),
        "keywords": (
            "gewichteter angewandter zoll", "weighted applied tariff",
            "real bezahlter zoll", "effektiver durchschnittszoll",
        ),
    },
}

# Generelle WITS/Trade-Tariff-Trigger — wenn keine konkrete Indicator-Kw
# matched, geben wir MFN + AHS Simple zurück (beide Kern-Indikatoren).
_GENERAL_TRIGGERS = (
    "wits", "world integrated trade solution",
    "zolltarif", "zolltarife", "zoll-tarif",
    "import-zoll", "importzoll", "importzölle", "importzoelle",
    "einfuhrzoll", "einfuhr-zoll", "einfuhrzölle", "einfuhrzoelle",
    "zollsatz", "zollsätze", "zollsaetze",
    # einzelnes "zoll" / "zölle" (mit Wort-Kontext via Substring; vorsichtiger
    # Boundary unten in claim_mentions_wits_cached über Word-Lookup)
    "zoll ", " zoll", "zölle", "zoelle",
    "tariff ", " tariff", "tariffs",
    "tariff rate", "tariff rates", "tariffs on",
    "customs duty", "customs duties",
    "handelsabkommen", "handels-abkommen",
    "freihandelsabkommen", "freihandels-abkommen",
    "free trade agreement", "free-trade agreement", "fta ",
    "trade agreement",
    "non-tariff measures", "non tariff measures", "ntm",
    "ntb", "non tariff barrier",
    "nicht-tarifär", "nicht tarifär", "nicht-tarifaer",
    "duty free", "zollfrei",
)

# ---------------------------------------------------------------------------
# Country-Map — 20+ EU + Welt-Top, ISO-3-Codes (WITS akzeptiert lowercase)
# ---------------------------------------------------------------------------
COUNTRY_MAP: dict[str, str] = {
    # DACH
    "österreich": "AUT", "oesterreich": "AUT", "austria": "AUT",
    "deutschland": "DEU", "germany": "DEU",
    "schweiz": "CHE", "switzerland": "CHE",
    # EU-27
    "frankreich": "FRA", "france": "FRA",
    "italien": "ITA", "italy": "ITA",
    "spanien": "ESP", "spain": "ESP",
    "niederlande": "NLD", "netherlands": "NLD", "holland": "NLD",
    "belgien": "BEL", "belgium": "BEL",
    "polen": "POL", "poland": "POL",
    "tschechien": "CZE", "czech": "CZE", "czechia": "CZE",
    "ungarn": "HUN", "hungary": "HUN",
    "rumänien": "ROU", "rumaenien": "ROU", "romania": "ROU",
    "bulgarien": "BGR", "bulgaria": "BGR",
    "kroatien": "HRV", "croatia": "HRV",
    "slowenien": "SVN", "slovenia": "SVN",
    "slowakei": "SVK", "slovakia": "SVK",
    "dänemark": "DNK", "daenemark": "DNK", "denmark": "DNK",
    "schweden": "SWE", "sweden": "SWE",
    "finnland": "FIN", "finland": "FIN",
    "portugal": "PRT",
    "griechenland": "GRC", "greece": "GRC",
    "irland": "IRL", "ireland": "IRL",
    "luxemburg": "LUX", "luxembourg": "LUX",
    "estland": "EST", "estonia": "EST",
    "lettland": "LVA", "latvia": "LVA",
    "litauen": "LTU", "lithuania": "LTU",
    "malta": "MLT",
    "zypern": "CYP", "cyprus": "CYP",
    # Erweiterung: nicht-EU EU-Nachbar + Welt-Top
    "norwegen": "NOR", "norway": "NOR",
    "vereinigtes königreich": "GBR", "vereinigtes koenigreich": "GBR",
    "großbritannien": "GBR", "grossbritannien": "GBR",
    "united kingdom": "GBR", "uk": "GBR",
    "türkei": "TUR", "tuerkei": "TUR", "turkey": "TUR", "türkiye": "TUR",
    "ukraine": "UKR",
    "russland": "RUS", "russia": "RUS",
    "usa": "USA", "vereinigte staaten": "USA", "united states": "USA",
    "china": "CHN",
    "indien": "IND", "india": "IND",
    "brasilien": "BRA", "brazil": "BRA",
    "japan": "JPN",
    "kanada": "CAN", "canada": "CAN",
    "südkorea": "KOR", "suedkorea": "KOR", "south korea": "KOR",
    "südafrika": "ZAF", "suedafrika": "ZAF", "south africa": "ZAF",
    "australien": "AUS", "australia": "AUS",
    "mexiko": "MEX", "mexico": "MEX",
    "indonesien": "IDN", "indonesia": "IDN",
    "vietnam": "VNM",
}

# Produkt-Slugs — Mapping Keyword → WITS-Product-Slug.
# WITS akzeptiert kurz "all" für Gesamt-Aggregat sowie verschiedene
# Kurz-Aliases ("Fuels"/"Chemical"/"Textiles"/"Transp"/"manuf"/"Food"/
# "OresMtls"/"AgrRaw"). Diese arbeiten breit über alle Reporter; die
# langen HS-präfixierten Codes ("72-83_Metals" etc.) sind je Reporter/
# Jahr nicht immer befüllt — die Aliases sind robuster.
PRODUCT_MAP: dict[str, list[str]] = {
    # Hinweis: nackte 2-Buchstaben-Substrings wie "öl" / "oil" sind hier
    # absichtlich nicht enthalten — sie matchen sonst innerhalb von
    # "zölle"/"foil"/"boil"/"toilet" usw. Bei Bedarf "erdöl"/"oil import"
    # über _find_product_slug erkennen.
    "Fuels": ["erdgas", "erdöl", "erdoel",
              "energie", "petroleum", "oil import", "oil-import",
              "fuel ", " fuel", "fuels",
              "kohle ", " kohle", "coal", "treibstoff",
              "gas-import", "gas import", "erdgas-import"],
    "Chemical": ["chemie", "chemicals", "chemikalien", "pharma",
                 "pharmaceutical", "arzneimittel", "drug", "drugs",
                 "medikament", "medikamente"],
    "manuf": ["maschinen", "machinery", "machines", "elektro",
              "electrical", "elektronik", "electronics", "halbleiter",
              "chips", "semiconductor", "industrieanlagen",
              "manufacture", "fertigung", "industrieprodukte"],
    "Transp": ["auto", "fahrzeug", "fahrzeuge", "kfz", "vehicle",
               "vehicles", "car", "cars", "automobile", "lkw",
               "transport equipment", "transp"],
    "Textiles": ["kleidung", "textil", "textilien", "clothing",
                 "apparel", "garment", "textile"],
    "OresMtls": ["stahl", "eisen", "steel", "iron", "metals", "metall",
                 "aluminium", "kupfer", "erz "],
    "AgrRaw": ["agrarrohstoff", "agricultural raw", "rohbaumwolle",
               "naturkautschuk"],
    "Food": ["getreide", "weizen", "grain", "wheat", "korn",
             "lebensmittel", "food", "nahrungsmittel",
             "fleisch", "milch", "tierprodukte", "meat", "dairy",
             "obst", "gemüse", "gemuese"],
}

# Default-Partner = Welt-Aggregat (WLD) = MFN-Satz auf alle WTO-Imports
PARTNER_WORLD = "wld"

# Default-Produkt: "all" = Aggregat über alle Sektoren
PRODUCT_ALL = "all"

# Default-Indicators wenn nur Allgemein-Trigger: MFN + AHS Simple-Avg
_DEFAULT_INDICATORS = ("MFN-SMPL-AVRG", "AHS-SMPL-AVRG")

# Default-Land falls Claim einen WITS-Term erwähnt aber kein Land
_DEFAULT_COUNTRY = "AUT"


# ---------------------------------------------------------------------------
# Trigger-Detection
# ---------------------------------------------------------------------------
def _claim_mentions_wits(claim_lc: str) -> bool:
    """Pure-string Trigger gegen WITS-Themen-Keywords."""
    if not claim_lc:
        return False
    if any(t in claim_lc for t in _GENERAL_TRIGGERS):
        return True
    for spec in WITS_INDICATORS.values():
        if any(kw in claim_lc for kw in spec["keywords"]):
            return True
    return False


def claim_mentions_wits_cached(claim: str) -> bool:
    """Public-API: lowercase + Trigger-Test."""
    return _claim_mentions_wits((claim or "").lower())


# ---------------------------------------------------------------------------
# Parameter-Detection aus dem Claim
# ---------------------------------------------------------------------------
def _find_indicators(text_lc: str) -> list[str]:
    """Findet WITS-Tariff-Indikatoren im Claim.

    Erst nach spezifischem Match (z. B. "präferentieller zoll" → AHS-*),
    sonst Default: MFN+AHS-Simple.
    """
    matched: list[str] = []
    for ind_id, spec in WITS_INDICATORS.items():
        if any(kw in text_lc for kw in spec["keywords"]):
            matched.append(ind_id)
    if matched:
        return matched[:2]
    return list(_DEFAULT_INDICATORS)


def _find_country(analysis: dict) -> str:
    """Erstes ISO-3 aus claim / NER. Default AUT."""
    claim = (analysis.get("claim") or "").lower()
    original = (analysis.get("original_claim") or "").lower()
    ner = (analysis.get("ner_entities") or {}).get("countries") or []
    haystack = " ".join([original, claim, *ner]).lower()
    # längste Namen zuerst, damit "südkorea" nicht durch "korea" überschrieben wird
    for name in sorted(COUNTRY_MAP.keys(), key=len, reverse=True):
        if name in haystack:
            return COUNTRY_MAP[name]
    return _DEFAULT_COUNTRY


def _find_product_slug(text_lc: str) -> str:
    """Map first matching keyword to WITS-Product-Slug, else 'all'."""
    if not text_lc:
        return PRODUCT_ALL
    candidates: list[tuple[int, int, str]] = []
    for slug, kws in PRODUCT_MAP.items():
        for kw in kws:
            idx = text_lc.find(kw)
            if idx >= 0:
                candidates.append((idx, -len(kw), slug))
    if not candidates:
        return PRODUCT_ALL
    candidates.sort()
    return candidates[0][2]


# ---------------------------------------------------------------------------
# API-Call
# ---------------------------------------------------------------------------
async def _fetch_indicator(
    client: httpx.AsyncClient,
    reporter: str,
    partner: str,
    product: str,
    year: str,
    indicator: str,
) -> tuple[float | None, str]:
    """Fetch 1 WITS-Indikator. Returns (value, source_url) — value=None bei Fehler.

    Cached 24h. Verwendet das tradestats-tariff-Endpoint mit JSON-Format.
    """
    key = (reporter, partner, product, year, indicator)
    now = time.time()
    hit = _cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL:
        cached_val = hit[1]
        return (cached_val.get("value") if cached_val else None,
                cached_val.get("url") if cached_val else "")

    url = (
        f"{BASE_URL}"
        f"/reporter/{reporter.lower()}"
        f"/year/{year}"
        f"/partner/{partner.lower()}"
        f"/product/{product}"
        f"/indicator/{indicator}"
        f"?format=JSON"
    )
    try:
        resp = await client.get(url)
        if resp.status_code == 403:
            logger.warning("WITS 403 (Cloudflare-Block) for %s/%s/%s/%s/%s",
                           reporter, partner, product, year, indicator)
            _cache[key] = (now, None)
            return (None, url)
        if resp.status_code == 404:
            _cache[key] = (now, None)
            return (None, url)
        if resp.status_code == 429:
            logger.warning("WITS rate-limit 429 for %s", indicator)
            return (None, url)
        if resp.status_code != 200:
            logger.debug("WITS HTTP %s for %s", resp.status_code, url)
            _cache[key] = (now, None)
            return (None, url)
        data = resp.json()
    except httpx.HTTPError as e:
        logger.warning("WITS HTTP error: %s", e)
        return (None, url)
    except Exception as e:  # noqa: BLE001
        logger.warning("WITS fetch failed for %s: %s", url, e)
        return (None, url)

    # SDMX-JSON: dataSets[0].series['0:0:0:0:0'].observations['0'] = [value, attr]
    try:
        series = data.get("dataSets", [{}])[0].get("series", {})
        if not series:
            _cache[key] = (now, None)
            return (None, url)
        first = next(iter(series.values()))
        obs = first.get("observations", {})
        if not obs:
            _cache[key] = (now, None)
            return (None, url)
        first_obs = next(iter(obs.values()))
        if not isinstance(first_obs, list) or not first_obs:
            _cache[key] = (now, None)
            return (None, url)
        val = first_obs[0]
        if val is None:
            _cache[key] = (now, None)
            return (None, url)
        try:
            num = float(val)
        except (TypeError, ValueError):
            _cache[key] = (now, None)
            return (None, url)
        _cache[key] = (now, {"value": num, "url": url})
        return (num, url)
    except Exception as e:  # noqa: BLE001
        logger.debug("WITS parse-fail for %s: %s", url, e)
        _cache[key] = (now, None)
        return (None, url)


# ---------------------------------------------------------------------------
# Format-Helpers
# ---------------------------------------------------------------------------
def _de_pct(v: float) -> str:
    """Deutsche Komma-Formatierung mit 2 Nachkomma + %-Zeichen."""
    return f"{v:.2f}".replace(".", ",") + " %"


def _product_label(slug: str) -> str:
    pretty = {
        "all": "Alle Sektoren",
        "Fuels": "Energieträger (Fuels)",
        "Chemical": "Chemikalien & Pharma (Chemical)",
        "manuf": "Industriegüter / Manufactures",
        "Transp": "Fahrzeuge & Transport (Transp)",
        "Textiles": "Textilien & Bekleidung",
        "OresMtls": "Erze & Metalle (Ores & Metals)",
        "AgrRaw": "Agrarrohstoffe",
        "Food": "Nahrungsmittel",
    }
    return pretty.get(slug, slug)


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_wits(analysis: dict) -> dict:
    """Live-API-Search gegen WITS für Tarif-Themen.

    Strategy:
    1. Trigger-Check. Wenn nein → empty.
    2. Reporter-Land aus Claim/NER (Default AUT).
    3. Indicator-Liste aus Keyword-Match (Default MFN+AHS-Simple).
    4. Produkt-Slug aus Keyword-Match (Default "all").
    5. Bis zu 2 Indicator-Queries gegen WLD (Welt-Aggregat).
    6. Fallback-Jahr bei leerem Resultat.
    """
    empty = {"source": "WITS", "type": "trade_tariffs", "results": []}

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_wits(matchable):
        return empty

    reporter = _find_country(analysis or {})
    indicators = _find_indicators(matchable)
    product_slug = _find_product_slug(matchable)

    year = DEFAULT_YEAR
    results: list[dict] = []

    try:
        async with polite_client(timeout=30.0) as client:
            for ind_id in indicators:
                value, used_url = await _fetch_indicator(
                    client, reporter, PARTNER_WORLD, product_slug, year, ind_id,
                )
                # Fallback-Jahr falls aktuelles Default leer
                if value is None and year != FALLBACK_YEAR:
                    value, used_url = await _fetch_indicator(
                        client, reporter, PARTNER_WORLD, product_slug,
                        FALLBACK_YEAR, ind_id,
                    )
                    used_year = FALLBACK_YEAR if value is not None else year
                else:
                    used_year = year
                if value is None:
                    continue

                spec = WITS_INDICATORS[ind_id]
                # Canonical Country-Names (manuelle Überschreibung für Akronyme)
                _country_display = {
                    "USA": "USA", "GBR": "Vereinigtes Königreich",
                    "AUT": "Österreich", "DEU": "Deutschland",
                    "CHE": "Schweiz", "FRA": "Frankreich",
                    "ITA": "Italien", "ESP": "Spanien",
                    "NLD": "Niederlande", "BEL": "Belgien",
                    "POL": "Polen", "CZE": "Tschechien", "HUN": "Ungarn",
                    "ROU": "Rumänien", "BGR": "Bulgarien", "HRV": "Kroatien",
                    "SVN": "Slowenien", "SVK": "Slowakei", "DNK": "Dänemark",
                    "SWE": "Schweden", "FIN": "Finnland", "PRT": "Portugal",
                    "GRC": "Griechenland", "IRL": "Irland", "LUX": "Luxemburg",
                    "EST": "Estland", "LVA": "Lettland", "LTU": "Litauen",
                    "MLT": "Malta", "CYP": "Zypern", "NOR": "Norwegen",
                    "TUR": "Türkei", "UKR": "Ukraine", "RUS": "Russland",
                    "CHN": "China", "IND": "Indien", "BRA": "Brasilien",
                    "JPN": "Japan", "CAN": "Kanada", "KOR": "Südkorea",
                    "ZAF": "Südafrika", "AUS": "Australien", "MEX": "Mexiko",
                    "IDN": "Indonesien", "VNM": "Vietnam",
                }
                country_name = _country_display.get(reporter, reporter)
                product_label = _product_label(product_slug)

                display_value = (
                    f"{country_name} {spec['short']} {used_year} "
                    f"({product_label}, Partner=Welt): {_de_pct(value)}"
                )

                description = (
                    f"{spec['description']} "
                    f"Quelle: WITS (World Bank / UNCTAD TRAINS) — "
                    f"Reporter {reporter}, Partner Welt-Aggregat (WLD), "
                    f"Sektor {product_label}, Jahr {used_year}. "
                    f"Hinweis: Daten sind 1–3 Jahre verzögert; "
                    f"'Simple' = arithmetischer Mittelwert über alle "
                    f"Tariff-Linien, 'Weighted' = handelsgewichtet "
                    f"(meist niedriger). Intra-EU-Handel ist seit 1993 "
                    f"zollfrei — AUT vs EU-Partner liefert 0 % zurück; "
                    f"AUT vs Welt nutzt den EU-MFN-Satz."
                )

                indicator_name = (
                    f"WITS {spec['short']} — {country_name} "
                    f"{product_label} {used_year}"
                )

                results.append({
                    "indicator_name": indicator_name,
                    "indicator": (
                        f"wits_{ind_id.lower().replace('-', '_')}_"
                        f"{reporter.lower()}_{product_slug.lower()}_{used_year}"
                    ),
                    "country": reporter,
                    "country_name": country_name,
                    "year": used_year,
                    "value": value,
                    "display_value": display_value,
                    "description": description,
                    "url": (
                        "https://wits.worldbank.org/CountryProfile/en/"
                        f"Country/{reporter}/Year/{used_year}/Summary"
                    ),
                    "secondary_url": used_url,
                    "source": (
                        "WITS (World Bank / UNCTAD TRAINS, Tarif-"
                        "Statistiken, Open Data CC BY 4.0)"
                    ),
                })
    except Exception as e:  # noqa: BLE001
        logger.warning("WITS search failed: %s", e)
        return empty

    # Top-3 cap (typisch 1–2)
    results = results[:3]
    logger.info(
        "WITS: %d results for reporter=%s indicators=%s product=%s year=%s",
        len(results), reporter, indicators, product_slug, year,
    )
    return {"source": "WITS", "type": "trade_tariffs", "results": results}
