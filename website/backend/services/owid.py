"""OurWorldInData (OWID) Live-Connector — Indikator-Lookup via Grapher-CSV-API.

OurWorldInData (Oxford Martin School / Global Change Data Lab) ist eine
der renommiertesten wissenschaftlich kuratierten Datenaggregationen
weltweit. Für Faktencheck-Zwecke liefert sie:
- Globale Vergleichszahlen (CO2, Lebenserwartung, Armut, Bildung, etc.)
- Konsistente Zeitreihen aus Primärquellen (UN, Weltbank, WHO, OECD,
  Global Carbon Project, IEA, EIU, etc.)
- DACH-Werte (AT/DE/CH) plus Welt-Aggregat in einer einzigen Abfrage

Komplementär zu existierenden Quellen:
- DESTATIS/Statistik Austria: nationale Detail-Daten
- WHO/RKI: Gesundheitsdaten national
- Wikipedia: enzyklopädische Definitionen
- OWID: globale Vergleichszahlen + Konsens-Aggregat

API: https://ourworldindata.org/grapher/{slug}.csv?country=AUT~DEU~CHE~OWID_WRL
- Returns CSV mit Spalten: Entity, Code, Year, <value-column>
- Free, kein Auth, polite User-Agent empfohlen
- Lizenz: CC-BY 4.0

Trigger: claim-`category` in {health, climate, economy, demographics}
ODER claim-Text matched mind. einen DE/EN-Keyword einer Whitelist-Indikator.

Limitations:
- OWID-Daten sind oft 1-2 Jahre alt (Zeitverzug bei UN/WB-Aggregationen)
- Pro Indikator nur 1 Wert pro Jahr (nicht monatlich/quartalsweise)
- Country-Coverage manchmal lückenhaft (besonders bei Demokratie-/EIU-Daten)

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle, keine kuratierte Konsens-DB).
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
from typing import Any

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

OWID_CSV_URL = (
    "https://ourworldindata.org/grapher/{slug}.csv"
    "?country=AUT~DEU~CHE~OWID_WRL"
)

# Hardcoded Indikator-Whitelist (~35 Einträge, sortiert nach Themen-Cluster).
# Jeder Eintrag: slug + DE/EN-Label + DE/EN-Keywords + Unit + Topic-Bucket.
# Die Keywords werden lower-case gegen Claim-Text + Analyse-Keywords gematcht.
INDICATOR_WHITELIST: list[dict[str, Any]] = [
    # --- Klima / Emissionen ---
    {
        "slug": "co2-emissions-per-capita",
        "label_de": "CO2 pro Kopf",
        "label_en": "CO2 per capita",
        "keywords_de": ["co2 pro kopf", "co2-pro-kopf", "co2 emissionen pro kopf",
                        "pro-kopf-emission", "kohlendioxid pro kopf"],
        "keywords_en": ["co2 per capita", "co2 emissions per capita",
                        "per capita emissions", "carbon per capita"],
        "unit": "t",
        "topic": "owid_climate",
    },
    {
        "slug": "co2-emissions",
        "label_de": "CO2 gesamt",
        "label_en": "CO2 total",
        "keywords_de": ["co2 gesamt", "co2-emissionen", "kohlendioxid-emissionen",
                        "treibhausgas-emissionen"],
        "keywords_en": ["co2 emissions", "carbon emissions", "ghg emissions",
                        "greenhouse gas emissions"],
        "unit": "t",
        "topic": "owid_climate",
    },
    {
        "slug": "annual-co2-emissions-per-country",
        "label_de": "CO2 jährlich pro Land",
        "label_en": "CO2 annual per country",
        "keywords_de": ["jährliche co2-emissionen", "co2 land", "co2 ausstoß"],
        "keywords_en": ["annual co2", "yearly co2 emissions"],
        "unit": "t",
        "topic": "owid_climate",
    },
    {
        "slug": "temperature-anomaly",
        "label_de": "Temperatur-Anomalie",
        "label_en": "Temperature anomaly",
        "keywords_de": ["temperatur-anomalie", "temperaturanomalie",
                        "klimawandel temperatur", "globale erwärmung",
                        "erderwärmung"],
        "keywords_en": ["temperature anomaly", "global warming", "climate warming"],
        "unit": "°C",
        "topic": "owid_climate",
    },
    # --- Energie ---
    {
        "slug": "share-electricity-renewables",
        "label_de": "Erneuerbare-Anteil Strom",
        "label_en": "Renewables share electricity",
        "keywords_de": ["erneuerbare", "ökostrom", "renewables-anteil",
                        "wind solar wasser", "strommix erneuerbar"],
        "keywords_en": ["renewables share", "renewable electricity",
                        "renewable energy share"],
        "unit": "%",
        "topic": "owid_climate",
    },
    {
        "slug": "share-electricity-fossil-fuels",
        "label_de": "Fossil-Anteil Strom",
        "label_en": "Fossil share electricity",
        "keywords_de": ["fossile energie", "fossil-anteil", "kohlestrom",
                        "gaskraftwerk", "öl strom"],
        "keywords_en": ["fossil fuels share", "fossil electricity",
                        "coal gas oil electricity"],
        "unit": "%",
        "topic": "owid_climate",
    },
    {
        "slug": "share-electricity-nuclear",
        "label_de": "Atomstrom-Anteil",
        "label_en": "Nuclear share electricity",
        "keywords_de": ["atomstrom", "kernenergie anteil", "kernkraft anteil",
                        "atomenergie anteil"],
        "keywords_en": ["nuclear share", "nuclear electricity", "nuclear power share"],
        "unit": "%",
        "topic": "owid_climate",
    },
    {
        "slug": "electricity-generation",
        "label_de": "Stromerzeugung gesamt",
        "label_en": "Electricity generation",
        "keywords_de": ["stromerzeugung", "elektrizität produktion",
                        "stromproduktion"],
        "keywords_en": ["electricity generation", "power generation"],
        "unit": "TWh",
        "topic": "owid_climate",
    },
    # --- Gesundheit ---
    {
        "slug": "life-expectancy",
        "label_de": "Lebenserwartung",
        "label_en": "Life expectancy",
        "keywords_de": ["lebenserwartung", "durchschnittliches lebensalter",
                        "wie alt werden"],
        "keywords_en": ["life expectancy", "lifespan", "average life"],
        "unit": "Jahre",
        "topic": "owid_health",
    },
    {
        "slug": "child-mortality",
        "label_de": "Kindersterblichkeit",
        "label_en": "Child mortality",
        "keywords_de": ["kindersterblichkeit", "säuglingssterblichkeit",
                        "kinder unter 5 sterben"],
        "keywords_en": ["child mortality", "infant mortality",
                        "under-5 mortality"],
        "unit": "‰",
        "topic": "owid_health",
    },
    {
        "slug": "maternal-mortality",
        "label_de": "Müttersterblichkeit",
        "label_en": "Maternal mortality",
        "keywords_de": ["müttersterblichkeit", "tod bei geburt",
                        "schwangerschaftstod"],
        "keywords_en": ["maternal mortality", "death childbirth"],
        "unit": "/100k",
        "topic": "owid_health",
    },
    {
        "slug": "vaccination-coverage-of-1-year-olds",
        "label_de": "Impfquote 1-Jährige",
        "label_en": "Vaccination 1-year-olds",
        "keywords_de": ["impfquote", "impfung kinder", "kinderimpfung",
                        "durchimpfungsrate"],
        "keywords_en": ["vaccination coverage", "child vaccination",
                        "immunization rate"],
        "unit": "%",
        "topic": "owid_health",
    },
    {
        "slug": "share-deaths-air-pollution",
        "label_de": "Luftverschmutzung-Tote (Anteil)",
        "label_en": "Air pollution deaths share",
        "keywords_de": ["luftverschmutzung tote", "feinstaub tote",
                        "luftqualität tod"],
        "keywords_en": ["air pollution deaths", "particulate deaths"],
        "unit": "%",
        "topic": "owid_health",
    },
    {
        "slug": "share-of-adults-defined-as-obese",
        "label_de": "Adipositas-Rate",
        "label_en": "Obesity rate",
        "keywords_de": ["adipositas", "fettleibigkeit", "übergewicht erwachsene"],
        "keywords_en": ["obesity rate", "obese adults"],
        "unit": "%",
        "topic": "owid_health",
    },
    {
        "slug": "daily-per-capita-caloric-supply",
        "label_de": "Kalorien pro Kopf täglich",
        "label_en": "Daily calories per capita",
        "keywords_de": ["kalorienzufuhr", "kalorien pro kopf",
                        "kalorienversorgung"],
        "keywords_en": ["caloric supply", "daily calories", "calorie intake"],
        "unit": "kcal",
        "topic": "owid_health",
    },
    {
        "slug": "meat-supply-per-person",
        "label_de": "Fleischkonsum pro Kopf",
        "label_en": "Meat per person",
        "keywords_de": ["fleischkonsum", "fleischverbrauch",
                        "fleisch pro kopf"],
        "keywords_en": ["meat supply", "meat per person",
                        "meat consumption"],
        "unit": "kg",
        "topic": "owid_health",
    },
    # --- Wirtschaft ---
    {
        "slug": "gdp-per-capita-worldbank",
        "label_de": "BIP pro Kopf",
        "label_en": "GDP per capita",
        "keywords_de": ["bip pro kopf", "bruttoinlandsprodukt pro kopf",
                        "wirtschaftskraft pro kopf", "pro-kopf-einkommen"],
        "keywords_en": ["gdp per capita", "gross domestic product per capita",
                        "income per capita"],
        "unit": "USD",
        "topic": "owid_economy",
    },
    {
        "slug": "gdp",
        "label_de": "BIP gesamt",
        "label_en": "GDP total",
        "keywords_de": ["bip gesamt", "bruttoinlandsprodukt",
                        "wirtschaftsleistung"],
        "keywords_en": ["gdp", "gross domestic product",
                        "total economic output"],
        "unit": "USD",
        "topic": "owid_economy",
    },
    {
        "slug": "share-of-population-in-extreme-poverty",
        "label_de": "Extremarmut-Anteil",
        "label_en": "Extreme poverty share",
        "keywords_de": ["extremarmut", "absolute armut",
                        "weniger als 2 dollar tag"],
        "keywords_en": ["extreme poverty", "below poverty line",
                        "1.90 dollar"],
        "unit": "%",
        "topic": "owid_economy",
    },
    {
        "slug": "economic-inequality-gini-index",
        "label_de": "Gini-Koeffizient",
        "label_en": "Gini coefficient",
        "keywords_de": ["gini-koeffizient", "gini-index",
                        "einkommensungleichheit"],
        "keywords_en": ["gini coefficient", "gini index", "income inequality"],
        "unit": "Index",
        "topic": "owid_economy",
    },
    {
        "slug": "military-expenditure-share-gdp",
        "label_de": "Militär-BIP-Anteil",
        "label_en": "Military spending GDP share",
        "keywords_de": ["militärausgaben", "verteidigungsausgaben",
                        "rüstungsausgaben", "militär bip"],
        "keywords_en": ["military expenditure", "defense spending",
                        "military gdp share"],
        "unit": "% BIP",
        "topic": "owid_economy",
    },
    # --- Demographie ---
    {
        "slug": "population",
        "label_de": "Bevölkerung",
        "label_en": "Population",
        "keywords_de": ["bevölkerung", "einwohnerzahl", "einwohner gesamt"],
        "keywords_en": ["population", "inhabitants", "total population"],
        "unit": "Personen",
        "topic": "owid_demographics",
    },
    {
        "slug": "fertility-rate",
        "label_de": "Geburtenrate",
        "label_en": "Fertility rate",
        "keywords_de": ["geburtenrate", "fruchtbarkeitsrate",
                        "kinder pro frau"],
        "keywords_en": ["fertility rate", "birth rate", "children per woman"],
        "unit": "Kinder/Frau",
        "topic": "owid_demographics",
    },
    {
        "slug": "median-age",
        "label_de": "Medianalter",
        "label_en": "Median age",
        "keywords_de": ["medianalter", "durchschnittsalter median",
                        "altersmedian"],
        "keywords_en": ["median age"],
        "unit": "Jahre",
        "topic": "owid_demographics",
    },
    {
        "slug": "urban-population-share",
        "label_de": "Stadtbevölkerung-Anteil",
        "label_en": "Urban population share",
        "keywords_de": ["stadtbevölkerung", "urbanisierung",
                        "städtische bevölkerung"],
        "keywords_en": ["urban population", "urbanization", "city dwellers"],
        "unit": "%",
        "topic": "owid_demographics",
    },
    # --- Bildung & Digitalisierung ---
    {
        "slug": "mean-years-of-schooling",
        "label_de": "Durchschnittliche Schuljahre",
        "label_en": "Mean years of schooling",
        "keywords_de": ["schuljahre", "durchschnittliche bildungsdauer",
                        "schulbildung jahre"],
        "keywords_en": ["years of schooling", "education years",
                        "schooling duration"],
        "unit": "Jahre",
        "topic": "owid_demographics",
    },
    {
        "slug": "literacy-rate",
        "label_de": "Alphabetisierungsrate",
        "label_en": "Literacy rate",
        "keywords_de": ["alphabetisierungsrate", "leseraten",
                        "lesefähigkeit"],
        "keywords_en": ["literacy rate", "reading ability"],
        "unit": "%",
        "topic": "owid_demographics",
    },
    {
        "slug": "share-of-individuals-using-the-internet",
        "label_de": "Internet-Nutzer-Anteil",
        "label_en": "Internet users share",
        "keywords_de": ["internet-nutzer", "internetnutzung",
                        "internet-zugang", "internet anteil"],
        "keywords_en": ["internet users", "internet penetration",
                        "internet access"],
        "unit": "%",
        "topic": "owid_demographics",
    },
    {
        "slug": "mobile-cellular-subscriptions",
        "label_de": "Mobilfunk-Anschlüsse",
        "label_en": "Mobile subscriptions",
        "keywords_de": ["mobilfunk", "handy-anschlüsse",
                        "mobilfunkverträge"],
        "keywords_en": ["mobile subscriptions", "cellular subscriptions",
                        "phone subscriptions"],
        "unit": "/100",
        "topic": "owid_demographics",
    },
    # --- Demokratie & Sonstiges ---
    {
        "slug": "democracy-index-eiu",
        "label_de": "Demokratie-Index (EIU)",
        "label_en": "Democracy Index (EIU)",
        "keywords_de": ["demokratie-index", "demokratiequalität",
                        "eiu democracy"],
        "keywords_en": ["democracy index", "eiu democracy",
                        "democratic quality"],
        "unit": "Score",
        "topic": "owid_demographics",
    },
    {
        "slug": "annual-deaths-from-natural-disasters",
        "label_de": "Naturkatastrophen-Tote jährlich",
        "label_en": "Annual natural disaster deaths",
        "keywords_de": ["naturkatastrophen tote",
                        "katastrophenopfer", "naturkatastrophe opfer"],
        "keywords_en": ["natural disaster deaths", "disaster victims",
                        "earthquake flood deaths"],
        "unit": "Personen",
        "topic": "owid_climate",
    },
]


# Mapping ISO3 → Anzeige-Code (kurz)
_ISO_TO_LABEL = {
    "AUT": "AT",
    "DEU": "DE",
    "CHE": "CH",
    "OWID_WRL": "Welt",
}

# Trigger-Topics aus analysis-`category`
_TRIGGER_CATEGORIES = {"health", "climate", "economy", "demographics"}


def _match_indicators(analysis: dict, claim_lc: str) -> list[dict]:
    """Wähle bis zu 3 passende Indikatoren aus der Whitelist.

    Match-Strategie:
    1. Sammele DE+EN-Keywords je Indikator und prüfe Substring-Match
       in claim-Text (lower-case) sowie in analysis-`keywords`-Liste.
    2. Bei category-Match (health/climate/economy/demographics) ohne
       konkreten Keyword-Match: KEIN Auto-Trigger (würde zu spammig).
    3. Limit max 3 Indikatoren — vermeidet API-Spam + redundante Treffer.
    """
    if not claim_lc and not analysis:
        return []

    # Sammele alle keyword-Quellen aus analysis
    analysis_keywords: list[str] = []
    for key in ("keywords", "keywords_de", "keywords_en"):
        val = (analysis or {}).get(key)
        if isinstance(val, list):
            analysis_keywords.extend([str(x).lower() for x in val if x])
    haystack = " ".join([claim_lc] + analysis_keywords)
    if not haystack.strip():
        return []

    matches: list[dict] = []
    seen_slugs: set[str] = set()
    for ind in INDICATOR_WHITELIST:
        if ind["slug"] in seen_slugs:
            continue
        kw_pool = (ind.get("keywords_de", []) or []) + (
            ind.get("keywords_en", []) or []
        )
        for kw in kw_pool:
            if not kw:
                continue
            if kw.lower() in haystack:
                matches.append(ind)
                seen_slugs.add(ind["slug"])
                break
        if len(matches) >= 3:
            break
    return matches


async def _fetch_csv_latest(client, slug: str) -> dict | None:
    """Hole CSV für einen OWID-Slug, extrahiere latest year je Land.

    Returns Dict mit Form ``{at: x, de: y, ch: z, world: w, year: yyyy,
    value_column: 'CO2 emissions per capita'}``
    ODER None bei Fehler / leerem Result.
    """
    url = OWID_CSV_URL.format(slug=slug)
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"OWID CSV fetch failed slug={slug} status={resp.status_code}"
            )
            return None
        text = resp.text
        if not text or len(text) < 50:
            return None
    except Exception as e:
        logger.debug(f"OWID CSV fetch exception slug={slug}: {e}")
        return None

    # CSV parsen — Spalten: Entity, Code, Year, <value-column>
    try:
        reader = csv.reader(io.StringIO(text))
        header = next(reader, None)
        if not header or len(header) < 4:
            return None
        # Find indices
        try:
            idx_code = header.index("Code")
            idx_year = header.index("Year")
        except ValueError:
            return None
        # value-column = letzte (oder erste nach Year/Code/Entity)
        # Heuristik: value-Spalte ist die einzige Spalte != Entity/Code/Year
        value_columns = [
            (i, h) for i, h in enumerate(header)
            if h not in ("Entity", "Code", "Year")
        ]
        if not value_columns:
            return None
        idx_value, value_label = value_columns[0]

        # latest-year-per-code aufsammeln
        per_country: dict[str, tuple[int, float]] = {}
        for row in reader:
            if len(row) <= max(idx_code, idx_year, idx_value):
                continue
            code = (row[idx_code] or "").strip()
            year_s = (row[idx_year] or "").strip()
            val_s = (row[idx_value] or "").strip()
            if code not in _ISO_TO_LABEL:
                continue
            if not year_s or not val_s:
                continue
            try:
                year = int(year_s)
                val = float(val_s)
            except ValueError:
                continue
            cur = per_country.get(code)
            if cur is None or year > cur[0]:
                per_country[code] = (year, val)
        if not per_country:
            return None
        # Wähle "Referenz-Jahr" = neuestes Welt-Jahr, fallback DE, fallback AT
        ref_year = None
        for code in ("OWID_WRL", "DEU", "AUT", "CHE"):
            if code in per_country:
                ref_year = per_country[code][0]
                break
        if ref_year is None:
            return None
        return {
            "at": per_country.get("AUT"),
            "de": per_country.get("DEU"),
            "ch": per_country.get("CHE"),
            "world": per_country.get("OWID_WRL"),
            "year": ref_year,
            "value_label": value_label,
        }
    except Exception as e:
        logger.debug(f"OWID CSV parse exception slug={slug}: {e}")
        return None


def _format_value(val_tuple: tuple[int, float] | None, unit: str) -> str:
    """Formatiere Wert als 'X.Y unit' oder '—' falls fehlend."""
    if val_tuple is None:
        return "—"
    val = val_tuple[1]
    # Heuristik: große Zahlen mit Tausender-Trennzeichen
    if abs(val) >= 1000:
        formatted = f"{val:,.0f}".replace(",", ".")
    elif abs(val) >= 10:
        formatted = f"{val:.1f}"
    else:
        formatted = f"{val:.2f}"
    if unit:
        return f"{formatted} {unit}"
    return formatted


def _build_result(ind: dict, data: dict) -> dict:
    """Baue ein result-Dict aus Indikator-Meta + CSV-Daten."""
    unit = ind.get("unit", "")
    at_str = _format_value(data.get("at"), unit)
    de_str = _format_value(data.get("de"), unit)
    ch_str = _format_value(data.get("ch"), unit)
    world_str = _format_value(data.get("world"), unit)
    year = data.get("year", "—")

    display = (
        f"AT {at_str} / DE {de_str} / CH {ch_str} / "
        f"Welt {world_str} ({year}, OWID)"
    )

    label = f"{ind['label_de']} {year}" if year != "—" else ind["label_de"]
    description = (
        f"{ind['label_de']} ({ind['label_en']}) — "
        f"OurWorldInData-Aggregat aus Primärquellen "
        f"(UN/Weltbank/WHO/Global Carbon Project u.a.). "
        f"Lizenz CC-BY 4.0."
    )

    return {
        "indicator_name": label,
        "indicator": "owid_indicator",
        "country": "AT/DE/CH/Welt",
        "year": str(year),
        "topic": ind.get("topic", "owid_indicator"),
        "display_value": display,
        "description": description[:300],
        "url": f"https://ourworldindata.org/grapher/{ind['slug']}",
        "secondary_url": "",
        "source": "OurWorldInData (CC-BY)",
    }


async def fetch_owid(client=None) -> list[dict]:
    """Prefetch-Hook — gibt Whitelist-Meta zurück (Indikator-Liste).

    Da OWID 35+ Indikatoren hat und jede CSV einen eigenen Roundtrip
    bedeutet, wird hier KEIN Bulk-Prefetch gemacht. Stattdessen liefert
    diese Funktion nur Meta-Info zur Indikator-Whitelist (für
    data_updater-Inventar / Status-Endpunkte).

    Echter Daten-Fetch geschieht on-demand in ``search_owid``.
    """
    return [
        {
            "indicator_name": ind["label_de"],
            "indicator": "owid_indicator_meta",
            "country": "—",
            "year": "—",
            "topic": ind["topic"],
            "display_value": (
                f"{ind['label_de']} ({ind['label_en']}) — "
                f"slug={ind['slug']}"
            ),
            "description": (
                f"OWID-Whitelist-Eintrag (on-demand-Fetch). "
                f"Unit: {ind.get('unit', '')}"
            ),
            "url": f"https://ourworldindata.org/grapher/{ind['slug']}",
            "secondary_url": "",
            "source": "OurWorldInData (Meta, CC-BY)",
        }
        for ind in INDICATOR_WHITELIST
    ]


async def search_owid(analysis: dict) -> dict:
    """Live-Lookup gegen OurWorldInData — bis zu 3 Indikatoren je Claim.

    Trigger:
    - claim-`category` in {health, climate, economy, demographics}, ODER
    - Claim-Text/Analyse-Keywords matchen einen Whitelist-Eintrag.

    Returns Dict mit ≤3 OWID-Indikator-Treffern (AT/DE/CH/Welt-Werte
    aus dem neuesten verfügbaren Jahr).
    """
    empty = {"source": "OurWorldInData", "type": "global_indicators",
             "results": []}

    if not analysis:
        return empty

    claim_text = (analysis.get("claim") or analysis.get("text") or "").lower()
    category = (analysis.get("category") or "").lower()

    # Pre-Check: Trigger nur wenn category-Match ODER keyword-haystack
    # einen Whitelist-Keyword enthält.
    if category not in _TRIGGER_CATEGORIES and not claim_text:
        return empty

    matched = _match_indicators(analysis, claim_text)
    if not matched:
        return empty

    async with polite_client(timeout=15.0) as client:
        tasks = [_fetch_csv_latest(client, ind["slug"]) for ind in matched]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = []
    for ind, data in zip(matched, fetched):
        if isinstance(data, Exception) or not data:
            continue
        # Mindestens AT ODER Welt-Wert vorhanden, sonst skip
        if not (data.get("at") or data.get("world")):
            continue
        results.append(_build_result(ind, data))

    if not results:
        logger.info(
            f"OWID: 0 Treffer für category='{category}' "
            f"matched={[m['slug'] for m in matched]}"
        )
        return empty

    logger.info(
        f"OWID: {len(results)} Indikator-Treffer geliefert "
        f"(slugs={[m['slug'] for m in matched[:3]]})"
    )
    return {
        "source": "OurWorldInData",
        "type": "global_indicators",
        "results": results,
    }
