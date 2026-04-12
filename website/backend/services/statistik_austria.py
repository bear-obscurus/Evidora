"""Statistik Austria Open Government Data — österreichische amtliche Statistiken.

Integriert vier Datasets:
1. VPI (Verbraucherpreisindex) — monatliche Inflationsdaten (Basis 2020=100)
2. Gesundheitsausgaben — jährliche Ausgaben nach Leistungsart und Finanzierung (SHA)
3. Sterblichkeit nach Kalenderwoche — wöchentliche Sterbefälle seit 2000 (für Übersterblichkeit)
4. VGR (Volkswirtschaftliche Gesamtrechnung) — jährliche BIP-Daten + 47 Aggregate (ESA 2010)

Datenquelle: https://data.statistik.gv.at
Lizenz: CC BY 4.0
Kein API-Key erforderlich — statische CSV-Downloads, lokal gecacht.
"""

import csv
import io
import logging
from datetime import datetime

import httpx

logger = logging.getLogger("evidora")

# --- Download URLs ---
VPI_CSV_URL = "https://data.statistik.gv.at/data/OGD_vpi20c18_VPI_2020COICOP18_1.csv"
HEALTH_EXP_CSV_URL = "https://data.statistik.gv.at/data/OGD_gesausgaben01_HVD_HCHF_1.csv"
MORTALITY_CSV_URL = "https://data.statistik.gv.at/data/OGD_gest_kalwo_GEST_KALWOCHE_100.csv"
VGR_CSV_URL = "https://data.statistik.gv.at/data/OGD_vgr101_VGRJahresR_3.csv"

# --- Cache ---
STAT_AT_CACHE_TTL = 86400  # 24h — VPI updates monthly, health expenditure annually

_vpi_cache: list[dict] | None = None
_vpi_cache_time: float = 0.0

_health_cache: list[dict] | None = None
_health_cache_time: float = 0.0

_mortality_cache: dict | None = None  # aggregated by year
_mortality_cache_time: float = 0.0

_vgr_cache: list[dict] | None = None
_vgr_cache_time: float = 0.0

# --- COICOP category labels (main groups only) ---
COICOP_LABELS = {
    "0": "Gesamtindex",
    "01": "Nahrungsmittel & alkoholfreie Getränke",
    "02": "Alkohol & Tabak",
    "03": "Bekleidung & Schuhe",
    "04": "Wohnung, Wasser, Energie",
    "05": "Hausrat & Instandhaltung",
    "06": "Gesundheit",
    "07": "Verkehr",
    "08": "Information & Kommunikation",
    "09": "Freizeit, Sport & Kultur",
    "10": "Bildung",
    "11": "Gastronomie & Beherbergung",
    "12": "Versicherungen & Finanzdienstleistungen",
    "13": "Körperpflege & persönliche Gebrauchsgegenstände",
    # Special aggregates
    "SA": "Industriegüter ohne Energie",
    "SE": "Energie",
    "SF": "Nahrungsmittel, Alkohol & Tabak",
    "SS": "Dienstleistungen",
}

# Health function labels (top-level)
HC_LABELS = {
    "HC-ALL_HC": "Alle Gesundheitsleistungen & -güter",
    "HC-HC1-HC2": "Kurative & rehabilitative Versorgung",
    "HC-HC3": "Langzeitpflege",
    "HC-HC4": "Hilfsleistungen",
    "HC-HC5": "Medizinische Güter",
    "HC-HC6": "Prävention",
    "HC-HC7": "Verwaltung & Finanzierung",
    "HC-HC1": "Kurative Versorgung",
    "HC-HC2": "Rehabilitative Versorgung",
}

# Financing scheme labels
HF_LABELS = {
    "F-ALLE_HF": "Gesamt",
    "F-HF1": "Staat & Pflichtversicherung",
    "F-HF11": "Staat (Bund/Länder/Gemeinden)",
    "F-HF12": "Pflichtversicherung",
    "F-HF2": "Freiwillige Systeme",
    "F-HF3": "Private Haushalte (Out-of-Pocket)",
}


# --- VGR ESA 2010 indicator labels ---
VGR_LABELS = {
    "ESVG2010-1": "Konsumausgaben",
    "ESVG2010-2": "Bruttoinvestitionen",
    "ESVG2010-3": "Exporte",
    "ESVG2010-4": "Importe",
    "ESVG2010-5": "Statistische Differenz",
    "ESVG2010-6": "Konsumausgaben der privaten Haushalte",
    "ESVG2010-7": "Konsumausgaben der priv. Organisationen o. Erwerbszweck",
    "ESVG2010-8": "Konsumausgaben des Staates",
    "ESVG2010-9": "Konsumausgaben des Staates f. Individualverbrauch",
    "ESVG2010-10": "Konsumausgaben des Staates f. Kollektivverbrauch",
    "ESVG2010-11": "Individualverbrauch",
    "ESVG2010-12": "Bruttoanlageinvestitionen Nutztiere/Nutzpflanzungen",
    "ESVG2010-13": "Bruttoanlageinvestitionen Maschinen/Geräte (inkl. Waffen)",
    "ESVG2010-14": "Bruttoanlageinvestitionen IKT",
    "ESVG2010-15": "Bruttoanlageinvestitionen Fahrzeuge",
    "ESVG2010-16": "Bruttoanlageinvestitionen Wohnbauten",
    "ESVG2010-17": "Bruttoanlageinvestitionen Nichtwohnbauten",
    "ESVG2010-18": "Bruttoanlageinvestitionen F&E",
    "ESVG2010-19": "Bruttoanlageinvestitionen Geistiges Eigentum",
    "ESVG2010-20": "Bruttoanlageinvestitionen insgesamt",
    "ESVG2010-21": "Lagerveränderungen",
    "ESVG2010-22": "Nettozugang an Wertsachen",
    "ESVG2010-23": "Warenexporte",
    "ESVG2010-24": "Dienstleistungsexporte",
    "ESVG2010-25": "Warenimporte",
    "ESVG2010-26": "Dienstleistungsimporte",
    "ESVG2010-27": "Außenbeitrag",
    "ESVG2010-28": "Primäreinkommen aus der übrigen Welt",
    "ESVG2010-29": "Primäreinkommen an die übrige Welt",
    "ESVG2010-30": "Bruttonationaleinkommen",
    "ESVG2010-31": "Laufende Transfers aus der übrigen Welt",
    "ESVG2010-32": "Laufende Transfers an die übrige Welt",
    "ESVG2010-33": "Abschreibungen",
    "ESVG2010-34": "Nettonationaleinkommen",
    "ESVG2010-35": "Verfügbares Nettonationaleinkommen",
    "ESVG2010-36": "Bruttowertschöpfung insgesamt",
    "ESVG2010-37": "Gütersteuern minus Gütersubventionen",
    "ESVG2010-38": "Bruttoinlandsprodukt",
    "ESVG2010-39": "Bruttobetriebsüberschuss und Selbständigeneinkommen",
    "ESVG2010-40": "Produktions- und Importabgaben minus Subventionen",
    "ESVG2010-41": "Sparen, netto",
    "ESVG2010-42": "Vermögenstransfers aus der übrigen Welt",
    "ESVG2010-43": "Vermögenstransfers an das Ausland",
    "ESVG2010-44": "Nettozugang an nichtproduzierten Vermögensgütern",
    "ESVG2010-45": "Finanzierungssaldo der Gesamtwirtschaft",
    "ESVG2010-46": "Bruttoinlandsprodukt pro Kopf",
    "ESVG2010-47": "Bruttonationaleinkommen pro Kopf",
}

# Mapping from claim keywords → relevant VGR indicator codes
VGR_INDICATOR_KEYWORDS: dict[str, list[str]] = {
    # BIP / GDP
    "ESVG2010-38": ["bip", "gdp", "bruttoinlandsprodukt", "gross domestic product",
                     "wirtschaftsleistung", "economic output", "wirtschaftswachstum",
                     "economic growth", "rezession", "recession", "konjunktur"],
    "ESVG2010-46": ["bip pro kopf", "gdp per capita", "pro-kopf-einkommen",
                     "wohlstand", "prosperity", "lebensstandard"],
    # Konsum
    "ESVG2010-1": ["konsum", "consumption", "verbrauch"],
    "ESVG2010-6": ["privatkonsum", "private consumption", "haushaltskonsum",
                    "consumer spending", "konsumausgaben privat"],
    "ESVG2010-8": ["staatskonsum", "staatsausgaben", "government spending",
                    "öffentliche ausgaben", "public spending"],
    # Investitionen
    "ESVG2010-20": ["investition", "investment", "anlageinvestition", "capex"],
    "ESVG2010-16": ["wohnbau", "housing investment", "wohnungsbau"],
    "ESVG2010-18": ["forschung", "research", "f&e", "r&d", "innovation"],
    # Außenhandel
    "ESVG2010-3": ["export", "ausfuhr"],
    "ESVG2010-4": ["import", "einfuhr"],
    "ESVG2010-27": ["außenbeitrag", "handelsbilanz", "trade balance",
                     "leistungsbilanz", "current account"],
    # Nationaleinkommen
    "ESVG2010-30": ["nationaleinkommen", "national income", "bruttonationaleinkommen", "gni"],
    "ESVG2010-41": ["sparquote", "sparen", "saving", "savings rate"],
    "ESVG2010-45": ["finanzierungssaldo", "budget deficit", "budgetdefizit",
                     "staatsdefizit", "fiscal balance", "haushaltssaldo",
                     "staatsverschuldung", "government debt"],
}


def _parse_decimal(value: str) -> float | None:
    """Parse Austrian decimal format (comma as separator)."""
    if not value or value.strip() == "":
        return None
    try:
        return float(value.strip().replace(",", "."))
    except (ValueError, AttributeError):
        return None


def _parse_period(code: str) -> tuple[int | None, int | None]:
    """Parse VPIZR-YYYYMM or VPIZR-YYYY into (year, month).
    Returns (year, None) for annual averages."""
    # Extract numeric part after the prefix
    parts = code.split("-", 1)
    if len(parts) < 2:
        return None, None
    num = parts[1]
    if len(num) == 6:
        return int(num[:4]), int(num[4:])
    elif len(num) == 4:
        return int(num), None
    return None, None


def _parse_vgr_year(code: str) -> int | None:
    """Parse A10-YYYY into year."""
    if not code.startswith("A10-"):
        return None
    try:
        return int(code[4:])
    except ValueError:
        return None


def _parse_health_year(code: str) -> int | None:
    """Parse ZEITGES-YYYY into year."""
    parts = code.split("-", 1)
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


# --- CSV Download & Parsing ---

async def fetch_vpi(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Download and parse VPI CSV. Returns list of row dicts."""
    import time
    global _vpi_cache, _vpi_cache_time

    now = time.time()
    if _vpi_cache is not None and (now - _vpi_cache_time) < STAT_AT_CACHE_TTL:
        return _vpi_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    try:
        resp = await client.get(VPI_CSV_URL)
        resp.raise_for_status()
        text = resp.text

        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        rows = []
        for row in reader:
            period_code = row.get("C-VPIZR-0", "")
            coicop_code = row.get("C-VPICOICOP18_5-0", "")

            year, month = _parse_period(period_code)
            if year is None:
                continue

            # Extract COICOP category number (after "VPICOICOP18-")
            cat = coicop_code.replace("VPICOICOP18-", "")

            rows.append({
                "period": period_code,
                "year": year,
                "month": month,
                "coicop_code": cat,
                "coicop_label": COICOP_LABELS.get(cat, cat),
                "index": _parse_decimal(row.get("F-VPIMZBM", "")),
                "pct_vs_prev_month": _parse_decimal(row.get("F-VPIPZVM", "")),
                "pct_vs_prev_year": _parse_decimal(row.get("F-VPIPZVJM", "")),
                "weight": _parse_decimal(row.get("F-VPIGEWBM", "")),
            })

        _vpi_cache = rows
        _vpi_cache_time = now
        logger.info(f"Statistik Austria VPI: {len(rows)} Datenpunkte geladen")
        return rows

    except Exception as e:
        logger.warning(f"Statistik Austria VPI download failed: {e}")
        return _vpi_cache or []
    finally:
        if close_client:
            await client.aclose()


async def fetch_health_expenditure(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Download and parse health expenditure CSV. Returns list of row dicts."""
    import time
    global _health_cache, _health_cache_time

    now = time.time()
    if _health_cache is not None and (now - _health_cache_time) < STAT_AT_CACHE_TTL:
        return _health_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    try:
        resp = await client.get(HEALTH_EXP_CSV_URL)
        resp.raise_for_status()
        text = resp.text

        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        rows = []
        for row in reader:
            year = _parse_health_year(row.get("C-ZEITGES-0", ""))
            hc_code = row.get("C-HCGES-0", "")
            if year is None:
                continue

            rows.append({
                "year": year,
                "hc_code": hc_code,
                "hc_label": HC_LABELS.get(hc_code, hc_code),
                "total": _parse_decimal(row.get("F-ALLE_HF", "")),
                "government": _parse_decimal(row.get("F-HF1", "")),
                "government_direct": _parse_decimal(row.get("F-HF11", "")),
                "social_insurance": _parse_decimal(row.get("F-HF12", "")),
                "voluntary": _parse_decimal(row.get("F-HF2", "")),
                "out_of_pocket": _parse_decimal(row.get("F-HF3", "")),
            })

        _health_cache = rows
        _health_cache_time = now
        logger.info(f"Statistik Austria Gesundheitsausgaben: {len(rows)} Datenpunkte geladen")
        return rows

    except Exception as e:
        logger.warning(f"Statistik Austria health expenditure download failed: {e}")
        return _health_cache or []
    finally:
        if close_client:
            await client.aclose()


async def fetch_mortality(client: httpx.AsyncClient | None = None) -> dict:
    """Download and parse mortality CSV. Returns dict aggregated by year.

    The raw CSV has ~1.2M rows (weekly × state × age × gender since 2000).
    We aggregate to yearly totals (+ by age group) at download time.
    """
    import time
    global _mortality_cache, _mortality_cache_time

    now = time.time()
    if _mortality_cache is not None and (now - _mortality_cache_time) < STAT_AT_CACHE_TTL:
        return _mortality_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=60.0)
        close_client = True

    try:
        resp = await client.get(MORTALITY_CSV_URL, timeout=60.0)
        resp.raise_for_status()
        text = resp.text

        reader = csv.DictReader(io.StringIO(text), delimiter=";")

        # Aggregate: {year: {total, under65, over65}}
        yearly: dict[int, dict] = {}
        for row in reader:
            kw_code = row.get("C-KALWOCHE-0", "")
            age_code = row.get("C-ALTERGR65-0", "")
            count_str = row.get("F-ANZ-1", "")

            # Extract year from KALW-YYYYWW
            if not kw_code.startswith("KALW-") or len(kw_code) < 11:
                continue
            try:
                year = int(kw_code[5:9])
            except ValueError:
                continue

            count = int(count_str) if count_str.strip() else 0

            if year not in yearly:
                yearly[year] = {"total": 0, "under65": 0, "over65": 0}
            yearly[year]["total"] += count
            if age_code == "ALTERSGR65-1":
                yearly[year]["under65"] += count
            elif age_code == "ALTERSGR65-2":
                yearly[year]["over65"] += count

        _mortality_cache = yearly
        _mortality_cache_time = now
        logger.info(f"Statistik Austria Sterblichkeit: {len(yearly)} Jahre geladen ({min(yearly.keys()) if yearly else '?'}–{max(yearly.keys()) if yearly else '?'})")
        return yearly

    except Exception as e:
        logger.warning(f"Statistik Austria mortality download failed: {e}")
        return _mortality_cache or {}
    finally:
        if close_client:
            await client.aclose()


async def fetch_vgr(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Download and parse VGR annual CSV (ESA 2010 main aggregates).

    Returns list of row dicts with year, indicator code/label, nominal value (Mio. EUR),
    and chained volume index.
    ~1,400 rows (47 indicators × ~30 years).
    """
    import time
    global _vgr_cache, _vgr_cache_time

    now = time.time()
    if _vgr_cache is not None and (now - _vgr_cache_time) < STAT_AT_CACHE_TTL:
        return _vgr_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    try:
        resp = await client.get(VGR_CSV_URL)
        resp.raise_for_status()
        text = resp.text

        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        rows = []
        for row in reader:
            year = _parse_vgr_year(row.get("C-A10-0", ""))
            indicator = row.get("C-ESVG2010-0", "")
            if year is None or not indicator:
                continue

            rows.append({
                "year": year,
                "indicator_code": indicator,
                "indicator_label": VGR_LABELS.get(indicator, indicator),
                "nominal_mio": _parse_decimal(row.get("F-ISIS-4575", "")),
                "volume_index": _parse_decimal(row.get("F-ISIS-4576", "")),
            })

        _vgr_cache = rows
        _vgr_cache_time = now
        logger.info(f"Statistik Austria VGR: {len(rows)} Datenpunkte geladen")
        return rows

    except Exception as e:
        logger.warning(f"Statistik Austria VGR download failed: {e}")
        return _vgr_cache or []
    finally:
        if close_client:
            await client.aclose()


# --- Keyword detection ---

VPI_KEYWORDS = [
    "inflation", "teuerung", "verbraucherpreis", "vpi", "cpi",
    "preissteigerung", "preisanstieg", "lebenshaltungskosten",
    "consumer price", "price increase", "cost of living",
    "lebensmittelpreise", "food prices", "energiepreise", "energy prices",
    "mietpreise", "wohnkosten", "housing cost",
]

HEALTH_EXP_KEYWORDS = [
    "gesundheitsausgaben", "health expenditure", "gesundheitskosten",
    "health spending", "gesundheitssystem", "healthcare system",
    "gesundheitswesen", "health care cost", "krankenhauskosten",
    "hospital cost", "arztkosten", "pflegekosten", "care cost",
    "medikamentenkosten", "pharmaceutical cost", "gesundheitsbudget",
    "health budget", "kassenbeiträge", "sozialversicherung",
    "gesundheitsfinanzierung", "health financing",
]

MORTALITY_KEYWORDS = [
    "übersterblichkeit", "excess mortality", "excess deaths",
    "sterblichkeit", "mortality", "sterbefälle", "todesfälle",
    "deaths", "gestorben", "sterben", "todesrate", "death rate",
    "lebenserwartung", "life expectancy",
    "corona tote", "covid tote", "pandemie tote", "pandemic deaths",
    "impftote", "impfung tod", "vaccine deaths",
]

VGR_KEYWORDS = [
    "bip", "gdp", "bruttoinlandsprodukt", "gross domestic product",
    "wirtschaftsleistung", "economic output", "wirtschaftswachstum",
    "economic growth", "rezession", "recession", "konjunktur",
    "bruttonationaleinkommen", "national income", "gni",
    "export", "import", "ausfuhr", "einfuhr", "außenhandel",
    "handelsbilanz", "trade balance", "leistungsbilanz",
    "investition", "investment", "bruttoanlageinvestition",
    "staatskonsum", "staatsausgaben", "government spending",
    "öffentliche ausgaben", "public spending",
    "sparquote", "sparen", "saving",
    "finanzierungssaldo", "budgetdefizit", "budget deficit",
    "staatsdefizit", "fiscal balance", "haushaltssaldo",
    "wertschöpfung", "value added", "volkswirtschaft",
    "privatkonsum", "private consumption", "konsumausgaben",
    "wohnbau", "housing investment", "forschung ausgaben",
]

# Keywords that indicate Austrian context
AUSTRIA_KEYWORDS = [
    "österreich", "austria", "wien", "vienna", "graz", "linz", "salzburg",
    "innsbruck", "klagenfurt", "vorarlberg", "tirol", "kärnten",
    "steiermark", "oberösterreich", "niederösterreich", "burgenland",
    "österreichisch", "austrian",
]


def _is_austria_context(text: str) -> bool:
    """Check if claim has Austrian context."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in AUSTRIA_KEYWORDS)


def _match_keywords(text: str, keywords: list[str]) -> bool:
    """Check if text matches any keyword."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


# --- VPI category detection from claim ---

VPI_CATEGORY_KEYWORDS = {
    "01": ["nahrungsmittel", "lebensmittel", "food", "essen", "brot", "milch", "fleisch", "gemüse", "obst"],
    "02": ["alkohol", "tabak", "zigaretten", "bier", "wein", "tobacco", "alcohol"],
    "03": ["bekleidung", "kleidung", "schuhe", "mode", "clothing", "shoes"],
    "04": ["wohnung", "miete", "wohnen", "energie", "strom", "gas", "heizung", "housing", "rent", "electricity"],
    "05": ["möbel", "haushaltsgerät", "furniture", "household"],
    "06": ["gesundheit", "arzt", "medikament", "health", "pharma", "apotheke", "krankenhaus"],
    "07": ["verkehr", "transport", "benzin", "diesel", "auto", "bahn", "öffi", "fuel", "car", "train"],
    "08": ["telefon", "internet", "kommunikation", "handy", "telecom"],
    "09": ["freizeit", "sport", "kultur", "urlaub", "recreation", "leisure"],
    "10": ["bildung", "schule", "universität", "education"],
    "11": ["restaurant", "gastronomie", "hotel", "dining", "accommodation"],
    "12": ["versicherung", "insurance", "finanz", "bank"],
    "13": ["körperpflege", "friseur", "personal care"],
    "SE": ["energiepreis", "energy price", "strom", "gas", "heizöl", "benzin", "diesel"],
    "SF": ["lebensmittelpreis", "food price", "nahrungsmittel"],
}


def _detect_vpi_categories(text: str) -> list[str]:
    """Detect which COICOP categories are relevant for the claim."""
    text_lower = text.lower()
    matched = []
    for cat, keywords in VPI_CATEGORY_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            matched.append(cat)
    return matched


# --- Month names ---
MONTH_NAMES = {
    1: "Jänner", 2: "Februar", 3: "März", 4: "April",
    5: "Mai", 6: "Juni", 7: "Juli", 8: "August",
    9: "September", 10: "Oktober", 11: "November", 12: "Dezember",
}


# --- Search functions ---

async def search_statistik_austria(analysis: dict) -> dict:
    """Search Statistik Austria for VPI, health expenditure, mortality, and VGR data."""
    claim = analysis.get("claim", "")
    entities = analysis.get("entities", [])
    subcategory = analysis.get("subcategory", "")
    search_text = f"{claim} {' '.join(entities)} {subcategory}"

    results = []

    # VPI / Inflation data
    if _match_keywords(search_text, VPI_KEYWORDS):
        vpi_results = await _search_vpi(search_text)
        results.extend(vpi_results)

    # Health expenditure data
    if _match_keywords(search_text, HEALTH_EXP_KEYWORDS):
        health_results = await _search_health_expenditure(search_text)
        results.extend(health_results)

    # Mortality / excess mortality data
    if _match_keywords(search_text, MORTALITY_KEYWORDS):
        mortality_results = await _search_mortality(search_text)
        results.extend(mortality_results)

    # VGR / GDP / national accounts data
    if _match_keywords(search_text, VGR_KEYWORDS):
        vgr_results = await _search_vgr(search_text)
        results.extend(vgr_results)

    # Add context caveat if we have results
    if results:
        results.append({
            "title": "WICHTIGER KONTEXT: Statistik Austria — österreichische Daten",
            "indicator": "Methodische Einordnung",
            "year": "",
            "value": "",
            "url": "https://data.statistik.gv.at",
            "description": (
                "Statistik Austria liefert amtliche österreichische Daten. "
                "Einschränkungen: "
                "(1) Der VPI misst den Durchschnitt eines definierten Warenkorbs — die "
                "individuelle Inflation kann je nach Konsumverhalten deutlich abweichen "
                "(Mieter vs. Eigentümer, Pendler vs. Nicht-Pendler). "
                "(2) Gesundheitsausgaben zeigen die Gesamtfinanzierung (öffentlich + privat), "
                "aber NICHT die Effizienz oder Qualität der Versorgung — höhere Ausgaben "
                "bedeuten nicht automatisch bessere Gesundheit. "
                "(3) Ausgaben pro Kopf sind aussagekräftiger als absolute Zahlen — "
                "Österreich hat ~9 Mio. Einwohner, Deutschland ~84 Mio. "
                "(4) Keine Aufschlüsselung nach Bevölkerungsgruppen (z.B. Asylwerber) — "
                "dafür sind spezifische Studien nötig. "
                "(5) Das BIP misst Wirtschaftsleistung, nicht Wohlstand — es erfasst "
                "weder Einkommensverteilung noch unbezahlte Arbeit oder Umweltkosten. "
                "Nominelle Werte sind inflationsbereinigt (Volumenindex) vergleichbarer."
            ),
        })

    return {
        "source": "Statistik Austria (OGD)",
        "type": "official_data",
        "results": results,
    }


async def _search_vpi(search_text: str) -> list[dict]:
    """Search VPI data for relevant inflation figures."""
    data = await fetch_vpi()
    if not data:
        return []

    results = []

    # Detect specific categories or use Gesamtindex
    target_cats = _detect_vpi_categories(search_text)
    if not target_cats:
        target_cats = ["0"]  # Gesamtindex

    # Always include Gesamtindex for context
    if "0" not in target_cats:
        target_cats.insert(0, "0")

    # Get the latest available data
    # Find most recent year+month with data
    max_period = max(
        (r for r in data if r["month"] is not None),
        key=lambda r: (r["year"], r["month"]),
        default=None,
    )
    if not max_period:
        return []

    latest_year = max_period["year"]
    latest_month = max_period["month"]

    for cat in target_cats[:4]:  # max 4 categories
        # Latest month
        latest = [r for r in data
                  if r["coicop_code"] == cat
                  and r["year"] == latest_year
                  and r["month"] == latest_month]
        if latest:
            row = latest[0]
            pct = row["pct_vs_prev_year"]
            idx = row["index"]
            month_name = MONTH_NAMES.get(row["month"], str(row["month"]))

            if pct is not None:
                sign = "+" if pct > 0 else ""
                results.append({
                    "title": f"VPI {row['coicop_label']} — {month_name} {row['year']}: {sign}{pct:.1f}% ggü. Vorjahr (Index: {idx:.1f})",
                    "indicator": f"Verbraucherpreisindex — {row['coicop_label']}",
                    "year": f"{row['year']}-{row['month']:02d}",
                    "value": pct,
                    "unit": "% Veränderung ggü. Vorjahresmonat",
                    "source": "Statistik Austria",
                    "url": "https://www.statistik.at/statistiken/volkswirtschaft-und-oeffentliche-finanzen/preise-und-preisindizes/verbraucherpreisindex-vpi/hvpi",
                    "dataset_id": "OGD_vpi20c18_VPI_2020COICOP18_1",
                })

        # Also add annual averages for trend (last 3 years)
        if cat == "0":  # Only for Gesamtindex
            annual = [r for r in data
                      if r["coicop_code"] == cat
                      and r["month"] is None]  # Annual averages
            annual.sort(key=lambda r: r["year"], reverse=True)
            for row in annual[:3]:
                pct = row["pct_vs_prev_year"]
                idx = row["index"]
                if pct is not None:
                    sign = "+" if pct > 0 else ""
                    results.append({
                        "title": f"VPI Jahresdurchschnitt {row['year']}: {sign}{pct:.1f}% (Index: {idx:.1f})",
                        "indicator": "Verbraucherpreisindex — Jahresdurchschnitt",
                        "year": str(row["year"]),
                        "value": pct,
                        "unit": "% Veränderung ggü. Vorjahr",
                        "source": "Statistik Austria",
                        "url": "https://www.statistik.at/statistiken/volkswirtschaft-und-oeffentliche-finanzen/preise-und-preisindizes/verbraucherpreisindex-vpi/hvpi",
                        "dataset_id": "OGD_vpi20c18_VPI_2020COICOP18_1",
                    })

    return results


async def _search_mortality(search_text: str) -> list[dict]:
    """Search mortality data for excess mortality analysis."""
    data = await fetch_mortality()
    if not data:
        return []

    results = []

    # Calculate baseline (2015-2019 average) for excess mortality comparison
    baseline_years = [2015, 2016, 2017, 2018, 2019]
    baseline_totals = [data[y]["total"] for y in baseline_years if y in data]
    if not baseline_totals:
        return []
    baseline_avg = sum(baseline_totals) / len(baseline_totals)

    # Show recent full years (2019–latest).
    # Exclude current year if it has fewer than 40 weeks of data (incomplete).
    all_years = sorted(data.keys(), reverse=True)
    latest_year = all_years[0] if all_years else 0
    # A full year has ~80K+ deaths; if way below baseline it's likely incomplete
    min_threshold = baseline_avg * 0.7
    recent_years = [
        y for y in all_years
        if y >= 2019 and (y != latest_year or data[y]["total"] >= min_threshold)
    ][:7]

    for year in recent_years:
        yr = data[year]
        total = yr["total"]
        under65 = yr["under65"]
        over65 = yr["over65"]

        # Calculate excess mortality vs. baseline
        excess = total - baseline_avg
        excess_pct = (excess / baseline_avg * 100) if baseline_avg > 0 else 0

        # Format
        sign = "+" if excess > 0 else ""
        parts = [f"Gesamt: {total:,}"]
        if over65 > 0:
            over65_pct = (over65 / total * 100) if total > 0 else 0
            parts.append(f"65+: {over65:,} ({over65_pct:.0f}%)")
        parts.append(f"vs. Baseline 2015–19: {sign}{excess:,.0f} ({sign}{excess_pct:.1f}%)")

        detail = " | ".join(parts)

        results.append({
            "title": f"Sterbefälle AT {year}: {detail}",
            "indicator": "Sterblichkeit nach Kalenderwoche",
            "year": str(year),
            "value": total,
            "excess": round(excess),
            "excess_pct": round(excess_pct, 1),
            "unit": "Personen",
            "source": "Statistik Austria",
            "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/bevoelkerung/gestorbene",
            "dataset_id": "OGD_gest_kalwo_GEST_KALWOCHE_100",
        })

    # Add baseline reference
    if baseline_totals:
        results.append({
            "title": f"Baseline 2015–2019: Ø {baseline_avg:,.0f} Sterbefälle/Jahr",
            "indicator": "Sterblichkeit — Referenzwert",
            "year": "2015–2019",
            "value": round(baseline_avg),
            "unit": "Personen/Jahr (Durchschnitt)",
            "source": "Statistik Austria",
            "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/bevoelkerung/gestorbene",
            "dataset_id": "OGD_gest_kalwo_GEST_KALWOCHE_100",
        })

    return results


async def _search_health_expenditure(search_text: str) -> list[dict]:
    """Search health expenditure data."""
    data = await fetch_health_expenditure()
    if not data:
        return []

    results = []

    # Focus on top-level aggregates
    target_hc = ["HC-ALL_HC"]  # Total

    # Add specific categories if detected in claim
    text_lower = search_text.lower()
    if any(kw in text_lower for kw in ["krankenhaus", "hospital", "stationär", "inpatient"]):
        target_hc.append("HC-HC1-HC2")
    if any(kw in text_lower for kw in ["langzeitpflege", "pflege", "long-term care", "care"]):
        target_hc.append("HC-HC3")
    if any(kw in text_lower for kw in ["medikament", "arzneimittel", "pharma", "medication", "drug"]):
        target_hc.append("HC-HC5")
    if any(kw in text_lower for kw in ["prävention", "vorsorge", "prevention"]):
        target_hc.append("HC-HC6")

    # Get latest 5 years for each target
    for hc_code in target_hc[:3]:
        rows = [r for r in data if r["hc_code"] == hc_code]
        rows.sort(key=lambda r: r["year"], reverse=True)

        for row in rows[:5]:
            total = row["total"]
            gov = row["government"]
            oop = row["out_of_pocket"]

            if total is None:
                continue

            # Build description parts
            parts = [f"Gesamt: {total:,.1f} Mio. €"]
            if gov is not None:
                gov_pct = (gov / total * 100) if total > 0 else 0
                parts.append(f"Öffentlich: {gov:,.1f} Mio. € ({gov_pct:.0f}%)")
            if oop is not None:
                oop_pct = (oop / total * 100) if total > 0 else 0
                parts.append(f"Privat (Out-of-Pocket): {oop:,.1f} Mio. € ({oop_pct:.0f}%)")

            detail = " | ".join(parts)

            results.append({
                "title": f"Gesundheitsausgaben AT {row['year']} — {row['hc_label']}: {detail}",
                "indicator": f"Gesundheitsausgaben — {row['hc_label']}",
                "year": str(row["year"]),
                "value": total,
                "unit": "Mio. EUR",
                "source": "Statistik Austria (SHA-Methodik)",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/gesundheit/gesundheitsausgaben",
                "dataset_id": "OGD_gesausgaben01_HVD_HCHF_1",
            })

    return results


def _detect_vgr_indicators(text: str) -> list[str]:
    """Detect which VGR indicators are relevant for the claim."""
    text_lower = text.lower()
    matched = []
    for code, keywords in VGR_INDICATOR_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            matched.append(code)
    return matched


async def _search_vgr(search_text: str) -> list[dict]:
    """Search VGR data for GDP and national accounts figures."""
    data = await fetch_vgr()
    if not data:
        return []

    results = []

    # Detect specific indicators or default to BIP + BIP pro Kopf
    target_codes = _detect_vgr_indicators(search_text)
    if not target_codes:
        target_codes = ["ESVG2010-38"]  # BIP as default

    # Always include BIP for context
    if "ESVG2010-38" not in target_codes:
        target_codes.insert(0, "ESVG2010-38")

    # Limit to max 5 indicators to keep results manageable
    target_codes = target_codes[:5]

    for code in target_codes:
        rows = [r for r in data if r["indicator_code"] == code]
        rows.sort(key=lambda r: r["year"], reverse=True)

        # Show latest 5 years for trend analysis
        for row in rows[:5]:
            nominal = row["nominal_mio"]
            vol_idx = row["volume_index"]
            label = row["indicator_label"]

            if nominal is None:
                continue

            # Calculate year-on-year growth (nominal)
            prev = next(
                (r for r in data
                 if r["indicator_code"] == code and r["year"] == row["year"] - 1),
                None,
            )
            growth_str = ""
            if prev and prev["nominal_mio"] and prev["nominal_mio"] > 0:
                growth = (nominal - prev["nominal_mio"]) / prev["nominal_mio"] * 100
                sign = "+" if growth > 0 else ""
                growth_str = f" | Nominelles Wachstum: {sign}{growth:.1f}%"

            # Real growth from volume index
            real_growth_str = ""
            if vol_idx is not None and prev and prev["volume_index"] and prev["volume_index"] > 0:
                real_growth = (vol_idx - prev["volume_index"]) / prev["volume_index"] * 100
                sign = "+" if real_growth > 0 else ""
                real_growth_str = f" | Reales Wachstum: {sign}{real_growth:.1f}%"

            # Format value (BIP pro Kopf in EUR, rest in Mrd. EUR)
            if code in ("ESVG2010-46", "ESVG2010-47"):
                value_str = f"{nominal:,.0f} EUR"
                unit = "EUR"
            elif abs(nominal) >= 1000:
                value_str = f"{nominal / 1000:,.1f} Mrd. €"
                unit = "Mrd. EUR"
            else:
                value_str = f"{nominal:,.1f} Mio. €"
                unit = "Mio. EUR"

            title = f"{label} AT {row['year']}: {value_str}{growth_str}{real_growth_str}"

            results.append({
                "title": title,
                "indicator": label,
                "year": str(row["year"]),
                "value": nominal,
                "volume_index": vol_idx,
                "unit": unit,
                "source": "Statistik Austria (VGR, ESA 2010)",
                "url": "https://www.statistik.at/statistiken/volkswirtschaft-und-oeffentliche-finanzen/volkswirtschaftliche-gesamtrechnungen",
                "dataset_id": "OGD_vgr101_VGRJahresR_3",
            })

    return results
