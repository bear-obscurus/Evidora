"""Statistik Austria Open Government Data — österreichische amtliche Statistiken.

Integriert acht Datasets:
1. VPI (Verbraucherpreisindex) — monatliche Inflationsdaten (Basis 2020=100)
2. Gesundheitsausgaben — jährliche Ausgaben nach Leistungsart und Finanzierung (SHA)
3. Sterblichkeit nach Kalenderwoche — wöchentliche Sterbefälle seit 2000 (für Übersterblichkeit)
4. VGR (Volkswirtschaftliche Gesamtrechnung) — jährliche BIP-Daten + 47 Aggregate (ESA 2010)
5. Bevölkerungsbewegung — int. Zu-/Abwanderung nach Bundesland (1961–2024, nur Ist-Daten)
6. Einbürgerungen — jährliche Einbürgerungen nach Geburtsland und Alter (1981–2025)
7. Arbeitsmarkt (Arbeitskräfteerhebung/LFS) — ILO-Arbeitslosenquote + Erwerbstätigenquote (2021–2025)
8. EU-SILC (Armut & Ungleichheit) — AROPE, Armutsgefährdungsquote, Gini, S80/S20 (2021–2024)

Datenquelle: https://data.statistik.gv.at
Lizenz: CC BY 4.0
Kein API-Key erforderlich — statische CSV-Downloads, lokal gecacht.
"""

import csv
import io
import logging
import re
from datetime import datetime

import httpx

logger = logging.getLogger("evidora")

# --- Download URLs ---
VPI_CSV_URL = "https://data.statistik.gv.at/data/OGD_vpi20c18_VPI_2020COICOP18_1.csv"
HEALTH_EXP_CSV_URL = "https://data.statistik.gv.at/data/OGD_gesausgaben01_HVD_HCHF_1.csv"
MORTALITY_CSV_URL = "https://data.statistik.gv.at/data/OGD_gest_kalwo_GEST_KALWOCHE_100.csv"
VGR_CSV_URL = "https://data.statistik.gv.at/data/OGD_vgr101_VGRJahresR_3.csv"
MIGRATION_CSV_URL = "https://data.statistik.gv.at/data/OGD_bevbewegung_BEV_BEW_3.csv"
NATURALIZATIONS_CSV_URL = "https://data.statistik.gv.at/data/OGD_einbuergerungen_EINBJ_1.csv"
ALQUO_CSV_URL = "https://data.statistik.gv.at/data/OGD_ake100_hvd_ogdonly_HVD_ALQUO_1.csv"
ETQUOTE_CSV_URL = "https://data.statistik.gv.at/data/OGD_ake101_hvd_ogdonly_HVD_ETQUOTE_1.csv"
ARMUT_CSV_URL = "https://data.statistik.gv.at/data/OGD_armsilc01_hvd_ogdonly_HVD_ARM_1.csv"
UNGLEICHHEIT_CSV_URL = "https://data.statistik.gv.at/data/OGD_unglsilc02_HVD_UNG_1.csv"

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

_migration_cache: dict | None = None  # {year: {immigration, emigration, net, births, deaths}}
_migration_cache_time: float = 0.0

_naturalizations_cache: dict | None = None  # {year: total}
_naturalizations_cache_time: float = 0.0

_arbeitsmarkt_cache: dict | None = None  # {year: {al, al_m, al_f, al_youth, al_lt, et, tz, al_by_bl}}
_arbeitsmarkt_cache_time: float = 0.0

_armut_cache: dict | None = None  # {year: {arope, arop, gini, s80s20, ...}}
_armut_cache_time: float = 0.0

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


# --- Migration movement type codes ---
BEWART_CODES = {
    "BEWART-1": "births",
    "BEWART-2": "deaths",
    "BEWART-3": "immigration",     # Internationale Zuwanderung
    "BEWART-4": "emigration",      # Internationale Abwanderung
    "BEWART-5": "internal_in",     # Binnenzuwanderung
    "BEWART-6": "internal_out",    # Binnenabwanderung
}

BUNDESLAND_LABELS = {
    "B00-1": "Burgenland", "B00-2": "Kärnten", "B00-3": "Niederösterreich",
    "B00-4": "Oberösterreich", "B00-5": "Salzburg", "B00-6": "Steiermark",
    "B00-7": "Tirol", "B00-8": "Vorarlberg", "B00-9": "Wien",
}

# ALQUO classification codes — key breakdowns we use for fact-checking
ALQUO_KEY_CODES = {
    "AKEQUOT_AL-1":  "Österreich gesamt",
    "AKEQUOT_AL-2":  "Männlich",
    "AKEQUOT_AL-3":  "Weiblich",
    "AKEQUOT_AL-4":  "15–24 Jahre (Jugend)",
    "AKEQUOT_AL-5":  "25–54 Jahre",
    "AKEQUOT_AL-6":  "55–74 Jahre",
    "AKEQUOT_AL-7":  "ISCED 0 (kein Abschluss)",
    "AKEQUOT_AL-8":  "ISCED 1–2 (Pflichtschule)",
    "AKEQUOT_AL-9":  "ISCED 3–4 (Matura/Lehre)",
    "AKEQUOT_AL-10": "ISCED 5–8 (Hochschule)",
}

# Bundesland codes within ALQUO classification (total unemployment)
ALQUO_BUNDESLAND_CODES = {
    "AKEQUOT_AL-11": "Burgenland",
    "AKEQUOT_AL-12": "Niederösterreich",
    "AKEQUOT_AL-13": "Wien",
    "AKEQUOT_AL-14": "Kärnten",
    "AKEQUOT_AL-15": "Steiermark",
    "AKEQUOT_AL-16": "Oberösterreich",
    "AKEQUOT_AL-17": "Salzburg",
    "AKEQUOT_AL-18": "Tirol",
    "AKEQUOT_AL-19": "Vorarlberg",
}

# NUTS 2 × 15–24 Jahre (youth unemployment by Bundesland) — AL-64..72
ALQUO_BUNDESLAND_YOUTH_CODES = {
    "AKEQUOT_AL-64": "Burgenland",
    "AKEQUOT_AL-65": "Niederösterreich",
    "AKEQUOT_AL-66": "Wien",
    "AKEQUOT_AL-67": "Kärnten",
    "AKEQUOT_AL-68": "Steiermark",
    "AKEQUOT_AL-69": "Oberösterreich",
    "AKEQUOT_AL-70": "Salzburg",
    "AKEQUOT_AL-71": "Tirol",
    "AKEQUOT_AL-72": "Vorarlberg",
}

# NUTS 2 × Geschlecht — männlich (AL-46..54) und weiblich (AL-55..63)
ALQUO_BUNDESLAND_MALE_CODES = {
    "AKEQUOT_AL-46": "Burgenland",
    "AKEQUOT_AL-47": "Niederösterreich",
    "AKEQUOT_AL-48": "Wien",
    "AKEQUOT_AL-49": "Kärnten",
    "AKEQUOT_AL-50": "Steiermark",
    "AKEQUOT_AL-51": "Oberösterreich",
    "AKEQUOT_AL-52": "Salzburg",
    "AKEQUOT_AL-53": "Tirol",
    "AKEQUOT_AL-54": "Vorarlberg",
}

ALQUO_BUNDESLAND_FEMALE_CODES = {
    "AKEQUOT_AL-55": "Burgenland",
    "AKEQUOT_AL-56": "Niederösterreich",
    "AKEQUOT_AL-57": "Wien",
    "AKEQUOT_AL-58": "Kärnten",
    "AKEQUOT_AL-59": "Steiermark",
    "AKEQUOT_AL-60": "Oberösterreich",
    "AKEQUOT_AL-61": "Salzburg",
    "AKEQUOT_AL-62": "Tirol",
    "AKEQUOT_AL-63": "Vorarlberg",
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


async def fetch_migration(client: httpx.AsyncClient | None = None) -> dict:
    """Download and parse Bevölkerungsbewegung CSV.

    Aggregates international migration flows to yearly Austria totals.
    Raw CSV: ~79K rows (120 years × 10 states × 6 types × 11 scenarios).
    We only use scenario V1 (Hauptszenario) and sum all states.
    Returns: {year: {immigration, emigration, net, births, deaths, by_state: {state: {immigration, emigration}}}}
    """
    import time
    global _migration_cache, _migration_cache_time

    now = time.time()
    if _migration_cache is not None and (now - _migration_cache_time) < STAT_AT_CACHE_TTL:
        return _migration_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=60.0)
        close_client = True

    try:
        resp = await client.get(MIGRATION_CSV_URL, timeout=60.0)
        resp.raise_for_status()
        text = resp.text

        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        yearly: dict[int, dict] = {}

        for row in reader:
            year_code = row.get("C-A10-0", "")
            state_code = row.get("C-B00-0", "")
            bewart_code = row.get("C-BEWART-0", "")

            # Parse year
            if not year_code.startswith("A10-"):
                continue
            try:
                year = int(year_code[4:])
            except ValueError:
                continue

            # Skip non-classifiable state
            if state_code == "B00-0":
                continue

            # Use only Hauptszenario (F-S25V1)
            count_str = row.get("F-S25V1", "")
            count = int(count_str) if count_str.strip() else 0

            # Detect projection years: V1 != V2 means projection
            v2_str = row.get("F-S25V2", "")
            v2 = int(v2_str) if v2_str.strip() else 0
            is_projection = (count != v2)

            # Skip projection years entirely — only use actual data
            if is_projection:
                continue

            bewart = BEWART_CODES.get(bewart_code)
            if not bewart:
                continue

            if year not in yearly:
                yearly[year] = {
                    "immigration": 0, "emigration": 0, "net": 0,
                    "births": 0, "deaths": 0,
                    "by_state": {},
                }

            yr = yearly[year]

            if bewart == "immigration":
                yr["immigration"] += count
                # Track by state
                state_name = BUNDESLAND_LABELS.get(state_code, state_code)
                if state_name not in yr["by_state"]:
                    yr["by_state"][state_name] = {"immigration": 0, "emigration": 0}
                yr["by_state"][state_name]["immigration"] += count
            elif bewart == "emigration":
                yr["emigration"] += count
                state_name = BUNDESLAND_LABELS.get(state_code, state_code)
                if state_name not in yr["by_state"]:
                    yr["by_state"][state_name] = {"immigration": 0, "emigration": 0}
                yr["by_state"][state_name]["emigration"] += count
            elif bewart == "births":
                yr["births"] += count
            elif bewart == "deaths":
                yr["deaths"] += count

        # Calculate net migration for each year
        for year, yr in yearly.items():
            yr["net"] = yr["immigration"] - yr["emigration"]

        _migration_cache = yearly
        _migration_cache_time = now
        years = sorted(yearly.keys())
        logger.info(f"Statistik Austria Migration: {len(yearly)} Jahre geladen ({years[0] if years else '?'}–{years[-1] if years else '?'})")
        return yearly

    except Exception as e:
        logger.warning(f"Statistik Austria migration download failed: {e}")
        return _migration_cache or {}
    finally:
        if close_client:
            await client.aclose()


async def fetch_naturalizations(client: httpx.AsyncClient | None = None) -> dict:
    """Download and parse Einbürgerungen CSV.

    Aggregates to yearly totals (+ by birth country: AT/foreign).
    Returns: {year: {total, born_austria, born_foreign}}
    """
    import time
    global _naturalizations_cache, _naturalizations_cache_time

    now = time.time()
    if _naturalizations_cache is not None and (now - _naturalizations_cache_time) < STAT_AT_CACHE_TTL:
        return _naturalizations_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    try:
        resp = await client.get(NATURALIZATIONS_CSV_URL)
        resp.raise_for_status()
        text = resp.text

        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        yearly: dict[int, dict] = {}

        for row in reader:
            year_code = row.get("C-BERJ-0", "")
            country_code = row.get("C-GEBLAND_DICHOTOM-0", "")
            count_str = row.get("F-ANZAHL", "")

            # Parse year from BERJ-YYYY
            if not year_code.startswith("BERJ-"):
                continue
            try:
                year = int(year_code[5:])
            except ValueError:
                continue

            count = int(count_str) if count_str.strip() else 0

            if year not in yearly:
                yearly[year] = {"total": 0, "born_austria": 0, "born_foreign": 0}

            yearly[year]["total"] += count
            if country_code == "GEBLAND_DICHOTOM-1":
                yearly[year]["born_austria"] += count
            elif country_code == "GEBLAND_DICHOTOM-2":
                yearly[year]["born_foreign"] += count

        _naturalizations_cache = yearly
        _naturalizations_cache_time = now
        logger.info(f"Statistik Austria Einbürgerungen: {len(yearly)} Jahre geladen")
        return yearly

    except Exception as e:
        logger.warning(f"Statistik Austria naturalizations download failed: {e}")
        return _naturalizations_cache or {}
    finally:
        if close_client:
            await client.aclose()


async def fetch_arbeitsmarkt(client: httpx.AsyncClient | None = None) -> dict:
    """Download and parse ALQUO (unemployment) + ETQUOTE (employment) CSVs.

    Returns: {year: {al, al_m, al_f, al_youth, al_lt, et, tz, al_by_bl, al_by_isced}}
    - al        : Arbeitslosenquote Österreich gesamt (ILO), %
    - al_m/f    : nach Geschlecht, %
    - al_youth  : 15–24-Jährige, %
    - al_lt     : Langzeitarbeitslosenquote, %
    - et        : Erwerbstätigenquote 20–64 J., %
    - tz        : Teilzeitquote 20–64 J., %
    - al_by_bl  : {Bundesland-Name: quote, %}
    - al_by_isced: {ISCED-Label: quote, %}
    Only annual data (no quarters). Data range: 2021–2025.
    """
    import time
    global _arbeitsmarkt_cache, _arbeitsmarkt_cache_time

    now = time.time()
    if _arbeitsmarkt_cache is not None and (now - _arbeitsmarkt_cache_time) < STAT_AT_CACHE_TTL:
        return _arbeitsmarkt_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    yearly: dict[int, dict] = {}

    def _get_year_annual(time_code: str) -> int | None:
        """Parse AKEQUOT_ZEIT-YYYY → year (annual only, skip quarterly YYYYQ)."""
        # time_code like "AKEQUOT_ZEIT-2024" (annual) or "AKEQUOT_ZEIT-20241" (quarterly)
        suffix = time_code.rsplit("-", 1)[-1]
        if len(suffix) == 4:  # annual
            try:
                return int(suffix)
            except ValueError:
                return None
        return None  # quarterly → skip

    try:
        # --- ALQUO: Arbeitslosenquote ---
        resp = await client.get(ALQUO_CSV_URL)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text), delimiter=";")

        for row in reader:
            year = _get_year_annual(row.get("C-AKEQUOT_ZEIT-0", ""))
            if year is None:
                continue

            al_code = row.get("C-AKEQUOT_AL-0", "")
            al_val = _parse_decimal(row.get("F-AKEQUOT_AL", ""))
            lt_val = _parse_decimal(row.get("F-AKEQUOT_LAL", ""))

            # Skip helper/category rows (codes >= AL-900)
            try:
                code_num = int(al_code.split("-")[-1])
                if code_num >= 900:
                    continue
            except (ValueError, IndexError):
                continue

            if year not in yearly:
                yearly[year] = {
                    "al": None, "al_m": None, "al_f": None,
                    "al_youth": None, "al_lt": None,
                    "et": None, "tz": None,
                    "al_by_bl": {}, "al_by_bl_youth": {},
                    "al_by_bl_m": {}, "al_by_bl_f": {},
                    "al_by_isced": {},
                }

            yr = yearly[year]

            if al_code == "AKEQUOT_AL-1" and al_val is not None:
                yr["al"] = al_val
            if al_code == "AKEQUOT_AL-1" and lt_val is not None:
                yr["al_lt"] = lt_val
            elif al_code == "AKEQUOT_AL-2" and al_val is not None:
                yr["al_m"] = al_val
            elif al_code == "AKEQUOT_AL-3" and al_val is not None:
                yr["al_f"] = al_val
            elif al_code == "AKEQUOT_AL-4" and al_val is not None:
                yr["al_youth"] = al_val

            # By Bundesland (total)
            if al_code in ALQUO_BUNDESLAND_CODES and al_val is not None:
                bl_name = ALQUO_BUNDESLAND_CODES[al_code]
                yr["al_by_bl"][bl_name] = al_val

            # By Bundesland × Jugend (15–24 Jahre)
            if al_code in ALQUO_BUNDESLAND_YOUTH_CODES and al_val is not None:
                bl_name = ALQUO_BUNDESLAND_YOUTH_CODES[al_code]
                yr["al_by_bl_youth"][bl_name] = al_val

            # By Bundesland × Geschlecht
            if al_code in ALQUO_BUNDESLAND_MALE_CODES and al_val is not None:
                bl_name = ALQUO_BUNDESLAND_MALE_CODES[al_code]
                yr["al_by_bl_m"][bl_name] = al_val
            if al_code in ALQUO_BUNDESLAND_FEMALE_CODES and al_val is not None:
                bl_name = ALQUO_BUNDESLAND_FEMALE_CODES[al_code]
                yr["al_by_bl_f"][bl_name] = al_val

            # By ISCED (education level)
            isced_map = {
                "AKEQUOT_AL-7": "ISCED 0 (kein Abschluss)",
                "AKEQUOT_AL-8": "ISCED 1–2 (Pflichtschule)",
                "AKEQUOT_AL-9": "ISCED 3–4 (Matura/Lehre)",
                "AKEQUOT_AL-10": "ISCED 5–8 (Hochschule)",
            }
            if al_code in isced_map and al_val is not None:
                yr["al_by_isced"][isced_map[al_code]] = al_val

        # --- ETQUOTE: Erwerbstätigenquote ---
        resp2 = await client.get(ETQUOTE_CSV_URL)
        resp2.raise_for_status()
        reader2 = csv.DictReader(io.StringIO(resp2.text), delimiter=";")

        for row in reader2:
            year = _get_year_annual(row.get("C-AKEQUOT_ZEIT-0", ""))
            if year is None or year not in yearly:
                continue

            et_code = row.get("C-AKEQUOT_ET-0", "")
            # ET-1 = Österreich gesamt (same pattern as ALQUO)
            if et_code != "AKEQUOT_ET-1":
                continue

            et_val = _parse_decimal(row.get("F-AKEQUOT_ET", ""))
            tz_val = _parse_decimal(row.get("F-AKEQUOT_TZ", ""))

            if et_val is not None:
                yearly[year]["et"] = et_val
            if tz_val is not None:
                yearly[year]["tz"] = tz_val

        _arbeitsmarkt_cache = yearly
        _arbeitsmarkt_cache_time = now
        logger.info(f"Statistik Austria Arbeitsmarkt: {len(yearly)} Jahre geladen")
        return yearly

    except Exception as e:
        logger.warning(f"Statistik Austria Arbeitsmarkt download failed: {e}")
        return _arbeitsmarkt_cache or {}
    finally:
        if close_client:
            await client.aclose()


async def fetch_armut(client: httpx.AsyncClient | None = None) -> dict:
    """Download and parse EU-SILC poverty (ARMUT) + inequality (UNGLEICHHEIT) CSVs.

    Returns: {year: {arope, arop, depriv, low_work, gini, s80s20,
                     arop_m, arop_f,
                     arop_child, arop_working_age, arop_elderly,
                     arop_austrian, arop_non_austrian, arop_non_eu,
                     arop_employed, arop_unemployed, arop_retired,
                     arop_by_bl, arop_by_isced}}
    - arope    : AROPE-Quote (Armut ODER soziale Ausgrenzung), %
    - arop     : Armutsgefährdungsquote (Einkommens-Schwelle 60% Median), %
    - depriv   : Erhebliche materielle und soziale Deprivation, %
    - low_work : Haushalt mit sehr niedriger Erwerbsintensität, %
    - gini     : Gini-Koeffizient des verfügbaren Äquivalenzeinkommens (0–100)
    - s80s20   : S80/S20-Einkommensquintilverhältnis
    Data range: 2021–2024 (EU-SILC jährlich, Referenzjahr t-1).
    """
    import time
    global _armut_cache, _armut_cache_time

    now = time.time()
    if _armut_cache is not None and (now - _armut_cache_time) < STAT_AT_CACHE_TTL:
        return _armut_cache

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    yearly: dict[int, dict] = {}

    def _get_year(time_code: str) -> int | None:
        """Parse ARMZEIT-YYYY or UNGZEIT-YYYY → year."""
        suffix = time_code.rsplit("-", 1)[-1]
        try:
            return int(suffix) if len(suffix) == 4 else None
        except ValueError:
            return None

    def _init_year(yr_dict: dict, year: int) -> None:
        if year not in yr_dict:
            yr_dict[year] = {
                "arope": None, "arop": None, "depriv": None, "low_work": None,
                "gini": None, "s80s20": None,
                "arop_m": None, "arop_f": None,
                "arop_child": None, "arop_working_age": None, "arop_elderly": None,
                "arop_austrian": None, "arop_non_austrian": None, "arop_non_eu": None,
                "arop_employed": None, "arop_unemployed": None, "arop_retired": None,
                "arop_by_bl": {}, "arop_by_isced": {},
            }

    # Bundesland codes
    arm_bl = {
        "ARM01-2": "Burgenland", "ARM01-3": "Kärnten",
        "ARM01-4": "Niederösterreich", "ARM01-5": "Oberösterreich",
        "ARM01-6": "Salzburg", "ARM01-7": "Steiermark",
        "ARM01-8": "Tirol", "ARM01-9": "Vorarlberg", "ARM01-10": "Wien",
    }
    arm_isced = {
        "ARM01-22": "ISCED 0",
        "ARM01-23": "ISCED 1–2 (Pflichtschule)",
        "ARM01-24": "ISCED 3–4 (Matura/Lehre)",
        "ARM01-25": "ISCED 5–8 (Hochschule)",
    }

    try:
        # --- Armut: AROPE, AROP, Deprivation, Low Work Intensity ---
        resp = await client.get(ARMUT_CSV_URL)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text), delimiter=";")

        for row in reader:
            year = _get_year(row.get("C-ARMZEIT-0", ""))
            if year is None:
                continue
            code = row.get("C-ARM01-0", "")
            arope = _parse_decimal(row.get("F-ARMUT_ANTE", ""))
            arop  = _parse_decimal(row.get("F-ARMUT_QUO", ""))
            depriv = _parse_decimal(row.get("F-DEPRIV", ""))
            low_w  = _parse_decimal(row.get("F-PERSO", ""))

            _init_year(yearly, year)
            yr = yearly[year]

            if code == "ARM01-1":
                if arope is not None: yr["arope"] = arope
                if arop  is not None: yr["arop"]  = arop
                if depriv is not None: yr["depriv"] = depriv
                if low_w  is not None: yr["low_work"] = low_w
            elif code == "ARM01-11" and arop is not None: yr["arop_m"] = arop
            elif code == "ARM01-12" and arop is not None: yr["arop_f"] = arop
            elif code == "ARM01-13" and arop is not None: yr["arop_child"] = arop
            elif code == "ARM01-16" and arop is not None: yr["arop_working_age"] = arop
            elif code == "ARM01-19" and arop is not None: yr["arop_elderly"] = arop
            elif code == "ARM01-26" and arop is not None: yr["arop_austrian"] = arop
            elif code == "ARM01-27" and arop is not None: yr["arop_non_austrian"] = arop
            elif code == "ARM01-29" and arop is not None: yr["arop_non_eu"] = arop
            elif code == "ARM01-34" and arop is not None: yr["arop_employed"] = arop
            elif code == "ARM01-38" and arop is not None: yr["arop_unemployed"] = arop
            elif code == "ARM01-39" and arop is not None: yr["arop_retired"] = arop
            elif code in arm_bl and arop is not None:
                yr["arop_by_bl"][arm_bl[code]] = arop
            elif code in arm_isced and arop is not None:
                yr["arop_by_isced"][arm_isced[code]] = arop

        # --- Ungleichheit: Gini + S80/S20 ---
        resp2 = await client.get(UNGLEICHHEIT_CSV_URL)
        resp2.raise_for_status()
        reader2 = csv.DictReader(io.StringIO(resp2.text), delimiter=";")

        for row in reader2:
            year = _get_year(row.get("C-UNGZEIT-0", ""))
            if year is None or year not in yearly:
                continue
            code = row.get("C-UNG01-0", "")
            if code != "UNG01-1":
                continue
            s80s20 = _parse_decimal(row.get("F-UNG_EKQU", ""))
            gini   = _parse_decimal(row.get("F-UNG_GINI", ""))
            if s80s20 is not None: yearly[year]["s80s20"] = s80s20
            if gini   is not None: yearly[year]["gini"]   = gini

        _armut_cache = yearly
        _armut_cache_time = now
        logger.info(f"Statistik Austria EU-SILC: {len(yearly)} Jahre geladen")
        return yearly

    except Exception as e:
        logger.warning(f"Statistik Austria EU-SILC download failed: {e}")
        return _armut_cache or {}
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

MIGRATION_KEYWORDS = [
    "zuwanderung", "abwanderung", "einwanderung", "auswanderung",
    "migration", "immigration", "emigration", "migranten", "migrants",
    "zuwanderer", "einwanderer", "bevölkerungsbewegung",
    "wanderung", "wanderungssaldo", "nettozuwanderung",
    "bevölkerungswachstum", "population growth",
    "ausländer", "foreigners", "geburten", "births",
    "geburtenrate", "birth rate", "fertilität",
]

NATURALIZATION_KEYWORDS = [
    "einbürgerung", "naturalization", "staatsbürgerschaft",
    "citizenship", "eingebürgert", "naturalized",
    "pass", "passport", "staatsangehörigkeit",
]

ARMUT_KEYWORDS = [
    "armut", "poverty", "armutsgefährdung", "armutsgefährdet",
    "at risk of poverty", "arope",
    "soziale ausgrenzung", "social exclusion",
    "materielle deprivation", "material deprivation",
    "einkommensungleichheit", "income inequality",
    "gini", "einkommensverteilung", "income distribution",
    "einkommensschere", "lohnschere",
    "ungleichheit", "inequality",
    "einkommensquintil", "s80/s20", "quintilverhältnis",
    "wohlstandsverteilung", "reichtum verteilung",
    "kinderarmut", "child poverty",
    "altersarmut", "pensionsarmut",
    "niedrigeinkommen", "niedriglohn", "low income",
]

ARBEITSMARKT_KEYWORDS = [
    "arbeitslosigkeit", "arbeitslos", "unemployment", "jobless",
    "arbeitslosenquote", "unemployment rate",
    "beschäftigung", "beschäftigte", "beschäftigungsquote",
    "employment", "employment rate", "erwerbstätigkeit", "erwerbstätigenquote",
    "erwerbsquote", "erwerbstätige",
    "jugendarbeitslosigkeit", "youth unemployment",
    "langzeitarbeitslosigkeit", "long-term unemployment", "langzeitarbeitslos",
    "teilzeit", "part-time", "teilzeitquote",
    "arbeitsmarkt", "labor market", "labour market",
    "arbeitssuchende", "arbeitssuchend", "stellensuchende",
    "ams", "jobverlust", "stellenabbau",
]

VGR_KEYWORDS = [
    "bip", "gdp", "bruttoinlandsprodukt", "gross domestic product",
    "wirtschaftsleistung", "economic output", "wirtschaftswachstum",
    "economic growth", "rezession", "recession", "konjunktur",
    "wirtschaft", "economy", "wachstum", "growth",
    "gewachsen", "geschrumpft", "schrumpf", "grew", "shrunk",
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

    # Migration / population movement data
    if _match_keywords(search_text, MIGRATION_KEYWORDS):
        migration_results = await _search_migration(search_text)
        results.extend(migration_results)

    # Naturalization data
    if _match_keywords(search_text, NATURALIZATION_KEYWORDS):
        nat_results = await _search_naturalizations(search_text)
        results.extend(nat_results)

    # Labor market / unemployment data
    if _match_keywords(search_text, ARBEITSMARKT_KEYWORDS):
        am_results = await _search_arbeitsmarkt(search_text)
        # Bug 16-V2: Der AMS-vs-ILO-Methodologie-Caveat MUSS an Position 0
        # in der Gesamtliste stehen, damit der Synthesizer ihn vor den
        # ILO-Werten liest und nicht überstimmt. Wir spalten ihn ab und
        # prependen ihn separat.
        ams_caveat = [r for r in am_results
                      if r.get("indicator") == "Methodologie-Vergleich AMS-vs-ILO"]
        am_other = [r for r in am_results
                    if r.get("indicator") != "Methodologie-Vergleich AMS-vs-ILO"]
        results = ams_caveat + results + am_other

    # Poverty and inequality data (EU-SILC)
    if _match_keywords(search_text, ARMUT_KEYWORDS):
        armut_results = await _search_armut(search_text)
        results.extend(armut_results)

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
                "Nominelle Werte sind inflationsbereinigt (Volumenindex) vergleichbarer. "
                "(6) Migrationsdaten zeigen internationale Zu-/Abwanderung, NICHT Asyl "
                "oder Aufenthaltstitel — dafür sind BMI-Daten nötig. "
                "Nettomigration = Zuwanderung minus Abwanderung. "
                "(7) Einbürgerungen ≠ Zuwanderung — viele Eingebürgerte leben seit "
                "Jahren in Österreich. "
                "(8) Arbeitslosenquote nach ILO-Methodik (Arbeitskräfteerhebung/LFS): "
                "Nur Personen, die aktiv suchen und sofort verfügbar sind. "
                "Die AMS-Arbeitslosenquote (nationale Methodik) ist höher, da sie auch "
                "Schulungsteilnehmende zählt. Daten nur ab 2021 verfügbar. "
                "(9) EU-SILC-Armutsdaten: AROPE = Armut ODER soziale Ausgrenzung (breiteste Definition). "
                "Armutsgefährdungsquote = Einkommen unter 60% des Medians. "
                "Gini-Koeffizient: 0 = völlige Gleichverteilung, 100 = maximale Ungleichheit. "
                "Referenzjahr ist t-1 (EU-SILC 2024 misst Einkommen 2023)."
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


def _extract_years_from_text(text: str) -> list[int]:
    """Extract 4-digit years (1995–2030) mentioned in claim text."""
    matches = re.findall(r"\b(19[9]\d|20[0-3]\d)\b", text)
    return sorted(set(int(y) for y in matches))


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

    # Extract years mentioned in claim (e.g., "seit 2019")
    mentioned_years = _extract_years_from_text(search_text)

    for code in target_codes:
        rows = [r for r in data if r["indicator_code"] == code]
        rows.sort(key=lambda r: r["year"], reverse=True)

        # Collect target years: latest 5 + any years mentioned in claim
        latest_5 = [r["year"] for r in rows[:5]]
        extra_years = [y for y in mentioned_years if y not in latest_5]
        target_years = set(latest_5) | set(extra_years)

        # Filter to target years, keep sorted descending
        filtered_rows = [r for r in rows if r["year"] in target_years]

        for row in filtered_rows:
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


async def _search_migration(search_text: str) -> list[dict]:
    """Search migration data for immigration/emigration flows."""
    data = await fetch_migration()
    if not data:
        return []

    results = []
    current_year = datetime.now().year

    # Extract mentioned years and determine range
    mentioned_years = _extract_years_from_text(search_text)

    # Get recent years (up to current year, skip future projections)
    all_years = sorted(
        (y for y in data.keys() if y <= current_year),
        reverse=True,
    )
    latest_5 = all_years[:5]
    extra_years = [y for y in mentioned_years if y not in latest_5 and y in data]
    target_years = sorted(set(latest_5) | set(extra_years), reverse=True)

    for year in target_years:
        yr = data[year]
        imm = yr["immigration"]
        emi = yr["emigration"]
        net = yr["net"]

        if imm == 0 and emi == 0:
            continue

        sign = "+" if net > 0 else ""
        parts = [
            f"Zuwanderung: {imm:,}",
            f"Abwanderung: {emi:,}",
            f"Saldo: {sign}{net:,}",
        ]

        # Top 3 states by immigration
        by_state = yr.get("by_state", {})
        if by_state:
            top_states = sorted(
                by_state.items(),
                key=lambda x: x[1]["immigration"],
                reverse=True,
            )[:3]
            state_strs = [f"{s}: {d['immigration']:,}" for s, d in top_states]
            parts.append(f"Top: {', '.join(state_strs)}")

        detail = " | ".join(parts)

        results.append({
            "title": f"Migration AT {year}: {detail}",
            "indicator": "Internationale Migration",
            "year": str(year),
            "immigration": imm,
            "emigration": emi,
            "net_migration": net,
            "unit": "Personen",
            "source": "Statistik Austria",
            "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/bevoelkerung/wanderungen",
            "dataset_id": "OGD_bevbewegung_BEV_BEW_3",
        })

    # Add births/deaths context for recent year if available
    if all_years:
        latest = all_years[0]
        yr = data[latest]
        if yr["births"] > 0 or yr["deaths"] > 0:
            nat_change = yr["births"] - yr["deaths"]
            sign = "+" if nat_change > 0 else ""
            results.append({
                "title": f"Natürliche Bevölkerungsbewegung AT {latest}: Geburten {yr['births']:,} | Sterbefälle {yr['deaths']:,} | Saldo: {sign}{nat_change:,}",
                "indicator": "Natürliche Bevölkerungsbewegung",
                "year": str(latest),
                "value": nat_change,
                "unit": "Personen",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/bevoelkerung/wanderungen",
                "dataset_id": "OGD_bevbewegung_BEV_BEW_3",
            })

    return results


async def _search_naturalizations(search_text: str) -> list[dict]:
    """Search naturalization data."""
    data = await fetch_naturalizations()
    if not data:
        return []

    results = []

    # Extract mentioned years
    mentioned_years = _extract_years_from_text(search_text)

    all_years = sorted(data.keys(), reverse=True)
    latest_5 = all_years[:5]
    extra_years = [y for y in mentioned_years if y not in latest_5 and y in data]
    target_years = sorted(set(latest_5) | set(extra_years), reverse=True)

    for year in target_years:
        yr = data[year]
        total = yr["total"]
        born_at = yr["born_austria"]
        born_foreign = yr["born_foreign"]

        if total == 0:
            continue

        foreign_pct = (born_foreign / total * 100) if total > 0 else 0

        parts = [f"Gesamt: {total:,}"]
        if born_foreign > 0:
            parts.append(f"im Ausland geboren: {born_foreign:,} ({foreign_pct:.0f}%)")
        if born_at > 0:
            parts.append(f"in AT geboren: {born_at:,}")

        detail = " | ".join(parts)

        results.append({
            "title": f"Einbürgerungen AT {year}: {detail}",
            "indicator": "Einbürgerungen",
            "year": str(year),
            "value": total,
            "unit": "Personen",
            "source": "Statistik Austria",
            "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/bevoelkerung/einbuergerungen",
            "dataset_id": "OGD_einbuergerungen_EINBJ_1",
        })

    return results


def _extract_pct_from_text(text: str) -> float | None:
    """Best-effort: pull the largest plausible percentage value from
    the claim text.  Returns None if no number found."""
    import re as _re
    # 7,5 % or 7.5% or 7,5 Prozent or 7.5 percent
    matches = _re.findall(
        r"(\d{1,2}(?:[,.]\d+)?)\s*(?:%|prozent|percent)",
        text.lower()
    )
    nums: list[float] = []
    for m in matches:
        try:
            nums.append(float(m.replace(",", ".")))
        except ValueError:
            continue
    if not nums:
        return None
    # Keep only plausible unemployment-quote values (1–25 %)
    plausible = [n for n in nums if 1.0 <= n <= 25.0]
    return max(plausible) if plausible else None


async def _search_arbeitsmarkt(search_text: str) -> list[dict]:
    """Search labor market data (unemployment + employment rates).

    Bug 16: AT-Arbeitslosigkeits-Claims zitieren oft die AMS-
    Methodik (registrierte AL inkl. Schulungsteilnehmer) — diese
    Quote liegt typischerweise 1.5–2 PP über der ILO-LFS-Quote
    von Statistik Austria.  Damit der Synthesizer einen AMS-
    konformen Wert nicht reflexhaft gegen ILO-Daten mit „false"
    bewertet, prependen wir bei AT-Arbeitslosigkeits-Claims mit
    einer konkreten Prozentzahl einen STRONG-WORDED Methodologie-
    Vergleich an die Spitze der Ergebnisliste.
    """
    data = await fetch_arbeitsmarkt()
    if not data:
        return []

    results = []
    text_lower = search_text.lower()

    # Detect claim focus
    want_youth = any(kw in text_lower for kw in [
        "jugend", "jung", "junge", "15-24", "under 25", "youngster",
        "jugendarbeitslosigkeit", "youth unemployment",
    ])
    want_gender = any(kw in text_lower for kw in [
        "frauen", "männer", "geschlecht", "gender", "weiblich", "männlich",
        "female", "male", "woman", "man",
    ])
    want_lt = any(kw in text_lower for kw in [
        "langzeit", "long-term", "langzeitarbeitslos",
    ])
    want_regional = any(kw in text_lower for kw in [
        "bundesland", "wien", "graz", "steiermark", "kärnten", "salzburg",
        "tirol", "vorarlberg", "burgenland", "niederösterreich", "oberösterreich",
        "regional", "bundesländer",
    ])
    want_education = any(kw in text_lower for kw in [
        "bildung", "akademiker", "isced", "matura", "hochschule",
        "university", "studium", "pflichtschule", "education",
    ])
    want_employment = any(kw in text_lower for kw in [
        "erwerbstätigkeit", "erwerbstätig", "beschäftigung", "beschäftigungsquote",
        "employment rate", "erwerbstätigenquote", "teilzeit", "part-time",
    ])

    # --- Main: national unemployment rate trend ---
    all_years = sorted(data.keys(), reverse=True)
    mentioned_years = _extract_years_from_text(search_text)
    extra_years = [y for y in mentioned_years if y not in all_years[:5] and y in data]
    target_years = sorted(set(all_years[:5]) | set(extra_years), reverse=True)

    for year in target_years:
        yr = data[year]
        al = yr.get("al")
        if al is None:
            continue

        parts = [f"{al:.1f}%"]

        al_lt = yr.get("al_lt")
        if al_lt is not None:
            parts.append(f"Langzeitarbeitslos: {al_lt:.1f}%")

        detail = " | ".join(parts)
        results.append({
            "title": f"Arbeitslosenquote AT {year} (ILO): {detail}",
            "indicator": "Arbeitslosenquote (ILO)",
            "year": str(year),
            "value": al,
            "unit": "%",
            "source": "Statistik Austria",
            "url": "https://www.statistik.at/statistiken/arbeitsmarkt/arbeitslosigkeit",
            "dataset_id": "OGD_ake100_hvd_ogdonly_HVD_ALQUO_1",
        })

    if not results:
        return []

    latest_year = all_years[0]
    yr_latest = data[latest_year]

    # --- Youth unemployment ---
    if want_youth:
        al_youth = yr_latest.get("al_youth")
        if al_youth is not None:
            results.append({
                "title": f"Jugendarbeitslosenquote AT {latest_year} (15–24 J., ILO): {al_youth:.1f}%",
                "indicator": "Jugendarbeitslosenquote (ILO)",
                "year": str(latest_year),
                "value": al_youth,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/arbeitsmarkt/arbeitslosigkeit",
                "dataset_id": "OGD_ake100_hvd_ogdonly_HVD_ALQUO_1",
            })

        # Youth unemployment by Bundesland (always show with youth claims — key for regional comparisons)
        al_by_bl_youth = yr_latest.get("al_by_bl_youth", {})
        if al_by_bl_youth:
            sorted_bl = sorted(al_by_bl_youth.items(), key=lambda x: x[1], reverse=True)
            # Filter out entries with statistically unreliable values (small cells return None above)
            bl_parts = [f"{name}: {rate:.1f}%" for name, rate in sorted_bl]
            results.append({
                "title": f"Jugendarbeitslosenquote nach Bundesland AT {latest_year} (15–24 J.): " + " | ".join(bl_parts),
                "indicator": "Jugendarbeitslosenquote nach Bundesland (ILO)",
                "year": str(latest_year),
                "value": al_by_bl_youth,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/arbeitsmarkt/arbeitslosigkeit",
                "dataset_id": "OGD_ake100_hvd_ogdonly_HVD_ALQUO_1",
            })

    # --- Gender breakdown ---
    if want_gender:
        al_m = yr_latest.get("al_m")
        al_f = yr_latest.get("al_f")
        if al_m is not None and al_f is not None:
            results.append({
                "title": f"Arbeitslosenquote nach Geschlecht AT {latest_year}: Männer {al_m:.1f}% | Frauen {al_f:.1f}%",
                "indicator": "Arbeitslosenquote nach Geschlecht (ILO)",
                "year": str(latest_year),
                "value": f"M: {al_m:.1f}%, F: {al_f:.1f}%",
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/arbeitsmarkt/arbeitslosigkeit",
                "dataset_id": "OGD_ake100_hvd_ogdonly_HVD_ALQUO_1",
            })

        # Gender × Bundesland (z.B. "Frauen in Wien öfter arbeitslos")
        al_by_bl_m = yr_latest.get("al_by_bl_m", {})
        al_by_bl_f = yr_latest.get("al_by_bl_f", {})
        if al_by_bl_m and al_by_bl_f:
            all_bl = sorted(set(al_by_bl_m) | set(al_by_bl_f))
            bl_parts = []
            for bl in sorted(all_bl):
                m_val = al_by_bl_m.get(bl)
                f_val = al_by_bl_f.get(bl)
                if m_val is not None and f_val is not None:
                    bl_parts.append(f"{bl}: M {m_val:.1f}% / F {f_val:.1f}%")
            if bl_parts:
                results.append({
                    "title": f"Arbeitslosenquote nach Bundesland & Geschlecht AT {latest_year}: " + " | ".join(bl_parts),
                    "indicator": "Arbeitslosenquote nach Bundesland und Geschlecht (ILO)",
                    "year": str(latest_year),
                    "value": {"männlich": al_by_bl_m, "weiblich": al_by_bl_f},
                    "unit": "%",
                    "source": "Statistik Austria",
                    "url": "https://www.statistik.at/statistiken/arbeitsmarkt/arbeitslosigkeit",
                    "dataset_id": "OGD_ake100_hvd_ogdonly_HVD_ALQUO_1",
                })

    # --- Regional breakdown (Bundesland) ---
    if want_regional:
        al_by_bl = yr_latest.get("al_by_bl", {})
        if al_by_bl:
            sorted_bl = sorted(al_by_bl.items(), key=lambda x: x[1], reverse=True)
            bl_parts = [f"{name}: {rate:.1f}%" for name, rate in sorted_bl]
            results.append({
                "title": f"Arbeitslosenquote nach Bundesland AT {latest_year}: " + " | ".join(bl_parts),
                "indicator": "Arbeitslosenquote nach Bundesland (ILO)",
                "year": str(latest_year),
                "value": al_by_bl,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/arbeitsmarkt/arbeitslosigkeit",
                "dataset_id": "OGD_ake100_hvd_ogdonly_HVD_ALQUO_1",
            })

    # --- Education breakdown ---
    if want_education:
        al_by_isced = yr_latest.get("al_by_isced", {})
        if al_by_isced:
            isced_parts = [f"{label}: {rate:.1f}%" for label, rate in sorted(al_by_isced.items())]
            results.append({
                "title": f"Arbeitslosenquote nach Bildung AT {latest_year}: " + " | ".join(isced_parts),
                "indicator": "Arbeitslosenquote nach Bildungsniveau (ILO)",
                "year": str(latest_year),
                "value": al_by_isced,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/arbeitsmarkt/arbeitslosigkeit",
                "dataset_id": "OGD_ake100_hvd_ogdonly_HVD_ALQUO_1",
            })

    # --- Employment rate (always if triggered, or if explicitly asked) ---
    if want_employment or True:  # include employment rate as standard context
        et = yr_latest.get("et")
        tz = yr_latest.get("tz")
        if et is not None:
            parts = [f"Erwerbstätigenquote: {et:.1f}%"]
            if tz is not None:
                parts.append(f"Teilzeitquote: {tz:.1f}%")
            results.append({
                "title": f"Erwerbsbeteiligung AT {latest_year} (20–64 J.): " + " | ".join(parts),
                "indicator": "Erwerbstätigenquote + Teilzeitquote (ILO)",
                "year": str(latest_year),
                "value": et,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/arbeitsmarkt/erwerbstaetigkeit",
                "dataset_id": "OGD_ake101_hvd_ogdonly_HVD_ETQUOTE_1",
            })

    # Bug 16: AMS-vs-ILO Methodologie-Caveat — als ERSTER Eintrag
    # prepend, weil der Synthesizer sonst die Krone-AMS-Zahl gegen
    # den niedrigeren ILO-Wert vergleicht und reflexhaft „false"
    # sagt.  Wenn der Claim eine konkrete Prozentzahl enthält UND
    # diese im AMS-Bereich liegt (≥ 6 %), liefern wir explizit den
    # Methodologie-Vergleich + die DECISION RULE für den Synthesizer.
    claim_pct = _extract_pct_from_text(search_text)
    latest_ilo = data.get(latest_year, {}).get("al")
    # AMS-Quoten 2024–2026 nach BMAW/AMS-Bilanz (jährlich aktualisierbar):
    ams_known = {
        2024: 6.7,   # AMS-Jahresquote 2024
        2025: 7.0,   # vorläufige AMS-Jahresquote 2025
        2026: 7.5,   # AMS-Schätzung März 2026 (Stand zum Build)
    }
    ams_latest_year = max(ams_known.keys())
    ams_latest = ams_known[ams_latest_year]

    is_ams_range = (claim_pct is not None and claim_pct >= 6.0 and claim_pct <= 12.0)
    is_ilo_range = (claim_pct is not None and 3.0 <= claim_pct < 6.0)

    if claim_pct is not None and latest_ilo is not None and (is_ams_range or is_ilo_range):
        # Strong-worded methodology comparison entry
        rule_hint = (
            "DECISION RULE für die Synthese: "
            f"Der im Claim genannte Wert ({claim_pct:.1f} %) liegt "
        )
        if is_ams_range:
            rule_hint += (
                f"deutlich über der ILO-Quote ({latest_ilo:.1f} %) und entspricht "
                f"der AMS-Methodik ({ams_latest} % im Stand {ams_latest_year}). "
                "Markiere die Behauptung NICHT als 'falsch', "
                "nur weil der ILO-Wert niedriger ist — die AMS-Methodik ist die "
                "in österreichischen Medien (incl. Krone) übliche Lesart. "
                "Wenn die Zahl +/- 0.5 PP von der AMS-Quote abweicht: "
                "verdict = 'true' oder 'mostly_true'. "
                "Wenn sie +/- 1 PP abweicht: verdict = 'mostly_true' mit Hinweis "
                "auf die Methodik. Erst bei größeren Abweichungen: 'mostly_false'."
            )
        else:
            rule_hint += (
                f"im ILO-Bereich (Statistik Austria: {latest_ilo:.1f} %). "
                "Vergleiche direkt mit der ILO-Quote für 'true/false'-Verdict. "
                "Die AMS-Quote (national) liegt mit "
                f"{ams_latest} % deutlich höher und ist eine andere Methodik."
            )
        results.insert(0, {
            "title": (
                "AMS-vs-ILO-Methodik — KRITISCHER METHODOLOGIE-HINWEIS"
            ),
            "indicator": "Methodologie-Vergleich AMS-vs-ILO",
            "year": str(ams_latest_year),
            "value": "",
            "url": "https://www.ams.at/arbeitsmarktdaten-und-medien/arbeitsmarkt-daten",
            "source": "Statistik Austria + AMS",
            "description": (
                f"Österreich hat ZWEI parallele Arbeitslosenquoten:\n"
                f"• ILO-Quote (Statistik Austria, EU-vergleichbar): "
                f"{latest_ilo:.1f} % im Jahr {latest_year}.\n"
                f"• AMS-Quote (nationale Methodik, registrierte AL + "
                f"Schulungsteilnehmer): {ams_latest} % im Stand {ams_latest_year}.\n"
                f"Die AMS-Quote ist systematisch ~1.5–2 PP höher und wird "
                f"in österreichischen Medien (Krone, Heute, ORF) "
                f"deutlich häufiger zitiert als die ILO-Quote.\n"
                f"{rule_hint}"
            ),
        })

    return results


async def _search_armut(search_text: str) -> list[dict]:
    """Search EU-SILC poverty and inequality data."""
    data = await fetch_armut()
    if not data:
        return []

    results = []
    text_lower = search_text.lower()

    want_child     = any(kw in text_lower for kw in ["kind", "kinder", "jugend", "child", "kinderarmut", "unter 18"])
    want_elderly   = any(kw in text_lower for kw in ["pension", "ältere", "senior", "65", "altersarmut", "rente"])
    want_gender    = any(kw in text_lower for kw in ["frauen", "männer", "frau", "mann", "gender", "weiblich", "männlich"])
    want_citizen   = any(kw in text_lower for kw in ["ausländer", "migrant", "staatsbürgerschaft", "citizenship",
                                                      "nicht-österreichisch", "nichtösterreichisch", "eu-bürger",
                                                      "geburtsland", "herkunft", "zuwanderer"])
    want_employ    = any(kw in text_lower for kw in ["arbeitslose", "erwerbstätige", "beschäftigt", "pension",
                                                     "employed", "unemployed", "retired"])
    want_education = any(kw in text_lower for kw in ["bildung", "akademiker", "isced", "matura", "hochschule",
                                                     "pflichtschule", "education"])
    want_regional  = any(kw in text_lower for kw in ["bundesland", "wien", "tirol", "salzburg", "steiermark",
                                                     "kärnten", "burgenland", "vorarlberg", "oberösterreich",
                                                     "niederösterreich", "regional"])
    want_gini      = any(kw in text_lower for kw in ["gini", "ungleichheit", "inequality", "einkommensverteilung",
                                                     "einkommensschere", "s80", "quintil"])

    all_years = sorted(data.keys(), reverse=True)
    mentioned_years = _extract_years_from_text(search_text)
    extra_years = [y for y in mentioned_years if y not in all_years[:4] and y in data]
    target_years = sorted(set(all_years[:4]) | set(extra_years), reverse=True)

    # --- Nationaler Trend: AROPE + Armutsgefährdungsquote ---
    for year in target_years:
        yr = data[year]
        arope = yr.get("arope")
        arop  = yr.get("arop")
        if arope is None and arop is None:
            continue

        parts = []
        if arope is not None: parts.append(f"AROPE: {arope:.1f}%")
        if arop  is not None: parts.append(f"Armutsgefährdung: {arop:.1f}%")
        depriv = yr.get("depriv")
        if depriv is not None: parts.append(f"Materielle Deprivation: {depriv:.1f}%")

        results.append({
            "title": f"Armut & soziale Ausgrenzung AT {year}: " + " | ".join(parts),
            "indicator": "AROPE / Armutsgefährdungsquote (EU-SILC)",
            "year": str(year),
            "value": arope,
            "unit": "%",
            "source": "Statistik Austria",
            "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/soziales/armut-und-soziale-eingliederung",
            "dataset_id": "OGD_armsilc01_hvd_ogdonly_HVD_ARM_1",
        })

    if not results:
        return []

    latest_year = all_years[0]
    yr = data[latest_year]

    # --- Gini + S80/S20 ---
    if want_gini or True:  # immer als Kontext
        gini   = yr.get("gini")
        s80s20 = yr.get("s80s20")
        if gini is not None or s80s20 is not None:
            parts = []
            if gini   is not None: parts.append(f"Gini: {gini:.1f}")
            if s80s20 is not None: parts.append(f"S80/S20: {s80s20:.1f}")
            results.append({
                "title": f"Einkommensungleichheit AT {latest_year}: " + " | ".join(parts),
                "indicator": "Gini-Koeffizient + S80/S20 (EU-SILC)",
                "year": str(latest_year),
                "value": gini,
                "unit": "Gini 0–100",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/soziales/armut-und-soziale-eingliederung",
                "dataset_id": "OGD_unglsilc02_HVD_UNG_1",
            })

    # --- Kinder vs. Ältere ---
    if want_child or want_elderly:
        parts = []
        arop_child   = yr.get("arop_child")
        arop_elderly = yr.get("arop_elderly")
        arop_wa      = yr.get("arop_working_age")
        if arop_child   is not None: parts.append(f"Kinder (<18 J.): {arop_child:.1f}%")
        if arop_wa      is not None: parts.append(f"Erwerbsalter (18–64 J.): {arop_wa:.1f}%")
        if arop_elderly is not None: parts.append(f"65+ Jahre: {arop_elderly:.1f}%")
        if parts:
            results.append({
                "title": f"Armutsgefährdung nach Altersgruppe AT {latest_year}: " + " | ".join(parts),
                "indicator": "Armutsgefährdungsquote nach Altersgruppe (EU-SILC)",
                "year": str(latest_year),
                "value": arop_child,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/soziales/armut-und-soziale-eingliederung",
                "dataset_id": "OGD_armsilc01_hvd_ogdonly_HVD_ARM_1",
            })

    # --- Geschlecht ---
    if want_gender:
        arop_m = yr.get("arop_m")
        arop_f = yr.get("arop_f")
        if arop_m is not None and arop_f is not None:
            results.append({
                "title": f"Armutsgefährdung nach Geschlecht AT {latest_year}: Männer {arop_m:.1f}% | Frauen {arop_f:.1f}%",
                "indicator": "Armutsgefährdungsquote nach Geschlecht (EU-SILC)",
                "year": str(latest_year),
                "value": f"M: {arop_m:.1f}%, F: {arop_f:.1f}%",
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/soziales/armut-und-soziale-eingliederung",
                "dataset_id": "OGD_armsilc01_hvd_ogdonly_HVD_ARM_1",
            })

    # --- Staatsbürgerschaft / Herkunft ---
    if want_citizen:
        arop_at     = yr.get("arop_austrian")
        arop_non_at = yr.get("arop_non_austrian")
        arop_non_eu = yr.get("arop_non_eu")
        if arop_at is not None or arop_non_at is not None:
            parts = []
            if arop_at     is not None: parts.append(f"Österr. Staatsbürger: {arop_at:.1f}%")
            if arop_non_at is not None: parts.append(f"Nicht-Österreicher: {arop_non_at:.1f}%")
            if arop_non_eu is not None: parts.append(f"davon Nicht-EU: {arop_non_eu:.1f}%")
            results.append({
                "title": f"Armutsgefährdung nach Staatsbürgerschaft AT {latest_year}: " + " | ".join(parts),
                "indicator": "Armutsgefährdungsquote nach Staatsbürgerschaft (EU-SILC)",
                "year": str(latest_year),
                "value": arop_non_at,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/soziales/armut-und-soziale-eingliederung",
                "dataset_id": "OGD_armsilc01_hvd_ogdonly_HVD_ARM_1",
            })

    # --- Erwerbsstatus ---
    if want_employ:
        parts = []
        a_emp  = yr.get("arop_employed")
        a_unem = yr.get("arop_unemployed")
        a_ret  = yr.get("arop_retired")
        if a_emp  is not None: parts.append(f"Erwerbstätige: {a_emp:.1f}%")
        if a_unem is not None: parts.append(f"Arbeitslose: {a_unem:.1f}%")
        if a_ret  is not None: parts.append(f"Pensionisten: {a_ret:.1f}%")
        if parts:
            results.append({
                "title": f"Armutsgefährdung nach Erwerbsstatus AT {latest_year}: " + " | ".join(parts),
                "indicator": "Armutsgefährdungsquote nach Erwerbsstatus (EU-SILC)",
                "year": str(latest_year),
                "value": a_unem,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/soziales/armut-und-soziale-eingliederung",
                "dataset_id": "OGD_armsilc01_hvd_ogdonly_HVD_ARM_1",
            })

    # --- Bildungsniveau ---
    if want_education:
        arop_by_isced = yr.get("arop_by_isced", {})
        if arop_by_isced:
            isced_parts = [f"{label}: {rate:.1f}%" for label, rate in sorted(arop_by_isced.items())]
            results.append({
                "title": f"Armutsgefährdung nach Bildungsniveau AT {latest_year}: " + " | ".join(isced_parts),
                "indicator": "Armutsgefährdungsquote nach Bildungsniveau (EU-SILC)",
                "year": str(latest_year),
                "value": arop_by_isced,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/soziales/armut-und-soziale-eingliederung",
                "dataset_id": "OGD_armsilc01_hvd_ogdonly_HVD_ARM_1",
            })

    # --- Regional (Bundesland) ---
    if want_regional:
        arop_by_bl = yr.get("arop_by_bl", {})
        if arop_by_bl:
            sorted_bl = sorted(arop_by_bl.items(), key=lambda x: x[1], reverse=True)
            bl_parts = [f"{name}: {rate:.1f}%" for name, rate in sorted_bl]
            results.append({
                "title": f"Armutsgefährdung nach Bundesland AT {latest_year}: " + " | ".join(bl_parts),
                "indicator": "Armutsgefährdungsquote nach Bundesland (EU-SILC)",
                "year": str(latest_year),
                "value": arop_by_bl,
                "unit": "%",
                "source": "Statistik Austria",
                "url": "https://www.statistik.at/statistiken/bevoelkerung-und-soziales/soziales/armut-und-soziale-eingliederung",
                "dataset_id": "OGD_armsilc01_hvd_ogdonly_HVD_ARM_1",
            })

    return results
