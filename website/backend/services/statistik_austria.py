"""Statistik Austria Open Government Data — österreichische amtliche Statistiken.

Integriert zwei Datasets:
1. VPI (Verbraucherpreisindex) — monatliche Inflationsdaten (Basis 2020=100)
2. Gesundheitsausgaben — jährliche Ausgaben nach Leistungsart und Finanzierung (SHA)

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

# --- Cache ---
STAT_AT_CACHE_TTL = 86400  # 24h — VPI updates monthly, health expenditure annually

_vpi_cache: list[dict] | None = None
_vpi_cache_time: float = 0.0

_health_cache: list[dict] | None = None
_health_cache_time: float = 0.0

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
    """Search Statistik Austria for VPI and health expenditure data."""
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
                "dafür sind spezifische Studien nötig."
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
