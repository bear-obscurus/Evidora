"""UBA Klima — österreichische Treibhausgas-Bilanz nach Sektor (UBA-Daten).

Datenquelle (Stand 2026-05): **Klimadashboard Open Data API**
(https://api.klimadashboard.org/v0/) — kuratiert die offizielle
Österreichische Luftschadstoff-Inventur (OLI/BLI) des Umweltbundesamtes
Österreich (UBA) PLUS UBA-Abschätzungen für das laufende Jahr. Lizenz
CC-BY-4.0.

Warum nicht direkt data.gv.at / CKAN?
-------------------------------------
data.gv.at hat 2026 von CKAN auf einen JS-SPA-Frontend ohne öffentliche
JSON-API umgestellt — die historischen Endpoints
``/katalog/api/3/action/package_search`` liefern jetzt HTML statt JSON
(404 + SPA-Bootstrap). Die UBA selbst publiziert OLI/BLI als PDF +
Excel; die Klimadashboard-API ist die einzige stabile, gepflegte,
maschinenlesbare Quelle für die *gleichen* UBA-Bilanzdaten (Quellfeld
"OLI 2025 (1990-2024)" / "BLI 2025 (1990-2023)" / "Abschätzung UBA").

Use-Case:
- "Österreichs Treibhausgas-Emissionen sind seit 1990 gestiegen"
  → 1990 vs. 2024 Vergleich (Gesamt + Sektor)
- "Verkehr ist der größte CO2-Sektor in Österreich"
  → ksg_transport-Anteil + Vergleich mit ksg_energy/ksg_industry
- "AT-Klimaziel verfehlt" / "EU-Klima-Strafzahlung droht"
  → KSG-Wert aktuell vs. Lineares Reduktionsziel (Synthesizer baut)
- "Landwirtschaft / Gebäude / Industrie — Sektor X stagniert"
  → Sektor-Zeitreihe

Trigger:
- "UBA", "Umweltbundesamt", "AT-Klimabilanz", "Klimadashboard"
- "Treibhausgas Österreich" / "GHG Austria" / "CO2-Emissionen AT"
- "AT-Sektor [Verkehr/Industrie/Landwirtschaft/Gebäude/Energie] Emissionen"
- AT-Hard-Skip-Schutz nicht nötig (ist explizit AT, KSG-Abgrenzung
  österreichisch).

Konsens-Schutz:
- Bei Klima-Skepsis-Claims ("CO2 ist gar nicht schädlich" / "Klimawandel
  natürlich") wird NICHT ausgespielt — das ist Aufgabe des IPCC-/EEA-/
  Skeptical-Science-Stacks. Dieser Service ist ein reiner Inventur-
  Liefer ant für Sektor-Werte.

Cache: 24 h (UBA-Inventur ist Jahresgranular, Abschätzung wird Q1 für
das Vorjahr nachgereicht).

------------------------------------------------------------------------
WIRING-SNIPPET (für main.py — NICHT automatisch eingebaut):
------------------------------------------------------------------------
    # main.py — Imports:
    from services.uba_klima import (
        search_uba_klima,
        claim_mentions_uba_cached,
    )

    # main.py — Task-Wiring (im pipeline-Block neben search_oenb):
        if claim_mentions_uba_cached(claim):
            tasks.append(cached("UBA Klima", search_uba_klima, analysis))

    # reranker.py — _AUTHORITATIVE_INDICATORS-Tuple ergänzen:
        "uba_klima_sector",
        "uba_klima_total",
        "uba_klima_baseline",

    # data_updater.py — Prefetch (optional, Live-API ist schnell genug
    # für reaktiven Fetch in der Anfrage; nur prefetchen, wenn der
    # Service in den Top-10-Triggered-Services landet):
    # NICHT erforderlich für v1 — Live-Call mit 24 h Cache reicht.
------------------------------------------------------------------------
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------
# Klimadashboard Open Data API v0 (Beta, CC-BY-4.0)
# Doku: https://api.klimadashboard.org/
KLIMA_API_BASE = "https://api.klimadashboard.org/v0/data"
EMISSIONS_ENDPOINT = f"{KLIMA_API_BASE}/emissions_data/records"

# UUID der Federal/AT-Gesamt-Region in der Klimadashboard-Datenbank
# (Bundesländer haben eigene UUIDs; wir wollen nur Federal-Level).
AT_FEDERAL_REGION = "3373d6d8-5fa2-4d5a-ac0b-790e69982f81"

CACHE_TTL_S = 24 * 60 * 60  # 24 h
DEFAULT_TIMEOUT = 25.0

# Wir holen 1990 (Baseline) + 2024 (aktuelle UBA-Inventur) + 2025
# (UBA-Abschätzung), gefiltert auf country=AT, type=Gesamt — sortiert
# nach Sektor.  Das reicht für die häufigsten Faktencheck-Claims.
TARGET_YEARS = (1990, 2023, 2024, 2025)

# KSG-Sektor-Mapping (Klimaschutzgesetz-Abgrenzung Österreich).
# Schlüssel = category-Feld der API; Wert = display-fähige Sektor-Bezeichnung.
SECTOR_LABELS: dict[str, str] = {
    "ksg":             "AT-Gesamt (KSG-Abgrenzung)",
    "ksg_energy":      "Energie & Industrie (KSG)",
    "ksg_transport":   "Verkehr",
    "ksg_industry":    "Industrie (gesamt inkl. EH)",
    "ksg_buildings":   "Gebäude",
    "ksg_agriculture": "Landwirtschaft",
    "ksg_waste":       "Abfallwirtschaft",
    "ksg_fgases":      "F-Gase (fluorierte Treibhausgase)",
    "total":           "AT-Gesamt (Inventur inkl. Landnutzung)",
}
SECTOR_KEYS = tuple(SECTOR_LABELS.keys())


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_UBA_PRIMARY_TERMS = (
    "uba", "umweltbundesamt",
    "klimadashboard", "klima-dashboard",
    "at-klimabilanz", "klimabilanz österreich", "klimabilanz at",
    "oli 2025", "bli 2025",
    "österreichische luftschadstoff-inventur", "luftschadstoff-inventur",
    "ksg-abgrenzung", "klimaschutzgesetz",
)

# THG/CO2 + Austria-Marker
_THG_TERMS = (
    "treibhausgas", "thg", "ghg",
    "co2-emission", "co2 emission", "co₂-emission",
    "co2-bilanz", "co2 bilanz",
    "kohlendioxid",
    "klimaemission", "klima-emission",
    "klimaziel", "klimaziele",
    "treibhausgase",
)

_AT_TERMS = (
    "österreich", "austria", "österreichisch",
    "republik österreich", "at-",
    "burgenland", "kärnten", "niederösterreich", "oberösterreich",
    "salzburg", "steiermark", "tirol", "vorarlberg",
    "wien", "vienna",
)

# Sektor-spezifische Trigger (DE + EN) — composite: Sektor-Wort + AT-Kontext
_SECTOR_DETECT_TERMS = {
    "ksg_transport":   ("verkehr", "transport", "mobilität", "kfz", "pkw",
                        "lkw", "verkehrssektor", "spritverbrauch"),
    "ksg_energy":      ("energie", "energiesektor", "stromerzeugung",
                        "kraftwerk", "energieversorgung"),
    "ksg_industry":    ("industrie", "industriesektor", "stahl", "zement",
                        "chemie", "industrielle emission"),
    "ksg_buildings":   ("gebäude", "heizung", "wohnbau", "raumwärme",
                        "gebäudesektor", "fernwärme"),
    "ksg_agriculture": ("landwirtschaft", "agrarsektor", "viehhaltung",
                        "rinder", "ackerbau", "düngung"),
    "ksg_waste":       ("abfall", "müll", "deponie", "abfallwirtschaft"),
    "ksg_fgases":      ("f-gas", "f-gase", "fluorierte", "hfkw"),
}


def _claim_mentions_uba(claim_lc: str) -> bool:
    """Trigger-Logik. Liefert True, wenn der Claim potentiell von einer
    UBA-Sektor-Bilanz beantwortbar ist.
    """
    if not claim_lc:
        return False

    # 1. Direkte UBA/Klimadashboard-Erwähnung
    if any(t in claim_lc for t in _UBA_PRIMARY_TERMS):
        return True

    # 2. Composite: THG/CO2-Vokabel + AT-Kontext
    has_thg = any(t in claim_lc for t in _THG_TERMS)
    has_at = any(t in claim_lc for t in _AT_TERMS)
    if has_thg and has_at:
        return True

    # 3. Composite: Sektor-Vokabel + (Emission/CO2/Klima) + AT-Kontext
    has_sector = any(
        any(t in claim_lc for t in tokens)
        for tokens in _SECTOR_DETECT_TERMS.values()
    )
    has_climate = any(t in claim_lc for t in (
        "emission", "emissionen", "co2", "co₂", "klima",
        "treibhaus", "klimaneutral",
    ))
    if has_sector and has_climate and has_at:
        return True

    return False


def claim_mentions_uba_cached(claim: str) -> bool:
    """Compat-Wrapper auf den Trigger; konvertiert Claim → lowercase."""
    return _claim_mentions_uba((claim or "").lower())


# ---------------------------------------------------------------------------
# Live-Fetch (24 h Cache)
# ---------------------------------------------------------------------------
# Cache-Struktur: dict mit (Jahr, Kategorie, Typ) → record
_cache: dict[tuple[int, str, str], dict] | None = None
_cache_time: float = 0.0


async def _fetch_records(
    client: httpx.AsyncClient,
    *,
    country: str = "AT",
    year_gte: int | None = None,
    limit: int = 1000,
) -> list[dict]:
    """Hole emissions_data-Records von der Klimadashboard-API.

    Die API filtert mit ``filter[field][_op]=value``-Syntax.  Wir paginie-
    ren nicht — 1000 Records reichen problemlos für AT 1990-2025.
    """
    params: list[tuple[str, str]] = [
        ("filter[country][_eq]", country),
        ("filter[region][_eq]", AT_FEDERAL_REGION),
        ("limit", str(limit)),
        ("sort", "-year"),
    ]
    if year_gte is not None:
        params.append(("filter[year][_gte]", str(year_gte)))
    try:
        r = await client.get(EMISSIONS_ENDPOINT, params=params,
                              timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        body = r.json()
        data = body.get("data") or []
        if not isinstance(data, list):
            return []
        return data
    except Exception as exc:
        logger.warning(f"UBA Klima fetch failed: {exc!r}")
        return []


async def _refresh_cache(client: httpx.AsyncClient | None = None) -> None:
    """Refresh den 24-h-Cache. Holt 1990 + alle Jahre ab 2020 (inkl.
    aktueller UBA-Inventur + Abschätzung)."""
    global _cache, _cache_time

    own_client = False
    if client is None:
        client = polite_client(timeout=DEFAULT_TIMEOUT)
        own_client = True

    try:
        # Zwei Anfragen: Baseline 1990 (year_eq 1990 → wir holen ab 1990
        # mit limit 10k, paginieren später wenn nötig).  Aber:
        # Klimadashboard-API hat KEIN year_eq-Operator-Spezialfall; wir
        # ziehen ab 1990 alle AT-Records (knapp 5300 Records, gut machbar).
        records = await _fetch_records(client, year_gte=1990, limit=10000)

        cache: dict[tuple[int, str, str], dict] = {}
        for rec in records:
            yr = rec.get("year")
            cat = rec.get("category")
            typ = rec.get("type") or ""
            if yr is None or cat is None:
                continue
            key = (int(yr), str(cat), str(typ))
            # Bei Duplikaten gewinnt die zuletzt geupdatete Quelle
            # (OLI > BLI > Abschätzung > NowCast für gleiches Jahr).
            existing = cache.get(key)
            if existing is None:
                cache[key] = rec
            else:
                # Priorisiere höheren Update-Stempel (ISO-String-Vergleich
                # funktioniert dank ISO 8601).
                if (rec.get("update") or "") > (existing.get("update") or ""):
                    cache[key] = rec

        _cache = cache
        _cache_time = time.time()
        logger.info(
            "UBA Klima cache refreshed: %d (year,category,type)-keys",
            len(cache),
        )
    finally:
        if own_client:
            await client.aclose()


async def _ensure_cache_fresh() -> dict[tuple[int, str, str], dict]:
    """Stelle sicher, dass der Cache <= 24 h alt ist; lade ggf. nach."""
    global _cache, _cache_time
    now = time.time()
    if _cache is None or (now - _cache_time) > CACHE_TTL_S:
        await _refresh_cache()
    return _cache or {}


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _fmt_mt(value: float | int | None) -> str:
    """Format Tonnen CO2äq → 'X,YY Mt' (Megatonnen, AT-Mengen sind > Mt).

    Hinweis: Trotz API-Doku ("kt CO2-equivalent") sind die Rohwerte in
    **Tonnen** angegeben — z. B. AT 2024 = 66.637.832 t = 66,64 Mt.
    Verifiziert per Quervergleich mit UBA-OLI 2025 Pressemitteilung.
    """
    if value is None:
        return "?"
    try:
        mt = float(value) / 1_000_000.0  # t → Mt
        # DE-Format: Komma als Dezimaltrenner, Punkt als Tausender
        formatted = f"{mt:,.2f}"  # → "66,637.83" (en_US)
        # Swap , and . for DE locale
        formatted = formatted.replace(",", "§").replace(".", ",").replace("§", ".")
        return f"{formatted} Mt"
    except (TypeError, ValueError):
        return "?"


def _de_pct(pct: float | None) -> str:
    if pct is None:
        return "?"
    return f"{pct:+.1f}%".replace(".", ",")


def _detect_focus_sectors(claim_lc: str) -> list[str]:
    """Welche KSG-Sektoren erwähnt der Claim explizit?  Default: alle."""
    hits: list[str] = []
    for key, tokens in _SECTOR_DETECT_TERMS.items():
        if any(t in claim_lc for t in tokens):
            hits.append(key)
    return hits


def _pick_latest_year(
    cache: dict[tuple[int, str, str], dict],
    category: str,
    type_: str = "Gesamt",
) -> tuple[int | None, dict | None]:
    """Finde das jüngste Jahr für (category, type) im Cache."""
    candidates = [(yr, rec) for (yr, c, t), rec in cache.items()
                  if c == category and t == type_]
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0]


def _build_sector_result(
    cache: dict[tuple[int, str, str], dict],
    sector_key: str,
    sector_label: str,
) -> dict | None:
    """Baue ein Result-Dict für einen Sektor (Baseline + Latest + Trend)."""
    type_pref = "Gesamt"
    baseline_rec = cache.get((1990, sector_key, type_pref))
    latest_year, latest_rec = _pick_latest_year(cache, sector_key, type_pref)

    if baseline_rec is None and latest_rec is None:
        return None

    baseline_val = (baseline_rec or {}).get("value")
    latest_val = (latest_rec or {}).get("value")
    pct_change: float | None = None
    if baseline_val and latest_val:
        try:
            pct_change = (float(latest_val) - float(baseline_val)) \
                          / float(baseline_val) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            pct_change = None

    src_label = (latest_rec or baseline_rec or {}).get("source") or "UBA OLI"
    update = (latest_rec or {}).get("update") or ""

    headline_parts = [f"AT-Sektor {sector_label} (UBA-Inventur)"]
    if baseline_val:
        headline_parts.append(f"1990 = {_fmt_mt(baseline_val)} CO₂äq")
    if latest_val and latest_year:
        headline_parts.append(
            f"{latest_year} = {_fmt_mt(latest_val)} CO₂äq"
        )
    if pct_change is not None:
        headline_parts.append(f"Δ 1990→{latest_year} = {_de_pct(pct_change)}")
    headline = "; ".join(headline_parts) + "."

    description = (
        f"Quelle: {src_label} (Update {update[:10] if update else 'n/a'}). "
        "KSG-Abgrenzung = Sektoren außerhalb EU-Emissionshandel; "
        "Werte in Mt CO₂-Äquivalent (Treibhausgase nach UNFCCC-CRT-Methodik). "
        "Daten gespiegelt von Klimadashboard.org (CC-BY-4.0) — "
        "Primärquelle ist das Umweltbundesamt Österreich (UBA)."
    )

    indicator_kind = "uba_klima_total" if sector_key in ("ksg", "total") \
                     else "uba_klima_sector"

    return {
        "indicator_name": f"UBA-Klimabilanz: {sector_label}",
        "indicator": indicator_kind,
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(latest_year) if latest_year else "1990",
        "value": latest_val,
        "display_value": headline,
        "description": description,
        "url": "https://klimadashboard.at/emissionen",
        "source": "Umweltbundesamt Österreich (via Klimadashboard CC-BY-4.0)",
    }


def _build_baseline_context(
    cache: dict[tuple[int, str, str], dict],
) -> dict | None:
    """Methodologie-/Kontext-Block — wird immer mitgeliefert (whitelist
    'uba_klima_baseline')."""
    total_1990 = cache.get((1990, "ksg", "Gesamt"))
    _, total_latest = _pick_latest_year(cache, "ksg", "Gesamt")
    if total_1990 is None and total_latest is None:
        return None

    base_val = (total_1990 or {}).get("value")
    latest_val = (total_latest or {}).get("value")
    latest_yr = (total_latest or {}).get("year")
    pct: float | None = None
    if base_val and latest_val:
        try:
            pct = (float(latest_val) - float(base_val)) \
                   / float(base_val) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            pct = None

    desc = (
        "METHODE: Österreich folgt der UNFCCC-CRT-Inventur-Methodik. "
        "Die KSG-Abgrenzung (Klimaschutzgesetz-Sektoren) umfasst die "
        "NICHT vom EU-Emissionshandel (EH) erfassten Sektoren — Energie "
        "(Nicht-EH), Verkehr, Gebäude, Landwirtschaft, F-Gase, Abfall. "
        "Diese Abgrenzung ist relevant für AT-Klimaziele unter EU-Effort-"
        "Sharing-Regulation (ESR). Werte in Mt CO₂-Äquivalent (Standard-"
        "Reporting-Einheit UNFCCC). 1990 ist die offizielle Baseline. "
        "Quelle der Rohdaten: UBA OLI/BLI; gespiegelt von Klimadashboard "
        "unter CC-BY-4.0."
    )

    return {
        "indicator_name": "UBA-Kontext: Methodik + 1990-Baseline",
        "indicator": "uba_klima_baseline",
        "country": "AUT",
        "country_name": "Österreich",
        "year": str(latest_yr) if latest_yr else "2024",
        "value": latest_val,
        "display_value": (
            f"AT-Gesamt THG (KSG-Sektoren): 1990 = {_fmt_mt(base_val)}, "
            f"{latest_yr} = {_fmt_mt(latest_val)}, "
            f"Veränderung 1990→{latest_yr} = {_de_pct(pct)}."
        ),
        "description": desc,
        "url": "https://www.umweltbundesamt.at/klima/treibhausgasinventur",
        "source": "UBA Österreich (Inventur OLI 2025)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_uba_klima(analysis: dict) -> dict:
    empty = {
        "source": "UBA Österreich",
        "type": "climate_at",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_uba(matchable):
        return empty

    try:
        cache = await _ensure_cache_fresh()
    except Exception as exc:
        logger.warning(f"UBA Klima cache refresh failed: {exc!r}")
        return empty

    if not cache:
        return empty

    # Welche Sektoren?  Default = alle KSG-Sektoren (Synthesizer soll den
    # ganzen Tabellenblock haben).  Bei Sektor-spezifischem Claim („Verkehr…")
    # priorisieren wir den gemeldeten Sektor zuerst.
    focus = _detect_focus_sectors(matchable)
    results: list[dict] = []

    # 1. Kontext-/Baseline-Block — immer (authoritative)
    ctx = _build_baseline_context(cache)
    if ctx is not None:
        results.append(ctx)

    # 2. Sektor-Blöcke
    ordered_keys: list[str] = []
    if focus:
        # Fokus zuerst, dann alle übrigen
        for k in focus:
            if k in SECTOR_LABELS and k not in ordered_keys:
                ordered_keys.append(k)
    for k in SECTOR_KEYS:
        if k in ordered_keys:
            continue
        # 'total' bewusst auslassen, wenn 'ksg' schon im Baseline-Block
        # ist — vermeidet Doppel-Total.
        if k == "total":
            continue
        ordered_keys.append(k)

    for key in ordered_keys:
        block = _build_sector_result(cache, key, SECTOR_LABELS[key])
        if block is not None:
            results.append(block)

    return {
        "source": "UBA Österreich",
        "type": "climate_at",
        "results": results,
    }
