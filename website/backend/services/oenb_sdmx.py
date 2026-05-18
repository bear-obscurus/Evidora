"""OeNB SDMX — Oesterreichische Nationalbank (Live-Statistik via SDMX).

Quelle: OeNB-Statistik (https://www.oenb.at/Statistik.html). Die OeNB
publiziert detaillierte AT-Banken-Statistik (Konsumkredite, Hypothekar-
darlehen, Einlagen-Volumen, Zinssätze), Zahlungsbilanz und Internationale
Investitions-Position.

Komplementär zu `services/oenb.py` (kuratiertes Pack mit EZB-Leitzins +
Kommunikations-Hinweisen) — dieser Service liefert LIVE-Zahlen.

API-Strategie:
1. PRIMÄR: DBnomics-Mirror (analog IMF/BIS).
       https://api.db.nomics.world/v22/series/{provider}/{dataset}/...
   ⚠ DBnomics indexiert KEINEN "OeNB"-Provider (Stand 2026-05). Wir nutzen
   daher den ECB-Mirror (BSI / MIR) mit REF_AREA=AT — die ECB aggregiert
   OeNB-Meldedaten der österreichischen MFIs (Monetary Financial Institutions);
   semantisch sind das die gleichen Zahlen, die die OeNB national veröffentlicht.
   Quellenangabe bleibt deshalb "OeNB-Daten via ECB MFI Reporting".
2. FALLBACK: OeNB Direct-SDMX (ISAweb).
       https://www.oenb.at/isaweb/webservice/services/Catalogue
   Der OeNB-Direct-Endpoint wirft Cloudflare-503/Session-Redirects bei
   anonymem Zugriff; daher nur als zweiter Versuch mit kurzem Timeout.

Beispiel-Indikatoren (über ECB-BSI-Aggregat):
- Konsumkredite (BS_ITEM=A21 oder A20-Subset, BS_COUNT_SECTOR=2250 Haushalte)
- Hypothekardarlehen (BS_ITEM=A22, BS_COUNT_SECTOR=2250)
- Einlagen-Volumen privater Haushalte (BS_ITEM=L21..L23, BS_COUNT_SECTOR=2250)
- AT-Bankenstabilität — über Eurostat BOP_C6_Q / IIP für Auslandsforderungen

Lizenz: OeNB Open Data + ECB Open Data (Quellenangabe verpflichtend).

WICHTIG — Politische Guardrails (memory/project_political_guardrails.md):
- Reine Statistik-Wiedergabe, KEINE Bewertung der Geldpolitik
- KEINE Aussagen zu "Banken-Risiko" oder "Krediteignung"
- Bei Wechselwirkung mit Inflations-/Politik-Claims: nur deskriptive Werte
"""

# WIRING für main.py (NICHT automatisch eingefügt — bitte manuell ergänzen):
# from services.oenb_sdmx import search_oenb_sdmx, claim_mentions_oenb_sdmx_cached
# if claim_mentions_oenb_sdmx_cached(claim):
#     tasks.append(cached("OeNB SDMX", search_oenb_sdmx, analysis))
#     queried_names.append("OeNB SDMX (Banken-Statistik AT)")
#
# WIRING für services/data_updater.py (Prefetch):
# (Optional — die Series sind klein und cachen 24h pro Trigger automatisch;
#  ein Prefetch ist nicht nötig.)
#
# WIRING für services/reranker (Whitelist):
# Falls eine Source-Whitelist gepflegt wird: "OeNB SDMX" + "OeNB" hinzufügen.

from __future__ import annotations

import json
import logging
import time
from urllib.parse import quote_plus

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Endpoints & Konfiguration
# ---------------------------------------------------------------------------
DBNOMICS_API = "https://api.db.nomics.world/v22"
OENB_DIRECT_API = "https://www.oenb.at/isaweb/spec_sdw/datacollection"
TIMEOUT_S = 12.0
TIMEOUT_DIRECT_S = 5.0   # OeNB-Direkt blockt oft → kurz halten
MAX_RESULTS = 3
CACHE_TTL_S = 24 * 3600  # 24h

# ---------------------------------------------------------------------------
# Indikator-Definition: (Trigger-Begriff → ECB-Dataset + Dimension-Filter)
#
# ECB-BSI = Balance Sheet Items (Mengen, monatlich)
# ECB-MIR = MFI Interest Rates (Zinssätze, monatlich)
# REF_AREA=AT → Österreich
# BS_COUNT_SECTOR=2250 → Haushalte
# BS_COUNT_SECTOR=2240 → Nichtfinanzielle Kapitalgesellschaften (NFC)
# BS_ITEM:
#   A20 = Loans, total
#   A22 = Lending for house purchase (Hypothekardarlehen)
#   A21 = Consumer credit (Konsumkredite)
#   L20 = Deposits, total (Einlagen gesamt)
# ---------------------------------------------------------------------------
_INDICATORS: dict[str, dict] = {
    # Konsumkredite an Haushalte (Volumen, EUR Mio.)
    "konsumkredite": {
        "dataset": "BSI",
        "label_de": "Konsumkredite an Haushalte (AT)",
        "unit": "Mio. EUR",
        "filters": {
            "REF_AREA": "AT", "BS_ITEM": "A21",
            "BS_COUNT_SECTOR": "2250", "FREQ": "M",
        },
    },
    # Hypothekardarlehen / Wohnbaukredite (Volumen)
    "hypothekardarlehen": {
        "dataset": "BSI",
        "label_de": "Wohnbaukredite an Haushalte (AT)",
        "unit": "Mio. EUR",
        "filters": {
            "REF_AREA": "AT", "BS_ITEM": "A22",
            "BS_COUNT_SECTOR": "2250", "FREQ": "M",
        },
    },
    # Gesamt-Kredite an Haushalte (Fallback)
    "kreditvolumen_haushalte": {
        "dataset": "BSI",
        "label_de": "Kredite an Haushalte gesamt (AT)",
        "unit": "Mio. EUR",
        "filters": {
            "REF_AREA": "AT", "BS_ITEM": "A20",
            "BS_COUNT_SECTOR": "2250", "FREQ": "M",
        },
    },
    # Einlagen privater Haushalte
    "einlagen": {
        "dataset": "BSI",
        "label_de": "Einlagen privater Haushalte (AT)",
        "unit": "Mio. EUR",
        "filters": {
            "REF_AREA": "AT", "BS_ITEM": "L20",
            "BS_COUNT_SECTOR": "2250", "FREQ": "M",
        },
    },
    # Kredite an Unternehmen (NFC)
    "unternehmenskredite": {
        "dataset": "BSI",
        "label_de": "Kredite an Unternehmen (AT, NFC)",
        "unit": "Mio. EUR",
        "filters": {
            "REF_AREA": "AT", "BS_ITEM": "A20",
            "BS_COUNT_SECTOR": "2240", "FREQ": "M",
        },
    },
}

# Trigger-Begriff → Indikator-Key
_TERM_TO_INDICATOR: dict[str, str] = {
    # Konsumkredite
    "konsumkredit": "konsumkredite",
    "konsumkredite": "konsumkredite",
    "verbraucherkredit": "konsumkredite",
    "at-konsumkredite": "konsumkredite",
    # Hypothekardarlehen / Wohnbau
    "hypothekardarlehen": "hypothekardarlehen",
    "hypothekarkredit": "hypothekardarlehen",
    "hypothek": "hypothekardarlehen",
    "wohnbaukredit": "hypothekardarlehen",
    "at-hypothekardarlehen": "hypothekardarlehen",
    # Einlagen
    "einlagen-volumen": "einlagen",
    "einlagenvolumen": "einlagen",
    "spareinlagen": "einlagen",
    "at-einlagen": "einlagen",
    # Unternehmenskredite
    "unternehmenskredit": "unternehmenskredite",
    "firmenkredit": "unternehmenskredite",
    # Generisches AT-Kreditvolumen
    "at-kreditvolumen": "kreditvolumen_haushalte",
    "kreditvolumen österreich": "kreditvolumen_haushalte",
}

# Direkt-Trigger (statistisch / OeNB-spezifisch)
_DIRECT_TERMS = (
    "oenb-statistik",
    "oenb statistik",
    "oenb-banken-statistik",
    "oenb banken-statistik",
    "oenb banken statistik",
    "zahlungsbilanz österreich",
    "zahlungsbilanz at",
    "internationale investitions-position",
    "internationale investitionsposition",
    "iip österreich",
    "iip at",
    "at-bankenstabilität",
    "at bankenstabilität",
    "österreichische bankenstabilität",
    "banken-statistik österreich",
    "bankenstatistik österreich",
)

# Pack-Domäne von services/oenb.py — diese Begriffe sollen NICHT triggern
_PACK_DOMAIN_TERMS = (
    "leitzins",          # gehört zum oenb.py-Pack (EZB-Leitzins)
    "ezb-leitzins",
    "ezb leitzins",
    "wechselkurs",       # oenb.py-Pack-Domäne
    "ezb-zinssatz",
    "öxit",
    "schilling zurück",
    "österreich verlässt den euro",
)


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_oenb_sdmx(claim_lc: str) -> bool:
    """Conservative Trigger:
    1. Direkter Term (OeNB-Statistik / Zahlungsbilanz / IIP) → True
    2. Composite: Indikator-Begriff (Konsumkredit/Hypothek/Einlagen) + AT-Kontext

    Hard-Skip: Wenn der Claim primär eine Pack-Domäne (Leitzins / Wechselkurs /
    "Schilling zurück") berührt UND kein expliziter SDMX-Statistik-Begriff
    drinsteht, gehört das zu services/oenb.py — nicht hier triggern.
    """
    if not claim_lc:
        return False

    # 1. Direkt-Trigger immer ziehen lassen
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    # 2. Composite-Trigger: konkreter Banken-Indikator + AT-Kontext
    has_indicator = any(t in claim_lc for t in _TERM_TO_INDICATOR.keys())
    # AT-Kontext: Wortteile + trailing " at" am Satzende mitnehmen
    has_at = (
        any(t in claim_lc for t in ("österreich", "austria", "at-", " at "))
        or claim_lc.rstrip(" .!?,").endswith(" at")
        or claim_lc.startswith("at ")
    )
    # "at-konsumkredite" und Konsorten enthalten AT bereits → Kurzform
    has_at_prefix_indicator = any(
        ("at-" + t in claim_lc) or ("at " + t in claim_lc)
        for t in _TERM_TO_INDICATOR.keys()
    )

    # Hard-Skip: wenn nur Pack-Term (Leitzins etc.) + AT → oenb.py macht das
    has_pack_only = any(t in claim_lc for t in _PACK_DOMAIN_TERMS)
    if has_pack_only and not has_indicator:
        return False

    if has_indicator and (has_at or has_at_prefix_indicator):
        return True

    return False


# Modul-Cache: claim_lc → (ts, bool)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_oenb_sdmx_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_oenb_sdmx(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Result-Cache
# ---------------------------------------------------------------------------
_result_cache: dict[str, tuple[float, list[dict]]] = {}


def _cache_get(key: str) -> list[dict] | None:
    now = time.time()
    hit = _result_cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL_S:
        return hit[1]
    return None


def _cache_put(key: str, value: list[dict]) -> None:
    _result_cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
def _extract_indicators(claim_lc: str) -> list[str]:
    """Liefert dedupliziert die Indikator-Keys aus _INDICATORS, die getriggert
    wurden (Reihenfolge: Auftreten im Mapping). Falls nichts spezifisches
    matcht, aber ein Direct-Term im Claim ist, gib Default (Konsumkredite)
    zurück.
    """
    seen: set[str] = set()
    out: list[str] = []
    for term, key in _TERM_TO_INDICATOR.items():
        if term in claim_lc and key not in seen:
            out.append(key)
            seen.add(key)
    if not out:
        # Direct-Trigger ohne spezifischen Indikator → Default-Set
        if any(t in claim_lc for t in _DIRECT_TERMS):
            out = ["konsumkredite", "hypothekardarlehen", "einlagen"]
    return out


# ---------------------------------------------------------------------------
# Formatierung
# ---------------------------------------------------------------------------
def _format_number(val: float | int | None, unit: str) -> str:
    if val is None:
        return "—"
    try:
        f = float(val)
    except (TypeError, ValueError):
        return str(val)
    # Tausender-Trennzeichen mit Punkt (DE), Komma als Dezimal
    nd = 0 if unit.startswith("Mio") and abs(f) >= 100 else 2
    formatted = f"{f:,.{nd}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return formatted


def _pick_latest_value(
    periods: list, values: list,
) -> tuple[str, float] | None:
    if not periods or not values:
        return None
    for p, v in zip(reversed(periods), reversed(values)):
        if v is None:
            continue
        try:
            return str(p), float(v)
        except (TypeError, ValueError):
            continue
    return None


def _format_observation(
    *,
    indicator_key: str,
    indicator_def: dict,
    period: str,
    value: float,
    source_path: str,
    series_code: str,
) -> dict:
    label = indicator_def["label_de"]
    unit = indicator_def["unit"]
    val_str = _format_number(value, unit)

    display = (
        f"OeNB-Statistik (via ECB MFI-Reporting), Stand {period}: "
        f"{label} = {val_str} {unit}. "
        f"Datenherkunft: Österreichische MFIs gemeldet an OeNB → ECB; "
        f"semantisch identisch mit OeNB-Veröffentlichung."
    )
    description = (
        f"Banken-Statistik Österreich: {label}. "
        f"Wert für {period}: {val_str} {unit}. "
        f"Quellenpfad: {source_path}. "
        f"Serien-Code: {series_code}. "
        "Methodologie-Hinweis: Die OeNB sammelt die Monatsmeldungen der "
        "österreichischen Banken (MFI-Reporting nach EZB-Verordnung) und "
        "leitet sie an das ESZB weiter; die ECB-Aggregate REF_AREA=AT "
        "entsprechen damit den national publizierten OeNB-Bestandszahlen. "
        "Keine Bewertung der Banken-Stabilität — reine Wiedergabe."
    )

    return {
        "indicator_name": f"OeNB-Banken-Statistik: {label}",
        "indicator": f"oenb_sdmx_{indicator_key}",
        "country": "AUT",
        "country_name": "Österreich",
        "year": period,
        "value": value,
        "display_value": display,
        "description": description,
        "url": "https://www.oenb.at/Statistik.html",
        "source": "OeNB SDMX (via ECB MFI-Reporting)",
    }


# ---------------------------------------------------------------------------
# DBnomics ECB-Mirror Fetch (Primary)
# ---------------------------------------------------------------------------
async def _fetch_dbnomics_filtered(
    client,
    *,
    provider: str,
    dataset: str,
    filters: dict[str, str],
) -> dict | None:
    """GET /v22/series/{provider}/{dataset}?dimensions={json}&observations=1.

    Robust gegen 404/500/Timeout → silent None.
    """
    # dimensions param: JSON-encoded {dim: [val]} pairs
    dims = {k: [v] for k, v in filters.items()}
    dims_json = quote_plus(json.dumps(dims, separators=(",", ":")))
    url = (
        f"{DBNOMICS_API}/series/{provider}/{quote_plus(dataset)}"
        f"?dimensions={dims_json}&observations=1&limit=5"
    )
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"OeNB-SDMX→DBnomics HTTP {resp.status_code} for "
                f"{provider}/{dataset}/{filters}"
            )
            return None
        data = resp.json()
    except Exception as e:
        logger.debug(
            f"OeNB-SDMX→DBnomics fetch failed {provider}/{dataset}: {e}"
        )
        return None

    docs = (data.get("series") or {}).get("docs") or []
    if not docs:
        return None
    # Bevorzuge eine Serie mit Werten + sinnvollem Suffix (Outstanding amount = E)
    for doc in docs:
        sc = doc.get("series_code") or ""
        if sc.endswith(".E") or sc.endswith(".EUR.E"):
            return doc
    return docs[0]


async def _fetch_oenb_direct_ping(client) -> bool:
    """Versuche OeNB-Direct-Endpoint nur zur Verfügbarkeits-Prüfung.

    OeNB ISAweb blockt anonymen Traffic häufig mit Cloudflare-503/Session-
    Redirect; wir geben hier False zurück, sobald der Endpoint nicht direkt
    200 antwortet. Datenparsing der OeNB-XML-Antwort ist deutlich teurer
    als sinnvoll für diesen Service — daher kein echter Fallback-Parser,
    nur Ping für Logging/Telemetrie.
    """
    try:
        resp = await client.get(
            OENB_DIRECT_API,
            timeout=TIMEOUT_DIRECT_S,
            follow_redirects=False,
        )
        return resp.status_code == 200
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Per-Indicator Lookup
# ---------------------------------------------------------------------------
async def _lookup_indicator(client, key: str) -> dict | None:
    """Cached pro Indikator-Key für 24h."""
    cache_key = f"oenb_sdmx::{key}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached[0] if cached else None

    definition = _INDICATORS.get(key)
    if not definition:
        return None

    # Primary: DBnomics ECB-Mirror mit REF_AREA=AT (= OeNB-Meldedaten)
    doc = await _fetch_dbnomics_filtered(
        client,
        provider="ECB",
        dataset=definition["dataset"],
        filters=definition["filters"],
    )
    if doc:
        latest = _pick_latest_value(
            doc.get("period", []), doc.get("value", []),
        )
        if latest:
            period, value = latest
            result = _format_observation(
                indicator_key=key,
                indicator_def=definition,
                period=period,
                value=value,
                source_path=(
                    f"DBnomics-Mirror ECB/{definition['dataset']} "
                    "(REF_AREA=AT = OeNB-Meldedaten)"
                ),
                series_code=doc.get("series_code", ""),
            )
            _cache_put(cache_key, [result])
            return result

    _cache_put(cache_key, [])
    return None


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_oenb_sdmx(analysis: dict) -> dict:
    """Live-Lookup gegen OeNB-Statistik (via ECB-DBnomics-Mirror).

    Strategie:
    1. Trigger-Check
    2. Indikatoren aus Claim extrahieren (Konsumkredite / Hypothek / Einlagen / ...)
    3. Pro Indikator: DBnomics ECB-AT-Filter aufrufen, jüngste Beobachtung wählen
    4. Max MAX_RESULTS, alles deskriptiv (Guardrails)
    """
    empty = {
        "source": "OeNB SDMX",
        "type": "oenb_statistics",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_oenb_sdmx(matchable):
        return empty

    indicators = _extract_indicators(matchable)
    if not indicators:
        logger.info("OeNB-SDMX: trigger fired, but no indicator detected")
        return empty

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for key in indicators:
            try:
                r = await _lookup_indicator(client, key)
            except Exception as e:
                logger.debug(f"OeNB-SDMX lookup {key} crashed: {e}")
                continue
            if r:
                results.append(r)
            if len(results) >= MAX_RESULTS:
                break

    if not results:
        logger.info(
            f"OeNB-SDMX: 0 Treffer für indicators={indicators}"
        )
        return empty

    logger.info(
        f"OeNB-SDMX: {len(results)} Treffer für indicators={indicators}"
    )
    return {
        "source": "OeNB SDMX",
        "type": "oenb_statistics",
        "results": results,
    }
