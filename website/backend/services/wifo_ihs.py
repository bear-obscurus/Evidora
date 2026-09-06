"""WIFO + IHS Konjunkturprognosen — österreichische Wirtschaftsforschung.

Datenquelle: kuratierte JSON aus den Quartals-Pressemeldungen der beiden
führenden österreichischen Wirtschaftsforschungsinstitute (WIFO + IHS).
Beide publizieren synchron 4 Mal jährlich; deren Konsens-Werte werden in
österreichischen Medien sehr häufig zitiert (BIP-Wachstum, Inflation,
Arbeitslosigkeit).

Use-Case:
- "WIFO erwartet 1,2 % Wachstum 2026"
- "IHS-Prognose Inflation"
- "Konjunkturprognose Österreich"
- "Wirtschaftsforschungsinstitut"
- "Rezession Österreich"
"""

import json
import logging
import os
from services._schreibweise import normalisiere, norm_terme

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "wifo_ihs.json",
)

_cache: dict | None = None


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_AT_CONTEXT_TERMS = norm_terme(
    "österreich", "austria", "österreichisch",
    "in at", "an at", "des at",
)
_WIFO_IHS_TERMS = norm_terme(
    "wifo", "ihs", "ihs-prognose", "ihs prognose",
    "wirtschaftsforschungsinstitut",
    "konjunkturprognose", "konjunktur-prognose",
    "wirtschaftsprognose österreich",
    "bip-prognose", "bip prognose",
    "inflations-prognose", "inflationsprognose",
    "wifo-prognose", "wifo prognose",
)


def _claim_mentions_wifo_ihs(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _WIFO_IHS_TERMS)
    if has_term:
        return True
    # Composite: ('wachstum' / 'rezession' / 'BIP') + AT-Kontext
    has_econ = any(t in claim_lc for t in (
        "bip-wachstum", "bip wachstum", "wirtschaftswachstum",
        "rezession", "konjunktur", "aufschwung", "abschwung",
        "wirtschaftserholung",
    ))
    has_at = any(t in claim_lc for t in _AT_CONTEXT_TERMS)
    if has_econ and has_at:
        return True
    return False


def claim_mentions_wifo_ihs_cached(claim: str) -> bool:
    return _claim_mentions_wifo_ihs(normalisiere(claim or ""))


# ---------------------------------------------------------------------------
# Static load
# ---------------------------------------------------------------------------
def _load_static_json() -> dict | None:
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(STATIC_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "data" not in data:
            logger.warning("wifo_ihs.json missing 'data' key")
            return None
        _cache = data
        logger.info("WIFO/IHS data loaded: 1 dataset")
        return _cache
    except FileNotFoundError:
        logger.warning(f"wifo_ihs.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"wifo_ihs.json load failed: {e}")
        return None


async def fetch_wifo_ihs(client=None):
    data = _load_static_json()
    if not data:
        return []
    return [data]


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _de_pct(v):
    if v is None:
        return "?"
    return f"{v}".replace(".", ",")


def _build_results(fact: dict, claim_lc: str) -> list[dict]:
    data = fact.get("data") or {}
    src = fact.get("source_url") or ""
    label = fact.get("source_label") or "WIFO + IHS"

    # Prognose-Runde kommt aus den DATEN, nicht aus dem Code. Bis 2026-09-05
    # stand "1/2026 (März 2026)" hier fest verdrahtet — ein Daten-Refresh
    # allein haette die falsche Runde weiter angezeigt.
    runde = data.get("prognose_runde") or "WIFO/IHS-Konjunkturprognose"

    headline = (
        f"{runde}: BIP 2026 +{_de_pct(data.get('bip_wachstum_2026_pct_real_wifo'))} % "
        f"(WIFO) / +{_de_pct(data.get('bip_wachstum_2026_pct_real_ihs'))} % (IHS) — "
        f"also WACHSTUM, keine Rezession; 2025 war mit "
        f"+{_de_pct(data.get('bip_2025_ist_pct_real'))} % bereits ein Erholungsjahr. "
        f"Inflation 2026 {_de_pct(data.get('inflation_2026_pct_wifo'))} % (WIFO) / "
        f"{_de_pct(data.get('inflation_2026_pct_ihs'))} % (IHS), deutlich ÜBER dem "
        f"EZB-Ziel. Arbeitslosenquote (nationale Definition) steigt 2026 leicht auf "
        f"{_de_pct(data.get('arbeitslosenquote_nat_2026_pct'))} % "
        f"(2025: {_de_pct(data.get('arbeitslosenquote_nat_2025_pct'))} %)."
    )

    description_parts = [
        f"2027-Ausblick: BIP +{_de_pct(data.get('bip_wachstum_2027_pct_real_wifo'))} % "
        f"(WIFO) / +{_de_pct(data.get('bip_wachstum_2027_pct_real_ihs'))} % (IHS), "
        f"Arbeitslosenquote {_de_pct(data.get('arbeitslosenquote_nat_2027_pct'))} %.",
        f"Budgetdefizit 2026: {_de_pct(data.get('budgetdefizit_2026_pct_bip'))} % des BIP.",
        data.get("konjunktur_charakterisierung", ""),
        data.get("inflation_charakterisierung", ""),
        data.get("arbeitsmarkt_charakterisierung", ""),
        data.get("ams_methodologie_caveat", ""),
    ]
    if data.get("treiber_2026"):
        description_parts.append("Treiber 2026: " + ", ".join(data["treiber_2026"]) + ".")
    if data.get("risiken_2026"):
        description_parts.append("Risiken 2026: " + ", ".join(data["risiken_2026"]) + ".")

    return [{
        "indicator_name": f"{runde} — Österreich",
        "indicator": "wifo_ihs_main",
        "country": "AUT",
        "country_name": "Österreich",
        "year": "2026",
        "value": data.get("bip_wachstum_2026_pct_real_wifo"),
        "display_value": headline,
        "description": " ".join(p for p in description_parts if p),
        "url": src,
        "source": label,
    }]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_wifo_ihs(analysis: dict) -> dict:
    empty = {
        "source": "WIFO + IHS",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = normalisiere(f"{original} {claim}")

    if not _claim_mentions_wifo_ihs(matchable):
        return empty

    data = _load_static_json()
    if not data:
        return empty

    return {
        "source": "WIFO + IHS",
        "type": "official_data",
        "results": _build_results(data, matchable),
    }
