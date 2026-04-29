"""OeNB — Oesterreichische Nationalbank.

Datenquelle: kuratierte JSON aus OeNB-Statistik + Geschäftsbericht.
Live-API der OeNB existiert (https://www.oenb.at/isaweb/webservice), ist
aber komplex; für die häufigsten Boulevard-Faktencheck-Themen reicht eine
schlanke kuratierte Liste:

- EZB-Leitzins (aktuell 2,15 %)
- OeNB-Inflations-/BIP-Prognose
- Wechselkurse EUR/USD, EUR/CHF
- Wichtige Hinweise (Leitzins ≠ Sparzins; Österreich kann EZB-Leitzins
  nicht eigenständig ändern)

Use-Case:
- "EZB-Leitzins ist auf X gesunken/gestiegen"
- "Österreich verlässt den Euro / kehrt zum Schilling zurück"
- "OeNB sagt Inflation X voraus"
- "Sparzins / Kreditzins — Banken zahlen mehr/weniger als Leitzins"
"""

import json
import logging
import os

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "oenb.json",
)

_cache: dict | None = None


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_OENB_TERMS = (
    "oenb", "oenb-prognose",
    "österreichische nationalbank", "oesterreichische nationalbank",
    "ezb-leitzins", "ezb leitzins",
    "leitzins ezb", "leitzins der ezb",
    "ezb-zinssatz", "ezb zinssatz",
    "geldpolitik europa", "geldpolitik eurozone",
    "wechselkurs eur", "euro-dollar", "euro/dollar",
    "schilling zurück", "österreich verlässt den euro",
    "österreich euro austritt",
)


def _claim_mentions_oenb(claim_lc: str) -> bool:
    has_term = any(t in claim_lc for t in _OENB_TERMS)
    if has_term:
        return True
    # Composite: 'leitzins' alleine + AT-/EU-Kontext
    has_leitzins = "leitzins" in claim_lc
    has_at_eu = any(t in claim_lc for t in (
        "österreich", "europa", "eurozone", "ezb", "euro",
    ))
    if has_leitzins and has_at_eu:
        return True
    # Composite: 'sparzins' / 'kreditzins' + AT
    has_zins = any(t in claim_lc for t in (
        "sparzins", "kreditzins", "hypothekenzins",
    ))
    has_at = any(t in claim_lc for t in (
        "österreich", "austria",
    ))
    if has_zins and has_at:
        return True
    return False


def claim_mentions_oenb_cached(claim: str) -> bool:
    return _claim_mentions_oenb((claim or "").lower())


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
            logger.warning("oenb.json missing 'data' key")
            return None
        _cache = data
        logger.info("OeNB data loaded: 1 dataset")
        return _cache
    except FileNotFoundError:
        logger.warning(f"oenb.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"oenb.json load failed: {e}")
        return None


async def fetch_oenb(client=None):
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
    label = fact.get("source_label") or "OeNB"

    headline = (
        f"OeNB-Stand April 2026: EZB-Leitzins (Hauptrefinanzierung) = "
        f"{_de_pct(data.get('ezb_leitzins_aktuell_pct'))} %, "
        f"Einlagensatz = {_de_pct(data.get('ezb_einlagensatz_aktuell_pct'))} %; "
        f"unverändert seit {data.get('ezb_konstante_seit', '')}. "
        f"OeNB-Inflationsprognose AT 2026: "
        f"{_de_pct(data.get('oenb_inflationsprognose_at_2026_pct'))} %; "
        f"BIP 2026: +{_de_pct(data.get('oenb_bip_prognose_at_2026_pct'))} %."
    )

    description_parts = [
        data.get("ezb_zinszyklus_2025_2026", ""),
    ]
    for hinweis in data.get("wichtige_hinweise") or []:
        description_parts.append(hinweis)

    results: list[dict] = [{
        "indicator_name": "OeNB EZB-Leitzins + Prognose-Stand 2026",
        "indicator": "oenb_main",
        "country": "AUT",
        "country_name": "Österreich",
        "year": "2026",
        "value": data.get("ezb_leitzins_aktuell_pct"),
        "display_value": headline,
        "description": " ".join(p for p in description_parts if p),
        "url": src,
        "source": label,
    }]

    # Spezial-Counter wenn Claim "Österreich verlässt Euro" o.ä.
    if any(s in claim_lc for s in (
        "schilling zurück", "österreich euro austritt",
        "österreich verlässt euro", "öxit",
        "österreich raus aus dem euro",
    )):
        results.insert(0, {
            "indicator_name": "OeNB-Counter: Österreich + Euro",
            "indicator": "oenb_euro_austritt_counter",
            "country": "AUT", "country_name": "Österreich",
            "year": "2026",
            "display_value": (
                "STRUKTURELL FALSCH: Österreich kann nicht 'einfach' aus dem "
                "Euro austreten. Ein Euro-Austritt würde einen EU-Austritt "
                "voraussetzen (Art. 50 EUV) — der wiederum verfassungsrechtlich "
                "in Österreich eine Volksabstimmung nach B-VG Art. 50 erfordert. "
                "Die OeNB ist Teil des Eurosystems; ihr Gouverneur sitzt im "
                "EZB-Rat. Eine Rückkehr zum Schilling ist seit dem Euro-"
                "Beitritt 1999 ohne EU-Austritt rechtlich nicht möglich. "
                "VERDICT-EMPFEHLUNG: 'false' mit Confidence 0.9."
            ),
            "description": (
                "Rechtsgrundlage Euro: Vertrag von Maastricht 1992, Art. 121 "
                "EU-Vertrag (jetzt AEUV). Österreich Euro-Beitritt: 01.01.1999. "
                "Schilling-Banknoten haben seit 28.02.2002 keinen gesetzlichen "
                "Zahlungsmittelstatus mehr."
            ),
            "url": src, "source": label,
        })

    return results


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_oenb(analysis: dict) -> dict:
    empty = {
        "source": "OeNB",
        "type": "official_data",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_oenb(matchable):
        return empty

    data = _load_static_json()
    if not data:
        return empty

    return {
        "source": "OeNB",
        "type": "official_data",
        "results": _build_results(data, matchable),
    }
