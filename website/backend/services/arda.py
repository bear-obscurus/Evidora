"""ARDA — Association of Religion Data Archives.

Quelle: ARDA (Pennsylvania State University, https://thearda.com/) — die
zentrale akademische Sammlung quantitativer Religionsdaten:
- 1.400+ quantitative Datensätze (Bevölkerungs-Religionsstatistiken,
  Konfessions-Mitgliederzahlen, Glaubensindex-Surveys)
- 900+ religionssoziologische Surveys (General Social Survey,
  Pew Religious Landscape, U.S. Religion Census, Baylor Religion Survey)
- Aggregiert Pew Research Center, WCD World Christian Database,
  Glenmary U.S. Religion Census, REMID + DESTATIS + Statistik Austria
- Lizenz: Public-Use / free academic
  (https://www.thearda.com/about/faq#policy)

Pack-Approach (KEIN Live-Crawler):
ARDA bietet keine offizielle Public-API mit stabilen Endpoints für
Faktencheck-Tooling — Daten werden als kuratierte Bulk-Downloads + via
SPSS/Stata-Variablen-Dictionaries publiziert. Für die häufigsten
Religions-Faktencheck-Themen reicht eine schlanke kuratierte JSON aus,
die die wichtigsten ARDA-/Pew-Aggregate enthält:
- Globale Religionsverteilung 2020 (Pew Global Religious Landscape)
- AT-Konfessionen (Statistik Austria Registerzählung 2021)
- DE-Konfessionen (DESTATIS Mikrozensus 2023)
- US-Denominationen (U.S. Religion Census 2020 + Pew RLS)
- Pew 'Future of World Religions' 2050-Projektionen

Use-Case-Trigger:
- "ARDA", "Association of Religion Data Archives"
- "Religionsstatistik [Land]", "Konfession USA"
- "Religious Demographics", "Religions-Anteile [global/Land]"
- "Konfessionen Deutschland/Österreich/USA"
- "Anteil Christen/Muslime/Konfessionslose [Land/weltweit]"
- "Religion 2050", "Pew Future of World Religions"

Cross-Cluster:
- religionsgemeinschaften_pack.py — Konsens-/Mythen-Faktencheck
  (Sekten, Religion-Gewalt-Korrelation, Vatikan-Finanzen). ARDA
  ergänzt rein DESKRIPTIVE Religions-Demografie.
- destatis.py / statistik_austria — sind teils Quelle für ARDA-Aggregate.

Politische Guardrails (memory/project_political_guardrails.md):
- NUR deskriptive Religions-Statistik
- KEINE Bewertung 'Säkularisierung ist gut/schlecht'
- KEINE Aussage über Glaubens-Wahrheit oder Religion-Hierarchie
- Demografie-Projektionen 2050 explizit als 'Pew deterministische
  Hochrechnung, kein Politik-Forecast' deklariert

24h-In-Memory-Cache: Lesen der JSON ist <1 ms, aber wir folgen dem
Service-Pattern (polite_client-Import + Cache-Hook für künftigen
Live-Mirror-Switch, falls ARDA jemals eine stabile JSON-API publiziert).
"""

# WIRING für main.py (NICHT in dieser Datei vornehmen):
#   from services.arda import search_arda, claim_mentions_arda_cached
#   if claim_mentions_arda_cached(claim):
#       tasks.append(cached("ARDA", search_arda, analysis))
#       queried_names.append("ARDA")
#
# WIRING für services/reranker.py (Whitelist):
#   "ARDA" in SOURCE_WHITELIST eintragen.
#
# WIRING für services/data_updater.py: KEIN Prefetch nötig — die
# Religions-Statistik-JSON ist statisch und wird beim ersten Lookup
# einmalig in den Modul-Cache geladen.

from __future__ import annotations

import json
import logging
import os
import time
from functools import lru_cache

# polite_client wird hier importiert, damit ein künftiger Switch auf einen
# ARDA-Live-Mirror (z. B. ARDA Open Data S3-Bucket) ohne Service-Rewrite
# möglich ist — analog zu cepii.py / ahrq.py.
from services._http_polite import polite_client  # noqa: F401

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------
STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "arda.json",
)

CACHE_TTL_S = 24 * 60 * 60  # 24h
MAX_RESULTS = 5

# Modul-Cache: (loaded_at, payload)
_data_cache: tuple[float, dict] | None = None


# ---------------------------------------------------------------------------
# Trigger-Vokabular
# ---------------------------------------------------------------------------
# Direkt-Trigger: namentliche Erwähnung von ARDA / Pew Religious Landscape
_DIRECT_TERMS = (
    "arda",
    "association of religion data archives",
    "religion data archives",
    "pew religious landscape",
    "pew religion",
    "us religion census", "u.s. religion census",
    "world christian database", "wcd",
    "future of world religions",
    "religion 2050", "religionen 2050",
    "global religious landscape", "globale religionsverteilung",
)

# Religions-Demografie-Begriffe — triggern in Kombination mit Land oder
# 'weltweit/global'
_RELIGION_DEMO_TERMS = (
    "religionsstatistik", "religions-statistik",
    "religionsanteil", "religions-anteil", "religions-anteile",
    "religionsanteile",
    "religionsverteilung", "religions-verteilung",
    "religionszugehörigkeit", "religionszugehoerigkeit",
    "religionen in", "religionen ",
    "konfession", "konfessionen", "konfessionell",
    "konfessionslos", "konfessionslose", "konfessionslosen",
    "denomination", "denominations", "denominational",
    "religious demographics", "religious demography",
    "religious composition", "religious landscape",
    "religious adherents", "religious affiliation",
)

# Religions-Subjekte für die Anteils-Frage
_RELIGION_GROUPS = (
    "christen", "christlich", "katholisch", "katholiken",
    "evangelisch", "evangelische", "protestanten", "protestantisch",
    "orthodox", "orthodoxe",
    "muslim", "muslime", "muslimisch", "islamisch",
    "juden", "jüdisch", "juedisch", "jewish",
    "buddhisten", "buddhistisch", "buddhist",
    "hindus", "hinduistisch", "hindu",
    "atheist", "atheisten", "atheistisch",
    "agnostiker", "agnostisch",
    "mormonen", "lds", "mormonisch",
    "evangelikal", "evangelikale", "evangelicals",
)

# Anteils-Verben / Mengen-Begriffe — Kontext, dass nach DEMOGRAFIE gefragt
# wird (nicht z. B. nach Theologie)
_QUANTITY_TERMS = (
    "anteil", "anteile", "prozent", "prozentual",
    "verteilung", "mehrheit", "minderheit",
    "wachstum", "wächst", "waechst", "schrumpft", "rückgang", "rueckgang",
    "anzahl", "millionen", "milliarden",
    "wieviele", "wie viele", "wie viel",
    "leben", "lebt", "leben in",
    "share", "percentage", "majority", "minority",
    "growth", "growing", "declining", "how many",
)

# Länder-/Region-Bezüge — gemeinsam mit demo-Term reicht für Trigger
_COUNTRY_TERMS = (
    "österreich", "oesterreich", "austria", "at",
    "deutschland", "germany", "de",
    "schweiz", "switzerland", "ch",
    "usa", "us ", "u.s.", "united states", "amerika",
    "weltweit", "global", "welt", "world",
    "dach",
)


def _claim_mentions_arda(claim_lc: str) -> bool:
    """Interner Trigger-Check (lowercase'tes Claim-Text)."""
    # 1) Direkter ARDA-/Pew-/RLS-Bezug
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    # 2) Religions-Demografie-Begriff + Länder-Bezug
    has_demo = any(t in claim_lc for t in _RELIGION_DEMO_TERMS)
    has_country = any(t in claim_lc for t in _COUNTRY_TERMS)
    if has_demo and has_country:
        return True

    # 3) Religions-Gruppe + Quantitäts-Verb + Länder-Bezug
    has_group = any(t in claim_lc for t in _RELIGION_GROUPS)
    has_quantity = any(t in claim_lc for t in _QUANTITY_TERMS)
    if has_group and has_quantity and has_country:
        return True

    return False


@lru_cache(maxsize=2048)
def claim_mentions_arda_cached(claim: str) -> bool:
    """Public cache-fähiger Trigger-Check (LRU pro normalisiertem Claim)."""
    return _claim_mentions_arda((claim or "").lower())


# ---------------------------------------------------------------------------
# Static load (24h-Cache)
# ---------------------------------------------------------------------------
def _load_data() -> dict | None:
    global _data_cache
    now = time.time()
    if _data_cache is not None and (now - _data_cache[0]) < CACHE_TTL_S:
        return _data_cache[1]
    try:
        with open(STATIC_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "facts" not in data:
            logger.warning("arda.json missing 'facts' key")
            return None
        _data_cache = (now, data)
        logger.info(f"ARDA data loaded: {len(data.get('facts') or [])} facts")
        return data
    except FileNotFoundError:
        logger.warning(f"arda.json not found at {STATIC_JSON_PATH}")
        return None
    except Exception as e:
        logger.warning(f"arda.json load failed: {type(e).__name__}: {e}")
        return None


async def fetch_arda(client=None):
    data = _load_data()
    if not data:
        return []
    return data.get("facts") or []


# ---------------------------------------------------------------------------
# Fact-Match
# ---------------------------------------------------------------------------
def _fact_matches(fact: dict, claim_lc: str) -> bool:
    """Match-Heuristik: fact-eigene trigger_terms + Land + Religions-Gruppe."""
    # Fact-spezifische Trigger-Terms haben Vorrang
    for t in (fact.get("trigger_terms") or []):
        if t and t.lower() in claim_lc:
            return True

    # Land-/Religions-Gruppen-Heuristik: bei "global" wenn Claim explizit
    # 'weltweit/global/welt' nennt
    country = (fact.get("country") or "").upper()
    country_name = (fact.get("country_name") or "").lower()
    topic = (fact.get("topic") or "").lower()

    if country == "WORLD" and any(
        s in claim_lc for s in ("weltweit", "global", "welt", "world")
    ):
        return True
    if country == "AUT" and any(
        s in claim_lc for s in ("österreich", "oesterreich", "austria")
    ):
        return True
    if country == "DEU" and any(
        s in claim_lc for s in ("deutschland", "germany")
    ):
        return True
    if country == "USA" and any(
        s in claim_lc for s in ("usa", "u.s.", "united states", "amerika")
    ):
        return True
    if country_name and country_name in claim_lc:
        return True
    if topic and topic.replace("_", " ") in claim_lc:
        return True

    return False


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _data_lines(d: dict) -> str:
    """Flacht das `data`-Dict zu einer lesbaren Schlüssel-Wert-Liste."""
    parts: list[str] = []
    for key, val in d.items():
        if key in ("kontext", "context"):
            continue
        if isinstance(val, (str, int, float)) and str(val).strip():
            label = key.replace("_", " ").strip()
            parts.append(f"{label.capitalize()}: {val}")
    return " | ".join(parts)


def _build_result(fact: dict, data_root: dict) -> dict:
    d = fact.get("data") or {}
    src_url = data_root.get("source_url") or "https://thearda.com/"
    secondary_url = data_root.get("secondary_url") or ""
    label = data_root.get("source_label") or "ARDA"
    notes = fact.get("context_notes") or []
    headline = fact.get("headline") or "?"
    year = str(fact.get("year") or "")

    display = f"{headline}. {_data_lines(d)}"
    description = (
        (d.get("kontext") or "") + " " + " | ".join(notes)
    ).strip()

    return {
        "indicator_name": headline,
        "indicator": fact.get("id") or fact.get("topic") or "arda_fact",
        "country": fact.get("country") or "—",
        "country_name": fact.get("country_name") or "",
        "year": year,
        "topic": fact.get("topic", ""),
        "display_value": display,
        "description": description,
        "url": src_url,
        "secondary_url": secondary_url,
        "source": label,
    }


def _caveat_result(data_root: dict) -> dict:
    return {
        "indicator_name": "KONTEXT: ARDA-Aggregat-Status",
        "indicator": "arda_caveat",
        "country": "—",
        "year": "",
        "topic": "arda_context",
        "display_value": (
            "ARDA aggregiert primär Pew Research Center, WCD World Christian "
            "Database, U.S. Religion Census, DESTATIS, Statistik Austria und "
            "akademische Surveys. Die Anteile sind DESKRIPTIV (Selbst-"
            "Auskunft + Register-Daten), KEINE Aussage über Glaubens-"
            "Intensität oder Religions-Praxis. 2050-Projektionen (Pew) sind "
            "DEMOGRAFISCHE Hochrechnungen — Fertilität + Migration, "
            "KEIN Politik-Forecast und KEINE Konversions-Prognose."
        ),
        "description": (
            "ARDA-Lizenz: Public-Use / free academic. "
            "Datenstand: kuratierter Snapshot 2020-2023 mit Pew-2050-Forward."
        ),
        "url": "https://thearda.com/",
        "source": data_root.get("source_label") or "ARDA",
    }


# ---------------------------------------------------------------------------
# Public Search
# ---------------------------------------------------------------------------
async def search_arda(analysis: dict) -> dict:
    """Liefert kuratierte ARDA-/Pew-Religions-Statistiken passend zum Claim."""
    empty: dict = {
        "source": "ARDA",
        "type": "religion_statistics",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_arda(matchable):
        return empty

    data_root = _load_data()
    if not data_root:
        return empty

    matched: list[dict] = []
    for fact in (data_root.get("facts") or []):
        if _fact_matches(fact, matchable):
            matched.append(_build_result(fact, data_root))
            if len(matched) >= MAX_RESULTS:
                break

    if not matched:
        # Trigger feuerte (z. B. namentlich "ARDA"), aber keine fact-spezifische
        # Match — wir liefern keine generischen Hits, sondern leeres Result,
        # damit der Synthesizer keine unpassenden Zahlen zitiert.
        logger.info(
            f"ARDA: trigger fired but 0 fact matches for claim='{claim[:80]}'"
        )
        return empty

    matched.append(_caveat_result(data_root))
    logger.info(f"ARDA: {len(matched) - 1} fact hits for claim='{claim[:80]}'")
    return {
        "source": "ARDA",
        "type": "religion_statistics",
        "results": matched,
    }
