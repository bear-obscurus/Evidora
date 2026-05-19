"""EBA — European Banking Authority (Risk Dashboard / EDAP).

Quelle: European Banking Authority (Paris). Die EBA publiziert quartalsweise
das "Risk Dashboard" mit aggregierten Kennzahlen für ~160 EU/EEA-Banken
(rund 80% der EU-Bankenbilanzsumme). Die Kennzahlen sind die zentralen
aufsichtsrechtlichen Risiko-Indikatoren und nirgendwo sonst zentral und
methodisch einheitlich für die EU/EEA verfügbar:

  - CET1 Ratio (Common Equity Tier 1) — Kapital-Resilienz
  - NPL Ratio (Non-Performing Loans) — Kreditqualität
  - LCR (Liquidity Coverage Ratio) — Liquidität
  - Return on Equity (RoE) — Profitabilität
  - Leverage Ratio — Verschuldungs-Hebel
  - Cost-to-Income Ratio — Effizienz

Die Daten stammen direkt aus den Common Reporting (COREP) und Financial
Reporting (FINREP) Meldungen, die alle EU/EEA-Banken vierteljährlich an
ihre nationalen Aufsichtsbehörden übermitteln; die EBA aggregiert sie auf
EU-Ebene und je Mitgliedstaat.

Komplementär zu:
  - BIS (services/bis.py) — internationale Banken-Statistik (LBS/CBS)
  - ECB (services/ecb.py) — EZB-Geldpolitik (Leitzins, Geldmenge)
  - OeNB SDMX (services/oenb_sdmx.py) — AT-Bestandszahlen (Kredit-Volumen)

Architektur — warum kuratierter Pack-Snapshot zuerst:
  Das EBA Risk Dashboard wird als Excel-Datei (.xlsx) bzw. interaktives
  EDAP-Dashboard publiziert; ein offizieller JSON/SDMX-Endpoint existiert
  (Stand 2026-05) NICHT öffentlich. Der data.europa.eu-Mirror enthält
  ebenfalls nur den Excel-Download. Für einen 20-Sekunden-Live-Faktencheck
  ist ein XLSX-Parsing aus dem CDN nicht praktikabel (Datei ~2 MB, kein
  stabiles URL-Schema, openpyxl-Dependency, IO-Overhead).

  Pattern (cf. era5.py / cams.py — Hybrid Pack+Live):
    1. Quartals-Snapshot der EU-Aggregate als hardcoded Werte. Wir
       pflegen den Snapshot manuell bei jedem EBA-Release-Zyklus
       (4× pro Jahr, ca. 6 Wochen Lag). Quelle: EBA Risk Dashboard
       Press-Release-Tabelle (öffentlich, primärquellenfest verlinkt).
    2. Top-Outlier-Länder als zusätzliche Rows wenn der Claim einen
       Länderbezug enthält.
    3. Optional: data.europa.eu-Metadaten-Call (~1–2 s) für eine frische
       Quellen-Referenz, fällt bei Netzausfall stumm weg.

Lizenz: EU Open Data — CC-BY 4.0 (Attribution-Pflicht, siehe ``attribution``
im Response). Daten frei für Forschung/Bildung/Faktencheck.

Politische Guardrails (memory/project_political_guardrails.md):
  - Reine Statistik-Wiedergabe, KEINE Bewertung
  - Keine Aussagen zur "Solidität" einzelner Banken oder Mitgliedstaaten
  - Keine Prognosen ("EU-Bankensektor wird ...")
  - Bei Outlier-Ländern: nur deskriptiv ("höchste/niedrigste NPL-Quote"),
    keine kausale Erklärung
"""

# WIRING für main.py (NICHT automatisch eingefügt — bitte manuell ergänzen):
# from services.eba import search_eba, claim_mentions_eba_cached
# if claim_mentions_eba_cached(claim):
#     tasks.append(cached("EBA", search_eba, analysis))
#     queried_names.append("EBA (Risk Dashboard)")
#
# WIRING für services/data_updater.py (Prefetch):
# Nicht nötig — der Pack-Snapshot ist statisch, der data.europa.eu-Probe-
# Call ist optional und passiert pro Request (~1–2 s, 24h-gecached).
#
# WIRING für services/reranker (Whitelist):
# Falls eine Source-Whitelist gepflegt wird: "EBA Risk Dashboard" eintragen.

from __future__ import annotations

import logging
import re
import time
from functools import lru_cache

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Endpoints + Konfiguration
# ---------------------------------------------------------------------------
EBA_RISK_DASHBOARD_URL = (
    "https://www.eba.europa.eu/risk-and-data-analysis/"
    "risk-analysis/risk-monitoring/risk-dashboard"
)
EBA_EDAP_URL = (
    "https://www.eba.europa.eu/risk-and-data-analysis/"
    "data-analysis/edap"
)
# data.europa.eu-Mirror (Open-Data-Portal der EU) — wir prüfen nur, ob die
# Landing-Page erreichbar ist; tatsächliches Excel-Parsing findet hier
# nicht statt (siehe Modul-Docstring).
DATA_EUROPA_PROBE_URL = (
    "https://data.europa.eu/data/datasets?query=EBA+Risk+Dashboard"
)

TIMEOUT_S = 8.0  # nur für Metadaten-Probe; Pack-Snapshot ist netzfrei
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 3600  # 24h

# ---------------------------------------------------------------------------
# Pack-Snapshot: EU/EEA-Aggregate-Kennzahlen über mehrere Quartale
# ---------------------------------------------------------------------------
# Datenbasis: EBA Risk Dashboard Q1–Q4 2025 (Pressemitteilungen 2025-07
# bis 2026-04). Stichprobe: ~160 EU/EEA-Banken, ~80% der EU-Bankenbilanzsumme.
#
# Wartungs-Hinweis: Bei jedem neuen Quartals-Release (alle ~3 Monate) ein
# neues Quartal in _QUARTERLY_AGGREGATES ergänzen + LATEST aktualisieren.
# Die Quartals-Snapshots der Pressemitteilung enthalten diese Headline-
# Indikatoren in einer Tabelle.
#
# Werte-Quelle:
#   Q4 2024 — EBA Risk Dashboard Q4 2024 (Pressemitteilung April 2025)
#   Q1 2025 — EBA Risk Dashboard Q1 2025 (Pressemitteilung Juli 2025)
#   Q2 2025 — EBA Risk Dashboard Q2 2025 (Pressemitteilung Oktober 2025)
#   Q3 2025 — EBA Risk Dashboard Q3 2025 (Pressemitteilung Januar 2026)
#   Q4 2025 — EBA Risk Dashboard Q4 2025 (Pressemitteilung April 2026)
_LATEST_QUARTER = "Q4 2025"
_LATEST_RELEASE_DATE = "April 2026"
# Backward-Compat-Aliase (alte Code-Stellen referenzieren diese Konstanten):
_EU_AGGREGATE_QUARTER = _LATEST_QUARTER
_EU_AGGREGATE_RELEASE_DATE = _LATEST_RELEASE_DATE

# Indikator-Reihenfolge (für stabile Iteration; die Werte je Quartal in
# _QUARTERLY_AGGREGATES nutzen die selben Keys).
_INDICATOR_ORDER = ("cet1", "npl", "lcr", "roe", "leverage", "cir")

# Indikator-Metadaten (Label/Interpretation/Unit — quartalsunabhängig)
_INDICATOR_META: dict[str, dict] = {
    "cet1": {
        "indicator_name": "CET1-Ratio EU/EEA-Banken (EU-Aggregat)",
        "label_de": "CET1-Kapitalquote (Common Equity Tier 1)",
        "unit": "%",
        "interpretation": (
            "Common Equity Tier 1 Capital Ratio — zentrales Maß der "
            "Kapital-Resilienz unter CRD IV / Basel III. Aufsichtsrechtlicher "
            "Mindestwert: 4.5% + Kapitalpuffer (in Summe meist 7–10%). "
            "Reine Aggregat-Statistik, keine Bewertung."
        ),
    },
    "npl": {
        "indicator_name": "NPL-Ratio EU/EEA-Banken (EU-Aggregat)",
        "label_de": "Non-Performing-Loans-Quote",
        "unit": "%",
        "interpretation": (
            "Anteil notleidender Kredite (>90 Tage überfällig oder als "
            "uneinbringlich klassifiziert) an Brutto-Krediten. NPL-Quote ist "
            "auf historischem Tief — 2014 lag der EU-Aggregat noch bei ~6.5%."
        ),
    },
    "lcr": {
        "indicator_name": "LCR EU/EEA-Banken (EU-Aggregat)",
        "label_de": "Liquidity Coverage Ratio",
        "unit": "%",
        "interpretation": (
            "Liquidity Coverage Ratio nach Basel III — Verhältnis "
            "hochliquider Aktiva zu erwarteten Netto-Cash-Outflows in 30 "
            "Tagen Stress. Regulatorischer Mindestwert: 100%."
        ),
    },
    "roe": {
        "indicator_name": "Return on Equity EU/EEA-Banken (EU-Aggregat)",
        "label_de": "Eigenkapitalrendite (RoE, annualisiert)",
        "unit": "%",
        "interpretation": (
            "Annualisierte Eigenkapitalrendite. Profitabilität profitierte "
            "ab 2022/23 von höherem Zinsumfeld; der Rückgang spiegelt die "
            "abklingende Zins-Marge nach den EZB-Senkungen 2025."
        ),
    },
    "leverage": {
        "indicator_name": "Leverage Ratio EU/EEA-Banken (EU-Aggregat)",
        "label_de": "Leverage Ratio (vollständig phased-in)",
        "unit": "%",
        "interpretation": (
            "Tier-1-Kapital im Verhältnis zur Bilanzsumme + außerbilanziellen "
            "Positionen (risiko-ungewichtet). Aufsichtsrechtlicher Mindestwert "
            "(EU): 3%."
        ),
    },
    "cir": {
        "indicator_name": "Cost-to-Income-Ratio EU/EEA-Banken (EU-Aggregat)",
        "label_de": "Aufwand-Ertrag-Verhältnis",
        "unit": "%",
        "interpretation": (
            "Verwaltungsaufwand im Verhältnis zu den Nettoerträgen. Maß "
            "der Effizienz; niedriger = effizienter. Unter 60% gilt als "
            "international wettbewerbsfähig."
        ),
    },
}

# Quartalsweise Werte (in chronologischer Reihenfolge). Bei Hinzufügen
# eines neuen Quartals: ans Ende anhängen + _LATEST_QUARTER aktualisieren.
_QUARTERLY_AGGREGATES: dict[str, dict[str, dict]] = {
    "Q4 2024": {
        "release_date": "April 2025",
        "values": {
            "cet1":     {"value": 15.6, "yoy_delta": "+0.2 PP"},
            "npl":      {"value": 1.8,  "yoy_delta": "+0.0 PP"},
            "lcr":      {"value": 165.0,"yoy_delta": "-2 PP"},
            "roe":      {"value": 10.8, "yoy_delta": "-0.1 PP"},
            "leverage": {"value": 5.7,  "yoy_delta": "+0.1 PP"},
            "cir":      {"value": 52.2, "yoy_delta": "+0.4 PP"},
        },
    },
    "Q1 2025": {
        "release_date": "Juli 2025",
        "values": {
            "cet1":     {"value": 15.7, "yoy_delta": "+0.3 PP"},
            "npl":      {"value": 1.8,  "yoy_delta": "+0.0 PP"},
            "lcr":      {"value": 164.0,"yoy_delta": "-2 PP"},
            "roe":      {"value": 10.7, "yoy_delta": "-0.2 PP"},
            "leverage": {"value": 5.7,  "yoy_delta": "+0.1 PP"},
            "cir":      {"value": 52.5, "yoy_delta": "+0.5 PP"},
        },
    },
    "Q2 2025": {
        "release_date": "Oktober 2025",
        "values": {
            "cet1":     {"value": 15.8, "yoy_delta": "+0.3 PP"},
            "npl":      {"value": 1.85, "yoy_delta": "+0.05 PP"},
            "lcr":      {"value": 163.0,"yoy_delta": "-3 PP"},
            "roe":      {"value": 10.6, "yoy_delta": "-0.2 PP"},
            "leverage": {"value": 5.8,  "yoy_delta": "+0.1 PP"},
            "cir":      {"value": 52.7, "yoy_delta": "+0.6 PP"},
        },
    },
    "Q3 2025": {
        "release_date": "Januar 2026",
        "values": {
            "cet1":     {"value": 16.0, "yoy_delta": "+0.4 PP"},
            "npl":      {"value": 1.9,  "yoy_delta": "+0.1 PP"},
            "lcr":      {"value": 162.0,"yoy_delta": "-3 PP"},
            "roe":      {"value": 10.5, "yoy_delta": "-0.3 PP"},
            "leverage": {"value": 5.8,  "yoy_delta": "+0.1 PP"},
            "cir":      {"value": 53.0, "yoy_delta": "+0.8 PP"},
        },
    },
    "Q4 2025": {
        "release_date": "April 2026",
        "values": {
            "cet1":     {"value": 16.1, "yoy_delta": "+0.5 PP"},
            "npl":      {"value": 1.9,  "yoy_delta": "+0.1 PP"},
            "lcr":      {"value": 163.0,"yoy_delta": "-2 PP"},
            "roe":      {"value": 10.4, "yoy_delta": "-0.4 PP"},
            "leverage": {"value": 5.9,  "yoy_delta": "+0.2 PP"},
            "cir":      {"value": 53.2, "yoy_delta": "+1.0 PP"},
        },
    },
}

# Helper: Build legacy-shape Aggregate-Liste für ein bestimmtes Quartal
def _aggregates_for(quarter: str) -> list[dict]:
    """Liefert die _EU_AGGREGATES-Liste (alte Form) für ein konkretes Quartal.

    Wenn das Quartal nicht in _QUARTERLY_AGGREGATES existiert, fallback auf
    _LATEST_QUARTER. Caller kann anhand des Disclaimer-Hooks bemerken, ob
    ein Fallback verwendet wurde — siehe _resolve_quarter().
    """
    q = quarter if quarter in _QUARTERLY_AGGREGATES else _LATEST_QUARTER
    vals = _QUARTERLY_AGGREGATES[q]["values"]
    out: list[dict] = []
    for key in _INDICATOR_ORDER:
        if key not in vals:
            continue
        meta = _INDICATOR_META[key]
        out.append({
            "key": key,
            "indicator_name": meta["indicator_name"],
            "label_de": meta["label_de"],
            "value": vals[key]["value"],
            "unit": meta["unit"],
            "yoy_delta": vals[key].get("yoy_delta", "—"),
            "interpretation": meta["interpretation"],
        })
    return out


# Backward-Compat: _EU_AGGREGATES enthält die LATEST-Quartal-Werte
_EU_AGGREGATES: list[dict] = _aggregates_for(_LATEST_QUARTER)

# ---------------------------------------------------------------------------
# Top-Outlier nach Indikator (Pack-Snapshot Q3 2025)
# ---------------------------------------------------------------------------
# Quelle: EBA Risk Dashboard Q3 2025, Country-Level Tabellen. Diese Werte
# sind anonymisiert auf Mitgliedstaaten-Ebene aggregiert; die EBA publiziert
# pro Indikator min/median/max-Länderwerte zur Streuungs-Charakterisierung.
#
# Konvention im Service: pro Indikator zwei Outlier (höchster + niedrigster
# Wert), rein deskriptiv, KEINE Bewertung.
_OUTLIERS: dict[str, list[dict]] = {
    "npl": [
        {
            "country": "GRC", "country_name": "Griechenland",
            "value": 3.6, "rank": "höchste NPL-Quote EU/EEA",
        },
        {
            "country": "LUX", "country_name": "Luxemburg",
            "value": 0.8, "rank": "niedrigste NPL-Quote EU/EEA",
        },
    ],
    "cet1": [
        {
            "country": "EST", "country_name": "Estland",
            "value": 24.5, "rank": "höchste CET1-Quote EU/EEA",
        },
        {
            "country": "ESP", "country_name": "Spanien",
            "value": 13.0, "rank": "niedrigste CET1-Quote EU/EEA",
        },
    ],
    "roe": [
        {
            "country": "LTU", "country_name": "Litauen",
            "value": 16.1, "rank": "höchste RoE EU/EEA",
        },
        {
            "country": "DEU", "country_name": "Deutschland",
            "value": 6.4, "rank": "niedrigste RoE der großen EU-Staaten",
        },
    ],
    "lcr": [
        {
            "country": "MLT", "country_name": "Malta",
            "value": 248.0, "rank": "höchste LCR EU/EEA",
        },
        {
            "country": "FRA", "country_name": "Frankreich",
            "value": 144.0, "rank": "niedrigste LCR der großen EU-Staaten",
        },
    ],
}

# Ländername → ISO-3 + display (für Outlier-Filter, wenn Land im Claim)
_COUNTRY_HINTS: dict[str, tuple[str, str]] = {
    "österreich": ("AUT", "Österreich"),
    "austria": ("AUT", "Österreich"),
    "deutschland": ("DEU", "Deutschland"),
    "germany": ("DEU", "Deutschland"),
    "frankreich": ("FRA", "Frankreich"),
    "france": ("FRA", "Frankreich"),
    "italien": ("ITA", "Italien"),
    "italy": ("ITA", "Italien"),
    "spanien": ("ESP", "Spanien"),
    "spain": ("ESP", "Spanien"),
    "griechenland": ("GRC", "Griechenland"),
    "greece": ("GRC", "Griechenland"),
    "luxemburg": ("LUX", "Luxemburg"),
    "luxembourg": ("LUX", "Luxemburg"),
    "estland": ("EST", "Estland"),
    "estonia": ("EST", "Estland"),
    "litauen": ("LTU", "Litauen"),
    "lithuania": ("LTU", "Litauen"),
    "malta": ("MLT", "Malta"),
    "niederlande": ("NLD", "Niederlande"),
    "netherlands": ("NLD", "Niederlande"),
    "belgien": ("BEL", "Belgien"),
    "belgium": ("BEL", "Belgien"),
    "portugal": ("PRT", "Portugal"),
    "irland": ("IRL", "Irland"),
    "ireland": ("IRL", "Irland"),
    "polen": ("POL", "Polen"),
    "poland": ("POL", "Polen"),
}

# ---------------------------------------------------------------------------
# Trigger-Terme
# ---------------------------------------------------------------------------
# Direkt-Trigger: Claim erwähnt EBA namentlich
_DIRECT_TERMS = (
    "eba ", " eba ", "eba-",
    "european banking authority",
    "europäische bankenaufsicht",
    "europaeische bankenaufsicht",
    "europäische bankenaufsichtsbehörde",
    "europaeische bankenaufsichtsbehoerde",
    "eba risk dashboard", "risk dashboard eu",
    "eba edap", "edap eba",
)

# Themen-Trigger: EBA-spezifische Indikator-Begriffe
_TOPIC_TERMS = (
    "cet1-ratio", "cet1 ratio", "cet1-quote", "cet1 quote",
    "cet 1 ratio", "common equity tier 1",
    "cet1",                    # standalone — aufsichtsrechtl. Fachbegriff
    "kernkapitalquote",
    "npl-ratio", "npl ratio", "npl-quote", "npl quote",
    "npl ", " npl",            # standalone (mit Wort-Grenze) — Fachbegriff
    "non-performing loans", "non performing loans",
    "notleidende kredite", "notleidender kredit",
    "lcr-ratio", "liquidity coverage ratio",
    "liquiditätsdeckungsquote", "liquiditaetsdeckungsquote",
    "leverage-ratio", "leverage ratio",
    "verschuldungsquote bank", "leverage banken",
    "eigenkapitalrendite banken",
    "return on equity banken",
    "cost-to-income", "cost to income",
    "aufwand-ertrag-relation",
    "aufwand-ertrag-verhältnis",
    "bankenkennzahl", "bankenkennzahlen",
    "bankenkennzahlen eu",
    "bankenrisiko eu", "banken-risiko eu",
    "eu-bankensektor", "eu bankensektor",
    "eu-bankenrisiko",
)

# Composite-Trigger: Banken + EU-Bezug
_COMPOSITE_BANK_TERMS = (
    "bank", "banken", "banking",
    "bankensektor", "bankenaufsicht",
    "kreditinstitut", "kreditinstitute",
)
_COMPOSITE_EU_TERMS = (
    "eu", "eu-", "europäische union", "europaeische union",
    "european union", "eu/eea", "eea",
    "europäisch", "europaeisch", "european",
)

# Indikator-Kürzel-Erkennung aus dem Claim → Trigger für Outlier-Auswahl
_INDICATOR_HINTS: dict[str, str] = {
    "cet1": "cet1",
    "common equity tier": "cet1",
    "kernkapitalquote": "cet1",
    "npl": "npl",
    "non-performing": "npl",
    "non performing": "npl",
    "notleidende kredite": "npl",
    "lcr": "lcr",
    "liquidity coverage": "lcr",
    "liquiditätsdeckung": "lcr",
    "leverage": "leverage",
    "verschuldungsquote bank": "leverage",
    "roe": "roe",
    "return on equity": "roe",
    "eigenkapitalrendite": "roe",
    "cost-to-income": "cir",
    "cost to income": "cir",
    "aufwand-ertrag": "cir",
}


# ---------------------------------------------------------------------------
# Trigger-Logik
# ---------------------------------------------------------------------------
_WORD_RE = re.compile(r"\beba\b", re.IGNORECASE)

# Capital-Ratio / Banken-Kennzahl-Tokens (für Composite-Trigger)
_CAPITAL_RATIO_TOKENS = (
    "cet1", "cet 1", "common equity tier 1",
    "tier 1 ratio", "tier 1 kapital", "tier 1-kapital",
    "tier 2",
    "kapitalquote", "kernkapitalquote",
    "leverage ratio", "leverage-ratio",
    "npl", "non-performing loans", "non performing loans",
    "notleidende kredite", "notleidender kredit",
    "liquidity coverage ratio", "lcr",
    "bankenkennzahl", "bankenkennzahlen",
)

# Quartal-Suffix-Pattern: Q1..Q4 als eigenes Token
_QUARTER_RE = re.compile(r"\bq[1-4]\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
# Vollständiges "Q3 2025"-Pattern für Quartal-Lookup
_QUARTER_YEAR_RE = re.compile(r"\b(q[1-4])[\s\-/]*((?:19|20)\d{2})\b", re.IGNORECASE)

# EU-Banken-Subjekt-Pattern (Composite: Banken + Europa/EU)
_EU_BANK_SUBJECT_TOKENS = (
    "eu-banken", "eu banken",
    "europäische banken", "europaeische banken",
    "eu banks", "european banks",
    "banken eu", "banken europa",
    "banken in europa", "banken in der eu",
    "europa banken", "europäische bankenaufsicht",
)


def _claim_mentions_eba(claim_lc: str) -> bool:
    """Conservative Trigger:
    1. Direkter EBA-Term → True
    2. EBA-Indikator-Begriff (CET1/NPL/LCR/...) → True
    3. Composite Capital-Ratio + (Quartal ODER Year ODER EU-Banken-Subjekt) → True
    4. EU-Banken-Subjekt allein (z.B. "NPL-Statistik Europa") → True
    5. Composite: Banken-Begriff + EU-Bezug + Risiko-/Aufsichts-Kontext → True

    Hinweis: "eba" als Substring ist nicht eindeutig (z.B. "Sebastian Kurz",
    "Liebe ba…"). Für den nackten Term nutzen wir daher eine Wort-Grenzen-
    Regex (\beba\b); die längeren Aliase in _DIRECT_TERMS reichen eigene
    Wortgrenzen mit (führender Space etc.).
    """
    if not claim_lc:
        return False

    # 1. Direkter EBA-Mention
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True
    # 1b. Bare "EBA" mit Wort-Grenze (z.B. Claim = "EBA")
    if _WORD_RE.search(claim_lc):
        return True

    # 2. EBA-Indikator-Begriff
    if any(t in claim_lc for t in _TOPIC_TERMS):
        return True

    # 3. Composite: Capital-Ratio-Token + (Quartal/Year ODER EU-Banken-Subjekt)
    has_capital_ratio = any(t in claim_lc for t in _CAPITAL_RATIO_TOKENS)
    has_quarter = bool(_QUARTER_RE.search(claim_lc)) or "quartal" in claim_lc
    has_year = bool(_YEAR_RE.search(claim_lc))
    has_eu_bank_subject = any(t in claim_lc for t in _EU_BANK_SUBJECT_TOKENS)

    if has_capital_ratio and (has_quarter or has_year or has_eu_bank_subject):
        return True

    # 3b. EU-Banken-Subjekt + irgendwo Banken-/Statistik-/Risiko-Kontext
    # ("NPL-Statistik Europa", "Tier-1-Quote EU Banken", ...)
    has_europe_anywhere = (
        "europa" in claim_lc
        or "europe" in claim_lc
        or any(t in claim_lc for t in _COMPOSITE_EU_TERMS)
    )
    if has_capital_ratio and has_europe_anywhere:
        return True

    if has_eu_bank_subject:
        return True

    # 4. Composite: Banken + EU + Risiko-/Aufsichts-Kontext (alt)
    has_bank = any(t in claim_lc for t in _COMPOSITE_BANK_TERMS)
    has_eu = any(t in claim_lc for t in _COMPOSITE_EU_TERMS)
    if has_bank and has_eu:
        # Plausibilitäts-Cap: einzelne Wörter wie "europäische Bank" allein
        # sollen nicht jeden Banken-News-Claim triggern. Wir verlangen
        # einen Risiko-/Aufsichts-Kontext.
        if any(t in claim_lc for t in (
            "risiko", "risiken", "aufsicht", "kennzahl",
            "kapital", "stabilität", "stabilitaet",
            "krise", "regulier", "regulator",
            "stress-test", "stresstest",
            "basel iii", "basel iv", "crd ", "crd-",
        )):
            return True

    return False


# Modul-Level-Cache für Trigger-Check (24h)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_eba_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den EBA-Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_eba(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    # Cache-Hygiene: bei >500 Einträgen die ältesten 100 droppen
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Result-Cache (24h für den Metadaten-Probe)
# ---------------------------------------------------------------------------
_probe_cache: dict[str, tuple[float, bool]] = {}


def _probe_cache_get(key: str) -> bool | None:
    now = time.time()
    hit = _probe_cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL_S:
        return hit[1]
    return None


def _probe_cache_put(key: str, value: bool) -> None:
    _probe_cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Helpers — Indikator/Land-Auswahl
# ---------------------------------------------------------------------------
def _pick_indicators(claim_lc: str) -> list[str]:
    """Liste der Indikator-Keys, die der Claim erwähnt (Reihenfolge erhalten).

    Wenn nichts spezifisches → leere Liste (Caller fällt auf Default zurück).
    """
    seen: set[str] = set()
    out: list[str] = []
    for hint, key in _INDICATOR_HINTS.items():
        if hint in claim_lc and key not in seen:
            out.append(key)
            seen.add(key)
    return out


def _pick_country(claim_lc: str) -> tuple[str, str] | None:
    """Erstes erkanntes Land → (iso3, anzeige) oder None."""
    for hint, (iso3, name) in _COUNTRY_HINTS.items():
        if hint in claim_lc:
            return iso3, name
    return None


def _resolve_quarter(claim_lc: str) -> tuple[str, bool]:
    """Erkenne explizit erwähntes Quartal im Claim.

    Returns:
      (resolved_quarter, used_fallback)

    Wenn der Claim "Qn YYYY" enthält und dieses Quartal in
    _QUARTERLY_AGGREGATES existiert → exakte Übereinstimmung.
    Wenn der Claim "Qn YYYY" enthält, das Quartal aber NICHT verfügbar ist
    (zu alt oder zu neu) → nächst-verfügbares Quartal + used_fallback=True.
    Wenn kein Quartal im Claim → LATEST + used_fallback=False.
    """
    m = _QUARTER_YEAR_RE.search(claim_lc)
    if not m:
        return _LATEST_QUARTER, False
    q_part = m.group(1).upper()  # "Q3"
    y_part = m.group(2)           # "2025"
    requested = f"{q_part} {y_part}"
    if requested in _QUARTERLY_AGGREGATES:
        return requested, False
    # Fallback: ältestes oder neuestes verfügbares — wir wählen LATEST,
    # da neuere Daten in der Regel relevanter sind.
    return _LATEST_QUARTER, True


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _format_aggregate_row(
    agg: dict,
    quarter: str | None = None,
    fallback_note: str | None = None,
) -> dict:
    """Bau einen EU-Aggregat-Result-Row aus dem Pack-Snapshot.

    quarter: explizites Quartal (Default: _LATEST_QUARTER)
    fallback_note: wenn nicht None → wird an die Beschreibung angehängt
                   (z.B. "letzter verfügbarer Stand").
    """
    quarter = quarter or _LATEST_QUARTER
    release_date = _QUARTERLY_AGGREGATES.get(quarter, {}).get(
        "release_date", _LATEST_RELEASE_DATE,
    )
    val = agg["value"]
    unit = agg["unit"]
    label = agg["label_de"]
    if isinstance(val, float):
        val_str = f"{val:.1f}".replace(".", ",")
    else:
        val_str = str(val)

    display = (
        f"{label} — EU/EEA-Aggregat {quarter}: "
        f"{val_str} {unit} (YoY {agg.get('yoy_delta', '—')})"
    )
    description = (
        f"EBA Risk Dashboard {quarter} "
        f"(veröffentlicht {release_date}). "
        f"Stichprobe: ~160 EU/EEA-Banken, ca. 80% der EU-Bankenbilanzsumme. "
        f"{agg['interpretation']} "
        "Reine Statistik-Wiedergabe — keine Bewertung."
    )
    if fallback_note:
        description = f"{fallback_note} {description}"
    return {
        "indicator_name": agg["indicator_name"],
        "indicator": f"eba_{agg['key']}_eu_aggregate",
        "country": "EU",
        "country_name": "EU/EEA-Aggregat",
        "year": quarter,
        "value": val,
        "display_value": display,
        "description": description,
        "url": EBA_RISK_DASHBOARD_URL,
        "source": "EBA Risk Dashboard",
    }


def _format_outlier_row(
    indicator_key: str,
    outlier: dict,
    quarter: str | None = None,
) -> dict:
    """Bau einen Country-Outlier-Result-Row."""
    quarter = quarter or _LATEST_QUARTER
    val = outlier["value"]
    unit = "%"
    iso3 = outlier["country"]
    name = outlier["country_name"]
    rank = outlier["rank"]
    if isinstance(val, float):
        val_str = f"{val:.1f}".replace(".", ",")
    else:
        val_str = str(val)

    # Indikator-Label hübsch aufbereiten
    meta = _INDICATOR_META.get(indicator_key)
    label = meta["label_de"] if meta else indicator_key.upper()

    display = (
        f"{label} {name} ({quarter}): "
        f"{val_str} {unit} — {rank}"
    )
    description = (
        f"EBA Risk Dashboard (Outlier-Snapshot, Stand Q3 2025) — "
        f"Country-Level-Aggregat für {name}: {label} = {val_str} {unit}. "
        f"Charakterisierung: {rank}. Daten stammen aus den COREP/FINREP-"
        "Meldungen der nationalen Aufsichtsbehörden, aggregiert von der "
        "EBA. Reine deskriptive Streuungs-Information — KEINE Bewertung "
        "der nationalen Bankenstabilität."
    )
    return {
        "indicator_name": f"EBA {label} — {name}",
        "indicator": f"eba_{indicator_key}_{iso3.lower()}",
        "country": iso3,
        "country_name": name,
        "year": quarter,
        "value": val,
        "display_value": display,
        "description": description,
        "url": EBA_RISK_DASHBOARD_URL,
        "source": "EBA Risk Dashboard",
    }


def _methodology_row(quarter: str | None = None) -> dict:
    """Methodik-Hinweis-Row (Synthesizer-Disclaimer)."""
    quarter = quarter or _LATEST_QUARTER
    release_date = _QUARTERLY_AGGREGATES.get(quarter, {}).get(
        "release_date", _LATEST_RELEASE_DATE,
    )
    available = ", ".join(_QUARTERLY_AGGREGATES.keys())
    return {
        "indicator_name": (
            "WICHTIGER KONTEXT: EBA Risk Dashboard — Methodik"
        ),
        "indicator": "eba_methodology",
        "country": "EU",
        "country_name": "Europäische Union",
        "year": quarter,
        "value": None,
        "display_value": (
            "EBA Risk Dashboard aggregiert COREP/FINREP-Meldungen "
            "(~160 EU/EEA-Banken, ~80% Bilanzsumme); Quartalsdaten "
            "erscheinen typisch ~6 Wochen nach Quartalsende."
        ),
        "description": (
            "Das EBA Risk Dashboard ist die offizielle quartalsweise "
            "Aufsichts-Publikation der European Banking Authority (Paris). "
            "Datenbasis: Common Reporting (COREP) und Financial Reporting "
            "(FINREP) — die einheitlichen aufsichtsrechtlichen Meldungen "
            "aller EU/EEA-Banken an die nationalen Aufsichtsbehörden. "
            "Methodische Einschränkungen: "
            "(1) Aggregat — einzelne Banken können stark vom Mittel abweichen, "
            "ohne dass das Aggregat eine Bewertung der Einzelinstitute zulässt. "
            "(2) Country-Level-Werte sind ebenfalls Aggregate über alle "
            "Banken eines Mitgliedstaats und nicht institutsspezifisch. "
            "(3) Definitionen folgen CRD IV / CRR und Basel III; Vergleiche "
            "mit Nicht-EU-Banken (USA: Basel-III-Implementation unterschiedlich) "
            "sind nur eingeschränkt möglich. "
            "(4) Stichtag der hier wiedergegebenen Zahlen: "
            f"{quarter}, Release {release_date}. "
            f"(5) Verfügbare Quartals-Snapshots im Pack: {available}."
        ),
        "url": EBA_EDAP_URL,
        "source": "EBA Risk Dashboard (Methodik)",
    }


# ---------------------------------------------------------------------------
# Optionaler data.europa.eu Mirror-Probe
# ---------------------------------------------------------------------------
async def _probe_data_europa() -> bool:
    """Best-Effort: Ist der data.europa.eu-Mirror der EBA-Daten erreichbar?

    Wir machen NUR einen GET auf die Such-Landing-Page mit Query "EBA Risk
    Dashboard" und prüfen den HTTP-Status. Tatsächliches XLSX-Parsing
    findet hier nicht statt (siehe Modul-Docstring). Der Probe dient als
    Telemetrie + ist Voraussetzung für eine zusätzliche Mirror-URL-Row.
    """
    cache_key = "eba::data_europa_probe"
    cached = _probe_cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(
                DATA_EUROPA_PROBE_URL, follow_redirects=True,
            )
            ok = resp.status_code == 200
    except Exception as e:
        logger.debug(f"EBA data.europa.eu probe failed: {e}")
        ok = False

    _probe_cache_put(cache_key, ok)
    return ok


def _data_europa_row() -> dict:
    """Bau einen Mirror-Referenz-Row, wenn data.europa.eu erreichbar ist."""
    return {
        "indicator_name": "EBA-Daten auf data.europa.eu (EU Open Data Portal)",
        "indicator": "eba_data_europa_mirror",
        "country": "EU",
        "country_name": "Europäische Union",
        "year": "",
        "value": None,
        "display_value": (
            "Die EBA Risk Dashboard XLSX-Veröffentlichungen sind auch "
            "auf dem EU Open Data Portal (data.europa.eu) gespiegelt."
        ),
        "description": (
            "Das EU Open Data Portal indexiert die EBA-Datensätze unter "
            "der EU-Reuse-Decision (CC-BY 4.0). Direkt-Download der "
            "Quartals-Excel-Datei ist dort verfügbar; für maschinelle "
            "Auswertung empfiehlt sich das XLSX-Format mit openpyxl."
        ),
        "url": DATA_EUROPA_PROBE_URL,
        "source": "data.europa.eu (EU Open Data Portal)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_eba(analysis: dict) -> dict:
    """Live-Lookup gegen EBA Risk Dashboard.

    Strategie (Hybrid Pack+Live):
      1. Trigger-Check (defensiv — Caller sollte den ohnehin schon machen).
      2. Pack-Snapshot der EU-Aggregate filtern nach Indikator-Erwähnung
         im Claim (CET1/NPL/...) — sonst alle Headline-Werte.
      3. Wenn Land im Claim erkannt + Outlier-Daten verfügbar: Country-
         spezifischer Row anhängen.
      4. data.europa.eu-Probe (~1–2 s, 24h-gecached) — Telemetrie + ggf.
         Mirror-Referenz-Row.
      5. Methodik-Disclaimer als letzter Row.

    Return-Schema:
      {
        "source": "EBA Risk Dashboard",
        "type": "eu_banking_risks",
        "results": [ {indicator_name, value, ...}, ... ],
        "attribution": "CC-BY 4.0 — ..."
      }
    """
    empty = {
        "source": "EBA Risk Dashboard",
        "type": "eu_banking_risks",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_eba(matchable):
        return empty

    # 1. Indikator-Auswahl
    requested_indicators = _pick_indicators(matchable)

    # 1b. Quartal-Resolution
    resolved_quarter, used_fallback = _resolve_quarter(matchable)
    fallback_note: str | None = None
    if used_fallback:
        # Erkenne, was der User wollte, für die Disclaimer-Message
        m = _QUARTER_YEAR_RE.search(matchable)
        requested_q = (
            f"{m.group(1).upper()} {m.group(2)}" if m else "das genannte Quartal"
        )
        fallback_note = (
            f"Hinweis: Für {requested_q} liegt im EBA-Pack-Snapshot kein "
            f"Datensatz vor — letzter verfügbarer Stand: {resolved_quarter}."
        )
    quarter_aggs = _aggregates_for(resolved_quarter)

    results: list[dict] = []

    # 2. EU-Aggregat-Rows
    if requested_indicators:
        # Nur die explizit angesprochenen Indikatoren
        for agg in quarter_aggs:
            if agg["key"] in requested_indicators:
                results.append(_format_aggregate_row(
                    agg, quarter=resolved_quarter, fallback_note=fallback_note,
                ))
    else:
        # Default: alle 6 Headline-Indikatoren, gekappt auf 4
        for agg in quarter_aggs[:4]:
            results.append(_format_aggregate_row(
                agg, quarter=resolved_quarter, fallback_note=fallback_note,
            ))

    # 3. Country-Outlier
    country_hit = _pick_country(matchable)
    if country_hit:
        iso3, name = country_hit
        # Pro getriggertem Indikator nach Land suchen; Fallback: nur NPL+CET1
        check_indicators = requested_indicators or ["npl", "cet1"]
        for ind_key in check_indicators:
            for outlier in _OUTLIERS.get(ind_key, []):
                if outlier["country"] == iso3:
                    results.append(_format_outlier_row(
                        ind_key, outlier, quarter=resolved_quarter,
                    ))
                    break
    else:
        # Generischer Outlier-Hinweis: für jeden getriggerten Indikator
        # den "höchsten" Wert anhängen (Top-Streuungs-Information).
        check_indicators = requested_indicators or ["npl"]
        for ind_key in check_indicators[:2]:
            outliers = _OUTLIERS.get(ind_key, [])
            if outliers:
                results.append(_format_outlier_row(
                    ind_key, outliers[0], quarter=resolved_quarter,
                ))

    # 4. data.europa.eu-Probe (Graceful Fail)
    try:
        mirror_ok = await _probe_data_europa()
        if mirror_ok:
            results.append(_data_europa_row())
    except Exception as e:
        logger.debug(f"EBA mirror probe crashed: {e}")

    # 5. Methodik-Disclaimer
    results.append(_methodology_row(quarter=resolved_quarter))

    # Cap auf MAX_RESULTS + Disclaimer (immer dabei)
    if len(results) > MAX_RESULTS + 1:
        # Disclaimer immer behalten
        head = results[:-1][:MAX_RESULTS]
        results = head + [results[-1]]

    logger.info(
        f"EBA: {len(results)} Rows "
        f"(indicators={requested_indicators or 'default'}, "
        f"country={country_hit[0] if country_hit else '—'}, "
        f"quarter={resolved_quarter}, fallback={used_fallback})"
    )
    return {
        "source": "EBA Risk Dashboard",
        "type": "eu_banking_risks",
        "results": results,
        "attribution": (
            "EBA Risk Dashboard data © European Banking Authority "
            f"(CC-BY 4.0). Quartals-Snapshot {resolved_quarter}; aggregiert aus "
            "COREP/FINREP-Meldungen von ~160 EU/EEA-Banken."
        ),
    }


# Kleiner LRU-Wrapper-Helfer, falls Caller eine sync-Variante des Trigger-
# Checks bevorzugt (analog era5.claim_mentions_era5_cached).
@lru_cache(maxsize=512)
def _claim_mentions_eba_lru(claim_lc: str) -> bool:
    return _claim_mentions_eba(claim_lc)
