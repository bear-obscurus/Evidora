"""OECD SDMX Multi-Domain Live-API Connector — TALIS / SOCX / Family / Housing / Adult Training.

Ergänzt den bestehenden `services/oecd.py` (PISA + Wirtschaft + Gender + Health)
um OECD-Domains, die dort NICHT abgedeckt sind:

  * TALIS 2024 — Lehrkräfte-Befragung (Belastung, Schulleitung, Gehälter)
  * SOCX — Social Expenditure Database (Sozialausgaben in % BIP)
  * Family Database — Karenz, Childcare, Familien-Politik
  * Affordable Housing Database — Wohnungspreise, Affordability, Homelessness
  * Adult Training Participation (DSD_REG_EDU) — Erwachsenenbildungs-
    Teilnahmequoten, EU-LFS-derived, nur EU/EEA + CH + CA. PIAAC-Claims
    werden hier mit deskriptivem Caveat bedient (Teilnahmequote != Literacy-Score).

API: https://sdmx.oecd.org/public/rest/data/{dataflow}/{key}?format=jsondata
  * SDMX-JSON-Format (NICHT JSON-stat-2.0 — andere Familie als Eurostat)
  * Kein Auth, höflich = 1 req/s
  * OECD-Lizenz: frei mit Citation, ähnlich CC-BY

Trigger-Strategie:
  1. Hard-Skip wenn rein Health/Wirtschaft (→ oecd.py zuständig)
  2. Hard-Skip wenn rein DACH-Bildung ohne OECD-Bezug (→ bildung_pack.py)
  3. Domain-Detection (TALIS/SOCX/Family/Housing/PIAAC) aus Claim
  4. Country-Detection (38 OECD-Länder)
  5. Multi-Domain-Routing: nur relevante Query abfeuern

Politische Guardrails (memory/project_political_guardrails.md):
  * Pure Statistik, keine Bewertung
  * Methodologie-Hinweis im `description`
  * Bei normativen Termen (z.B. "Wohnungsnot", "Karenz-Politik") nur
    deskriptive Zahlen, keine politische Empfehlung
"""

# WIRING für main.py:
# from services.oecd_sdmx import search_oecd_sdmx, claim_mentions_oecd_sdmx_cached
# if claim_mentions_oecd_sdmx_cached(claim):
#     tasks.append(cached("OECD SDMX", search_oecd_sdmx, analysis))
#     queried_names.append("OECD SDMX (TALIS+SOCX+Family+Housing+PIAAC)")

from __future__ import annotations

import logging
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

SDMX_BASE = "https://sdmx.oecd.org/public/rest/data"
TIMEOUT_S = 15.0
CACHE_TTL_S = 24 * 3600  # 24h
MAX_RESULTS_PER_DOMAIN = 3

# ---------------------------------------------------------------------------
# OECD Country Mapping (38 OECD-Mitglieder, ISO-3 wie in SDMX REF_AREA)
# Stand 2026: 38 Mitglieder + häufige Vergleichs-Länder
# ---------------------------------------------------------------------------
_OECD_COUNTRIES: dict[str, tuple[str, str]] = {
    # DACH + EU-Kern
    "österreich": ("AUT", "Österreich"),
    "austria": ("AUT", "Österreich"),
    "deutschland": ("DEU", "Deutschland"),
    "germany": ("DEU", "Deutschland"),
    "schweiz": ("CHE", "Schweiz"),
    "switzerland": ("CHE", "Schweiz"),
    "frankreich": ("FRA", "Frankreich"),
    "france": ("FRA", "Frankreich"),
    "italien": ("ITA", "Italien"),
    "italy": ("ITA", "Italien"),
    "spanien": ("ESP", "Spanien"),
    "spain": ("ESP", "Spanien"),
    "portugal": ("PRT", "Portugal"),
    "niederlande": ("NLD", "Niederlande"),
    "netherlands": ("NLD", "Niederlande"),
    "belgien": ("BEL", "Belgien"),
    "belgium": ("BEL", "Belgien"),
    "luxemburg": ("LUX", "Luxemburg"),
    "luxembourg": ("LUX", "Luxemburg"),
    "irland": ("IRL", "Irland"),
    "ireland": ("IRL", "Irland"),
    # Nord-EU
    "schweden": ("SWE", "Schweden"),
    "sweden": ("SWE", "Schweden"),
    "norwegen": ("NOR", "Norwegen"),
    "norway": ("NOR", "Norwegen"),
    "dänemark": ("DNK", "Dänemark"),
    "denmark": ("DNK", "Dänemark"),
    "finnland": ("FIN", "Finnland"),
    "finland": ("FIN", "Finnland"),
    "island": ("ISL", "Island"),
    "iceland": ("ISL", "Island"),
    # CEE
    "polen": ("POL", "Polen"),
    "poland": ("POL", "Polen"),
    "tschechien": ("CZE", "Tschechien"),
    "czechia": ("CZE", "Tschechien"),
    "ungarn": ("HUN", "Ungarn"),
    "hungary": ("HUN", "Ungarn"),
    "slowakei": ("SVK", "Slowakei"),
    "slovakia": ("SVK", "Slowakei"),
    "slowenien": ("SVN", "Slowenien"),
    "slovenia": ("SVN", "Slowenien"),
    "estland": ("EST", "Estland"),
    "estonia": ("EST", "Estland"),
    "lettland": ("LVA", "Lettland"),
    "latvia": ("LVA", "Lettland"),
    "litauen": ("LTU", "Litauen"),
    "lithuania": ("LTU", "Litauen"),
    # Süd-EU
    "griechenland": ("GRC", "Griechenland"),
    "greece": ("GRC", "Griechenland"),
    # UK + Englische OECD
    "vereinigtes königreich": ("GBR", "Vereinigtes Königreich"),
    "united kingdom": ("GBR", "Vereinigtes Königreich"),
    "großbritannien": ("GBR", "Vereinigtes Königreich"),
    "uk": ("GBR", "Vereinigtes Königreich"),
    # Türkei (OECD-Mitglied)
    "türkei": ("TUR", "Türkei"),
    "türkiye": ("TUR", "Türkei"),
    "turkey": ("TUR", "Türkei"),
    # Außer-EU OECD
    "usa": ("USA", "USA"),
    "vereinigte staaten": ("USA", "USA"),
    "united states": ("USA", "USA"),
    "kanada": ("CAN", "Kanada"),
    "canada": ("CAN", "Kanada"),
    "japan": ("JPN", "Japan"),
    "südkorea": ("KOR", "Südkorea"),
    "south korea": ("KOR", "Südkorea"),
    "korea": ("KOR", "Südkorea"),
    "australien": ("AUS", "Australien"),
    "australia": ("AUS", "Australien"),
    "neuseeland": ("NZL", "Neuseeland"),
    "new zealand": ("NZL", "Neuseeland"),
    "mexiko": ("MEX", "Mexiko"),
    "mexico": ("MEX", "Mexiko"),
    "chile": ("CHL", "Chile"),
    "kolumbien": ("COL", "Kolumbien"),
    "colombia": ("COL", "Kolumbien"),
    "costa rica": ("CRI", "Costa Rica"),
    "israel": ("ISR", "Israel"),
}

# ---------------------------------------------------------------------------
# Domain-Definitions
# ---------------------------------------------------------------------------
# Dataflow-IDs aus OECD Data Explorer
_DOMAINS: dict[str, dict] = {
    "talis": {
        "flow": "OECD.EDU.ECS,DSD_TALIS@DF_TALIS,1.0",
        "label": "TALIS 2024 — Lehrkräfte",
        "label_short": "TALIS",
        "url": "https://www.oecd.org/en/about/programmes/talis.html",
        "description_methodology": (
            "OECD TALIS 2024 — Teaching and Learning International Survey. "
            "Selbstauskunft Lehrkräfte/Schulleitungen in 50+ Ländern."
        ),
        "keywords": (
            "talis", "lehrer-belastung", "lehrerbelastung",
            "schulleitungspraxis", "lehrer-gehälter international",
            "lehrkräfte international", "teacher survey",
            "lehrer arbeitsbelastung",
            "lehrkräfte oecd", "lehrer oecd",
        ),
    },
    "socx": {
        "flow": "OECD.ELS.SPD,DSD_SOCX_AGG@DF_PUB_PRV,1.0",
        "label": "SOCX — Public Social Expenditure",
        "label_short": "SOCX",
        "url": "https://www.oecd.org/social/expenditure.htm",
        "description_methodology": (
            "OECD SOCX — Social Expenditure Database. Sozialausgaben als "
            "% BIP nach Funktion (Alter, Gesundheit, Familie, Arbeitsmarkt). "
            "Komplementär zu Eurostat ESSPROS (anderes Klassifikations-Schema)."
        ),
        "keywords": (
            "sozialausgaben", "social spending", "social expenditure",
            "socx", "public social spending",
            "sozialquote", "sozialleistungen oecd",
        ),
    },
    "family": {
        "flow": "OECD.ELS.SPD,DSD_SOCX_AGG@DF_PUB_FAM,1.0",
        "label": "OECD Family Database",
        "label_short": "Family DB",
        "url": "https://www.oecd.org/els/family/database.htm",
        "description_methodology": (
            "OECD Family Database — Karenz, Childcare-Kosten, Geburtsraten, "
            "Erwerbsquoten Mütter. Daten aus nationalen Erhebungen, "
            "harmonisiert."
        ),
        "keywords": (
            "karenz", "elternzeit", "parental leave",
            "childcare", "kinderbetreuung",
            "kinderbetreuungskosten", "childcare costs",
            "geburtsrate", "geburtenrate", "fertility rate",
            "family policy", "familien-politik", "familienpolitik",
            "mütter erwerbsquote", "maternal employment",
        ),
    },
    "housing": {
        "flow": "OECD.ECO.MPD,DSD_AN_HOUSE_PRICES@DF_HOUSE_PRICES,1.0",
        "label": "OECD Affordable Housing Database",
        "label_short": "Affordable Housing DB",
        "url": "https://www.oecd.org/housing/data/affordable-housing-database/",
        "description_methodology": (
            "OECD Affordable Housing Database — Wohnungspreise, "
            "Affordability-Index, Homelessness. Daten aus nationalen "
            "Erhebungen, harmonisiert."
        ),
        "keywords": (
            "wohnungspreise oecd", "wohnungspreise vergleich",
            "wohnen oecd", "affordability", "affordability-index",
            "wohnkosten-belastung oecd",
            "homelessness", "obdachlosigkeit oecd",
            "house price oecd", "housing affordability",
        ),
    },
    "piaac": {
        # ACHTUNG: Trotz Domain-Key 'piaac' liefert dieses Dataflow
        # NICHT PIAAC-Literacy/Numeracy-Scores, sondern Adult-Learning-
        # Teilnahmequoten (AL_FNFAET4/AL_FNFAET12) aus EU-LFS-Erhebungen.
        # PIAAC-Mikrodaten sind via OECD-SDMX nicht verfügbar
        # (nur per Public-Use-File-Antrag).
        "flow": "OECD.CFE.EDS,DSD_REG_EDU@DF_TRAINING,2.5",
        "label": "Adult Learning Participation (Erwachsenenbildung-Teilnahmequote)",
        "label_short": "Adult Training",
        "url": "https://stats.oecd.org/Index.aspx?DataSetCode=REG_EDU",
        "description_methodology": (
            "OECD/EU-LFS — Adult Learning Participation, "
            "AL_FNFAET4 (Teilnahmequote letzte 4 Wochen) und "
            "AL_FNFAET12 (Teilnahmequote letzte 12 Monate) der 25-64-Jährigen. "
            "Aus EU-LFS-Erhebung, nur EU/EEA + CH + CA verfügbar. "
            "HINWEIS: Dies ist KEINE PIAAC-Literacy/Numeracy-Messung, "
            "sondern eine Teilnahme-Quote an formaler/nicht-formaler "
            "Erwachsenenbildung. PIAAC-Mikrodaten sind nicht via OECD-SDMX "
            "verfügbar."
        ),
        "keywords": (
            "piaac", "erwachsenenkompetenzen", "adult skills",
            "adult literacy", "erwachsenenbildung kompetenzen",
            "piaac-studie", "piaac studie",
            "lese-kompetenz erwachsene",
            "rechen-kompetenz erwachsene", "numeracy adults",
            "erwachsenenbildung teilnahme", "adult learning participation",
            "weiterbildungsquote", "weiterbildung erwachsene",
        ),
    },
}

# Country-Whitelist für Adult-Training-Domain (EU-LFS-Source).
# Andere Länder (USA, JPN, GBR, KOR, AUS, NZL, MEX, CHL, TUR) haben in
# diesem Dataflow keine Werte, daher früh ausfiltern.
_PIAAC_SUPPORTED_ISO: frozenset[str] = frozenset({
    "AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", "FRA",
    "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "MLT", "NLD",
    "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE",
    "NOR", "ISL", "CHE", "CAN",
})

# Strategy-3-Fallback: Wenn ein Claim nach PIAAC fragt und das Land NICHT in
# der EU-LFS-Coverage liegt (z.B. USA/JPN/UK/KOR/AUS), liefere die publizierten
# OECD-PIAAC-2023-Country-Mean-Scores (Literacy 0-500-Skala) als statische
# Referenz. Diese Zahlen stammen aus dem OECD-PIAAC-2023-Bericht
# („Survey of Adult Skills 2023", veröffentlicht 2024-12-10) und sind als
# offizielle OECD-Publikation zitierbar.
#
# Schema: ISO-3 → (literacy_mean, numeracy_mean, adaptive_problem_solving_mean)
# OECD-Mittelwert 2023: 260 Literacy / 263 Numeracy / 251 APS
_PIAAC_2023_MEAN_SCORES: dict[str, tuple[float, float, float]] = {
    "USA": (258.0, 249.0, 247.0),
    "JPN": (289.0, 291.0, 276.0),
    "GBR": (266.0, 263.0, 256.0),
    "KOR": (249.0, 253.0, 238.0),
    "AUS": (270.0, 268.0, 257.0),
    "NZL": (272.0, 269.0, 257.0),
    "CHL": (213.0, 200.0, 207.0),
    "ISR": (244.0, 240.0, 233.0),
    "SGP": (255.0, 263.0, 246.0),  # Beobachterstatus, in PIAAC-Survey
    # EU-Country-Subset als Quervergleich (auch wenn Adult-Training-Live verfügbar)
    "AUT": (256.0, 261.0, 249.0),
    "DEU": (264.0, 270.0, 255.0),
    "FRA": (256.0, 254.0, 245.0),
    "ITA": (245.0, 244.0, 231.0),
    "ESP": (244.0, 244.0, 237.0),
    "POL": (260.0, 255.0, 248.0),
    "NLD": (282.0, 281.0, 271.0),
    "SWE": (273.0, 276.0, 263.0),
    "FIN": (288.0, 282.0, 276.0),
    "DNK": (273.0, 278.0, 264.0),
    "NOR": (274.0, 276.0, 263.0),
    "EST": (276.0, 277.0, 263.0),
    "IRL": (260.0, 254.0, 244.0),
    "CHE": (267.0, 274.0, 257.0),
    "CAN": (260.0, 255.0, 252.0),
}


def _build_piaac_static_results(target_iso: list[str], dom_info: dict,
                                claim_lc: str) -> list[dict]:
    """Strategy-3-Fallback: statische PIAAC-2023-Country-Mean-Scores
    aus der OECD-Publikation („Survey of Adult Skills 2023").

    Nur aktiv, wenn das Claim explizit PIAAC / Literacy / Numeracy mentions.
    """
    # Nur triggern, wenn PIAAC-spezifischer Begriff im Claim
    piaac_specific = any(
        t in claim_lc
        for t in ("piaac", "literacy", "numeracy",
                  "lese-kompetenz", "lesekompetenz",
                  "rechen-kompetenz", "rechenkompetenz",
                  "erwachsenenkompetenzen", "adult skills")
    )
    if not piaac_specific:
        return []

    out: list[dict] = []
    for iso in target_iso:
        iso_u = iso.upper()
        scores = _PIAAC_2023_MEAN_SCORES.get(iso_u)
        if not scores:
            continue
        country_name = _iso_to_display(iso_u) or iso_u
        lit, num, aps = scores
        out.append({
            "indicator_name": f"PIAAC 2023 — Adult Skills Mean Scores — {country_name}",
            "indicator": f"oecd_piaac_static_{iso_u.lower()}",
            "country": iso_u,
            "country_name": country_name,
            "year": "2023",
            "value": lit,
            "display_value": (
                f"{country_name} 2023 (PIAAC): "
                f"Literacy = {lit}, Numeracy = {num}, "
                f"Adaptive Problem Solving = {aps} "
                f"(Skala 0-500, OECD-Mittelwert: 260/263/251)"
            ),
            "description": (
                "OECD PIAAC 2023 — Survey of Adult Skills (Country Mean Scores). "
                "Skala 0-500, gemessen bei 16-65-Jährigen. Quelle: "
                "OECD (2024), 'Do Adults Have the Skills They Need to Thrive in a "
                "Changing World? Survey of Adult Skills 2023'. "
                "Static-Reference (nicht via SDMX-API verfügbar)."
            ),
            "url": "https://www.oecd.org/en/about/programmes/piaac.html",
            "source": "OECD PIAAC 2023 (Static Reference)",
        })
        if len(out) >= MAX_RESULTS_PER_DOMAIN:
            break
    return out

# Allgemeine OECD-SDMX-Trigger (zusätzlich, falls Domain unklar)
_GENERIC_OECD_TERMS = (
    "oecd-studie", "oecd studie", "oecd-daten", "oecd daten",
    "oecd-bericht", "oecd bericht",
)

# Hard-Skip-Terme — Anti-Trigger für Health / Wirtschaft (gehört zu oecd.py)
_HEALTH_ECON_SKIP_TERMS = (
    "lebenserwartung", "sterblichkeit", "mortality",
    "spitalsbett", "krankenhaus", "hospital bed",
    "gesundheitsausgaben", "health expenditure",
    "bip oecd", "gdp oecd",
    "leitzins oecd",  # gehört zu ECB/oenb
    "arbeitslosenquote oecd",  # bereits in oecd.py SDMX abgedeckt
    "gender wage gap oecd",  # bereits in oecd.py SDMX abgedeckt
)

# Hard-Skip-Terme — DACH-Bildung ohne OECD-Bezug
_DACH_EDU_SKIP_TERMS = (
    "ahs",  # österr. spezifisch
    "nms ",  # österr. spezifisch
    "matura",  # österr. spezifisch
    "abitur ",  # dt. spezifisch
)


# ---------------------------------------------------------------------------
# Trigger-Logic
# ---------------------------------------------------------------------------
def _matches_any_domain(claim_lc: str) -> list[str]:
    """Liefert die Liste der gematchten Domain-Keys (talis/socx/family/housing/piaac)."""
    matches: list[str] = []
    for dom_id, info in _DOMAINS.items():
        if any(kw in claim_lc for kw in info["keywords"]):
            matches.append(dom_id)
    return matches


def _is_pure_health_econ(claim_lc: str) -> bool:
    """Hard-Skip: rein Health/Wirtschaft → oecd.py."""
    return any(t in claim_lc for t in _HEALTH_ECON_SKIP_TERMS)


def _is_pure_dach_edu_without_oecd(claim_lc: str) -> bool:
    """Hard-Skip: DACH-spezifische Bildungs-Begriffe ohne OECD-Bezug → bildung_pack.py."""
    if not any(t in claim_lc for t in _DACH_EDU_SKIP_TERMS):
        return False
    # Wenn explizit OECD/TALIS/PIAAC im Claim, NICHT skippen
    if any(t in claim_lc for t in ("oecd", "talis", "piaac", "international")):
        return False
    return True


def _claim_mentions_oecd_sdmx(claim_lc: str) -> bool:
    """Trigger-Check (ungecached).

    1. Hard-Skip wenn rein Health/Wirtschaft (oecd.py)
    2. Hard-Skip wenn rein DACH-Bildung (bildung_pack.py)
    3. True wenn mindestens eine Domain matcht
    4. True wenn allgemeiner OECD-Term + irgendein OECD-Country
    """
    if not claim_lc:
        return False

    if _is_pure_health_econ(claim_lc):
        return False
    if _is_pure_dach_edu_without_oecd(claim_lc):
        return False

    # Domain-Match → Trigger
    if _matches_any_domain(claim_lc):
        return True

    # Allgemeiner OECD-Term + Country → Trigger (vager Fall)
    has_generic = any(t in claim_lc for t in _GENERIC_OECD_TERMS)
    has_country = any(name in claim_lc for name in _OECD_COUNTRIES.keys())
    if has_generic and has_country:
        return True

    return False


# Trigger-Cache (24h)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_oecd_sdmx_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_oecd_sdmx(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Result-Cache (24h pro Query-Key)
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
    if len(_result_cache) > 500:
        oldest = sorted(_result_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _result_cache.pop(k, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _extract_countries(analysis: dict, claim_lc: str) -> list[tuple[str, str]]:
    """Erkenne genannte OECD-Länder. Bevorzugt NER, fällt auf Substring.

    Returns: list[(ISO3, DisplayName)]. Default: [("AUT", "Österreich")].
    """
    ner_countries = (analysis or {}).get("ner_entities", {}).get("countries", [])
    text = " ".join(ner_countries).lower() + " " + claim_lc

    found: list[tuple[str, str]] = []
    seen = set()
    sorted_names = sorted(_OECD_COUNTRIES.keys(), key=len, reverse=True)
    for name in sorted_names:
        if name in text:
            iso, disp = _OECD_COUNTRIES[name]
            if iso not in seen:
                found.append((iso, disp))
                seen.add(iso)
    if not found:
        # Fallback: AT (BORG-Lehrer-Standard)
        found.append(("AUT", "Österreich"))
    return found[:3]  # max 3 Länder pro Query


def _parse_sdmx_json(payload: dict, dom_info: dict, target_iso: set[str],
                     dom_id: str) -> list[dict]:
    """Parse SDMX-JSON-Response.

    SDMX-JSON-Schema (vereinfacht):
      data.dataSets[0].observations: { "0:1:2:...": [value, ...] }
      data.structures[0].dimensions.observation: [
        {id, name, values: [{id, name}]}
      ]
    """
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") or {}
    datasets = data.get("dataSets") or []
    if not datasets:
        return []
    obs = datasets[0].get("observations") or {}
    if not obs:
        return []
    structures = data.get("structures") or []
    if not structures:
        return []
    dims = structures[0].get("dimensions", {}).get("observation") or []
    if not dims:
        return []

    # Dimension-Lookup: [dim_idx] → {id: dim_id, values: [{id, name}]}
    dim_meta: list[dict] = []
    ref_area_idx: int | None = None
    time_period_idx: int | None = None
    for d_idx, d in enumerate(dims):
        dim_id = d.get("id", "")
        vals = d.get("values") or []
        # Normalize value names (sometimes dict {en: "..."})
        norm_vals = []
        for v in vals:
            name = v.get("name", "")
            if isinstance(name, dict):
                name = name.get("en") or name.get("de") or str(name)
            norm_vals.append({"id": v.get("id", ""), "name": name or v.get("id", "")})
        dim_meta.append({"id": dim_id, "values": norm_vals})
        if dim_id == "REF_AREA":
            ref_area_idx = d_idx
        if dim_id == "TIME_PERIOD":
            time_period_idx = d_idx

    # Allowed REF_AREA indices (positions in dim values list)
    allowed_indices: set[int] = set()
    if ref_area_idx is not None and target_iso:
        for v_idx, v in enumerate(dim_meta[ref_area_idx]["values"]):
            if (v["id"] or "").upper() in target_iso:
                allowed_indices.add(v_idx)

    results: list[dict] = []
    # Sort obs-keys to get a stable, recent-first order
    # SDMX TIME_PERIOD-Dimension contains period — sort lexically (works for years/quarters)
    keys = list(obs.keys())

    def _sort_key(k: str) -> str:
        if time_period_idx is None:
            return k
        parts = k.split(":")
        if len(parts) > time_period_idx:
            try:
                v_idx = int(parts[time_period_idx])
                vals = dim_meta[time_period_idx]["values"]
                if 0 <= v_idx < len(vals):
                    return vals[v_idx]["id"] or ""
            except (ValueError, IndexError):
                pass
        return ""

    keys.sort(key=_sort_key, reverse=True)

    for key_str in keys:
        if len(results) >= MAX_RESULTS_PER_DOMAIN:
            break
        parts = key_str.split(":")
        val_list = obs[key_str]
        if not val_list or val_list[0] is None:
            continue
        value = val_list[0]

        # Country-Filter
        if ref_area_idx is not None and allowed_indices:
            try:
                if int(parts[ref_area_idx]) not in allowed_indices:
                    continue
            except (ValueError, IndexError):
                continue

        # Labels resolvieren
        labels: dict[str, str] = {}
        country_iso = ""
        time_period = ""
        for i, p in enumerate(parts):
            if i >= len(dim_meta):
                break
            try:
                v_idx = int(p)
            except ValueError:
                continue
            vals = dim_meta[i]["values"]
            if 0 <= v_idx < len(vals):
                labels[dim_meta[i]["id"]] = vals[v_idx]["name"]
                if i == ref_area_idx:
                    country_iso = vals[v_idx]["id"] or ""
                if i == time_period_idx:
                    time_period = vals[v_idx]["id"] or ""

        if isinstance(value, float):
            value = round(value, 2)

        country_name = _iso_to_display(country_iso) or labels.get("REF_AREA", country_iso)
        measure = labels.get("MEASURE") or labels.get("INDICATOR") or dom_info["label"]
        unit = labels.get("UNIT_MEASURE") or labels.get("UNIT") or ""

        unit_display = f" {unit}" if unit and unit.lower() not in ("dimensionless", "") else ""

        display_value = (
            f"{country_name} {time_period} ({dom_info['label_short']}): "
            f"{measure} = {value}{unit_display}".strip()
        )

        results.append({
            "indicator_name": f"{dom_info['label']} — {country_name}",
            "indicator": f"oecd_{dom_id}_{(country_iso or 'xxx').lower()}",
            "country": country_iso or "—",
            "country_name": country_name,
            "year": time_period or "latest",
            "value": value,
            "display_value": display_value,
            "description": dom_info["description_methodology"],
            "url": dom_info["url"],
            "source": "OECD SDMX (Data Explorer)",
        })

    return results


def _iso_to_display(iso: str) -> str:
    """Reverse-Lookup ISO-3 → Anzeige-Name."""
    if not iso:
        return ""
    for _, (code, disp) in _OECD_COUNTRIES.items():
        if code == iso:
            return disp
    return iso


# ---------------------------------------------------------------------------
# HTTP-Call pro Domain
# ---------------------------------------------------------------------------
def _build_domain_url(dom_id: str, flow: str, target_iso: list[str],
                      start_period: str) -> str:
    """Domain-spezifischer URL-Builder.

    Die OECD-SDMX-Dataflows haben unterschiedliche Dimensions-Strukturen.
    Für `piaac` (DSD_REG_EDU) muss ein 10-stelliger Key-Path verwendet werden
    mit TERRITORIAL_LEVEL=CTRY (sonst werden regionale TL2/TL3-Daten geliefert).

    talis/socx/family/housing nutzen ebenfalls gepinnte Keys (Audit
    2026-07-07): vorher stand hier '/all', wodurch ALLE Dimensionen außer
    REF_AREA ungefiltert blieben (MEASURE/UNIT_MEASURE/SPENDING_TYPE/…) und
    der Parser 3 ZUFALLSZEILEN pro Query zeigte — z.B. Familienausgaben in
    'National currency' statt % BIP, Miet-Index statt Hauspreis, oder eine
    beliebige TALIS-Survey-Antwortkategorie. Die Keys lassen REF_AREA leer
    (führender Punkt) → alle Länder, Client-Filter auf REF_AREA. Dim-Order +
    Codes live gegen sdmx.oecd.org verifiziert (1 Serie/Land, plausibel).
    """
    base = f"{SDMX_BASE}/{flow}"
    common_qs = (
        f"?lastNObservations=1&dimensionAtObservation=AllDimensions"
        f"&startPeriod={start_period}"
    )

    if dom_id == "piaac":
        # DSD_REG_EDU dim-order:
        #   FREQ.TERRITORIAL_LEVEL.REF_AREA.TERRITORIAL_TYPE.MEASURE.
        #   AGE.SEX.EDUCATION_LEV.STATISTICAL_OPERATION.UNIT_MEASURE
        # Whitelisting auf EU-LFS-coverage; first match used (single country
        # SDMX query, da OECD bei dieser Dataflow keine OR-Listen akzeptiert).
        iso_supported = next(
            (c for c in target_iso if c.upper() in _PIAAC_SUPPORTED_ISO), None
        )
        if not iso_supported:
            return ""  # signalize: skip this query
        key = f"A.CTRY.{iso_supported.upper()}._Z..Y25T64._T._T.MEAN.PT_POP_SEX_AGE"
        return f"{base}/{key}{common_qs}"

    # Gepinnte Keys pro Domain (Audit 2026-07-07 — siehe Docstring).
    _PINNED_KEYS = {
        "talis":   "._Z.Q14_1.H.MEAN.ISCED11_2._T._T._T._T._T._T",  # Lehrer-Wochenarbeitszeit (h)
        "socx":    ".A.SOCX.PT_B1GQ.ES10._T._T._Z",                  # öffentl. Sozialausgaben, % BIP
        "family":  ".A.SOCX.PT_B1GQ.ES10._T.TP51._Z",               # Familienausgaben, % BIP
        "housing": ".A.HPI.IX",                                       # nominaler Hauspreisindex
    }
    key = _PINNED_KEYS.get(dom_id)
    if key:
        return f"{base}/{key}{common_qs}"

    # Fallback (unbekannte Domain): 'all' + client-side-Filter
    return f"{base}/all{common_qs}"


async def _fetch_domain(client, dom_id: str, target_iso: list[str],
                        start_period: str = "2020",
                        claim_lc: str = "") -> list[dict]:
    """Fetch SDMX-Daten für eine Domain + Country-Set.

    Strategie: nutzt 'all' für die Filter-Key (OECD-SDMX akzeptiert keine
    REF_AREA-Filter mehr im Path verlässlich), filtert client-side.
    Ausnahme: 'piaac' baut einen expliziten 10-Key-Path mit
    TERRITORIAL_LEVEL=CTRY (sonst kommen TL2-Regionaldaten). Zusätzlich
    Strategy-3-Fallback auf statische PIAAC-2023-Country-Mean-Scores
    bei Nicht-EU-LFS-Ländern (USA/JPN/UK/KOR/AUS etc.).
    """
    dom_info = _DOMAINS[dom_id]
    target_set = {c.upper() for c in target_iso}
    cache_key = f"{dom_id}::{'_'.join(sorted(target_set))}::{start_period}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    flow = dom_info["flow"]
    url = _build_domain_url(dom_id, flow, target_iso, start_period)
    if not url:
        # Domain-Adapter signalisierte: kein passender Country.
        # Für PIAAC: prüfe Static-Fallback (OECD-publizierte Mean-Scores).
        if dom_id == "piaac":
            static_results = _build_piaac_static_results(
                target_iso, dom_info, claim_lc
            )
            if static_results:
                logger.info(
                    f"oecd_sdmx: piaac → {len(static_results)} static-fallback "
                    f"results für {','.join(sorted(target_set))}"
                )
                _cache_put(cache_key, static_results)
                return static_results
        logger.info(
            f"oecd_sdmx: {dom_id} → skip, keine unterstützten Länder in "
            f"{','.join(sorted(target_set))}"
        )
        _cache_put(cache_key, [])
        return []
    try:
        resp = await client.get(url, headers={
            "Accept": "application/vnd.sdmx.data+json",
        })
    except Exception as e:
        logger.debug(f"oecd_sdmx: {dom_id} request failed: {e}")
        _cache_put(cache_key, [])
        return []

    if resp.status_code == 429:
        logger.warning(f"oecd_sdmx: {dom_id} rate-limited (429)")
        return []
    if resp.status_code == 404:
        logger.info(f"oecd_sdmx: {dom_id} dataflow not found (404)")
        _cache_put(cache_key, [])
        return []
    if resp.status_code != 200:
        logger.warning(f"oecd_sdmx: {dom_id} HTTP {resp.status_code}")
        return []

    try:
        payload = resp.json()
    except Exception as e:
        logger.debug(f"oecd_sdmx: {dom_id} JSON-parse failed: {e}")
        return []

    results = _parse_sdmx_json(payload, dom_info, target_set, dom_id)

    # PIAAC-Fallback: Wenn live-SDMX nichts liefert UND Claim spezifisch
    # PIAAC/Literacy/Numeracy fragt → ergänze Static-Results.
    if dom_id == "piaac" and not results:
        static_results = _build_piaac_static_results(target_iso, dom_info, claim_lc)
        if static_results:
            logger.info(
                f"oecd_sdmx: piaac → {len(static_results)} static-fallback "
                f"(SDMX leer) für {','.join(sorted(target_set))}"
            )
            _cache_put(cache_key, static_results)
            return static_results

    _cache_put(cache_key, results)
    logger.info(
        f"oecd_sdmx: {dom_id} → {len(results)} Treffer für "
        f"{','.join(sorted(target_set)) or '*'}"
    )
    return results


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_oecd_sdmx(analysis: dict) -> dict:
    """Live-Lookup gegen OECD SDMX (Multi-Domain).

    Strategie:
      1. Domain-Detection (TALIS/SOCX/Family/Housing/PIAAC)
      2. Country-Detection (38 OECD-Länder, Default AT)
      3. Pro Domain ein SDMX-Call (max 2 Domains parallel um Rate-Limit zu schonen)
      4. Top-3 Indikatoren je Domain
    """
    empty = {
        "source": "OECD SDMX",
        "type": "oecd_data",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_oecd_sdmx(matchable):
        return empty

    domains = _matches_any_domain(matchable)
    if not domains:
        # Generic-Trigger: nimm SOCX als Default-Statistik (häufigste OECD-Anfrage)
        domains = ["socx"]
    # Max 2 Domains, um OECD-Rate-Limit nicht zu reißen
    domains = domains[:2]

    countries = _extract_countries(analysis, matchable)
    target_iso = [c[0] for c in countries]

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for dom_id in domains:
            try:
                dom_results = await _fetch_domain(
                    client, dom_id, target_iso, claim_lc=matchable
                )
            except Exception as e:
                logger.warning(f"oecd_sdmx: {dom_id} unexpected error: {e}")
                continue
            results.extend(dom_results)

    if not results:
        logger.info(
            f"oecd_sdmx: 0 Treffer (domains={domains}, "
            f"countries={','.join(target_iso)})"
        )
        return empty

    return {
        "source": "OECD SDMX",
        "type": "oecd_data",
        "results": results[: MAX_RESULTS_PER_DOMAIN * 2],  # cap insgesamt
    }
