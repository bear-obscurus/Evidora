"""ITF / OECD Transport Statistics — Live-API-Connector (Road Safety + Trends).

Quelle: International Transport Forum (ITF) — eine zwischenstaatliche
Organisation mit ~66 Mitgliedstaaten, organisatorisch an der OECD in Paris
angesiedelt. Die ITF-Statistik-Plattform ist die zentrale internationale
Datensammlung für Verkehrssicherheit (IRTAD — International Traffic Safety
Data and Analysis Group, seit 1988) sowie für Verkehrs-Aktivität,
Infrastruktur und Investitionen.

Dieser Service deckt die zwei wichtigsten ITF-Dataflows ab:

  * `OECD.ITF,DSD_TRENDS@DF_TRENDSSAFETY,1.0`
    — **Annual road fatalities, injured, injury crashes** (IRTAD-Datenbank).
    55 Länder × 3 Indikatoren (Tote / Verletzte / Unfälle mit Personenschaden) ×
    Jahre 2020-2024. Absolute Zahlen.

  * `OECD.ITF,DSD_INDICATORS@DF_SAFETY,1.0`
    — **Transport safety indicators** (Raten pro Bezugsgröße).
    53 Länder × 3 Rate-Einheiten (pro 100 000 Einwohner / pro Mrd. Fahrzeug-km /
    pro 10 000 Kraftfahrzeuge) × Jahre 2022-2025.

Zusätzlich (sekundär, opportunistisch):

  * `OECD.ITF,DSD_TRENDS@DF_TRENDS,1.0`
    — **Annual transport trends** (Infrastruktur-Längen, Fahrzeugbestand,
    Passagier-/Frachtleistung). 59 Länder, breitere Indikatoren-Liste.

Lizenz: ITF/OECD Statistik-Daten stehen unter den OECD Terms & Conditions
mit Zitierpflicht (effektiv CC-BY-äquivalent). Der `source`-Feld in jedem
Result enthält die Attribution explizit.

API: https://sdmx.oecd.org/public/rest/data/{flow}?...
  * SDMX-JSON-Format (gleicher Endpoint wie OECD SDMX in services/oecd_sdmx.py)
  * Kein Auth, höflich = 1 req/s

Architektur:
  Wir nutzen den **`all`-Filter** + clientseitiges Country-Filtering, wie auch
  oecd_sdmx.py es macht — der OECD-SDMX-Path akzeptiert REF_AREA-Filter
  nicht zuverlässig.

Trigger-Strategie:
  1. Verkehrs-Safety-Kernbegriffe (Verkehrstote, Road Deaths, ...) ODER
  2. Verkehrs-Trends + Land-Stem (Mobilität + AT/DE/EU ...)
  3. Hard-Skip: Hardcore-Politik-Begriffe (Mietendeckel, etc.) sind ohnehin
     ausgeschlossen, da unsere Trigger-Tokens darauf nicht reagieren.

Politische Guardrails (memory/project_political_guardrails.md):
  * Reine Statistik-Wiedergabe, keine Bewertung
  * Bei normativen Begriffen ("Verkehrspolitik versagt") nur deskriptive
    Zahlen, keine kausale Erklärung
  * Methodische Hinweise im `description`-Feld
"""

# WIRING für main.py (NICHT automatisch eingefügt — bitte manuell ergänzen):
# from services.itf_transport import (
#     search_itf_transport, claim_mentions_itf_transport_cached,
# )
# if claim_mentions_itf_transport_cached(claim):
#     tasks.append(cached("ITF Transport", search_itf_transport, analysis))
#     queried_names.append("ITF/OECD Transport (IRTAD + Trends)")
#
# WIRING für services/reranker (Whitelist):
# "ITF/OECD IRTAD Road Safety" + "ITF/OECD Transport Trends" eintragen.

from __future__ import annotations

import logging
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Endpoints / Konfiguration
# ---------------------------------------------------------------------------
SDMX_BASE = "https://sdmx.oecd.org/public/rest/data"
TIMEOUT_S = 15.0
CACHE_TTL_S = 24 * 3600  # 24h
MAX_RESULTS_PER_DOMAIN = 3
MAX_TOTAL_RESULTS = 6

DATA_EXPLORER_URL = (
    "https://www.itf-oecd.org/transport-data"
)
IRTAD_URL = (
    "https://www.itf-oecd.org/IRTAD"
)

# ---------------------------------------------------------------------------
# Country-Mapping (55 ITF-Länder, Subset)
# ITF-Statistik-Plattform deckt OECD + zusätzliche ITF-Mitglieder + manche
# Beobachter-Länder ab. Hier die häufigsten DE/EN-Namensformen.
# ---------------------------------------------------------------------------
_ITF_COUNTRIES: dict[str, tuple[str, str]] = {
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
    "rumänien": ("ROU", "Rumänien"),
    "romania": ("ROU", "Rumänien"),
    "bulgarien": ("BGR", "Bulgarien"),
    "bulgaria": ("BGR", "Bulgarien"),
    "kroatien": ("HRV", "Kroatien"),
    "croatia": ("HRV", "Kroatien"),
    # Süd-EU
    "griechenland": ("GRC", "Griechenland"),
    "greece": ("GRC", "Griechenland"),
    # UK
    "vereinigtes königreich": ("GBR", "Vereinigtes Königreich"),
    "united kingdom": ("GBR", "Vereinigtes Königreich"),
    "großbritannien": ("GBR", "Vereinigtes Königreich"),
    "uk": ("GBR", "Vereinigtes Königreich"),
    # Türkei
    "türkei": ("TUR", "Türkei"),
    "türkiye": ("TUR", "Türkei"),
    "turkey": ("TUR", "Türkei"),
    # Außer-EU
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
    "russland": ("RUS", "Russland"),
    "russia": ("RUS", "Russland"),
    "serbien": ("SRB", "Serbien"),
    "serbia": ("SRB", "Serbien"),
    # Westbalkan (ITF-Mitglieder)
    "bosnien": ("BIH", "Bosnien-Herzegowina"),
    "albanien": ("ALB", "Albanien"),
    "albania": ("ALB", "Albanien"),
    "ukraine": ("UKR", "Ukraine"),
    "moldau": ("MDA", "Moldau"),
    "georgien": ("GEO", "Georgien"),
    "georgia": ("GEO", "Georgien"),
    "armenien": ("ARM", "Armenien"),
    "armenia": ("ARM", "Armenien"),
}

# ---------------------------------------------------------------------------
# Indikator-Labels — die SDMX-MEASURE-IDs werden hier in DE-Labels übersetzt
# ---------------------------------------------------------------------------
_MEASURE_LABELS: dict[str, str] = {
    "FATALITIES": "Verkehrstote",
    "INJURED": "Verletzte im Straßenverkehr",
    "CRASHES": "Unfälle mit Personenschaden",
    "FREIGHT": "Frachtleistung (Tonnenkilometer)",
    "PASS": "Personenverkehrsleistung (Personenkilometer)",
    "INFRASTRUCTURE": "Verkehrsinfrastruktur-Länge",
    "CONTAINER": "Container-Transport",
    "EQUIPMENT": "Fahrzeugbestand",
    "TRAFFIC": "Verkehrsleistung",
}

_UNIT_LABELS: dict[str, str] = {
    "PS": "Personen",
    "CRA": "Unfälle",
    "10P5HB": "pro 100 000 Einwohner",
    "10P9VEHKM": "pro Mrd. Fahrzeug-km",
    "10P4VEH_MOT_ROAD": "pro 10 000 Kraftfahrzeuge",
    "KM": "km",
    "T": "Tonnen",
    "TKM": "Tonnenkilometer",
    "PKM": "Personenkilometer",
    "NUMBER": "",
}

_MODE_LABELS: dict[str, str] = {
    "ROAD": "Straße",
    "RAIL": "Schiene",
    "IWW": "Binnenwasserstraßen",
    "AIR": "Luftverkehr",
    "SEA": "Seeverkehr",
    "TOT_INL": "Inland (gesamt)",
    "_T": "alle Verkehrsträger",
}

# ---------------------------------------------------------------------------
# Dataflow-Definitionen
# ---------------------------------------------------------------------------
_DOMAINS: dict[str, dict] = {
    "safety_abs": {
        # Annual road fatalities, injured, injury crashes (IRTAD-Datenbank)
        "flow": "OECD.ITF,DSD_TRENDS@DF_TRENDSSAFETY,1.0",
        "label": "ITF IRTAD — Annual Road Safety (Fatalities / Injured / Crashes)",
        "label_short": "IRTAD Road Safety",
        "url": IRTAD_URL,
        "description_methodology": (
            "ITF/OECD IRTAD-Datenbank — International Traffic Safety Data "
            "and Analysis Group. Quartals- und Jahresdaten zu Verkehrstoten, "
            "Verletzten und Unfällen mit Personenschaden. 55 Länder, "
            "Annual-Frequency. Verkehrstote: 30-Tage-Definition (gilt für "
            "die meisten Länder). Quelle: nationale Polizei- und "
            "Verkehrssicherheitsbehörden, harmonisiert von der ITF."
        ),
        "start_period": "2020",
    },
    "safety_rate": {
        # Transport safety indicators (per 100k inhabitants / per bn vehicle-km / per 10k motor vehicles)
        "flow": "OECD.ITF,DSD_INDICATORS@DF_SAFETY,1.0",
        "label": "ITF/OECD Transport Safety Indicators (Raten pro Bezugsgröße)",
        "label_short": "Safety Rates",
        "url": DATA_EXPLORER_URL,
        "description_methodology": (
            "ITF/OECD Transport Safety Indicators — Verkehrstoten-Raten "
            "normiert auf 100 000 Einwohner, pro Mrd. Fahrzeug-Kilometer "
            "und pro 10 000 Kraftfahrzeuge. 53 Länder, Annual-Frequency. "
            "Internationale Vergleichbarkeit höher als bei Absolut-Zahlen, "
            "da Größeneffekte und Verkehrsleistung herausgerechnet sind."
        ),
        "start_period": "2022",
    },
    "trends": {
        # Annual transport trends (infrastructure, vehicle stock, passenger/freight)
        "flow": "OECD.ITF,DSD_TRENDS@DF_TRENDS,1.0",
        "label": "ITF/OECD Annual Transport Trends (Infrastruktur / Fahrzeuge / Verkehrsleistung)",
        "label_short": "Transport Trends",
        "url": DATA_EXPLORER_URL,
        "description_methodology": (
            "ITF/OECD Annual Transport Trends — Infrastruktur-Längen "
            "(Straße / Autobahn / Schiene), Fahrzeugbestand, "
            "Personen- und Güterverkehrsleistung. 59 Länder, "
            "Annual-Frequency. Quelle: nationale Verkehrsministerien."
        ),
        "start_period": "2022",
    },
}

# ---------------------------------------------------------------------------
# Trigger-Terme
# ---------------------------------------------------------------------------
# Hard-Trigger: Verkehrssicherheits-Kern (immer aktivieren)
_SAFETY_HARD_TERMS = (
    "verkehrstote", "verkehrstoten", "verkehrstoter",
    "road deaths", "road fatalities", "road fatality",
    "verkehrssicherheit", "road safety",
    "irtad",
    "verkehrsopfer",
    "verkehrsunfall", "verkehrsunfälle", "verkehrsunfaelle",
    "road accidents", "road crashes", "traffic crashes",
    "tödliche verkehrsunfälle", "tödliche verkehrsunfaelle",
    "verkehrstodesfälle", "verkehrstodesfaelle",
    "getötet im straßenverkehr", "getoetet im strassenverkehr",
    "im straßenverkehr getötet", "im strassenverkehr getoetet",
    "ums leben gekommen straßenverkehr",
    "pkw-tote", "pkw tote",
    "fußgänger-tote", "fußgänger tote",
    "fussgaenger-tote", "fussgaenger tote",
    "pedestrian deaths", "pedestrian fatalities",
    "fahrrad-tote", "fahrrad tote",
    "radfahrer-tote", "radfahrer tote",
    "cyclist deaths", "cyclist fatalities",
    "motorradtote", "motorrad-tote", "motorrad tote",
    "motorcyclist deaths", "motorcyclist fatalities",
    "straßenverkehrstote", "strassenverkehrstote",
    "vision zero",  # politisches Konzept = Verkehrssicherheits-Statistik
)

# Soft-Trigger: Mobilität / Verkehr (nur in Kombination mit Land oder Jahr)
_MOBILITY_SOFT_TERMS = (
    "mobilität", "mobilitaet", "mobility",
    "verkehrsleistung", "passenger transport", "freight transport",
    "personenkilometer", "tonnenkilometer",
    "autobahnnetz", "autobahnen", "motorway",
    "schienennetz", "schienen-länge", "rail length",
    "fahrzeugbestand", "vehicle stock",
    "pkw-dichte", "pkw dichte", "motorisierungsgrad",
    "motorization rate",
    "itf-statistik", "itf statistik", "international transport forum",
)

# Verkehrs-Schlüsselbegriffe (allgemein, für Composite-Trigger)
_TRANSPORT_KEYWORDS = (
    "verkehr", "transport", "traffic", "straßenverkehr", "strassenverkehr",
    "straße", "strasse", "road",
    "auto", "pkw", "kraftfahrzeug", "motorrad",
    "fahrrad", "radfahrer", "fußgänger", "fussgaenger", "pedestrian",
)

# Anti-Trigger: Begriffe, die zwar "verkehr" enthalten, aber NICHT
# Verkehrsstatistik sind (Zahlungsverkehr, Datenverkehr, ...)
_ANTI_TRIGGERS = (
    "zahlungsverkehr", "geldverkehr", "datenverkehr",
    "internet-verkehr", "internetverkehr", "internet traffic",
    "netzwerkverkehr", "network traffic",
    "geschlechtsverkehr",
    "fernsehverkehr",
)

# ---------------------------------------------------------------------------
# Trigger-Logik
# ---------------------------------------------------------------------------
def _is_anti_triggered(claim_lc: str) -> bool:
    """Anti-Trigger: 'Verkehr' in Daten/Zahlungs-Kontext skippen."""
    return any(t in claim_lc for t in _ANTI_TRIGGERS)


def _has_country(claim_lc: str) -> bool:
    return any(name in claim_lc for name in _ITF_COUNTRIES.keys())


def _has_year_2010_plus(claim_lc: str) -> bool:
    """Erkenne explizites Jahr 2010+ als Trigger-Hilfe (Verkehrsstatistik
    bezieht sich fast immer auf konkrete Jahre)."""
    import re
    return bool(re.search(r"\b20(1[0-9]|2[0-9])\b", claim_lc))


def _claim_mentions_itf_transport(claim_lc: str) -> bool:
    """Conservative Trigger:
      1. Anti-Trigger (Zahlungsverkehr etc.) → False
      2. Safety-Hard-Term → True
      3. Mobility-Soft-Term + (Country ODER Year) → True
      4. Verkehrs-Schlüsselbegriff + Country + (Year ODER Mortalitäts-Kontext)
         → True (z.B. "Wie viele Menschen starben 2023 in Österreich im Straßenverkehr?")
    """
    if not claim_lc:
        return False

    if _is_anti_triggered(claim_lc):
        return False

    # 1. Hard-Trigger
    if any(t in claim_lc for t in _SAFETY_HARD_TERMS):
        return True

    has_country = _has_country(claim_lc)
    has_year = _has_year_2010_plus(claim_lc)

    # 2. Soft-Trigger (Mobilität) — braucht Country oder Year
    if any(t in claim_lc for t in _MOBILITY_SOFT_TERMS):
        if has_country or has_year:
            return True
        # Standalone "itf-statistik" auch ohne Country/Year → True
        if "itf" in claim_lc:
            return True

    # 3. Composite: Verkehrs-Begriff + Country + Mortalitäts-/Anzahl-Kontext
    has_transport = any(t in claim_lc for t in _TRANSPORT_KEYWORDS)
    has_mortality_context = any(t in claim_lc for t in (
        "starb", "starben", "tot", "tote", "tod",
        "gestorben", "ums leben", "umkamen", "umgekommen",
        "killed", "deaths", "fatal", "died",
        "verletzt", "verletzte", "injured", "injuries",
        "unfall", "unfälle", "unfaelle", "crash", "crashes",
    ))
    if has_transport and has_country and (has_mortality_context or has_year):
        return True

    return False


# Modul-Level-Cache für Trigger-Check (24h)
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_itf_transport_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den ITF-Transport-Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_itf_transport(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Result-Cache (24h pro Domain × Country-Set)
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
    """Erkenne genannte ITF-Länder. Bevorzugt NER, fällt auf Substring.

    Returns: list[(ISO3, DisplayName)]. Default: [("AUT", "Österreich")].
    """
    ner_countries = (analysis or {}).get("ner_entities", {}).get("countries", [])
    text = " ".join(ner_countries).lower() + " " + claim_lc

    found: list[tuple[str, str]] = []
    seen = set()
    sorted_names = sorted(_ITF_COUNTRIES.keys(), key=len, reverse=True)
    for name in sorted_names:
        if name in text:
            iso, disp = _ITF_COUNTRIES[name]
            if iso not in seen:
                found.append((iso, disp))
                seen.add(iso)
    if not found:
        # Fallback: AT (Evidora-Standard)
        found.append(("AUT", "Österreich"))
    return found[:3]  # max 3 Länder pro Query


def _iso_to_display(iso: str) -> str:
    """Reverse-Lookup ISO-3 → Anzeige-Name."""
    if not iso:
        return ""
    for _, (code, disp) in _ITF_COUNTRIES.items():
        if code == iso:
            return disp
    return iso


def _detect_measure_filter(claim_lc: str) -> set[str] | None:
    """Wenn der Claim spezifisch nach Toten/Verletzten/Unfällen fragt,
    nur diese MEASURE liefern. Sonst None (= alle)."""
    filters: set[str] = set()
    if any(t in claim_lc for t in (
        "tote", "tot", "starb", "starben", "verkehrstote",
        "todesfälle", "todesfaelle", "fatalities", "deaths", "killed",
        "ums leben", "umkamen", "fatal",
    )):
        filters.add("FATALITIES")
    if any(t in claim_lc for t in (
        "verletzte", "verletzt", "injured", "injuries",
    )):
        filters.add("INJURED")
    if any(t in claim_lc for t in (
        "unfälle", "unfaelle", "unfall", "crash", "crashes",
    )) and "FATALITIES" not in filters:
        # "Unfall" alleine deutet auf CRASHES, aber wenn schon Tote im Claim
        # waren, ist FATALITIES dominant
        filters.add("CRASHES")
    return filters or None


def _select_domains(claim_lc: str) -> list[str]:
    """Welche Dataflows passen zum Claim?
      * Verkehrssicherheit (hart/weich) → safety_abs + safety_rate
      * Mobility-Soft-Terms → trends
    Max 2 Domains parallel.
    """
    domains: list[str] = []
    is_safety = (
        any(t in claim_lc for t in _SAFETY_HARD_TERMS)
        or "verkehrssicherheit" in claim_lc
    )
    if is_safety:
        domains.append("safety_abs")
        # Rate-Indikator zusätzlich, wenn explizit nach "pro 100k" / "Rate" /
        # "vergleich" / mehreren Ländern gefragt
        if any(t in claim_lc for t in (
            "pro 100", "per 100", "rate", "vergleich", "ranking",
            "höchste", "niedrigste", "highest", "lowest",
        )):
            domains.append("safety_rate")

    # Mobility-Trend-Begriffe
    is_trends = any(t in claim_lc for t in _MOBILITY_SOFT_TERMS)
    # Composite: Auto/PKW/Autobahn/Schiene im Claim → Trends
    if any(t in claim_lc for t in (
        "autobahn", "autobahnnetz", "motorway",
        "schienennetz", "rail network",
        "pkw-bestand", "pkw bestand", "fahrzeugbestand",
        "motorisierung", "motorization",
        "personenkilometer", "tonnenkilometer",
    )):
        is_trends = True
    if is_trends:
        domains.append("trends")

    # Wenn nichts spezifisch: Default = safety_abs (häufigster Use-Case)
    if not domains:
        domains.append("safety_abs")

    return domains[:2]  # cap auf 2 Domains


# ---------------------------------------------------------------------------
# SDMX-Parser
# ---------------------------------------------------------------------------
def _parse_sdmx_json(
    payload: dict,
    dom_info: dict,
    dom_id: str,
    target_iso: set[str],
    measure_filter: set[str] | None,
    transport_mode_pref: str = "ROAD",
) -> list[dict]:
    """Parse SDMX-JSON-Response für einen ITF-Dataflow.

    Filterung:
      * REF_AREA auf target_iso (clientseitig)
      * MEASURE auf measure_filter (wenn nicht None)
      * TRANSPORT_MODE bevorzugt 'ROAD' (für Safety); _T (Total) fallback
      * Nur die jeweils aktuellsten 2 Jahre pro (Country × Measure × Mode)
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

    # Dimension-Lookup
    dim_meta: list[dict] = []
    ref_area_idx: int | None = None
    time_idx: int | None = None
    measure_idx: int | None = None
    mode_idx: int | None = None
    unit_idx: int | None = None
    for d_idx, d in enumerate(dims):
        dim_id = d.get("id", "")
        vals = d.get("values") or []
        norm_vals = []
        for v in vals:
            name = v.get("name", "")
            if isinstance(name, dict):
                name = name.get("en") or name.get("de") or str(name)
            norm_vals.append({"id": v.get("id", ""), "name": name or v.get("id", "")})
        dim_meta.append({"id": dim_id, "values": norm_vals})
        if dim_id == "REF_AREA":
            ref_area_idx = d_idx
        elif dim_id == "TIME_PERIOD":
            time_idx = d_idx
        elif dim_id == "MEASURE":
            measure_idx = d_idx
        elif dim_id == "TRANSPORT_MODE":
            mode_idx = d_idx
        elif dim_id == "UNIT_MEASURE":
            unit_idx = d_idx

    # Allowed REF_AREA indices
    allowed_ref_area_indices: set[int] = set()
    if ref_area_idx is not None and target_iso:
        for v_i, v in enumerate(dim_meta[ref_area_idx]["values"]):
            if (v["id"] or "").upper() in target_iso:
                allowed_ref_area_indices.add(v_i)

    # Allowed MEASURE indices
    allowed_measure_indices: set[int] | None = None
    if measure_idx is not None and measure_filter:
        allowed_measure_indices = set()
        for v_i, v in enumerate(dim_meta[measure_idx]["values"]):
            if v["id"] in measure_filter:
                allowed_measure_indices.add(v_i)

    # Preferred TRANSPORT_MODE index (ROAD), fallback _T
    preferred_mode_index: int | None = None
    fallback_mode_index: int | None = None
    if mode_idx is not None:
        for v_i, v in enumerate(dim_meta[mode_idx]["values"]):
            if v["id"] == transport_mode_pref:
                preferred_mode_index = v_i
            if v["id"] == "_T":
                fallback_mode_index = v_i

    # Sammeln aller passenden Observations
    raw_rows: list[dict] = []
    for key_str, val_list in obs.items():
        if not val_list or val_list[0] is None:
            continue
        parts = key_str.split(":")
        if len(parts) < len(dim_meta):
            continue
        try:
            idx_tuple = [int(p) for p in parts]
        except ValueError:
            continue

        # Filter: REF_AREA
        if ref_area_idx is not None and allowed_ref_area_indices:
            if idx_tuple[ref_area_idx] not in allowed_ref_area_indices:
                continue
        # Filter: MEASURE
        if (measure_idx is not None and allowed_measure_indices is not None
                and idx_tuple[measure_idx] not in allowed_measure_indices):
            continue

        # Labels resolvieren
        labels: dict[str, str] = {}
        for i, v_i in enumerate(idx_tuple):
            if i >= len(dim_meta):
                break
            vals = dim_meta[i]["values"]
            if 0 <= v_i < len(vals):
                labels[dim_meta[i]["id"]] = vals[v_i]["name"]
                if i == ref_area_idx:
                    labels["_REF_AREA_ID"] = vals[v_i]["id"]
                if i == time_idx:
                    labels["_TIME_PERIOD_ID"] = vals[v_i]["id"]
                if i == measure_idx:
                    labels["_MEASURE_ID"] = vals[v_i]["id"]
                if i == mode_idx:
                    labels["_TRANSPORT_MODE_ID"] = vals[v_i]["id"]
                if i == unit_idx:
                    labels["_UNIT_MEASURE_ID"] = vals[v_i]["id"]

        value = val_list[0]
        if isinstance(value, float):
            # Verkehrstoten-Raten brauchen 2 Nachkommastellen
            value = round(value, 2)

        raw_rows.append({
            "labels": labels,
            "value": value,
            "ref_area_iso": labels.get("_REF_AREA_ID", "—"),
            "time_period": labels.get("_TIME_PERIOD_ID", "—"),
            "measure_id": labels.get("_MEASURE_ID", ""),
            "mode_id": labels.get("_TRANSPORT_MODE_ID", ""),
            "unit_id": labels.get("_UNIT_MEASURE_ID", ""),
        })

    if not raw_rows:
        return []

    # TRANSPORT_MODE bevorzugen: ROAD vor _T, restliche verwerfen.
    # Strategie:
    #   Pro (country, measure, year) nur ROAD; falls für (country, measure,
    #   year) kein ROAD existiert, _T als Fallback.
    if mode_idx is not None:
        grouped: dict[tuple, dict] = {}  # (ctry, meas, year) → best row
        for row in raw_rows:
            ctry = row["ref_area_iso"]
            meas = row["measure_id"]
            year = row["time_period"]
            mode = row["mode_id"]
            key = (ctry, meas, year)
            existing = grouped.get(key)
            # ROAD > _T > others
            prio = 0
            if mode == "ROAD":
                prio = 2
            elif mode == "_T":
                prio = 1
            if existing is None or prio > existing["_prio"]:
                row["_prio"] = prio
                grouped[key] = row
        raw_rows = list(grouped.values())

    # Sortiere: Country (in target-Reihenfolge), dann Year desc
    target_order = list(target_iso)
    def _sort_key(r: dict) -> tuple:
        ctry = r["ref_area_iso"]
        try:
            ctry_rank = target_order.index(ctry)
        except ValueError:
            ctry_rank = len(target_order)
        return (ctry_rank, r["measure_id"], -int(r["time_period"]) if r["time_period"].isdigit() else 0)
    raw_rows.sort(key=_sort_key)

    # Pro (country × measure) nur die zwei neuesten Jahre
    deduped: list[dict] = []
    seen_pairs: dict[tuple, int] = {}
    for row in raw_rows:
        pair = (row["ref_area_iso"], row["measure_id"])
        cnt = seen_pairs.get(pair, 0)
        if cnt >= 2:
            continue
        seen_pairs[pair] = cnt + 1
        deduped.append(row)

    # Schließlich: cap auf MAX_RESULTS_PER_DOMAIN, Result-Rows bauen
    results: list[dict] = []
    for row in deduped:
        if len(results) >= MAX_RESULTS_PER_DOMAIN:
            break
        labels = row["labels"]
        ctry_iso = row["ref_area_iso"]
        ctry_name = _iso_to_display(ctry_iso) or labels.get("REF_AREA", ctry_iso)
        year = row["time_period"]
        value = row["value"]
        measure_id = row["measure_id"]
        unit_id = row["unit_id"]
        mode_id = row["mode_id"]

        measure_label = _MEASURE_LABELS.get(measure_id) or labels.get(
            "MEASURE", measure_id or dom_info["label_short"]
        )
        unit_label = _UNIT_LABELS.get(unit_id, "")
        mode_label = _MODE_LABELS.get(mode_id, "")

        # display_value bauen
        value_str = (
            f"{value:.2f}".replace(".", ",")
            if isinstance(value, float) else f"{value}"
        )
        # Trailing .00 entfernen
        if value_str.endswith(",00"):
            value_str = value_str[:-3]

        unit_suffix = f" {unit_label}" if unit_label else ""
        mode_suffix = f" ({mode_label})" if mode_label and mode_id == "ROAD" else ""
        display_value = (
            f"{ctry_name} {year}{mode_suffix}: "
            f"{measure_label} = {value_str}{unit_suffix}"
        ).strip()

        results.append({
            "indicator_name": f"{dom_info['label']} — {measure_label} {ctry_name}",
            "indicator": f"itf_{dom_id}_{measure_id.lower()}_{ctry_iso.lower()}",
            "country": ctry_iso,
            "country_name": ctry_name,
            "year": year,
            "value": value,
            "display_value": display_value,
            "description": dom_info["description_methodology"],
            "url": dom_info["url"],
            "source": "ITF/OECD Statistics (CC-BY, attribution required)",
        })

    return results


# ---------------------------------------------------------------------------
# HTTP-Call pro Domain
# ---------------------------------------------------------------------------
async def _fetch_domain(
    client,
    dom_id: str,
    target_iso: list[str],
    measure_filter: set[str] | None,
) -> list[dict]:
    """Fetch SDMX-Daten für eine Domain + Country-Set."""
    dom_info = _DOMAINS[dom_id]
    target_set = {c.upper() for c in target_iso}
    cache_key = (
        f"itf::{dom_id}::{'_'.join(sorted(target_set))}::"
        f"{'_'.join(sorted(measure_filter)) if measure_filter else 'all'}"
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    flow = dom_info["flow"]
    start = dom_info["start_period"]
    url = (
        f"{SDMX_BASE}/{flow}/all"
        f"?startPeriod={start}&dimensionAtObservation=AllDimensions"
    )

    try:
        resp = await client.get(url, headers={
            "Accept": "application/vnd.sdmx.data+json",
        })
    except Exception as e:
        logger.debug(f"itf_transport: {dom_id} request failed: {e}")
        _cache_put(cache_key, [])
        return []

    if resp.status_code == 429:
        logger.warning(f"itf_transport: {dom_id} rate-limited (429)")
        return []
    if resp.status_code == 404:
        logger.info(f"itf_transport: {dom_id} dataflow not found (404)")
        _cache_put(cache_key, [])
        return []
    if resp.status_code != 200:
        logger.warning(f"itf_transport: {dom_id} HTTP {resp.status_code}")
        return []

    try:
        payload = resp.json()
    except Exception as e:
        logger.debug(f"itf_transport: {dom_id} JSON-parse failed: {e}")
        return []

    results = _parse_sdmx_json(
        payload, dom_info, dom_id, target_set, measure_filter,
        transport_mode_pref="ROAD",
    )

    _cache_put(cache_key, results)
    logger.info(
        f"itf_transport: {dom_id} → {len(results)} Treffer für "
        f"{','.join(sorted(target_set)) or '*'}"
    )
    return results


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_itf_transport(analysis: dict) -> dict:
    """Live-Lookup gegen ITF/OECD Transport Statistics.

    Strategie:
      1. Trigger-Check (defensiv — Caller hat das wahrscheinlich schon
         gemacht).
      2. Domain-Selection (safety_abs / safety_rate / trends).
      3. Country-Detection (Default Österreich).
      4. Measure-Filter (Tote / Verletzte / Unfälle) aus Claim-Begriffen.
      5. Pro Domain ein SDMX-Call (parallel via async, max 2 Domains).
      6. Top-5 Results insgesamt.

    Return-Schema:
      {
        "source": "ITF/OECD IRTAD Road Safety",
        "type": "transport_safety",
        "results": [...],
        "attribution": "..."
      }
    """
    empty = {
        "source": "ITF/OECD IRTAD Road Safety",
        "type": "transport_safety",
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

    if not _claim_mentions_itf_transport(matchable):
        return empty

    domains = _select_domains(matchable)
    countries = _extract_countries(analysis, matchable)
    target_iso = [c[0] for c in countries]
    measure_filter = _detect_measure_filter(matchable)

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for dom_id in domains:
            try:
                dom_results = await _fetch_domain(
                    client, dom_id, target_iso, measure_filter,
                )
            except Exception as e:
                logger.warning(f"itf_transport: {dom_id} unexpected: {e}")
                continue
            results.extend(dom_results)

    if not results:
        logger.info(
            f"itf_transport: 0 Treffer (domains={domains}, "
            f"countries={','.join(target_iso)}, measure={measure_filter})"
        )
        return empty

    # Total cap
    if len(results) > MAX_TOTAL_RESULTS:
        results = results[:MAX_TOTAL_RESULTS]

    return {
        "source": "ITF/OECD IRTAD Road Safety",
        "type": "transport_safety",
        "results": results,
        "attribution": (
            "International Transport Forum (ITF), OECD Statistics. "
            "Daten unter OECD-Lizenzbedingungen (Zitierpflicht). "
            "Verkehrstoten-Definition: 30-Tage-Frist nach Unfall (Standard "
            "der meisten ITF/IRTAD-Länder)."
        ),
    }
