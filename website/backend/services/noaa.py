"""NOAA NCEI Climate Data Online (CDO v2) — Globale historische Klimadaten.

Quelle: NOAA National Centers for Environmental Information (NCEI),
Climate Data Online API v2. Liefert Wetterstations- und
Klimasummary-Daten (GHCN daily/monthly, GSOM, GSOY, Storm Events,
Hurricane-Saison-Statistik). Komplementär zu ECMWF ERA5 (`era5.py`)
und Copernicus C3S (`copernicus.py`).

Datenzugang:
  - REST-API: https://www.ncei.noaa.gov/cdo-web/api/v2/
  - Endpoints: /data, /stations, /datasets, /datatypes, /locations
  - Auth: Token OPTIONAL via env ``NOAA_API_TOKEN`` (gratis via
    https://www.ncdc.noaa.gov/cdo-web/token). Ohne Token funktioniert
    der Katalog-Browse mit DEMO_TOKEN limitiert; mit Token 10.000
    Requests/Tag, 5 req/s.
  - Format: JSON
  - Lizenz: US Public Domain (Werke der US-Bundesregierung).

Architektur-Hinweis — Pack+Live-Hybrid (cf. era5.py):
  Die NOAA-CDO-API erlaubt schnelle Katalog-/Station-Lookups (~1–2 s),
  Datenpunkt-Anfragen ("alle Tagestemperaturen für Station X im Jahr Y")
  können bei breiteren Anfragen aber träger werden. Wir verwenden daher
  das ERA5-bewährte Pattern:

    1. Kuratierte NOAA-Eckwerte als statische Live-Hooks (globale
       Temperatur-Anomalie 2024, Atlantic-Hurricane-Saison-Rekorde,
       Tornado-Statistik USA, höchste je gemessene Temperatur Death
       Valley). Diese sind WMO-/NCEI-Konsens und brauchen keine API.
    2. Optional: NCEI-Dataset-Metadaten / Stations-Suche nachladen
       (geht in 1–2 s). Liefert Referenz-URL für den Synthesizer.
    3. Ohne Token oder bei Fehler: nur Pack-Eckwerte (Graceful Fail).

Komplementär zu:
  - era5.py: globale Reanalyse, hochauflösendes Gitter, Rekord-Patterns
  - copernicus.py: NASA GISS + Berkeley Earth-Mix
  - noaa.py (dieser Service): US-zentrierte Storm-Events, GHCN-Stationen,
    Hurricane-/Tornado-Statistik, globale NOAA-Klimabilanzen

Hard-Skips:
  - Rein-AT-Wetter-Claims (Station Wien-Hohe Warte, SPARTACUS-Grid):
    gehen zu geosphere/zamg-Services, nicht NOAA.

Politische Guardrails: Reine Datenquelle. KEINE
Attribution-Behauptungen ("Klimawandel hat Hurricane X verursacht").
Eckwerte sind faktuelle Rekord-Zahlen; die kausale Einordnung
übernimmt der Synthesizer mit IPCC-/NOAA-Disclaimer.

WIRING für main.py (NICHT in dieser Datei vornehmen):
  from services.noaa import search_noaa, claim_mentions_noaa_cached
  if claim_mentions_noaa_cached(claim):
      tasks.append(cached("NOAA", search_noaa, analysis))
      queried_names.append("NOAA NCEI")
  # Zusätzlich:
  #   - reranker-Whitelist: "NOAA NCEI" eintragen
  #   - data_updater.py: KEIN Prefetch nötig (Pack-Eckwerte sind statisch,
  #     Katalog-Call ist <2s und passiert pro Request).
"""

from __future__ import annotations

import logging
import os
import re
import time
from functools import lru_cache

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# NOAA CDO v2 Endpoints
NOAA_CDO_BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2"
NOAA_DATASETS_URL = f"{NOAA_CDO_BASE}/datasets"
NOAA_STATIONS_URL = f"{NOAA_CDO_BASE}/stations"
NOAA_DATA_URL = f"{NOAA_CDO_BASE}/data"

# Token (optional) — without it CDO requests fail, but Pack-Eckwerte gehen immer
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN", "").strip()

CACHE_TTL_S = 24 * 60 * 60  # 24 h
TIMEOUT_S = 8.0  # Katalog-Call; Pack-Eckwerte brauchen kein Netz

# Modul-Cache für Katalog-Metadaten (selten geändert, 24h reicht)
_catalogue_cache: dict[str, tuple[float, dict | None]] = {}


# ---------------------------------------------------------------------------
# Trigger — wann soll dieser Service angefragt werden?
# ---------------------------------------------------------------------------
_NOAA_TERMS = (
    # Direkter Name
    "noaa", "ncei", "ncdc", "national oceanic", "national centers for environmental",
    "climate data online", "cdo-web",
    # GHCN / Datensätze
    "ghcn", "ghcnd", "ghcn-daily", "ghcn-monthly", "global historical climatology",
    "gsom", "gsoy", "storm events database", "storm events",
    # Hurricane / Tropische Wirbelstürme (US/atlantic-zentriert)
    "hurricane", "hurrikan", "hurricanes", "hurrikane",
    "atlantic hurricane", "atlantik-hurrikan", "atlantik hurrikan",
    "tropensturm", "tropical storm", "tropical cyclone",
    "saffir-simpson", "kategorie 4", "kategorie 5", "category 4", "category 5",
    # Tornados (US-Schwerpunkt)
    "tornado", "tornados", "tornado alley", "ef5", "ef-5", "ef4", "ef-4",
    "fujita-skala", "fujita scale",
    # Globale Klima-Bilanzen NOAA-spezifisch
    "noaa-klima", "noaa klima", "noaa climate",
    "globale temperatur-anomalie noaa", "global temperature anomaly noaa",
    "nclimdiv", "nclimgrid",
    # Death Valley / US-Klimarekorde
    "death valley", "furnace creek",
)

# Composite-Trigger: US-Wetterereignis-Patterns
_US_WEATHER_VERBS = (
    "saison", "season", "rekord", "record",
    "höchste", "stärkste", "schwerste",
    "deadliest", "strongest", "most destructive",
)

_US_WEATHER_NOUNS = (
    "hurricane", "hurrikan", "tornado", "atlantic", "atlantik",
    "tropensturm", "tropical",
)

_YEAR_REGEX = re.compile(r"\b(18[5-9]\d|19\d\d|20\d\d|21\d\d)\b")

# AT/EU-Klima-Indikatoren, bei denen wir NICHT triggern wollen
# (GeoSphere/SPARTACUS/ZAMG-Domain). Wenn der Claim eindeutig AT-only ist,
# bleiben wir raus.
_AT_ONLY_MARKERS = (
    "spartacus", "geosphere", "zamg", "hohe warte",
    "alpenraum", "österreichisches klima",
)


def _claim_mentions_noaa(claim_lc: str) -> bool:
    """Trigger-Pre-Check für NOAA NCEI.

    True wenn:
      1. Direkter NOAA-/GHCN-/Hurricane-/Tornado-Begriff im Claim, ODER
      2. US-Wetter-Verb + US-Wetter-Substantiv (klassische
         "Atlantic Hurricane Season 2024"-Patterns).
    False wenn der Claim eindeutig AT-only-Klima ist (SPARTACUS-Domain).
    """
    if not claim_lc:
        return False

    # Hard-Skip: rein-AT-Klima-Marker
    # Aber nur Skip wenn KEIN expliziter NOAA-Begriff vorkommt (manche
    # Vergleichs-Claims kombinieren beide).
    has_at_only_marker = any(m in claim_lc for m in _AT_ONLY_MARKERS)
    has_direct_noaa = any(t in claim_lc for t in _NOAA_TERMS)

    if has_at_only_marker and not has_direct_noaa:
        return False

    # 1) Direkt-Trigger
    if has_direct_noaa:
        return True

    # 2) Composite: US-Wetter-Verb + US-Wetter-Substantiv (ggf. Jahr)
    has_verb = any(v in claim_lc for v in _US_WEATHER_VERBS)
    has_noun = any(n in claim_lc for n in _US_WEATHER_NOUNS)
    if has_verb and has_noun:
        if _YEAR_REGEX.search(claim_lc) or len(claim_lc) > 40:
            return True

    return False


@lru_cache(maxsize=512)
def claim_mentions_noaa_cached(claim: str) -> bool:
    """Public-Wrapper für Trigger-Check (case-normalisiert, LRU-gecached)."""
    return _claim_mentions_noaa((claim or "").lower())


# ---------------------------------------------------------------------------
# Kuratierte NOAA-Eckwerte ("Pack-Fallback")
# ---------------------------------------------------------------------------
# Konsens-Highlights aus NOAA-Quellen, die ohne Live-Query reproduzierbar
# sind. Quellen:
#   - NOAA NCEI State of the Climate Reports (jährlich)
#   - NOAA NHC Tropical Cyclone Reports
#   - NOAA SPC Tornado Annual Summaries
#   - NCEI Daily Global Highlights
#
# Zahlenwerte sind gerundet auf 1–2 Nachkommastellen, mit Quellen-URL.
_NOAA_HIGHLIGHTS: list[dict] = [
    {
        "title": (
            "NOAA-Globaler Temperatur-Rekord 2024: wärmstes Jahr seit "
            "Beginn der NOAA-Aufzeichnungen 1850 (Land+Ozean-Mittel +1.46°C "
            "über 20.-Jh.-Durchschnitt, ca. +1.55°C über vorindustriell)"
        ),
        "indicator_name": "NOAA Global Land+Ocean 2024",
        "indicator": "noaa_global_temp_2024",
        "year": "2024",
        "value": "+1.46°C vs. 20.-Jh.-Mittel (+1.55°C vs. vorindustriell)",
        "description": (
            "Laut NOAA NCEI war 2024 das wärmste Jahr seit Beginn der "
            "globalen Aufzeichnungen 1850. Die globale Land+Ozean-"
            "Temperaturanomalie lag bei +1.46°C über dem 20.-Jh.-Mittel "
            "(1901–2000). Das entspricht ca. +1.55°C über vorindustriellem "
            "Niveau (1850–1900). Alle 10 wärmsten Jahre der NOAA-Reihe "
            "liegen seit 2014. NOAA bestätigt damit unabhängig den "
            "Copernicus/ERA5-Befund (siehe era5_service)."
        ),
        "source": "NOAA NCEI Annual Global Climate Report 2024",
        "url": "https://www.ncei.noaa.gov/access/monitoring/monthly-report/global/202413",
    },
    {
        "title": (
            "Atlantic-Hurricane-Saison 2024: 18 benannte Stürme, 11 Hurrikane, "
            "5 Major Hurricanes (Kat. 3+); überdurchschnittlich aktiv"
        ),
        "indicator_name": "Atlantic Hurricane Season 2024",
        "indicator": "noaa_atlantic_hurricane_2024",
        "year": "2024",
        "value": "18 benannte Stürme, 11 Hurrikane, 5 Major Hurricanes",
        "description": (
            "Die Atlantik-Hurrikan-Saison 2024 (1. Juni – 30. November) "
            "produzierte laut NOAA NHC 18 benannte Stürme, davon 11 "
            "Hurrikane und 5 Major Hurricanes (Kategorie 3 oder höher auf "
            "der Saffir-Simpson-Skala). Das langjährige Mittel (1991–2020) "
            "liegt bei 14 benannten Stürmen / 7 Hurrikanen / 3 Majors — "
            "2024 lag damit deutlich über dem Durchschnitt. Hurrikan "
            "Helene und Milton (beide Major) verursachten die größten "
            "Schäden in Florida und den südlichen USA."
        ),
        "source": "NOAA National Hurricane Center",
        "url": "https://www.nhc.noaa.gov/data/tcr/",
    },
    {
        "title": (
            "Atlantic-Hurricane-Saison 2020: Rekord-Saison mit 30 benannten "
            "Stürmen — bisher aktivste Saison seit Aufzeichnungsbeginn 1851"
        ),
        "indicator_name": "Atlantic Hurricane Season 2020 (Rekord)",
        "indicator": "noaa_atlantic_hurricane_2020_record",
        "year": "2020",
        "value": "30 benannte Stürme (Rekord)",
        "description": (
            "Die Atlantik-Hurrikan-Saison 2020 ist mit 30 benannten Stürmen "
            "die aktivste je gemessene Atlantik-Saison seit Aufzeichnungs-"
            "beginn 1851 — sie übertraf den bisherigen Rekord von 2005 "
            "(28 Stürme). 14 davon wurden zu Hurrikanen, 7 zu Major "
            "Hurricanes. Das offizielle Namens-Alphabet wurde aufgebraucht; "
            "NOAA musste auf griechische Buchstaben zurückgreifen "
            "(zuletzt 2005). Seitdem führt die WMO eine ergänzende "
            "Namensliste statt der griechischen Buchstaben."
        ),
        "source": "NOAA NHC / NCEI",
        "url": "https://www.nhc.noaa.gov/data/tcr/",
    },
    {
        "title": (
            "US-Tornado-Jahresbilanz 2024: ~1.860 bestätigte Tornados — "
            "zweithöchste Jahressumme seit Aufzeichnungsbeginn 1950"
        ),
        "indicator_name": "US-Tornados 2024",
        "indicator": "noaa_us_tornadoes_2024",
        "year": "2024",
        "value": "~1.860 Tornados (zweithöchste Jahressumme)",
        "description": (
            "Laut NOAA Storm Prediction Center (SPC) verzeichneten die "
            "USA 2024 etwa 1.860 bestätigte Tornados — die zweithöchste "
            "Jahressumme seit Beginn der systematischen Aufzeichnung 1950 "
            "(Rekord: 2004 mit ~1.817 nach finaler Korrektur, 2024 "
            "vorläufig höher). Das langjährige Mittel (1991–2020) liegt "
            "bei ~1.200 Tornados/Jahr. Die EF5-Skala (höchste Kategorie) "
            "wurde 2024 nicht erreicht; der stärkste Tornado war EF4."
        ),
        "source": "NOAA SPC / NCEI Storm Events Database",
        "url": "https://www.spc.noaa.gov/climo/online/monthly/newm.html",
    },
    {
        "title": (
            "Höchste je auf der Erde gemessene Temperatur: 56.7°C in "
            "Furnace Creek (Death Valley, Kalifornien), 10. Juli 1913"
        ),
        "indicator_name": "Welt-Temperaturrekord Death Valley",
        "indicator": "noaa_death_valley_record",
        "year": "1913",
        "value": "56.7°C (134°F)",
        "description": (
            "Die offiziell höchste je auf der Erde an einer Wetterstation "
            "gemessene Lufttemperatur beträgt 56.7°C (134°F), gemessen am "
            "10. Juli 1913 an der NOAA-Station Furnace Creek im Death "
            "Valley, Kalifornien. Die WMO bestätigte diesen Wert 2016 nach "
            "einer Überprüfung. Ein konkurrierender Wert (58.0°C, El Azizia, "
            "Libyen 1922) wurde 2012 von der WMO als ungültig disqualifiziert "
            "(Messfehler). Death Valley hält auch den Rekord für die "
            "höchste je gemessene Bodentemperatur (93.9°C, 15. Juli 1972)."
        ),
        "source": "NOAA NCEI / WMO Archive of Weather and Climate Extremes",
        "url": "https://wmo.asu.edu/content/world-highest-temperature",
    },
    {
        "title": (
            "GHCN-Stationsnetz: ~115.000 Wetterstationen weltweit liefern "
            "tägliche Klimadaten — Rückgrat globaler Temperatur-Rekonstruktion"
        ),
        "indicator_name": "GHCN-Daily Stationsnetz",
        "indicator": "noaa_ghcn_network",
        "year": "heute",
        "value": "~115.000 aktive Stationen weltweit",
        "description": (
            "Das Global Historical Climatology Network Daily (GHCN-Daily) "
            "von NOAA NCEI integriert tägliche Wetter-Beobachtungen aus "
            "~115.000 Stationen in über 180 Ländern. Die ältesten "
            "Stationsreihen reichen bis 1763 zurück. GHCN ist das primäre "
            "Stationsnetz für globale Temperatur-Rekonstruktionen "
            "(NOAA NOAAGlobalTemp, NASA GISS GISTEMP, Berkeley Earth, "
            "Hadley Centre HadCRUT). Lizenz: US Public Domain."
        ),
        "source": "NOAA NCEI Global Historical Climatology Network",
        "url": "https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily",
    },
    {
        "title": (
            "NOAA Storm Events Database: lückenlose US-Unwetter-Chronik "
            "seit 1950 — Tornados, Hurrikane, Hagel, Sturmfluten, "
            "Schadensbilanzen"
        ),
        "indicator_name": "NOAA Storm Events Database",
        "indicator": "noaa_storm_events_db",
        "year": "1950–heute",
        "value": "1+ Million Einzelereignisse",
        "description": (
            "Die Storm Events Database (StormEvents) von NOAA NCEI "
            "dokumentiert lückenlos US-Unwetterereignisse seit 1950 — "
            "Tornados, Hurrikane, Hagel, Flash Floods, Schneestürme, "
            "Sturmfluten — mit über 1 Million Einzelereignissen, jeweils "
            "mit Geokoordinaten, Schadens- und Todesfallzahlen. Primärer "
            "US-Referenzdatensatz für historische Schadensereignisse "
            "(NOAA Billion-Dollar Disasters bauen darauf auf)."
        ),
        "source": "NOAA NCEI Storm Events Database",
        "url": "https://www.ncdc.noaa.gov/stormevents/",
    },
    {
        "title": (
            "NOAA Billion-Dollar Weather/Climate Disasters: 2024 = "
            "27 US-Schadenereignisse mit jeweils ≥ 1 Mrd. USD Schaden "
            "(zweithöchste Jahresanzahl je)"
        ),
        "indicator_name": "Billion-Dollar Disasters USA 2024",
        "indicator": "noaa_billion_dollar_2024",
        "year": "2024",
        "value": "27 Ereignisse mit ≥ 1 Mrd. USD Schaden",
        "description": (
            "NOAA NCEI dokumentiert für 2024 insgesamt 27 US-Wetter-/Klima-"
            "Katastrophen mit jeweils mindestens 1 Mrd. USD Schaden "
            "(inflationsbereinigt) — die zweithöchste Jahressumme nach "
            "2023 (28 Ereignisse). Hurrikan Helene allein verursachte "
            "~78.7 Mrd. USD Schaden und ~250 Todesfälle. Das langjährige "
            "Mittel (1980–2023, inflationsbereinigt) liegt bei ~8 solchen "
            "Ereignissen/Jahr; die Frequenz hat sich seit 2010 verdreifacht."
        ),
        "source": "NOAA NCEI Billion-Dollar Weather and Climate Disasters",
        "url": "https://www.ncei.noaa.gov/access/billions/",
    },
]


def _noaa_methodology_row() -> dict:
    """Methodik-Disclaimer (V-Dem/ERA5-Pattern): nennt dem Synthesizer
    die Einschränkungen von NOAA-Daten — bevor er aus den Eckwerten ein
    Verdict baut.
    """
    return {
        "title": "Methodik: NOAA NCEI Climate Data Online",
        "indicator_name": "WICHTIGER KONTEXT: NOAA NCEI ist US-zentriert mit globalen Datensätzen",
        "indicator": "noaa_methodology",
        "year": "",
        "value": "",
        "description": (
            "NOAA National Centers for Environmental Information (NCEI) ist "
            "die US-Bundesinstitution für historische Klimadaten. "
            "Einschränkungen: "
            "(1) Geographischer Fokus — Storm Events Database und Tornado-"
            "Statistik sind rein US-spezifisch. Für AT-/EU-Klima nutze "
            "GeoSphere Austria, EU Copernicus C3S, ECMWF ERA5. "
            "(2) Globale Temperaturreihe (NOAAGlobalTemp) ist eines von "
            "5 Referenzdatensätzen (NASA GISS, Berkeley Earth, HadCRUT, "
            "JMA, ERA5) — die Werte stimmen innerhalb von 0.05–0.1°C "
            "überein, kleine Differenzen sind methodisch bedingt "
            "(Baseline, Lückenschluss). "
            "(3) Hurrikan-Saison-Zählung — Saffir-Simpson misst nur "
            "Windgeschwindigkeit, nicht Sturmflut oder Niederschlag. "
            "Eine 'Kat-2'-Hurrikan kann mehr Schaden anrichten als ein "
            "'Kat-4', wenn die Topografie ungünstig ist (z.B. Florence 2018). "
            "(4) Tornado-Zählung 1950 vs. heute — die Erfassung wurde mit "
            "Doppler-Radar (1990er+) systematischer; ältere Reihen "
            "unterschätzen schwache (EF0/EF1) Tornados. Vergleiche über "
            "Jahrzehnte daher mit Vorsicht. "
            "(5) Attributions-Aussagen — NOAA macht zu Einzelereignissen "
            "(Hurricane X, Hitzewelle Y) KEINE direkte Klimawandel-"
            "Attribution. Solche Aussagen kommen von World Weather "
            "Attribution oder IPCC."
        ),
        "source": "NOAA NCEI Documentation",
        "url": "https://www.ncei.noaa.gov/about",
    }


def _select_relevant_highlights(claim_lc: str) -> list[dict]:
    """Pick highlights, deren Jahr/Thema zum Claim passt.

    Wenn der Claim z.B. '2020' oder 'Hurricane Helene' enthält, kommt der
    passende Eintrag zuerst. Sonst Default-Reihenfolge (jüngste zuerst).
    """
    if not claim_lc:
        return list(_NOAA_HIGHLIGHTS)

    scored: list[tuple[int, int, dict]] = []
    for idx, h in enumerate(_NOAA_HIGHLIGHTS):
        score = 0
        year = h.get("year", "")
        # Jahres-Match (4-stellig oder Range "1850")
        for y in re.findall(r"\d{4}", year):
            if y in claim_lc:
                score += 3
        # Indikator-Stichworte
        name_lc = h.get("indicator_name", "").lower()
        for token in (
            "hurricane", "hurrikan", "tornado", "atlantic", "atlantik",
            "death valley", "ghcn", "storm events", "billion-dollar",
            "global", "globale", "temperature", "temperatur",
        ):
            if token in name_lc and token in claim_lc:
                score += 2
        scored.append((score, -idx, h))

    scored.sort(reverse=True)
    return [h for _, _, h in scored]


# ---------------------------------------------------------------------------
# Optionaler NOAA-CDO-Katalog-Fetch (schnell, ~1–2s)
# ---------------------------------------------------------------------------
async def _fetch_noaa_catalogue() -> dict | None:
    """Hole NOAA CDO Datasets-Liste als Quellen-Referenz.

    Cached 24h. Liefert None bei Fehler oder fehlendem Token
    (Pack-Eckwerte sind dann der einzige Output — Graceful Fail).

    Hinweis: Der CDO-Endpoint erwartet einen Token im ``token``-Header.
    Ohne Token gibt der Server 401/403 zurück. Wir machen den Call
    daher nur, wenn ``NOAA_API_TOKEN`` gesetzt ist.
    """
    if not NOAA_API_TOKEN:
        return None

    now = time.time()
    cache_key = "datasets"
    cached = _catalogue_cache.get(cache_key)
    if cached and now - cached[0] < CACHE_TTL_S:
        return cached[1]

    try:
        headers = {"token": NOAA_API_TOKEN}
        async with polite_client(timeout=TIMEOUT_S, headers=headers) as client:
            # Limit=5, damit der Call sicher unter 2s bleibt
            resp = await client.get(NOAA_DATASETS_URL, params={"limit": 5})
            resp.raise_for_status()
            metadata = resp.json()
        _catalogue_cache[cache_key] = (now, metadata)
        return metadata
    except Exception as e:
        logger.warning(f"NOAA CDO catalogue fetch failed: {e}")
        # Negative cache (kurz), damit wir nicht jeden Request neu probieren
        _catalogue_cache[cache_key] = (now, None)
        return None


def _format_catalogue_row(metadata: dict) -> dict | None:
    """Bau einen Quellen-Referenz-Row aus dem CDO-Datasets-JSON.

    Die CDO-API liefert ``{"metadata": {...}, "results": [{...}, ...]}``.
    Wir picken die ersten paar Datasets, zählen die Gesamtsumme.
    """
    if not isinstance(metadata, dict):
        return None
    results = metadata.get("results") or []
    meta = metadata.get("metadata") or {}
    total = meta.get("resultset", {}).get("count")

    if not results:
        return None

    # Top-3 Dataset-Namen als Kurzliste
    names = []
    for ds in results[:3]:
        name = ds.get("name") or ds.get("id") or ""
        if name:
            names.append(name)
    names_str = ", ".join(names) if names else "GHCN-Daily, GSOM, GSOY, Storm Events"

    return {
        "title": (
            "NOAA CDO v2 Katalog-Referenz: "
            f"{total or '11+'} Datensätze verfügbar "
            f"(u.a. {names_str})"
        ),
        "indicator_name": "NOAA CDO v2 Datensatz-Katalog",
        "indicator": "noaa_cdo_catalogue",
        "year": "live",
        "value": f"{total or 'unbekannt'} Datensätze",
        "description": (
            "Live-Abfrage des NOAA NCEI Climate Data Online v2 Katalogs. "
            "Verfügbare Datensätze umfassen GHCN-Daily (tägliche Stations-"
            "daten), GSOM (Global Summary of the Month), GSOY (Global "
            "Summary of the Year), Storm Events Database, sowie regionale "
            "US-Reihen (NCLIMDIV, NCLIMGRID). API: 5 req/s, 10.000 "
            "Requests/Tag mit kostenlosem Token."
        ),
        "source": "NOAA NCEI Climate Data Online v2",
        "url": "https://www.ncei.noaa.gov/cdo-web/webservices/v2",
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def search_noaa(analysis: dict) -> dict:
    """Liefere NOAA-relevante Eckwerte + (optional) Katalog-Referenz.

    Strategie:
      1. Trigger-Check (sollte vom Caller bereits gemacht sein, aber wir
         double-checken — billig).
      2. Kuratierte NOAA-Highlights nach Claim-Relevanz sortieren
         (Jahres-/Thema-Match priorisiert).
      3. Optional: NOAA CDO Katalog-Referenz anhängen (~1–2 s) —
         nur wenn ``NOAA_API_TOKEN`` gesetzt ist. Graceful Fail
         wenn das Netz hängt.
      4. Methodik-Disclaimer als letzter Row.

    Hinweis: Wir machen KEINE breiten /data-Punkt-Anfragen (große
    Date-Ranges können > unser 20-s-Live-Budget brauchen). Für
    spezifische Station-Daten ist das ein Offline-Job.
    """
    empty = {
        "source": "NOAA",
        "type": "climate_data",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    combined_lc = f"{original} {claim}".strip().lower()

    if not _claim_mentions_noaa(combined_lc):
        return empty

    # 1) Kuratierte Highlights, nach Relevanz sortiert
    highlights = _select_relevant_highlights(combined_lc)
    # Cap: max 4 Highlights, sonst überfrachten wir den Synthesizer
    results: list[dict] = highlights[:4]

    # 2) Katalog-Referenz (best-effort, nur mit Token, Graceful Fail)
    if NOAA_API_TOKEN:
        metadata = await _fetch_noaa_catalogue()
        if metadata:
            try:
                row = _format_catalogue_row(metadata)
                if row:
                    results.append(row)
            except Exception as e:
                logger.warning(f"NOAA catalogue row format failed: {e}")
    else:
        logger.debug(
            "NOAA: NOAA_API_TOKEN nicht gesetzt — nur Pack-Eckwerte + "
            "Methodik-Disclaimer (kein Live-Katalog-Lookup)."
        )

    # 3) Methodik-Disclaimer als Hinweis-Row
    results.append(_noaa_methodology_row())

    logger.info(
        f"NOAA: {len(results)} Einträge geliefert "
        f"(Highlights + {'Katalog + ' if NOAA_API_TOKEN else ''}Disclaimer)"
    )
    return {
        "source": "NOAA",
        "type": "climate_data",
        "results": results,
        "attribution": (
            "NOAA NCEI Climate Data Online v2 — US Public Domain. "
            "Climate data: NOAA National Centers for Environmental "
            "Information (NCEI). Hurricane data: NOAA National Hurricane "
            "Center (NHC). Tornado data: NOAA Storm Prediction Center (SPC)."
        ),
    }
