"""CAMS — Copernicus Atmosphere Monitoring Service.

Globale modellbasierte Luftqualitäts- und Atmosphären-Daten via ADS
(Atmosphere Data Store). CAMS liefert Reanalyse + Forecast für:

- Schadstoffe: PM2.5, PM10, NO2, O3, CO, SO2 (Flächendaten, 0.4° Raster)
- Treibhausgas-Inversionen: CO2, CH4 (globale Säulen-Konzentrationen)
- Aerosole + Staub (Sahara-Staub-Episoden über Europa)
- GFAS Brand-System (globale Vegetationsbrand-Emissionen, täglich)

Komplementär zu existierenden Air-Quality-Quellen:
- openaq.py: PUNKT-Mess-Werte einzelner Stationen (Bodensensoren)
- mobilitaet_pack: kuratierte DACH-Hotspot-Konsens-Aussagen
- copernicus.py: ERA5-Klima + CDS-Katalog (CDS, andere API-Domain!)
- CAMS hier: MODELLBASIERTE Flächendaten + atmosphärische Reanalyse
  (ergänzt OpenAQ-Punkt-Messungen mit räumlich-zeitlich konsistenter
  Modell-Grundlage; nützlich für regionale Vergleiche + Trends)

Hybrid-Implementierung (Pack-mit-Live-Augmentation):
  1. Kuratierte CAMS-Eckwerte (typische EU-Hintergrund-Werte, GFAS-Brand-
     Saison-Trends, Sahara-Staub-Frequenz, NO2-COVID-Effekt) als Anker.
  2. Optionaler Live-Lookup ADS-Katalog wenn `ADS_API_KEY` gesetzt
     (CDS-API kompatibel, gleiche Authentifizierung wie copernicus.py).
  3. Graceful-fail: ohne Key → kuratierte Eckwerte; mit Key → +
     Live-Augmentation, ohne Crash bei Timeout/Quota.

Lizenz: CC-BY 4.0 (Copernicus Atmosphere Monitoring Service).

# ---------------------------------------------------------------------------
# WIRING-SNIPPET (NICHT in dieser Datei applizieren, nur Anleitung!)
# ---------------------------------------------------------------------------
# In main.py:
#   from services.cams import search_cams, claim_mentions_cams_cached
#   ...
#   if claim_mentions_cams_cached(claim):
#       tasks.append(search_cams(analysis))
#
# In services/reranker.py (AUTHORITATIVE_INDICATORS-Set):
#   "Copernicus CAMS",
#
# In services/data_updater.py (optional, kein Prefetch nötig — Pack ist
# inline, Live-Augmentation passiert on-demand pro Request):
#   keine Änderung erforderlich.
# ---------------------------------------------------------------------------
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# Atmosphere Data Store (ADS) — CDS-API kompatibel, separate Domain.
ADS_CATALOGUE_URL = (
    "https://ads.atmosphere.copernicus.eu/api/catalogue/v1/collections"
)

# Cache 24h (kuratierte Eckwerte ändern sich nicht; Live-Katalog seltener)
CAMS_CACHE_TTL = 86400
_catalogue_cache: dict[str, dict] | None = None
_catalogue_cache_ts: float = 0.0


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
# Luftqualitäts-/Atmosphären-Keywords (DE + EN). Bewusst breit gehalten,
# Stadt-Detection wie in openaq.py NICHT nötig — CAMS liefert globale
# Flächendaten, kein Stations-Lookup.
_CAMS_KEYWORDS = (
    # CAMS / Copernicus explizit
    "cams", "copernicus atmosphere", "atmosphere monitoring",
    "atmosphärenüberwachung", "ads atmosphere",
    # Schadstoffe
    "pm2.5", "pm 2.5", "pm25", "pm10", "pm 10",
    "no2", "stickstoffdioxid", "stickoxid",
    "ozon", "o3", "ozone",
    "so2", "schwefeldioxid",
    "kohlenmonoxid", "carbon monoxide",
    # Luftqualität allgemein
    "luftqualität", "luftgüte", "air quality",
    "feinstaub", "particulate matter", "fine particulate",
    "smog", "luftverschmutzung", "air pollution",
    "schadstoff", "pollutant",
    # Brand / Aerosole / Staub
    "gfas", "vegetationsbrand", "wildfire emission",
    "saharastaub", "sahara-staub", "sahara dust",
    "aerosol", "saharan dust",
    # Treibhausgas-Inversionen (CAMS-Spezialität, ergänzt copernicus.py)
    "treibhausgas-inversion", "ghg inversion",
    "methan-inversion", "co2-inversion",
)


_CAMS_RE = re.compile(
    r"(?i)(" + "|".join(re.escape(k) for k in _CAMS_KEYWORDS) + r")"
)


def _claim_mentions_cams(claim_lc: str) -> bool:
    """Prüfe, ob der Claim ein CAMS-relevantes Keyword enthält.

    Erwartet bereits lowercased Input (Konvention wie oenb.py).
    """
    if not claim_lc:
        return False
    return bool(_CAMS_RE.search(claim_lc))


def claim_mentions_cams_cached(claim: str) -> bool:
    """Public-Trigger für main.py — nimmt rohen Claim, lowercased intern."""
    return _claim_mentions_cams((claim or "").lower())


# ---------------------------------------------------------------------------
# Pollutant-Mapping (Keyword → ADS-Dataset + CAMS-Variable + Anker-Fakt)
# ---------------------------------------------------------------------------
# Jeder Eintrag liefert einen kuratierten "Anker-Fakt" mit typischen
# europäischen Hintergrund-Werten (Stand 2024/2025 CAMS-Reanalyse-Mittel).
# Werte sind konservative Größenordnungen, keine Stations-Messungen —
# für Punkt-Werte siehe openaq.py.
POLLUTANT_MAP: dict[str, dict[str, Any]] = {
    "pm2.5": {
        "dataset_id": "cams-global-reanalysis-eac4",
        "variable": "particulate_matter_2.5um",
        "label": "CAMS Globale Reanalyse — PM2.5 (Feinstaub)",
        "anchor": (
            "CAMS EAC4-Reanalyse: PM2.5-Hintergrund-Konzentrationen in "
            "Mittel-/Westeuropa typisch 8–15 µg/m³ Jahresmittel (2020–2024). "
            "Episoden mit Saharastaub können Tageswerte kurzzeitig auf "
            "30–60 µg/m³ heben. WHO-Limit: 5 µg/m³ Jahresmittel "
            "(strenger als EU 25 µg/m³)."
        ),
    },
    "pm25": {  # Alias ohne Punkt
        "dataset_id": "cams-global-reanalysis-eac4",
        "variable": "particulate_matter_2.5um",
        "label": "CAMS Globale Reanalyse — PM2.5 (Feinstaub)",
        "anchor": (
            "CAMS EAC4-Reanalyse: PM2.5-Hintergrund Mittel-/Westeuropa "
            "typisch 8–15 µg/m³ Jahresmittel (2020–2024)."
        ),
    },
    "pm10": {
        "dataset_id": "cams-global-reanalysis-eac4",
        "variable": "particulate_matter_10um",
        "label": "CAMS Globale Reanalyse — PM10",
        "anchor": (
            "CAMS EAC4-Reanalyse: PM10-Hintergrund Mittel-/Westeuropa "
            "typisch 15–25 µg/m³ Jahresmittel. WHO-Limit 15 µg/m³, "
            "EU-Limit 40 µg/m³ Jahresmittel."
        ),
    },
    "no2": {
        "dataset_id": "cams-european-air-quality-forecasts",
        "variable": "nitrogen_dioxide",
        "label": "CAMS Europäischer Luftqualitäts-Forecast — NO2",
        "anchor": (
            "CAMS-Europa-Forecast: NO2 in urbanen Gebieten typisch "
            "20–45 µg/m³ Jahresmittel; CAMS bestätigte den COVID-Lockdown-"
            "Effekt 2020 mit −30 % bis −50 % NO2 über europäischen "
            "Ballungsräumen ggü. 2019. WHO-Limit 10 µg/m³, EU 40 µg/m³."
        ),
    },
    "stickstoffdioxid": {
        "dataset_id": "cams-european-air-quality-forecasts",
        "variable": "nitrogen_dioxide",
        "label": "CAMS Europäischer Luftqualitäts-Forecast — NO2",
        "anchor": (
            "CAMS-Europa: NO2 in Städten 20–45 µg/m³ Jahresmittel; "
            "COVID-Lockdown 2020 → −30 bis −50 % über Ballungsräumen."
        ),
    },
    "ozon": {
        "dataset_id": "cams-european-air-quality-forecasts",
        "variable": "ozone",
        "label": "CAMS Europäischer Luftqualitäts-Forecast — Ozon (O3)",
        "anchor": (
            "CAMS-Europa-Forecast: Bodennahes Ozon (O3) Sommer-Spitzen "
            "in Mitteleuropa 100–180 µg/m³ als 8h-Maximum; CAMS-Trend "
            "2010–2024: leicht steigende Sommer-Peaks trotz fallender "
            "NOx-Emissionen (Klimaeffekt + VOC-Limitierung)."
        ),
    },
    "o3": {
        "dataset_id": "cams-european-air-quality-forecasts",
        "variable": "ozone",
        "label": "CAMS Europäischer Luftqualitäts-Forecast — Ozon (O3)",
        "anchor": (
            "CAMS-Europa: O3 Sommer-Spitzen Mitteleuropa 100–180 µg/m³ "
            "(8h-Max); Trend leicht steigend trotz fallender NOx."
        ),
    },
    "co": {
        "dataset_id": "cams-global-reanalysis-eac4",
        "variable": "carbon_monoxide",
        "label": "CAMS Globale Reanalyse — CO (Kohlenmonoxid)",
        "anchor": (
            "CAMS EAC4-Reanalyse: CO-Hintergrund Mittel-/Westeuropa "
            "typisch 100–200 µg/m³ Jahresmittel. WHO-24h-Limit 4 mg/m³ "
            "(= 4000 µg/m³) — Hintergrund-Konzentrationen liegen weit "
            "darunter; CO ist hauptsächlich verkehrs-/heizungsbedingt."
        ),
    },
    "so2": {
        "dataset_id": "cams-european-air-quality-forecasts",
        "variable": "sulphur_dioxide",
        "label": "CAMS Europäischer Luftqualitäts-Forecast — SO2",
        "anchor": (
            "CAMS-Europa: SO2 Hintergrund Mitteleuropa < 5 µg/m³ "
            "Jahresmittel (Rückgang ~95 % seit 1990 durch EU-Schwefel-"
            "Reduktion). WHO-24h-Limit 40 µg/m³."
        ),
    },
    "saharastaub": {
        "dataset_id": "cams-global-reanalysis-eac4",
        "variable": "dust_aerosol_optical_depth_550nm",
        "label": "CAMS Globale Reanalyse — Dust Aerosol Optical Depth",
        "anchor": (
            "CAMS-Aerosol-Reanalyse: Saharastaub-Episoden über Mittel-"
            "/Westeuropa treten typisch 5–15× pro Jahr auf, häufiger "
            "im Spätfrühling/Frühsommer (März–Juni). Einzelne Episoden "
            "können Tages-PM10 in den Alpen kurzfristig auf 50–100 µg/m³ "
            "heben. CAMS visualisiert AOD-Felder global in 0.4°-Raster."
        ),
    },
    "sahara dust": {
        "dataset_id": "cams-global-reanalysis-eac4",
        "variable": "dust_aerosol_optical_depth_550nm",
        "label": "CAMS Globale Reanalyse — Dust Aerosol Optical Depth",
        "anchor": (
            "CAMS-Aerosol: Saharastaub-Episoden über Europa 5–15× p.a., "
            "Spätfrühling/Frühsommer-Peak."
        ),
    },
    "gfas": {
        "dataset_id": "cams-global-fire-emissions-gfas",
        "variable": "wildfire_flux_of_carbon_dioxide",
        "label": "CAMS GFAS — Global Fire Assimilation System",
        "anchor": (
            "CAMS GFAS (Global Fire Assimilation System): tägliche "
            "satellitenbasierte Brand-Emissionen (CO2, CO, PM, Aerosole) "
            "global in 0.1°-Raster. GFAS dokumentierte u. a. die "
            "Mediterran-Brandsaison 2023 (Griechenland, Italien) mit "
            "Spitzen-Emissionen >5 Mt CO2 pro Tag im August."
        ),
    },
    "vegetationsbrand": {
        "dataset_id": "cams-global-fire-emissions-gfas",
        "variable": "wildfire_flux_of_carbon_dioxide",
        "label": "CAMS GFAS — Global Fire Assimilation System",
        "anchor": (
            "CAMS GFAS: tägliche globale Brand-Emissionen aus Satelliten-"
            "Feuerdetektion (MODIS); dokumentiert Mediterran-Brandsaison "
            "2023 mit Spitzen >5 Mt CO2/Tag im August."
        ),
    },
    "co2-inversion": {
        "dataset_id": "cams-global-greenhouse-gas-inversion",
        "variable": "carbon_dioxide",
        "label": "CAMS Globale Treibhausgas-Inversion — CO2",
        "anchor": (
            "CAMS-GHG-Inversionen: globale CO2-Säulen-Konzentrationen "
            "aus Satelliten- + Bodendaten, 2024 globale mittlere "
            "atmosphärische CO2-Konzentration ~424 ppm (NOAA/Mauna Loa). "
            "Inversions-Methode trennt natürliche Senken (Wald/Ozean) "
            "von anthropogenen Emissionen."
        ),
    },
    "methan-inversion": {
        "dataset_id": "cams-global-greenhouse-gas-inversion",
        "variable": "methane",
        "label": "CAMS Globale Treibhausgas-Inversion — CH4",
        "anchor": (
            "CAMS-GHG-Inversionen: globale CH4-Konzentrationen 2024 bei "
            "~1932 ppb (NOAA), Anstieg seit 2007 deutlich beschleunigt. "
            "CAMS-Inversion identifiziert tropische Feuchtgebiete + "
            "fossile Quellen als Haupttreiber."
        ),
    },
}


# ---------------------------------------------------------------------------
# Live-Augmentation (optional, nur wenn ADS_API_KEY gesetzt)
# ---------------------------------------------------------------------------
async def _fetch_ads_metadata(client, dataset_id: str) -> dict | None:
    """Hole ADS-Katalog-Metadaten für ein Dataset (cached 24h).

    Returns None bei Fehler / Timeout. Niemals Exception nach außen.
    """
    global _catalogue_cache, _catalogue_cache_ts

    now = time.time()
    if _catalogue_cache is None or (now - _catalogue_cache_ts) > CAMS_CACHE_TTL:
        _catalogue_cache = {}
        _catalogue_cache_ts = now

    if dataset_id in _catalogue_cache:
        return _catalogue_cache[dataset_id]

    try:
        resp = await client.get(f"{ADS_CATALOGUE_URL}/{dataset_id}")
        if resp.status_code != 200:
            logger.debug(
                f"CAMS ADS-Katalog für {dataset_id} → {resp.status_code}"
            )
            return None
        meta = resp.json()
        _catalogue_cache[dataset_id] = meta
        return meta
    except Exception as e:
        logger.debug(f"CAMS ADS-Katalog-Fetch failed für {dataset_id}: {e}")
        return None


def _find_pollutants(claim_lc: str) -> list[dict[str, Any]]:
    """Identifiziere bis zu 3 betroffene Pollutant-/Topic-Datasets.

    Dedup über dataset_id+variable; Order = Keyword-Reihenfolge im Map.
    """
    matched: dict[str, dict[str, Any]] = {}
    for keyword, entry in POLLUTANT_MAP.items():
        if keyword in claim_lc:
            key = f"{entry['dataset_id']}::{entry['variable']}"
            if key not in matched:
                matched[key] = entry
                if len(matched) >= 3:
                    break
    return list(matched.values())


def _build_anchor_result(entry: dict[str, Any]) -> dict[str, Any]:
    """Baue ein kuratiertes CAMS-Anker-Result (immer verfügbar, kein API-Call)."""
    return {
        "title": entry["label"],
        "indicator_name": entry["label"],
        "indicator": "cams_anchor",
        "dataset_id": entry["dataset_id"],
        "variable": entry["variable"],
        "display_value": entry["anchor"],
        "description": entry["anchor"],
        "source": "Copernicus CAMS",
        "url": (
            f"https://ads.atmosphere.copernicus.eu/datasets/"
            f"{entry['dataset_id']}"
        ),
        "attribution": (
            "Contains modified Copernicus Atmosphere Monitoring Service "
            "information (2024–2025). CC-BY 4.0."
        ),
    }


def _build_live_result(
    entry: dict[str, Any], metadata: dict[str, Any],
) -> dict[str, Any]:
    """Augmentiere Anker mit Live-ADS-Katalog-Metadaten (Title + Time-Range)."""
    title = metadata.get("title") or entry["label"]
    description = metadata.get("description") or ""
    if description:
        description = description[:300].rsplit(" ", 1)[0] + "…"

    temporal = (metadata.get("extent") or {}).get("temporal") or {}
    interval = temporal.get("interval") or [[]]
    time_range = ""
    if interval and len(interval[0]) >= 2:
        start = (interval[0][0] or "")[:4]
        end_raw = interval[0][1] or ""
        end = end_raw[:4] if end_raw else "heute"
        time_range = f"{start}–{end}"

    return {
        "title": title,
        "indicator_name": title,
        "indicator": "cams_live",
        "dataset_id": entry["dataset_id"],
        "variable": entry["variable"],
        "display_value": entry["anchor"],
        "description": (
            entry["anchor"]
            + (f" Datenabdeckung: {time_range}." if time_range else "")
            + (f" {description}" if description else "")
        ),
        "time_range": time_range,
        "source": "Copernicus CAMS",
        "url": (
            f"https://ads.atmosphere.copernicus.eu/datasets/"
            f"{entry['dataset_id']}"
        ),
        "attribution": (
            "Contains modified Copernicus Atmosphere Monitoring Service "
            "information (2024–2025). CC-BY 4.0."
        ),
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_cams(analysis: dict) -> dict:
    """Suche CAMS-Atmosphären-/Luftqualitäts-Daten für einen Claim.

    Hybrid-Pattern:
    - Trigger via _claim_mentions_cams (Keyword-Match)
    - Bei Hit: kuratierte CAMS-Anker-Fakten (immer verfügbar)
    - Optional: Live-ADS-Katalog-Augmentation wenn `ADS_API_KEY` gesetzt
    - Graceful-fail: ohne Key oder bei API-Fehler → nur kuratierte Eckwerte

    Return-Schema (kompatibel zu copernicus.py / openaq.py):
        {"source": "Copernicus CAMS", "type": "air_quality_model",
         "results": [...]}
    """
    empty = {
        "source": "Copernicus CAMS",
        "type": "air_quality_model",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_cams(matchable):
        return empty

    matched_entries = _find_pollutants(matchable)
    if not matched_entries:
        # Trigger sprach an (z. B. "luftqualität"), aber kein konkretes
        # Schadstoff-Mapping → liefere allgemeinen EAC4-Anker
        matched_entries = [POLLUTANT_MAP["pm2.5"]]

    api_key = (os.getenv("ADS_API_KEY") or "").strip()

    results: list[dict[str, Any]] = []

    if not api_key:
        # Reine Pack-Antwort (keine Live-Augmentation)
        logger.debug(
            "CAMS: ADS_API_KEY nicht gesetzt — liefere kuratierte "
            "Anker-Fakten ohne ADS-Katalog-Augmentation"
        )
        for entry in matched_entries:
            results.append(_build_anchor_result(entry))
    else:
        # Live-Augmentation versuchen — bei Fehler graceful auf Anker zurück
        try:
            async with polite_client(timeout=15.0) as client:
                for entry in matched_entries:
                    metadata = await _fetch_ads_metadata(
                        client, entry["dataset_id"],
                    )
                    if metadata:
                        results.append(_build_live_result(entry, metadata))
                    else:
                        results.append(_build_anchor_result(entry))
        except Exception as e:
            logger.warning(f"CAMS Live-Augmentation failed: {e}")
            results = [_build_anchor_result(e_) for e_ in matched_entries]

    # Methodik-Hinweis als letzter Eintrag (V-Dem/Berkeley-Pattern)
    results.append({
        "title": "Methodik: Copernicus CAMS — Modell vs. Punkt-Messung",
        "indicator_name": (
            "WICHTIGER KONTEXT: CAMS liefert modellbasierte Flächendaten, "
            "keine Punkt-Stations-Messungen"
        ),
        "indicator": "cams_methodology",
        "display_value": "",
        "description": (
            "CAMS-Daten basieren auf Modell-Assimilation (ECMWF IFS-COMPO) "
            "von Satelliten + Stations-Daten — räumlich-zeitlich "
            "konsistente Flächenfelder im 0.4°-/0.1°-Raster (~40 km / 10 km). "
            "Sinnvoll für regionale Vergleiche, Trends und Episoden-Tracking "
            "(z. B. Saharastaub, GFAS-Brände, COVID-NOx-Effekt). "
            "Für punktgenaue Stations-Messwerte bitte OpenAQ-/UBA-Daten "
            "konsultieren — CAMS-Pixel können von Stations-Punktwerten "
            "abweichen, besonders bei lokalen Quellen (z. B. Verkehrsachsen)."
        ),
        "source": "Copernicus CAMS",
        "url": "https://atmosphere.copernicus.eu/",
        "attribution": (
            "Contains modified Copernicus Atmosphere Monitoring Service "
            "information (2024–2025). CC-BY 4.0."
        ),
    })

    return {
        "source": "Copernicus CAMS",
        "type": "air_quality_model",
        "results": results,
    }
