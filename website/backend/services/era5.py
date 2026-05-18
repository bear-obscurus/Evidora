"""ECMWF ERA5 Reanalysis — Wetter-/Klima-Rekord-Referenzdatensatz.

Quelle: ECMWF ERA5 (5th Generation Atmospheric Reanalysis) via Copernicus
Climate Data Store (CDS). Globale stündliche atmosphärische + ozeanische
Variablen seit 1940, ~31 km Grid. Referenz-Reanalyse für die Verifikation
von Wetter-/Klima-Rekord-Behauptungen ("wärmster Tag in Wien", "Hitzewelle
2003", "Niederschlagsrekord 2024", ...).

Datenzugang:
  - CDS-API (live, asynchron):
      https://cds.climate.copernicus.eu/api/v2/
      Authentifizierung: CDS-API-Token (Env-Var ``CDS_API_KEY``).
      Format: NetCDF.
  - AWS-S3-Mirror (kein Auth):
      https://era5-pds.s3.amazonaws.com/
  - Google Cloud Mirror (kein Auth):
      gs://gcp-public-data-arco-era5/
  - Lizenz: CC-BY 4.0 (Copernicus-Attribution erforderlich).

Architektur-Hinweis — warum kuratierte Pack-Eckwerte zuerst:
  ERA5-Anfragen an die CDS-API laufen über eine Server-Queue und dauern
  typisch 5–30 Minuten bis Stunden, je nach Last und Granularität. Für
  einen Live-Faktencheck-Pipeline-Schritt mit harten Timeouts (~20 s)
  ist das nicht nutzbar. Pattern (cf. ARCHITECTURE.md §3.5 Static-First):

    1. Kuratierte ERA5-Highlights als hardcoded Live-Hooks (wärmstes
       Jahr global, Hitzewelle 2003, Sommer 2018/2022, ...). Diese
       Eckwerte sind Wikipedia/IPCC-konsens und brauchen keine API.
    2. Optional: CDS-Katalog-Metadaten als Quellen-Referenz nachladen
       (geht in 1–2 s, kein Queueing). Liefert Dataset-Link für den
       Synthesizer.
    3. Wenn ``CDS_API_KEY`` fehlt oder Katalog-Call fehlschlägt: nur
       Pack-Eckwerte zurückgeben (Graceful Fail, 0 != []).

Komplementär zu:
  - copernicus.py (gleicher CDS-Adapter, breiterer Klima-Datensatz-Mix:
    Berkeley Earth + NASA GISS + Katalog-Browse). era5.py fokussiert auf
    explizite Rekord-Behauptungen mit Ort/Jahr.
  - nasa_giss/berkeley_earth (über copernicus.py): globale +
    länderspezifische Jahres-Mittel. ERA5 liefert die feine Auflösung
    (Wien, Salzburg, ...) und Sub-Tagesgranularität.

Politische Guardrails: Reine Datenquelle. KEINE
Attribution-Behauptungen ("Klimawandel hat X verursacht"). Eckwerte
sind faktuelle Rekord-Zahlen; die kausale Einordnung übernimmt der
Synthesizer mit dem üblichen IPCC-Konsens-Disclaimer.

WIRING für main.py (NICHT in dieser Datei vornehmen):
  from services.era5 import search_era5, claim_mentions_era5_cached
  if claim_mentions_era5_cached(claim):
      tasks.append(cached("ERA5", search_era5, analysis))
      queried_names.append("ECMWF ERA5")
  # Zusätzlich:
  #   - reranker-Whitelist: "ECMWF ERA5" eintragen
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

# CDS-API-Endpoints
CDS_CATALOGUE_URL = "https://cds.climate.copernicus.eu/api/catalogue/v1/collections"
ERA5_DATASET_ID = "reanalysis-era5-single-levels-monthly-means"
ERA5_DATASET_URL = f"https://cds.climate.copernicus.eu/datasets/{ERA5_DATASET_ID}"

# AWS-S3-Mirror (öffentliche Zone, kein Auth)
ERA5_S3_MIRROR = "https://era5-pds.s3.amazonaws.com/"

CACHE_TTL_S = 24 * 60 * 60  # 24 h
TIMEOUT_S = 8.0  # Katalog-Call; Pack-Eckwerte brauchen kein Netz

# Modul-Cache für Katalog-Metadaten (Wechsel selten, 24h reicht)
_catalogue_cache: dict[str, tuple[float, dict | None]] = {}


# ---------------------------------------------------------------------------
# Trigger — wann soll dieser Service angefragt werden?
# ---------------------------------------------------------------------------
_ERA5_TERMS = (
    # Direkter Name
    "era5", "era-5", "ecmwf reanalysis", "ecmwf-reanalyse",
    "ecmwf reanalyse", "copernicus reanalyse",
    # Reanalyse-Begriff (generisch)
    "reanalyse", "reanalysis", "rückrechnung",
    # Wetter-/Klima-Rekord-Patterns
    "temperatur-rekord", "temperaturrekord", "temperature record",
    "wärmster tag", "wärmster monat", "wärmste woche",
    "warmest day", "warmest month", "warmest year",
    "hitze-rekord", "hitzerekord", "heat record",
    "kälte-rekord", "kälterekord", "cold record",
    "kältester tag", "coldest day",
    "hitzewelle", "heatwave", "heat wave",
    "niederschlags-rekord", "niederschlagsrekord", "precipitation record",
    "regenrekord", "rainfall record",
    "wind-rekord", "windrekord", "wind record",
    "sturmrekord", "storm record",
    "wetter-rekord", "wetterrekord", "weather record",
    "klima-rekord", "klimarekord", "climate record",
)

# Composite-Trigger: Ortsbezogene Hitze-/Wetter-Behauptungen.
# Wir wollen Pattern wie "wärmster Sommer in Wien seit 1850" erfassen,
# auch wenn das Wort "ERA5" nicht im Claim steht.
_RECORD_VERBS = (
    "rekord", "record", "höchster", "höchste", "höchstes",
    "wärmster", "wärmste", "wärmstes", "warmest", "hottest",
    "kältester", "kälteste", "kältestes", "coldest",
    "trockenster", "trockenste", "driest",
    "feuchtester", "feuchteste", "wettest",
    "höher als je", "noch nie so",
)

_WEATHER_NOUNS = (
    "temperatur", "temperature", "hitze", "heat",
    "kälte", "cold",
    "niederschlag", "regen", "precipitation", "rainfall",
    "schnee", "snow", "schneefall", "snowfall",
    "wind", "sturm", "storm",
    "sommer", "summer", "winter", "frühling", "spring", "herbst", "autumn",
    "monat", "month", "jahr", "year", "tag", "day",
    "dürre", "drought", "hochwasser", "flood",
)

# Jahreszahl 1850–2099 (4-stellig)
_YEAR_REGEX = re.compile(r"\b(18[5-9]\d|19\d\d|20\d\d|21\d\d)\b")


def _claim_mentions_era5(claim_lc: str) -> bool:
    """Trigger-Pre-Check für ERA5.

    True wenn:
      1. Direkter ERA5/Reanalyse-Begriff im Claim, ODER
      2. Wetter-/Klima-Rekord-Verb + Wetter-Substantiv (ggf. Jahr) —
         klassische "wärmster Sommer 2003"-Patterns.
    """
    if not claim_lc:
        return False
    # 1) Direkt-Trigger
    if any(t in claim_lc for t in _ERA5_TERMS):
        return True
    # 2) Composite: Rekord-Verb + Wetter-Substantiv
    has_record_verb = any(v in claim_lc for v in _RECORD_VERBS)
    if not has_record_verb:
        return False
    has_weather_noun = any(n in claim_lc for n in _WEATHER_NOUNS)
    if has_weather_noun:
        # Jahreszahl ist optional, aber wenn fehlt UND Claim sehr kurz,
        # eher kein Rekord-Claim → konservativer Cut.
        if _YEAR_REGEX.search(claim_lc) or len(claim_lc) > 40:
            return True
    return False


@lru_cache(maxsize=512)
def claim_mentions_era5_cached(claim: str) -> bool:
    """Public-Wrapper für Trigger-Check (case-normalisiert, LRU-gecached)."""
    return _claim_mentions_era5((claim or "").lower())


# ---------------------------------------------------------------------------
# Kuratierte ERA5-Eckwerte ("Pack-Fallback")
# ---------------------------------------------------------------------------
# Konsens-Highlights, die ohne Live-Query reproduzierbar sind. Quellen:
#   - Copernicus C3S Klimabulletins (jährlich, https://climate.copernicus.eu/)
#   - WMO State of the Global Climate Reports
#   - ECMWF ERA5-Auswertungen (z.B. CarbonBrief-Reproduktionen)
# Zahlenwerte sind gerundet auf 1–2 Nachkommastellen, mit Quellen-URL.
_ERA5_HIGHLIGHTS: list[dict] = [
    {
        "title": (
            "Globale Jahresmitteltemperatur 2024: erstmals >+1.5°C über "
            "vorindustriellem Mittel (ca. +1.60°C, ERA5-Auswertung Copernicus C3S)"
        ),
        "indicator_name": "Globale Mitteltemperatur 2024",
        "indicator": "era5_global_mean_2024",
        "year": "2024",
        "value": "+1.60°C vs. vorindustriell (1850–1900)",
        "description": (
            "2024 war laut ERA5 das wärmste Jahr seit Beginn der globalen "
            "Aufzeichnungen. Es war zudem das erste Kalenderjahr, in dem die "
            "Jahresmitteltemperatur das 1.5°C-Ziel des Pariser Abkommens "
            "(bezogen auf den vorindustriellen Mittelwert 1850–1900) "
            "überschritten hat. Das bedeutet noch keinen 'Bruch' des Pariser "
            "Ziels (das auf einen 20-Jahres-Mittelwert abstellt), wohl aber "
            "ein deutliches Warnsignal."
        ),
        "source": "ECMWF ERA5 / Copernicus C3S Climate Bulletin",
        "url": "https://climate.copernicus.eu/copernicus-2024-first-year-exceed-15degc-above-pre-industrial-level",
    },
    {
        "title": (
            "Europäische Hitzewelle Sommer 2003: ERA5 dokumentiert +2.3°C "
            "Temperatur-Anomalie im Sommer-Mittel (JJA) gegenüber 1991–2020"
        ),
        "indicator_name": "Hitzewelle Europa 2003",
        "indicator": "era5_heatwave_2003",
        "year": "2003",
        "value": "+2.3°C Sommer-Anomalie",
        "description": (
            "Der Sommer 2003 (JJA, Juni–August) gilt als eine der "
            "verheerendsten Hitzewellen Europas mit ~70.000 hitzebedingten "
            "Zusatztodesfällen (Robine et al. 2008). ERA5 dokumentiert für "
            "Mittel- und Westeuropa Sommer-Anomalien von +2 bis +4°C "
            "gegenüber 1991–2020, mit Spitzen von >+5°C in Frankreich."
        ),
        "source": "ECMWF ERA5",
        "url": ERA5_DATASET_URL,
    },
    {
        "title": (
            "Europäischer Sommer 2022: laut ERA5 wärmster Sommer in Europa "
            "seit Messbeginn (ca. +1.4°C über 1991–2020-Mittel)"
        ),
        "indicator_name": "Sommer 2022 Europa",
        "indicator": "era5_summer_2022_europe",
        "year": "2022",
        "value": "+1.4°C JJA-Anomalie Europa",
        "description": (
            "Der Sommer 2022 war laut ERA5 der wärmste Sommer für Europa "
            "seit Beginn der Reanalyse-Aufzeichnungen 1940. Großflächige "
            "Dürre und Hitzewellen prägten Juli und August; Frankreich, "
            "Spanien und Italien meldeten nationale Temperaturrekorde."
        ),
        "source": "ECMWF ERA5 / Copernicus C3S",
        "url": "https://climate.copernicus.eu/copernicus-summer-2022-europes-hottest-record",
    },
    {
        "title": (
            "Hitzewelle Sommer 2018 Mittel- und Nordeuropa: ERA5 zeigt "
            "Anomalien von +2 bis +4°C im JJA-Mittel"
        ),
        "indicator_name": "Hitzewelle Nordeuropa 2018",
        "indicator": "era5_heatwave_2018",
        "year": "2018",
        "value": "+2 bis +4°C JJA-Anomalie",
        "description": (
            "Der Sommer 2018 verzeichnete in Skandinavien, Deutschland und "
            "Österreich extreme Hitze- und Trockenperioden. ERA5 belegt "
            "Anomalien von +2 bis +4°C (JJA-Mittel) gegenüber 1991–2020; "
            "die Vegetationsperiode war über weite Teile Mitteleuropas "
            "die trockenste seit Messbeginn."
        ),
        "source": "ECMWF ERA5",
        "url": ERA5_DATASET_URL,
    },
    {
        "title": (
            "Wärmstes Dekaden-Mittel: 2015–2024 ist laut ERA5 das wärmste "
            "Jahrzehnt seit 1850 (ca. +1.24°C über vorindustriell)"
        ),
        "indicator_name": "Dekaden-Mittel 2015–2024",
        "indicator": "era5_decade_2015_2024",
        "year": "2015–2024",
        "value": "+1.24°C vs. vorindustriell (10-Jahres-Mittel)",
        "description": (
            "Das Jahrzehnt 2015–2024 ist laut ERA5 und WMO-Konsens-Auswertung "
            "das wärmste 10-Jahres-Mittel seit Beginn der instrumentellen "
            "Aufzeichnungen (1850). Jedes einzelne Jahr ab 2015 zählt zu den "
            "zehn wärmsten je gemessenen Jahren."
        ),
        "source": "ECMWF ERA5 / WMO State of the Global Climate 2024",
        "url": "https://climate.copernicus.eu/global-climate-highlights-2024",
    },
]


def _era5_methodology_row() -> dict:
    """Methodik-Disclaimer (V-Dem/Berkeley-Pattern): ein Hinweis-Eintrag,
    der dem Synthesizer die Einschränkungen von ERA5 nennt — bevor er
    aus den Eckwerten ein Verdict baut.
    """
    return {
        "title": "Methodik: ECMWF ERA5 Reanalyse",
        "indicator_name": "WICHTIGER KONTEXT: ERA5 ist eine Reanalyse, kein direktes Messnetz",
        "indicator": "era5_methodology",
        "year": "",
        "value": "",
        "description": (
            "ERA5 kombiniert globale Beobachtungsdaten (Wetterstationen, Bojen, "
            "Schiffe, Satelliten, Radiosonden) mit einem numerischen Wettermodell "
            "zu einem flächendeckenden, gitterbasierten Datensatz (~31 km Grid, "
            "stündliche Auflösung, seit 1940). Einschränkungen: "
            "(1) Auflösung — lokale Extreme (Alpen-Täler, einzelne Wetter-"
            "stationen) können vom ERA5-Gitterwert abweichen. "
            "(2) Datenarme Regionen — vor ca. 1979 (Satelliten-Ära) ist die "
            "Beobachtungsdichte in der Südhemisphäre und über Ozeanen geringer; "
            "ältere Werte haben größere Unsicherheit. "
            "(3) Vorindustrielle Referenz — ERA5 beginnt 1940; Aussagen 'seit "
            "vorindustriell' beziehen sich auf eine Kombination mit historischen "
            "Stationsreihen (HadCRUT/Berkeley). "
            "(4) Rekord-Aussagen — 'wärmstes Jahr seit 1940' ist robust; "
            "'wärmstes Jahr seit 1850' kombiniert ERA5 mit anderen Quellen."
        ),
        "source": "ECMWF ERA5 Documentation",
        "url": "https://confluence.ecmwf.int/display/CKB/ERA5",
    }


def _select_relevant_highlights(claim_lc: str) -> list[dict]:
    """Pick highlights, deren Jahr/Region zum Claim passt.

    Wenn der Claim z.B. '2003' oder 'Hitzewelle 2003' enthält, kommt der
    2003-Eintrag zuerst. Sonst Default-Reihenfolge (jüngste zuerst).
    """
    if not claim_lc:
        return list(_ERA5_HIGHLIGHTS)

    # Score: +3 für Jahres-Match, +1 für Indicator-Stichwort
    scored: list[tuple[int, int, dict]] = []
    for idx, h in enumerate(_ERA5_HIGHLIGHTS):
        score = 0
        year = h.get("year", "")
        # Jahres-Range "2015–2024" enthält Komponenten; einfacher Substring-Check
        for y in re.findall(r"\d{4}", year):
            if y in claim_lc:
                score += 3
        # Indikator-Stichworte
        name_lc = h.get("indicator_name", "").lower()
        for token in ("hitzewelle", "heatwave", "sommer", "summer",
                      "dekade", "decade", "jahresmittel", "global"):
            if token in name_lc and token in claim_lc:
                score += 1
        scored.append((score, -idx, h))  # idx-tiebreak: bevorzuge frühere = neuere Einträge

    scored.sort(reverse=True)
    return [h for _, _, h in scored]


# ---------------------------------------------------------------------------
# Optionaler CDS-Katalog-Fetch (schnell, ~1–2s)
# ---------------------------------------------------------------------------
async def _fetch_era5_catalogue() -> dict | None:
    """Hole ERA5-Dataset-Metadaten vom CDS-Katalog.

    Cached 24h. Liefert None bei Fehler (Pack-Eckwerte sind dann
    der einzige Output — Graceful Fail).

    Hinweis: Der Katalog-Endpoint ist öffentlich und braucht keinen
    CDS-API-Key. Der Key ist nur für die queue-basierte Daten-Anfrage
    nötig — die machen wir hier ohnehin nicht.
    """
    now = time.time()
    cached = _catalogue_cache.get(ERA5_DATASET_ID)
    if cached and now - cached[0] < CACHE_TTL_S:
        return cached[1]

    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(f"{CDS_CATALOGUE_URL}/{ERA5_DATASET_ID}")
            resp.raise_for_status()
            metadata = resp.json()
        _catalogue_cache[ERA5_DATASET_ID] = (now, metadata)
        return metadata
    except Exception as e:
        logger.warning(f"ERA5 catalogue fetch failed: {e}")
        # Negative cache (kurz), damit wir nicht jeden Request neu probieren
        _catalogue_cache[ERA5_DATASET_ID] = (now, None)
        return None


def _format_catalogue_row(metadata: dict) -> dict:
    """Bau einen Quellen-Referenz-Row aus dem Katalog-JSON."""
    title = metadata.get("title", "ERA5 monthly averaged data on single levels from 1940 to present")
    description = metadata.get("description", "") or ""
    if description:
        description = description[:240].rsplit(" ", 1)[0] + "…"

    temporal = metadata.get("extent", {}).get("temporal", {})
    interval = temporal.get("interval", [[]])
    time_range = ""
    if interval and len(interval[0]) >= 2:
        start = (interval[0][0] or "")[:4]
        end = (interval[0][1] or "")[:4] if interval[0][1] else "heute"
        time_range = f"{start}–{end}"

    return {
        "title": f"{title} (Datensatz-Referenz)",
        "indicator_name": "ERA5-Datensatz (CDS-Katalog)",
        "indicator": "era5_dataset_reference",
        "year": time_range,
        "value": "globale stündliche Reanalyse",
        "description": (
            description
            or "ERA5 ist die fünfte Generation der ECMWF-Reanalyse und der "
            "umfassendste globale Klimadatensatz mit stündlicher Auflösung "
            "seit 1940. Frei zugänglich über den Copernicus Climate Data Store."
        ),
        "source": "Copernicus Climate Data Store (ECMWF/EU)",
        "url": ERA5_DATASET_URL,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def search_era5(analysis: dict) -> dict:
    """Liefere ERA5-relevante Eckwerte + (optional) Katalog-Referenz.

    Strategie:
      1. Trigger-Check (sollte vom Caller bereits gemacht sein, aber wir
         double-checken — billig).
      2. Kuratierte ERA5-Highlights nach Claim-Relevanz sortieren
         (Jahres-Match priorisiert).
      3. Optional: CDS-Katalog-Metadaten anhängen (~1–2 s) — wenn das
         Netz hängt, fällt der Block stumm weg (Graceful Fail).
      4. Methodik-Disclaimer als letzter Row anhängen.

    Hinweis: Wir machen KEINE queue-basierten Daten-Anfragen an die
    CDS-API. Diese dauern ≫ unsere 20-s-Live-Budget. Wenn jemand
    "ERA5-Live-Plot für Wien 12.08.2003" haben will, ist das ein
    Offline-Job, kein Live-Faktencheck.
    """
    empty = {
        "source": "ECMWF ERA5",
        "type": "reanalysis_climate",
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

    if not _claim_mentions_era5(combined_lc):
        return empty

    # 1) Kuratierte Highlights, nach Relevanz sortiert
    highlights = _select_relevant_highlights(combined_lc)
    # Cap: nicht mehr als 4 Highlights, sonst überfrachten wir den Synthesizer
    results: list[dict] = highlights[:4]

    # 2) Katalog-Referenz (best-effort, Graceful Fail)
    metadata = await _fetch_era5_catalogue()
    if metadata:
        try:
            results.append(_format_catalogue_row(metadata))
        except Exception as e:
            logger.warning(f"ERA5 catalogue row format failed: {e}")

    # 3) Methodik-Disclaimer als Hinweis-Row
    results.append(_era5_methodology_row())

    # 4) Token-Hinweis nur als Log (nicht im Output), falls keiner gesetzt ist.
    # CDS-Key wird hier nicht gebraucht (nur für queue-Anfragen), aber für
    # zukünftige Erweiterungen ist das Logging nützlich.
    if not os.getenv("CDS_API_KEY"):
        logger.debug(
            "ERA5: CDS_API_KEY nicht gesetzt — Pack-Eckwerte + Katalog-Ref "
            "werden zurückgegeben (queue-Anfragen ohnehin nicht möglich)."
        )

    logger.info(
        f"ERA5: {len(results)} Einträge geliefert "
        f"(Highlights + Katalog + Disclaimer)"
    )
    return {
        "source": "ECMWF ERA5",
        "type": "reanalysis_climate",
        "results": results,
        "attribution": (
            "Contains modified Copernicus Climate Change Service information "
            "(2024). Neither the European Commission nor ECMWF is responsible "
            "for any use of this information. ERA5 data: ECMWF / Copernicus C3S "
            "(CC-BY 4.0)."
        ),
    }
