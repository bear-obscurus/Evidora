"""GDELT v2 Global Knowledge Graph (GKG) — Live-BigQuery-Connector.

GDELT 2.0 GKG ist eine globale News-Knowledge-Database mit 15-min-Update-
Intervall. Sie aggregiert Persons + Organizations + Locations + Themes +
Tone aus über 100.000 News-Quellen weltweit. Komplementär zu:
- Faktencheck-RSS (Snopes/Correctiv/...): redaktionelle Bewertungen
- News-RSS (CDC Newsroom/EIGE): einzelne Quellen
- Static-First-Packs: kuratierte Konsens-Daten
GDELT bietet GLOBALE Themen-/Entity-Aggregation aus 100k+ Quellen.

WARUM BIGQUERY statt Free-DOC-API:
GDELT Public DOC 2.0 API hat 5s-Rate-Limit pro IP. Bei mehreren Claims
parallel kollabiert das schnell. BigQuery hat KEIN Rate-Limit, sondern
1 TB Query-Volumen kostenlos pro Monat (free tier permanent).

KOSTEN-STRATEGIE:
- 7-Tage-Partition-Filter (~10 GB/Query statt 30+ GB für 30-Tage)
- BigQuery query_cache aktiviert (Standard) — identische Queries kostenlos
- Hauptcache läuft bereits in main.py (cached() helper) auf claim-Ebene
- Bei ~50 unique Claims/Tag → ~10 GB × 50 = 500 GB/Monat — gut innerhalb 1 TB-Limit

AUTH:
- env-var GOOGLE_APPLICATION_CREDENTIALS verweist auf Service-Account-JSON
  (typisch /app/secrets/gdelt-key.json im Docker-Container)
- env-var GDELT_BIGQUERY_PROJECT setzt das GCP-Projekt für Billing
- Bei fehlenden Credentials: graceful skip (return empty results)

TRIGGER: claim hat ≥1 Entity (Persons/Organizations/Locations).

Wiring: main.py imports + tasks.append, reranker (NICHT in
AUTHORITATIVE_INDICATORS — ist Live-Quelle, keine kuratierte Konsens-DB).
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta

logger = logging.getLogger("evidora")

# Lazy-Init: bigquery.Client erst beim ersten Aufruf instanziieren.
# Verhindert ImportError + Auth-Errors beim Modul-Import.
_bq_client = None
_init_attempted = False


def _get_client():
    """Initialisiere BigQuery-Client einmalig.

    Returns Client oder None wenn Credentials fehlen / Library fehlt.
    Logged Status nur beim ersten Aufruf, danach silent.
    """
    global _bq_client, _init_attempted
    if _bq_client is not None:
        return _bq_client
    if _init_attempted:
        return None
    _init_attempted = True

    project_id = os.getenv("GDELT_BIGQUERY_PROJECT", "").strip()
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    if not project_id:
        logger.info("GDELT: GDELT_BIGQUERY_PROJECT env-var not set — Connector inaktiv")
        return None
    if not creds_path:
        logger.info("GDELT: GOOGLE_APPLICATION_CREDENTIALS env-var not set — Connector inaktiv")
        return None
    if not os.path.isfile(creds_path):
        logger.warning(f"GDELT: Credentials-Datei nicht gefunden: {creds_path}")
        return None

    try:
        from google.cloud import bigquery
        _bq_client = bigquery.Client(project=project_id)
        logger.info(f"GDELT: BigQuery-Client initialisiert (Projekt={project_id})")
        return _bq_client
    except ImportError:
        logger.warning("GDELT: google-cloud-bigquery library nicht installiert")
        return None
    except Exception as e:
        logger.warning(f"GDELT: BigQuery-Client-Init fehlgeschlagen: {e}")
        return None


# DE → EN Entity-Mapping für GDELT-GKG (überwiegend englischsprachiger Corpus).
# Wenn Claim "Österreich"/"Russland"/"Wien" enthält, wird Pattern erweitert um
# "Austria"/"Russia"/"Vienna" — sonst kein Treffer in EN-News-Artikeln.
# Stress-Test PDF #46 zeigte AT-Lokalisierungs-Lücke ("ÖBB"-Claim → 0 Treffer
# weil "Österreich" nicht zu "Austria" gemappt wurde).
DE_EN_ENTITY_MAP = {
    # Länder
    "österreich": "Austria",
    "deutschland": "Germany",
    "schweiz": "Switzerland",
    "russland": "Russia",
    "türkei": "Turkey",
    "frankreich": "France",
    "italien": "Italy",
    "spanien": "Spain",
    "griechenland": "Greece",
    "polen": "Poland",
    "tschechien": "Czech Republic",
    "ungarn": "Hungary",
    "rumänien": "Romania",
    "bulgarien": "Bulgaria",
    "kroatien": "Croatia",
    "schweden": "Sweden",
    "norwegen": "Norway",
    "dänemark": "Denmark",
    "finnland": "Finland",
    "niederlande": "Netherlands",
    "belgien": "Belgium",
    "großbritannien": "United Kingdom",
    "grossbritannien": "United Kingdom",
    "vereinigtes königreich": "United Kingdom",
    "vereinigte staaten": "United States",
    "indonesien": "Indonesia",
    "japan": "Japan",
    "südkorea": "South Korea",
    "nordkorea": "North Korea",
    "südafrika": "South Africa",
    "ägypten": "Egypt",
    "saudi-arabien": "Saudi Arabia",
    "iran": "Iran",
    "irak": "Iraq",
    "syrien": "Syria",
    "afghanistan": "Afghanistan",
    "pakistan": "Pakistan",
    "indien": "India",
    "china": "China",
    "ukraine": "Ukraine",
    # Großstädte AT/DE/CH
    "wien": "Vienna",
    "salzburg": "Salzburg",
    "graz": "Graz",
    "innsbruck": "Innsbruck",
    "linz": "Linz",
    "münchen": "Munich",
    "köln": "Cologne",
    "berlin": "Berlin",
    "hamburg": "Hamburg",
    "frankfurt": "Frankfurt",
    "düsseldorf": "Dusseldorf",
    "zürich": "Zurich",
    "genf": "Geneva",
    "basel": "Basel",
    "bern": "Bern",
    # Internationale Großstädte
    "moskau": "Moscow",
    "peking": "Beijing",
    "tokio": "Tokyo",
    "neu-delhi": "New Delhi",
    "kairo": "Cairo",
    "rom": "Rome",
    "athen": "Athens",
    "warschau": "Warsaw",
    "prag": "Prague",
    "budapest": "Budapest",
    "lissabon": "Lisbon",
    # Politische Funktionen / Institutionen
    "bundeskanzler": "Chancellor",
    "bundespräsident": "Federal President",
    "bundespraesident": "Federal President",
    "bundestag": "Bundestag",
    "nationalrat": "Nationalrat National Council",
    "europäische union": "European Union",
    "europaeische union": "European Union",
    "europäische kommission": "European Commission",
    "europäisches parlament": "European Parliament",
    "vereinte nationen": "United Nations",
    # Häufige Wirtschafts-/Tech-Begriffe
    "weltbank": "World Bank",
    "weltgesundheitsorganisation": "World Health Organization WHO",
    "internationaler währungsfonds": "International Monetary Fund IMF",
}


def _sanitize_entities(entities: list[str]) -> list[str]:
    """Filter Entities auf BigQuery-regex-sichere Strings + DE→EN-Expansion.

    - Entferne zu kurze (<3 Zeichen)
    - Entferne Sonderzeichen außer Buchstaben + Ziffern + Leerzeichen
    - DE→EN-Expansion: bei bekannten DE-Entitäten wird das EN-Pendant als
      zusätzlicher Pattern-Term hinzugefügt (z.B. "Österreich" → auch "Austria")
    - Limit auf 8 Entities (Regex-Komplexität, mit EN-Expansion mehr Patterns nötig)
    """
    safe: list[str] = []
    seen_lc: set[str] = set()  # avoid duplicates after expansion

    for e in entities[:10]:  # Pre-filter top 10
        if not e or len(e) < 3:
            continue
        # Erlaube Buchstaben + Ziffern + Leerzeichen + Bindestrich
        cleaned = re.sub(r"[^\w\s\-]", "", e, flags=re.UNICODE).strip()
        if len(cleaned) < 3:
            continue
        cleaned_lc = cleaned.lower()
        if cleaned_lc in seen_lc:
            continue
        seen_lc.add(cleaned_lc)
        safe.append(re.escape(cleaned))

        # DE→EN-Expansion: wenn die DE-Form bekannt ist, EN-Form als
        # zusätzliche Pattern-Variante hinzufügen (multi-word OK).
        en_equivalent = DE_EN_ENTITY_MAP.get(cleaned_lc)
        if en_equivalent and en_equivalent.lower() not in seen_lc:
            seen_lc.add(en_equivalent.lower())
            safe.append(re.escape(en_equivalent))

        if len(safe) >= 8:  # Limit etwas erhöht wegen EN-Expansion
            break
    return safe


def _build_query(safe_entities: list[str], days_back: int = 7,
                 max_results: int = 5) -> str:
    """Baue parametrisierte BigQuery-Statement.

    Verwendet 7-Tage-Partition-Filter zur Kostenkontrolle. Sucht in
    AllNames, V2Persons, V2Organizations, V2Locations + V2Themes nach
    Entity-Match (case-insensitive).

    GKG-Schema-Doku: http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf
    """
    pattern = "|".join(safe_entities)
    cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    query = f"""
    SELECT
      DocumentIdentifier as url,
      DATE(_PARTITIONTIME) as date,
      SourceCommonName as source_name,
      ARRAY_TO_STRING(
        ARRAY(
          SELECT theme FROM UNNEST(SPLIT(V2Themes, ';')) theme
          WHERE LENGTH(theme) > 0
          LIMIT 5
        ), ', '
      ) as top_themes,
      V2Tone as tone_full
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE _PARTITIONTIME >= TIMESTAMP("{cutoff}")
      AND DocumentIdentifier IS NOT NULL
      AND DocumentIdentifier != ''
      AND (
        REGEXP_CONTAINS(LOWER(IFNULL(AllNames, '')), r'(?i){pattern}')
        OR REGEXP_CONTAINS(LOWER(IFNULL(V2Persons, '')), r'(?i){pattern}')
        OR REGEXP_CONTAINS(LOWER(IFNULL(V2Organizations, '')), r'(?i){pattern}')
        OR REGEXP_CONTAINS(LOWER(IFNULL(V2Locations, '')), r'(?i){pattern}')
      )
    ORDER BY _PARTITIONTIME DESC
    LIMIT {max_results}
    """
    return query


def _parse_tone(tone_full: str | None) -> str:
    """V2Tone-Format: tone,positive_score,negative_score,polarity,activity_ref_density,self_ref_density,word_count

    Returns kompakte Beschreibung wie "Tone -2.3 (negativ)" oder "n/a".
    """
    if not tone_full:
        return "n/a"
    parts = str(tone_full).split(",")
    if not parts:
        return "n/a"
    try:
        tone = float(parts[0])
        if tone > 2:
            return f"Tone +{tone:.1f} (positiv)"
        elif tone < -2:
            return f"Tone {tone:.1f} (negativ)"
        else:
            return f"Tone {tone:+.1f} (neutral)"
    except (ValueError, TypeError):
        return "n/a"


async def search_gdelt(analysis: dict) -> dict:
    """Live-Query gegen GDELT v2 GKG via BigQuery.

    Returns Dict mit ≤5 News-Article-Treffern aus den letzten 7 Tagen
    weltweit, mit Top-Themen-Tags + V2Tone-Score.
    """
    empty = {
        "source": "GDELT v2 GKG",
        "type": "global_knowledge_graph",
        "results": [],
    }

    client = _get_client()
    if client is None:
        return empty

    entities = (analysis or {}).get("entities", []) or []
    safe_entities = _sanitize_entities(entities)
    if not safe_entities:
        return empty

    query = _build_query(safe_entities)

    try:
        from google.cloud.bigquery import QueryJobConfig
        config = QueryJobConfig(use_query_cache=True, use_legacy_sql=False)

        # BigQuery-Library ist sync — in Thread-Pool ausführen
        loop = asyncio.get_event_loop()
        job = await loop.run_in_executor(
            None,
            lambda: client.query(query, job_config=config),
        )
        rows = await loop.run_in_executor(
            None,
            lambda: list(job.result(timeout=30)),
        )
    except Exception as e:
        logger.warning(f"GDELT BigQuery query failed: {type(e).__name__}: {e}")
        return empty

    if not rows:
        logger.info(
            f"GDELT: 0 Treffer für Entities "
            f"{[e[:30] for e in safe_entities[:3]]}..."
        )
        return empty

    results: list[dict] = []
    for row in rows:
        url = (row.url or "")[:300]
        date_str = row.date.isoformat() if row.date else "—"
        year_str = str(row.date.year) if row.date else "—"
        source_name = (row.source_name or "")[:80]
        top_themes = (row.top_themes or "")[:200]
        tone_str = _parse_tone(row.tone_full)

        display = (
            f"{source_name or 'Quelle unbekannt'} ({date_str}) — "
            f"Top-Themen: {top_themes or 'keine'} | {tone_str}"
        )[:400]

        results.append({
            "indicator_name": (
                f"{source_name} {date_str}".strip()
                if source_name else f"GDELT-Article {date_str}"
            )[:200],
            "indicator": "gdelt_gkg_article",
            "country": "—",
            "year": year_str,
            "topic": "gdelt_global_knowledge_graph",
            "display_value": display,
            "description": f"GDELT v2 GKG Artikel-Eintrag aus {date_str}",
            "url": url,
            "secondary_url": "",
            "source": (
                "GDELT v2 Global Knowledge Graph "
                "(BigQuery, 100k+ News-Quellen weltweit, 15-min-Aktualisierung)"
            ),
        })

    logger.info(f"GDELT: {len(results)} Treffer geliefert")
    return {
        "source": "GDELT v2 GKG",
        "type": "global_knowledge_graph",
        "results": results,
    }
