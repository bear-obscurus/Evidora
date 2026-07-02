"""CORDIS — EU-Forschungsprojekte (Horizon Europe + Horizon 2020) via Bulk-Cache.

Hybrid-Architektur (Lehrgeld 2026-05-24):
- CORDIS Data Extraction REST API ist async-batch-only (Polling 30s+,
  ungeeignet für Live-Faktencheck). → API-Key bleibt in .env für
  zukünftige Custom-Queries / Pack-Authoring-Tools.
- Live-Pipeline nutzt Bulk-ZIP-Downloads von data.europa.eu /
  cordis.europa.eu/data/ (kein Token nötig, CC-BY 4.0).

Datenquellen:
- Horizon Europe (2021-2027): ~21.000 Projekte (~36 MB ZIP)
- Horizon 2020 (2014-2020): ~35.000 Projekte (~62 MB ZIP)
- Gesamt: ~56.000 Projekte → Slim-Cache ~30-50 MB JSON

Slim-Strategie:
Nur kern-relevante Felder extrahiert (id, acronym, title, objective,
keywords, ecMaxContribution, startDate, endDate, frameworkProgramme,
status). Drop: organization.json (108 MB!), webLink, euroSciVoc-Detail.

Use-Case:
Bei Claims über EU-Forschungsförderung, Horizon-Projekte, ERC-Grants,
Marie-Curie-Stipendien → konkrete Projekte mit DOI + Förderhöhe
liefern als Primär-Beleg.

Architektur:
- prefetch_cordis(client): Download beide ZIPs, Slim, Cache schreiben
  → einmalig beim Backend-Start in data_updater.py:prefetch_all()
- _load_slim_cache(): Lazy In-Memory-Load der Slim-JSON
- search_cordis(analysis): Live-Lookup mit Keyword-Match
- Cron-Refresh quartalsweise via tools/refresh_cordis.py

Trigger:
- Direct: „horizon", „cordis", „ERC", „marie curie", „MSCA", „EU-Forschung"
- Composite: „EU" + („Forschung" / „Projekt" / „Förderung" / „gefördert")

Limitationen:
- Slim-Cache enthält Standard-Felder, keine Organisations-Daten
  (Coordinator etc.) — dafür API-Key + Custom-Query nötig
- Refresh-Frequenz: quartalsweise reicht (CORDIS-Updates kommen ~wöchentlich,
  aber Faktencheck-relevante Großprojekt-Records ändern sich selten)
- ~56k Projekte ergeben ~80 MB In-Memory-Footprint
"""

import asyncio
import io
import json
import logging
import os
import zipfile

import httpx

logger = logging.getLogger("evidora")

CORDIS_HORIZON_EUROPE_ZIP = (
    "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip"
)
CORDIS_H2020_ZIP = (
    "https://cordis.europa.eu/data/cordis-h2020projects-json.zip"
)

# Slim-Cache in /app/data/ (im Image; wird beim Container-Rebuild neu
# geladen über prefetch_cordis() in data_updater.prefetch_all() —
# ~30s Cold-Start-Aufpreis akzeptiert).
SLIM_CACHE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "cordis_projects_slim.json",
)

# Felder die wir behalten (~30 % der Original-Größe)
SLIM_FIELDS = (
    "id",
    "acronym",
    "title",
    "objective",
    "keywords",
    "ecMaxContribution",
    "totalCost",
    "startDate",
    "endDate",
    "frameworkProgramme",
    "fundingScheme",
    "status",
    "grantDoi",
    "legalBasis",
)

# Direct-Trigger-Keywords (case-insensitive)
_DIRECT_TRIGGERS = (
    "horizon europe",
    "horizon 2020",
    "horizon-europe",
    "horizon-2020",
    "cordis",
    "erc-grant",
    "erc grant",
    "marie curie",
    "marie-curie",
    "marie skłodowska-curie",
    "msca-projekt",
    "msca grant",
    "msca ",
    " msca",
    "eu-forschungsförderung",
    "europäische forschungsförderung",
    "eu-forschungsprojekt",
    "eu-forschung",
)

# Composite: „EU" + Forschungs-Marker
_EU_WORDS = ("eu-", " eu ", "europäisch", "european union", "europäische union")
_RESEARCH_MARKERS = (
    "forschung",
    "forschungsprojekt",
    "forschungsförderung",
    "förderprojekt",
    "research project",
    "research grant",
)

# DE→EN Mapping für Forschungs-Begriffe (CORDIS-Records sind englisch).
# Wird in _extract_query_keywords ergänzt, um Multi-Lingual-Search zu ermöglichen.
_DE_TO_EN_TERMS = {
    "quantencomputing": ["quantum", "computing"],
    "quantenphysik": ["quantum", "physics"],
    "klimaforschung": ["climate"],
    "klimawandel": ["climate", "change"],
    "künstliche intelligenz": ["artificial", "intelligence"],
    "künstliche": ["artificial"],
    "intelligenz": ["intelligence"],
    "krebsforschung": ["cancer"],
    "krebs": ["cancer", "tumor"],
    "medizinforschung": ["medical", "medicine"],
    "medizinische": ["medical"],
    "impfstoff": ["vaccine", "vaccination"],
    "impfung": ["vaccine", "vaccination"],
    "pandemie": ["pandemic", "covid"],
    "covid": ["covid", "sars-cov"],
    "wasserstoff": ["hydrogen"],
    "solarzellen": ["solar", "photovoltaic"],
    "solarenergie": ["solar"],
    "batterieforschung": ["battery", "batteries"],
    "batterien": ["battery", "batteries"],
    "mikrobiologie": ["microbiology"],
    "genetik": ["genetic", "genomics"],
    "genomforschung": ["genome", "genomics"],
    "bildungsforschung": ["education"],
    "migrationsforschung": ["migration"],
    "energieforschung": ["energy"],
    "energiewende": ["energy", "transition"],
    "mobilitätsforschung": ["mobility", "transport"],
    "cybersicherheit": ["cybersecurity", "cyber"],
    "cybersecurity": ["cybersecurity"],
    "demokratieforschung": ["democracy"],
    "meeresforschung": ["marine", "ocean"],
    "raumfahrt": ["space", "satellite"],
    "halbleiter": ["semiconductor"],
    "erc-grant": ["erc"],
    "marie-curie": ["marie", "curie", "msca"],
    "marie curie": ["marie", "curie", "msca"],
    "msca-projekt": ["msca"],
    "horizon-europe": ["horizon"],
    "horizon europe": ["horizon"],
    "horizon-2020": ["horizon", "h2020"],
    "horizon 2020": ["horizon", "h2020"],
    "nachhaltigkeit": ["sustainability", "sustainable"],
    "nachhaltige": ["sustainable"],
}

TIMEOUT_S = 120.0  # Download kann bis ~30s dauern
MAX_RESULTS = 3


# ---------------------------------------------------------------------------
# Prefetch — Bulk-Download + Slim + Write
# ---------------------------------------------------------------------------

def _slim_record(rec: dict) -> dict:
    """Behalte nur SLIM_FIELDS. Strings auf 1000 Zeichen begrenzen (objective)."""
    out = {}
    for f in SLIM_FIELDS:
        v = rec.get(f)
        if isinstance(v, str) and len(v) > 1500:
            v = v[:1500]
        out[f] = v
    return out


def _records_from_zip(zf: zipfile.ZipFile, framework_label: str) -> list[dict]:
    """Extrahiere Projekt-Records aus einem CORDIS-ZIP — beide Formate:

    - Legacy (bis ~Mitte 2026): eine ``project.json`` mit einem Array.
    - Aktuell (Upstream-Umstellung, entdeckt 2026-07-02): eine Datei pro
      Projekt (``project-rcn-<rcn>_en.json``), Felder flach und
      SLIM_FIELDS-kompatibel; ``frameworkProgramme`` fehlt dort und wird
      aus der ZIP-Herkunft gesetzt.
    """
    names = zf.namelist()
    if "project.json" in names:
        with zf.open("project.json") as f:
            data = json.load(f)
        if not isinstance(data, list):
            logger.warning("CORDIS prefetch: project.json not a list")
            return []
        return [_slim_record(r) for r in data]

    per_project = [n for n in names
                   if n.startswith("project-") and n.endswith(".json")]
    if not per_project:
        logger.warning(
            "CORDIS prefetch: weder project.json noch project-*.json im ZIP"
        )
        return []
    out: list[dict] = []
    for name in per_project:
        try:
            with zf.open(name) as f:
                rec = json.load(f)
        except (json.JSONDecodeError, KeyError):
            continue
        if not isinstance(rec, dict):
            continue
        slim = _slim_record(rec)
        if not slim.get("frameworkProgramme"):
            slim["frameworkProgramme"] = framework_label
        out.append(slim)
    return out


async def _download_and_extract(
    client: httpx.AsyncClient, url: str
) -> list[dict]:
    """Lädt ein CORDIS-ZIP und extrahiert die Projekt-Records."""
    logger.info(f"CORDIS prefetch: downloading {url}")
    r = await client.get(url, timeout=TIMEOUT_S, follow_redirects=True)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    framework = "HORIZON" if "HORIZON" in url else "H2020"
    records = _records_from_zip(zf, framework)
    if not records:
        logger.warning(f"CORDIS prefetch: 0 records aus {url}")
    return records


async def prefetch_cordis(client: httpx.AsyncClient | None = None) -> int:
    """Download Horizon Europe + Horizon 2020 ZIPs, Slim, Cache schreiben.

    Returns: Anzahl der gecachten Records (Slim).
    """
    # Conditional Skip: Cache ist frisch (<90 Tage) → kein Refresh
    import time as _time
    if os.path.exists(SLIM_CACHE_PATH):
        age_days = (_time.time() - os.path.getmtime(SLIM_CACHE_PATH)) / 86400
        if age_days < 90:
            try:
                with open(SLIM_CACHE_PATH, "r", encoding="utf-8") as f:
                    cached_count = len(json.load(f))
                logger.info(
                    f"CORDIS prefetch: cache {age_days:.1f} days old "
                    f"({cached_count} records) — skipping refresh"
                )
                return cached_count
            except Exception:
                pass  # Cache corrupt? Fall-through und neu downloaden

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT_S)
        close_client = True
    try:
        he_records, h2020_records = await asyncio.gather(
            _download_and_extract(client, CORDIS_HORIZON_EUROPE_ZIP),
            _download_and_extract(client, CORDIS_H2020_ZIP),
            return_exceptions=True,
        )
        if isinstance(he_records, Exception):
            logger.warning(f"CORDIS Horizon Europe download failed: {he_records}")
            he_records = []
        if isinstance(h2020_records, Exception):
            logger.warning(f"CORDIS H2020 download failed: {h2020_records}")
            h2020_records = []

        all_records = list(he_records) + list(h2020_records)
        if not all_records:
            logger.warning("CORDIS prefetch: 0 records (both downloads failed?)")
            return 0

        # Schreibe Slim-Cache
        os.makedirs(os.path.dirname(SLIM_CACHE_PATH), exist_ok=True)
        with open(SLIM_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False)

        # Reset In-Memory-Cache damit nächster Load die neue Datei sieht
        global _MEMORY_CACHE
        _MEMORY_CACHE = None

        size_mb = os.path.getsize(SLIM_CACHE_PATH) / 1_000_000
        logger.info(
            f"CORDIS prefetch: {len(all_records)} records cached "
            f"({size_mb:.1f} MB slim)"
        )
        return len(all_records)
    finally:
        if close_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Lazy In-Memory-Load
# ---------------------------------------------------------------------------

_MEMORY_CACHE: list[dict] | None = None


def _load_slim_cache() -> list[dict]:
    """Lädt Slim-Cache lazy einmalig in Memory. Returns [] falls Datei fehlt."""
    global _MEMORY_CACHE
    if _MEMORY_CACHE is not None:
        return _MEMORY_CACHE
    if not os.path.exists(SLIM_CACHE_PATH):
        logger.debug(
            f"CORDIS slim cache missing at {SLIM_CACHE_PATH} — "
            f"prefetch not run yet?"
        )
        _MEMORY_CACHE = []
        return _MEMORY_CACHE
    try:
        with open(SLIM_CACHE_PATH, "r", encoding="utf-8") as f:
            _MEMORY_CACHE = json.load(f)
        logger.info(
            f"CORDIS slim cache loaded: {len(_MEMORY_CACHE)} records"
        )
    except Exception as e:
        logger.warning(f"CORDIS slim cache load failed: {e}")
        _MEMORY_CACHE = []
    return _MEMORY_CACHE


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------

def claim_triggers_cordis(claim: str) -> bool:
    """Trigger-Check für main.py-Wiring."""
    if not claim:
        return False
    cl = claim.lower()
    if any(t in cl for t in _DIRECT_TRIGGERS):
        return True
    # Composite: „EU" + „Forschung"-Marker
    has_eu = any(w in cl for w in _EU_WORDS)
    has_research = any(m in cl for m in _RESEARCH_MARKERS)
    return has_eu and has_research


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def _extract_query_keywords(claim: str) -> list[str]:
    """Extrahiere 3-7 Suchbegriffe ≥4 Zeichen aus dem Claim."""
    cl = claim.lower()
    # Entferne Stopwords + Punktuation grob
    stopwords = {
        "der", "die", "das", "und", "oder", "aber", "auch", "doch", "sich",
        "eine", "einer", "einem", "einen", "ein", "von", "vom", "zur", "zum",
        "bei", "beim", "auf", "über", "unter", "nach", "ohne", "mit", "gegen",
        "sind", "wird", "werden", "wurde", "wurden", "hat", "haben", "war",
        "waren", "ist", "sein", "diese", "dieser", "dieses", "jene", "jener",
        "welche", "welcher", "welches", "alle", "manche", "viele", "wenige",
        "the", "and", "for", "with", "from", "this", "that", "have", "has",
        "are", "was", "were", "been", "will", "would", "could", "should",
        "horizon", "europe", "europa", "europäisch", "europäische", "europäischen",
        "forschung", "forschungsprojekt", "projekt", "förderung", "förderprogramm",
        "research", "project", "grant", "funded", "funding",
    }
    import re
    words = re.findall(r"[a-zäöüß]{4,}", cl)
    seen, out = set(), []
    for w in words:
        if w in stopwords or w in seen:
            continue
        seen.add(w)
        out.append(w)
        if len(out) >= 7:
            break

    # Ergänze EN-Übersetzungen für deutsche Forschungs-Begriffe.
    # CORDIS-Records sind englisch — DE-Match würde sonst 0 Treffer liefern.
    for de_term, en_terms in _DE_TO_EN_TERMS.items():
        if de_term in cl:
            for en in en_terms:
                if en not in seen:
                    seen.add(en)
                    out.append(en)
    return out


def _score_record(rec: dict, keywords: list[str]) -> int:
    """Heuristisches Scoring: Anzahl Keyword-Matches in Title + Acronym + Objective + Keywords-Feld."""
    if not keywords:
        return 0
    haystack_parts = []
    title = rec.get("title")
    if isinstance(title, str):
        haystack_parts.append(title.lower())
    acronym = rec.get("acronym")
    if isinstance(acronym, str):
        haystack_parts.append(acronym.lower())
    keywords_field = rec.get("keywords")
    if isinstance(keywords_field, str):
        haystack_parts.append(keywords_field.lower())
    objective = rec.get("objective")
    if isinstance(objective, str):
        # Truncate für Performance
        haystack_parts.append(objective.lower()[:500])
    hay = " | ".join(haystack_parts)
    score = 0
    for kw in keywords:
        if kw in hay:
            score += 1
            # Bonus wenn im Title oder Acronym
            if (title and kw in title.lower()) or (
                acronym and kw in acronym.lower()
            ):
                score += 2
    return score


def _format_funding(eu_contrib, total_cost) -> str:
    """Formatiere Förderhöhe als '€217.965' oder '€1,2 Mio (EU)' / '€2,3 Mio total'."""
    try:
        eu = float(eu_contrib) if eu_contrib not in (None, "") else 0.0
    except (ValueError, TypeError):
        eu = 0.0
    try:
        tot = float(total_cost) if total_cost not in (None, "") else 0.0
    except (ValueError, TypeError):
        tot = 0.0
    parts = []
    if eu >= 1_000_000:
        parts.append(f"EU-Förderung €{eu / 1_000_000:.2f} Mio")
    elif eu > 0:
        parts.append(f"EU-Förderung €{int(eu):,}".replace(",", "."))
    if tot >= 1_000_000 and tot != eu:
        parts.append(f"Gesamtkosten €{tot / 1_000_000:.2f} Mio")
    return ", ".join(parts) if parts else "Förderhöhe n/a"


def _format_result(rec: dict) -> dict:
    """Formatiere CORDIS-Record zum Evidora-Result-Schema."""
    proj_id = rec.get("id", "")
    acronym = rec.get("acronym") or ""
    title = rec.get("title") or "—"
    objective = rec.get("objective") or ""
    framework = rec.get("frameworkProgramme") or ""
    start_date = (rec.get("startDate") or "")[:10]
    end_date = (rec.get("endDate") or "")[:10]
    funding_str = _format_funding(
        rec.get("ecMaxContribution"), rec.get("totalCost")
    )
    funding_scheme = rec.get("fundingScheme") or ""
    status = rec.get("status") or ""

    name_parts = []
    if acronym:
        name_parts.append(acronym)
    if framework:
        name_parts.append(framework)
    name_prefix = " · ".join(name_parts) if name_parts else "CORDIS"
    indicator_name = (f"{name_prefix}: {title}")[:300]

    period = f"{start_date}–{end_date}" if start_date and end_date else ""
    display_parts = [framework or "EU-Forschungsprojekt"]
    if period:
        display_parts.append(period)
    if funding_str:
        display_parts.append(funding_str)
    if funding_scheme:
        display_parts.append(funding_scheme)
    if status:
        display_parts.append(f"Status: {status}")
    display_value = " | ".join(display_parts)[:500]

    description = (
        (objective[:280] + "…") if len(objective) > 280 else objective
    ) or "Kein Abstract verfügbar"

    url = (
        f"https://cordis.europa.eu/project/id/{proj_id}"
        if proj_id else "https://cordis.europa.eu/projects"
    )

    return {
        "indicator_name": indicator_name,
        "indicator": "cordis_project",
        "country": "EU",
        "year": start_date[:4] if start_date else "—",
        "topic": "eu_research_project",
        "display_value": display_value,
        "description": description,
        "url": url,
        "secondary_url": (
            f"https://doi.org/{rec.get('grantDoi')}"
            if rec.get("grantDoi") else ""
        ),
        "source": "CORDIS Bulk-Cache (data.europa.eu, CC-BY 4.0)",
    }


async def search_cordis(analysis: dict) -> dict:
    """Live-Lookup im CORDIS Bulk-Cache. Returns ≤MAX_RESULTS Treffer."""
    empty = {
        "source": "CORDIS",
        "type": "eu_research_projects",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original") or ""
    if not isinstance(claim, str):
        claim = str(claim or "")

    if not claim_triggers_cordis(claim):
        return empty

    cache = _load_slim_cache()
    if not cache:
        logger.info("CORDIS: cache empty, skipping (prefetch not run?)")
        return empty

    keywords = _extract_query_keywords(claim)
    if not keywords:
        logger.debug("CORDIS: no usable keywords extracted from claim")
        return empty

    # Score all records, return top-K
    scored: list[tuple[int, dict]] = []
    for rec in cache:
        s = _score_record(rec, keywords)
        if s > 0:
            scored.append((s, rec))
    if not scored:
        logger.info(
            f"CORDIS: 0 Treffer für Keywords {keywords[:3]} "
            f"(cache hat {len(cache)} Records)"
        )
        return empty
    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:MAX_RESULTS]
    results = [_format_result(rec) for _, rec in top]
    logger.info(
        f"CORDIS: {len(results)} Treffer geliefert für Keywords "
        f"{keywords[:3]} (Top-Score {top[0][0]})"
    )
    return {
        "source": "CORDIS",
        "type": "eu_research_projects",
        "results": results,
    }
