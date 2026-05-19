"""IDMC — Internal Displacement Monitoring Centre (Live-API).

Datenquelle: Global Internal Displacement Database (GIDD) + Internal
Displacement Updates (IDU) des IDMC (Geneva), einer NRC-Tochter.
Goldstandard für quantitative Binnenvertreibungs-Daten (200+ Länder,
ab 2008/2009), Custodian-Status für SDG-Indikator 10.7.4 (forcibly
displaced population).

Komplementaer zum Konflikt-/Krisen-Cluster:
- UNHCR-Service: REFUGEES (über Landesgrenzen geflohen)
- IDMC-Service:  INTERNALLY DISPLACED PERSONS / IDPs
                 (innerhalb des Heimatlandes vertrieben)
- UCDP-Service:  Konflikt-Events (Battle-Deaths, nicht Bevölkerungs-
                 Bewegung)
- ReliefWeb:     Humanitäre Lage-Berichte (Sekundär-Berichterstattung)

Endpoints (helix-tools-api, Stand Mai 2026):
- GIDD displacements/?iso3__in=<ISO3>      Combined conflict+disaster
                                            Jahres-Aggregate
- GIDD conflicts/?iso3__in=<ISO3>           Nur Konflikt-Vertreibung
- GIDD disasters/?iso3__in=<ISO3>           Disaster-Events (Klima/Erdbeben)
- idus/last-180-days/                       Tagesaktuelle IDU-Events

API-Key: IDMC publiziert einen öffentlichen client_id
(``IDMCWSHSOLO009``) für read-only-Zugriff auf GIDD/IDU. Ein eigener
client_id kann via ``IDMC_CLIENT_ID``-ENV-Var gesetzt werden; ohne
ENV-Override fällt der Service auf den öffentlichen Default zurück
(IDMC erlaubt das ausdrücklich für Open-Data-Embedding).

Lizenz: CC BY 4.0 — Evidora-tauglich (Attribution Pflicht).
Zitation: IDMC (2026) Global Internal Displacement Database. Geneva.
HDX-Mirror: https://data.humdata.org/organization/idmc verfügbar als
Fallback, hier aber nicht aktiv (Live-API liefert dieselben Zahlen
schneller).

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren IDMC-Zahlen + Definitionen, KEINE eigene Bewertung
  („Lage in X ist katastrophal" → nein; „X new IDPs in Jahr Y" → ja).
- IDMC unterscheidet methodisch zwischen ``new displacements``
  (Flow im Jahr) und ``total displacement`` (Stock am Jahresende).
  Beide werden ausgewiesen, niemals vermischt.
- Konflikt vs. Disaster wird separat gezeigt (IDMC-Methodik);
  Mehrfachvertreibungen führen dazu, dass ``new`` höher sein kann
  als die Bevölkerung des Landes (z. B. Ukraine 2022: 16,87 Mio.
  new displacements bei ~40 Mio. Einwohnern, weil viele Personen
  mehrfach gezählt werden). Caveat steht im description-Block.
- IDU sind PRELIMINARY (vor Konsolidierung); GIDD ist VALIDATED
  (jährliche Validation). Kennzeichnung erfolgt im Result.
"""

# WIRING für main.py:
# from services.idmc import search_idmc, claim_mentions_idmc_cached
# if claim_mentions_idmc_cached(claim):
#     tasks.append(cached("IDMC", search_idmc, analysis))
#     queried_names.append("IDMC")
#
# (data_updater.py: KEIN Prefetch nötig — IDMC wird live abgefragt,
#  Modul-internes 24h-Cache pro Land.)

from __future__ import annotations

import logging
import os
import re
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

IDMC_API_BASE = "https://helix-tools-api.idmcdb.org/external-api"
IDMC_DEFAULT_CLIENT_ID = "IDMCWSHSOLO009"  # IDMC-publizierter Public-Key
IDMC_RELEASE_ENV = "RELEASE"

TIMEOUT_S = 20.0
CACHE_TTL_S = 24 * 60 * 60  # 24 h
MAX_RESULTS = 6


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_IDMC_DIRECT_TERMS = (
    # Marken/Quelle
    "idmc", "internal displacement monitoring centre",
    "internal displacement monitoring center",
    "global internal displacement database", "gidd",
    "internal displacement update", "internal displacement updates",
    # Begriff DE
    "binnenvertreibung", "binnenvertriebene", "binnenvertriebener",
    "binnen-vertriebene", "binnen vertriebene",
    "im eigenen land vertrieben", "im eigenen land geflohen",
    "im land geflohen", "intern vertrieben", "intern vertriebene",
    "vertriebene im eigenen land",
    "vertreibung im land", "vertreibung im eigenen land",
    "klima-vertreibung", "klimavertreibung",
    "klima-vertriebene", "klimavertriebene",
    "katastrophen-vertreibung", "katastrophenvertreibung",
    "konflikt-vertreibung", "konfliktvertreibung",
    # Begriff EN
    "internal displacement", "internally displaced",
    "internally displaced person", "internally displaced persons",
    "idp", "idps",
    "climate displacement", "disaster displacement",
    "conflict displacement",
)


def _claim_mentions_idmc(claim_lc: str) -> bool:
    """Trigger-Pre-Check (case-normalisiert).

    Match auf:
    1. Direkter IDMC-/IDP-/Binnenvertreibungs-Term
    2. ``"idp"`` als Token (Wort-Grenze, sonst Kollision mit ``idp_``-
       Komposita etc.)
    """
    if not claim_lc:
        return False
    for t in _IDMC_DIRECT_TERMS:
        if t == "idp" or t == "idps":
            # Wort-Grenze, sonst trifft "idp" zufaellig in Bezeichnern
            if re.search(rf"(?<![a-z0-9]){t}(?![a-z0-9])", claim_lc):
                return True
            continue
        if t in claim_lc:
            return True
    return False


# Trigger-Resolve-Cache: { claim_lc: (ts, bool) }
_TRIGGER_CACHE: dict[str, tuple[float, bool]] = {}


def claim_mentions_idmc_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    if not claim:
        return False
    key = claim.lower().strip()
    if not key:
        return False
    now = time.time()
    cached = _TRIGGER_CACHE.get(key)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_idmc(key)
    if len(_TRIGGER_CACHE) > 512:
        oldest = sorted(_TRIGGER_CACHE.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _TRIGGER_CACHE.pop(k, None)
    _TRIGGER_CACHE[key] = (now, result)
    return result


# ---------------------------------------------------------------------------
# Country-Mapping (Claim-Name → ISO-3-Code + Anzeigename)
# IDMC nutzt ISO-3 als ``iso3__in``-Filter (Komma-Liste möglich).
# ---------------------------------------------------------------------------
_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    # Aktuelle Konflikt-Hotspots
    "ukraine": ("UKR", "Ukraine"),
    "russland": ("RUS", "Russland"), "russia": ("RUS", "Russland"),
    "syrien": ("SYR", "Syrien"), "syria": ("SYR", "Syrien"),
    "sudan": ("SDN", "Sudan"),
    "südsudan": ("SSD", "Südsudan"), "suedsudan": ("SSD", "Südsudan"),
    "south sudan": ("SSD", "Südsudan"),
    "äthiopien": ("ETH", "Äthiopien"), "aethiopien": ("ETH", "Äthiopien"),
    "ethiopia": ("ETH", "Äthiopien"),
    "somalia": ("SOM", "Somalia"),
    "myanmar": ("MMR", "Myanmar"), "burma": ("MMR", "Myanmar"),
    "nigeria": ("NGA", "Nigeria"),
    "jemen": ("YEM", "Jemen"), "yemen": ("YEM", "Jemen"),
    "mali": ("MLI", "Mali"),
    "burkina faso": ("BFA", "Burkina Faso"),
    "niger": ("NER", "Niger"),
    "afghanistan": ("AFG", "Afghanistan"),
    "irak": ("IRQ", "Irak"), "iraq": ("IRQ", "Irak"),
    "libyen": ("LBY", "Libyen"), "libya": ("LBY", "Libyen"),
    "palästina": ("PSE", "Palästina"), "palaestina": ("PSE", "Palästina"),
    "palestine": ("PSE", "Palästina"),
    "gaza": ("PSE", "Palästina"),
    "israel": ("ISR", "Israel"),
    "demokratische republik kongo": ("COD", "DR Kongo"),
    "dr kongo": ("COD", "DR Kongo"),
    "drk": ("COD", "DR Kongo"), "drc": ("COD", "DR Kongo"),
    "kongo-kinshasa": ("COD", "DR Kongo"),
    "zentralafrikanische republik": ("CAF", "Zentralafrika"),
    "kamerun": ("CMR", "Kamerun"), "cameroon": ("CMR", "Kamerun"),
    "mosambik": ("MOZ", "Mosambik"), "mozambique": ("MOZ", "Mosambik"),
    "kolumbien": ("COL", "Kolumbien"), "colombia": ("COL", "Kolumbien"),
    "venezuela": ("VEN", "Venezuela"),
    "haiti": ("HTI", "Haiti"),
    "honduras": ("HND", "Honduras"),
    "mexiko": ("MEX", "Mexiko"), "mexico": ("MEX", "Mexiko"),
    "pakistan": ("PAK", "Pakistan"),
    "indien": ("IND", "Indien"), "india": ("IND", "Indien"),
    "bangladesch": ("BGD", "Bangladesch"), "bangladesh": ("BGD", "Bangladesch"),
    "philippinen": ("PHL", "Philippinen"), "philippines": ("PHL", "Philippinen"),
    "tschad": ("TCD", "Tschad"), "chad": ("TCD", "Tschad"),
    "ägypten": ("EGY", "Ägypten"), "aegypten": ("EGY", "Ägypten"),
    "egypt": ("EGY", "Ägypten"),
    "türkei": ("TUR", "Türkei"), "tuerkei": ("TUR", "Türkei"),
    "turkey": ("TUR", "Türkei"),
    "iran": ("IRN", "Iran"),
    "armenien": ("ARM", "Armenien"), "armenia": ("ARM", "Armenien"),
    "aserbaidschan": ("AZE", "Aserbaidschan"),
    "azerbaijan": ("AZE", "Aserbaidschan"),
    "bergkarabach": ("AZE", "Aserbaidschan"),
    "berg-karabach": ("AZE", "Aserbaidschan"),
    "berg karabach": ("AZE", "Aserbaidschan"),
    # Klima-Vertreibungs-Hotspots
    "indonesien": ("IDN", "Indonesien"), "indonesia": ("IDN", "Indonesien"),
    "china": ("CHN", "China"),
    "japan": ("JPN", "Japan"),
    "vietnam": ("VNM", "Vietnam"),
    "brasilien": ("BRA", "Brasilien"), "brazil": ("BRA", "Brasilien"),
    "tuvalu": ("TUV", "Tuvalu"),
    "kiribati": ("KIR", "Kiribati"),
    "fidschi": ("FJI", "Fidschi"), "fiji": ("FJI", "Fidschi"),
    "tonga": ("TON", "Tonga"),
    # AT/DE/CH zur Vollständigkeit (IDMC-Daten oft 0 für Hocheinkommen)
    "österreich": ("AUT", "Österreich"), "austria": ("AUT", "Österreich"),
    "deutschland": ("DEU", "Deutschland"), "germany": ("DEU", "Deutschland"),
    "schweiz": ("CHE", "Schweiz"), "switzerland": ("CHE", "Schweiz"),
}


def _detect_countries(claim_lc: str, max_n: int = 2) -> list[tuple[str, str]]:
    """Country-Detection mit Wort-Grenzen (greedy, längstes Keyword first).

    Returns Liste (ISO3, Anzeigename).
    """
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    sorted_kws = sorted(_COUNTRY_MAP.keys(), key=len, reverse=True)
    masked = claim_lc
    for kw in sorted_kws:
        iso, name = _COUNTRY_MAP[kw]
        if iso in seen:
            continue
        pattern = r"(?<![a-zäöüß])" + re.escape(kw) + r"(?![a-zäöüß])"
        if re.search(pattern, masked):
            found.append((iso, name))
            seen.add(iso)
            masked = re.sub(pattern, " " * len(kw), masked)
            if len(found) >= max_n:
                break
    return found


# ---------------------------------------------------------------------------
# HTTP-Layer + Cache
# ---------------------------------------------------------------------------
_query_cache: dict[str, tuple[float, list[dict]]] = {}


def _client_id() -> str:
    """Return effective IDMC client_id (ENV override > Public-Default)."""
    return os.getenv("IDMC_CLIENT_ID", "").strip() or IDMC_DEFAULT_CLIENT_ID


async def _get_json(client, url: str, params: dict) -> dict | None:
    """Single GET → JSON. Handles 4xx/5xx mit kurzem Log."""
    try:
        resp = await client.get(url, params=params)
        if resp.status_code >= 400:
            logger.debug(
                f"IDMC HTTP {resp.status_code} für {url}: "
                f"{resp.text[:200]}"
            )
            return None
        return resp.json()
    except Exception as e:
        logger.debug(f"IDMC GET failed ({url}): {e}")
        return None


async def _fetch_displacements(client, iso3: str) -> list[dict]:
    """Hole GIDD displacements (Konflikt + Disaster kombiniert) für ein Land.

    GIDD-Filter ``iso3__in`` akzeptiert eine Komma-Liste, returnt alle
    verfügbaren Jahre auf einmal (klein genug, 2008–aktuell ≈ 15-20
    Records pro Land). 24h-Cache pro Land.
    """
    cache_key = f"gidd-displacements|{iso3}"
    cached = _query_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]

    url = f"{IDMC_API_BASE}/gidd/displacements/"
    params = {
        "client_id": _client_id(),
        "release_environment": IDMC_RELEASE_ENV,
        "iso3__in": iso3,
    }
    data = await _get_json(client, url, params)
    if not isinstance(data, dict):
        _query_cache[cache_key] = (time.time(), [])
        return []
    results = data.get("results") or []
    if not isinstance(results, list):
        results = []
    _query_cache[cache_key] = (time.time(), results)
    return results


# ---------------------------------------------------------------------------
# Aggregation / Result-Builder
# ---------------------------------------------------------------------------
def _safe_int(v) -> int | None:
    """Cast zu int, None bei leerem/ungültigem Wert."""
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _fmt_int_de(v: int | None) -> str:
    """Tausender-Punkt (deutsch) für IDP-Counts."""
    if v is None:
        return "—"
    return f"{v:,}".replace(",", ".")


def _pick_year_records(records: list[dict], iso3: str) -> list[dict]:
    """Filter auf gewünschtes Land + sortiere absteigend nach Jahr."""
    out = []
    for r in records:
        if not isinstance(r, dict):
            continue
        if (r.get("iso3") or "").upper() != iso3.upper():
            continue
        out.append(r)
    out.sort(key=lambda r: r.get("year") or 0, reverse=True)
    return out


def _build_country_result(records: list[dict], country_name: str,
                          iso3: str) -> dict | None:
    """Erzeuge ein Evidora-Result-Dict pro Land.

    Verwendet den jüngsten Jahres-Record als Headline; description
    listet die letzten bis zu 5 Jahre (Konflikt + Disaster getrennt).
    """
    year_records = _pick_year_records(records, iso3)
    if not year_records:
        return None

    latest = year_records[0]
    year = latest.get("year") or "—"
    conflict_new = _safe_int(latest.get("conflict_new_displacement"))
    conflict_total = _safe_int(latest.get("conflict_total_displacement"))
    disaster_new = _safe_int(latest.get("disaster_new_displacement"))
    disaster_total = _safe_int(latest.get("disaster_total_displacement"))

    # Headline-Wert: bevorzugt Konflikt-Total (Stock), sonst Disaster-New
    headline_value: int | None = None
    if conflict_total is not None:
        headline_value = conflict_total
    elif conflict_new is not None:
        headline_value = conflict_new
    elif disaster_new is not None:
        headline_value = disaster_new

    parts: list[str] = []
    if conflict_new is not None:
        parts.append(f"Konflikt-new {_fmt_int_de(conflict_new)}")
    if conflict_total is not None:
        parts.append(f"Konflikt-total {_fmt_int_de(conflict_total)}")
    if disaster_new is not None:
        parts.append(f"Disaster-new {_fmt_int_de(disaster_new)}")
    if disaster_total is not None:
        parts.append(f"Disaster-total {_fmt_int_de(disaster_total)}")

    display = (
        f"IDMC GIDD / {country_name} {year}: " + " | ".join(parts)
        if parts else f"IDMC GIDD / {country_name} {year}: keine Werte"
    )

    # Mehr-Jahres-Trend in description (bis 5 Jahre)
    year_lines = []
    for r in year_records[:5]:
        cn = _safe_int(r.get("conflict_new_displacement"))
        ct = _safe_int(r.get("conflict_total_displacement"))
        dn = _safe_int(r.get("disaster_new_displacement"))
        line_parts = [str(r.get("year") or "?")]
        if cn is not None:
            line_parts.append(f"K-new {_fmt_int_de(cn)}")
        if ct is not None:
            line_parts.append(f"K-total {_fmt_int_de(ct)}")
        if dn is not None:
            line_parts.append(f"D-new {_fmt_int_de(dn)}")
        if len(line_parts) > 1:
            year_lines.append(": ".join([line_parts[0], " | ".join(line_parts[1:])]))

    description = (
        f"IDMC-Methodik: 'new displacement' (Flow) zählt Bewegungen "
        f"im Jahr — eine Person kann mehrfach vertrieben werden und "
        f"taucht dann mehrfach in der Statistik auf. "
        f"'total displacement' (Stock) ist die Schätzung der Anzahl "
        f"intern Vertriebener am Jahresende — eine Person wird nur "
        f"einmal gezählt. Konflikt- und Disaster-Vertreibung werden "
        f"separat geführt; Mehrfachvertreibungen können dazu führen, "
        f"dass 'new' höher ist als die Bevölkerung. "
        f"Trend: " + " ; ".join(year_lines) if year_lines else ""
    )

    return {
        "indicator_name": f"IDMC GIDD: {country_name} {year}",
        "indicator": f"idmc_displacements_{iso3.lower()}",
        "country": iso3,
        "country_name": country_name,
        "year": str(year),
        "value": headline_value if headline_value is not None else 0,
        "display_value": display,
        "description": description,
        "url": f"https://www.internal-displacement.org/countries/{country_name.lower()}",
        "source": "IDMC Global Internal Displacement Database (CC BY 4.0)",
    }


def _context_result() -> dict:
    """Methodik-Kontext (immer angehängt, wenn Resultate vorhanden)."""
    return {
        "indicator_name": "WICHTIGER KONTEXT: IDMC-Methodik",
        "indicator": "idmc_context",
        "country": "",
        "country_name": "",
        "year": "",
        "value": "",
        "display_value": "",
        "url": "https://www.internal-displacement.org/database/",
        "description": (
            "IDMC (Internal Displacement Monitoring Centre, Genf, "
            "Tochter der Norwegian Refugee Council) ist der offizielle "
            "Custodian für SDG-Indikator 10.7.4 (forcibly displaced "
            "population). Definitionen: "
            "(1) IDPs (Internally Displaced Persons) sind Personen, "
            "die gezwungen wurden, ihr Zuhause zu verlassen, aber "
            "NICHT eine international anerkannte Staatsgrenze "
            "überschritten haben (UN Guiding Principles 1998). "
            "Wer eine Grenze überquert, ist ein REFUGEE (UNHCR-"
            "Mandat). "
            "(2) 'New displacements' = Flow im Jahr (Bewegungen, "
            "Person kann mehrfach gezählt werden). "
            "(3) 'Total number of IDPs' = Stock am 31.12. (Person "
            "wird nur einmal gezählt). "
            "(4) Konflikt-Vertreibung + Disaster-Vertreibung werden "
            "getrennt erfasst — addieren ist methodisch unsauber, "
            "da Mehrfachursachen vorkommen. "
            "(5) GIDD-Zahlen sind jährlich validiert; IDU (Internal "
            "Displacement Updates) sind tagesaktuelle vorläufige "
            "Schätzungen vor Konsolidierung."
        ),
        "source": "IDMC",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_idmc(analysis: dict) -> dict:
    """Live-Lookup gegen IDMC GIDD für Binnenvertreibungs-Claims.

    Strategie:
    1. Trigger-Check (IDMC/IDP/Binnenvertreibungs-Term).
    2. Country-Detection (max. 2 Länder, NER + Claim-Text).
    3. Pro Land: GIDD-displacements abrufen, jüngste Jahresreihe
       aufbereiten (Konflikt + Disaster getrennt).
    4. Methodik-Kontext anhängen (Flow vs. Stock, IDP vs. Refugee).
    """
    empty = {
        "source": "IDMC",
        "type": "internal_displacement",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    combined_lc = f"{original} {claim}".lower().strip()

    if not _claim_mentions_idmc(combined_lc):
        return empty

    # Country-Detection (NER-Bonus aus analysis)
    ner_countries = (analysis.get("ner_entities") or {}).get("countries", []) or []
    extra_text = " ".join(c.lower() for c in ner_countries if isinstance(c, str))
    detect_text = (combined_lc + " " + extra_text).strip()
    countries = _detect_countries(detect_text)
    if not countries:
        logger.debug("IDMC: Trigger ja, aber kein Land identifiziert")
        return empty

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S) as client:
        for iso3, name in countries:
            try:
                records = await _fetch_displacements(client, iso3)
            except Exception as e:
                logger.debug(f"IDMC: fetch-error {iso3}: {e}")
                continue
            r = _build_country_result(records, name, iso3)
            if r:
                results.append(r)
                if len(results) >= MAX_RESULTS - 1:  # Platz für Kontext
                    break

    if not results:
        logger.info(
            f"IDMC: 0 Treffer für countries={[c[0] for c in countries]}"
        )
        return empty

    results.append(_context_result())
    logger.info(
        f"IDMC: {len(results) - 1} Country-Treffer, "
        f"countries={[c[0] for c in countries]}"
    )
    return {
        "source": "IDMC",
        "type": "internal_displacement",
        "results": results,
    }
