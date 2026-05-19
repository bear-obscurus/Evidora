"""UCDP — Uppsala Conflict Data Program (Live-API).

Datenquelle: https://ucdp.uu.se/ — akademischer Goldstandard für
geocodierte Konflikt-Event-Daten (1989–heute) und aggregierte Konflikt-
Datensätze (1946–heute). UCDP ist OECD-/SIPRI-anerkannt und liefert die
empirische Basis für die meisten internationalen Konflikt-Statistiken.

Komplementär zum Sicherheits-Cluster:
- SIPRI-Service: Militärausgaben (Input/Strukturindikator)
- UCDP-Service: Konflikt-Events + Battle-Deaths (Output/Eskalations-
  indikator)
- AIID-Service: KI-Schadensfälle (Tech-Cluster, nicht überschneidend)

Endpoints (Version 24.1 — aktuelles Major-Release, Stand Mai 2026):
- gedevents/24.1     → Georeferenced Events Dataset (Einzel-Vorfälle)
- ucdpprioconflict/24.1 → UCDP/PRIO Armed Conflict Dataset (Konflikt-
                          Jahre aggregiert)

Auth (NEU seit Feb 2026): UCDP fordert Token via Header
    x-ucdp-access-token: <token>
Ohne Token liefert die API 401. Wir lesen den Token aus
``os.getenv("UCDP_TOKEN")``; ohne Token degradiert der Service zu einem
No-Op (loggt Warnung beim ersten Aufruf, returnt leere Result-Liste).

Lizenz: CC-BY 4.0 — Evidora-tauglich (Attribution Pflicht).
Zitation: UCDP Georeferenced Event Dataset (GED) Global version 25.1 /
          UCDP/PRIO Armed Conflict Dataset version 25.1.

GUARDRAILS (siehe project_political_guardrails.md):
- Wir zitieren UCDP-Event-Zählungen und Battle-Death-Schätzungen, KEINE
  eigene Ursachenanalyse.
- "Battle-Deaths" sind direkt im Kampf getötete Personen — KEINE
  Gesamt-Kriegstoten (UCDP-OSV/NSV-Datasets erfassen einseitige Gewalt
  separat; wir verweisen darauf, mischen aber nicht).
- Täter/Opfer-Zuordnung (side_a / side_b) wird übernommen, nicht
  interpretiert. UCDP klassifiziert Konflikt-Dyaden faktisch, keine
  Schuldzuweisung.
- Best-/Low-/High-Estimate-Range wird gemeinsam ausgewiesen (UCDP-
  Methodik), nicht auf eine Punktzahl reduziert.
"""

# WIRING für main.py:
# from services.ucdp import search_ucdp, claim_mentions_ucdp_cached
# if claim_mentions_ucdp_cached(claim):
#     tasks.append(cached("UCDP", search_ucdp, analysis))
#     queried_names.append("UCDP Conflict Data")
#
# (data_updater.py: KEIN Prefetch nötig — UCDP wird live abgefragt,
#  Modul-internes 24h-Cache pro Query.)

from __future__ import annotations

import logging
import os
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

UCDP_API_BASE = "https://ucdpapi.pcr.uu.se/api"
UCDP_GED_VERSION = "25.1"  # 2026-05-19: war 24.1, nach Token-Setup auf aktuell upgegradet
UCDP_CONFLICT_VERSION = "25.1"

TIMEOUT_S = 20.0
CACHE_TTL_S = 24 * 60 * 60  # 24 h
MAX_RESULTS = 5
MAX_EVENTS_PER_COUNTRY = 50  # Sample-Größe für Aggregation pro Land
# UCDP-API hat SQL-Injection-Vulnerabilität bei Country= Filter mit Spaces/
# Parens (Strings wie "Russia (Soviet Union)" brechen SQL). Fix: Date-Range-
# Filter (StartDate/EndDate funktionieren zuverlässig) + Client-Side
# Country-Filter über `country`-Field im Event.
UCDP_FETCH_LIMIT = 500  # Events pro Date-Range-Fetch (Client-Side gefiltert)

# Modul-Level Cache: cache-key → (ts, payload)
_query_cache: dict[str, tuple[float, dict]] = {}
_no_token_warned = False  # einmaliges Logging der Token-Lücke


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_UCDP_DIRECT_TERMS = (
    # Marken-/Quelle
    "ucdp", "uppsala conflict", "uppsala conflict data",
    "uppsala konfliktdaten",
    # Konflikt-Termini Deutsch
    "bewaffneter konflikt", "bewaffnete konflikte",
    "bürgerkrieg", "buergerkrieg", "bürgerkriege", "buergerkriege",
    "stellvertreterkrieg", "proxy-krieg",
    "konflikt-tote", "konflikttote", "kriegs-tote", "kriegstote",
    "gefallene", "schlachttote", "battle-deaths", "battle deaths",
    "kampf-tote", "kampftote",
    "kriegsopfer", "konfliktopfer",
    "konflikt-eskalation", "konflikteskalation",
    # Englisch
    "armed conflict", "armed conflicts",
    "civil war", "civil wars", "intra-state conflict",
    "inter-state conflict", "interstate conflict",
    "one-sided violence",
    "conflict deaths", "conflict casualties",
    "battle-related deaths",
)

# Konflikt-Komposite: "Konflikt" + Land/Region
_CONFLICT_GENERIC = (
    "konflikt", "krieg", "konflikts", "kriegs",
    "conflict", "war ", " war",
)
_REGION_HINTS = (
    "in der ukraine", "in ukraine", "in russland", "in syrien", "in libyen",
    "in sudan", "in äthiopien", "in aethiopien", "in myanmar", "in nigeria",
    "in jemen", "in yemen", "in mali", "in burkina faso", "in afghanistan",
    "in irak", "im irak", "in palästina", "in palaestina", "in gaza",
    "im gazastreifen", "in nahost", "im nahen osten", "in der sahelzone",
    "im sahel", "in kongo", "in der drk", "in zentralafrika",
    "in somalia", "in mosambik", "in kamerun",
    "in armenia", "in armenien", "in aserbaidschan", "in azerbaijan",
    "in bergkarabach", "berg-karabach", "berg karabach",
    "in chechnya", "in tschetschenien",
)


def _claim_mentions_ucdp(claim_lc: str) -> bool:
    """Trigger-Pre-Check.

    True wenn:
      1. Direkter UCDP-/Konflikt-Term im Claim
      2. Generisches "Konflikt"/"Krieg" + Regionshinweis
    """
    if not claim_lc:
        return False
    if any(t in claim_lc for t in _UCDP_DIRECT_TERMS):
        return True
    # Composite: Konflikt + Region
    if any(g in claim_lc for g in _CONFLICT_GENERIC):
        if any(r in claim_lc for r in _REGION_HINTS):
            return True
    return False


def claim_mentions_ucdp_cached(claim: str) -> bool:
    """Public-Wrapper (case-normalisiert)."""
    return _claim_mentions_ucdp((claim or "").lower())


# ---------------------------------------------------------------------------
# Country-Mapping (Claim-Name → UCDP-Country-String)
# UCDP nutzt englische Länder-Namen als ``country=`` Query-Parameter.
# ---------------------------------------------------------------------------
COUNTRY_MAP: dict[str, str] = {
    "ukraine": "Ukraine",
    "russland": "Russia (Soviet Union)", "russia": "Russia (Soviet Union)",
    "syrien": "Syria", "syria": "Syria",
    "libyen": "Libya", "libya": "Libya",
    "sudan": "Sudan",
    "südsudan": "South Sudan", "south sudan": "South Sudan",
    "äthiopien": "Ethiopia", "aethiopien": "Ethiopia", "ethiopia": "Ethiopia",
    "somalia": "Somalia",
    "myanmar": "Myanmar (Burma)", "burma": "Myanmar (Burma)",
    "nigeria": "Nigeria",
    "jemen": "Yemen (North Yemen)", "yemen": "Yemen (North Yemen)",
    "mali": "Mali",
    "burkina faso": "Burkina Faso",
    "niger": "Niger",
    "afghanistan": "Afghanistan",
    "irak": "Iraq", "iraq": "Iraq",
    "iran": "Iran",
    "israel": "Israel",
    "palästina": "Palestine", "palaestina": "Palestine",
    "gaza": "Israel",  # UCDP führt Israel-Palästina unter "Israel"
    "kongo": "DR Congo (Zaire)", "drk": "DR Congo (Zaire)",
    "kongo-kinshasa": "DR Congo (Zaire)",
    "zentralafrikanische republik": "Central African Republic",
    "kamerun": "Cameroon", "cameroon": "Cameroon",
    "mosambik": "Mozambique", "mozambique": "Mozambique",
    "kolumbien": "Colombia", "colombia": "Colombia",
    "mexiko": "Mexico", "mexico": "Mexico",
    "venezuela": "Venezuela",
    "pakistan": "Pakistan",
    "indien": "India", "india": "India",
    "philippinen": "Philippines", "philippines": "Philippines",
    "armenien": "Armenia", "armenia": "Armenia",
    "aserbaidschan": "Azerbaijan", "azerbaijan": "Azerbaijan",
    "tschetschenien": "Russia (Soviet Union)",
    "bergkarabach": "Azerbaijan", "berg-karabach": "Azerbaijan",
    "berg karabach": "Azerbaijan",
    "türkei": "Turkey", "tuerkei": "Turkey", "turkey": "Turkey",
    "ägypten": "Egypt", "aegypten": "Egypt", "egypt": "Egypt",
    "tschad": "Chad", "chad": "Chad",
    "südafrika": "South Africa", "south africa": "South Africa",
}


def _find_countries(analysis: dict, max_n: int = 2) -> list[str]:
    """Extract UCDP-Country-Strings aus Claim/NER (max 2 für Performance)."""
    ner_countries = (analysis.get("ner_entities") or {}).get("countries", []) or []
    claim = analysis.get("claim", "") or ""
    original = analysis.get("original_claim") or claim
    search_terms = list(ner_countries) + [original, claim]

    found: list[str] = []
    seen: set[str] = set()
    for term in search_terms:
        if not isinstance(term, str):
            continue
        term_lower = term.lower()
        for name, ucdp_name in COUNTRY_MAP.items():
            if name in term_lower and ucdp_name not in seen:
                found.append(ucdp_name)
                seen.add(ucdp_name)
                if len(found) >= max_n:
                    return found
    return found


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def _ucdp_headers() -> dict[str, str]:
    """Build request headers. Includes UCDP_TOKEN if set."""
    global _no_token_warned
    headers: dict[str, str] = {"Accept": "application/json"}
    token = os.getenv("UCDP_TOKEN", "").strip()
    if token:
        headers["x-ucdp-access-token"] = token
    elif not _no_token_warned:
        logger.warning(
            "UCDP: kein UCDP_TOKEN gesetzt — Service liefert leere "
            "Resultate. Token bei https://ucdp.uu.se/ anfordern und "
            "als ENV-Variable bereitstellen."
        )
        _no_token_warned = True
    return headers


async def _get_json(client, url: str, params: dict) -> dict | None:
    """Single GET → JSON. Handles 401/4xx/5xx mit kurzem Log."""
    try:
        resp = await client.get(url, params=params)
        if resp.status_code == 401:
            logger.debug(f"UCDP 401 (token fehlt/ungültig) für {url}")
            return None
        if resp.status_code >= 400:
            logger.debug(f"UCDP HTTP {resp.status_code} für {url}: "
                         f"{resp.text[:200]}")
            return None
        return resp.json()
    except Exception as e:
        logger.debug(f"UCDP GET failed ({url}): {e}")
        return None


def _country_matches(event_country: str, target_country: str) -> bool:
    """Match UCDP `country`-Field gegen unser COUNTRY_MAP-Target.

    UCDP nutzt verbose Namen wie 'Russia (Soviet Union)', 'Yemen (North Yemen)',
    'Myanmar (Burma)', 'DR Congo (Zaire)'. Match-Strategy:
    1. Exact match (z.B. target='Ukraine' == event.country='Ukraine')
    2. Prefix-Match auf Wortgrenze (target='Russia' matches 'Russia (...)')
    """
    if not event_country or not target_country:
        return False
    if event_country == target_country:
        return True
    # Prefix-Match: target ist Anfang von event.country, gefolgt von Space/Paren
    if event_country.startswith(target_country):
        rest = event_country[len(target_country):]
        if not rest or rest[0] in (" ", "(", ",", "/"):
            return True
    # Inverse: target enthält event.country als Substring (Mapping zur
    # langen Form kann auch andersrum passen)
    if target_country.startswith(event_country):
        rest = target_country[len(event_country):]
        if not rest or rest[0] in (" ", "(", ",", "/"):
            return True
    return False


async def _fetch_ged_for_country(client, country: str) -> list[dict]:
    """Hole jüngste GED-Events für ein Land via Date-Range-Filter + Client-
    Side-Country-Filter (24h-Cache).

    UCDP-API-Country-Filter ist broken bei Spaces/Parens (SQL-Injection-Bug),
    daher StartDate/EndDate-Filter + lokales Country-Matching.
    """
    cache_key = f"ged|{country}"
    cached = _query_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1].get("Result") or []

    # Letzte 3 Kalenderjahre — deckt aktuelle Konflikte + Ukraine-Krieg ab
    import datetime as _dt
    today = _dt.date.today()
    start = f"{today.year - 3}-01-01"
    end = f"{today.year}-12-31"

    url = f"{UCDP_API_BASE}/gedevents/{UCDP_GED_VERSION}"
    params = {
        "StartDate": start,
        "EndDate": end,
        "pagesize": UCDP_FETCH_LIMIT,
    }
    data = await _get_json(client, url, params)
    if not data or not isinstance(data, dict):
        _query_cache[cache_key] = (time.time(), {"Result": []})
        return []
    all_events = data.get("Result") or []
    if not isinstance(all_events, list):
        _query_cache[cache_key] = (time.time(), {"Result": []})
        return []
    # Client-Side Filter nach country-Field
    filtered = [
        ev for ev in all_events
        if _country_matches(ev.get("country") or "", country)
    ][:MAX_EVENTS_PER_COUNTRY]
    _query_cache[cache_key] = (time.time(), {"Result": filtered})
    return filtered


async def _fetch_conflict_for_country(client, country: str) -> list[dict]:
    """Hole aktive Konflikte für ein Land via Year-Filter + Client-Side
    Country-Match (UCDP/PRIO Armed Conflict). Date-Range-Filter ist robust;
    Country= ist API-broken (SQL-Injection-Quirk)."""
    cache_key = f"conflict|{country}"
    cached = _query_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1].get("Result") or []

    # Aktive Konflikte: letzte 5 Jahre
    import datetime as _dt
    today = _dt.date.today()
    start = f"{today.year - 5}-01-01"
    end = f"{today.year}-12-31"

    url = f"{UCDP_API_BASE}/ucdpprioconflict/{UCDP_CONFLICT_VERSION}"
    params = {
        "StartDate": start,
        "EndDate": end,
        "pagesize": UCDP_FETCH_LIMIT,
    }
    data = await _get_json(client, url, params)
    if not data or not isinstance(data, dict):
        _query_cache[cache_key] = (time.time(), {"Result": []})
        return []
    all_conflicts = data.get("Result") or []
    if not isinstance(all_conflicts, list):
        _query_cache[cache_key] = (time.time(), {"Result": []})
        return []
    # Client-Side-Filter via location-Field (UCDP/PRIO Format)
    # Location-Field z.B. "Ukraine" oder "Russia (Soviet Union), Ukraine"
    filtered = [
        c for c in all_conflicts
        if any(_country_matches(loc.strip(), country)
               for loc in (c.get("location") or "").split(","))
    ][:30]
    _query_cache[cache_key] = (time.time(), {"Result": filtered})
    return filtered


# ---------------------------------------------------------------------------
# Aggregation / Result-Builder
# ---------------------------------------------------------------------------
def _safe_int(v) -> int:
    try:
        if v is None or v == "":
            return 0
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _aggregate_ged(events: list[dict]) -> dict:
    """Aggregiere GED-Sample zu Jahr-Buckets.

    UCDP-GED-Felder (Auszug v24.1):
      year, side_a, side_b, country, deaths_a, deaths_b, deaths_civilians,
      deaths_unknown, best, low, high, type_of_violence, date_start,
      date_end, where_coordinates, conflict_name, dyad_name
    """
    by_year: dict[int, dict] = {}
    actors: dict[str, int] = {}
    latest_date = ""
    for ev in events:
        if not isinstance(ev, dict):
            continue
        year = _safe_int(ev.get("year"))
        if year <= 0:
            continue
        bucket = by_year.setdefault(year, {
            "events": 0, "best": 0, "low": 0, "high": 0, "civilians": 0,
        })
        bucket["events"] += 1
        bucket["best"] += _safe_int(ev.get("best"))
        bucket["low"] += _safe_int(ev.get("low"))
        bucket["high"] += _safe_int(ev.get("high"))
        bucket["civilians"] += _safe_int(ev.get("deaths_civilians"))
        # Actor-Häufigkeit
        for key in ("side_a", "side_b"):
            name = (ev.get(key) or "").strip()
            if name:
                actors[name] = actors.get(name, 0) + 1
        d = (ev.get("date_end") or ev.get("date_start") or "").strip()
        if d and d > latest_date:
            latest_date = d
    top_actors = sorted(actors.items(), key=lambda kv: -kv[1])[:4]
    return {
        "by_year": by_year,
        "top_actors": [a for a, _ in top_actors],
        "latest_date": latest_date,
        "sample_size": len(events),
    }


def _format_country_result(country: str, ged_agg: dict,
                           conflicts: list[dict]) -> dict | None:
    """Erzeuge ein Evidora-Result-Dict pro Land."""
    if not ged_agg.get("by_year") and not conflicts:
        return None

    by_year = ged_agg.get("by_year") or {}
    sample = ged_agg.get("sample_size") or 0
    top_actors = ged_agg.get("top_actors") or []
    latest_date = ged_agg.get("latest_date") or "—"

    # Aktuellstes Jahr aus Sample
    headline_year = max(by_year.keys()) if by_year else None
    headline = f"UCDP {country}"
    parts: list[str] = []
    if headline_year is not None:
        h = by_year[headline_year]
        parts.append(
            f"{headline_year}: {h['events']} Event(s) im Sample"
        )
        if h["best"]:
            parts.append(
                f"Battle-Deaths best={h['best']} "
                f"(low {h['low']} / high {h['high']})"
            )
        if h["civilians"]:
            parts.append(f"Zivilisten-Tote {h['civilians']}")
    if top_actors:
        parts.append("Konfliktparteien: " + ", ".join(top_actors[:3]))
    if conflicts:
        active = []
        for c in conflicts[:3]:
            if not isinstance(c, dict):
                continue
            nm = (c.get("conflict_name") or c.get("location") or "").strip()
            yr = c.get("year") or ""
            if nm:
                active.append(f"{nm} ({yr})" if yr else nm)
        if active:
            parts.append("Konflikt-Dyaden: " + "; ".join(active))

    display = headline + " — " + " | ".join(parts) if parts else headline

    # Längere description: alle Jahres-Buckets + Caveat
    year_lines = []
    for y in sorted(by_year.keys(), reverse=True)[:5]:
        b = by_year[y]
        year_lines.append(
            f"{y}: {b['events']} Events | best {b['best']} "
            f"(low {b['low']}/high {b['high']}) | Zivil {b['civilians']}"
        )
    description = (
        "UCDP-Georeferenced-Event-Sample (max. 50 jüngste Events). "
        + (f"Sample-Umfang: {sample} Events. " if sample else "")
        + (f"Letztes Event-Datum im Sample: {latest_date}. " if latest_date != "—" else "")
        + (" | ".join(year_lines) if year_lines else "")
        + " | Methodik-Caveat: UCDP-Battle-Deaths sind direkt im Kampf "
        "getötete Personen (Soldaten + Zivilisten in Kampfhandlungen). "
        "Sie erfassen NICHT einseitige Gewalt gegen Zivilisten "
        "(UCDP-OSV-Dataset, separat) und NICHT indirekte Kriegstote "
        "(Hunger, Krankheiten). best/low/high sind UCDP-Schätzbereiche; "
        "die Wahrheit liegt zwischen low und high."
    )

    # Stabiles "year"-Feld für UI-Anzeige
    year_str = str(headline_year) if headline_year is not None else "—"

    return {
        "indicator_name": f"UCDP Konflikt-Events: {country}",
        "indicator": f"ucdp_events_{country.lower().replace(' ', '_')}",
        "country": country,
        "country_name": country,
        "year": year_str,
        "value": by_year.get(headline_year, {}).get("best", 0) if headline_year else 0,
        "display_value": display,
        "description": description,
        "url": "https://ucdp.uu.se/",
        "source": "UCDP Georeferenced Event Dataset v25.1 (CC-BY 4.0)",
    }


def _context_result() -> dict:
    """Methodik-Kontext (immer angehängt, wenn Resultate vorhanden)."""
    return {
        "indicator_name": "WICHTIGER KONTEXT: UCDP-Methodik",
        "indicator": "ucdp_context",
        "country": "",
        "country_name": "",
        "year": "",
        "value": "",
        "display_value": "",
        "url": "https://ucdp.uu.se/downloads/",
        "description": (
            "UCDP (Uppsala Conflict Data Program) ist der akademische "
            "Goldstandard für quantitative Konflikt-Daten. Definitionen: "
            "(1) 'Bewaffneter Konflikt' = mindestens 25 Schlacht-Tote pro "
            "Kalenderjahr zwischen zwei organisierten Konfliktparteien, "
            "von denen mindestens eine die Regierung eines Staates ist. "
            "(2) 'Battle-Deaths' = direkt im Kampf getötete Personen — "
            "explizit ausgeschlossen sind indirekte Kriegstote (Hunger, "
            "Krankheit) und einseitige Gewalt gegen Zivilisten "
            "(UCDP-OSV-Dataset, separat). "
            "(3) UCDP gibt für jedes Event eine best-estimate plus low/"
            "high-Spannweite an — Punktzahlen sind tendenziell zu "
            "präzise. "
            "(4) Die Datenbank wird jährlich publiziert und reicht für "
            "Geo-Events bis 1989 zurück (Konflikt-Aggregate bis 1946). "
            "(5) UCDP klassifiziert side_a/side_b faktisch und nimmt "
            "KEINE Schuldzuweisung vor; die Reihenfolge in den Dyaden "
            "folgt einer methodischen Konvention, nicht einer "
            "moralischen Wertung."
        ),
        "source": "UCDP",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_ucdp(analysis: dict) -> dict:
    """Live-Lookup gegen UCDP-API.

    Strategie:
      1. Country-Detection aus NER + Claim-Text (max. 2 Länder).
      2. Pro Land: GED-Event-Sample (Battle-Deaths) + aktive Konflikte.
      3. Aggregation zu Jahr-Buckets + Konfliktparteien-Liste.
    """
    empty = {
        "source": "UCDP",
        "type": "conflict_events",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim", "") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    combined_lc = f"{original} {claim}".lower()

    if not _claim_mentions_ucdp(combined_lc):
        return empty

    countries = _find_countries(analysis)
    if not countries:
        # Ohne klares Land kein sinnvoller UCDP-Lookup
        logger.debug("UCDP: Trigger ja, aber kein Land im Claim identifiziert")
        return empty

    # Ohne Token → früh aussteigen (Logging erfolgt in _ucdp_headers).
    headers = _ucdp_headers()
    if "x-ucdp-access-token" not in headers:
        return empty

    results: list[dict] = []
    async with polite_client(timeout=TIMEOUT_S, headers=headers) as client:
        for country in countries:
            ged_events = await _fetch_ged_for_country(client, country)
            conflicts = await _fetch_conflict_for_country(client, country)
            ged_agg = _aggregate_ged(ged_events)
            r = _format_country_result(country, ged_agg, conflicts)
            if r:
                results.append(r)
                if len(results) >= MAX_RESULTS:
                    break

    if not results:
        logger.info(f"UCDP: 0 Treffer (countries={countries})")
        return empty

    results.append(_context_result())
    logger.info(
        f"UCDP: {len(results) - 1} Country-Treffer, "
        f"countries={countries}"
    )
    return {
        "source": "UCDP",
        "type": "conflict_events",
        "results": results,
    }
