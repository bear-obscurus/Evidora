"""AI Incident Database (AIID) — Live-Connector via GraphQL.

Datenquelle: https://incidentdatabase.ai/ — 3.500+ dokumentierte
KI-Schadensfälle weltweit. Verwaltet von der Responsible AI Collaborative
(RAIC), OECD-anerkannt als offizielles AI-Incidents-Repository.
Lizenz: Apache 2.0 (Code) + ODbL (Daten) — CC-BY-SA-kompatibel,
Evidora-tauglich.

API: POST https://incidentdatabase.ai/api/graphql (GraphQL, kein Auth)
Wichtig: Die API ist auf Browser-Clients beschränkt (CORS via Origin-
Header). Wir müssen `Origin` + `Referer` + Browser-`User-Agent` setzen,
sonst kommt 403 "Forbidden - Invalid origin".

Schema-Auszüge (entdeckt via Introspection):
- `incidents(filter, sort, pagination)`-Field mit:
  - filter: IncidentFilterType { incident_id: IntFilter, title: StringFilter,
    description: StringFilter, ... }
  - StringFilter: { REGEX, OPTIONS, EQ, IN, ... } (MongoDB-Style)
  - IntFilter:    { EQ, IN, GT, ... }
  - sort: IncidentSortType { date: ASC/DESC, incident_id: ASC/DESC, ... }
  - pagination: { limit: Int, skip: Int }
- Felder: incident_id, title, date, description,
  AllegedDeveloperOfAISystem { name }, AllegedDeployerOfAISystem { name },
  reports { title, url, source_domain }

Komplementär zu existierenden Tech-Quellen:
- OSV.dev / NVD: Open-Source-Vulnerabilities (technische Schwachstellen).
- AIID: KI-Schadensfälle (algorithmische Diskriminierung, Halluzinationen,
  autonome Systeme, Deepfakes, …) — i.d.R. KEINE klassischen CVEs.

Politische Guardrails: Reine Vorfalls-Dokumentation. KEINE Bewertung
der Verantwortlichkeit (`Alleged*`-Felder bleiben "alleged"). KEINE
Schuldzuweisungen. Synthesizer baut Disclaimer ("Vorfälle sind
gemeldete Allegations, kein abschließendes Urteil über Schuld").
"""

# WIRING für main.py:
# from services.aiid import search_aiid, claim_mentions_aiid_cached
# if claim_mentions_aiid_cached(claim):
#     tasks.append(cached("AIID", search_aiid, analysis))
#     queried_names.append("AI Incident Database")

from __future__ import annotations

import logging
import re
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

AIID_GRAPHQL_URL = "https://incidentdatabase.ai/api/graphql"

# Die API blockt non-Browser-Clients via Origin-Check.
# Wir setzen Mozilla-UA + Origin/Referer; die Anfrage bleibt aber ehrlich
# (Evidora identifiziert sich via Origin auf evidora.eu wäre nicht
# erlaubt, deshalb der Site-eigene Origin).
_AIID_HEADERS: dict[str, str] = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://incidentdatabase.ai",
    "Referer": "https://incidentdatabase.ai/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
}

TIMEOUT_S = 15.0
MAX_RESULTS = 5
CACHE_TTL_S = 24 * 60 * 60  # 24 h

# Modul-Level Cache: cache-key → (ts, payload)
_search_cache: dict[str, tuple[float, list[dict]]] = {}
_incident_cache: dict[int, tuple[float, dict | None]] = {}


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_AIID_TERMS = (
    # Direkt
    "aiid", "ai incident database", "ai-incident-database",
    "responsible ai collaborative",
    # Deutsche KI-Vorfalls-Terme
    "ki-vorfall", "ki-vorfälle", "ki-skandal", "ki-zwischenfall",
    "ai-vorfall", "ai vorfall",
    "ki-diskriminierung", "ki diskriminierung",
    "algorithmische diskriminierung", "algorithmischer bias",
    "algorithmen-diskriminierung",
    "deepfake-vorfall", "deepfake vorfall", "deepfake-skandal",
    "deepfake skandal", "deepfake-betrug",
    "gesichtserkennung fehler", "gesichtserkennungs-fehler",
    "gesichtserkennung-fehlalarm",
    "polizei-ki", "predictive policing fehler",
    "autonomer fahrzeug-unfall", "autonomes fahrzeug unfall",
    "autonomous vehicle accident",
    "tesla autopilot unfall", "tesla autopilot crash",
    "waymo unfall", "cruise unfall",
    # Englische Varianten (oft 1:1 übernommen)
    "ai incident", "ai harm", "ai bias incident",
    "facial recognition error", "facial recognition false match",
    "facial recognition false arrest",
    "algorithmic bias", "algorithmic discrimination",
    "ai accident", "ai-related accident",
    # Spezifisch häufige Vorfalls-Pattern
    "ki hat falsch geantwortet", "chatbot halluzination",
    "chatgpt halluziniert", "claude halluziniert", "gemini halluziniert",
)

# Composite-Trigger: KI-Modell + falsch/Fehler/Vorfall/Diskriminierung
_AI_MODEL_TOKENS = (
    "chatgpt", "claude", "gemini", "copilot", "midjourney", "dall-e",
    "dall e", "stable diffusion", "llama", "mistral", "grok",
    "deepseek", "openai", "anthropic", "google bard", " bard ",
    "tesla autopilot", "tesla fsd", "waymo", "cruise",
    "compas algorithmus", "compas score",
    "amazon rekognition", "clearview ai", "palantir",
)

_INCIDENT_KEYWORDS = (
    "vorfall", "skandal", "diskriminierung", "fehler", "fehlalarm",
    "unfall", "verurteilt", "klage", "lawsuit", "schadensfall",
    "bias", "vorurteil", "rassismus", "geschlechter-bias",
    "halluzination", "halluziniert", "ausgegeben",
    "falsch identifiziert", "wrongful arrest", "false match",
    "false positive", "hat falsch", "hat menschen", "ausgespuckt",
    "menschen geschädigt", "geschädigt", "diskriminiert",
)

# Direkte Incident-ID-Erkennung: "AIID-Incident #123" oder "Incident #123"
_INCIDENT_ID_REGEX = re.compile(
    r"\b(?:AIID[-\s]*(?:Incident\s*)?#?|Incident\s*#)(\d{1,5})\b",
    re.IGNORECASE,
)


def _claim_mentions_aiid(claim_lc: str) -> bool:
    """Trigger-Pre-Check.

    True wenn:
      1. AIID-Incident-ID explizit referenziert
      2. Direkter Trigger-Term ("KI-Vorfall", "algorithmische Diskriminierung")
      3. KI-Modell-Name + Incident-Keyword
    """
    if not claim_lc:
        return False
    # 1) Direkt-Trigger
    if any(t in claim_lc for t in _AIID_TERMS):
        return True
    # 2) Incident-ID-Pattern
    if _INCIDENT_ID_REGEX.search(claim_lc):
        # Aber nur, wenn KI-Kontext im Claim ist (sonst greift Pattern auch
        # für Polizei-Bericht "Vorfall #42" etc.).
        has_ai_context = any(t in claim_lc for t in _AI_MODEL_TOKENS) or any(
            t in claim_lc for t in (
                "ki ", " ki", "ai ", " ai", "künstliche intelligenz",
                "kuenstliche intelligenz", "artificial intelligence",
                "machine learning", "ml-modell", "algorithmus",
            )
        )
        if has_ai_context:
            return True
    # 3) Composite: KI-Modell + Incident-Kw
    has_model = any(t in claim_lc for t in _AI_MODEL_TOKENS)
    if has_model:
        has_incident_kw = any(t in claim_lc for t in _INCIDENT_KEYWORDS)
        if has_incident_kw:
            return True
    return False


def claim_mentions_aiid_cached(claim: str) -> bool:
    """Public-Wrapper für Trigger-Check (case-normalisiert)."""
    return _claim_mentions_aiid((claim or "").lower())


# ---------------------------------------------------------------------------
# Query-Builder
# ---------------------------------------------------------------------------
# Begriffe, die NICHT als Such-Token taugen (zu generisch).
_STOPWORDS = frozenset({
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen",
    "einer", "eines", "und", "oder", "aber", "wenn", "dann", "also",
    "auch", "noch", "doch", "schon", "sein", "haben", "werden", "wird",
    "wurde", "wurden", "ist", "sind", "war", "waren", "kann", "konnte",
    "soll", "sollte", "muss", "musste", "darf", "durfte", "mag",
    "the", "and", "for", "with", "from", "this", "that", "these",
    "those", "have", "has", "had", "will", "would", "could", "should",
    "ich", "du", "er", "sie", "es", "wir", "ihr", "mich", "dich", "sich",
    "in", "an", "auf", "zu", "bei", "von", "vor", "nach", "über", "unter",
    "zum", "zur", "im", "ins", "am", "ans", "vom", "beim",
    "ki", "ai", "vorfall", "incident", "skandal", "fehler",
    "wer", "was", "wie", "wo", "wann", "warum", "welche", "welcher",
    "welches", "ja", "nein",
})


def _extract_incident_ids(claim: str) -> list[int]:
    """Extrahiere explizite Incident-IDs aus Claim-Text (max 3, de-dupliziert)."""
    if not claim:
        return []
    out: list[int] = []
    seen: set[int] = set()
    for m in _INCIDENT_ID_REGEX.findall(claim):
        try:
            n = int(m)
        except (TypeError, ValueError):
            continue
        if n in seen or n <= 0:
            continue
        seen.add(n)
        out.append(n)
        if len(out) >= 3:
            break
    return out


_WORD_REGEX = re.compile(r"[A-Za-zÀ-ɏ][A-Za-z0-9À-ɏ\-]{2,}")


def _extract_keywords(claim: str, max_n: int = 4) -> list[str]:
    """Top-Keywords aus Claim-Text.

    Heuristik:
      - Erst Eigennamen / KI-Modell-Tokens, die auch in der API stehen.
      - Dann allgemeine Substantive/Begriffe ≥ 3 Zeichen, ohne Stopwords.
      - De-dupliziert, Reihenfolge nach Auftreten.
    """
    if not claim:
        return []
    lc = claim.lower()
    out: list[str] = []
    seen: set[str] = set()

    # 1) KI-Modell-Tokens zuerst (sehr selektiv)
    for tok in _AI_MODEL_TOKENS:
        key = tok.strip()
        if not key:
            continue
        if key in lc and key not in seen:
            seen.add(key)
            out.append(key)
            if len(out) >= max_n:
                return out

    # 2) Sonstige Wort-Tokens
    for m in _WORD_REGEX.findall(claim):
        token = m.lower()
        if token in seen or token in _STOPWORDS:
            continue
        if len(token) < 4:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= max_n:
            break
    return out


def _build_search_query(keywords: list[str]) -> str:
    """Erzeuge MongoDB-style REGEX-Pattern.

    Beispiel: ["chatgpt", "halluziniert"] → "chatgpt|halluziniert"
    Wir nutzen REGEX + OPTIONS:"i" für case-insensitive Substring-Match
    auf `title` (und in einer 2. Query auf `description`).
    """
    if not keywords:
        return ""
    parts = [re.escape(k) for k in keywords if k]
    return "|".join(parts)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
_GQL_FIELDS = (
    "incident_id title date description "
    "AllegedDeveloperOfAISystem { name } "
    "AllegedDeployerOfAISystem { name } "
    "reports { title url source_domain }"
)


async def _post_graphql(client, query: str, variables: dict | None = None) -> dict | None:
    """Generischer POST gegen AIID-GraphQL.

    Returns das `data`-Objekt oder None bei Fehler.
    """
    body = {"query": query}
    if variables:
        body["variables"] = variables
    try:
        resp = await client.post(AIID_GRAPHQL_URL, json=body)
        if resp.status_code != 200:
            logger.debug(f"AIID HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        payload = resp.json()
        if not isinstance(payload, dict):
            return None
        if payload.get("errors"):
            logger.debug(f"AIID GraphQL errors: {payload['errors'][:1]}")
            return None
        return payload.get("data")
    except Exception as e:
        logger.debug(f"AIID GraphQL POST failed: {e}")
        return None


async def _fetch_by_incident_id(client, incident_id: int) -> dict | None:
    """Direkter Incident-Lookup über `incident_id: { EQ: N }`.

    Mit 24 h-Cache.
    """
    cached = _incident_cache.get(incident_id)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]

    query = (
        "query ($id: Int!) { incidents("
        "filter: { incident_id: { EQ: $id } }, "
        "pagination: { limit: 1 }"
        ") { " + _GQL_FIELDS + " } }"
    )
    data = await _post_graphql(client, query, {"id": int(incident_id)})
    if not data:
        _incident_cache[incident_id] = (time.time(), None)
        return None
    incidents = data.get("incidents") or []
    if not incidents:
        _incident_cache[incident_id] = (time.time(), None)
        return None
    inc = incidents[0] if isinstance(incidents[0], dict) else None
    _incident_cache[incident_id] = (time.time(), inc)
    return inc


async def _search_by_keywords(client, regex_pattern: str) -> list[dict]:
    """Keyword-Search via REGEX-Filter auf title (Fallback: description).

    Sortiert nach date DESC. Mit 24 h-Cache.
    """
    cache_key = regex_pattern
    cached = _search_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < CACHE_TTL_S:
        return cached[1]

    # Erst Title-Match (selektiver), dann Description-Match als Ergänzung.
    title_query = (
        "query ($pat: String!) { incidents("
        "filter: { title: { REGEX: $pat, OPTIONS: \"i\" } }, "
        "sort: { date: DESC }, "
        f"pagination: {{ limit: {MAX_RESULTS} }}"
        ") { " + _GQL_FIELDS + " } }"
    )
    data = await _post_graphql(client, title_query, {"pat": regex_pattern})
    incidents = (data or {}).get("incidents") or []
    if not isinstance(incidents, list):
        incidents = []

    if len(incidents) < MAX_RESULTS:
        # Beschreibungs-Match als Ergänzung
        desc_query = (
            "query ($pat: String!) { incidents("
            "filter: { description: { REGEX: $pat, OPTIONS: \"i\" } }, "
            "sort: { date: DESC }, "
            f"pagination: {{ limit: {MAX_RESULTS} }}"
            ") { " + _GQL_FIELDS + " } }"
        )
        data2 = await _post_graphql(client, desc_query, {"pat": regex_pattern})
        more = (data2 or {}).get("incidents") or []
        if isinstance(more, list):
            seen_ids = {
                i.get("incident_id") for i in incidents if isinstance(i, dict)
            }
            for inc in more:
                if not isinstance(inc, dict):
                    continue
                if inc.get("incident_id") in seen_ids:
                    continue
                incidents.append(inc)
                if len(incidents) >= MAX_RESULTS:
                    break

    incidents = [i for i in incidents if isinstance(i, dict)][:MAX_RESULTS]
    _search_cache[cache_key] = (time.time(), incidents)
    return incidents


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _entity_names(entries: list | None, max_n: int = 3) -> list[str]:
    """Extrahiere Namen aus AllegedDeveloperOfAISystem etc."""
    if not isinstance(entries, list):
        return []
    out: list[str] = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        name = e.get("name")
        if isinstance(name, str) and name.strip():
            out.append(name.strip())
            if len(out) >= max_n:
                break
    return out


def _report_summary(reports: list | None, max_n: int = 3) -> str:
    """Kurze Bericht-Liste 'Titel (domain)'."""
    if not isinstance(reports, list):
        return ""
    bits: list[str] = []
    for r in reports[:max_n]:
        if not isinstance(r, dict):
            continue
        title = (r.get("title") or "").strip()
        dom = (r.get("source_domain") or "").strip()
        if title and dom:
            short = title if len(title) <= 100 else title[:97] + "…"
            bits.append(f"'{short}' ({dom})")
        elif title:
            short = title if len(title) <= 100 else title[:97] + "…"
            bits.append(f"'{short}'")
        elif dom:
            bits.append(dom)
    return "; ".join(bits)


def _format_incident(inc: dict) -> dict | None:
    """Mapping AIID-Incident → Evidora-Result-Dict."""
    if not isinstance(inc, dict):
        return None
    incident_id = inc.get("incident_id")
    if incident_id is None:
        return None
    try:
        incident_id_int = int(incident_id)
    except (TypeError, ValueError):
        return None

    title = (inc.get("title") or "").strip()
    date_str = (inc.get("date") or "").strip()
    description_raw = (inc.get("description") or "").strip()
    developers = _entity_names(inc.get("AllegedDeveloperOfAISystem"))
    deployers = _entity_names(inc.get("AllegedDeployerOfAISystem"))
    reports = inc.get("reports") or []
    n_reports = len(reports) if isinstance(reports, list) else 0

    year = date_str[:4] if len(date_str) >= 4 and date_str[:4].isdigit() else "—"

    # Kurz-Beschreibung
    short_desc = description_raw
    if len(short_desc) > 280:
        short_desc = short_desc[:277] + "…"
    display_value = short_desc or title or f"AIID-Incident #{incident_id_int}"

    # Längere description: Description + Entities + Reports + Datum
    description_parts: list[str] = []
    if description_raw:
        description_parts.append(description_raw[:600])
    extras: list[str] = []
    if developers:
        extras.append(f"Alleged Developer: {', '.join(developers)}")
    if deployers:
        extras.append(f"Alleged Deployer: {', '.join(deployers)}")
    if extras:
        description_parts.append(" · ".join(extras))
    rep_str = _report_summary(reports)
    if rep_str:
        description_parts.append(f"Reports: {rep_str}")
    if date_str:
        description_parts.append(f"Vorfalls-Datum: {date_str}")
    description_parts.append(
        f"Quelle: AI Incident Database (AIID), {n_reports} verlinkte "
        "Berichte. 'Alleged' bedeutet: berichteter Vorwurf, kein "
        "abschließendes Urteil über Schuld."
    )
    description = " — ".join(p for p in description_parts if p)[:1200]

    # Country-Heuristik: AIID hat selten ein Country-Feld direkt; wir
    # nutzen Source-Domains der Reports für eine grobe Schätzung.
    country_code, country_name = _infer_country(reports)

    return {
        "indicator_name": (
            f"AIID-Incident #{incident_id_int}: {title}"
        )[:300],
        "indicator": f"aiid_{incident_id_int}",
        "country": country_code,
        "country_name": country_name,
        "year": year,
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": f"https://incidentdatabase.ai/cite/{incident_id_int}",
        "source": "AI Incident Database (Responsible AI Collaborative)",
    }


def _infer_country(reports: list | None) -> tuple[str, str]:
    """Grobe Länder-Schätzung aus Report-Domains.

    Sehr konservativ — Default ist 'INT' / 'International'.
    """
    default = ("INT", "International")
    if not isinstance(reports, list):
        return default
    domains = []
    for r in reports[:5]:
        if isinstance(r, dict):
            d = (r.get("source_domain") or "").strip().lower()
            if d:
                domains.append(d)
    if not domains:
        return default
    # Country-TLD-Heuristik
    tld_map = {
        ".at": ("AUT", "Österreich (Reports)"),
        ".de": ("DEU", "Deutschland (Reports)"),
        ".ch": ("CHE", "Schweiz (Reports)"),
        ".uk": ("GBR", "Vereinigtes Königreich (Reports)"),
        ".fr": ("FRA", "Frankreich (Reports)"),
        ".it": ("ITA", "Italien (Reports)"),
        ".es": ("ESP", "Spanien (Reports)"),
        ".nl": ("NLD", "Niederlande (Reports)"),
        ".se": ("SWE", "Schweden (Reports)"),
        ".ca": ("CAN", "Kanada (Reports)"),
        ".au": ("AUS", "Australien (Reports)"),
        ".jp": ("JPN", "Japan (Reports)"),
        ".cn": ("CHN", "China (Reports)"),
        ".in": ("IND", "Indien (Reports)"),
    }
    # Eindeutige Mehrheit?
    counts: dict[tuple[str, str], int] = {}
    for d in domains:
        for tld, val in tld_map.items():
            if d.endswith(tld):
                counts[val] = counts.get(val, 0) + 1
                break
        else:
            # generische TLDs (.com/.org/.ai/…) → USA-Default-Tendenz, aber
            # nicht zuverlässig, daher unter "INT" lassen.
            pass
    if not counts:
        # Mehrheit generisch → USA als wahrscheinlichster Origin für KI-
        # Berichterstattung; aber Default lieber neutral.
        return ("USA", "USA / International (vermutet)")
    top = max(counts.items(), key=lambda kv: kv[1])
    return top[0]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_aiid(analysis: dict) -> dict:
    """Live-Lookup gegen AIID-GraphQL.

    Strategie:
      1. Incident-ID im Claim ("AIID-Incident #123") → direkter Lookup.
      2. Top-Keywords aus Claim → REGEX-Suche auf title (Fallback: description).
      3. Sortiert nach date DESC, gedeckelt auf MAX_RESULTS.
    """
    empty = {
        "source": "AI Incident Database",
        "type": "ai_incidents",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    combined = f"{original} {claim}".strip()
    combined_lc = combined.lower()

    if not _claim_mentions_aiid(combined_lc):
        return empty

    incident_ids = _extract_incident_ids(combined)
    keywords = _extract_keywords(combined)
    regex_pattern = _build_search_query(keywords)

    if not incident_ids and not regex_pattern:
        return empty

    results: list[dict] = []
    seen_keys: set[str] = set()

    async with polite_client(timeout=TIMEOUT_S, headers=_AIID_HEADERS) as client:
        # 1) Direkte ID-Lookups
        for inc_id in incident_ids:
            inc = await _fetch_by_incident_id(client, inc_id)
            if not inc:
                continue
            r = _format_incident(inc)
            if r and r["indicator"] not in seen_keys:
                seen_keys.add(r["indicator"])
                results.append(r)
                if len(results) >= MAX_RESULTS:
                    break

        # 2) Keyword-Search (falls Platz)
        if len(results) < MAX_RESULTS and regex_pattern:
            incs = await _search_by_keywords(client, regex_pattern)
            for inc in incs:
                if len(results) >= MAX_RESULTS:
                    break
                r = _format_incident(inc)
                if r and r["indicator"] not in seen_keys:
                    seen_keys.add(r["indicator"])
                    results.append(r)

    if not results:
        logger.info(
            f"AIID: 0 Treffer (ids={incident_ids[:3]}, "
            f"keywords={keywords[:4]})"
        )
        return empty

    logger.info(f"AIID: {len(results)} Incident-Treffer geliefert")
    return {
        "source": "AI Incident Database",
        "type": "ai_incidents",
        "results": results,
    }
