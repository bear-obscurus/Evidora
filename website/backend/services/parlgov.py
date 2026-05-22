"""ParlGov — Parliaments and Governments Database (Univ. Bremen / Harvard Dataverse).

Datenquelle: ParlGov 2024 (Doring & Manow), EU+OECD-Demokratien, 1900-2023.
URL:    https://www.parlgov.org/data-info/
Bulk:   CSV-Downloads (parties.csv, elections.csv, cabinets.csv)
Dataverse: doi:10.7910/DVN/Q6CVHX
Lizenz: Permissive Open-Source (CC-Attribution).
Coverage: ~1700 Parteien / ~1000 Wahlen / ~1600 Kabinette / 37 Länder.

Architektur: Static-First-Hybrid (wie wahlen.py / volksbegehren.py).
ParlGov ist eine relationale CSV-Sammlung ohne Query-API — wir hinterlegen
einen kuratierten AT/DE-Schwerpunkt + EU-Wichtige (UK, FR, IT, ES) als
JSON unter ``data/parlgov.json``. Bulk-Refresh via Script (out-of-scope
hier, optionaler Stub mit polite_client für späteres data_updater-Wiring).

Use-Case:
- "Wahlergebnis Deutschland 2021"
- "Kabinett-Bildung Italien 2022"
- "Koalition Spanien Sánchez"
- "ParlGov sagt Schröder I war SPD-Grüne"

GUARDRAILS (siehe project_political_guardrails.md, Tabu-Guard 2.0):
- ParlGov ist Country-Level-Wahldata — **keine** Partei-Wertung.
- Wir zitieren Stimmen/Sitze/Koalitionen, ohne politische Einordnung.
- Politik-Tabu-Guard 2.0: Partei + Korruption + Superlativ ohne Anker
  → kein Trigger (siehe services/_topic_match.politik_guard_action).
- Keine Wahlprognosen — nur historisch abgeschlossene Wahlen.
- Komplementär zu services/wahlen.py (BMI-AT-Wahlen detail) und
  services/parlament_at.py (AT-Parlament Live-Status). ParlGov ergänzt
  um DE/UK/FR/IT/ES + Kabinettsbildung.

Wiring (NICHT in dieser Datei):
  # from services.parlgov import search_parlgov, claim_mentions_parlgov_cached
  # if claim_mentions_parlgov_cached(claim):
  #     tasks.append(cached("ParlGov", search_parlgov, analysis))
  #     queried_names.append("ParlGov (Univ. Bremen Election Database)")
  #
  # reranker.py Whitelist:
  #     "ParlGov": "elections_data",
  #
  # data_updater.py Prefetch (optional, Static-Load only):
  #     from services.parlgov import fetch_parlgov
  #     await fetch_parlgov()
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import re
import time

import httpx

from services._http_polite import polite_client
from services._topic_match import is_party_corruption_superlative_claim

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "parlgov.json",
)

# 24h cache TTL — ParlGov ist jährliches Release, intraday-Cache reicht.
CACHE_TTL = 86400

_cache: dict | None = None
_cache_time: float = 0.0


# ---------------------------------------------------------------------------
# Trigger vocabulary
# ---------------------------------------------------------------------------
_PARLGOV_TERMS = (
    "parlgov", "parl gov",
    "parliaments and governments",
    "doring manow", "döring manow",
    "harvard dataverse election",
    "kabinettsbildung", "kabinett-bildung", "kabinett bildung",
    "regierungsbildung",
    "koalitionsbildung", "koalitions-bildung",
)

# Land-spezifische Wahl-Begriffe (allein-genügend, ohne Land-Token)
_COUNTRY_ELECTION_TERMS = (
    "bundestagswahl", "bundestags-wahl", "bundestags wahl",
    "nationalratswahl", "nationalrats-wahl", "nationalrats wahl",
    "präsidentschaftswahl", "praesidentschaftswahl",
    "présidentielle", "presidentielle",
    "elezioni politiche", "elecciones generales",
    "general election",
)

# Generelle Wahl-/Koalitions-Trigger (composite: brauchen Land-Token)
_ELECTION_TERMS = (
    "wahlergebnis", "wahlergebnisse", "wahl-ergebnis",
    "kabinett ", "regierung ",
    "koalition", "koalitions",
    "regierungskoalition", "regierungs-koalition",
    "mehrheitsregierung", "minderheitsregierung",
    "hung parliament",
)

# Composite-Trigger: Wahl-/Regierungs-Begriff + EU-/Land-Token
_LAND_TOKENS = (
    # DE
    "deutschland", "germany", "bundesrepublik",
    # UK
    "großbritannien", "grossbritannien", "uk ", " uk", "united kingdom",
    "vereinigtes königreich", "vereinigtes koenigreich",
    # FR
    "frankreich", "france", "französisch", "franzoesisch",
    # IT
    "italien", "italy", "italia", "italienisch",
    # ES
    "spanien", "spain", "españa", "espana", "spanisch",
    # AT (komplementär zu wahlen.py)
    "österreich", "oesterreich", "austria",
)

# Spezifische bekannte Wahlen / Kabinette (Composite-Anker)
_FAMOUS_CABINETS = (
    "adenauer", "brandt", "schmidt ", "kohl", "schröder", "schroeder",
    "merkel", "scholz", "merz",
    "blair", "cameron", "may regierung", "johnson regierung", "starmer",
    "chirac", "sarkozy", "hollande", "macron",
    "berlusconi", "prodi", "renzi", "conte regierung", "draghi", "meloni",
    "zapatero", "rajoy", "sánchez", "sanchez",
    "vranitzky", "schüssel", "schuessel", "gusenbauer", "faymann",
    "kurz regierung", "kurz i", "kurz ii",
    "nehammer", "stocker regierung",
    "ampel-koalition", "ampel koalition", "große koalition", "grosse koalition",
    "rot-grün", "rot grün", "rot-gruen", "rot gruen",
    "schwarz-grün", "schwarz grün", "schwarz-gruen", "schwarz gruen",
    "schwarz-blau", "schwarz blau",
    "schwarz-rot", "schwarz rot",
)


def _claim_mentions_parlgov(claim_lc: str) -> bool:
    """Pure-string Trigger-Test gegen ParlGov-Themenkeywords.

    Politik-Tabu-Guard 2.0: Partei + Korruption + Superlativ ohne Anker
    → kein Trigger (Country-Level-Quellen würden Kategorienfehler erzeugen).
    """
    if not claim_lc:
        return False
    # 0) Politik-Tabu-Guard 2.0
    if is_party_corruption_superlative_claim(claim_lc):
        return False
    # 1) Direkte ParlGov-Begriffe
    if any(t in claim_lc for t in _PARLGOV_TERMS):
        return True
    # 2) Land-spezifische Wahl-Begriffe (Bundestagswahl/Nationalratswahl/...)
    if any(t in claim_lc for t in _COUNTRY_ELECTION_TERMS):
        return True
    # 3) Bekannte Kabinette / Koalitions-Muster
    if any(t in claim_lc for t in _FAMOUS_CABINETS):
        return True
    # 4) Composite: Wahl-/Regierungs-Begriff + Land-Token
    has_election = any(t in claim_lc for t in _ELECTION_TERMS)
    has_country = any(t in claim_lc for t in _LAND_TOKENS)
    if has_election and has_country:
        return True
    # 5) Composite: Person-Anker (Regierungschef) + Wahl-/Jahr-Begriff
    has_person = any(p in claim_lc for p in PERSON_TO_COUNTRY)
    has_year_or_wahl = (
        bool(_YEAR_RE.search(claim_lc))
        or "wahl" in claim_lc
        or "election" in claim_lc
    )
    if has_person and has_year_or_wahl:
        return True
    # 6) Composite: Generic Wahl-Token + (Year-Pattern oder Land-Token)
    #    Wahl-Tokens decken Wahl, Europawahl, Parlamentswahl, Wahlumfrage,
    #    election etc. via Substring "wahl"/"election" ab.
    has_generic_wahl = ("wahl" in claim_lc) or ("election" in claim_lc)
    has_year = bool(_YEAR_RE.search(claim_lc))
    has_country = any(t in claim_lc for t in _LAND_TOKENS)
    if has_generic_wahl and (has_year or has_country):
        return True
    return False


def claim_mentions_parlgov_cached(claim: str) -> bool:
    """Public-API: lowercase + Trigger-Test."""
    return _claim_mentions_parlgov((claim or "").lower())


# ---------------------------------------------------------------------------
# Static load
# ---------------------------------------------------------------------------
def _load_static_json() -> dict | None:
    """Lade ``data/parlgov.json`` mit 24h-Memory-Cache."""
    global _cache, _cache_time
    now = time.time()
    if _cache is not None and (now - _cache_time) < CACHE_TTL:
        return _cache
    try:
        with open(STATIC_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "countries" not in data:
            logger.warning("parlgov.json missing 'countries' key")
            return None
        _cache = data
        _cache_time = now
        n_countries = len(data.get("countries") or {})
        n_elections = sum(
            len((c or {}).get("elections") or [])
            for c in (data.get("countries") or {}).values()
        )
        logger.info(
            "ParlGov data loaded: %d countries, %d elections",
            n_countries, n_elections,
        )
        return _cache
    except FileNotFoundError:
        logger.warning("parlgov.json not found at %s", STATIC_JSON_PATH)
        return None
    except Exception as e:  # noqa: BLE001
        logger.warning("parlgov.json load failed: %s", e)
        return None


async def fetch_parlgov(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Optionaler Bulk-Refresh-Hook für data_updater (NICHT in main.py wired).

    Aktuell: Static-Load. Bei späterer Bulk-CSV-Integration würde hier
    ``polite_client`` (s. _http_polite) die ParlGov-CSVs holen und in
    JSON konvertieren. Aktuell nur ein Cache-Warmup.
    """
    _ = polite_client  # noqa: F841 — Marker für späteres Bulk-Refresh
    data = _load_static_json()
    if not data:
        return []
    return [data]


# ---------------------------------------------------------------------------
# Country detection
# ---------------------------------------------------------------------------
COUNTRY_KEYWORDS: dict[str, str] = {
    # AT
    "österreich": "AUT", "oesterreich": "AUT", "austria": "AUT",
    "nationalratswahl": "AUT", "nationalrat ": "AUT",
    # DE
    "deutschland": "DEU", "germany": "DEU", "bundesrepublik": "DEU",
    "bundestagswahl": "DEU", "bundestag ": "DEU",
    # UK
    "großbritannien": "GBR", "grossbritannien": "GBR",
    "united kingdom": "GBR", "vereinigtes königreich": "GBR",
    "vereinigtes koenigreich": "GBR", " uk ": "GBR",
    # FR
    "frankreich": "FRA", "france": "FRA", "französisch": "FRA",
    "franzoesisch": "FRA", "présidentielle": "FRA", "presidentielle": "FRA",
    # IT
    "italien": "ITA", "italy": "ITA", "italia": "ITA", "italienisch": "ITA",
    "elezioni": "ITA",
    # ES
    "spanien": "ESP", "spain": "ESP", "españa": "ESP", "espana": "ESP",
    "spanisch": "ESP", "generales": "ESP",
}

# Person-Anker → Land (für Kabinette/Regierungschefs)
PERSON_TO_COUNTRY: dict[str, str] = {
    "adenauer": "DEU", "brandt": "DEU", "schmidt": "DEU", "kohl": "DEU",
    "schröder": "DEU", "schroeder": "DEU", "merkel": "DEU",
    "scholz": "DEU", "merz": "DEU",
    "blair": "GBR", "brown": "GBR", "cameron": "GBR", "may": "GBR",
    "johnson": "GBR", "truss": "GBR", "sunak": "GBR", "starmer": "GBR",
    "chirac": "FRA", "sarkozy": "FRA", "hollande": "FRA", "macron": "FRA",
    "berlusconi": "ITA", "prodi": "ITA", "letta": "ITA", "renzi": "ITA",
    "gentiloni": "ITA", "conte": "ITA", "draghi": "ITA", "meloni": "ITA",
    "zapatero": "ESP", "rajoy": "ESP", "sánchez": "ESP", "sanchez": "ESP",
    "vranitzky": "AUT", "schüssel": "AUT", "schuessel": "AUT",
    "gusenbauer": "AUT", "faymann": "AUT", "kern": "AUT",
    "kurz": "AUT", "schallenberg": "AUT", "nehammer": "AUT",
    "stocker": "AUT",
}


def _find_countries(claim_lc: str) -> list[str]:
    """Extract ISO-3-Codes aus dem Claim."""
    found: list[str] = []
    seen: set[str] = set()
    # längste Keys zuerst (z.B. "österreich" vor "rosa")
    for key in sorted(COUNTRY_KEYWORDS.keys(), key=len, reverse=True):
        if key in claim_lc:
            code = COUNTRY_KEYWORDS[key]
            if code not in seen:
                found.append(code)
                seen.add(code)
    # Person-Anker
    for person, code in PERSON_TO_COUNTRY.items():
        if person in claim_lc and code not in seen:
            found.append(code)
            seen.add(code)
    return found[:3]


# ---------------------------------------------------------------------------
# Year detection
# ---------------------------------------------------------------------------
import re as _re
_YEAR_RE = _re.compile(r"\b(19\d{2}|20\d{2})\b")


def _find_years(claim_lc: str) -> list[int]:
    """Extract years 1900-2099 aus dem Claim."""
    matches = _YEAR_RE.findall(claim_lc)
    seen: set[int] = set()
    years: list[int] = []
    for m in matches:
        y = int(m)
        if 1949 <= y <= 2030 and y not in seen:
            seen.add(y)
            years.append(y)
    return years[:3]


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _de_pct(v) -> str:
    if v is None:
        return "k. A."
    return f"{v}".replace(".", ",")


# --- Stichtagsbezug-Schutz (Phase 4 ParlGov) -------------------------------
#
# Daten-Schema-Boundary: ``data/parlgov.json`` hinterlegt NUR
# ``cabinet_start``, kein ``cabinet_end``. Das implizite Ende eines
# Kabinetts ist der ``cabinet_start`` des nächsten Kabinetts desselben
# Landes. Wir berechnen Supersession aus der Nachbarliste statt aus einem
# Feld, das im Datensatz nicht existiert.
#
# Pattern: lessons_learned.md Synthesizer-Inversions-Falle (Stichtagsbezug-
# Schutz) — analog services/wikidata.py _struct_marker(kind="amt") und
# services/ema.py.
_ISO_DATE_RE = re.compile(r"^(-?\d{4})-(\d{2})-(\d{2})")


def _parse_iso_date(d_iso: str | None) -> _dt.date | None:
    """ISO-Date-String → datetime.date oder None bei unparsbarem Wert."""
    if not d_iso:
        return None
    m = _ISO_DATE_RE.match(d_iso.strip())
    if not m:
        return None
    try:
        return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except (ValueError, TypeError):
        return None


def _is_cabinet_superseded(
    country_elections: list[dict],
    current_election: dict,
    today: _dt.date,
) -> tuple[bool, str | None]:
    """True wenn ein NEUERES Kabinett im selben Land existiert, dessen
    ``cabinet_start`` ≤ heute liegt.

    Returns ``(is_superseded, successor_start_iso)``. successor_start_iso
    ist das früheste Kabinetts-Start-Datum, das größer als das aktuelle
    ist und ≤ heute liegt (= effektives implizites End-Datum). Wenn nicht
    superseded, ist successor_start_iso None.
    """
    current_start = _parse_iso_date(current_election.get("cabinet_start"))
    if current_start is None:
        return False, None
    successor_start: _dt.date | None = None
    for e in country_elections:
        if e is current_election:
            continue
        cs = _parse_iso_date(e.get("cabinet_start"))
        if cs is None:
            continue
        if cs <= current_start:
            continue
        if cs > today:
            continue
        if successor_start is None or cs < successor_start:
            successor_start = cs
    if successor_start is None:
        return False, None
    return True, successor_start.isoformat()


def _is_stale_cabinet_status(
    election: dict,
    is_superseded: bool,
    today: _dt.date,
    months: int = 36,
) -> tuple[bool, str | None]:
    """Soft-Caveat-Trigger: jüngstes Kabinett seines Landes (kein
    Nachfolger gefunden), aber ``cabinet_start`` deutlich älter als
    ``months`` Monate → mögliche Daten-Refresh-Lücke.

    Returns ``(is_stale, since_iso)``. Wenn nicht stale, since_iso None.

    Hinweis: KEIN harter STRUKTURELL-Marker — das ist ein Daten-Caveat,
    kein Faktenfehler. Synthesizer soll wissen, dass die ParlGov-Daten
    möglicherweise nicht aktuell sind.

    Daten-Schema-Boundary: data/parlgov.json hinterlegt NIE ein
    ``cabinet_end``-Feld. Daher kann die Spec-Variante "start älter als 6
    Monate UND end leer" nicht angewendet werden (würde auf jedes aktuelle
    Kabinett feuern). Stattdessen: Schwelle gegen ParlGov-Release-Cadence
    (jährlich) — ein "latest" Kabinett, das ≥ 3 Jahre alt ist und keinen
    Nachfolger im Datensatz hat, ist verdächtig für eine Daten-Lücke,
    während Kabinette aus den letzten 1-2 Jahren als plausibel-aktuell
    durchgehen.
    """
    if is_superseded:
        return False, None
    cs = _parse_iso_date(election.get("cabinet_start"))
    if cs is None:
        return False, None
    # Threshold ≈ months × 30 Tage. Default 36 Monate (3 Jahre) > typische
    # Legislaturperiode-Hälfte, < kurze EU-Legislatur 5 Jahre.
    delta_days = (today - cs).days
    if delta_days < months * 30:
        return False, None
    return True, cs.isoformat()


def _struct_marker_parlgov(
    cname: str,
    etype: str,
    year: int | None,
    cabinet: str,
    cabinet_start: str,
    successor_start_iso: str,
    today_iso: str,
    base_headline: str,
) -> str:
    """STRUKTURELL FALSCH:-Prefix für superseded Kabinette.

    Pattern analog services/wikidata.py _struct_marker(kind="amt") /
    services/ema.py: explizites Marker-Prefix mit Verweis auf nachfolgendes
    Kabinett, damit der Synthesizer "X ist aktuell Regierungschef" /
    "X ist Bundeskanzler" / "Koalition Y regiert" gegen ein abgelöstes
    Kabinett korrekt als mostly_false/false verdicted.
    """
    year_str = f" {year}" if year is not None else ""
    return (
        f"STRUKTURELL FALSCH: Das Kabinett '{cabinet}' "
        f"({cname}, nach {etype}{year_str}, gebildet ab {cabinet_start}) "
        f"wurde laut ParlGov spätestens am {successor_start_iso} durch "
        f"ein nachfolgendes Kabinett abgelöst (heute: {today_iso}) — "
        f"es ist NICHT mehr die amtierende Regierung. Präsens-Aussagen "
        f"'X ist Regierungschef:in von {cname}' / 'Y-Koalition regiert "
        f"{cname}' sind für dieses historische Kabinett ohne neuere "
        f"Quelle nicht mehr zutreffend. Roh-Daten: {base_headline}"
    )


def _build_election_result(
    country_code: str,
    country: dict,
    election: dict,
) -> dict:
    """Baue ein Result-Dict für eine einzelne Wahl."""
    cname = country.get("name") or country_code
    parliament = country.get("parliament") or "Parlament"
    year = election.get("year")
    date = election.get("date") or ""
    etype = election.get("type") or "Wahl"
    winner = election.get("winner") or "k. A."
    winner_pct = election.get("winner_pct")
    winner_seats = election.get("winner_seats")
    cabinet = election.get("cabinet") or "k. A."
    cabinet_start = election.get("cabinet_start") or ""
    note = election.get("note") or ""

    seats_str = (
        f", {winner_seats} Sitze" if winner_seats is not None else ""
    )
    headline = (
        f"{cname} {etype} {year} (Datum: {date}): "
        f"Stärkste Kraft = {winner} mit {_de_pct(winner_pct)} %"
        f"{seats_str}. "
        f"Folgekabinett: {cabinet}"
        f"{f' (ab {cabinet_start})' if cabinet_start else ''}."
    )

    # Stichtagsbezug-Schutz: superseded → harter STRUKTURELL-Marker.
    # Stale (latest + ≥ 36 Monate alt + kein Nachfolger) → soft-Caveat.
    today = _dt.date.today()
    today_iso = today.isoformat()
    country_elections = country.get("elections") or []
    is_superseded, successor_start_iso = _is_cabinet_superseded(
        country_elections, election, today,
    )
    is_stale, stale_since_iso = _is_stale_cabinet_status(
        election, is_superseded, today, months=36,
    )

    if is_superseded and successor_start_iso and cabinet_start:
        display_value = _struct_marker_parlgov(
            cname=cname,
            etype=etype,
            year=year,
            cabinet=cabinet,
            cabinet_start=cabinet_start,
            successor_start_iso=successor_start_iso,
            today_iso=today_iso,
            base_headline=headline,
        )
    elif is_stale and stale_since_iso:
        # Daten-Caveat, KEIN STRUKTURELL-Marker — Kabinett ist laut
        # ParlGov-Snapshot das jüngste, aber ParlGov-Refresh-Boundary
        # könnte einen Nachfolger verpasst haben.
        display_value = (
            f"{headline} [Hinweis: ParlGov-Daten zu diesem Kabinett "
            f"nicht aktualisiert seit {stale_since_iso} — bei sehr "
            f"aktuellen Regierungswechseln ggf. eigene Recherche prüfen]"
        )
    else:
        display_value = headline

    description_parts = [
        f"ParlGov-Eintrag für die {etype} {year} in {cname} "
        f"({parliament}). Ergebnis: {winner} = {_de_pct(winner_pct)} % "
        f"der Stimmen{seats_str}. "
        f"Regierungsbildung: {cabinet}.",
    ]
    if note:
        description_parts.append(f"Kontext: {note}")
    description_parts.append(
        "Methodik: ParlGov 2024 aggregiert offizielle Wahlergebnisse und "
        "Kabinetts-Daten aus 37 EU+OECD-Demokratien (Doring & Manow, "
        "Univ. Bremen / Harvard Dataverse). Lizenz: permissive Open-Source. "
        "Zitiert werden Stimmen, Sitze, Koalitionen — keine eigene "
        "politische Bewertung."
    )

    return {
        "indicator_name": f"ParlGov {cname} {etype} {year}",
        "indicator": f"parlgov_{country_code.lower()}_{year}",
        "country": country_code,
        "country_name": cname,
        "year": str(year) if year is not None else "",
        "value": winner_pct,
        "display_value": display_value,
        "description": " ".join(p for p in description_parts if p),
        "url": "https://www.parlgov.org/data-info/",
        "source": "ParlGov (Univ. Bremen / Harvard Dataverse)",
    }


def _select_elections(
    country_code: str,
    country: dict,
    years: list[int],
    claim_lc: str,
) -> list[dict]:
    """Wähle relevante Wahlen für ein Land aus.

    - Jahresangaben im Claim haben Priorität.
    - Person-Anker → Wahlen, deren Kabinett den Namen enthält.
    - Sonst: jüngste 2 Wahlen.
    """
    elections = country.get("elections") or []
    if not elections:
        return []

    matched: list[dict] = []
    seen_keys: set[tuple] = set()

    # 1) Jahre
    for y in years:
        for e in elections:
            if e.get("year") == y and (country_code, y, e.get("date")) not in seen_keys:
                matched.append(e)
                seen_keys.add((country_code, y, e.get("date")))

    # 2) Person-Anker → Kabinett-Match
    for person in PERSON_TO_COUNTRY:
        if PERSON_TO_COUNTRY[person] != country_code:
            continue
        if person not in claim_lc:
            continue
        for e in elections:
            cab = (e.get("cabinet") or "").lower()
            if person in cab:
                key = (country_code, e.get("year"), e.get("date"))
                if key not in seen_keys:
                    matched.append(e)
                    seen_keys.add(key)

    # 3) Fallback: jüngste 2
    if not matched:
        elections_sorted = sorted(
            elections,
            key=lambda e: e.get("year") or 0,
            reverse=True,
        )
        matched = elections_sorted[:2]

    return matched[:3]


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_parlgov(analysis: dict) -> dict:
    """Static-First-Search gegen die ParlGov-Election-Database."""
    empty = {"source": "ParlGov", "type": "elections_data", "results": []}

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_parlgov(matchable):
        return empty

    data = _load_static_json()
    if not data:
        return empty

    countries_data = data.get("countries") or {}

    # Welche Länder?
    iso_codes = _find_countries(matchable)
    if not iso_codes:
        # Kein Land genannt — Default AT+DE (Schwerpunkt der Pack-Kuration)
        iso_codes = ["AUT", "DEU"]

    years = _find_years(matchable)

    results: list[dict] = []
    for code in iso_codes:
        country = countries_data.get(code)
        if not country:
            continue
        elections = _select_elections(code, country, years, matchable)
        for e in elections:
            results.append(_build_election_result(code, country, e))

    # Maximal 6 Results (i.d.R. 2 Länder × ≤ 3 Wahlen)
    results = results[:6]

    logger.info(
        "ParlGov: %d results for countries=%s years=%s",
        len(results), iso_codes, years,
    )
    return {
        "source": "ParlGov",
        "type": "elections_data",
        "results": results,
    }
