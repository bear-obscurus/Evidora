"""OSM Nominatim — Geocoding-Connector (Search/Reverse).

OpenStreetMap Nominatim ist ein globales Gazetteer, das Orts-/Adress-
Strings auf normalisierte Geo-Datensätze (Geometrie, Adresskomponenten,
OSM-Klassifikation) mappt. Für Faktencheck-Zwecke liefert es:

- Verifizierung "Stadt liegt in Land" (z. B. "Wien liegt in Österreich")
- "Hauptstadt von X" — Plausibilitätsprüfung über display_name + Klasse
- "Adresse [Strasse Hausnummer] in [Stadt]" — Existenz / PLZ-Check
- "Koordinaten von [Ort]" — autoritative lat/lon

Komplementär zu Wikidata (services/wikidata.py):
- Wikidata: strukturierte Triples (Hauptstadt-Beziehung, Einwohner)
- Nominatim: physische Verortung + Adress-Hierarchie + Geometrie

Bei Personen-Lebensdaten KEIN Trigger — das ist Wikidata-Domäne.
Bei Ortsnamen → parallel OK (verschiedene Datenmodelle).

API:
- https://nominatim.openstreetmap.org/search
- Public-Endpoint, kein Key
- **STRENGES Rate-Limit von 1 req/s** — wir setzen 1.5s Pause zwischen
  Calls, um in keinem Fall ans Limit zu kommen.
- User-Agent ZWINGEND identifizierbar, sonst 403/429.
- ODbL-Lizenz: Attribution + Share-Alike beim Re-Distributing.

Limitations:
- 1 req/s ist sehr knapp; bei Burst-Anfragen wäre eine eigene Nominatim-
  Instanz besser. Wir cachen daher aggressiv (24 h Modul-Cache).
- Max 2 Orte pro Claim (sonst frisst das Latency-Budget).
- Heuristische Ort-Extraktion: bevorzugt analysis.entities, fallback
  capitalised Token-Chunks im Claim.

Wiring: NICHT in AUTHORITATIVE_INDICATORS — Live-Quelle.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_OSM_URL = "https://www.openstreetmap.org/{otype}/{oid}"

# Nominatim verlangt **identifizierbaren** User-Agent. Wir setzen einen
# spezifischen UA mit Kontakt-Adresse — sonst droht 403/429.
NOMINATIM_USER_AGENT = (
    "Evidora/1.0 (https://evidora.eu; contact@evidora.eu)"
)

# Strenges Rate-Limit: 1 req/s laut Nominatim Usage Policy. Wir gehen
# auf 1.5s, um Burst-Anfragen sicher unter dem Limit zu halten.
RATE_LIMIT_SLEEP_S = 1.5
MAX_LOCATIONS_PER_CLAIM = 2
MAX_RESULTS_PER_LOCATION = 3
HTTP_TIMEOUT_S = 12.0

# Cache: location-key → (timestamp, response_dict). 24h TTL — Geocoding-
# Ergebnisse sind extrem stabil.
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 24 * 3600.0


# ---------------------------------------------------------------------------
# Trigger-Detection
# ---------------------------------------------------------------------------
#
# Wir triggern Nominatim nur, wenn der Claim klar geographisch ist —
# Wikidata bedient strukturierte Triples (Person → Geburtsort), wir
# bedienen Verortung/Adresse.

_NOMINATIM_TRIGGER_TERMS: tuple[str, ...] = (
    # Verortungs-Behauptungen
    "liegt in", "befindet sich in", "ist gelegen in",
    "liegt im", "befindet sich im",
    "liegt am", "liegt an der",
    # Adress-/PLZ-Patterns
    "postleitzahl", "plz ",
    "adresse ", "anschrift",
    # Hauptstadt + explizite Geographie-Trigger
    "hauptstadt von", "hauptstadt der",
    "ist hauptstadt", "ist die hauptstadt",
    # Koordinaten
    "koordinaten von", "koordinaten der",
    "geokoordinaten", "geo-koordinaten",
    "längengrad", "breitengrad",
    # Stadt-Land-Zuordnung explicit
    "stadt in ", "ort in ",
)

# Negative Trigger — wenn Claim klar Personen-Bezug hat, ist Wikidata
# zuständig. Wir wollen nicht für "Mozart wurde in Salzburg geboren"
# Geocoding triggern (Wikidata liefert das strukturiert + besser).
_PERSON_CONTEXT_TERMS: tuple[str, ...] = (
    "geboren in", "geboren am", "geburtsdatum",
    "gestorben in", "gestorben am", "verstarb in",
)


def _claim_mentions_nominatim(claim_lc: str) -> bool:
    """Prüft, ob der Claim eine Nominatim-Geocoding-Lookup rechtfertigt.

    Strategie:
    - Wenn der Claim klar Personen-Lebensdaten-Bezug hat ("geboren in"),
      Trigger ablehnen → Wikidata-Domäne.
    - Ansonsten: Substring-Match auf geographische Trigger-Terme.
    """
    if not claim_lc:
        return False
    if any(t in claim_lc for t in _PERSON_CONTEXT_TERMS):
        return False
    return any(t in claim_lc for t in _NOMINATIM_TRIGGER_TERMS)


def claim_mentions_nominatim_cached(claim: str) -> bool:
    """Stable Lower-Case-Wrapper für main.py-Wiring."""
    return _claim_mentions_nominatim((claim or "").lower())


# ---------------------------------------------------------------------------
# Place-Extraction
# ---------------------------------------------------------------------------
#
# Bevorzugte Quelle: analysis.entities (vom claim_analyzer befüllt mit
# Orten, Personen, Organisationen). Fallback: capitalised Wörter im Claim
# der Länge ≥3, mit kleinem Blacklist-Filter.

# Häufige Nicht-Orte, die durch das Capitalisation-Filter rutschen würden.
_PLACE_BLACKLIST: frozenset[str] = frozenset({
    "Postleitzahl", "Hauptstadt", "Stadt", "Ort", "Land", "Adresse",
    "Anschrift", "Koordinaten", "Geokoordinaten", "Längengrad",
    "Breitengrad", "Wikipedia", "Google", "Wikidata", "OSM",
    "Nominatim", "OpenStreetMap",
    # Monate/Wochentage — capitalised aber keine Orte
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
    "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag",
    "Samstag", "Sonntag",
})

# Auch zwei-Wort-Eigennamen erkennen: "New York", "Sankt Pölten".
_TOKEN_PATTERN = re.compile(
    r"\b([A-ZÄÖÜ][a-zäöüß]{2,}(?:[- ][A-ZÄÖÜ][a-zäöüß]{2,})?)\b"
)


def _extract_places(claim: str, analysis: dict) -> list[str]:
    """Liefert plausible Ortsnamen-Kandidaten in Priorität-Reihenfolge.

    1. analysis.entities (Strings, nach Längen-Filter ≥3)
    2. Capitalised Token-Chunks aus Claim (Blacklist-gefiltert)

    Deduplikation case-insensitive; Reihenfolge stabil (Auftreten).
    Maximum MAX_LOCATIONS_PER_CLAIM Treffer.
    """
    candidates: list[str] = []
    seen_lc: set[str] = set()

    def _push(name: str) -> None:
        n = (name or "").strip().strip(",. ;:!?")
        if not n or len(n) < 3:
            return
        if n in _PLACE_BLACKLIST:
            return
        key = n.lower()
        if key in seen_lc:
            return
        seen_lc.add(key)
        candidates.append(n)

    entities = (analysis or {}).get("entities", []) or []
    for ent in entities:
        if isinstance(ent, str):
            _push(ent)
        elif isinstance(ent, dict):
            # Defensiv — falls Analyzer mal strukturierte Entities liefert
            v = ent.get("name") or ent.get("value") or ent.get("text")
            if isinstance(v, str):
                _push(v)

    if len(candidates) < MAX_LOCATIONS_PER_CLAIM and claim:
        for match in _TOKEN_PATTERN.finditer(claim):
            _push(match.group(1))
            if len(candidates) >= MAX_LOCATIONS_PER_CLAIM:
                break

    return candidates[:MAX_LOCATIONS_PER_CLAIM]


# ---------------------------------------------------------------------------
# Geocoding-Helper
# ---------------------------------------------------------------------------
_OSM_TYPE_PATH = {
    "node": "node",
    "way": "way",
    "relation": "relation",
}

_COUNTRY_CODE_TO_ISO3 = {
    "at": "AUT", "de": "DEU", "ch": "CHE", "li": "LIE",
    "it": "ITA", "fr": "FRA", "es": "ESP", "pt": "PRT",
    "nl": "NLD", "be": "BEL", "lu": "LUX", "dk": "DNK",
    "se": "SWE", "no": "NOR", "fi": "FIN", "is": "ISL",
    "ie": "IRL", "gb": "GBR", "us": "USA", "ca": "CAN",
    "mx": "MEX", "br": "BRA", "ar": "ARG",
    "cz": "CZE", "sk": "SVK", "pl": "POL", "hu": "HUN",
    "si": "SVN", "hr": "HRV", "rs": "SRB", "ro": "ROU",
    "bg": "BGR", "gr": "GRC", "tr": "TUR", "ua": "UKR",
    "ru": "RUS", "by": "BLR", "ee": "EST", "lv": "LVA",
    "lt": "LTU", "jp": "JPN", "cn": "CHN", "kr": "KOR",
    "in": "IND", "au": "AUS", "nz": "NZL", "za": "ZAF",
    "eg": "EGY", "ma": "MAR", "ng": "NGA",
}

_COUNTRY_CODE_TO_NAME_DE = {
    "at": "Österreich", "de": "Deutschland", "ch": "Schweiz",
    "li": "Liechtenstein", "it": "Italien", "fr": "Frankreich",
    "es": "Spanien", "pt": "Portugal", "nl": "Niederlande",
    "be": "Belgien", "lu": "Luxemburg", "dk": "Dänemark",
    "se": "Schweden", "no": "Norwegen", "fi": "Finnland",
    "is": "Island", "ie": "Irland", "gb": "Vereinigtes Königreich",
    "us": "Vereinigte Staaten", "ca": "Kanada", "mx": "Mexiko",
    "br": "Brasilien", "ar": "Argentinien", "cz": "Tschechien",
    "sk": "Slowakei", "pl": "Polen", "hu": "Ungarn",
    "si": "Slowenien", "hr": "Kroatien", "rs": "Serbien",
    "ro": "Rumänien", "bg": "Bulgarien", "gr": "Griechenland",
    "tr": "Türkei", "ua": "Ukraine", "ru": "Russland",
    "by": "Belarus", "ee": "Estland", "lv": "Lettland",
    "lt": "Litauen", "jp": "Japan", "cn": "China",
    "kr": "Südkorea", "in": "Indien", "au": "Australien",
    "nz": "Neuseeland", "za": "Südafrika", "eg": "Ägypten",
    "ma": "Marokko", "ng": "Nigeria",
}


def _format_coord(lat: str | float | None, lon: str | float | None) -> str:
    """48.2082 / 16.3738 → ``48.2082°N, 16.3738°E``."""
    try:
        flat = float(lat)
        flon = float(lon)
    except (TypeError, ValueError):
        return ""
    ns = "N" if flat >= 0 else "S"
    ew = "E" if flon >= 0 else "W"
    return f"{abs(flat):.4f}°{ns}, {abs(flon):.4f}°{ew}"


def _build_result(query: str, entry: dict) -> dict | None:
    """Mappt einen Nominatim-Eintrag auf das Evidora-Result-Schema."""
    if not isinstance(entry, dict):
        return None

    osm_type_raw = (entry.get("osm_type") or "").lower()
    osm_type = _OSM_TYPE_PATH.get(osm_type_raw, "")
    osm_id = entry.get("osm_id")
    place_class = entry.get("class") or ""
    place_type = entry.get("type") or ""
    importance = entry.get("importance")
    display_name = entry.get("display_name") or query
    address = entry.get("address") or {}

    cc = (address.get("country_code") or "").lower()
    iso3 = _COUNTRY_CODE_TO_ISO3.get(cc, cc.upper() if cc else "—")
    country_name = (
        _COUNTRY_CODE_TO_NAME_DE.get(cc)
        or address.get("country")
        or "—"
    )

    # Primären Ortsbezeichner aus Adresse herausziehen — sonst display_name.
    primary = (
        address.get("city")
        or address.get("town")
        or address.get("village")
        or address.get("municipality")
        or address.get("county")
        or address.get("state")
        or entry.get("name")
        or display_name.split(",")[0].strip()
    )
    # Vermeide "Österreich, Österreich" wenn primary == country_name
    same_as_country = (
        country_name
        and country_name != "—"
        and primary
        and primary.lower() == country_name.lower()
    )
    if country_name and country_name != "—" and not same_as_country:
        indicator_name = f"{primary}, {country_name}"
    else:
        indicator_name = primary

    coord_str = _format_coord(entry.get("lat"), entry.get("lon"))
    type_label = (
        f"{place_class}={place_type}"
        if place_class and place_type
        else (place_class or place_type or "place")
    )
    display_value_bits = [primary]
    if place_type:
        display_value_bits.append(f" ({place_type})")
    if country_name and country_name != "—" and not same_as_country:
        display_value_bits.append(f", {country_name}")
    if coord_str:
        display_value_bits.append(f" — {coord_str}")
    display_value = "".join(display_value_bits)

    importance_str = (
        f"{importance:.3f}" if isinstance(importance, (int, float)) else "—"
    )
    description = (
        f"OSM-Klassifikation: {type_label}, importance={importance_str}. "
        f"display_name={display_name[:200]}"
    )

    if osm_type and osm_id is not None:
        url = NOMINATIM_OSM_URL.format(otype=osm_type, oid=osm_id)
        indicator = f"nominatim_{osm_id}"
    else:
        url = "https://www.openstreetmap.org/"
        indicator = "nominatim_unknown"

    return {
        "indicator_name": indicator_name,
        "indicator": indicator,
        "country": iso3,
        "country_name": country_name,
        "year": None,
        "value": None,
        "display_value": display_value,
        "description": description,
        "url": url,
        "source": "OpenStreetMap Nominatim (ODbL)",
    }


async def _fetch_one(client, query: str) -> list[dict]:
    """1 HTTP-Call gegen Nominatim → roh-Liste der Treffer (max 3)."""
    params = {
        "q": query,
        "format": "json",
        "limit": str(MAX_RESULTS_PER_LOCATION),
        "addressdetails": "1",
    }
    try:
        resp = await client.get(
            NOMINATIM_SEARCH_URL,
            params=params,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            logger.info(
                f"Nominatim HTTP {resp.status_code} für '{query[:50]}' "
                f"(body: {resp.text[:120]!r})"
            )
            return []
        data = resp.json()
        if not isinstance(data, list):
            return []
        return data[:MAX_RESULTS_PER_LOCATION]
    except Exception as e:
        logger.info(f"Nominatim fetch failed für '{query[:50]}': {e}")
        return []


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_nominatim(analysis: dict) -> dict:
    """Live-Geocoding-Lookup gegen OSM Nominatim.

    Liefert max MAX_RESULTS_PER_LOCATION × MAX_LOCATIONS_PER_CLAIM = 6
    Geocoding-Treffer (in der Praxis meist ≤3, weil wir pro Ort nur den
    besten Treffer übernehmen).

    Strategie:
    1. Trigger-Check (Lower-Case-Substrings im Claim).
    2. Place-Extraction (analysis.entities + Capital-Token-Fallback).
    3. Pro Ort 1 API-Call mit 1.5s Pause zwischen Calls (Rate-Limit
       1 req/s laut Nominatim Usage Policy).
    4. Top-Result pro Ort übernehmen.
    """
    empty = {
        "source": "OSM Nominatim",
        "type": "geocoding",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_nominatim(matchable):
        return empty

    places = _extract_places(claim or original, analysis or {})
    if not places:
        logger.info("Nominatim: Trigger ok, aber keine Ortsnamen extrahiert")
        return empty

    results: list[dict] = []
    headers = {"User-Agent": NOMINATIM_USER_AGENT}

    async with polite_client(timeout=HTTP_TIMEOUT_S, headers=headers) as client:
        for idx, place in enumerate(places):
            cache_key = place.lower()
            now = time.time()
            cached = _CACHE.get(cache_key)
            if cached and (now - cached[0] < _CACHE_TTL_S):
                logger.info(f"Nominatim: Cache-Hit für '{place[:40]}'")
                cached_results = cached[1].get("results", []) or []
                if cached_results:
                    results.extend(cached_results[:1])
                continue

            # Rate-Limit-Pause zwischen Live-Calls (nicht vor dem ersten).
            if idx > 0:
                await asyncio.sleep(RATE_LIMIT_SLEEP_S)

            raw = await _fetch_one(client, place)
            if not raw:
                # Negativ-Cache, damit wir nicht ständig retry'en
                _CACHE[cache_key] = (now, {"results": []})
                continue

            built: list[dict] = []
            seen_ids: set[str] = set()
            for entry in raw:
                r = _build_result(place, entry)
                if not r:
                    continue
                ind = r.get("indicator", "")
                if ind in seen_ids:
                    continue
                seen_ids.add(ind)
                built.append(r)
                if len(built) >= MAX_RESULTS_PER_LOCATION:
                    break

            # Top-1 pro Ort übernehmen (Constraint: max 3 Results gesamt).
            if built:
                results.append(built[0])

            # Cache: speichere Top-Results für 24h.
            _CACHE[cache_key] = (now, {"results": built[:1]})

    # Hard-Cap auf 3 Results (Constraint: "Limit max 3 Results").
    results = results[:3]

    if results:
        logger.info(
            f"Nominatim: {len(results)} Geocoding-Treffer für "
            f"places={places}"
        )

    return {
        "source": "OSM Nominatim",
        "type": "geocoding",
        "results": results,
    }
